//! [`ModalBackend`] — a SIBLING `TrainingBackend` to [`super::HfJobsBackend`]
//! (spec §7/§8): Modal's T4 GPUs + the $30/mo free credit make this the
//! cheapest REAL-training arm-C path. Headless only — [`ModalCredentials`]
//! is read from `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET` env vars
//! ([`ModalCredentials::from_env`]), never hardcoded, never logged (its
//! `Debug` impl redacts both fields). `submit`/`poll` talk to a Modal web
//! endpoint (the trainer app deployed by `deploy/experiment/modal/
//! trainer.py`) over plain HTTP+JSON via `reqwest` — no `modal` CLI, no
//! browser flow, nothing interactive.
//!
//! Same separation-of-duties posture as every other backend in this
//! module: [`ModalBackend::poll`] returns artifact DIGESTS only
//! (`JobStatus::Done`), never weights, and this type has NO path to
//! `deblob::model_registry::ModelRegistry::promote` — proved at compile
//! time in `modal::tests` via `static_assertions::assert_not_impl_any!`.
//! Promotion stays entirely in Deblob, exactly as it does for
//! [`super::FakeBackend`] and [`super::HfJobsBackend`].
//!
//! The budget ceiling (spec §8: "a spec over `max_usd` is rejected before
//! submit") is enforced TWICE, deliberately redundantly: once generically
//! by [`super::TrainingBackendFineTuneHook::train`] (via
//! [`super::validate_budget`], shared by every backend), and AGAIN here
//! inside [`ModalBackend::submit`] itself — so a caller that constructs a
//! `ModalBackend` directly and calls `submit` without going through the
//! hook still gets the same guarantee. `submit_calls()` only increments
//! after that second check passes, so a budget-rejection test can assert
//! the network path was never reached (mirrors `FakeBackend::submit_calls`
//! for the very same purpose).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;

use super::{
    validate_budget, BudgetPolicy, JobHandle, JobStatus, LoraParams, TrainingBackend,
    TrainingBackendError, TrainingJobSpec,
};

/// Env var names [`ModalCredentials::from_env`] reads — named constants so
/// the deploy-side Secret manifest and this code can never drift on the
/// literal string.
pub const MODAL_TOKEN_ID_ENV: &str = "MODAL_TOKEN_ID";
pub const MODAL_TOKEN_SECRET_ENV: &str = "MODAL_TOKEN_SECRET";

/// Modal's own token-pair auth (sent as the `Modal-Key`/`Modal-Secret`
/// headers Modal's proxy-auth convention expects). ALWAYS sourced from env
/// — [`Self::from_env`] is the only path a deploy-time caller should use;
/// the plain struct literal exists so tests can supply fixed values
/// without touching process env at all.
#[derive(Clone, PartialEq, Eq)]
pub struct ModalCredentials {
    pub token_id: String,
    pub token_secret: String,
}

impl std::fmt::Debug for ModalCredentials {
    /// Redacted — a token pair must never end up in a log line or a panic
    /// message, so `Debug` never exposes either field's real value.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModalCredentials")
            .field("token_id", &"<redacted>")
            .field("token_secret", &"<redacted>")
            .finish()
    }
}

impl ModalCredentials {
    /// Reads `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET` from the process
    /// environment — headless, non-interactive, no fallback to a
    /// hardcoded value. `Err` (never a panic) if either is unset.
    pub fn from_env() -> Result<Self, TrainingBackendError> {
        let token_id = std::env::var(MODAL_TOKEN_ID_ENV).map_err(|_| {
            TrainingBackendError::Submit(format!("{MODAL_TOKEN_ID_ENV} is not set"))
        })?;
        let token_secret = std::env::var(MODAL_TOKEN_SECRET_ENV).map_err(|_| {
            TrainingBackendError::Submit(format!("{MODAL_TOKEN_SECRET_ENV} is not set"))
        })?;
        Ok(Self {
            token_id,
            token_secret,
        })
    }
}

/// Deploy-time configuration for [`ModalBackend`] — the trainer app's own
/// web-endpoint base URL plus the caching/spend-cap knobs Hermes flagged
/// (cold starts on Modal are billed): pin an image tag/digest and a named
/// Modal Volume for base-model weights so a run reuses both instead of
/// rebuilding/re-downloading every round.
#[derive(Debug, Clone)]
pub struct ModalConfig {
    /// Base URL of the deployed Modal web endpoint (e.g.
    /// `https://<workspace>--deblob-trainer.modal.run`) that
    /// `deploy/experiment/modal/trainer.py` exposes. `/submit` and
    /// `/status/<job_id>` are appended to this by [`ModalBackend`].
    pub endpoint_base: String,
    /// The Modal app name — audit/labeling only, never part of auth.
    pub app_name: String,
    /// Pinned trainer image tag/digest — reused across rounds so Modal
    /// serves a warm/cached image instead of rebuilding one per submit
    /// (spend-cap note: image builds are billed compute time).
    pub cached_image_tag: String,
    /// Name of the Modal Volume the trainer mounts to cache base-model
    /// weights across cold starts (spend-cap note: re-downloading a base
    /// model every round is the single biggest avoidable cost on a
    /// pay-per-second GPU).
    pub cached_volume_name: String,
    /// The SAME ceiling [`super::validate_budget`] enforces at the hook
    /// level — duplicated here so `ModalBackend::submit` rejects an
    /// over-budget spec even if called directly (see module docs).
    pub budget_policy: BudgetPolicy,
}

/// A translated, ready-to-send Modal HTTP request — separated from
/// actually sending it so `submit`/`poll`'s request CONSTRUCTION is
/// unit-testable without any network or mock server (mirrors
/// `HfJobsBackend::build_command`'s pure-argv-builder pattern).
#[derive(Debug, Clone, PartialEq)]
pub struct ModalRequest {
    pub url: String,
    /// `(header name, header value)` pairs, in insertion order. Carries
    /// the `Modal-Key`/`Modal-Secret` auth pair — sourced from
    /// [`ModalCredentials`] (itself env-sourced), never a literal in this
    /// function.
    pub headers: Vec<(String, String)>,
    pub body: serde_json::Value,
}

/// Modal's own wire format for a submit call — a FLAT JSON object with
/// `method` as the conventional wire STRING (`spec.method.as_str()`:
/// `"lora-sft"` / `"needle-custom"` / a custom tag), never the Rust enum's
/// derived tag shape. Mirrors `HfJobsBackend::build_command`'s
/// `spec.method.as_str().to_string()` convention so both backends speak
/// the same string vocabulary to whatever trains the model.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
struct ModalTrainingRequestBody {
    base_bundle_digest: String,
    dataset_digest: String,
    feedback_cutoff: i64,
    trainer_image_digest: String,
    method: String,
    lora: LoraParams,
    replay_manifest_digest: String,
    seed: u64,
    budget_max_usd: f64,
    budget_max_runtime_minutes: u32,
    output_uri: String,
    /// Deploy-side caching knobs (see [`ModalConfig`] docs) — passed
    /// through so the trainer reuses the pinned image/volume rather than
    /// re-resolving them per job.
    cached_image_tag: String,
    cached_volume_name: String,
}

impl ModalTrainingRequestBody {
    fn from_spec(spec: &TrainingJobSpec, config: &ModalConfig) -> Self {
        Self {
            base_bundle_digest: spec.base_bundle_digest.clone(),
            dataset_digest: spec.dataset_digest.clone(),
            feedback_cutoff: spec.feedback_cutoff,
            trainer_image_digest: spec.trainer_image_digest.clone(),
            method: spec.method.as_str().to_string(),
            lora: spec.lora.clone(),
            replay_manifest_digest: spec.replay_manifest_digest.clone(),
            seed: spec.seed,
            budget_max_usd: spec.budget.max_usd,
            budget_max_runtime_minutes: spec.budget.max_runtime_minutes,
            output_uri: spec.output_uri.clone(),
            cached_image_tag: config.cached_image_tag.clone(),
            cached_volume_name: config.cached_volume_name.clone(),
        }
    }
}

/// Modal's own wire format for a submit response.
#[derive(Debug, serde::Deserialize)]
struct ModalSubmitResponseBody {
    job_id: String,
}

/// Modal's own wire format for a status-poll response — externally
/// tagged on `status` so a malformed/unexpected body fails to deserialize
/// (never silently mis-mapped) rather than panicking.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum ModalPollResponseBody {
    Running,
    Done {
        artifact_digests: BTreeMap<String, String>,
    },
    Failed {
        reason: String,
    },
}

/// Parses a raw Modal status-poll HTTP body into a provider-neutral
/// [`JobStatus`] — pure, no I/O, so it is fully unit-testable on its own.
/// A body that fails to parse maps to a SAFE `JobStatus::Failed` (never a
/// panic, never an infinite `Running`) — the poll loop in
/// `TrainingBackendFineTuneHook::train` is bounded regardless, but this
/// keeps a single malformed response from masquerading as either success
/// or an unbounded retry.
fn parse_poll_response(body: &str) -> JobStatus {
    match serde_json::from_str::<ModalPollResponseBody>(body) {
        Ok(ModalPollResponseBody::Running) => JobStatus::Running,
        Ok(ModalPollResponseBody::Done { artifact_digests }) => {
            JobStatus::Done { artifact_digests }
        }
        Ok(ModalPollResponseBody::Failed { reason }) => JobStatus::Failed { reason },
        Err(e) => JobStatus::Failed {
            reason: format!("malformed Modal poll response: {e}"),
        },
    }
}

/// The Modal `TrainingBackend` — Arm C's cheapest real-training path (T4 +
/// the $30/mo free credit). A SIBLING of [`super::HfJobsBackend`], picked
/// by config (`backend = "modal"` in `deploy/experiment/
/// 30-experiment-config.yaml`) — nothing else in the runner changes.
pub struct ModalBackend {
    config: ModalConfig,
    credentials: ModalCredentials,
    http: reqwest::Client,
    /// Incremented only AFTER the budget check passes and immediately
    /// before the HTTP submit call — see module docs.
    submit_calls: AtomicUsize,
    poll_calls: AtomicUsize,
}

impl ModalBackend {
    pub fn new(config: ModalConfig, credentials: ModalCredentials) -> Self {
        Self {
            config,
            credentials,
            http: reqwest::Client::new(),
            submit_calls: AtomicUsize::new(0),
            poll_calls: AtomicUsize::new(0),
        }
    }

    /// Convenience constructor for deploy-time callers: reads
    /// [`ModalCredentials::from_env`] rather than requiring the caller to
    /// plumb env-reading through themselves.
    pub fn from_env(config: ModalConfig) -> Result<Self, TrainingBackendError> {
        Ok(Self::new(config, ModalCredentials::from_env()?))
    }

    pub fn submit_calls(&self) -> usize {
        self.submit_calls.load(Ordering::SeqCst)
    }

    pub fn poll_calls(&self) -> usize {
        self.poll_calls.load(Ordering::SeqCst)
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        vec![
            ("Modal-Key".to_string(), self.credentials.token_id.clone()),
            (
                "Modal-Secret".to_string(),
                self.credentials.token_secret.clone(),
            ),
        ]
    }

    /// Builds the non-interactive submit request — pure, independently
    /// testable without a network call.
    pub fn build_submit_request(&self, spec: &TrainingJobSpec) -> ModalRequest {
        let body = ModalTrainingRequestBody::from_spec(spec, &self.config);
        ModalRequest {
            url: format!("{}/submit", self.config.endpoint_base),
            headers: self.auth_headers(),
            body: serde_json::to_value(body).expect("ModalTrainingRequestBody always serializes"),
        }
    }

    /// Builds the non-interactive status-poll request — pure, same
    /// rationale as [`Self::build_submit_request`].
    pub fn build_status_request(&self, handle: &JobHandle) -> ModalRequest {
        ModalRequest {
            url: format!("{}/status/{}", self.config.endpoint_base, handle.0),
            headers: self.auth_headers(),
            body: serde_json::Value::Null,
        }
    }
}

#[async_trait]
impl TrainingBackend for ModalBackend {
    async fn submit(&self, spec: &TrainingJobSpec) -> Result<JobHandle, TrainingBackendError> {
        // Budget ceiling enforced BEFORE any network call (spec §8) — see
        // module docs on why this is checked here TOO, not solely at the
        // hook level.
        validate_budget(spec, &self.config.budget_policy)?;

        let req = self.build_submit_request(spec);
        self.submit_calls.fetch_add(1, Ordering::SeqCst);

        let mut builder = self.http.post(&req.url).json(&req.body);
        for (name, value) in &req.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| TrainingBackendError::Submit(e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| TrainingBackendError::Submit(e.to_string()))?;
        if !status.is_success() {
            return Err(TrainingBackendError::Submit(format!(
                "Modal submit returned {status}: {text}"
            )));
        }
        let parsed: ModalSubmitResponseBody = serde_json::from_str(&text).map_err(|e| {
            TrainingBackendError::Submit(format!("malformed Modal submit response: {e}"))
        })?;
        Ok(JobHandle(parsed.job_id))
    }

    async fn poll(&self, handle: &JobHandle) -> Result<JobStatus, TrainingBackendError> {
        let req = self.build_status_request(handle);
        self.poll_calls.fetch_add(1, Ordering::SeqCst);

        let mut builder = self.http.get(&req.url);
        for (name, value) in &req.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .send()
            .await
            .map_err(|e| TrainingBackendError::Poll(e.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|e| TrainingBackendError::Poll(e.to_string()))?;
        if !status.is_success() {
            return Err(TrainingBackendError::Poll(format!(
                "Modal status returned {status}: {text}"
            )));
        }
        Ok(parse_poll_response(&text))
    }
}

#[cfg(test)]
mod tests;
