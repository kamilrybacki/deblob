//! Provider-neutral remote fine-tune hook (spec §7/§8): a `TrainingJobSpec`
//! together with a `TrainingBackend` (submit/poll) sit BELOW
//! `deblob::retrain::FineTuneHook` (reused verbatim, never reimplemented).
//! [`TrainingBackendFineTuneHook`] adapts any [`TrainingBackend`] into a
//! `FineTuneHook`, so `deblob::retrain::RetrainPlan::run` drives a real
//! remote submit/poll job exactly as it already drives a synchronous
//! shell-out hook — no change to that orchestrator at all.
//!
//! The remote worker's role is TRAIN + UPLOAD ONLY: [`TrainingBackend::poll`]
//! returns artifact DIGESTS (`JobStatus::Done`), never weights. Promotion
//! stays entirely in Deblob — `continual::prequential` feeds those digests
//! into `deblob::model_registry`'s statistical gate + two-stage canary,
//! unchanged.
//!
//! Backend selection is config-driven: `RetrainPlan`/`PrequentialRunner`
//! only ever see `&dyn FineTuneHook` (a [`TrainingBackendFineTuneHook`]
//! wrapping whichever `Arc<dyn TrainingBackend>` the caller constructed) —
//! adding a Modal or Vast backend later means writing a new
//! `TrainingBackend` impl, never touching the runner.
//!
//! - [`fake_backend`] — [`FakeBackend`], the deterministic in-process
//!   backend EVERY test in this crate uses.
//! - [`hf_jobs`] — [`HfJobsBackend`], the original real backend (shells out
//!   to the `hf jobs` CLI), never invoked in this crate's test suite.
//! - [`modal`] — [`modal::ModalBackend`], the CHOSEN arm-C real backend
//!   (Modal T4 + the $30/mo free credit — the cheapest real-training
//!   path): talks to a Modal web endpoint over HTTP+JSON, headless token
//!   pair from env, never invoked in this crate's test suite except
//!   against a mocked HTTP boundary (`wiremock`), never a live network
//!   call.

pub mod fake_backend;
pub mod hf_jobs;
pub mod modal;

pub use fake_backend::FakeBackend;
pub use hf_jobs::{HfJobsBackend, HfJobsConfig};
pub use modal::{ModalBackend, ModalConfig, ModalCredentials};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use deblob::retrain::{FineTuneError, FineTuneHook, ModelArtifact, ReplaySet};
use deblob_slm::runtime::ModelFamily;
use sha2::{Digest, Sha256};

/// Which training method produced (or will produce) an artifact. Spec §8:
/// "Needle's method is `needle-custom`, NOT `lora-sft`" — kept as a
/// distinct enum variant (not a string constant) so the spec + reporter
/// carry it unambiguously. `Other` is the extension point for a future
/// method this harness doesn't know about yet.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TrainingMethod {
    LoraSft,
    NeedleCustom,
    Other(String),
}

impl TrainingMethod {
    pub fn as_str(&self) -> &str {
        match self {
            Self::LoraSft => "lora-sft",
            Self::NeedleCustom => "needle-custom",
            Self::Other(s) => s.as_str(),
        }
    }

    /// Spec §8: "reuse Task 3's Needle tag" —
    /// `deblob_slm::runtime::ModelFamily::NeedleContinualUpdate` maps to
    /// [`Self::NeedleCustom`]; every other family defaults to
    /// [`Self::LoraSft`]. This is the ONE place that mapping happens, so a
    /// future non-Needle `NeedleContinualUpdate` model (Task 3's own
    /// caveat) can override it explicitly rather than silently inheriting
    /// the wrong label.
    pub fn from_model_family(family: ModelFamily) -> Self {
        match family {
            ModelFamily::NeedleContinualUpdate => Self::NeedleCustom,
            ModelFamily::StandardForwardPass => Self::LoraSft,
        }
    }
}

/// LoRA hyperparameters — a minimal, additive stand-in for whichever
/// adapter-training knobs a real backend needs. **Unvalidated — ablate**,
/// same posture as every other tunable default in this codebase
/// (`deblob::feedback::FeedbackWeights`, `deblob::retrain::CurationConfig`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LoraParams {
    pub rank: u32,
    pub alpha: u32,
    pub learning_rate: f64,
    pub epochs: u32,
}

impl Default for LoraParams {
    fn default() -> Self {
        Self {
            rank: 8,
            alpha: 16,
            learning_rate: 1e-4,
            epochs: 1,
        }
    }
}

/// Cost/time ceiling for one training job.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Budget {
    pub max_usd: f64,
    pub max_runtime_minutes: u32,
}

/// The provider-neutral remote training job request (spec §8's literal
/// field list). Every field is a plain digest/string/number — no
/// backend-specific shape leaks in here; a `TrainingBackend` impl
/// translates this into its own wire format (e.g. an `hf jobs run`
/// command line, a Modal function call).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TrainingJobSpec {
    pub base_bundle_digest: String,
    pub dataset_digest: String,
    pub feedback_cutoff: i64,
    pub trainer_image_digest: String,
    pub method: TrainingMethod,
    pub lora: LoraParams,
    pub replay_manifest_digest: String,
    pub seed: u64,
    pub budget: Budget,
    pub output_uri: String,
}

/// Opaque handle a [`TrainingBackend::submit`] call returns — backend-
/// specific identity (a job id, a run URL, ...), never interpreted by
/// anything outside that backend's own `submit`/`poll` pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobHandle(pub String);

/// Conventional keys [`JobStatus::Done`]'s `artifact_digests` map carries —
/// every backend MUST populate both, so a caller never needs
/// backend-specific knowledge to find them.
pub const TRAINING_CHECKPOINT_KEY: &str = "training_checkpoint";
pub const QUANTIZED_WEIGHTS_KEY: &str = "quantized_weights";

/// A remote training job's lifecycle state. Spec §8: "submit job -> receive
/// gated quantized adapter" — `Done` carries DIGESTS only, never weights;
/// [`TrainingBackendFineTuneHook::train`] is the only place those digests
/// become a [`ModelArtifact`] `deblob::retrain::RetrainPlan` can gate.
#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    Running,
    Done {
        artifact_digests: BTreeMap<String, String>,
    },
    Failed {
        reason: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum TrainingBackendError {
    #[error("spec exceeds the budget ceiling: max_usd {requested} > allowed {allowed}")]
    OverBudget { requested: f64, allowed: f64 },
    #[error("submit failed: {0}")]
    Submit(String),
    #[error("poll failed: {0}")]
    Poll(String),
}

/// The provider-neutral remote-training port (spec §8). `submit` enqueues
/// `spec` and returns immediately with a [`JobHandle`]; `poll` is called
/// repeatedly (by [`TrainingBackendFineTuneHook::train`]) until it reports
/// `Done`/`Failed` — mirrors any real batch-job API (HF Jobs, a Modal
/// function call, a spot-GPU queue) without committing to one.
#[async_trait]
pub trait TrainingBackend: Send + Sync {
    async fn submit(&self, spec: &TrainingJobSpec) -> Result<JobHandle, TrainingBackendError>;
    async fn poll(&self, handle: &JobHandle) -> Result<JobStatus, TrainingBackendError>;
}

/// The ceiling [`validate_budget`] enforces — deliberately separate from
/// [`TrainingJobSpec::budget`] (the job's OWN requested budget): a spec
/// requesting more than this ceiling is rejected before any backend is
/// ever touched (spec §8: "Budget ceiling enforced: a spec over `max_usd`
/// is rejected before submit").
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BudgetPolicy {
    pub max_usd_ceiling: f64,
}

/// Rejects `spec` BEFORE any `TrainingBackend::submit` call if its
/// requested budget exceeds `policy`. Kept as a free function (not a
/// backend method) so the SAME check protects every backend uniformly —
/// `FakeBackend`, `HfJobsBackend`, and any future one — without
/// duplicating the guard in each `submit` impl.
pub fn validate_budget(
    spec: &TrainingJobSpec,
    policy: &BudgetPolicy,
) -> Result<(), TrainingBackendError> {
    if spec.budget.max_usd > policy.max_usd_ceiling {
        return Err(TrainingBackendError::OverBudget {
            requested: spec.budget.max_usd,
            allowed: policy.max_usd_ceiling,
        });
    }
    Ok(())
}

pub(super) fn digest_hex(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Every [`TrainingJobSpec`] field this harness holds fixed across a whole
/// continual-learning run (deploy-time configuration), as opposed to the
/// fields [`TrainingBackendFineTuneHook::train`] derives per-call
/// (`base_bundle_digest` from `base_snapshot`, `dataset_digest`/
/// `replay_manifest_digest` from the replay set's own content,
/// `feedback_cutoff` from wall-clock "now").
#[derive(Debug, Clone)]
pub struct FixedJobParams {
    pub trainer_image_digest: String,
    pub method: TrainingMethod,
    pub lora: LoraParams,
    pub seed: u64,
    pub requested_budget: Budget,
    pub output_uri: String,
}

/// Bounded poll attempts [`TrainingBackendFineTuneHook::train`] makes
/// before giving up — never an unbounded loop. `FakeBackend` (used by
/// every test) reports `Done` on its very first poll, so this bound is
/// never exercised in this crate's test suite; a real backend's poll
/// cadence/backoff is that backend's own concern.
const MAX_POLL_ATTEMPTS: u32 = 10;

/// Adapts any [`TrainingBackend`] into a `deblob::retrain::FineTuneHook` —
/// see the module docs. `RetrainPlan::run` calls `train(base_snapshot,
/// replay_set)` exactly as it would call `ShellFineTuneHook`; everything
/// below that call (submit -> poll -> digest extraction) is this type's
/// job alone.
pub struct TrainingBackendFineTuneHook<B: TrainingBackend> {
    backend: Arc<B>,
    budget_policy: BudgetPolicy,
    fixed: FixedJobParams,
    /// Test/audit seam: incremented every time `submit` is actually
    /// reached — lets a budget-ceiling test assert `submit` was NEVER
    /// called for an over-budget spec, not merely that `train` returned an
    /// error.
    submit_attempts: AtomicUsize,
}

impl<B: TrainingBackend> TrainingBackendFineTuneHook<B> {
    pub fn new(backend: Arc<B>, budget_policy: BudgetPolicy, fixed: FixedJobParams) -> Self {
        Self {
            backend,
            budget_policy,
            fixed,
            submit_attempts: AtomicUsize::new(0),
        }
    }

    pub fn submit_attempts(&self) -> usize {
        self.submit_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl<B: TrainingBackend> FineTuneHook for TrainingBackendFineTuneHook<B> {
    async fn train(
        &self,
        base_snapshot: &str,
        replay_set: &ReplaySet,
    ) -> Result<ModelArtifact, FineTuneError> {
        let jsonl = replay_set.to_jsonl();
        let dataset_digest = digest_hex(jsonl.as_bytes());
        let spec = TrainingJobSpec {
            base_bundle_digest: base_snapshot.to_string(),
            dataset_digest: dataset_digest.clone(),
            feedback_cutoff: now_ms(),
            trainer_image_digest: self.fixed.trainer_image_digest.clone(),
            method: self.fixed.method.clone(),
            lora: self.fixed.lora.clone(),
            replay_manifest_digest: dataset_digest,
            seed: self.fixed.seed,
            budget: self.fixed.requested_budget,
            output_uri: self.fixed.output_uri.clone(),
        };

        // Budget ceiling enforced BEFORE any backend is ever touched (spec
        // §8) — `submit_attempts` stays 0 on this path, proving `submit`
        // itself was never reached.
        validate_budget(&spec, &self.budget_policy)
            .map_err(|e| FineTuneError::Process(e.to_string()))?;

        self.submit_attempts.fetch_add(1, Ordering::SeqCst);
        let handle = self
            .backend
            .submit(&spec)
            .await
            .map_err(|e| FineTuneError::Process(e.to_string()))?;

        for _ in 0..MAX_POLL_ATTEMPTS {
            match self.backend.poll(&handle).await {
                Ok(JobStatus::Done { artifact_digests }) => {
                    let training_checkpoint_digest = artifact_digests
                        .get(TRAINING_CHECKPOINT_KEY)
                        .cloned()
                        .ok_or_else(|| {
                            FineTuneError::Parse(format!(
                                "backend omitted `{TRAINING_CHECKPOINT_KEY}`"
                            ))
                        })?;
                    let quantized_weights_digest = artifact_digests
                        .get(QUANTIZED_WEIGHTS_KEY)
                        .cloned()
                        .ok_or_else(|| {
                            FineTuneError::Parse(format!(
                                "backend omitted `{QUANTIZED_WEIGHTS_KEY}`"
                            ))
                        })?;
                    return Ok(ModelArtifact {
                        model_id: format!("{}-{}", self.fixed.method.as_str(), handle.0),
                        training_checkpoint_digest,
                        quantized_weights_digest,
                    });
                }
                Ok(JobStatus::Running) => continue,
                Ok(JobStatus::Failed { reason }) => {
                    return Err(FineTuneError::Process(format!(
                        "remote training job failed: {reason}"
                    )));
                }
                Err(e) => return Err(FineTuneError::Process(e.to_string())),
            }
        }
        Err(FineTuneError::Process(
            "remote training job did not complete within the bounded poll budget".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests;
