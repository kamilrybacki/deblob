//! Shadow classifier + shadow decision log (deblob-p2ab Task 5).
//!
//! Authoritative field list / policy grid / go-live criteria:
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 5 — shadow
//! log + go-live gate"; human-readable risk-coverage + go-live doc:
//! `docs/shadow-golive-gate.md`.
//!
//! # ZERO STATE MUTATION (the load-bearing invariant)
//!
//! [`ShadowClassifier::maybe_classify`] PROPOSES and LOGS. It never applies
//! anything to registry/index/candidate/schema state:
//!
//! - It calls `EvidenceStore::get_candidate` (read-only) and NOTHING ELSE on
//!   the `EvidenceStore` — never `upsert_candidate`, `set_state`,
//!   `set_cluster`, `add_variant`, or `append_evidence`.
//! - It calls `Registry::list_families_by_band_depth` (via
//!   [`crate::retrieval::retrieve_topk`], read-only) and NOTHING ELSE on the
//!   `Registry` — never `publish`.
//! - It calls `SemanticInferencer::classify` (an external HTTP round trip,
//!   not a local state mutation).
//! - It writes ONLY to the append-only [`ShadowLog`] — a side channel that
//!   is not registry/index/candidate/schema state at all.
//!
//! This is structural (the classifier is never handed a mutating method to
//! call), not just tested — the integration test `shadow_applies_nothing`
//! below exists to catch a regression, not to be the sole guarantee.
//!
//! The classifier's own in-memory debounce cache (`classified`, below) is
//! private bookkeeping — not registry/candidate/schema state — and is
//! explicitly excluded from the zero-mutation claim; it exists only to
//! avoid redundant model calls for an unchanged candidate-set digest, and
//! resets on process restart (same caveat as `ColdLane`'s in-memory rate
//! limiter).
//!
//! # Policy: deterministic gates only, never model confidence
//!
//! [`evaluate_policy`] is a pure function of [`PolicyGateInputs`] — a type
//! that, BY CONSTRUCTION, has no confidence/self-reported-certainty field
//! (there is none in the Task 1 contract to read: `InferenceDecision`
//! carries only fixed enums, never a score). The policy grid combines
//! deterministic retrieval geometry (rank, structural distance, top1/top2
//! margin, observation count) with the model's categorical `relation`
//! selection (itself gated against the deterministic id it names, never
//! trusted as self-reported confidence).

use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use data_encoding::HEXLOWER;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, SchemaId};
use deblob_core::ports::{CandidateState, EvidenceStore, Registry};
use deblob_monoid::Profile;
use deblob_slm::{
    build_prompt, AbstainCause, CandidateProfileView, EndpointStatus as InferenceEndpointStatus,
    FamilyCandidate, InferenceBudget, InferenceDecision, InferenceRequest, Novelty, Relation,
    SemanticInferencer,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::policy::PromotionPolicy;
use crate::retrieval::{retrieve_topk, RetrievalResult, RETRIEVAL_VERSION};

/// The 3-way contract version this classifier targets (Task 1's contract
/// has no global version constant of its own — `InferenceRequest
/// ::contract_version` is a caller-pinned field; this is the shadow
/// classifier's pin).
pub const SHADOW_CONTRACT_VERSION: u32 = 1;

/// Version of [`deblob_slm::build_prompt`]'s rendered template this
/// classifier targets. Bump whenever the prompt's fixed instruction/section
/// layout changes, so a shadow-log record can distinguish "same candidate,
/// different prompt" from noise (mirrors [`RETRIEVAL_VERSION`]'s purpose).
pub const SHADOW_PROMPT_TEMPLATE_VERSION: u32 = 1;

/// Version of the redaction policy `deblob_slm::prompt` applies
/// (`redact_field_name`/`detect_injection`). `deblob-slm` doesn't expose a
/// version constant of its own yet; pinned here until it does.
pub const SHADOW_REDACTION_POLICY_VERSION: &str = "deblob-slm-redact-v1";

// --- Policy grid (Hermes review, Task 5 — authoritative) -------------------

/// Maximum structural distance (Task 3 weighted distance, `[0.0, 1.0]`) the
/// selected candidate may have for the policy to accept.
pub const POLICY_MAX_DISTANCE: f32 = 0.15;
/// Minimum top1/top2 retrieval margin the policy requires.
pub const POLICY_MIN_MARGIN: f32 = 0.10;
/// Minimum candidate observation count the policy requires.
pub const POLICY_MIN_OBSERVATIONS: u64 = 20;

/// Deterministic gate inputs for [`evaluate_policy`] — and ONLY these.
///
/// Deliberately does not, and must never, carry a model self-confidence /
/// certainty score: there is no such field anywhere in the Task 1 contract
/// (`InferenceDecision` is a fixed 3-way enum with no numeric channel), so
/// there is structurally nothing of the kind to add here. `relation` is the
/// model's categorical selection, but it is evaluated against a fixed
/// allow-list (`{Exact, CompatibleDrift}`), never trusted as a confidence
/// signal.
#[derive(Debug, Clone, PartialEq)]
pub struct PolicyGateInputs {
    /// `true` iff the model proposed `MatchSchema` at all (as opposed to
    /// `NewCandidate`/`Abstain`, or the endpoint being unavailable).
    pub is_match_schema: bool,
    /// Retrieval rank (1-based) of the schema the model selected, if it
    /// selected one found in the retrieved top-k (it always is, by
    /// contract validation — `None` only when `is_match_schema` is false).
    pub selected_rank: Option<u32>,
    /// Structural distance (Task 3) of the selected schema.
    pub selected_distance: Option<f32>,
    /// `distance(rank 2) - distance(rank 1)` from retrieval — computed
    /// independently of what the model selected.
    pub top1_top2_margin: f32,
    /// The candidate cluster's `sample_count`.
    pub observation_count: u64,
    /// The model's selected relation, if `is_match_schema`.
    pub relation: Option<Relation>,
    /// Deterministic structural-compatibility check result (see
    /// [`ShadowDecision::deterministic_compatibility_result`]'s docs for
    /// what this currently measures and its documented limitation).
    pub deterministic_compat_passed: bool,
    /// `true` if the candidate's redacted field-name set contains a
    /// collision (two distinct original names redacting to the same
    /// escaped path — see [`detect_redaction_collision`]).
    pub redaction_collision: bool,
}

/// Why [`evaluate_policy`] rejected a decision. Bounded enum (not prose),
/// mirroring the Task 1 contract's own "enums, not free text" convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateReason {
    /// The model never proposed `MatchSchema` (it proposed `NewCandidate`,
    /// `Abstain`, or the endpoint was unavailable).
    NoMatchProposed,
    RankNotOne,
    DistanceExceeded,
    MarginTooSmall,
    InsufficientObservations,
    RelationNotEligible,
    DeterministicCompatibilityFailed,
    RedactionCollision,
}

/// What the DETERMINISTIC policy grid would decide for one shadow
/// classification — computed from [`PolicyGateInputs`] only, never from a
/// model confidence score (there is none).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyOutcome {
    pub would_accept: bool,
    /// Empty iff `would_accept`. Every gate that failed, not just the
    /// first — so a shadow-log reader can see the full rejection profile.
    pub gate_reasons: Vec<GateReason>,
}

/// The initial policy grid (Hermes review, Task 5 — authoritative): `rank
/// == 1`, `distance <= 0.15`, `margin >= 0.10`, `observations >= 20`,
/// `relation` in `{exact, compatible_drift}`, deterministic compatibility
/// passed, no redaction collision. Evaluates EVERY gate (does not
/// short-circuit) so [`PolicyOutcome::gate_reasons`] is complete.
pub fn evaluate_policy(inputs: &PolicyGateInputs) -> PolicyOutcome {
    let mut reasons = Vec::new();

    if !inputs.is_match_schema {
        reasons.push(GateReason::NoMatchProposed);
        return PolicyOutcome {
            would_accept: false,
            gate_reasons: reasons,
        };
    }

    if inputs.selected_rank != Some(1) {
        reasons.push(GateReason::RankNotOne);
    }
    match inputs.selected_distance {
        Some(d) if d <= POLICY_MAX_DISTANCE => {}
        _ => reasons.push(GateReason::DistanceExceeded),
    }
    if inputs.top1_top2_margin < POLICY_MIN_MARGIN {
        reasons.push(GateReason::MarginTooSmall);
    }
    if inputs.observation_count < POLICY_MIN_OBSERVATIONS {
        reasons.push(GateReason::InsufficientObservations);
    }
    match inputs.relation {
        Some(Relation::Exact) | Some(Relation::CompatibleDrift) => {}
        _ => reasons.push(GateReason::RelationNotEligible),
    }
    if !inputs.deterministic_compat_passed {
        reasons.push(GateReason::DeterministicCompatibilityFailed);
    }
    if inputs.redaction_collision {
        reasons.push(GateReason::RedactionCollision);
    }

    PolicyOutcome {
        would_accept: reasons.is_empty(),
        gate_reasons: reasons,
    }
}

/// What would happen if this decision were applied by a (not-yet-built, P3)
/// live-application path — the "counterfactual" the go-live gate's
/// precision/false-merge metrics are computed against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum LiveDisposition {
    /// The model proposed a match AND the policy grid would accept it.
    WouldAcceptMatch {
        schema_id: SchemaId,
        relation: Relation,
    },
    /// The model proposed a match but the policy grid would reject it.
    WouldRejectMatch { gate_reasons: Vec<GateReason> },
    /// The model proposed `NewCandidate`/`Abstain`, or the endpoint was
    /// unavailable — no live merge action either way.
    WouldRemainShadowOnly,
}

// --- Redaction collision -----------------------------------------------

/// `true` if two distinct field-tree positions in `fields` redact to the
/// SAME escaped path (e.g. two long original names truncated at
/// `MAX_NAME_LEN` to an identical prefix) — a case where the model literally
/// cannot distinguish the two original fields from the redacted view alone.
/// Computed entirely from already-redacted data (no raw name is read here).
pub fn detect_redaction_collision(fields: &[deblob_slm::RedactedFieldStat]) -> bool {
    let mut seen = HashSet::new();
    for field in fields {
        let joined = field
            .path
            .iter()
            .map(|seg| seg.escaped.as_str())
            .collect::<Vec<_>>()
            .join(">");
        if !seen.insert(joined) {
            return true;
        }
    }
    false
}

// --- Shadow decision log ----------------------------------------------

/// Whether the `SemanticInferencer` endpoint answered at all for this
/// classification. `Unavailable` covers every `InferenceError` variant
/// (transport failure, timeout, unparseable transport response) per the
/// global constraint: "endpoint unavailable/timeout = a shadow 'unavailable'
/// outcome, never a cold-lane/relay failure."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointStatus {
    Available,
    Unavailable,
}

/// The parsed model decision, log-shaped: selected id + rank, relation,
/// novelty/abstain code. `None` (the whole struct) when the endpoint was
/// unavailable — see [`ShadowDecision::endpoint_status`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParsedDecisionLog {
    pub selected_schema_id: Option<SchemaId>,
    pub selected_rank: Option<u32>,
    pub relation: Option<Relation>,
    pub novelty: Option<Novelty>,
    pub abstain_cause: Option<AbstainCause>,
}

/// Operator-supplied model/runtime metadata. The `SemanticInferencer` port
/// (Task 1) deliberately returns only an `InferenceDecision` — no
/// confidence, no metadata envelope — so none of this is DISCOVERABLE from
/// a `classify()` call; it is configured once per deployed endpoint and
/// stamped onto every [`ShadowDecision`] the classifier logs against that
/// endpoint. Fields the operator doesn't know (e.g. an opaque hosted
/// endpoint's exact runtime/quantization) are `None`, never guessed.
#[derive(Debug, Clone, Default)]
pub struct ModelMeta {
    pub model_id: String,
    pub model_digest: Option<String>,
    pub server_runtime_version: Option<String>,
    pub quantization: Option<String>,
    pub temperature: Option<f32>,
    pub seed: Option<u64>,
    pub structured_output_backend: Option<String>,
}

/// One immutable shadow-classification record (Hermes review, Task 5 —
/// authoritative field list). Never mutated after construction; appended
/// once to a [`ShadowLog`] and never updated in place — a human/adjudicated
/// label (always `None` in P2) is added by a SEPARATE, later, offline
/// process operating on the logged stream, not by rewriting this struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowDecision {
    // --- identity / evidence provenance ---------------------------------
    pub decision_id: String,
    pub cluster_id: CandidateId,
    pub source_id: String,
    pub observation_count: u64,
    pub observation_window_ms: i64,
    pub canonicalizer_version: String,
    pub monoid_version: String,
    pub redaction_policy_version: String,
    pub structural_evidence_hash: String,

    // --- retrieval -------------------------------------------------------
    pub retrieval_algorithm_version: u32,
    pub retrieved: Vec<FamilyCandidate>,
    pub top1_top2_margin: f32,
    pub candidate_set_hash: String,
    pub retrieval_latency_ms: u64,

    // --- prompt / model call metadata ------------------------------------
    pub prompt_template_version: u32,
    pub rendered_prompt_hash: String,
    pub model_id: String,
    pub model_digest: Option<String>,
    pub server_runtime_version: Option<String>,
    pub quantization: Option<String>,
    pub temperature: Option<f32>,
    pub seed: Option<u64>,
    pub max_tokens: Option<u32>,
    pub structured_output_backend: Option<String>,
    /// Sourced from `InferenceOutcome::telemetry.request_tokens` /
    /// `.response_tokens` (Task 5b). `None` when the `SemanticInferencer`
    /// endpoint was unavailable for this call, on a cache hit, or when the
    /// endpoint's response didn't include a `usage` object — never a
    /// fabricated value.
    pub request_tokens: Option<u32>,
    pub response_tokens: Option<u32>,

    // --- model response ----------------------------------------------------
    /// The decision text as returned by the model, AFTER contract
    /// validation. Safe to store verbatim: the Task 1 contract enforces
    /// `deny_unknown_fields` and validates `schema_id` against the
    /// retrieved id allow-list, so this can never contain a raw payload
    /// value or free prose — only the fixed enum/id vocabulary. Treat the
    /// underlying Redis stream as access-controlled regardless (per the
    /// amendment's "raw model response (access-controlled)").
    pub raw_model_response: Option<String>,
    pub parsed_decision: Option<ParsedDecisionLog>,
    /// `Some(..)` (a fixed descriptive message, not the underlying
    /// `ContractError` text — the `SemanticInferencer` port only surfaces a
    /// boolean flag, see `InferenceTelemetry::parse_error`) iff the final
    /// decision is a safe-abstain fallback caused by an unrecoverable
    /// parse-class contract failure.
    pub parse_error: Option<String>,
    /// Same shape/caveats as `parse_error`, for an unrecoverable
    /// `schema_id`-not-in-allow-list failure
    /// (`InferenceTelemetry::schema_validation_error`).
    pub schema_validation_error: Option<String>,
    /// Sourced from `InferenceOutcome::telemetry.repair_count` (Task 5b):
    /// `1` iff `HttpInferencer::classify`'s one mechanical repair ran for
    /// this call, `0` otherwise (including endpoint-unavailable and
    /// cache-hit calls).
    pub repair_count: u32,
    /// Sourced from `InferenceOutcome::telemetry.ttft_ms` (Task 5b). The
    /// current port is request/response, not streaming, so this is the
    /// documented conservative proxy (full call latency), not a true
    /// time-to-first-token — see `InferenceTelemetry::ttft_ms`'s docs.
    /// `None` on endpoint-unavailable or a cache hit.
    pub ttft_ms: Option<u64>,
    pub total_latency_ms: u64,
    pub endpoint_status: EndpointStatus,
    pub provider_error: Option<String>,

    // --- policy --------------------------------------------------------
    pub deterministic_compatibility_result: bool,
    pub policy_outcome: PolicyOutcome,
    pub counterfactual_live_disposition: LiveDisposition,

    // --- offline labeling (populated later, out of P2's scope) -----------
    pub human_label: Option<String>,
    pub correct_schema_id: Option<SchemaId>,
    pub correct_family_id: Option<FamilyId>,
    pub correct_relation: Option<Relation>,
    pub labeler_id: Option<String>,
    pub adjudication_version: Option<String>,

    pub logged_at_ms: i64,
}

/// Append-only shadow decision log. `append` must NEVER be interpreted as
/// (and no implementation may become) a write path for registry/candidate/
/// schema state — it is a side channel exclusively for [`ShadowDecision`]
/// records.
#[async_trait::async_trait]
pub trait ShadowLog: Send + Sync {
    async fn append(
        &self,
        cand_id: &CandidateId,
        decision: &ShadowDecision,
    ) -> Result<(), CoreError>;
}

/// Shadow stream entries are (approximately) trimmed to this many most
/// recent decisions per candidate — same bounded-growth pattern as
/// `deblob_redis::evidence`'s `EVIDENCE_STREAM_MAXLEN` (spec §6 precedent).
const SHADOW_STREAM_MAXLEN: u64 = 1000;

fn shadow_key(cand_id: &CandidateId) -> String {
    format!("deblob:shadow:{}", cand_id.as_str())
}

/// The default [`ShadowLog`]: `XADD deblob:shadow:<candidate_id> MAXLEN ~
/// 1000 * data <json>` — an unbounded-growth-safe, append-only Redis
/// stream, one per candidate cluster, mirroring
/// `deblob_redis::RedisEvidence`'s own evidence-stream pattern.
pub struct RedisShadowLog {
    conn: redis::aio::ConnectionManager,
}

impl RedisShadowLog {
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(deblob_redis::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self { conn })
    }
}

#[async_trait::async_trait]
impl ShadowLog for RedisShadowLog {
    async fn append(
        &self,
        cand_id: &CandidateId,
        decision: &ShadowDecision,
    ) -> Result<(), CoreError> {
        let mut conn = self.conn.clone();
        let key = shadow_key(cand_id);
        let payload = serde_json::to_string(decision).map_err(|e| {
            CoreError::RegistryUnavailable(format!("serialize shadow decision: {e}"))
        })?;

        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("MAXLEN")
            .arg("~")
            .arg(SHADOW_STREAM_MAXLEN)
            .arg("*")
            .arg("data")
            .arg(&payload)
            .query_async(&mut conn)
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("XADD shadow log: {e}")))?;

        Ok(())
    }
}

// --- Shadow classifier --------------------------------------------------

/// Configuration for [`ShadowClassifier`].
#[derive(Debug, Clone, Copy)]
pub struct ShadowConfig {
    /// The "stable" gate: `sample_count >= min_samples AND (last_seen_ms -
    /// first_seen_ms) >= min_age_ms`. Deliberately reuses
    /// [`PromotionPolicy`]'s exact shape (same evidentiary-bar concept as
    /// promotion) but is configured as an INDEPENDENT instance — shadow
    /// eligibility and promotion eligibility are allowed to diverge (e.g.
    /// shadow-classifying earlier than a candidate becomes
    /// promotion-eligible, to build up labeled precision samples sooner).
    pub eligibility: PromotionPolicy,
    pub retrieval_k: usize,
    pub inference_timeout_ms: u64,
    pub max_prompt_tokens: u32,
}

impl Default for ShadowConfig {
    fn default() -> Self {
        Self {
            eligibility: PromotionPolicy::default(),
            retrieval_k: crate::retrieval::DEFAULT_K,
            inference_timeout_ms: 5_000,
            max_prompt_tokens: 4_096,
        }
    }
}

/// A shadow classification once per STABLE candidate cluster, debounced by
/// candidate-set digest. SHADOW ONLY — see the module docs' zero-mutation
/// invariant.
pub struct ShadowClassifier {
    evidence: Arc<dyn EvidenceStore>,
    registry: Arc<dyn Registry>,
    inferencer: Arc<dyn SemanticInferencer>,
    log: Arc<dyn ShadowLog>,
    model: ModelMeta,
    config: ShadowConfig,
    /// Debounce cache: `(candidate_id, candidate_set_hash)` pairs already
    /// classified. Process-local, not registry/candidate/schema state — see
    /// module docs.
    classified: StdMutex<HashSet<(CandidateId, String)>>,
}

impl ShadowClassifier {
    pub fn new(
        evidence: Arc<dyn EvidenceStore>,
        registry: Arc<dyn Registry>,
        inferencer: Arc<dyn SemanticInferencer>,
        log: Arc<dyn ShadowLog>,
        model: ModelMeta,
        config: ShadowConfig,
    ) -> Self {
        Self {
            evidence,
            registry,
            inferencer,
            log,
            model,
            config,
            classified: StdMutex::new(HashSet::new()),
        }
    }

    /// Runs one shadow classification for `cand_id` IFF it is currently
    /// stable ([`ShadowConfig::eligibility`]) and has not already been
    /// shadow-classified for its current candidate-set digest. Returns
    /// `Ok(None)` for: candidate not found, not yet stable, or a debounced
    /// repeat — none of these are errors. `source_id` is provenance-only
    /// (the caller's best knowledge of which producer this cluster came
    /// from; the `EvidenceStore`/`Registry` ports carry no such field
    /// themselves per candidate).
    pub async fn maybe_classify(
        &self,
        cand_id: &CandidateId,
        source_id: &str,
    ) -> Result<Option<ShadowDecision>, CoreError> {
        let Some(record) = self.evidence.get_candidate(cand_id).await? else {
            return Ok(None);
        };
        if self.config.eligibility.check(&record).is_err() {
            return Ok(None);
        }

        let profile: Profile = serde_json::from_value(record.profile.clone())
            .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt profile: {e}")))?;

        let retrieval_started = Instant::now();
        let retrieval: RetrievalResult =
            retrieve_topk(&profile, self.registry.as_ref(), self.config.retrieval_k).await?;
        let retrieval_latency_ms = retrieval_started.elapsed().as_millis() as u64;

        let candidate_set_hash = candidate_set_digest(&retrieval.candidates);

        {
            let mut seen = self.classified.lock().unwrap();
            let key = (cand_id.clone(), candidate_set_hash.clone());
            if seen.contains(&key) {
                return Ok(None);
            }
            seen.insert(key);
        }

        let candidate_view = CandidateProfileView::from_profile(&profile);
        let allowed_ids: Vec<SchemaId> = retrieval
            .candidates
            .iter()
            .map(|c| c.schema_id.clone())
            .collect();
        let prompt = build_prompt(&candidate_view, &retrieval.candidates, &allowed_ids);

        let request = InferenceRequest {
            candidate: candidate_view.clone(),
            retrieved: retrieval.candidates.clone(),
            contract_version: SHADOW_CONTRACT_VERSION,
            budget: InferenceBudget {
                max_prompt_tokens: self.config.max_prompt_tokens,
                timeout_ms: self.config.inference_timeout_ms,
            },
            prompt: prompt.text.clone(),
        };

        let call_started = Instant::now();
        let call_result = self.inferencer.classify(request).await;
        let total_latency_ms = call_started.elapsed().as_millis() as u64;

        let (endpoint_status, provider_error, raw_model_response, telemetry) = match &call_result {
            Ok(outcome) => (
                match outcome.telemetry.endpoint_status {
                    InferenceEndpointStatus::Ok => EndpointStatus::Available,
                    InferenceEndpointStatus::Unavailable | InferenceEndpointStatus::Timeout => {
                        EndpointStatus::Unavailable
                    }
                },
                None,
                serde_json::to_string(&outcome.decision).ok(),
                Some(outcome.telemetry.clone()),
            ),
            Err(err) => (
                EndpointStatus::Unavailable,
                Some(err.to_string()),
                None,
                None,
            ),
        };

        let (is_match_schema, selected_rank, selected_distance, relation) =
            match call_result.as_ref().ok().map(|outcome| &outcome.decision) {
                Some(InferenceDecision::MatchSchema {
                    schema_id,
                    relation,
                }) => {
                    let found = retrieval
                        .candidates
                        .iter()
                        .find(|c| &c.schema_id == schema_id);
                    (
                        true,
                        found.map(|c| c.rank),
                        found.map(|c| c.distance),
                        Some(*relation),
                    )
                }
                _ => (false, None, None, None),
            };

        let parsed_decision = call_result.as_ref().ok().map(|outcome| ParsedDecisionLog {
            selected_schema_id: match &outcome.decision {
                InferenceDecision::MatchSchema { schema_id, .. } => Some(schema_id.clone()),
                _ => None,
            },
            selected_rank,
            relation,
            novelty: match &outcome.decision {
                InferenceDecision::NewCandidate { novelty } => Some(*novelty),
                _ => None,
            },
            abstain_cause: match &outcome.decision {
                InferenceDecision::Abstain { cause } => Some(*cause),
                _ => None,
            },
        });

        let deterministic_compat_passed = selected_distance
            .map(|d| d <= POLICY_MAX_DISTANCE)
            .unwrap_or(false);
        let redaction_collision = detect_redaction_collision(&candidate_view.fields);

        let gate_inputs = PolicyGateInputs {
            is_match_schema,
            selected_rank,
            selected_distance,
            top1_top2_margin: retrieval.top1_top2_margin,
            observation_count: record.sample_count,
            relation,
            deterministic_compat_passed,
            redaction_collision,
        };
        let policy_outcome = evaluate_policy(&gate_inputs);

        let counterfactual_live_disposition = match (
            call_result.as_ref().ok().map(|outcome| &outcome.decision),
            is_match_schema,
        ) {
            (
                Some(InferenceDecision::MatchSchema {
                    schema_id,
                    relation,
                }),
                true,
            ) => {
                if policy_outcome.would_accept {
                    LiveDisposition::WouldAcceptMatch {
                        schema_id: schema_id.clone(),
                        relation: *relation,
                    }
                } else {
                    LiveDisposition::WouldRejectMatch {
                        gate_reasons: policy_outcome.gate_reasons.clone(),
                    }
                }
            }
            _ => LiveDisposition::WouldRemainShadowOnly,
        };

        let decision = ShadowDecision {
            decision_id: uuid::Uuid::now_v7().to_string(),
            cluster_id: cand_id.clone(),
            source_id: source_id.to_string(),
            observation_count: record.sample_count,
            observation_window_ms: record.last_seen_ms - record.first_seen_ms,
            canonicalizer_version: deblob_fingerprint::CANONICALIZER.to_string(),
            monoid_version: deblob_monoid::GENERALIZER.to_string(),
            redaction_policy_version: SHADOW_REDACTION_POLICY_VERSION.to_string(),
            structural_evidence_hash: HEXLOWER.encode(&profile.generalized_fingerprint()),

            retrieval_algorithm_version: RETRIEVAL_VERSION,
            retrieved: retrieval.candidates.clone(),
            top1_top2_margin: retrieval.top1_top2_margin,
            candidate_set_hash,
            retrieval_latency_ms,

            prompt_template_version: SHADOW_PROMPT_TEMPLATE_VERSION,
            rendered_prompt_hash: HEXLOWER.encode(&prompt.prompt_hash),
            model_id: self.model.model_id.clone(),
            model_digest: self.model.model_digest.clone(),
            server_runtime_version: self.model.server_runtime_version.clone(),
            quantization: self.model.quantization.clone(),
            temperature: self.model.temperature,
            seed: self.model.seed,
            max_tokens: Some(self.config.max_prompt_tokens),
            structured_output_backend: self.model.structured_output_backend.clone(),
            request_tokens: telemetry.as_ref().and_then(|t| t.request_tokens),
            response_tokens: telemetry.as_ref().and_then(|t| t.response_tokens),

            raw_model_response,
            parsed_decision,
            parse_error: telemetry
                .as_ref()
                .filter(|t| t.parse_error)
                .map(|_| "parse_error flagged by SemanticInferencer telemetry".to_string()),
            schema_validation_error: telemetry
                .as_ref()
                .filter(|t| t.schema_validation_error)
                .map(|_| {
                    "schema_validation_error flagged by SemanticInferencer telemetry".to_string()
                }),
            repair_count: telemetry.as_ref().map(|t| t.repair_count).unwrap_or(0),
            ttft_ms: telemetry.as_ref().and_then(|t| t.ttft_ms),
            total_latency_ms,
            endpoint_status,
            provider_error,

            deterministic_compatibility_result: deterministic_compat_passed,
            policy_outcome,
            counterfactual_live_disposition,

            human_label: None,
            correct_schema_id: None,
            correct_family_id: None,
            correct_relation: None,
            labeler_id: None,
            adjudication_version: None,

            logged_at_ms: now_ms(),
        };

        self.log.append(cand_id, &decision).await?;

        Ok(Some(decision))
    }
}

// --- Periodic shadow sweep (P2-A/B Task 5b) ---------------------------

/// `source_id` stamped on every [`ShadowDecision`] the periodic sweep
/// produces (`ShadowDecision::source_id`), distinguishing sweep-triggered
/// classifications in the log from any other future caller of
/// `ShadowClassifier::maybe_classify`.
pub const SWEEP_SOURCE_ID: &str = "shadow-sweep";

/// Page size for the sweep's `EvidenceStore::list_candidates` pagination.
const SWEEP_PAGE_SIZE: usize = 500;

/// Periodically enumerates every PROVISIONAL candidate and offers each one
/// to `classifier.maybe_classify`, until `shutdown` is cancelled — the
/// runtime driver [`crate::serve::serve`] wires up when `[slm].enabled` is
/// `true` (see `crate::config::SlmConfig`).
///
/// This function is PURE SCHEDULING. It duplicates none of
/// [`ShadowClassifier::maybe_classify`]'s own eligibility logic: whether a
/// candidate is "stable enough" is entirely decided by
/// `ShadowConfig::eligibility` (built from `[slm].min_samples`/
/// `[slm].min_window_ms`) inside `maybe_classify` itself, and whether a
/// candidate has already been shadow-classified for its current
/// candidate-set digest is entirely decided by `maybe_classify`'s own
/// `classified` debounce cache. Calling `maybe_classify` for every
/// provisional candidate on every tick is therefore always safe and
/// idempotent — ineligible/already-classified candidates simply come back
/// `Ok(None)`.
///
/// Zero-mutation is preserved by construction: this loop performs only a
/// read-only `EvidenceStore::list_candidates` scan and calls
/// `maybe_classify` (itself proven zero-mutation, see the module docs) —
/// it issues no registry/evidence writes of its own.
pub async fn run_shadow_sweep(
    classifier: Arc<ShadowClassifier>,
    evidence: Arc<dyn EvidenceStore>,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick fires immediately (tokio::time::interval's documented
    // behavior) — consume it so the sweep doesn't run twice back-to-back
    // at startup before the configured interval has actually elapsed.
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("shadow sweep shutting down");
                return;
            }
            _ = ticker.tick() => {
                sweep_once(classifier.as_ref(), evidence.as_ref()).await;
            }
        }
    }
}

/// One full pass over every PROVISIONAL candidate, paginating until
/// `EvidenceStore::list_candidates` reports no further cursor.
async fn sweep_once(classifier: &ShadowClassifier, evidence: &dyn EvidenceStore) {
    let mut cursor: Option<String> = None;
    loop {
        let page = evidence
            .list_candidates(CandidateState::Provisional, cursor.clone(), SWEEP_PAGE_SIZE)
            .await;
        let (records, next_cursor) = match page {
            Ok(page) => page,
            Err(err) => {
                tracing::warn!(error = %err, "shadow sweep: list_candidates failed, will retry next tick");
                return;
            }
        };

        for record in &records {
            if let Err(err) = classifier
                .maybe_classify(&record.candidate_id, SWEEP_SOURCE_ID)
                .await
            {
                tracing::warn!(
                    candidate_id = %record.candidate_id.as_str(),
                    error = %err,
                    "shadow sweep: maybe_classify failed for candidate"
                );
            }
        }

        match next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
}

/// Deterministic digest of a retrieved candidate set — the debounce key
/// AND `ShadowDecision::candidate_set_hash`. Order-independent (sorted by
/// `schema_id` before hashing) so the same retrieved set never digests
/// differently due to caller-side ordering.
fn candidate_set_digest(candidates: &[FamilyCandidate]) -> String {
    let mut sorted: Vec<&FamilyCandidate> = candidates.iter().collect();
    sorted.sort_by(|a, b| a.schema_id.as_str().cmp(b.schema_id.as_str()));

    let mut hasher = Sha256::new();
    for c in sorted {
        hasher.update(c.schema_id.as_str().as_bytes());
        hasher.update(b"|");
        hasher.update(c.family_id.as_str().as_bytes());
        hasher.update(b"|");
        hasher.update(c.version.to_le_bytes());
        hasher.update(b";");
    }
    HEXLOWER.encode(&hasher.finalize())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::FamilyVersion;
    use deblob_core::ports::{CandidateRecord, CandidateState, FamilyRef, SchemaRecord};
    use deblob_fingerprint::{parse_bounded, Limits};
    use deblob_slm::{InferenceError, InferenceOutcome, InferenceTelemetry};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex2;

    use crate::coldlane::{ColdLane, SampleMeta};

    // --- Fakes -----------------------------------------------------------

    /// In-memory `EvidenceStore` fake that ALSO counts write-method calls,
    /// so `shadow_applies_nothing` can assert zero writes directly (not
    /// just infer it from unchanged state) — duplicated (not shared) from
    /// `crate::coldlane`'s own private test fake, per this workspace's
    /// established per-file fake convention.
    #[derive(Default)]
    struct FakeEvidence {
        candidates: StdMutex2<HashMap<CandidateId, CandidateRecord>>,
        clusters: StdMutex2<HashMap<String, CandidateId>>,
        variants: StdMutex2<HashMap<CandidateId, Vec<(String, String)>>>,
        write_calls: StdMutex2<u32>,
    }

    type EvidenceSnapshot = (Vec<(CandidateId, String)>, Vec<(String, String)>);

    impl FakeEvidence {
        fn snapshot(&self) -> EvidenceSnapshot {
            let candidates = self
                .candidates
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::to_string(v).unwrap()))
                .collect::<Vec<_>>();
            let clusters = self
                .clusters
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().to_string()))
                .collect::<Vec<_>>();
            (candidates, clusters)
        }

        fn write_call_count(&self) -> u32 {
            *self.write_calls.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl EvidenceStore for FakeEvidence {
        async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError> {
            *self.write_calls.lock().unwrap() += 1;
            self.candidates
                .lock()
                .unwrap()
                .insert(rec.candidate_id.clone(), rec);
            Ok(())
        }

        async fn get_candidate(
            &self,
            id: &CandidateId,
        ) -> Result<Option<CandidateRecord>, CoreError> {
            Ok(self.candidates.lock().unwrap().get(id).cloned())
        }

        async fn list_candidates(
            &self,
            state: CandidateState,
            _cursor: Option<String>,
            limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            let items: Vec<_> = self
                .candidates
                .lock()
                .unwrap()
                .values()
                .filter(|c| c.state == state)
                .take(limit)
                .cloned()
                .collect();
            Ok((items, None))
        }

        async fn append_evidence(
            &self,
            _id: &CandidateId,
            _stats: serde_json::Value,
        ) -> Result<(), CoreError> {
            *self.write_calls.lock().unwrap() += 1;
            Ok(())
        }

        async fn set_state(
            &self,
            id: &CandidateId,
            state: CandidateState,
        ) -> Result<(), CoreError> {
            *self.write_calls.lock().unwrap() += 1;
            if let Some(rec) = self.candidates.lock().unwrap().get_mut(id) {
                rec.state = state;
            }
            Ok(())
        }

        async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
            Ok(self.clusters.lock().unwrap().get(gen_fp).cloned())
        }

        async fn set_cluster(&self, gen_fp: &str, cand_id: &CandidateId) -> Result<(), CoreError> {
            *self.write_calls.lock().unwrap() += 1;
            self.clusters
                .lock()
                .unwrap()
                .insert(gen_fp.to_string(), cand_id.clone());
            Ok(())
        }

        async fn add_variant(
            &self,
            cand_id: &CandidateId,
            bucket_key: &str,
            fp_b32: &str,
        ) -> Result<(), CoreError> {
            *self.write_calls.lock().unwrap() += 1;
            let mut variants = self.variants.lock().unwrap();
            let entry = variants.entry(cand_id.clone()).or_default();
            let pair = (bucket_key.to_string(), fp_b32.to_string());
            if !entry.contains(&pair) {
                entry.push(pair);
            }
            Ok(())
        }

        async fn get_variants(
            &self,
            cand_id: &CandidateId,
        ) -> Result<Vec<(String, String)>, CoreError> {
            Ok(self
                .variants
                .lock()
                .unwrap()
                .get(cand_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    /// `Registry` fake that serves a fixed family list to
    /// `list_families_by_band_depth` and PANICS if `publish` is ever
    /// called — the shadow classifier must never promote anything, so any
    /// invocation is a hard test failure by construction, not just an
    /// after-the-fact count check.
    struct FakeRegistry {
        families: Vec<FamilyRef>,
    }

    #[async_trait::async_trait]
    impl Registry for FakeRegistry {
        async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
            Ok(None)
        }
        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            Ok(None)
        }
        async fn publish(
            &self,
            _record: SchemaRecord,
            _alias_from: &CandidateId,
            _bucket_key: &str,
            _variant_members: &[(String, String)],
            _actor: &str,
            _reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            panic!("shadow classifier must NEVER call Registry::publish — shadow means shadow");
        }
        async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
            Ok(None)
        }
        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
            Ok((vec![], None))
        }
        async fn list_families_in_buckets(
            &self,
            _bucket_keys: &[String],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            Ok(self.families.clone())
        }
        async fn list_families_by_band_depth(
            &self,
            _bands: &[u32],
            _depths: &[u32],
        ) -> Result<Vec<FamilyRef>, CoreError> {
            Ok(self.families.clone())
        }
        async fn family_version_schema(
            &self,
            _family_id: &deblob_core::id::FamilyId,
            _version: deblob_core::id::FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by shadow-lane tests")
        }
    }

    #[derive(Default)]
    struct InMemoryShadowLog {
        entries: StdMutex2<Vec<(CandidateId, ShadowDecision)>>,
    }

    impl InMemoryShadowLog {
        fn entries(&self) -> Vec<(CandidateId, ShadowDecision)> {
            self.entries.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ShadowLog for InMemoryShadowLog {
        async fn append(
            &self,
            cand_id: &CandidateId,
            decision: &ShadowDecision,
        ) -> Result<(), CoreError> {
            self.entries
                .lock()
                .unwrap()
                .push((cand_id.clone(), decision.clone()));
            Ok(())
        }
    }

    #[derive(Clone)]
    enum FakeOutcome {
        Decision(InferenceDecision),
        Unavailable(String),
    }

    /// Non-trivial [`InferenceTelemetry`] fixture — every field the shadow
    /// classifier is expected to plumb through gets a distinguishing,
    /// non-default value so a test can prove the mapping actually happened
    /// rather than merely leaving every `ShadowDecision` field at its
    /// zero value by coincidence.
    fn fake_telemetry() -> InferenceTelemetry {
        InferenceTelemetry {
            request_tokens: Some(123),
            response_tokens: Some(7),
            ttft_ms: Some(42),
            total_latency_ms: Some(55),
            repair_count: 1,
            endpoint_status: InferenceEndpointStatus::Ok,
            parse_error: false,
            schema_validation_error: false,
            model_id: Some("test-model".to_string()),
        }
    }

    struct FakeInferencer(FakeOutcome);

    #[async_trait::async_trait]
    impl SemanticInferencer for FakeInferencer {
        async fn classify(
            &self,
            _req: InferenceRequest,
        ) -> Result<InferenceOutcome, InferenceError> {
            match &self.0 {
                FakeOutcome::Decision(d) => Ok(InferenceOutcome {
                    decision: d.clone(),
                    telemetry: fake_telemetry(),
                }),
                FakeOutcome::Unavailable(msg) => Err(InferenceError::Transport(msg.clone())),
            }
        }
    }

    // --- fixtures ----------------------------------------------------------

    fn node_of(json: &str) -> deblob_fingerprint::Node {
        parse_bounded(json.as_bytes(), &Limits::default()).unwrap()
    }

    fn cand_id_of(json: &str) -> CandidateId {
        let node = node_of(json);
        let shape = deblob_fingerprint::shape_of(&node);
        CandidateId::from_digest(&deblob_fingerprint::fingerprint(&shape))
    }

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    /// Builds a minimal generalized-canonical JSON string matching the
    /// candidate's own shape closely enough for retrieval to score it a
    /// near-zero distance — same fixture pattern `retrieval.rs`'s own
    /// tests use (`gen_canonical`).
    fn gen_canonical_matching(candidate_json: &str) -> String {
        let node = node_of(candidate_json);
        let profile = Profile::from_node(&node);
        profile.generalized_canonical_json()
    }

    fn family_ref(schema_byte: u8, canonical: String) -> FamilyRef {
        FamilyRef {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(schema_byte),
            version: FamilyVersion(1),
            canonical,
        }
    }

    /// `ShadowConfig` with a lenient eligibility gate (`min_samples: 1,
    /// min_age_ms: 0`) so a single, freshly-ingested `ColdLane::ingest`
    /// call is immediately "stable" for these tests — production defaults
    /// (`PromotionPolicy::default()`) require real observation-window
    /// elapsed time, which a synchronous test can't fast-forward.
    fn lenient_config() -> ShadowConfig {
        ShadowConfig {
            eligibility: PromotionPolicy {
                min_samples: 1,
                min_age_ms: 0,
            },
            ..ShadowConfig::default()
        }
    }

    fn model_meta() -> ModelMeta {
        ModelMeta {
            model_id: "test-model".to_string(),
            ..ModelMeta::default()
        }
    }

    // --- tests --------------------------------------------------------

    /// CRITICAL INVARIANT: a shadow run logs exactly one `ShadowDecision`
    /// with the expected identity fields, AND leaves `EvidenceStore` state
    /// byte-for-byte unchanged (proved two ways: a before/after snapshot
    /// equality AND a zero write-call-count assertion) and never calls
    /// `Registry::publish` (proved structurally — see `FakeRegistry`'s
    /// `panic!`). Shadow means shadow.
    #[tokio::test]
    async fn shadow_applies_nothing() {
        let evidence = Arc::new(FakeEvidence::default());
        let payload = r#"{"widget_count":1,"active":true}"#;
        let cand_id = cand_id_of(payload);

        let lane = ColdLane::new(evidence.clone());
        lane.ingest(
            cand_id.clone(),
            &node_of(payload),
            SampleMeta {
                source: "src-a".to_string(),
                cursor: None,
            },
        )
        .await
        .unwrap();

        let registry = Arc::new(FakeRegistry {
            families: vec![family_ref(1, gen_canonical_matching(payload))],
        });
        let inferencer = Arc::new(FakeInferencer(FakeOutcome::Decision(
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            },
        )));
        let log = Arc::new(InMemoryShadowLog::default());
        let classifier = ShadowClassifier::new(
            evidence.clone(),
            registry.clone(),
            inferencer,
            log.clone(),
            model_meta(),
            lenient_config(),
        );

        let before = evidence.snapshot();
        let before_writes = evidence.write_call_count();

        let outcome = classifier.maybe_classify(&cand_id, "src-a").await.unwrap();

        let after = evidence.snapshot();
        let after_writes = evidence.write_call_count();

        assert!(
            outcome.is_some(),
            "a stable candidate must produce a decision"
        );
        let decision = outcome.unwrap();
        assert_eq!(decision.cluster_id, cand_id);
        assert_eq!(decision.observation_count, 1);
        assert_eq!(decision.endpoint_status, EndpointStatus::Available);
        assert!(
            decision.policy_outcome.would_accept
                || !decision.policy_outcome.gate_reasons.is_empty()
        );

        assert_eq!(log.entries().len(), 1, "exactly one ShadowDecision logged");
        assert_eq!(log.entries()[0].0, cand_id);

        assert_eq!(
            before, after,
            "EvidenceStore state must be byte-for-byte unchanged by a shadow run"
        );
        assert_eq!(
            before_writes, after_writes,
            "zero EvidenceStore write-method calls during a shadow run"
        );
        assert!(
            after_writes > 0,
            "sanity: the write(s) counted are ColdLane::ingest's own, made BEFORE the shadow run"
        );
    }

    /// Task 5b: proves `ShadowDecision`'s telemetry fields are actually
    /// populated from `InferenceOutcome::telemetry`, not left at their old
    /// hardcoded `None`/`0` values — the whole point of threading
    /// `InferenceOutcome` through the `SemanticInferencer` port.
    #[tokio::test]
    async fn shadow_decision_carries_telemetry_from_outcome() {
        let evidence = Arc::new(FakeEvidence::default());
        let payload = r#"{"telemetry_field":1}"#;
        let cand_id = cand_id_of(payload);

        let lane = ColdLane::new(evidence.clone());
        lane.ingest(
            cand_id.clone(),
            &node_of(payload),
            SampleMeta {
                source: "src-a".to_string(),
                cursor: None,
            },
        )
        .await
        .unwrap();

        let registry = Arc::new(FakeRegistry {
            families: vec![family_ref(9, gen_canonical_matching(payload))],
        });
        let inferencer = Arc::new(FakeInferencer(FakeOutcome::Decision(
            InferenceDecision::MatchSchema {
                schema_id: schema_id(9),
                relation: Relation::Exact,
            },
        )));
        let log = Arc::new(InMemoryShadowLog::default());
        let classifier = ShadowClassifier::new(
            evidence,
            registry,
            inferencer,
            log,
            model_meta(),
            lenient_config(),
        );

        let decision = classifier
            .maybe_classify(&cand_id, "src-a")
            .await
            .unwrap()
            .expect("stable candidate must produce a decision");

        let telemetry = fake_telemetry();
        assert_eq!(decision.request_tokens, telemetry.request_tokens);
        assert_eq!(decision.response_tokens, telemetry.response_tokens);
        assert_eq!(decision.ttft_ms, telemetry.ttft_ms);
        assert_eq!(decision.repair_count, telemetry.repair_count);
        assert_eq!(
            decision.repair_count, 1,
            "sanity: fixture repair_count is 1"
        );
        assert!(
            decision.request_tokens.is_some(),
            "request_tokens must no longer be structurally None"
        );
        assert!(
            decision.response_tokens.is_some(),
            "response_tokens must no longer be structurally None"
        );
        assert_eq!(decision.parse_error, None, "fixture has parse_error=false");
        assert_eq!(
            decision.schema_validation_error, None,
            "fixture has schema_validation_error=false"
        );
        assert_eq!(decision.endpoint_status, EndpointStatus::Available);
    }

    #[tokio::test]
    async fn endpoint_unavailable_records_and_continues() {
        let evidence = Arc::new(FakeEvidence::default());
        let payload = r#"{"x":1}"#;
        let cand_id = cand_id_of(payload);
        let lane = ColdLane::new(evidence.clone());
        lane.ingest(
            cand_id.clone(),
            &node_of(payload),
            SampleMeta {
                source: "src-a".to_string(),
                cursor: None,
            },
        )
        .await
        .unwrap();

        let registry = Arc::new(FakeRegistry {
            families: vec![family_ref(2, gen_canonical_matching(payload))],
        });
        let inferencer = Arc::new(FakeInferencer(FakeOutcome::Unavailable(
            "HTTP 503".to_string(),
        )));
        let log = Arc::new(InMemoryShadowLog::default());
        let classifier = ShadowClassifier::new(
            evidence.clone(),
            registry,
            inferencer,
            log.clone(),
            model_meta(),
            lenient_config(),
        );

        let before_writes = evidence.write_call_count();
        let outcome = classifier
            .maybe_classify(&cand_id, "src-a")
            .await
            .expect("endpoint unavailable must not error the caller (cold lane unaffected)");
        let after_writes = evidence.write_call_count();

        let decision = outcome.expect("an unavailable endpoint still logs a decision");
        assert_eq!(decision.endpoint_status, EndpointStatus::Unavailable);
        assert!(decision.provider_error.is_some());
        assert!(decision.parsed_decision.is_none());
        assert!(matches!(
            decision.counterfactual_live_disposition,
            LiveDisposition::WouldRemainShadowOnly
        ));
        assert_eq!(
            before_writes, after_writes,
            "zero writes even on endpoint failure"
        );
        assert_eq!(log.entries().len(), 1);
    }

    #[tokio::test]
    async fn debounce_one_shadow_per_candidate_set() {
        let evidence = Arc::new(FakeEvidence::default());
        let payload = r#"{"y":2}"#;
        let cand_id = cand_id_of(payload);
        let lane = ColdLane::new(evidence.clone());
        lane.ingest(
            cand_id.clone(),
            &node_of(payload),
            SampleMeta {
                source: "src-a".to_string(),
                cursor: None,
            },
        )
        .await
        .unwrap();

        let registry = Arc::new(FakeRegistry {
            families: vec![family_ref(3, gen_canonical_matching(payload))],
        });
        let inferencer = Arc::new(FakeInferencer(FakeOutcome::Decision(
            InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            },
        )));
        let log = Arc::new(InMemoryShadowLog::default());
        let classifier = ShadowClassifier::new(
            evidence,
            registry,
            inferencer,
            log.clone(),
            model_meta(),
            lenient_config(),
        );

        let first = classifier.maybe_classify(&cand_id, "src-a").await.unwrap();
        assert!(first.is_some(), "first call must classify");

        let second = classifier.maybe_classify(&cand_id, "src-a").await.unwrap();
        assert!(
            second.is_none(),
            "second call for the SAME candidate-set digest must be debounced"
        );

        assert_eq!(
            log.entries().len(),
            1,
            "debounce must prevent a redundant log entry, not just a redundant return value"
        );
    }

    /// Proves `evaluate_policy` is driven ENTIRELY by deterministic gate
    /// variables: two inputs where the model's categorical signal (relation
    /// = Exact, i.e. the strongest thing the contract lets a model say) is
    /// IDENTICAL, differing only in `top1_top2_margin` (a deterministic
    /// retrieval quantity) — flip a single deterministic number and the
    /// verdict flips, with nothing resembling a confidence score anywhere
    /// in `PolicyGateInputs` to have driven that flip instead.
    #[test]
    fn policy_uses_deterministic_gates_only() {
        let base = PolicyGateInputs {
            is_match_schema: true,
            selected_rank: Some(1),
            selected_distance: Some(0.05),
            top1_top2_margin: 0.02, // below POLICY_MIN_MARGIN
            observation_count: 50,
            relation: Some(Relation::Exact),
            deterministic_compat_passed: true,
            redaction_collision: false,
        };
        let rejected = evaluate_policy(&base);
        assert!(!rejected.would_accept);
        assert!(rejected.gate_reasons.contains(&GateReason::MarginTooSmall));

        let mut accepted_inputs = base.clone();
        accepted_inputs.top1_top2_margin = 0.25; // above POLICY_MIN_MARGIN
        let accepted = evaluate_policy(&accepted_inputs);
        assert!(
            accepted.would_accept,
            "gate_reasons: {:?}",
            accepted.gate_reasons
        );
        assert!(accepted.gate_reasons.is_empty());

        // A decision the model never proposed as a match at all is rejected
        // regardless of every other (otherwise-passing) gate.
        let mut no_match = base;
        no_match.is_match_schema = false;
        no_match.top1_top2_margin = 0.9;
        let outcome = evaluate_policy(&no_match);
        assert!(!outcome.would_accept);
        assert_eq!(outcome.gate_reasons, vec![GateReason::NoMatchProposed]);
    }

    #[test]
    fn redaction_collision_detects_duplicate_escaped_paths() {
        use deblob_slm::{RedactedFieldStat, RedactedName};

        let make = |escaped: &str| RedactedFieldStat {
            path: vec![RedactedName {
                escaped: escaped.to_string(),
                truncated: false,
                injection_flagged: false,
            }],
            depth: 0,
            present: 1,
            explicit_null: 0,
            types: deblob_monoid::TypeCounts::default(),
            nullable: false,
            numeric_buckets: vec![],
            array_empty_seen: false,
            array_partial_seen: false,
        };

        let no_collision = vec![make("\"a\""), make("\"b\"")];
        assert!(!detect_redaction_collision(&no_collision));

        let collision = vec![make("\"a\""), make("\"a\"")];
        assert!(detect_redaction_collision(&collision));
    }

    #[test]
    fn candidate_set_digest_is_order_independent() {
        let a = family_ref(1, "{}".to_string());
        let b = family_ref(2, "{}".to_string());
        let ranked = |refs: &[FamilyRef]| -> Vec<FamilyCandidate> {
            refs.iter()
                .enumerate()
                .map(|(i, r)| FamilyCandidate {
                    family_id: r.family_id.clone(),
                    schema_id: r.schema_id.clone(),
                    version: r.version.0,
                    distance: 0.0,
                    rank: (i + 1) as u32,
                })
                .collect()
        };
        let forward = candidate_set_digest(&ranked(&[a.clone(), b.clone()]));
        let backward = candidate_set_digest(&ranked(&[b, a]));
        assert_eq!(forward, backward);
    }
}
