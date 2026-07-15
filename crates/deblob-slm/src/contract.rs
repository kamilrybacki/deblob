//! The 3-way inference contract (spec §3.2; authoritative shape per
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 1 — contract").
//!
//! The model's output is EXACTLY one of:
//!
//! ```json
//! {"decision":"match_schema","schema_id":"sch_…","relation":"exact|compatible_drift|incompatible_similarity"}
//! {"decision":"new_candidate","novelty":"structural|semantic"}
//! {"decision":"abstain","cause":"ambiguous|insufficient_evidence|candidate_missing"}
//! ```
//!
//! No rationale/confidence field is ever requested or accepted — `relation`,
//! `novelty`, and `cause` are the only degrees of freedom, and they are fixed
//! enums. `schema_id` is validated deterministically outside the model
//! against the exact retrieved top-k id set (never trust the model's
//! self-reported allow-list membership).

use async_trait::async_trait;
use deblob_core::id::{FamilyId, SchemaId};
use serde::{Deserialize, Serialize};

/// The 3-way decision a `SemanticInferencer` proposes for a candidate cluster.
///
/// Serde-tagged discriminated union on the `decision` field; each variant
/// rejects unknown fields so a model that smuggles in extra keys (e.g. a
/// `confidence` or free-text `rationale`) is rejected deterministically
/// rather than silently accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum InferenceDecision {
    MatchSchema {
        schema_id: SchemaId,
        relation: Relation,
    },
    NewCandidate {
        novelty: Novelty,
    },
    Abstain {
        cause: AbstainCause,
    },
}

impl InferenceDecision {
    /// True only for a `MatchSchema` whose `relation` grants permission to
    /// tag the candidate as that schema (`Exact` or `CompatibleDrift`).
    ///
    /// CRITICAL: `IncompatibleSimilarity` returns `false`. It lives under
    /// `MatchSchema` because the model is reporting resemblance to a known
    /// schema, but resemblance is not permission — a schema that merely
    /// *looks like* `schema_id` must never be mistaken for a match
    /// downstream (that mistake is a false merge, the P2 shadow-log go-live
    /// hard gate). `NewCandidate` and `Abstain` are never accepted matches.
    pub fn is_accepted_match(&self) -> bool {
        matches!(
            self,
            InferenceDecision::MatchSchema {
                relation: Relation::Exact | Relation::CompatibleDrift,
                ..
            }
        )
    }
}

/// The relationship a `MatchSchema` decision claims between the candidate
/// and `schema_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    /// Physical/canonical equivalence. Deterministic code already knows
    /// this case; it is kept as a calibration/control case in the model
    /// output and every disagreement with the deterministic verdict is
    /// logged.
    Exact,
    /// Likely the same family AND deterministically compatible.
    CompatibleDrift,
    /// Resemblance WITHOUT permission to tag as that schema. Never an
    /// accepted match — see [`InferenceDecision::is_accepted_match`].
    IncompatibleSimilarity,
}

/// Why a candidate is proposed as a new family, when `MatchSchema` was not
/// warranted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Novelty {
    Structural,
    Semantic,
}

/// Why the model declined to decide. Bounded enum, never free prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbstainCause {
    Ambiguous,
    InsufficientEvidence,
    CandidateMissing,
}

/// Errors from validating a raw model response against the 3-way contract.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    /// A `MatchSchema` named a `schema_id` outside the retrieved top-k
    /// `allowed_ids` passed to [`validate_decision`].
    #[error("schema_id not in the retrieved top-k allow-list")]
    IdNotAllowed,
    /// The raw payload carried a field not in the contract (e.g. a
    /// `confidence` or `rationale` field).
    #[error("decision payload contains an unknown field")]
    UnknownField,
    /// The raw payload was not valid JSON, or did not match any contract
    /// variant.
    #[error("malformed decision payload: {0}")]
    Malformed(String),
}

/// Parse `raw` JSON into an [`InferenceDecision`] and enforce the id
/// allow-list deterministically, outside the model.
///
/// - Unknown fields (e.g. a smuggled `confidence`) → [`ContractError::UnknownField`].
/// - Any other parse/shape failure → [`ContractError::Malformed`].
/// - A `MatchSchema` naming a `schema_id` not present in `allowed_ids` →
///   [`ContractError::IdNotAllowed`], even if the payload was otherwise
///   well-formed.
pub fn validate_decision(
    raw: &str,
    allowed_ids: &[SchemaId],
) -> Result<InferenceDecision, ContractError> {
    let decision: InferenceDecision = serde_json::from_str(raw).map_err(|err| {
        if err.to_string().contains("unknown field") {
            ContractError::UnknownField
        } else {
            ContractError::Malformed(err.to_string())
        }
    })?;

    if let InferenceDecision::MatchSchema { schema_id, .. } = &decision {
        if !allowed_ids.contains(schema_id) {
            return Err(ContractError::IdNotAllowed);
        }
    }

    Ok(decision)
}

// --- Minimal request/port scaffolding, consumed by Tasks 2-5 -------------

/// Redacted, monoid-statistics-only view of the candidate cluster — see
/// [`crate::prompt::CandidateProfileView`] (Task 4) for the concrete shape
/// and its `from_profile` constructor. Re-exported here (rather than
/// defined here) because [`InferenceRequest`] needs the type but the
/// PII-safe redaction logic that produces it belongs to `crate::prompt`.
pub use crate::prompt::CandidateProfileView;

/// One retrieved top-k family candidate (deterministic retrieval, Task 3),
/// offered to the model as part of the allowed `schema_id` set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FamilyCandidate {
    pub family_id: FamilyId,
    pub schema_id: SchemaId,
    pub version: u32,
    pub distance: f32,
    pub rank: u32,
}

/// Prompt/response budget bounds enforced by the caller (Task 2's
/// `HttpInferencer`), never by the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InferenceBudget {
    pub max_prompt_tokens: u32,
    pub timeout_ms: u64,
}

/// The full input to `SemanticInferencer::classify`: the redacted candidate
/// view + the retrieved top-k + the contract version being spoken + the
/// budget for this call.
///
/// `prompt` is the already-rendered prompt text sent to the model. Task 2
/// (`HttpInferencer`) only consumes this field verbatim — it does not build
/// prompts. The real PII-safe builder (monoid stats + redacted, length-capped,
/// injection-checked field NAMES only; never raw payload values) is Task 4
/// (`deblob-slm::prompt`). Until Task 4 lands, callers (including this
/// crate's own tests) may pass a placeholder string here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    pub contract_version: u32,
    pub budget: InferenceBudget,
    pub prompt: String,
}

/// Failure modes from a `SemanticInferencer` implementation. Every variant
/// maps to a shadow "unavailable" outcome at the caller — never a cold-lane
/// or relay failure (spec §3.3, §6). Reserved for a TOTAL failure: no
/// [`InferenceOutcome`] (and therefore no [`InferenceTelemetry`]) could be
/// produced at all. A response that arrived but failed contract validation
/// (even after the one mechanical repair attempt) is NOT this — it is an
/// `Ok(InferenceOutcome { decision: InferenceDecision::Abstain { .. }, .. })`
/// with `telemetry.parse_error`/`telemetry.schema_validation_error` set. See
/// [`SemanticInferencer::classify`].
#[derive(Debug, thiserror::Error)]
pub enum InferenceError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("timeout")]
    Timeout,
    #[error("parse error: {0}")]
    Parse(String),
}

/// Endpoint reachability outcome carried in [`InferenceTelemetry`].
///
/// `deblob::shadow::EndpointStatus` (the app-crate type stamped onto
/// `ShadowDecision`) has only `Available`/`Unavailable` — it cannot be
/// reused here directly (`deblob` depends on `deblob-slm`, not the
/// reverse). This type ALIGNS with that one: `Ok` maps to `Available`,
/// `Unavailable`/`Timeout` both map to `Unavailable`. `Timeout` exists as a
/// distinct variant so a telemetry consumer can tell a deadline-exceeded
/// failure apart from a generic transport failure without re-stringifying
/// an [`InferenceError`]; in the current [`crate::http::HttpInferencer`], a
/// telemetry-carrying `Ok(InferenceOutcome)` is only ever produced once the
/// endpoint has actually answered, so `endpoint_status` is always `Ok` in
/// practice today — `Unavailable`/`Timeout` are reserved for a future
/// implementation that can produce a degraded-but-usable outcome (e.g. a
/// safe default) instead of a hard [`InferenceError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointStatus {
    Ok,
    Unavailable,
    Timeout,
}

/// Telemetry for one [`SemanticInferencer::classify`] call — latency,
/// token counts, repair/error flags — surfaced alongside the
/// [`InferenceDecision`] so the shadow log (`deblob::shadow::ShadowDecision`)
/// and the eval harness (Tasks 6-8) can compute TTFT, prefill/decode,
/// token, cost, and repair-rate metrics. Every field is best-effort:
/// `None`/`0`/`false` means "not observed for this call", never a
/// fabricated value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceTelemetry {
    /// Prompt/input token count from the endpoint's `usage` object, if the
    /// response included one. `None` if the endpoint didn't report usage
    /// (not every OpenAI-compatible server does), or on a cache hit.
    pub request_tokens: Option<u32>,
    /// Completion/output token count from the endpoint's `usage` object,
    /// under the same caveats as `request_tokens`.
    pub response_tokens: Option<u32>,
    /// Time-to-first-token. The current port is request/response, not
    /// streaming, so there is no way to observe a true TTFT distinct from
    /// the full response latency; implementations MAY set this equal to
    /// `total_latency_ms` as a documented conservative proxy rather than
    /// leave it `None` when a real value isn't obtainable without
    /// streaming support. `None` on a cache hit (no call was made).
    pub ttft_ms: Option<u64>,
    /// Wall-clock latency of this `classify()` call (all HTTP attempts,
    /// including the one mechanical repair if it ran). `None` on a cache
    /// hit.
    pub total_latency_ms: Option<u64>,
    /// `1` if the one mechanical repair (Task 2's single retry-on-syntax-
    /// error) ran, `0` otherwise (including the `IdNotAllowed` immediate-
    /// abstain path, which never retries, and a cache hit).
    pub repair_count: u32,
    /// Whether the endpoint was reached for this call. See
    /// [`EndpointStatus`]'s docs for what each variant means in practice.
    pub endpoint_status: EndpointStatus,
    /// `true` iff the FINAL returned decision is a safe-abstain fallback
    /// caused by a parse-class contract failure ([`ContractError::Malformed`]
    /// or [`ContractError::UnknownField`]) that a repair attempt could not
    /// resolve. `false` when the call produced a validly-parsed decision,
    /// whether on the first attempt or after a successful repair — a
    /// transient parse failure that repair fixed is NOT a `parse_error`
    /// for telemetry purposes (see `repair_count` for that signal).
    pub parse_error: bool,
    /// `true` iff the FINAL returned decision is a safe-abstain fallback
    /// caused by [`ContractError::IdNotAllowed`] (a `schema_id` outside the
    /// retrieved top-k allow-list) that could not be resolved. Same
    /// "final outcome only" semantics as `parse_error`.
    pub schema_validation_error: bool,
    /// The configured model id this call targeted, echoed from
    /// [`crate::http::SlmHttpConfig::model`] (or the equivalent config of a
    /// future `SemanticInferencer` implementation).
    pub model_id: Option<String>,
}

/// The full result of one [`SemanticInferencer::classify`] call: the 3-way
/// decision plus the [`InferenceTelemetry`] observed while producing it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InferenceOutcome {
    pub decision: InferenceDecision,
    pub telemetry: InferenceTelemetry,
}

/// Vendor-free port for a small-language-model classifier. `deblob-core`
/// does not define this trait (checked against `ports.rs` as merged to
/// `main` from P1); it lives here since only `deblob-slm`'s implementations
/// (`HttpInferencer`, Task 2; later `LocalInferencer`) and its callers need
/// it, and putting it in `deblob-core` would give the core crate an
/// `async-trait` shaped opinion about a lane it must stay agnostic to.
///
/// Returns `Err(InferenceError)` ONLY for a total transport/timeout failure
/// with no usable outcome at all (see [`InferenceError`]'s docs). A
/// contract-invalid-but-recovered (or unrecoverable-but-safely-abstained)
/// response is `Ok(InferenceOutcome)` — see [`InferenceTelemetry`]'s
/// `parse_error`/`schema_validation_error` fields.
#[async_trait]
pub trait SemanticInferencer: Send + Sync {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    #[test]
    fn match_schema_roundtrips_discriminated() {
        let id = schema_id(7);

        let match_decision = InferenceDecision::MatchSchema {
            schema_id: id.clone(),
            relation: Relation::Exact,
        };
        let json = serde_json::to_value(&match_decision).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "decision": "match_schema",
                "schema_id": id.as_str(),
                "relation": "exact",
            })
        );
        assert_eq!(
            serde_json::from_value::<InferenceDecision>(json).unwrap(),
            match_decision
        );

        let new_candidate = InferenceDecision::NewCandidate {
            novelty: Novelty::Structural,
        };
        let json = serde_json::to_value(&new_candidate).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"decision": "new_candidate", "novelty": "structural"})
        );
        assert_eq!(
            serde_json::from_value::<InferenceDecision>(json).unwrap(),
            new_candidate
        );

        let abstain = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };
        let json = serde_json::to_value(&abstain).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"decision": "abstain", "cause": "insufficient_evidence"})
        );
        assert_eq!(
            serde_json::from_value::<InferenceDecision>(json).unwrap(),
            abstain
        );
    }

    #[test]
    fn incompatible_similarity_is_not_accepted_match() {
        let id = schema_id(1);

        let incompatible = InferenceDecision::MatchSchema {
            schema_id: id.clone(),
            relation: Relation::IncompatibleSimilarity,
        };
        assert!(!incompatible.is_accepted_match());

        let exact = InferenceDecision::MatchSchema {
            schema_id: id.clone(),
            relation: Relation::Exact,
        };
        assert!(exact.is_accepted_match());

        let drift = InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::CompatibleDrift,
        };
        assert!(drift.is_accepted_match());

        assert!(!InferenceDecision::NewCandidate {
            novelty: Novelty::Semantic
        }
        .is_accepted_match());
        assert!(!InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous
        }
        .is_accepted_match());
    }

    #[test]
    fn validate_rejects_id_outside_topk() {
        let allowed = schema_id(2);
        let outside = schema_id(3);
        let raw = format!(
            r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
            outside.as_str()
        );

        let err = validate_decision(&raw, std::slice::from_ref(&allowed)).unwrap_err();
        assert!(matches!(err, ContractError::IdNotAllowed));
    }

    #[test]
    fn validate_rejects_unknown_field() {
        let id = schema_id(4);
        let raw = format!(
            r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact","confidence":0.9}}"#,
            id.as_str()
        );

        let err = validate_decision(&raw, std::slice::from_ref(&id)).unwrap_err();
        assert!(matches!(err, ContractError::UnknownField));
    }

    #[test]
    fn enums_serialize_snake_case() {
        assert_eq!(
            serde_json::to_string(&Relation::Exact).unwrap(),
            "\"exact\""
        );
        assert_eq!(
            serde_json::to_string(&Relation::CompatibleDrift).unwrap(),
            "\"compatible_drift\""
        );
        assert_eq!(
            serde_json::to_string(&Relation::IncompatibleSimilarity).unwrap(),
            "\"incompatible_similarity\""
        );
        assert_eq!(
            serde_json::from_str::<Relation>("\"compatible_drift\"").unwrap(),
            Relation::CompatibleDrift
        );

        assert_eq!(
            serde_json::to_string(&Novelty::Structural).unwrap(),
            "\"structural\""
        );
        assert_eq!(
            serde_json::to_string(&Novelty::Semantic).unwrap(),
            "\"semantic\""
        );
        assert_eq!(
            serde_json::from_str::<Novelty>("\"semantic\"").unwrap(),
            Novelty::Semantic
        );

        assert_eq!(
            serde_json::to_string(&AbstainCause::Ambiguous).unwrap(),
            "\"ambiguous\""
        );
        assert_eq!(
            serde_json::to_string(&AbstainCause::InsufficientEvidence).unwrap(),
            "\"insufficient_evidence\""
        );
        assert_eq!(
            serde_json::to_string(&AbstainCause::CandidateMissing).unwrap(),
            "\"candidate_missing\""
        );
        assert_eq!(
            serde_json::from_str::<AbstainCause>("\"candidate_missing\"").unwrap(),
            AbstainCause::CandidateMissing
        );
    }
}
