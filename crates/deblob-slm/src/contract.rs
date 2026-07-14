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

/// Redacted, monoid-statistics view of the candidate cluster. A placeholder
/// container until Task 4 (PII-safe prompt builder) defines the concrete
/// redacted shape; carrying an opaque `serde_json::Value` for now keeps this
/// crate compiling for later tasks without pre-committing to that shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateProfileView {
    pub stats: serde_json::Value,
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    pub contract_version: u32,
    pub budget: InferenceBudget,
}

/// Failure modes from a `SemanticInferencer` implementation. Every variant
/// maps to a shadow "unavailable" outcome at the caller — never a cold-lane
/// or relay failure (spec §3.3, §6).
#[derive(Debug, thiserror::Error)]
pub enum InferenceError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("timeout")]
    Timeout,
    #[error("parse error: {0}")]
    Parse(String),
}

/// Vendor-free port for a small-language-model classifier. `deblob-core`
/// does not define this trait (checked against `ports.rs` as merged to
/// `main` from P1); it lives here since only `deblob-slm`'s implementations
/// (`HttpInferencer`, Task 2; later `LocalInferencer`) and its callers need
/// it, and putting it in `deblob-core` would give the core crate an
/// `async-trait` shaped opinion about a lane it must stay agnostic to.
#[async_trait]
pub trait SemanticInferencer: Send + Sync {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceDecision, InferenceError>;
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
