//! `deblob-slm`: the SLM discovery lane.
//!
//! P2-A/B Task 1 (foundation): the `deblob-slm` crate scaffold + the 3-way
//! inference contract types (`InferenceDecision::{MatchSchema,NewCandidate,Abstain}`)
//! and the `SemanticInferencer` port. See
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 1 — contract"
//! (authoritative over `docs/superpowers/plans/2026-07-14-deblob-p2ab.md` § Task 1).
//!
//! `deblob-core::ports` does NOT define `SemanticInferencer` (checked against
//! P1 as merged to `main`) — it is defined here instead, scoped to this crate,
//! since only `deblob-slm` implementations (`HttpInferencer`, later
//! `LocalInferencer`) and callers of this port need it.

pub mod cache;
pub mod contract;
pub mod http;
pub mod prompt;

pub use contract::{
    validate_decision, AbstainCause, ContractError, EndpointStatus, FamilyCandidate,
    InferenceBudget, InferenceDecision, InferenceError, InferenceOutcome, InferenceRequest,
    InferenceTelemetry, Novelty, Relation, SemanticInferencer,
};
pub use http::{HttpInferencer, SlmHttpConfig};
pub use prompt::{
    build_prompt, detect_injection, redact_field_name, CandidateProfileView, NumericBucket, Prompt,
    RedactedFieldStat, RedactedName, MAX_FIELDS, MAX_NAME_LEN, MAX_PATH_DEPTH,
};
