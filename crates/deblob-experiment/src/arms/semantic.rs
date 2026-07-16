//! [`SemanticArm`] — the trait-shaped seam over `deblob_slm
//! ::SemanticInferencer` that B0/B1 wrap (spec §8: "Leave B0/B1 as a
//! trait-shaped seam that takes a `SemanticInferencer` (port already in
//! deblob-slm)"). B0 = a bare `SemanticArm` (no gate). B1 =
//! `crate::arms::gate::GatedArm::new(ArmId::B1, Box::new(SemanticArm::new(..)))`.
//!
//! `SemanticInferencer::classify` is `async`; [`Arm::decide`] is
//! deliberately sync (spec §8's literal trait shape). This task's mock
//! inferencer never performs real I/O, so bridging with
//! `futures_executor::block_on` (a single poll, no runtime required) is
//! sufficient — a real network-backed adapter (Task 3) can still implement
//! `SemanticInferencer` and plug in here unchanged; only the bridging cost
//! changes from "free" to "one blocking wait per call".

use std::sync::Arc;

use deblob_slm::{AbstainCause, InferenceBudget, InferenceRequest, SemanticInferencer};

use crate::labels::InferenceInput;

use super::{Arm, ArmDecision, ArmId};

/// Contract version + budget this harness stamps on every `InferenceRequest`
/// — mirrors `deblob_eval::metrics::{CONTRACT_VERSION, DEFAULT_BUDGET}`
/// (kept as local constants rather than importing those private-to-the-
/// crate items, since `deblob-eval` does not export them).
pub const CONTRACT_VERSION: u32 = 1;
pub const DEFAULT_BUDGET: InferenceBudget = InferenceBudget {
    max_prompt_tokens: 4096,
    timeout_ms: 30_000,
};

/// Wraps a `SemanticInferencer` as an [`Arm`]. A total transport/timeout/
/// parse failure (`InferenceError`, see that type's docs: reserved for "no
/// usable `InferenceOutcome` at all") becomes `Abstain
/// {cause: InsufficientEvidence}` here — a safe, conservative fallback,
/// consistent with the product's own "endpoint unavailable = shadow
/// unavailable outcome, never a hard crash" convention.
pub struct SemanticArm {
    id: ArmId,
    inferencer: Arc<dyn SemanticInferencer>,
}

impl SemanticArm {
    pub fn new(id: ArmId, inferencer: Arc<dyn SemanticInferencer>) -> Self {
        Self { id, inferencer }
    }
}

impl Arm for SemanticArm {
    fn id(&self) -> ArmId {
        self.id
    }

    fn decide(&self, input: &InferenceInput) -> ArmDecision {
        let request = InferenceRequest {
            candidate: input.candidate.clone(),
            retrieved: input.retrieved.clone(),
            contract_version: CONTRACT_VERSION,
            budget: DEFAULT_BUDGET,
            prompt: input.prompt.clone(),
        };
        match futures_executor::block_on(self.inferencer.classify(request)) {
            Ok(outcome) => outcome.decision,
            Err(_) => deblob_slm::InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arms::mock::ScriptedInferencer;
    use deblob_core::id::{FamilyId, SchemaId};
    use deblob_slm::{CandidateProfileView, FamilyCandidate, InferenceDecision, Relation};

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn fc(byte: u8, rank: u32, distance: f32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(byte),
            version: 1,
            distance,
            rank,
        }
    }

    fn input_with(retrieved: Vec<FamilyCandidate>) -> InferenceInput {
        InferenceInput {
            candidate: CandidateProfileView {
                observation_count: 100,
                fields: vec![],
                truncated: false,
            },
            allowed_ids: retrieved.iter().map(|c| c.schema_id.clone()).collect(),
            retrieved,
            prompt: String::new(),
        }
    }

    #[test]
    fn semantic_arm_returns_the_mocks_scripted_decision() {
        let mock = ScriptedInferencer::new(vec![InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        }]);
        let arm = SemanticArm::new(ArmId::B0, Arc::new(mock));
        let input = input_with(vec![fc(1, 1, 0.0)]);
        assert_eq!(
            arm.decide(&input),
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn semantic_arm_falls_back_to_abstain_on_total_failure() {
        struct AlwaysFails;
        #[async_trait::async_trait]
        impl SemanticInferencer for AlwaysFails {
            async fn classify(
                &self,
                _req: InferenceRequest,
            ) -> Result<deblob_slm::InferenceOutcome, deblob_slm::InferenceError> {
                Err(deblob_slm::InferenceError::Timeout)
            }
        }
        let arm = SemanticArm::new(ArmId::B0, Arc::new(AlwaysFails));
        let input = input_with(vec![fc(1, 1, 0.0)]);
        assert_eq!(
            arm.decide(&input),
            InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence
            }
        );
    }
}
