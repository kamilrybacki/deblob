//! The FROZEN trust-gate wrapper (spec §1: "the trust gate is FROZEN
//! across the entire learning comparison" / "the gate is FROZEN/identical
//! across arms that use it — only the decider feeding it changes — that's
//! what makes B2 a fair redundancy test").
//!
//! [`GatedArm`] wraps any inner [`Arm`] (the "decider") and runs its
//! proposal through `deblob::shadow::evaluate_policy` — the REAL product
//! gate, called verbatim, never reimplemented. B1 (semantic decider) and
//! B2 (deterministic top-1 decider) are both just a [`GatedArm`] around a
//! different inner `Arm`, constructed with the identical [`GateAblation`]
//! (normally [`GateAblation::none`]) — the ONLY thing that differs between
//! them is `inner`.

use deblob::shadow::{evaluate_policy, PolicyGateInputs, PolicyOutcome};
use deblob_slm::{AbstainCause, FamilyCandidate, InferenceDecision};

use crate::labels::InferenceInput;

use super::deterministic::margin_of;
use super::{Arm, ArmDecision, ArmId};

/// Per-predicate gate ablation toggles (spec §1: "no-rank / no-distance /
/// no-margin / no-obs-floor / no-corroboration / no-SLM"), analysis-only —
/// never used to decide a real arm's deployed output, only to attribute
/// how much of the gate's safety comes from each individual guard.
/// "no-SLM" is not a field here: it is realized by choosing [`ArmId::B2`]'s
/// deterministic inner decider instead of B1's semantic one, not by an
/// ablation flag on the gate itself.
///
/// Each `disable_*` flag NEUTRALIZES (forces to its most permissive value)
/// the corresponding [`PolicyGateInputs`] field before calling
/// `evaluate_policy` — the gate LOGIC in `deblob::shadow::evaluate_policy`
/// is never touched; only the inputs fed to it are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GateAblation {
    pub disable_rank: bool,
    pub disable_distance: bool,
    pub disable_margin: bool,
    pub disable_obs_floor: bool,
    /// Neutralizes `deterministic_compat_passed`. Currently a documented
    /// no-op in this synthetic-corpus harness: see
    /// [`gate_inputs_for`]'s docs for why that field is a fixed placeholder
    /// here regardless of this flag.
    pub disable_corroboration: bool,
}

impl GateAblation {
    pub fn none() -> Self {
        Self::default()
    }
}

/// Builds the deterministic [`PolicyGateInputs`] for `decision` over
/// `input`, applying `ablation`'s neutralizations. Shared by every
/// [`GatedArm`] so B1 and B2 (and every ablation variant) compute these
/// inputs via the exact same code path — the only thing that ever differs
/// between calls is `decision` (which arm proposed it) and `ablation`.
///
/// `deterministic_compat_passed` is a fixed `true` placeholder: the real
/// structural-compatibility check (`ShadowDecision
/// ::deterministic_compatibility_result`) lives deeper in the product's
/// `deblob_semantic` canonicalization path and needs signal this
/// synthetic-corpus harness does not model (Task 1 is scoped to the
/// synthetic corpus; a later task wiring real corpora can compute a real
/// value here). This is a documented limitation, not a fabricated
/// always-pass fact — see the module docs.
pub fn gate_inputs_for(
    input: &InferenceInput,
    decision: &ArmDecision,
    ablation: &GateAblation,
) -> PolicyGateInputs {
    let is_match_schema = matches!(decision, InferenceDecision::MatchSchema { .. });
    let (raw_rank, raw_distance) = match decision {
        InferenceDecision::MatchSchema { schema_id, .. } => {
            selected_rank_distance(&input.retrieved, schema_id)
        }
        _ => (None, None),
    };
    let relation = match decision {
        InferenceDecision::MatchSchema { relation, .. } => Some(*relation),
        _ => None,
    };

    PolicyGateInputs {
        is_match_schema,
        selected_rank: if ablation.disable_rank {
            Some(1)
        } else {
            raw_rank
        },
        selected_distance: if ablation.disable_distance {
            Some(0.0)
        } else {
            raw_distance
        },
        top1_top2_margin: if ablation.disable_margin {
            f32::INFINITY
        } else {
            margin_of(&input.retrieved)
        },
        observation_count: if ablation.disable_obs_floor {
            u64::MAX
        } else {
            input.candidate.observation_count
        },
        relation,
        deterministic_compat_passed: true,
        redaction_collision: deblob::shadow::detect_redaction_collision(&input.candidate.fields),
    }
}

pub(crate) fn selected_rank_distance(
    retrieved: &[FamilyCandidate],
    schema_id: &deblob_core::id::SchemaId,
) -> (Option<u32>, Option<f32>) {
    retrieved
        .iter()
        .find(|c| &c.schema_id == schema_id)
        .map(|c| (Some(c.rank), Some(c.distance)))
        .unwrap_or((None, None))
}

/// A gate-blocked `MatchSchema` proposal becomes this abstain cause,
/// regardless of WHICH gate predicate(s) failed — the full failure profile
/// is never lost, it travels alongside as [`GatedArm::decide_with_gate`]'s
/// [`PolicyOutcome::gate_reasons`]; this is just the bounded-enum stand-in
/// `ArmDecision`'s 3-way shape requires.
const GATE_BLOCKED_CAUSE: AbstainCause = AbstainCause::InsufficientEvidence;

/// Wraps `inner` (the decider) with the FROZEN trust gate. See the module
/// docs.
pub struct GatedArm {
    id: ArmId,
    inner: Box<dyn Arm>,
    ablation: GateAblation,
}

impl GatedArm {
    pub fn new(id: ArmId, inner: Box<dyn Arm>) -> Self {
        Self {
            id,
            inner,
            ablation: GateAblation::none(),
        }
    }

    pub fn with_ablation(id: ArmId, inner: Box<dyn Arm>, ablation: GateAblation) -> Self {
        Self {
            id,
            inner,
            ablation,
        }
    }

    /// Runs `inner`'s proposal through the gate and returns BOTH the final
    /// 3-way decision and the [`PolicyOutcome`] that produced it — the
    /// richer pair Layer 3 (gate-containment) needs (guard-activation
    /// reasons), which the plain [`Arm::decide`] trait method (by design,
    /// spec §8) does not carry.
    ///
    /// A proposal that was already `NewCandidate`/`Abstain` (never eligible
    /// for the trust gate at all — mirrors `deblob::trusted::trusted_verdict`'s
    /// Rule 1) passes through UNCHANGED; `evaluate_policy` is still called
    /// for bookkeeping symmetry (an honest `is_match_schema: false` input),
    /// but its `would_accept: false` never overrides `proposal` in that
    /// case. Only a gate-REJECTED `MatchSchema` proposal is downgraded to
    /// `Abstain { cause: GATE_BLOCKED_CAUSE }`.
    pub fn decide_with_gate(&self, input: &InferenceInput) -> (ArmDecision, PolicyOutcome) {
        let proposal = self.inner.decide(input);
        let gate_inputs = gate_inputs_for(input, &proposal, &self.ablation);
        let outcome = evaluate_policy(&gate_inputs);

        if !matches!(proposal, InferenceDecision::MatchSchema { .. }) {
            return (proposal, outcome);
        }

        let final_decision = if outcome.would_accept {
            proposal
        } else {
            InferenceDecision::Abstain {
                cause: GATE_BLOCKED_CAUSE,
            }
        };
        (final_decision, outcome)
    }
}

impl Arm for GatedArm {
    fn id(&self) -> ArmId {
        self.id
    }

    fn decide(&self, input: &InferenceInput) -> ArmDecision {
        self.decide_with_gate(input).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arms::deterministic::A0RetrievalOnly;
    use deblob_core::id::{FamilyId, SchemaId};
    use deblob_slm::{CandidateProfileView, Relation};

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

    fn input_with(obs: u64, retrieved: Vec<FamilyCandidate>) -> InferenceInput {
        InferenceInput {
            candidate: CandidateProfileView {
                observation_count: obs,
                fields: vec![],
                truncated: false,
            },
            allowed_ids: retrieved.iter().map(|c| c.schema_id.clone()).collect(),
            retrieved,
            prompt: String::new(),
        }
    }

    #[test]
    fn a_strong_top1_proposal_is_accepted_by_the_gate() {
        let gated = GatedArm::new(ArmId::B2, Box::new(A0RetrievalOnly));
        let input = input_with(1_000, vec![fc(1, 1, 0.0), fc(2, 2, 0.9)]);
        let (decision, outcome) = gated.decide_with_gate(&input);
        assert!(
            outcome.would_accept,
            "gate_reasons={:?}",
            outcome.gate_reasons
        );
        assert_eq!(
            decision,
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn insufficient_observations_blocks_the_gate_and_downgrades_to_abstain() {
        let gated = GatedArm::new(ArmId::B2, Box::new(A0RetrievalOnly));
        let input = input_with(1, vec![fc(1, 1, 0.0), fc(2, 2, 0.9)]);
        let (decision, outcome) = gated.decide_with_gate(&input);
        assert!(!outcome.would_accept);
        assert!(outcome
            .gate_reasons
            .contains(&deblob::shadow::GateReason::InsufficientObservations));
        assert_eq!(
            decision,
            InferenceDecision::Abstain {
                cause: GATE_BLOCKED_CAUSE
            }
        );
    }

    #[test]
    fn no_obs_floor_ablation_neutralizes_the_observation_gate() {
        let gated = GatedArm::with_ablation(
            ArmId::B2,
            Box::new(A0RetrievalOnly),
            GateAblation {
                disable_obs_floor: true,
                ..GateAblation::none()
            },
        );
        let input = input_with(1, vec![fc(1, 1, 0.0), fc(2, 2, 0.9)]);
        let (decision, outcome) = gated.decide_with_gate(&input);
        assert!(
            outcome.would_accept,
            "gate_reasons={:?}",
            outcome.gate_reasons
        );
        assert!(decision.is_accepted_match());
    }

    #[test]
    fn a_non_match_proposal_passes_through_the_gate_unchanged() {
        struct AlwaysNewCandidate;
        impl Arm for AlwaysNewCandidate {
            fn id(&self) -> ArmId {
                ArmId::B2
            }
            fn decide(&self, _input: &InferenceInput) -> ArmDecision {
                InferenceDecision::NewCandidate {
                    novelty: deblob_slm::Novelty::Structural,
                }
            }
        }
        let gated = GatedArm::new(ArmId::B2, Box::new(AlwaysNewCandidate));
        let input = input_with(1_000, vec![fc(1, 1, 0.0)]);
        let (decision, outcome) = gated.decide_with_gate(&input);
        assert_eq!(
            decision,
            InferenceDecision::NewCandidate {
                novelty: deblob_slm::Novelty::Structural
            }
        );
        assert!(!outcome.would_accept);
        assert!(outcome
            .gate_reasons
            .contains(&deblob::shadow::GateReason::NoMatchProposed));
    }
}
