//! A0 (retrieval-only) and A1 (the fair deterministic policy baseline) —
//! spec §1's table rows.

use deblob::shadow::{POLICY_MAX_DISTANCE, POLICY_MIN_MARGIN, POLICY_MIN_OBSERVATIONS};
use deblob_slm::{AbstainCause, FamilyCandidate, InferenceDecision, Novelty, Relation};

use crate::labels::InferenceInput;

use super::{Arm, ArmDecision, ArmId};

/// The rank-1 (closest, per Task 3 structural distance) retrieved
/// candidate, or `None` if `retrieved` is empty. Ties on `rank` are
/// resolved by `schema_id` for determinism (mirrors
/// `deblob_slm::build_prompt`'s own canonical retrieved ordering).
pub fn top1(retrieved: &[FamilyCandidate]) -> Option<&FamilyCandidate> {
    retrieved.iter().min_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| a.schema_id.as_str().cmp(b.schema_id.as_str()))
    })
}

/// `distance(rank 2) - distance(rank 1)`, computed independently of what
/// any arm selects — mirrors `deblob::shadow::PolicyGateInputs
/// ::top1_top2_margin`'s docs exactly. `f32::INFINITY` when fewer than two
/// candidates were retrieved (no ambiguity is possible with a single or no
/// candidate — the convention this crate's gate wiring relies on so a
/// single-candidate case is never rejected on margin alone).
pub fn margin_of(retrieved: &[FamilyCandidate]) -> f32 {
    let mut sorted: Vec<&FamilyCandidate> = retrieved.iter().collect();
    sorted.sort_by(|a, b| {
        a.rank
            .cmp(&b.rank)
            .then_with(|| a.schema_id.as_str().cmp(b.schema_id.as_str()))
    });
    match (sorted.first(), sorted.get(1)) {
        (Some(r1), Some(r2)) => r2.distance - r1.distance,
        _ => f32::INFINITY,
    }
}

/// **A0** — retrieval-capability floor. No gate, no thresholds: always
/// proposes the rank-1 candidate as an exact match, or abstains
/// (`CandidateMissing`) when nothing was retrieved at all. Deliberately
/// naive — A0 exists to measure retrieval geometry (Layer 1), not to be a
/// competitive decider.
#[derive(Debug, Default, Clone, Copy)]
pub struct A0RetrievalOnly;

impl Arm for A0RetrievalOnly {
    fn id(&self) -> ArmId {
        ArmId::A0
    }

    fn decide(&self, input: &InferenceInput) -> ArmDecision {
        match top1(&input.retrieved) {
            Some(candidate) => InferenceDecision::MatchSchema {
                schema_id: candidate.schema_id.clone(),
                relation: Relation::Exact,
            },
            None => InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing,
            },
        }
    }
}

/// **A1** — the fair deterministic policy: the STRONG baseline the SLM
/// must beat (spec §1). Tuned thresholds, calibrated abstain, and — the
/// fairness property that makes it a strong (not straw-man) baseline —
/// the SAME hard trust constraints B1's gate enforces
/// (`deblob::shadow::POLICY_MAX_DISTANCE`/`POLICY_MIN_MARGIN`
/// /`POLICY_MIN_OBSERVATIONS`), applied directly rather than through
/// `evaluate_policy` (A1 has no model `relation` to feed that function —
/// it decides `relation` itself, deterministically, from distance alone).
#[derive(Debug, Clone, Copy)]
pub struct A1FairDeterministic {
    /// Distance at or below which a rank-1 match is labeled `Exact` rather
    /// than `CompatibleDrift`. Tunable (spec: "tuned thresholds on dev
    /// data") — defaults to a tight epsilon around zero distance.
    pub exact_distance_epsilon: f32,
}

impl Default for A1FairDeterministic {
    fn default() -> Self {
        Self {
            exact_distance_epsilon: 1e-6,
        }
    }
}

impl Arm for A1FairDeterministic {
    fn id(&self) -> ArmId {
        ArmId::A1
    }

    fn decide(&self, input: &InferenceInput) -> ArmDecision {
        let Some(top) = top1(&input.retrieved) else {
            return InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing,
            };
        };

        if input.candidate.observation_count < POLICY_MIN_OBSERVATIONS {
            return InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            };
        }

        if top.distance > POLICY_MAX_DISTANCE {
            // Too far from anything known to be the same family at all —
            // propose a new family rather than force a merge.
            return InferenceDecision::NewCandidate {
                novelty: Novelty::Structural,
            };
        }

        if margin_of(&input.retrieved) < POLICY_MIN_MARGIN {
            // Close enough to be plausible, but not decisively closer than
            // the runner-up — calibrated abstain rather than a guess.
            return InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            };
        }

        let relation = if top.distance <= self.exact_distance_epsilon {
            Relation::Exact
        } else {
            Relation::CompatibleDrift
        };
        InferenceDecision::MatchSchema {
            schema_id: top.schema_id.clone(),
            relation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{FamilyId, SchemaId};
    use deblob_slm::CandidateProfileView;

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
    fn top1_picks_lowest_rank() {
        let retrieved = vec![fc(2, 2, 0.1), fc(1, 1, 0.05)];
        assert_eq!(top1(&retrieved).unwrap().schema_id, schema_id(1));
    }

    #[test]
    fn margin_of_computes_rank1_rank2_distance_gap() {
        let retrieved = vec![fc(1, 1, 0.05), fc(2, 2, 0.30)];
        let margin = margin_of(&retrieved);
        assert!((margin - 0.25).abs() < 1e-6, "margin={margin}");
    }

    #[test]
    fn margin_of_is_infinite_with_fewer_than_two_candidates() {
        assert_eq!(margin_of(&[fc(1, 1, 0.05)]), f32::INFINITY);
        assert_eq!(margin_of(&[]), f32::INFINITY);
    }

    #[test]
    fn a0_always_proposes_top1_as_exact_match() {
        let input = input_with(5, vec![fc(2, 2, 0.9), fc(1, 1, 0.5)]);
        let decision = A0RetrievalOnly.decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn a0_abstains_when_nothing_retrieved() {
        let input = input_with(5, vec![]);
        let decision = A0RetrievalOnly.decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing
            }
        );
    }

    #[test]
    fn a1_abstains_below_observation_floor() {
        let input = input_with(POLICY_MIN_OBSERVATIONS - 1, vec![fc(1, 1, 0.0)]);
        let decision = A1FairDeterministic::default().decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence
            }
        );
    }

    #[test]
    fn a1_proposes_new_candidate_when_distance_exceeds_threshold() {
        let input = input_with(
            1_000,
            vec![fc(1, 1, POLICY_MAX_DISTANCE + 0.01), fc(2, 2, 0.9)],
        );
        let decision = A1FairDeterministic::default().decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::NewCandidate {
                novelty: Novelty::Structural
            }
        );
    }

    #[test]
    fn a1_abstains_ambiguous_when_margin_too_small() {
        let d = POLICY_MAX_DISTANCE / 2.0;
        let input = input_with(
            1_000,
            vec![fc(1, 1, d), fc(2, 2, d + POLICY_MIN_MARGIN / 2.0)],
        );
        let decision = A1FairDeterministic::default().decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous
            }
        );
    }

    #[test]
    fn a1_matches_exact_at_near_zero_distance() {
        let input = input_with(1_000, vec![fc(1, 1, 0.0), fc(2, 2, 0.9)]);
        let decision = A1FairDeterministic::default().decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn a1_matches_compatible_drift_away_from_zero_but_within_threshold() {
        let d = POLICY_MAX_DISTANCE / 2.0;
        let input = input_with(1_000, vec![fc(1, 1, d), fc(2, 2, 0.99)]);
        let decision = A1FairDeterministic::default().decide(&input);
        assert_eq!(
            decision,
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::CompatibleDrift,
            }
        );
    }
}
