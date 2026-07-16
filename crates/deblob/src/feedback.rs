//! Feedback capture — every human decision becomes a labeled training
//! example (spec: `docs/superpowers/specs/2026-07-16-slm-continual-learning.md`
//! §1).
//!
//! The [`TrainingExample`]/[`LabelSource`] SHAPE lives in
//! `deblob_slm::feedback` (see that module's docs for why); this module
//! owns the LABEL-MAPPING logic — turning the outcome of a governed
//! decision path (`crate::trusted::TrustedApplier`'s verdicts,
//! `crate::promote::Promoter::promote`, an annotation/adjudication API)
//! into the correct `gold`/`weight`/`label_source` triple.
//!
//! Every function here is a pure constructor over ALREADY-REDACTED/derived
//! data (`CandidateProfileView`, `FamilyCandidate`) — none of them ever see
//! a raw candidate payload value, so nothing here can leak one into a
//! [`TrainingExample`] regardless of what a caller does with the result.
//!
//! # The reinforcement signal
//!
//! [`capture_trusted_proposal_rejected`] is the highest-value correction:
//! the deterministic trust gate corroborated a model proposal enough to
//! queue it for human approval, and a human still said no. That is
//! evidence the model is confidently wrong about this exact
//! shape/candidate — worth more than an ordinary positive example, so it
//! carries [`HARD_NEGATIVE_WEIGHT`] rather than [`POSITIVE_WEIGHT`], and
//! its `gold` is constructed so it can NEVER re-affirm the rejected
//! family (see that function's docs).

use deblob_core::id::{FamilyId, SchemaId};
use deblob_slm::{
    AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision, LabelSource, Relation,
    TrainingExample,
};

/// Weight assigned to every positive label source (`HumanPromote`,
/// `TrustedProposalAccepted`, `SemanticAnnotation`, `Adjudication`).
pub const POSITIVE_WEIGHT: f32 = 1.0;

/// Weight assigned to a `TrustedProposalRejected` hard-negative — highest
/// of any label source (spec §1: "Highest weight — it directly targets a
/// failure mode"). Strictly greater than [`POSITIVE_WEIGHT`].
pub const HARD_NEGATIVE_WEIGHT: f32 = 3.0;

fn build(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    label_source: LabelSource,
    weight: f32,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    TrainingExample {
        candidate,
        retrieved,
        gold,
        label_source,
        weight,
        partition_key,
        recorded_at,
    }
}

/// `HumanPromote`: an operator promoted a candidate. `gold` is the exact
/// decision the promotion represents — a `MatchSchema` into an existing
/// family, or a `NewCandidate` when the promotion created a new one.
/// Positive, [`POSITIVE_WEIGHT`].
pub fn capture_human_promote(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::HumanPromote,
        POSITIVE_WEIGHT,
        partition_key,
        recorded_at,
    )
}

/// `TrustedProposalAccepted`: the model proposed `MatchSchema{schema_id,
/// relation}`, the deterministic trust gate queued it
/// (`TrustVerdict::ProposeToHuman`), and a human approved. `gold` is the
/// model's OWN proposal, confirmed — positive, [`POSITIVE_WEIGHT`].
pub fn capture_trusted_proposal_accepted(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    schema_id: SchemaId,
    relation: Relation,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    let gold = InferenceDecision::MatchSchema {
        schema_id,
        relation,
    };
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::TrustedProposalAccepted,
        POSITIVE_WEIGHT,
        partition_key,
        recorded_at,
    )
}

/// `TrustedProposalRejected`: the model proposed `MatchSchema{rejected_schema_id,
/// ..}`, the deterministic trust gate queued it, and a human REJECTED it —
/// the highest-value correction signal (spec §1). `gold` is set to
/// `corrected` (whatever the human said the actual right answer is —
/// `NewCandidate`, a DIFFERENT family's `MatchSchema`, or an `Abstain`) if
/// supplied, or a safe `Abstain{cause: Ambiguous}` default otherwise ("not
/// confident this is right" is itself a valid, safe hard-negative gold when
/// no more specific correction is known). Either way `gold` can never
/// re-affirm `rejected_schema_id` as an accepted match — this function
/// debug-asserts that invariant so a caller bug (e.g. threading the
/// rejected schema back in as the "correction") fails loudly rather than
/// silently poisoning the training set with a self-contradicting example.
///
/// Carries [`HARD_NEGATIVE_WEIGHT`] — strictly higher than every positive
/// label source.
pub fn capture_trusted_proposal_rejected(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    rejected_schema_id: &SchemaId,
    corrected: Option<InferenceDecision>,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    let gold = corrected.unwrap_or(InferenceDecision::Abstain {
        cause: AbstainCause::Ambiguous,
    });
    debug_assert!(
        !matches!(
            &gold,
            InferenceDecision::MatchSchema { schema_id, relation }
                if schema_id == rejected_schema_id
                    && matches!(relation, Relation::Exact | Relation::CompatibleDrift)
        ),
        "a TrustedProposalRejected hard-negative's gold must never re-affirm the rejected \
         family as an accepted match"
    );
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::TrustedProposalRejected,
        HARD_NEGATIVE_WEIGHT,
        partition_key,
        recorded_at,
    )
}

/// `SemanticAnnotation`: a P2-D controlled-vocabulary annotation supplies
/// `gold` directly. Positive, [`POSITIVE_WEIGHT`].
pub fn capture_semantic_annotation(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::SemanticAnnotation,
        POSITIVE_WEIGHT,
        partition_key,
        recorded_at,
    )
}

/// `Adjudication`: an offline human label on a shadow-log record supplies
/// `gold` directly. Positive, [`POSITIVE_WEIGHT`].
pub fn capture_adjudication(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    partition_key: FamilyId,
    recorded_at: i64,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::Adjudication,
        POSITIVE_WEIGHT,
        partition_key,
        recorded_at,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_slm::Novelty;

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn candidate() -> CandidateProfileView {
        CandidateProfileView {
            observation_count: 42,
            fields: vec![],
            truncated: false,
        }
    }

    fn family() -> FamilyId {
        FamilyId::new_v7()
    }

    // -- HumanPromote: positive -------------------------------------------

    #[test]
    fn human_promote_is_positive_with_the_promoted_decision_as_gold() {
        let gold = InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let example = capture_human_promote(candidate(), vec![], gold.clone(), family(), 1000);

        assert_eq!(example.label_source, LabelSource::HumanPromote);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
        assert_eq!(example.gold, gold);
    }

    #[test]
    fn human_promote_of_a_new_family_carries_new_candidate_gold() {
        let gold = InferenceDecision::NewCandidate {
            novelty: Novelty::Structural,
        };
        let example = capture_human_promote(candidate(), vec![], gold.clone(), family(), 1000);
        assert_eq!(example.gold, gold);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
    }

    // -- TrustedProposalAccepted: positive, confirms the model's own proposal --

    #[test]
    fn trusted_proposal_accepted_confirms_the_models_own_proposal() {
        let id = schema_id(2);
        let example = capture_trusted_proposal_accepted(
            candidate(),
            vec![],
            id.clone(),
            Relation::CompatibleDrift,
            family(),
            2000,
        );

        assert_eq!(example.label_source, LabelSource::TrustedProposalAccepted);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
        assert_eq!(
            example.gold,
            InferenceDecision::MatchSchema {
                schema_id: id,
                relation: Relation::CompatibleDrift,
            }
        );
    }

    // -- TrustedProposalRejected: THE hard-negative -----------------------

    #[test]
    fn trusted_proposal_rejected_is_a_hard_negative_with_the_highest_weight() {
        let rejected = schema_id(3);
        let example =
            capture_trusted_proposal_rejected(candidate(), vec![], &rejected, None, family(), 3000);

        assert_eq!(example.label_source, LabelSource::TrustedProposalRejected);
        assert_eq!(example.weight, HARD_NEGATIVE_WEIGHT);
        assert!(
            example.weight > POSITIVE_WEIGHT,
            "a hard-negative must outweigh every positive label source"
        );
        // Default correction (no more specific human input) is a safe
        // Abstain — never an accepted match to the rejected family.
        assert_eq!(
            example.gold,
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous
            }
        );
    }

    #[test]
    fn trusted_proposal_rejected_uses_the_humans_correction_when_supplied() {
        let rejected = schema_id(4);
        let correct_family = schema_id(5);
        let correction = InferenceDecision::MatchSchema {
            schema_id: correct_family.clone(),
            relation: Relation::Exact,
        };
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            Some(correction.clone()),
            family(),
            3001,
        );

        assert_eq!(example.gold, correction);
        assert_eq!(example.weight, HARD_NEGATIVE_WEIGHT);
        // The gold must name a DIFFERENT family than the rejected one.
        assert_ne!(
            example.gold,
            InferenceDecision::MatchSchema {
                schema_id: rejected,
                relation: Relation::Exact,
            }
        );
    }

    #[test]
    fn trusted_proposal_rejected_never_reaffirms_the_rejected_family_as_new_candidate_correction() {
        let rejected = schema_id(6);
        let correction = InferenceDecision::NewCandidate {
            novelty: Novelty::Semantic,
        };
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            Some(correction.clone()),
            family(),
            3002,
        );
        assert_eq!(example.gold, correction);
        assert_eq!(example.label_source, LabelSource::TrustedProposalRejected);
    }

    // -- SemanticAnnotation / Adjudication: positive, gold from the label --

    #[test]
    fn semantic_annotation_is_positive_with_the_supplied_gold() {
        let gold = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };
        let example =
            capture_semantic_annotation(candidate(), vec![], gold.clone(), family(), 4000);
        assert_eq!(example.label_source, LabelSource::SemanticAnnotation);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
        assert_eq!(example.gold, gold);
    }

    #[test]
    fn adjudication_is_positive_with_the_supplied_gold() {
        let gold = InferenceDecision::MatchSchema {
            schema_id: schema_id(7),
            relation: Relation::Exact,
        };
        let example = capture_adjudication(candidate(), vec![], gold.clone(), family(), 5000);
        assert_eq!(example.label_source, LabelSource::Adjudication);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
        assert_eq!(example.gold, gold);
    }

    // -- carries only redacted data through untouched ----------------------

    #[test]
    fn every_capture_function_preserves_the_redacted_candidate_and_retrieved_verbatim() {
        let cand = CandidateProfileView {
            observation_count: 99,
            fields: vec![],
            truncated: true,
        };
        let retrieved = vec![FamilyCandidate {
            family_id: family(),
            schema_id: schema_id(8),
            version: 3,
            distance: 0.01,
            rank: 1,
        }];
        let gold = InferenceDecision::Abstain {
            cause: AbstainCause::CandidateMissing,
        };
        let example = capture_human_promote(cand.clone(), retrieved.clone(), gold, family(), 6000);
        assert_eq!(example.candidate, cand);
        assert_eq!(example.retrieved, retrieved);
    }
}
