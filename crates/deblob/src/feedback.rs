//! Feedback capture — every human decision becomes a labeled training
//! example (spec: `docs/superpowers/specs/2026-07-16-slm-continual-learning.md`
//! §1, amendments A2/A3).
//!
//! The [`TrainingExample`]/[`LabelSource`] SHAPE lives in
//! `deblob_slm::feedback` (see that module's docs for why); this module
//! owns the LABEL-MAPPING logic — turning the outcome of a governed
//! decision path (`crate::trusted::TrustedApplier`'s verdicts,
//! `crate::promote::Promoter::promote`, an annotation/adjudication API)
//! into the correct `gold`/`weight`/`label_source` triple, plus (as of
//! the joint-research amendments) the provenance stamped on every emitted
//! example and the rejection-reason gate that decides whether a
//! `TrustedProposalRejected` is trustworthy generator-negative data at
//! all.
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
//! carries [`FeedbackWeights::hard_negative`] rather than
//! [`FeedbackWeights::positive`], and its `gold` is constructed so it can
//! NEVER re-affirm the rejected family (see that function's docs).
//!
//! # Rejection semantics are NOT automatic (amendment A2)
//!
//! A `TrustedProposalRejected` is only evidence the GENERATOR was wrong
//! when `rejection_reason.is_generator_fault()` — `WrongFamily`,
//! `ShouldBeNewCandidate`, or `ShouldAbstain`. A `PolicyDenial` (a
//! deterministic policy gate said no, independent of whether the match
//! was semantically correct) or a `RetrievalMiss` (the correct family was
//! never offered to the model, so it could not have proposed it) is NOT
//! the generator being wrong; emitting either as a generator hard-negative
//! would poison the training set by teaching the model to avoid a family
//! it was never actually wrong about. `capture_trusted_proposal_rejected`
//! therefore returns `Option<TrainingExample>` and returns `None` for
//! every non-generator-fault reason — documented, not silently dropped:
//! callers that want to capture a `PolicyDenial`/`RetrievalMiss` as
//! *retrieval/policy* signal (a different training target entirely) must
//! do so through a separate capture path; this module does not synthesize
//! one, since that signal has a different shape (it is not about what the
//! GENERATOR should output).

use deblob_core::id::{FamilyId, SchemaId};
use deblob_slm::{
    AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision, LabelSource,
    RejectionReason, Relation, SourceTrustLevel, TrainingExample,
};

/// Weight assigned to every positive label source (`HumanPromote`,
/// `TrustedProposalAccepted`, `SemanticAnnotation`, `Adjudication`) by
/// [`FeedbackWeights::default`]. **Unvalidated — ablate** (spec amendment
/// A3 / joint-research `jr-slm-cl-161812`: no controlled study backs this
/// number for this model class).
pub const POSITIVE_WEIGHT: f32 = 1.0;

/// Weight assigned to a `TrustedProposalRejected` hard-negative by
/// [`FeedbackWeights::default`] — highest of any label source (spec §1:
/// "Highest weight — it directly targets a failure mode"). Strictly
/// greater than [`POSITIVE_WEIGHT`]. **Unvalidated — ablate** (spec
/// amendment A3: "`weight=3.0` has no evidentiary basis").
pub const HARD_NEGATIVE_WEIGHT: f32 = 3.0;

/// Ablatable training-weight configuration (spec amendment A3: "`weight`
/// ... is NOT hard-coded — it is a config parameter"). [`Default`]
/// reproduces the ORIGINAL hard-coded values ([`POSITIVE_WEIGHT`],
/// [`HARD_NEGATIVE_WEIGHT`]) so existing behavior is unchanged until a
/// caller deliberately overrides it — but every `capture_*` function in
/// this module now reads weight from here, never from a constant
/// directly, so an operator can ablate `weight` without touching this
/// module's code.
///
/// **Both defaults are unvalidated** — the joint-research report is
/// explicit that `weight=3.0` "has no evidentiary basis"; treat both
/// numbers as a starting point for a local ablation, not a settled
/// hyperparameter.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FeedbackWeights {
    /// Weight for every positive label source.
    pub positive: f32,
    /// Weight for a `TrustedProposalRejected` hard-negative. Must stay
    /// strictly greater than `positive` for the reinforcement signal to
    /// mean anything — [`FeedbackWeights::new`] enforces this.
    pub hard_negative: f32,
}

impl Default for FeedbackWeights {
    /// **Unvalidated — ablate.** Reproduces the module's original
    /// hard-coded constants.
    fn default() -> Self {
        Self {
            positive: POSITIVE_WEIGHT,
            hard_negative: HARD_NEGATIVE_WEIGHT,
        }
    }
}

impl FeedbackWeights {
    /// Constructs a config, enforcing the one invariant every capture
    /// function here relies on: a hard-negative must outweigh a positive,
    /// or the "reinforcement" signal degenerates to noise. `None` if
    /// `hard_negative <= positive`.
    pub fn new(positive: f32, hard_negative: f32) -> Option<Self> {
        if hard_negative > positive {
            Some(Self {
                positive,
                hard_negative,
            })
        } else {
            None
        }
    }
}

/// Non-decision metadata every `capture_*` call must supply: the
/// provenance fields anti-poisoning defense needs (spec amendment A4) and
/// the weight config (amendment A3), bundled so the `capture_*` function
/// signatures don't grow an unreadable positional-parameter list.
#[derive(Debug, Clone)]
pub struct CaptureContext {
    /// Who/what supplied this decision (operator id, annotation-API
    /// caller id, `"retrain:v1"` for a system actor, …). Never empty in
    /// practice — `FeedbackStore` quarantine is keyed on this field.
    pub actor: String,
    /// How much `actor` is trusted at capture time.
    pub source_trust_level: SourceTrustLevel,
    /// The 3-way tool contract version (`deblob_slm::contract`) this
    /// decision was made under.
    pub tool_schema_version: u32,
    /// Near-duplicate grouping key — see
    /// `deblob_slm::TrainingExample::dedup_cluster`'s docs for the
    /// `"safety:"`-prefix reservation convention. Empty string if this
    /// example isn't part of any known cluster.
    pub dedup_cluster: String,
    /// Ablatable weight config (amendment A3) — see [`FeedbackWeights`].
    pub weights: FeedbackWeights,
    /// The family this example's evidence is about.
    pub partition_key: FamilyId,
    /// Wall-clock capture time (epoch milliseconds).
    pub recorded_at: i64,
}

fn build(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    label_source: LabelSource,
    weight: f32,
    rejection_reason: Option<RejectionReason>,
    ctx: &CaptureContext,
) -> TrainingExample {
    TrainingExample {
        candidate,
        retrieved,
        gold,
        label_source,
        weight,
        partition_key: ctx.partition_key.clone(),
        recorded_at: ctx.recorded_at,
        rejection_reason,
        actor: ctx.actor.clone(),
        source_trust_level: ctx.source_trust_level,
        tool_schema_version: ctx.tool_schema_version,
        dedup_cluster: ctx.dedup_cluster.clone(),
    }
}

/// `HumanPromote`: an operator promoted a candidate. `gold` is the exact
/// decision the promotion represents — a `MatchSchema` into an existing
/// family, or a `NewCandidate` when the promotion created a new one.
/// Positive, `ctx.weights.positive`.
pub fn capture_human_promote(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    ctx: &CaptureContext,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::HumanPromote,
        ctx.weights.positive,
        None,
        ctx,
    )
}

/// `TrustedProposalAccepted`: the model proposed `MatchSchema{schema_id,
/// relation}`, the deterministic trust gate queued it
/// (`TrustVerdict::ProposeToHuman`), and a human approved. `gold` is the
/// model's OWN proposal, confirmed — positive, `ctx.weights.positive`.
pub fn capture_trusted_proposal_accepted(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    schema_id: SchemaId,
    relation: Relation,
    ctx: &CaptureContext,
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
        ctx.weights.positive,
        None,
        ctx,
    )
}

/// `TrustedProposalRejected`: the model proposed `MatchSchema{rejected_schema_id,
/// ..}`, the deterministic trust gate queued it, and a human REJECTED it.
///
/// **Amendment A2 — rejection semantics are not automatic.** Returns
/// `None` unless `rejection_reason.is_generator_fault()` — a
/// `PolicyDenial` or `RetrievalMiss` (or any other non-generator-fault
/// reason) means the GENERATOR was not necessarily wrong, so it is never
/// emitted as generator-negative training data through this path (see
/// this module's docs for where that signal belongs instead).
///
/// When `rejection_reason` IS a generator fault, `gold` is set to
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
/// Carries `ctx.weights.hard_negative` — strictly higher than every
/// positive label source (enforced by [`FeedbackWeights::new`]).
pub fn capture_trusted_proposal_rejected(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    rejected_schema_id: &SchemaId,
    rejection_reason: RejectionReason,
    corrected: Option<InferenceDecision>,
    ctx: &CaptureContext,
) -> Option<TrainingExample> {
    if !rejection_reason.is_generator_fault() {
        // PolicyDenial / RetrievalMiss / Other: not the generator being
        // wrong. Documented no-op, per this module's and
        // `RejectionReason`'s docs — capture a retrieval/policy signal
        // through a separate path if one is needed; this constructor
        // never manufactures generator-negative data from a reason that
        // doesn't implicate the generator.
        return None;
    }

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
    Some(build(
        candidate,
        retrieved,
        gold,
        LabelSource::TrustedProposalRejected,
        ctx.weights.hard_negative,
        Some(rejection_reason),
        ctx,
    ))
}

/// `SemanticAnnotation`: a P2-D controlled-vocabulary annotation supplies
/// `gold` directly. Positive, `ctx.weights.positive`.
pub fn capture_semantic_annotation(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    ctx: &CaptureContext,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::SemanticAnnotation,
        ctx.weights.positive,
        None,
        ctx,
    )
}

/// `Adjudication`: an offline human label on a shadow-log record supplies
/// `gold` directly. Positive, `ctx.weights.positive`.
pub fn capture_adjudication(
    candidate: CandidateProfileView,
    retrieved: Vec<FamilyCandidate>,
    gold: InferenceDecision,
    ctx: &CaptureContext,
) -> TrainingExample {
    build(
        candidate,
        retrieved,
        gold,
        LabelSource::Adjudication,
        ctx.weights.positive,
        None,
        ctx,
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

    fn ctx() -> CaptureContext {
        CaptureContext {
            actor: "operator:alice".to_string(),
            source_trust_level: SourceTrustLevel::Standard,
            tool_schema_version: 1,
            dedup_cluster: String::new(),
            weights: FeedbackWeights::default(),
            partition_key: family(),
            recorded_at: 1000,
        }
    }

    // -- FeedbackWeights: config, not a constant (amendment A3) -----------

    #[test]
    fn feedback_weights_default_reproduces_original_hard_coded_values() {
        let w = FeedbackWeights::default();
        assert_eq!(w.positive, POSITIVE_WEIGHT);
        assert_eq!(w.hard_negative, HARD_NEGATIVE_WEIGHT);
    }

    #[test]
    fn feedback_weights_new_rejects_a_hard_negative_that_does_not_outweigh_positive() {
        assert!(FeedbackWeights::new(1.0, 3.0).is_some());
        assert!(FeedbackWeights::new(1.0, 1.0).is_none());
        assert!(FeedbackWeights::new(2.0, 1.0).is_none());
    }

    #[test]
    fn capture_functions_read_weight_from_config_not_a_constant() {
        let mut c = ctx();
        c.weights = FeedbackWeights::new(5.0, 50.0).unwrap();
        let gold = InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let positive = capture_human_promote(candidate(), vec![], gold, &c);
        assert_eq!(positive.weight, 5.0);

        let rejected = schema_id(9);
        let hard_negative = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::WrongFamily,
            None,
            &c,
        )
        .expect("WrongFamily is a generator fault, must be emitted");
        assert_eq!(hard_negative.weight, 50.0);
    }

    // -- HumanPromote: positive -------------------------------------------

    #[test]
    fn human_promote_is_positive_with_the_promoted_decision_as_gold() {
        let gold = InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let c = ctx();
        let example = capture_human_promote(candidate(), vec![], gold.clone(), &c);

        assert_eq!(example.label_source, LabelSource::HumanPromote);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
        assert_eq!(example.gold, gold);
        assert_eq!(example.rejection_reason, None);
        assert_eq!(example.actor, c.actor);
        assert_eq!(example.source_trust_level, c.source_trust_level);
        assert_eq!(example.tool_schema_version, c.tool_schema_version);
    }

    #[test]
    fn human_promote_of_a_new_family_carries_new_candidate_gold() {
        let gold = InferenceDecision::NewCandidate {
            novelty: Novelty::Structural,
        };
        let c = ctx();
        let example = capture_human_promote(candidate(), vec![], gold.clone(), &c);
        assert_eq!(example.gold, gold);
        assert_eq!(example.weight, POSITIVE_WEIGHT);
    }

    // -- TrustedProposalAccepted: positive, confirms the model's own proposal --

    #[test]
    fn trusted_proposal_accepted_confirms_the_models_own_proposal() {
        let id = schema_id(2);
        let c = ctx();
        let example = capture_trusted_proposal_accepted(
            candidate(),
            vec![],
            id.clone(),
            Relation::CompatibleDrift,
            &c,
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
        assert_eq!(example.rejection_reason, None);
    }

    // -- TrustedProposalRejected: THE hard-negative -----------------------

    #[test]
    fn trusted_proposal_rejected_is_a_hard_negative_with_the_highest_weight() {
        let rejected = schema_id(3);
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::WrongFamily,
            None,
            &c,
        )
        .expect("WrongFamily is a generator fault");

        assert_eq!(example.label_source, LabelSource::TrustedProposalRejected);
        assert_eq!(example.weight, HARD_NEGATIVE_WEIGHT);
        assert!(
            example.weight > POSITIVE_WEIGHT,
            "a hard-negative must outweigh every positive label source"
        );
        assert_eq!(example.rejection_reason, Some(RejectionReason::WrongFamily));
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
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::ShouldBeNewCandidate,
            Some(correction.clone()),
            &c,
        )
        .expect("ShouldBeNewCandidate is a generator fault");

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
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::ShouldAbstain,
            Some(correction.clone()),
            &c,
        )
        .expect("ShouldAbstain is a generator fault");
        assert_eq!(example.gold, correction);
        assert_eq!(example.label_source, LabelSource::TrustedProposalRejected);
    }

    // -- Amendment A2: policy-denial / retrieval-miss are NOT generator-negative

    #[test]
    fn policy_denial_rejection_is_not_emitted_as_generator_negative_data() {
        let rejected = schema_id(7);
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::PolicyDenial,
            None,
            &c,
        );
        assert!(
            example.is_none(),
            "a PolicyDenial rejection must never become generator-negative training data"
        );
    }

    #[test]
    fn retrieval_miss_rejection_is_not_emitted_as_generator_negative_data() {
        let rejected = schema_id(8);
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::RetrievalMiss,
            None,
            &c,
        );
        assert!(
            example.is_none(),
            "a RetrievalMiss rejection must never become generator-negative training data"
        );
    }

    #[test]
    fn retrieval_miss_rejection_is_excluded_even_with_a_corrected_target() {
        // Even when a human supplies a concrete correction, a
        // RetrievalMiss still isn't evidence the GENERATOR was wrong (it
        // never saw the right answer to be wrong about) — the presence of
        // a correction does not override the reason gate.
        let rejected = schema_id(10);
        let correction = InferenceDecision::MatchSchema {
            schema_id: schema_id(11),
            relation: Relation::Exact,
        };
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::RetrievalMiss,
            Some(correction),
            &c,
        );
        assert!(example.is_none());
    }

    #[test]
    fn other_rejection_reason_is_not_emitted_as_generator_negative_data() {
        let rejected = schema_id(12);
        let c = ctx();
        let example = capture_trusted_proposal_rejected(
            candidate(),
            vec![],
            &rejected,
            RejectionReason::Other,
            None,
            &c,
        );
        assert!(example.is_none());
    }

    // -- SemanticAnnotation / Adjudication: positive, gold from the label --

    #[test]
    fn semantic_annotation_is_positive_with_the_supplied_gold() {
        let gold = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };
        let c = ctx();
        let example = capture_semantic_annotation(candidate(), vec![], gold.clone(), &c);
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
        let c = ctx();
        let example = capture_adjudication(candidate(), vec![], gold.clone(), &c);
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
        let c = ctx();
        let example = capture_human_promote(cand.clone(), retrieved.clone(), gold, &c);
        assert_eq!(example.candidate, cand);
        assert_eq!(example.retrieved, retrieved);
    }

    // -- provenance is stamped from CaptureContext (amendment A4) ---------

    #[test]
    fn provenance_fields_are_stamped_from_the_capture_context() {
        let c = CaptureContext {
            actor: "annotation-api:bob".to_string(),
            source_trust_level: SourceTrustLevel::Unverified,
            tool_schema_version: 3,
            dedup_cluster: "cluster-42".to_string(),
            weights: FeedbackWeights::default(),
            partition_key: family(),
            recorded_at: 12345,
        };
        let gold = InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        };
        let example = capture_semantic_annotation(candidate(), vec![], gold, &c);

        assert_eq!(example.actor, "annotation-api:bob");
        assert_eq!(example.source_trust_level, SourceTrustLevel::Unverified);
        assert_eq!(example.tool_schema_version, 3);
        assert_eq!(example.dedup_cluster, "cluster-42");
        assert_eq!(example.partition_key, c.partition_key);
        assert_eq!(example.recorded_at, 12345);
    }
}
