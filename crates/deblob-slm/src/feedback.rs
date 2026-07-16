//! Shared training-example shape for the SLM continual-learning loop
//! (spec: `docs/superpowers/specs/2026-07-16-slm-continual-learning.md`).
//!
//! [`TrainingExample`] deliberately mirrors the shape of
//! `deblob_eval::corpus::EvalCase` (redacted `candidate` + `retrieved` +
//! ground truth) so a labeled human decision and a synthetic corpus case
//! can combine into one training/eval set without a translation layer. It
//! lives in `deblob-slm` (rather than the `deblob` crate, where the
//! capture/label-mapping logic in `deblob::feedback` actually runs) so both
//! `deblob` (label mapping) and `deblob-redis` (the durable store) can
//! depend on it directly — `deblob-redis` cannot depend on the `deblob`
//! crate itself (`deblob` already depends on `deblob-redis`, and Cargo
//! rejects the cycle).

use deblob_core::id::FamilyId;
use serde::{Deserialize, Serialize};

use crate::contract::{CandidateProfileView, FamilyCandidate, InferenceDecision};

/// Which governed decision path produced a [`TrainingExample`] — drives
/// both its `weight` and, for `TrustedProposalRejected`, the sign of the
/// reinforcement signal (a hard negative rather than a positive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelSource {
    /// An operator promoted a candidate — the gold match/new decision.
    /// Positive.
    HumanPromote,
    /// The model proposed a match, the deterministic trust gate proposed
    /// it to a human, and the human APPROVED. Positive (confirms the
    /// model's own proposal).
    TrustedProposalAccepted,
    /// The model proposed a match, a human REJECTED it. Hard-negative: the
    /// gold is "this is NOT that family" — the highest-value correction
    /// signal, since it directly targets a failure mode the trust gate
    /// already caught deterministically.
    TrustedProposalRejected,
    /// A P2-D semantic annotation (controlled-vocabulary ground truth).
    SemanticAnnotation,
    /// An offline human label on a shadow-log record.
    Adjudication,
}

/// Why a `TrustedProposalRejected` human decision rejected the model's
/// proposal (spec amendment A2). Drives whether the rejection is emitted
/// as generator-negative training data at all: a rejection is only ever
/// evidence the GENERATOR was wrong when the reason names a generator
/// fault. `PolicyDenial` (a deterministic policy gate said no, independent
/// of whether the match was semantically correct) and `RetrievalMiss`
/// (the right family was never offered to the model, so it couldn't have
/// proposed it) are NOT generator faults — mislabeling either as a
/// generator hard-negative would poison the training set by teaching the
/// model to avoid a family it was never wrong about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    /// The model matched the wrong family outright. Generator fault.
    WrongFamily,
    /// The candidate should have been proposed as a new family, not a
    /// match. Generator fault.
    ShouldBeNewCandidate,
    /// The model should have abstained rather than propose a match.
    /// Generator fault.
    ShouldAbstain,
    /// A deterministic policy gate denied the proposal independent of
    /// whether it was semantically correct — NOT a generator fault.
    PolicyDenial,
    /// The correct family was never in the retrieved top-k offered to the
    /// model — a retrieval failure, NOT a generator fault (the generator
    /// cannot propose an id it was never shown).
    RetrievalMiss,
    /// Any other reason not covered above. Conservatively treated the
    /// same as `PolicyDenial`/`RetrievalMiss`: NOT a generator fault, so
    /// it is never emitted as generator-negative training data by
    /// `deblob::feedback::capture_trusted_proposal_rejected`.
    Other,
}

impl RejectionReason {
    /// `true` for the three reasons the spec names as "become
    /// generator-negative training data" — the model itself was wrong.
    /// `false` for `PolicyDenial`/`RetrievalMiss`/`Other`, which are
    /// never emitted as generator-negative data (spec amendment A2).
    pub fn is_generator_fault(self) -> bool {
        matches!(
            self,
            RejectionReason::WrongFamily
                | RejectionReason::ShouldBeNewCandidate
                | RejectionReason::ShouldAbstain
        )
    }
}

/// How much this capture's source is trusted, for anti-poisoning policy
/// (spec amendment A4). A coarse classification only — the hard
/// enforcement mechanism is `FeedbackStore` quarantine (keyed by `actor`),
/// not this field; this field is the audit signal a quarantine decision
/// is made from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTrustLevel {
    /// A governed decision path with deterministic corroboration behind
    /// it (e.g. `TrustedProposalAccepted`/`Rejected` — the trust gate
    /// already vetted the proposal before a human ever saw it).
    Trusted,
    /// An ordinary human decision with no additional corroboration
    /// (e.g. an unassisted `HumanPromote`).
    Standard,
    /// A source that has not (yet) earned `Standard`/`Trusted` — e.g. a
    /// new integration, an unauthenticated annotation channel, or any
    /// actor flagged for review. Not automatically excluded from
    /// capture, but the natural default an operator quarantines from
    /// under `FeedbackStore::quarantine_actor` when it starts producing
    /// anomalous volume.
    Unverified,
}

/// One labeled training example — same shape as
/// `deblob_eval::corpus::EvalCase` (redacted `candidate` + `retrieved` +
/// ground-truth `gold`) so feedback and the synthetic corpus combine into
/// one training/eval set without a translation layer.
///
/// Carries ONLY already-redacted/derived data: `candidate` is a
/// `CandidateProfileView` (monoid statistics only, never a raw value —
/// see `deblob_slm::prompt`'s module docs), and `retrieved` carries only
/// ids/distances. No [`TrainingExample`] constructor may ever be handed a
/// raw candidate payload value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrainingExample {
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    pub gold: InferenceDecision,
    pub label_source: LabelSource,
    /// Training weight. `TrustedProposalRejected` (hard-negative) carries
    /// the highest weight of any label source — see
    /// `deblob::feedback::HARD_NEGATIVE_WEIGHT`. Sourced from a
    /// `deblob::feedback::FeedbackWeights` config value at capture time
    /// (spec amendment A3) — never hard-coded in this struct.
    pub weight: f32,
    /// The family this example's evidence is about — the unit the
    /// train/holdout split (spec §2/§5) partitions on, so a fine-tune
    /// holdout never contains a train family's sibling example.
    pub partition_key: FamilyId,
    /// Wall-clock capture time (epoch milliseconds).
    pub recorded_at: i64,
    /// `Some` only for a `label_source: TrustedProposalRejected` example
    /// — WHY the human rejected the model's proposal (spec amendment A2).
    /// `None` for every other label source. Preserved on the emitted
    /// example purely for audit; the emit/reject decision itself is made
    /// once, at capture time, by
    /// `deblob::feedback::capture_trusted_proposal_rejected`.
    #[serde(default)]
    pub rejection_reason: Option<RejectionReason>,
    /// Who/what supplied this example (a human operator id, an
    /// annotation-API caller id, `"retrain:v1"` for a system actor, …).
    /// Anti-poisoning provenance (spec amendment A4): `FeedbackStore`
    /// quarantine is keyed on this field.
    #[serde(default)]
    pub actor: String,
    /// How much `actor` is trusted at capture time (spec amendment A4).
    #[serde(default = "default_source_trust_level")]
    pub source_trust_level: SourceTrustLevel,
    /// The version of the 3-way tool contract (`crate::contract`) this
    /// example's `gold` was captured against. Lets a future consumer
    /// detect and exclude a training example captured under a contract
    /// version the current fine-tune target no longer speaks (spec
    /// amendment A4).
    #[serde(default)]
    pub tool_schema_version: u32,
    /// Near-duplicate grouping key (spec amendment A4/A5). Examples that
    /// are paraphrases/synthetic siblings of the same underlying
    /// observation share a `dedup_cluster` so `FeedbackStore` can (a)
    /// deduplicate them and (b) keep every member of one cluster on the
    /// SAME side of a train/holdout split (a paraphrase must never cross
    /// that boundary). Empty string means "not clustered" — such an
    /// example is never deduplicated against another. A `"safety:"`
    /// prefix reserves the example for the permanent
    /// `never_trained_safety_suite` partition (spec amendment A5) — see
    /// `deblob_redis::feedback_store`'s module docs.
    #[serde(default)]
    pub dedup_cluster: String,
}

fn default_source_trust_level() -> SourceTrustLevel {
    SourceTrustLevel::Standard
}
