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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    /// `deblob::feedback::HARD_NEGATIVE_WEIGHT`.
    pub weight: f32,
    /// The family this example's evidence is about — the unit the
    /// train/holdout split (spec §2/§5) partitions on, so a fine-tune
    /// holdout never contains a train family's sibling example.
    pub partition_key: FamilyId,
    /// Wall-clock capture time (epoch milliseconds).
    pub recorded_at: i64,
}
