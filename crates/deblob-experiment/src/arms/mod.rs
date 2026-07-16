//! The shared `Arm` trait + arm identifiers (spec §1 "The arms (and
//! ablations)", §8 "arms (`arms/`) — A0/A1/B0/B1/B2 + Cn deciders over a
//! shared trait").
//!
//! Every arm is a pure, deterministic (given the same [`InferenceInput`]
//! and, for B0/B1, the same scripted/seeded inferencer) function from a
//! leak-free [`InferenceInput`] to a 3-way [`ArmDecision`]. No arm ever
//! sees a [`crate::labels::GoldSidecar`] — that would defeat the whole
//! anti-tautology point of the experiment (spec §2).
//!
//! - [`deterministic`] — A0 (retrieval-only, ungated) and A1 (the fair,
//!   tuned-threshold deterministic baseline).
//! - [`gate`] — [`gate::GatedArm`], the FROZEN trust-gate wrapper shared by
//!   every gated arm (B1, B2, and the per-predicate ablation variants).
//!   Reuses `deblob::shadow::evaluate_policy` verbatim — never
//!   reimplemented.
//! - [`semantic`] — [`semantic::SemanticArm`], the seam over
//!   `deblob_slm::SemanticInferencer` that B0/B1 wrap. Real backend
//!   adapters are Task 3; this task only wires a mock.
//! - [`mock`] — the Task-1 `SemanticInferencer` stand-ins: a fully scripted
//!   playback inferencer for exact-control tests, and a deterministic
//!   heuristic inferencer for full-corpus runs.

pub mod deterministic;
pub mod gate;
pub mod mock;
pub mod semantic;

/// The 3-way decision an arm proposes for one candidate cluster. Reused
/// verbatim from `deblob_slm::InferenceDecision` — the exact contract
/// shape (spec §8: "reuse, don't reinvent") rather than a parallel enum.
pub type ArmDecision = deblob_slm::InferenceDecision;

/// Identifies which of the spec §1 arms produced a given
/// [`ArmDecision`]/report row. `C { round }` (continual-learning
/// trajectory, spec §7) is B1's model retrained over prequential rounds —
/// the SAME `GatedArm`/gate every other arm reuses, wrapping whichever
/// `SemanticInferencer` that round's gate-passing candidate produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArmId {
    /// Retrieval-only, ungated: top-1 by structural distance.
    A0,
    /// The fair deterministic policy: tuned thresholds, calibrated
    /// abstain, the SAME hard trust constraints (`deblob::shadow`'s
    /// `POLICY_*` constants) B1's gate enforces.
    A1,
    /// Raw SLM output, NO trust gate. Diagnostic only — never deployed.
    B0,
    /// Deterministic retrieval + SLM + the full trust gate.
    B1,
    /// Deterministic top-1 substituted for the SLM, through the SAME trust
    /// gate — the redundancy ablation.
    B2,
    /// B1's model after `round` prequential retrain-and-gate cycles (spec
    /// §7). `round == 0` is `B_v0` — B1's original model, before any
    /// continual-learning round has run.
    C { round: u32 },
}

impl ArmId {
    pub fn label(self) -> String {
        match self {
            ArmId::A0 => "A0 (retrieval-only)".to_string(),
            ArmId::A1 => "A1 (fair deterministic policy)".to_string(),
            ArmId::B0 => "B0 (raw SLM, no gate)".to_string(),
            ArmId::B1 => "B1 (SLM + trust gate)".to_string(),
            ArmId::B2 => "B2 (det-top1 + trust gate, redundancy ablation)".to_string(),
            ArmId::C { round } => format!("C{round} (B1 after {round} continual round(s))"),
        }
    }
}

/// A decider from a leak-free [`InferenceInput`] to a 3-way
/// [`ArmDecision`]. Implemented by every concrete arm in [`deterministic`],
/// [`gate`], and [`semantic`].
pub trait Arm: Send + Sync {
    fn id(&self) -> ArmId;
    fn decide(&self, input: &crate::labels::InferenceInput) -> ArmDecision;
}
