//! Per-round adaptation-gain + retention-loss (spec §7: "Report adaptation
//! gain (future-slice performance) AND retention loss (frozen-slice
//! regression) — improving recent rejects while forgetting established
//! families is NOT improvement") plus the shared accuracy primitive
//! [`crate::continual::prequential`] scores single models with.
//!
//! Reuses the SAME external-label scoring convention Task 1's Layer 2/4
//! metrics use (`decision == expected.decision`, scored against
//! `deblob_eval::Expected` — never the gate's own predicates) rather than
//! reimplementing correctness. The `C_final`-vs-`B_v0` sealed-audit
//! comparison reuses `crate::metrics::l4_utility::compute_l4` directly (see
//! `prequential::FrozenTrajectory::c_final_vs_b_v0`) for its paired
//! McNemar/bootstrap statistics — this module only adds the single-model
//! (not paired-arm) accuracy primitive Layer 4's `L4CaseView` has no slot
//! for.

use deblob_eval::Expected;

use crate::arms::ArmDecision;

/// Plain exact-match accuracy against the external label — the same
/// per-case correctness check `metrics::l4_utility::is_correct` applies,
/// exposed here because an adaptation/retention probe scores ONE model
/// against a batch (no second arm to pair against), unlike every Layer 4
/// view.
pub fn accuracy(decisions: &[ArmDecision], expecteds: &[Expected]) -> f64 {
    if decisions.is_empty() {
        return 0.0;
    }
    let correct = decisions
        .iter()
        .zip(expecteds.iter())
        .filter(|(d, e)| *d == &e.decision)
        .count();
    correct as f64 / decisions.len() as f64
}

/// One round's adaptation-gain / retention-loss pair.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct AdaptationRetention {
    /// `accuracy(model_{r+1}, future_batch) - accuracy(model_r, future_batch)`
    /// — positive means the retrained model generalizes better to data
    /// neither model has been evaluated-and-fed-back on yet. `None` for
    /// the FINAL round (no future batch exists in the round stream).
    pub adaptation_gain: Option<f64>,
    /// `accuracy(model_r, frozen_slices) - accuracy(model_{r+1}, frozen_slices)`
    /// — POSITIVE means forgetting (retention got WORSE after retraining).
    /// `None` for round 0 (no frozen history exists yet to check against).
    pub retention_loss: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::SchemaId;
    use deblob_slm::{AbstainCause, InferenceDecision, Relation};

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn expected_match(byte: u8) -> Expected {
        Expected {
            decision: InferenceDecision::MatchSchema {
                schema_id: schema_id(byte),
                relation: Relation::Exact,
            },
            gold_schema_id: Some(schema_id(byte)),
            gold_rank: Some(1),
            false_merge_trap: false,
            false_split_trap: false,
        }
    }

    #[test]
    fn accuracy_is_the_fraction_of_exact_matches() {
        let decisions = vec![
            InferenceDecision::MatchSchema {
                schema_id: schema_id(1),
                relation: Relation::Exact,
            },
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
        ];
        let expecteds = vec![expected_match(1), expected_match(2)];
        assert_eq!(accuracy(&decisions, &expecteds), 0.5);
    }

    #[test]
    fn accuracy_of_an_empty_batch_is_zero_not_a_panic() {
        assert_eq!(accuracy(&[], &[]), 0.0);
    }
}
