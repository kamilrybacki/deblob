//! Layer 3 — trust-gate containment (spec §3): "fraction of raw SLM
//! errors BLOCKED; fraction of CORRECT SLM decisions blocked
//! (over-blocking cost); accepted coverage; externally-measured accepted
//! risk; false-merge count with N + upper confidence bound (rule-of-three
//! for zero-event); guard-activation reasons; added latency + review
//! cost."
//!
//! Compares a RAW decision (e.g. B0) against the SAME decider's GATED
//! output (e.g. B1, or B2) on the identical case, using
//! `deblob::shadow::PolicyOutcome` captured per case by
//! `crate::arms::gate::GatedArm::decide_with_gate` — never recomputed
//! here; this module only aggregates what the real gate already decided.

use deblob::shadow::{GateReason, PolicyOutcome};
use deblob_eval::Expected;

use crate::arms::ArmDecision;
use crate::metrics::stats::upper_bound_95;

pub struct L3CaseView<'a> {
    pub raw_decision: &'a ArmDecision,
    pub gated_decision: &'a ArmDecision,
    pub gate_outcome: &'a PolicyOutcome,
    pub expected: &'a Expected,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GuardActivation {
    pub reason: GateReason,
    pub count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GateContainmentMetrics {
    pub total_cases: usize,
    pub raw_error_count: usize,
    /// Of the raw-error cases where the gate was even invoked (raw
    /// proposed a match), the fraction the gate blocked (rejected). `None`
    /// if the gate was never invoked on a raw error.
    pub fraction_raw_errors_blocked: Option<f64>,
    pub raw_correct_count: usize,
    /// Over-blocking cost: of the raw-CORRECT cases where the gate was
    /// invoked, the fraction the gate rejected anyway. `None` if the gate
    /// was never invoked on a raw-correct case.
    pub fraction_correct_blocked: Option<f64>,
    /// Fraction of ALL cases whose FINAL gated decision is an accepted
    /// match.
    pub accepted_coverage: f64,
    pub accepted_count: usize,
    /// Of the accepted (gated) cases, the fraction that were externally
    /// wrong. `None` if nothing was accepted.
    pub accepted_external_risk: Option<f64>,
    /// An accepted gated decision naming the wrong family (spec §2/§3:
    /// "false merges corrupt identity", the hard go-live gate).
    pub false_merge_count: usize,
    /// `accepted_count` — false merges can only occur among accepted
    /// decisions, so this (not the full corpus size) is the population the
    /// rule-of-three bound is over.
    pub false_merge_n: usize,
    pub false_merge_upper_bound_95: Option<f64>,
    pub guard_activations: Vec<GuardActivation>,
    /// Fraction of gated decisions that ended up `Abstain` — the human
    /// review queue this arm hands off, whether from the raw decider
    /// itself or from a gate rejection.
    pub review_queue_fraction: f64,
    /// Deferred: the mock inferencer used by this task has no realistic
    /// latency to measure — see `crate::arms::mock`'s docs. Wired once a
    /// real endpoint (Task 3) is in the loop.
    pub added_latency_ms: Option<f64>,
}

fn is_wrong(decision: &ArmDecision, expected: &Expected) -> bool {
    *decision != expected.decision
}

pub(crate) fn gold_disagrees(decision: &ArmDecision, expected: &Expected) -> bool {
    use deblob_slm::InferenceDecision::MatchSchema;
    let MatchSchema { schema_id, .. } = decision else {
        return false;
    };
    if let Some(gold) = &expected.gold_schema_id {
        return schema_id != gold;
    }
    if let MatchSchema {
        schema_id: gold_id, ..
    } = &expected.decision
    {
        return schema_id != gold_id;
    }
    // No gold family at all (e.g. expected a NewCandidate/Abstain) — any
    // accepted match here is, by construction, to a wrong family.
    true
}

/// Every [`GateReason`] variant, in a fixed order — used to tally guard
/// activations without requiring `GateReason: Hash` (it derives neither
/// `Hash` nor `Ord`, only `PartialEq`/`Eq`).
const ALL_GATE_REASONS: [GateReason; 8] = [
    GateReason::NoMatchProposed,
    GateReason::RankNotOne,
    GateReason::DistanceExceeded,
    GateReason::MarginTooSmall,
    GateReason::InsufficientObservations,
    GateReason::RelationNotEligible,
    GateReason::DeterministicCompatibilityFailed,
    GateReason::RedactionCollision,
];

pub fn compute_l3(views: &[L3CaseView]) -> GateContainmentMetrics {
    let total = views.len();

    let mut raw_error_count = 0usize;
    let mut raw_error_gate_invoked = 0usize;
    let mut raw_error_gate_blocked = 0usize;
    let mut raw_correct_count = 0usize;
    let mut raw_correct_gate_invoked = 0usize;
    let mut raw_correct_gate_blocked = 0usize;
    let mut accepted_count = 0usize;
    let mut accepted_wrong = 0usize;
    let mut false_merge_count = 0usize;
    let mut review_count = 0usize;
    let mut guard_counts = [0usize; ALL_GATE_REASONS.len()];

    for v in views {
        let raw_gate_invoked = matches!(v.raw_decision, ArmDecision::MatchSchema { .. });
        if is_wrong(v.raw_decision, v.expected) {
            raw_error_count += 1;
            if raw_gate_invoked {
                raw_error_gate_invoked += 1;
                if !v.gate_outcome.would_accept {
                    raw_error_gate_blocked += 1;
                }
            }
        } else {
            raw_correct_count += 1;
            if raw_gate_invoked {
                raw_correct_gate_invoked += 1;
                if !v.gate_outcome.would_accept {
                    raw_correct_gate_blocked += 1;
                }
            }
        }

        if v.gated_decision.is_accepted_match() {
            accepted_count += 1;
            if is_wrong(v.gated_decision, v.expected) {
                accepted_wrong += 1;
            }
            if gold_disagrees(v.gated_decision, v.expected) {
                false_merge_count += 1;
            }
        }

        if matches!(v.gated_decision, ArmDecision::Abstain { .. }) {
            review_count += 1;
        }

        if !v.gate_outcome.would_accept {
            for reason in &v.gate_outcome.gate_reasons {
                if let Some(idx) = ALL_GATE_REASONS.iter().position(|r| r == reason) {
                    guard_counts[idx] += 1;
                }
            }
        }
    }

    let fraction_raw_errors_blocked = if raw_error_gate_invoked == 0 {
        None
    } else {
        Some(raw_error_gate_blocked as f64 / raw_error_gate_invoked as f64)
    };
    let fraction_correct_blocked = if raw_correct_gate_invoked == 0 {
        None
    } else {
        Some(raw_correct_gate_blocked as f64 / raw_correct_gate_invoked as f64)
    };
    let accepted_external_risk = if accepted_count == 0 {
        None
    } else {
        Some(accepted_wrong as f64 / accepted_count as f64)
    };

    let guard_activations: Vec<GuardActivation> = ALL_GATE_REASONS
        .iter()
        .zip(guard_counts.iter())
        .filter(|(_, count)| **count > 0)
        .map(|(reason, count)| GuardActivation {
            reason: *reason,
            count: *count,
        })
        .collect();

    GateContainmentMetrics {
        total_cases: total,
        raw_error_count,
        fraction_raw_errors_blocked,
        raw_correct_count,
        fraction_correct_blocked,
        accepted_coverage: if total == 0 {
            0.0
        } else {
            accepted_count as f64 / total as f64
        },
        accepted_count,
        accepted_external_risk,
        false_merge_count,
        false_merge_n: accepted_count,
        false_merge_upper_bound_95: upper_bound_95(false_merge_count, accepted_count),
        guard_activations,
        review_queue_fraction: if total == 0 {
            0.0
        } else {
            review_count as f64 / total as f64
        },
        added_latency_ms: None,
    }
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

    fn accept_outcome() -> PolicyOutcome {
        PolicyOutcome {
            would_accept: true,
            gate_reasons: vec![],
        }
    }

    fn reject_outcome(reasons: Vec<GateReason>) -> PolicyOutcome {
        PolicyOutcome {
            would_accept: false,
            gate_reasons: reasons,
        }
    }

    #[test]
    fn blocks_a_raw_error_and_reports_it() {
        let expected = expected_match(1);
        let raw_wrong = InferenceDecision::MatchSchema {
            schema_id: schema_id(2),
            relation: Relation::Exact,
        };
        let gated_abstain = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };
        let outcome = reject_outcome(vec![GateReason::DistanceExceeded]);
        let views = vec![L3CaseView {
            raw_decision: &raw_wrong,
            gated_decision: &gated_abstain,
            gate_outcome: &outcome,
            expected: &expected,
        }];
        let metrics = compute_l3(&views);
        assert_eq!(metrics.raw_error_count, 1);
        assert_eq!(metrics.fraction_raw_errors_blocked, Some(1.0));
        assert_eq!(metrics.accepted_count, 0);
        assert_eq!(metrics.guard_activations.len(), 1);
        assert_eq!(
            metrics.guard_activations[0].reason,
            GateReason::DistanceExceeded
        );
    }

    #[test]
    fn over_blocking_a_correct_raw_decision_is_reported_separately() {
        let expected = expected_match(1);
        let raw_correct = InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let gated_abstain = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };
        let outcome = reject_outcome(vec![GateReason::InsufficientObservations]);
        let views = vec![L3CaseView {
            raw_decision: &raw_correct,
            gated_decision: &gated_abstain,
            gate_outcome: &outcome,
            expected: &expected,
        }];
        let metrics = compute_l3(&views);
        assert_eq!(metrics.raw_correct_count, 1);
        assert_eq!(metrics.fraction_correct_blocked, Some(1.0));
        assert_eq!(metrics.fraction_raw_errors_blocked, None);
    }

    #[test]
    fn false_merge_upper_bound_is_the_rule_of_three_over_accepted_n() {
        let expected = expected_match(1);
        let decision = InferenceDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let outcome = accept_outcome();
        let mut views = Vec::new();
        for _ in 0..99 {
            views.push(L3CaseView {
                raw_decision: &decision,
                gated_decision: &decision,
                gate_outcome: &outcome,
                expected: &expected,
            });
        }
        // 99 accepted, all correct -> false_merge_n = 99? we need exactly
        // 100; add one more to make N a round number for the assertion.
        views.push(L3CaseView {
            raw_decision: &decision,
            gated_decision: &decision,
            gate_outcome: &outcome,
            expected: &expected,
        });
        let metrics = compute_l3(&views);
        assert_eq!(metrics.false_merge_count, 0);
        assert_eq!(metrics.false_merge_n, 100);
        assert_eq!(metrics.false_merge_upper_bound_95, Some(0.03));
    }

    #[test]
    fn accepted_external_risk_flags_a_wrong_but_accepted_match() {
        let expected = expected_match(1);
        let raw = InferenceDecision::MatchSchema {
            schema_id: schema_id(2),
            relation: Relation::Exact,
        };
        let outcome = accept_outcome();
        let views = vec![L3CaseView {
            raw_decision: &raw,
            gated_decision: &raw,
            gate_outcome: &outcome,
            expected: &expected,
        }];
        let metrics = compute_l3(&views);
        assert_eq!(metrics.accepted_external_risk, Some(1.0));
        assert_eq!(metrics.false_merge_count, 1);
    }
}
