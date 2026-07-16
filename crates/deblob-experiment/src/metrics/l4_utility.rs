//! Layer 4 — incremental system utility, B1 vs A1 (spec §3, "the primary
//! comparison"): "contingency vs A1 at the same external risk bound: `B
//! correct / A wrong`, `B correct / A abstained`, `A correct / B
//! wrong-or-abstained`, both correct, both abstained. human-review-queue
//! reduction; extra CPU latency per uniquely-resolved event. significance
//! via McNemar / paired bootstrap CIs (A and B see identical events)."

use deblob_eval::Expected;
use deblob_slm::InferenceDecision;

use crate::arms::ArmDecision;
use crate::metrics::stats::{mcnemar, paired_bootstrap_ci, BootstrapCi, McNemarResult};

pub struct L4CaseView<'a> {
    pub a_decision: &'a ArmDecision,
    pub b_decision: &'a ArmDecision,
    pub expected: &'a Expected,
}

/// The 5 named cells from spec §3 Layer 4, plus a residual bucket
/// (`both_wrong_non_abstain`) so every event lands in exactly one bucket —
/// see [`compute_l4`]'s bucket-assignment order for the exact (mutually
/// exclusive, exhaustive) tie-break rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct Contingency {
    pub b_correct_a_wrong: usize,
    pub b_correct_a_abstained: usize,
    pub a_correct_b_wrong_or_abstained: usize,
    pub both_correct: usize,
    pub both_abstained: usize,
    pub both_wrong_non_abstain: usize,
    pub n: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct UtilityReport {
    pub contingency: Contingency,
    pub a_review_queue_fraction: f64,
    pub b_review_queue_fraction: f64,
    /// `(a_review - b_review) / a_review`. `None` if A never sent anything
    /// to review (nothing to reduce).
    pub human_review_reduction: Option<f64>,
    pub mcnemar: McNemarResult,
    /// `None` if there are no paired events or `bootstrap_iterations == 0`.
    pub bootstrap: Option<BootstrapCi>,
    /// Deferred: no realistic per-call latency in this task's mock
    /// inferencer seam — see `crate::arms::mock`'s docs.
    pub extra_latency_ms_per_resolved_event: Option<f64>,
}

fn is_correct(decision: &ArmDecision, expected: &Expected) -> bool {
    decision == &expected.decision
}

fn is_abstain(decision: &ArmDecision) -> bool {
    matches!(decision, InferenceDecision::Abstain { .. })
}

pub fn compute_l4(
    views: &[L4CaseView],
    bootstrap_seed: u64,
    bootstrap_iterations: usize,
) -> UtilityReport {
    let mut contingency = Contingency {
        n: views.len(),
        ..Contingency::default()
    };

    let mut pairs: Vec<(bool, bool)> = Vec::with_capacity(views.len());
    let mut a_review = 0usize;
    let mut b_review = 0usize;

    for v in views {
        let a_correct = is_correct(v.a_decision, v.expected);
        let b_correct = is_correct(v.b_decision, v.expected);
        let a_abstain = is_abstain(v.a_decision);
        let b_abstain = is_abstain(v.b_decision);
        if a_abstain {
            a_review += 1;
        }
        if b_abstain {
            b_review += 1;
        }
        pairs.push((a_correct, b_correct));

        if a_correct && b_correct {
            contingency.both_correct += 1;
        } else if b_correct && !a_correct && a_abstain {
            contingency.b_correct_a_abstained += 1;
        } else if b_correct && !a_correct {
            contingency.b_correct_a_wrong += 1;
        } else if a_correct && !b_correct {
            contingency.a_correct_b_wrong_or_abstained += 1;
        } else if a_abstain && b_abstain {
            contingency.both_abstained += 1;
        } else {
            contingency.both_wrong_non_abstain += 1;
        }
    }

    let total = views.len();
    let a_review_queue_fraction = if total == 0 {
        0.0
    } else {
        a_review as f64 / total as f64
    };
    let b_review_queue_fraction = if total == 0 {
        0.0
    } else {
        b_review as f64 / total as f64
    };
    let human_review_reduction = if a_review == 0 {
        None
    } else {
        Some((a_review as f64 - b_review as f64) / a_review as f64)
    };

    UtilityReport {
        contingency,
        a_review_queue_fraction,
        b_review_queue_fraction,
        human_review_reduction,
        mcnemar: mcnemar(&pairs),
        bootstrap: paired_bootstrap_ci(&pairs, bootstrap_seed, bootstrap_iterations),
        extra_latency_ms_per_resolved_event: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::SchemaId;
    use deblob_slm::{AbstainCause, Relation};

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

    fn m(byte: u8) -> ArmDecision {
        InferenceDecision::MatchSchema {
            schema_id: schema_id(byte),
            relation: Relation::Exact,
        }
    }

    fn abstain() -> ArmDecision {
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        }
    }

    #[test]
    fn contingency_buckets_partition_every_event_exactly_once() {
        let e1 = expected_match(1);
        let a1 = abstain(); // A abstained, wrong (gold is a match)
        let b1 = m(1); // B correct
        let e2 = expected_match(2);
        let a2 = m(2); // A correct
        let b2 = abstain(); // B abstained -> a_correct_b_wrong_or_abstained
        let e3 = expected_match(3);
        let a3 = m(3);
        let b3 = m(3); // both correct
        let e4 = expected_match(4);
        let a4 = abstain();
        let b4 = abstain(); // both abstained, both wrong

        let views = vec![
            L4CaseView {
                a_decision: &a1,
                b_decision: &b1,
                expected: &e1,
            },
            L4CaseView {
                a_decision: &a2,
                b_decision: &b2,
                expected: &e2,
            },
            L4CaseView {
                a_decision: &a3,
                b_decision: &b3,
                expected: &e3,
            },
            L4CaseView {
                a_decision: &a4,
                b_decision: &b4,
                expected: &e4,
            },
        ];
        let report = compute_l4(&views, 1, 100);
        let c = report.contingency;
        assert_eq!(c.b_correct_a_abstained, 1);
        assert_eq!(c.a_correct_b_wrong_or_abstained, 1);
        assert_eq!(c.both_correct, 1);
        assert_eq!(c.both_abstained, 1);
        assert_eq!(c.b_correct_a_wrong, 0);
        assert_eq!(c.both_wrong_non_abstain, 0);
        let sum = c.b_correct_a_wrong
            + c.b_correct_a_abstained
            + c.a_correct_b_wrong_or_abstained
            + c.both_correct
            + c.both_abstained
            + c.both_wrong_non_abstain;
        assert_eq!(sum, c.n);
        assert_eq!(c.n, 4);
    }

    #[test]
    fn human_review_reduction_reflects_fewer_b_abstains_than_a() {
        let e = expected_match(1);
        let a = abstain();
        let b = m(1);
        let views = vec![
            L4CaseView {
                a_decision: &a,
                b_decision: &b,
                expected: &e,
            },
            L4CaseView {
                a_decision: &a,
                b_decision: &b,
                expected: &e,
            },
        ];
        let report = compute_l4(&views, 1, 100);
        assert_eq!(report.a_review_queue_fraction, 1.0);
        assert_eq!(report.b_review_queue_fraction, 0.0);
        assert_eq!(report.human_review_reduction, Some(1.0));
    }

    #[test]
    fn b_dominance_over_a_is_mcnemar_significant() {
        let e = expected_match(1);
        let a = abstain();
        let b = m(1);
        let views: Vec<L4CaseView> = (0..30)
            .map(|_| L4CaseView {
                a_decision: &a,
                b_decision: &b,
                expected: &e,
            })
            .collect();
        let report = compute_l4(&views, 1, 500);
        assert!(report.mcnemar.significant_at_95, "{:?}", report.mcnemar);
        let ci = report.bootstrap.unwrap();
        assert!(ci.point_estimate > 0.0);
    }

    #[test]
    fn a_tie_is_mcnemar_non_significant() {
        let e1 = expected_match(1);
        let a1 = abstain();
        let b1 = m(1);
        let e2 = expected_match(2);
        let a2 = m(2);
        let b2 = abstain();

        let mut views = Vec::new();
        for _ in 0..10 {
            views.push(L4CaseView {
                a_decision: &a1,
                b_decision: &b1,
                expected: &e1,
            });
            views.push(L4CaseView {
                a_decision: &a2,
                b_decision: &b2,
                expected: &e2,
            });
        }
        let report = compute_l4(&views, 1, 500);
        assert!(!report.mcnemar.significant_at_95, "{:?}", report.mcnemar);
    }
}
