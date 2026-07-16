//! Layer 2 — raw SLM capability, BEFORE the gate (spec §3): "3-way
//! macro-F1; exact-family accuracy; ... abstention precision+recall;
//! JSON/schema parse rate; wrong-valid rate; Brier score; expected
//! calibration error; externally-labeled risk-coverage curve."
//!
//! Computed over an arm's RAW decisions (in practice, B0 — the ungated
//! SLM) against [`crate::labels::GoldSidecar`]'s external label. The
//! contract (`deblob_slm::InferenceDecision`) carries no self-reported
//! confidence, so Brier/ECE/risk-coverage here use a documented, honest
//! proxy confidence — `1 - clamp(selected_distance, 0, 1)` — derived
//! entirely from deterministic retrieval geometry, never from the model.
//! This is a real limitation of the 3-way contract, not a shortcut: see
//! each field's doc comment.

use deblob_eval::Expected;
use deblob_slm::{FamilyCandidate, InferenceDecision};

use crate::arms::gate::selected_rank_distance;
use crate::arms::ArmDecision;

/// One case's raw arm decision + retrieval geometry + external gold label
/// — everything Layer 2 needs per case.
pub struct L2CaseView<'a> {
    pub decision: &'a ArmDecision,
    pub retrieved: &'a [FamilyCandidate],
    pub expected: &'a Expected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Match,
    New,
    Abstain,
}

fn kind_of(decision: &InferenceDecision) -> Kind {
    match decision {
        InferenceDecision::MatchSchema { .. } => Kind::Match,
        InferenceDecision::NewCandidate { .. } => Kind::New,
        InferenceDecision::Abstain { .. } => Kind::Abstain,
    }
}

fn f1(precision: f64, recall: f64) -> f64 {
    if precision + recall == 0.0 {
        0.0
    } else {
        2.0 * precision * recall / (precision + recall)
    }
}

fn class_f1(views: &[L2CaseView], kind: Kind) -> f64 {
    let tp = views
        .iter()
        .filter(|v| kind_of(v.decision) == kind && kind_of(&v.expected.decision) == kind)
        .count() as f64;
    let predicted = views.iter().filter(|v| kind_of(v.decision) == kind).count() as f64;
    let actual = views
        .iter()
        .filter(|v| kind_of(&v.expected.decision) == kind)
        .count() as f64;
    let precision = if predicted == 0.0 {
        0.0
    } else {
        tp / predicted
    };
    let recall = if actual == 0.0 { 0.0 } else { tp / actual };
    f1(precision, recall)
}

fn confidence_of(decision: &InferenceDecision, retrieved: &[FamilyCandidate]) -> Option<f64> {
    match decision {
        InferenceDecision::MatchSchema { schema_id, .. } => {
            let (_, distance) = selected_rank_distance(retrieved, schema_id);
            distance.map(|d| (1.0 - f64::from(d)).clamp(0.0, 1.0))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RiskCoveragePoint {
    pub coverage: f64,
    pub risk: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct L2Metrics {
    pub total_cases: usize,
    pub macro_f1_3way: f64,
    /// Of cases where the gold decision was `MatchSchema`, the fraction
    /// where the predicted `schema_id` equals the gold family (ignoring
    /// `relation`). `None` if the run has no match-expected case.
    pub exact_family_accuracy: Option<f64>,
    pub abstention_precision: Option<f64>,
    pub abstention_recall: Option<f64>,
    /// Deferred: the mock `SemanticInferencer` used by this task never
    /// crosses a wire boundary (no JSON to fail to parse) — see the module
    /// docs. Becomes meaningful once a real HTTP-backed adapter (Task 3)
    /// is wired in as the raw decider.
    pub json_parse_rate: Option<f64>,
    /// Fraction of ALL cases whose decision is contract-shaped (always
    /// true for an `InferenceDecision` produced in-process — there is no
    /// wire boundary to fail) but semantically WRONG.
    pub wrong_valid_rate: f64,
    pub wrong_valid_count: usize,
    /// `None` if no case proposed `MatchSchema` (Brier is undefined with
    /// no probability observations at all).
    pub brier_score: Option<f64>,
    pub expected_calibration_error: Option<f64>,
    /// Sorted by descending confidence; `coverage` is cumulative fraction
    /// of ALL cases (not just matches) committed to by that point, `risk`
    /// is the cumulative wrong-fraction within that prefix.
    pub risk_coverage_curve: Vec<RiskCoveragePoint>,
}

fn brier_and_ece(pairs: &[(f64, bool)]) -> (Option<f64>, Option<f64>) {
    if pairs.is_empty() {
        return (None, None);
    }
    let brier = pairs
        .iter()
        .map(|(p, correct)| {
            let o = if *correct { 1.0 } else { 0.0 };
            (p - o).powi(2)
        })
        .sum::<f64>()
        / pairs.len() as f64;

    const BINS: usize = 10;
    let mut bin_conf = [0.0f64; BINS];
    let mut bin_correct = [0.0f64; BINS];
    let mut bin_count = [0usize; BINS];
    for (p, correct) in pairs {
        let idx = ((*p * BINS as f64) as usize).min(BINS - 1);
        bin_conf[idx] += p;
        bin_correct[idx] += if *correct { 1.0 } else { 0.0 };
        bin_count[idx] += 1;
    }
    let mut ece = 0.0;
    for i in 0..BINS {
        if bin_count[i] == 0 {
            continue;
        }
        let avg_conf = bin_conf[i] / bin_count[i] as f64;
        let accuracy = bin_correct[i] / bin_count[i] as f64;
        ece += (avg_conf - accuracy).abs() * (bin_count[i] as f64 / pairs.len() as f64);
    }
    (Some(brier), Some(ece))
}

fn risk_coverage_curve(mut pairs: Vec<(f64, bool)>, total: usize) -> Vec<RiskCoveragePoint> {
    if total == 0 || pairs.is_empty() {
        return Vec::new();
    }
    pairs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    let mut wrong_so_far = 0usize;
    let mut out = Vec::with_capacity(pairs.len());
    for (i, (_, correct)) in pairs.iter().enumerate() {
        if !correct {
            wrong_so_far += 1;
        }
        let prefix = i + 1;
        out.push(RiskCoveragePoint {
            coverage: prefix as f64 / total as f64,
            risk: wrong_so_far as f64 / prefix as f64,
        });
    }
    out
}

pub fn compute_l2(views: &[L2CaseView]) -> L2Metrics {
    let total = views.len();

    let macro_f1 = if total == 0 {
        0.0
    } else {
        (class_f1(views, Kind::Match) + class_f1(views, Kind::New) + class_f1(views, Kind::Abstain))
            / 3.0
    };

    let match_expected: Vec<&L2CaseView> = views
        .iter()
        .filter(|v| matches!(v.expected.decision, InferenceDecision::MatchSchema { .. }))
        .collect();
    let exact_family_accuracy = if match_expected.is_empty() {
        None
    } else {
        let correct = match_expected
            .iter()
            .filter(|v| match (&v.decision, &v.expected.decision) {
                (
                    InferenceDecision::MatchSchema { schema_id: a, .. },
                    InferenceDecision::MatchSchema { schema_id: b, .. },
                ) => a == b,
                _ => false,
            })
            .count();
        Some(correct as f64 / match_expected.len() as f64)
    };

    let abstain_actual = views
        .iter()
        .filter(|v| matches!(v.decision, InferenceDecision::Abstain { .. }))
        .count();
    let abstain_should = views
        .iter()
        .filter(|v| matches!(v.expected.decision, InferenceDecision::Abstain { .. }))
        .count();
    let abstain_tp = views
        .iter()
        .filter(|v| {
            matches!(v.decision, InferenceDecision::Abstain { .. })
                && matches!(v.expected.decision, InferenceDecision::Abstain { .. })
        })
        .count();
    let abstention_precision = if abstain_actual == 0 {
        None
    } else {
        Some(abstain_tp as f64 / abstain_actual as f64)
    };
    let abstention_recall = if abstain_should == 0 {
        None
    } else {
        Some(abstain_tp as f64 / abstain_should as f64)
    };

    let wrong_valid_count = views
        .iter()
        .filter(|v| *v.decision != v.expected.decision)
        .count();

    let confidence_pairs: Vec<(f64, bool)> = views
        .iter()
        .filter_map(|v| {
            confidence_of(v.decision, v.retrieved).map(|c| (c, *v.decision == v.expected.decision))
        })
        .collect();
    let (brier_score, expected_calibration_error) = brier_and_ece(&confidence_pairs);
    let risk_coverage_curve = risk_coverage_curve(confidence_pairs, total);

    L2Metrics {
        total_cases: total,
        macro_f1_3way: macro_f1,
        exact_family_accuracy,
        abstention_precision,
        abstention_recall,
        json_parse_rate: None,
        wrong_valid_rate: if total == 0 {
            0.0
        } else {
            wrong_valid_count as f64 / total as f64
        },
        wrong_valid_count,
        brier_score,
        expected_calibration_error,
        risk_coverage_curve,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{FamilyId, SchemaId};
    use deblob_slm::{AbstainCause, Novelty, Relation};

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

    fn expected(decision: InferenceDecision) -> Expected {
        Expected {
            decision,
            gold_schema_id: None,
            gold_rank: None,
            false_merge_trap: false,
            false_split_trap: false,
        }
    }

    #[test]
    fn macro_f1_is_one_when_every_decision_matches_expected_kind() {
        let retrieved = vec![fc(1, 1, 0.0)];
        let d1 = ArmDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let d2 = ArmDecision::NewCandidate {
            novelty: Novelty::Structural,
        };
        let d3 = ArmDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        };
        let e1 = expected(d1.clone());
        let e2 = expected(d2.clone());
        let e3 = expected(d3.clone());
        let views = vec![
            L2CaseView {
                decision: &d1,
                retrieved: &retrieved,
                expected: &e1,
            },
            L2CaseView {
                decision: &d2,
                retrieved: &retrieved,
                expected: &e2,
            },
            L2CaseView {
                decision: &d3,
                retrieved: &retrieved,
                expected: &e3,
            },
        ];
        let metrics = compute_l2(&views);
        assert!((metrics.macro_f1_3way - 1.0).abs() < 1e-9);
        assert_eq!(metrics.wrong_valid_count, 0);
    }

    #[test]
    fn wrong_valid_counts_schema_valid_but_semantically_wrong_decisions() {
        let retrieved = vec![fc(1, 1, 0.0), fc(2, 2, 0.5)];
        let actual = ArmDecision::MatchSchema {
            schema_id: schema_id(2),
            relation: Relation::Exact,
        };
        let gold = expected(ArmDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        });
        let views = vec![L2CaseView {
            decision: &actual,
            retrieved: &retrieved,
            expected: &gold,
        }];
        let metrics = compute_l2(&views);
        assert_eq!(metrics.wrong_valid_count, 1);
        assert_eq!(metrics.wrong_valid_rate, 1.0);
        assert_eq!(metrics.exact_family_accuracy, Some(0.0));
    }

    #[test]
    fn brier_and_ece_are_none_with_no_match_decisions() {
        let retrieved = vec![];
        let d = ArmDecision::Abstain {
            cause: AbstainCause::CandidateMissing,
        };
        let e = expected(d.clone());
        let views = vec![L2CaseView {
            decision: &d,
            retrieved: &retrieved,
            expected: &e,
        }];
        let metrics = compute_l2(&views);
        assert_eq!(metrics.brier_score, None);
        assert_eq!(metrics.expected_calibration_error, None);
        assert!(metrics.risk_coverage_curve.is_empty());
    }

    #[test]
    fn brier_is_zero_for_a_perfectly_calibrated_confident_correct_match() {
        let retrieved = vec![fc(1, 1, 0.0)];
        let d = ArmDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let e = expected(d.clone());
        let views = vec![L2CaseView {
            decision: &d,
            retrieved: &retrieved,
            expected: &e,
        }];
        let metrics = compute_l2(&views);
        // distance 0.0 -> confidence 1.0, correct -> outcome 1.0 -> brier 0.
        assert!((metrics.brier_score.unwrap()).abs() < 1e-9);
        assert_eq!(metrics.risk_coverage_curve.len(), 1);
        assert_eq!(metrics.risk_coverage_curve[0].risk, 0.0);
    }

    #[test]
    fn abstention_precision_recall_match_hand_computed_values() {
        let retrieved = vec![fc(1, 1, 0.0)];
        let should_abstain = expected(ArmDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        });
        let should_match = expected(ArmDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        });
        let did_abstain = ArmDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        };
        let did_match = ArmDecision::MatchSchema {
            schema_id: schema_id(1),
            relation: Relation::Exact,
        };
        let views = vec![
            L2CaseView {
                decision: &did_abstain,
                retrieved: &retrieved,
                expected: &should_abstain,
            },
            L2CaseView {
                decision: &did_abstain,
                retrieved: &retrieved,
                expected: &should_match,
            },
            L2CaseView {
                decision: &did_match,
                retrieved: &retrieved,
                expected: &should_match,
            },
        ];
        let metrics = compute_l2(&views);
        assert_eq!(metrics.abstention_precision, Some(0.5));
        assert_eq!(metrics.abstention_recall, Some(1.0));
    }
}
