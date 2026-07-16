//! Layer 1 — deterministic retrieval capability (spec §3): "recall@1,
//! recall@k of the true family; MRR; ... candidate-set miss rate; broken
//! out by family + observation count. Separates 'gold absent from top-k'
//! (retrieval fault) from model-decision fault. Reported as its own
//! independent gate."
//!
//! Purely a function of retrieval geometry + the external gold label
//! (`EvalCase::expected.{gold_schema_id,gold_rank}`) — it does NOT depend
//! on any arm's decision at all, which is exactly the point: retrieval
//! quality is measured independently of whatever any decider (deterministic
//! or model) does with it.

use std::collections::HashMap;

use deblob_core::id::SchemaId;
use deblob_eval::Expected;
use serde::Serialize;

/// One case's observation count paired with its external gold label — the
/// only two things Layer 1 needs per case.
#[derive(Debug, Clone, Copy)]
pub struct L1CaseView<'a> {
    pub observation_count: u64,
    pub expected: &'a Expected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationBucket {
    /// Below `deblob::shadow::POLICY_MIN_OBSERVATIONS` — never eligible
    /// for a gated accept regardless of retrieval quality.
    BelowFloor,
    Adequate,
    Abundant,
}

fn bucket_of(observation_count: u64) -> ObservationBucket {
    if observation_count < deblob::shadow::POLICY_MIN_OBSERVATIONS {
        ObservationBucket::BelowFloor
    } else if observation_count < 200 {
        ObservationBucket::Adequate
    } else {
        ObservationBucket::Abundant
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BucketRecall {
    pub bucket: ObservationBucket,
    pub n: usize,
    pub recall_at_1: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FamilyRecall {
    pub gold_schema_id: SchemaId,
    pub n: usize,
    pub recall_at_1: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalMetrics {
    pub total_cases: usize,
    pub gold_bearing_cases: usize,
    pub recall_at_1: Option<f64>,
    pub recall_at_3: Option<f64>,
    pub recall_at_5: Option<f64>,
    pub mrr: Option<f64>,
    /// Fraction of gold-bearing cases where the gold family was NOT found
    /// anywhere in the retrieved top-k at all (`gold_rank.is_none()`) — the
    /// mandatory "gold-absent" trap category. Distinct from a wrong-rank
    /// retrieval: this is a total retrieval miss.
    pub candidate_set_miss_rate: Option<f64>,
    pub by_observation_bucket: Vec<BucketRecall>,
    pub by_family: Vec<FamilyRecall>,
}

fn recall_at_k(gold_bearing: &[&Expected], k: u32) -> Option<f64> {
    if gold_bearing.is_empty() {
        return None;
    }
    let hits = gold_bearing
        .iter()
        .filter(|e| e.gold_rank.is_some_and(|r| r <= k))
        .count();
    Some(hits as f64 / gold_bearing.len() as f64)
}

pub fn compute_l1(views: &[L1CaseView]) -> RetrievalMetrics {
    let gold_bearing: Vec<&Expected> = views
        .iter()
        .filter(|v| v.expected.gold_schema_id.is_some())
        .map(|v| v.expected)
        .collect();

    let mrr = if gold_bearing.is_empty() {
        None
    } else {
        let sum: f64 = gold_bearing
            .iter()
            .map(|e| e.gold_rank.map(|r| 1.0 / f64::from(r)).unwrap_or(0.0))
            .sum();
        Some(sum / gold_bearing.len() as f64)
    };

    let miss_rate = if gold_bearing.is_empty() {
        None
    } else {
        let misses = gold_bearing
            .iter()
            .filter(|e| e.gold_rank.is_none())
            .count();
        Some(misses as f64 / gold_bearing.len() as f64)
    };

    let mut bucket_totals: HashMap<ObservationBucket, (usize, usize)> = HashMap::new();
    for v in views {
        if v.expected.gold_schema_id.is_some() {
            let entry = bucket_totals
                .entry(bucket_of(v.observation_count))
                .or_default();
            entry.1 += 1;
            if v.expected.gold_rank.is_some_and(|r| r <= 1) {
                entry.0 += 1;
            }
        }
    }
    let mut by_observation_bucket: Vec<BucketRecall> = [
        ObservationBucket::BelowFloor,
        ObservationBucket::Adequate,
        ObservationBucket::Abundant,
    ]
    .into_iter()
    .filter_map(|bucket| {
        bucket_totals.get(&bucket).map(|(correct, n)| BucketRecall {
            bucket,
            n: *n,
            recall_at_1: if *n == 0 {
                None
            } else {
                Some(*correct as f64 / *n as f64)
            },
        })
    })
    .collect();
    by_observation_bucket.sort_by_key(|b| format!("{:?}", b.bucket));

    let mut family_totals: HashMap<String, (SchemaId, usize, usize)> = HashMap::new();
    for v in views {
        if let Some(gold_id) = &v.expected.gold_schema_id {
            let entry = family_totals
                .entry(gold_id.as_str().to_string())
                .or_insert_with(|| (gold_id.clone(), 0, 0));
            entry.2 += 1;
            if v.expected.gold_rank.is_some_and(|r| r <= 1) {
                entry.1 += 1;
            }
        }
    }
    let mut by_family: Vec<FamilyRecall> = family_totals
        .into_values()
        .map(|(gold_schema_id, correct, n)| FamilyRecall {
            gold_schema_id,
            n,
            recall_at_1: if n == 0 {
                None
            } else {
                Some(correct as f64 / n as f64)
            },
        })
        .collect();
    by_family.sort_by(|a, b| a.gold_schema_id.as_str().cmp(b.gold_schema_id.as_str()));

    RetrievalMetrics {
        total_cases: views.len(),
        gold_bearing_cases: gold_bearing.len(),
        recall_at_1: recall_at_k(&gold_bearing, 1),
        recall_at_3: recall_at_k(&gold_bearing, 3),
        recall_at_5: recall_at_k(&gold_bearing, 5),
        mrr,
        candidate_set_miss_rate: miss_rate,
        by_observation_bucket,
        by_family,
    }
}

// `ObservationBucket` needs `Hash`/`Eq` for the `HashMap` keys above.
impl std::hash::Hash for ObservationBucket {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_slm::{AbstainCause, InferenceDecision};

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn expected_with_rank(gold: Option<u8>, rank: Option<u32>) -> Expected {
        Expected {
            decision: InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            gold_schema_id: gold.map(schema_id),
            gold_rank: rank,
            false_merge_trap: false,
            false_split_trap: false,
        }
    }

    #[test]
    fn recall_and_mrr_match_hand_computed_values() {
        let e1 = expected_with_rank(Some(1), Some(1));
        let e2 = expected_with_rank(Some(1), Some(2));
        let e3 = expected_with_rank(Some(1), Some(3));
        let e4 = expected_with_rank(Some(9), None); // gold-absent (miss)
        let views = vec![
            L1CaseView {
                observation_count: 100,
                expected: &e1,
            },
            L1CaseView {
                observation_count: 100,
                expected: &e2,
            },
            L1CaseView {
                observation_count: 100,
                expected: &e3,
            },
            L1CaseView {
                observation_count: 100,
                expected: &e4,
            },
        ];
        let metrics = compute_l1(&views);
        assert_eq!(metrics.gold_bearing_cases, 4);
        assert_eq!(metrics.recall_at_1, Some(0.25));
        assert_eq!(metrics.recall_at_3, Some(0.75));
        assert_eq!(metrics.recall_at_5, Some(0.75));
        assert_eq!(metrics.candidate_set_miss_rate, Some(0.25));
        let expected_mrr = (1.0 + 0.5 + 1.0 / 3.0 + 0.0) / 4.0;
        assert!((metrics.mrr.unwrap() - expected_mrr).abs() < 1e-9);
    }

    #[test]
    fn no_gold_bearing_cases_yields_none_everywhere() {
        let e = expected_with_rank(None, None);
        let views = vec![L1CaseView {
            observation_count: 10,
            expected: &e,
        }];
        let metrics = compute_l1(&views);
        assert_eq!(metrics.recall_at_1, None);
        assert_eq!(metrics.mrr, None);
        assert_eq!(metrics.candidate_set_miss_rate, None);
    }

    #[test]
    fn observation_bucket_breakdown_separates_below_floor_from_adequate() {
        let e_low = expected_with_rank(Some(1), Some(1));
        let e_high = expected_with_rank(Some(2), None);
        let views = vec![
            L1CaseView {
                observation_count: 5,
                expected: &e_low,
            },
            L1CaseView {
                observation_count: 500,
                expected: &e_high,
            },
        ];
        let metrics = compute_l1(&views);
        let below = metrics
            .by_observation_bucket
            .iter()
            .find(|b| b.bucket == ObservationBucket::BelowFloor)
            .unwrap();
        assert_eq!(below.n, 1);
        assert_eq!(below.recall_at_1, Some(1.0));
        let abundant = metrics
            .by_observation_bucket
            .iter()
            .find(|b| b.bucket == ObservationBucket::Abundant)
            .unwrap();
        assert_eq!(abundant.n, 1);
        assert_eq!(abundant.recall_at_1, Some(0.0));
    }

    #[test]
    fn per_family_breakdown_groups_by_gold_schema_id() {
        let e1 = expected_with_rank(Some(1), Some(1));
        let e2 = expected_with_rank(Some(1), Some(2));
        let views = vec![
            L1CaseView {
                observation_count: 100,
                expected: &e1,
            },
            L1CaseView {
                observation_count: 100,
                expected: &e2,
            },
        ];
        let metrics = compute_l1(&views);
        assert_eq!(metrics.by_family.len(), 1);
        assert_eq!(metrics.by_family[0].gold_schema_id, schema_id(1));
        assert_eq!(metrics.by_family[0].n, 2);
        assert_eq!(metrics.by_family[0].recall_at_1, Some(0.5));
    }
}
