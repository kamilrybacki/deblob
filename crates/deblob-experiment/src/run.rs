//! The runner (spec §8): "loads corpus → runs arms → emits per-arm tables
//! + the headline risk-coverage plot data ... Deterministic seed."
//!
//! This task's runner is scoped to the SYNTHETIC corpus
//! (`deblob_eval::generate_corpus`, seeded) and the mock inferencer seam
//! (`crate::arms::mock::HeuristicMockInferencer`) — real corpus ingestion
//! (spec §6b) and live model adapters (spec §5) are later tasks.
//! [`run_experiment`] is a pure function of [`RunConfig`]: the same config
//! (in particular, the same `seed`) always produces byte-identical
//! [`ExperimentReport`] JSON — see the `run` module's determinism test.

use std::sync::Arc;

use deblob_eval::{generate_corpus, GenerateConfig};
use deblob_slm::SemanticInferencer;
use serde::Serialize;

use crate::arms::deterministic::{A0RetrievalOnly, A1FairDeterministic};
use crate::arms::gate::GatedArm;
use crate::arms::mock::HeuristicMockInferencer;
use crate::arms::semantic::SemanticArm;
use crate::arms::{Arm, ArmDecision, ArmId};
use crate::labels::{split_corpus, GoldSidecar};
use crate::metrics::l3_gate::gold_disagrees;
use crate::metrics::stats::upper_bound_95;
use crate::metrics::{
    compute_l1, compute_l2, compute_l3, compute_l4, GateContainmentMetrics, L1CaseView, L2CaseView,
    L2Metrics, L3CaseView, L4CaseView, RetrievalMetrics, UtilityReport,
};

#[derive(Debug, Clone, Copy)]
pub struct RunConfig {
    pub seed: u64,
    pub families: usize,
    pub variants_per_family: usize,
    pub bootstrap_iterations: usize,
    /// Fraction of eligible cases where `HeuristicMockInferencer`
    /// deliberately diverges from the plain structural heuristic — see
    /// that type's docs. `0.0` makes B0/B1 behave identically to A0/A1's
    /// structural logic (a useful sanity-check config: B1≈B2 exactly).
    pub mock_disagreement_rate: f64,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            seed: 42,
            families: 12,
            variants_per_family: 8,
            bootstrap_iterations: 2_000,
            mock_disagreement_rate: 0.2,
        }
    }
}

/// One row of the headline risk-vs-coverage table (spec §4/§8): the FINAL
/// decision an arm produces, scored against the external label, with no
/// notion of "raw vs gated" — just what that arm actually outputs.
#[derive(Debug, Clone, Serialize)]
pub struct ArmReport {
    pub id: ArmId,
    pub total_cases: usize,
    pub accepted_coverage: f64,
    pub accepted_count: usize,
    pub accepted_external_risk: Option<f64>,
    pub false_merge_count: usize,
    pub false_merge_upper_bound_95: Option<f64>,
}

fn arm_report(id: ArmId, decisions: &[ArmDecision], sidecars: &[GoldSidecar]) -> ArmReport {
    let total = decisions.len();
    let mut accepted = 0usize;
    let mut accepted_wrong = 0usize;
    let mut false_merge = 0usize;
    for (decision, sidecar) in decisions.iter().zip(sidecars.iter()) {
        if decision.is_accepted_match() {
            accepted += 1;
            if *decision != sidecar.expected.decision {
                accepted_wrong += 1;
            }
            if gold_disagrees(decision, &sidecar.expected) {
                false_merge += 1;
            }
        }
    }
    ArmReport {
        id,
        total_cases: total,
        accepted_coverage: if total == 0 {
            0.0
        } else {
            accepted as f64 / total as f64
        },
        accepted_count: accepted,
        accepted_external_risk: if accepted == 0 {
            None
        } else {
            Some(accepted_wrong as f64 / accepted as f64)
        },
        false_merge_count: false_merge,
        false_merge_upper_bound_95: upper_bound_95(false_merge, accepted),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentReport {
    pub seed: u64,
    pub total_cases: usize,
    /// Layer 1 — retrieval capability, independent of any arm.
    pub retrieval: RetrievalMetrics,
    /// Layer 2 — raw (ungated) SLM capability, over B0.
    pub b0_raw: L2Metrics,
    /// Layer 3 — gate containment, B0 (raw) vs B1 (gated).
    pub gate_containment_b1: GateContainmentMetrics,
    /// Layer 3 — gate containment, A0 (raw det-top1) vs B2 (gated
    /// det-top1) — the redundancy ablation's own containment profile.
    pub gate_containment_b2: GateContainmentMetrics,
    /// Layer 4 — the primary comparison, B1 vs A1.
    pub b1_vs_a1: UtilityReport,
    /// The headline risk-vs-coverage table, one row per arm — A0, A1, B0,
    /// B1, B2 side by side (spec §10 acceptance: "the `B2` ablation is
    /// reported alongside `B1`").
    pub headline: Vec<ArmReport>,
}

/// Runs the full A0/A1/B0/B1/B2 comparison over a seeded synthetic corpus
/// and computes every applicable metric layer. See the module docs for the
/// determinism contract.
pub fn run_experiment(cfg: &RunConfig) -> ExperimentReport {
    let generated = generate_corpus(&GenerateConfig {
        families: cfg.families,
        variants_per_family: cfg.variants_per_family,
        seed: cfg.seed,
    });
    let corpus = generated.cases;
    let (inputs, sidecars) = split_corpus(&corpus);

    let a0 = A0RetrievalOnly;
    let a1 = A1FairDeterministic::default();
    let mock: Arc<dyn SemanticInferencer> = Arc::new(HeuristicMockInferencer::new(
        cfg.seed,
        cfg.mock_disagreement_rate,
    ));
    let b0 = SemanticArm::new(ArmId::B0, Arc::clone(&mock));
    let b1 = GatedArm::new(
        ArmId::B1,
        Box::new(SemanticArm::new(ArmId::B1, Arc::clone(&mock))),
    );
    let b2 = GatedArm::new(ArmId::B2, Box::new(A0RetrievalOnly));

    let a0_decisions: Vec<ArmDecision> = inputs.iter().map(|i| a0.decide(i)).collect();
    let a1_decisions: Vec<ArmDecision> = inputs.iter().map(|i| a1.decide(i)).collect();
    let b0_decisions: Vec<ArmDecision> = inputs.iter().map(|i| b0.decide(i)).collect();
    let b1_pairs: Vec<_> = inputs.iter().map(|i| b1.decide_with_gate(i)).collect();
    let b2_pairs: Vec<_> = inputs.iter().map(|i| b2.decide_with_gate(i)).collect();
    let b1_decisions: Vec<ArmDecision> = b1_pairs.iter().map(|(d, _)| d.clone()).collect();
    let b2_decisions: Vec<ArmDecision> = b2_pairs.iter().map(|(d, _)| d.clone()).collect();

    // -- Layer 1: retrieval, independent of any arm.
    let l1_views: Vec<L1CaseView> = inputs
        .iter()
        .zip(sidecars.iter())
        .map(|(input, sidecar)| L1CaseView {
            observation_count: input.candidate.observation_count,
            expected: &sidecar.expected,
        })
        .collect();
    let retrieval = compute_l1(&l1_views);

    // -- Layer 2: raw SLM (B0) vs external label.
    let l2_views: Vec<L2CaseView> = (0..corpus.len())
        .map(|i| L2CaseView {
            decision: &b0_decisions[i],
            retrieved: &inputs[i].retrieved,
            expected: &sidecars[i].expected,
        })
        .collect();
    let b0_raw = compute_l2(&l2_views);

    // -- Layer 3: gate containment. B0 (raw) -> B1 (gated).
    let l3_b1_views: Vec<L3CaseView> = (0..corpus.len())
        .map(|i| L3CaseView {
            raw_decision: &b0_decisions[i],
            gated_decision: &b1_decisions[i],
            gate_outcome: &b1_pairs[i].1,
            expected: &sidecars[i].expected,
        })
        .collect();
    let gate_containment_b1 = compute_l3(&l3_b1_views);

    // A0 (raw) -> B2 (gated) — the redundancy ablation's own containment.
    let l3_b2_views: Vec<L3CaseView> = (0..corpus.len())
        .map(|i| L3CaseView {
            raw_decision: &a0_decisions[i],
            gated_decision: &b2_decisions[i],
            gate_outcome: &b2_pairs[i].1,
            expected: &sidecars[i].expected,
        })
        .collect();
    let gate_containment_b2 = compute_l3(&l3_b2_views);

    // -- Layer 4: the primary comparison, B1 vs A1, identical events.
    let l4_views: Vec<L4CaseView> = (0..corpus.len())
        .map(|i| L4CaseView {
            a_decision: &a1_decisions[i],
            b_decision: &b1_decisions[i],
            expected: &sidecars[i].expected,
        })
        .collect();
    let b1_vs_a1 = compute_l4(&l4_views, cfg.seed, cfg.bootstrap_iterations);

    let headline = vec![
        arm_report(ArmId::A0, &a0_decisions, &sidecars),
        arm_report(ArmId::A1, &a1_decisions, &sidecars),
        arm_report(ArmId::B0, &b0_decisions, &sidecars),
        arm_report(ArmId::B1, &b1_decisions, &sidecars),
        arm_report(ArmId::B2, &b2_decisions, &sidecars),
    ];

    ExperimentReport {
        seed: cfg.seed,
        total_cases: corpus.len(),
        retrieval,
        b0_raw,
        gate_containment_b1,
        gate_containment_b2,
        b1_vs_a1,
        headline,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_cfg(seed: u64) -> RunConfig {
        RunConfig {
            seed,
            families: 6,
            variants_per_family: 8,
            bootstrap_iterations: 200,
            mock_disagreement_rate: 0.2,
        }
    }

    #[test]
    fn every_arm_reports_a_row_and_b1_b2_sit_side_by_side() {
        let report = run_experiment(&small_cfg(1));
        assert_eq!(report.headline.len(), 5);
        let ids: Vec<ArmId> = report.headline.iter().map(|r| r.id).collect();
        assert_eq!(
            ids,
            vec![ArmId::A0, ArmId::A1, ArmId::B0, ArmId::B1, ArmId::B2]
        );
        assert_eq!(report.total_cases, 6 * 8);
        assert_eq!(report.retrieval.total_cases, report.total_cases);
        assert_eq!(report.b0_raw.total_cases, report.total_cases);
    }

    #[test]
    fn same_seed_produces_byte_identical_report_json() {
        let a = run_experiment(&small_cfg(7));
        let b = run_experiment(&small_cfg(7));
        let a_json = serde_json::to_string(&a).unwrap();
        let b_json = serde_json::to_string(&b).unwrap();
        assert_eq!(a_json, b_json);
    }

    #[test]
    fn different_seed_produces_a_different_report() {
        let a = run_experiment(&small_cfg(1));
        let b = run_experiment(&small_cfg(2));
        let a_json = serde_json::to_string(&a).unwrap();
        let b_json = serde_json::to_string(&b).unwrap();
        assert_ne!(a_json, b_json);
    }

    #[test]
    fn zero_disagreement_makes_the_raw_slm_mirror_a1_exactly() {
        // With the mock inferencer's disagreement rate at 0.0, its
        // decision logic mirrors A1FairDeterministic's thresholds exactly
        // (same distance/margin/obs-floor branches, same relation choice)
        // — demonstrating the harness CAN show "the SLM adds nothing"
        // (spec §4's honest no-lift report) when there is genuinely
        // nothing to add.
        let generated = generate_corpus(&GenerateConfig {
            families: 6,
            variants_per_family: 8,
            seed: 3,
        });
        let (inputs, _sidecars) = split_corpus(&generated.cases);
        let a1 = A1FairDeterministic::default();
        let mock: Arc<dyn SemanticInferencer> = Arc::new(HeuristicMockInferencer::new(3, 0.0));
        let b0 = SemanticArm::new(ArmId::B0, mock);

        for input in &inputs {
            assert_eq!(a1.decide(input), b0.decide(input));
        }
    }

    #[test]
    fn b2_gate_containment_is_computed_alongside_b1() {
        let report = run_experiment(&small_cfg(3));
        assert_eq!(
            report.gate_containment_b2.total_cases,
            report.gate_containment_b1.total_cases
        );
    }
}
