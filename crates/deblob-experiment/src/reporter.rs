//! Markdown + machine-readable JSON rendering of an [`ExperimentReport`]
//! (spec §8: "reporter — emits markdown + machine-readable JSON (for later
//! charting)").

use crate::run::ExperimentReport;

fn pct(v: f64) -> String {
    format!("{:.2}%", v * 100.0)
}

fn pct_opt(v: Option<f64>) -> String {
    v.map(pct).unwrap_or_else(|| "n/a".to_string())
}

/// Renders `report` as the machine-readable JSON blob (every field,
/// via `ExperimentReport`'s `Serialize` impl — nothing hand-picked or
/// summarized away).
pub fn render_json(report: &ExperimentReport) -> serde_json::Value {
    serde_json::to_value(report).unwrap_or(serde_json::Value::Null)
}

/// Renders `report` as a human-readable Markdown document: the headline
/// risk-vs-coverage table (per arm, with false-merge upper bounds) first,
/// then each metric layer.
pub fn render_markdown(report: &ExperimentReport) -> String {
    let mut s = String::new();

    s.push_str(&format!(
        "# Deblob Comparative Experiment Report (seed={}, n={})\n\n",
        report.seed, report.total_cases
    ));

    s.push_str("## Headline: risk vs coverage, per arm\n\n");
    s.push_str("| Arm | Coverage | Accepted N | Accepted external risk | False merges | Upper 95% bound |\n");
    s.push_str("|---|---|---|---|---|---|\n");
    for row in &report.headline {
        s.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            row.id.label(),
            pct(row.accepted_coverage),
            row.accepted_count,
            pct_opt(row.accepted_external_risk),
            row.false_merge_count,
            pct_opt(row.false_merge_upper_bound_95),
        ));
    }
    s.push('\n');

    s.push_str("## Layer 1 — retrieval capability\n\n");
    s.push_str(&format!(
        "recall@1={} recall@3={} recall@5={} MRR={} candidate-set-miss={}\n\n",
        pct_opt(report.retrieval.recall_at_1),
        pct_opt(report.retrieval.recall_at_3),
        pct_opt(report.retrieval.recall_at_5),
        report
            .retrieval
            .mrr
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "n/a".to_string()),
        pct_opt(report.retrieval.candidate_set_miss_rate),
    ));

    s.push_str("## Layer 2 — raw SLM (B0), before the gate\n\n");
    s.push_str(&format!(
        "macro-F1={:.4} exact-family-acc={} abstain P/R={}/{} wrong-valid={} brier={} ECE={}\n\n",
        report.b0_raw.macro_f1_3way,
        pct_opt(report.b0_raw.exact_family_accuracy),
        pct_opt(report.b0_raw.abstention_precision),
        pct_opt(report.b0_raw.abstention_recall),
        pct(report.b0_raw.wrong_valid_rate),
        report
            .b0_raw
            .brier_score
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "n/a".to_string()),
        report
            .b0_raw
            .expected_calibration_error
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "n/a".to_string()),
    ));

    s.push_str("## Layer 3 — gate containment (B0 -> B1)\n\n");
    s.push_str(&format!(
        "raw errors blocked={} correct blocked (over-block cost)={} accepted coverage={} accepted risk={} false merges={}/{} (upper 95%: {})\n\n",
        pct_opt(report.gate_containment_b1.fraction_raw_errors_blocked),
        pct_opt(report.gate_containment_b1.fraction_correct_blocked),
        pct(report.gate_containment_b1.accepted_coverage),
        pct_opt(report.gate_containment_b1.accepted_external_risk),
        report.gate_containment_b1.false_merge_count,
        report.gate_containment_b1.false_merge_n,
        pct_opt(report.gate_containment_b1.false_merge_upper_bound_95),
    ));

    s.push_str("## Layer 3 — gate containment (A0 -> B2, redundancy ablation)\n\n");
    s.push_str(&format!(
        "raw errors blocked={} correct blocked={} accepted coverage={} accepted risk={} false merges={}/{} (upper 95%: {})\n\n",
        pct_opt(report.gate_containment_b2.fraction_raw_errors_blocked),
        pct_opt(report.gate_containment_b2.fraction_correct_blocked),
        pct(report.gate_containment_b2.accepted_coverage),
        pct_opt(report.gate_containment_b2.accepted_external_risk),
        report.gate_containment_b2.false_merge_count,
        report.gate_containment_b2.false_merge_n,
        pct_opt(report.gate_containment_b2.false_merge_upper_bound_95),
    ));

    s.push_str("## Layer 4 — incremental utility, B1 vs A1\n\n");
    let c = &report.b1_vs_a1.contingency;
    s.push_str(&format!(
        "contingency: b_correct/a_wrong={} b_correct/a_abstained={} a_correct/b_wrong_or_abstained={} both_correct={} both_abstained={} (n={})\n",
        c.b_correct_a_wrong,
        c.b_correct_a_abstained,
        c.a_correct_b_wrong_or_abstained,
        c.both_correct,
        c.both_abstained,
        c.n,
    ));
    s.push_str(&format!(
        "human review queue: A={} B={} reduction={}\n",
        pct(report.b1_vs_a1.a_review_queue_fraction),
        pct(report.b1_vs_a1.b_review_queue_fraction),
        pct_opt(report.b1_vs_a1.human_review_reduction),
    ));
    s.push_str(&format!(
        "McNemar statistic={:.4} significant@95={} (n01={}, n10={})\n",
        report.b1_vs_a1.mcnemar.statistic,
        report.b1_vs_a1.mcnemar.significant_at_95,
        report.b1_vs_a1.mcnemar.n01,
        report.b1_vs_a1.mcnemar.n10,
    ));
    if let Some(ci) = &report.b1_vs_a1.bootstrap {
        s.push_str(&format!(
            "Paired bootstrap delta(B-A) accuracy: point={:.4} 95% CI=[{:.4}, {:.4}]\n",
            ci.point_estimate, ci.ci_low_95, ci.ci_high_95
        ));
    }

    s.push_str(
        "\n_Note: this report was produced by the synthetic-corpus, mock-inferencer offline \
         harness (Task 1). Real corpora + live model adapters are later tasks — see the spec's \
         §6b/§5._\n",
    );

    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::{run_experiment, RunConfig};

    #[test]
    fn markdown_report_surfaces_the_headline_table_and_all_five_arms() {
        let report = run_experiment(&RunConfig {
            seed: 5,
            families: 6,
            variants_per_family: 8,
            bootstrap_iterations: 200,
            mock_disagreement_rate: 0.2,
        });
        let md = render_markdown(&report);
        assert!(md.contains("Headline: risk vs coverage"));
        for label in ["A0", "A1", "B0", "B1", "B2"] {
            assert!(md.contains(label), "missing arm {label} in report:\n{md}");
        }
        assert!(md.contains("McNemar"));
        assert!(md.contains("Paired bootstrap") || report.b1_vs_a1.bootstrap.is_none());
    }

    #[test]
    fn json_report_round_trips_every_top_level_field() {
        let report = run_experiment(&RunConfig {
            seed: 5,
            families: 6,
            variants_per_family: 8,
            bootstrap_iterations: 200,
            mock_disagreement_rate: 0.2,
        });
        let json = render_json(&report);
        assert_eq!(json["seed"], serde_json::json!(5));
        assert!(json["headline"].as_array().unwrap().len() == 5);
        assert!(json["retrieval"].is_object());
        assert!(json["b1_vs_a1"]["mcnemar"].is_object());
    }
}
