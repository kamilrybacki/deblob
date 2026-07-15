//! Human + machine eval report (deblob-p2ab Task 7).
//!
//! [`report`] renders a [`crate::metrics::Metrics`] value two ways: a
//! human-readable text report (wrong-valid and false-merge surfaced up
//! top, per the plan: "a readable text report with wrong-valid and
//! false-merge prominently") and a `serde_json::Value` for machine
//! consumption (CI gating, dashboards, Task 8's self-test assertions).

use crate::metrics::Metrics;

fn fmt_pct(rate: f64) -> String {
    format!("{:.2}%", rate * 100.0)
}

fn fmt_pct_opt(rate: Option<f64>) -> String {
    match rate {
        Some(r) => fmt_pct(r),
        None => "n/a (no supporting cases in this run)".to_string(),
    }
}

fn fmt_u64_opt(value: Option<u64>, suffix: &str) -> String {
    match value {
        Some(v) => format!("{v}{suffix}"),
        None => "n/a".to_string(),
    }
}

fn fmt_f64_opt(value: Option<f64>, precision: usize) -> String {
    match value {
        Some(v) => format!("{v:.precision$}"),
        None => "n/a".to_string(),
    }
}

/// Renders `metrics` as a human-readable report and a machine-readable
/// JSON blob (via `Metrics`'s `Serialize` impl — every field, including
/// the documented `None`s, round-trips into the JSON so a CI gate or
/// dashboard sees exactly what the human report describes).
pub fn report(metrics: &Metrics) -> (String, serde_json::Value) {
    let mut s = String::new();

    s.push_str("=== Deblob SLM Eval Report ===\n");
    s.push_str(&format!("Total cases: {}\n\n", metrics.total_cases));

    s.push_str("--- HEADLINE (go/no-go gates; Hermes' Task 5 review) ---\n");
    s.push_str(&format!(
        "Wrong-valid rate:   {:>28}  ({}/{} cases)\n",
        fmt_pct(metrics.wrong_valid_rate),
        metrics.wrong_valid_count,
        metrics.total_cases
    ));
    s.push_str(
        "                    schema-valid (parsed + contract-conformant) BUT semantically \
         WRONG.\n                    Tracked SEPARATELY from schema-valid rate below — a high \
         schema-valid rate\n                    can never mask this number. Go-live gate: ≤ \
         0.5%.\n",
    );
    s.push_str(&format!(
        "False-merge rate:   {:>28}  ({}/{} false-merge-trap cases)\n",
        fmt_pct_opt(metrics.false_merge_rate),
        metrics.false_merge_count,
        metrics.false_merge_trap_count
    ));
    s.push_str(
        "                    an ACCEPTED match to the WRONG family. Hermes' HARD go-live gate: \
         ZERO\n                    false merges — false merges corrupt identity; false splits \
         (below) merely\n                    reduce coverage and are repairable.\n",
    );
    s.push_str(&format!(
        "False-split rate:   {:>28}  ({}/{} false-split-trap cases)\n\n",
        fmt_pct_opt(metrics.false_split_rate),
        metrics.false_split_count,
        metrics.false_split_trap_count
    ));

    s.push_str("--- Parse / schema-valid / semantic correctness ---\n");
    s.push_str(&format!(
        "JSON parse rate:            {}\n",
        fmt_pct(metrics.json_parse_rate)
    ));
    s.push_str(&format!(
        "Schema-valid rate:          {}  (NOT a success criterion on its own)\n",
        fmt_pct(metrics.schema_valid_rate)
    ));
    s.push_str(&format!(
        "Exact semantic accuracy:    {}\n",
        fmt_pct(metrics.exact_semantic_accuracy)
    ));
    s.push_str(&format!(
        "Decision-choice accuracy:   {}\n\n",
        fmt_pct(metrics.decision_choice_accuracy)
    ));

    s.push_str("--- Abstention ---\n");
    s.push_str(&format!(
        "Precision: {}   Recall: {}\n\n",
        fmt_pct_opt(metrics.abstention_precision),
        fmt_pct_opt(metrics.abstention_recall)
    ));

    s.push_str("--- Id-constraint ---\n");
    s.push_str(&format!(
        "Violations (schema_id outside retrieved top-k): {}\n\n",
        metrics.id_constraint_violations
    ));

    s.push_str("--- Retrieval quality ---\n");
    s.push_str(&format!(
        "recall@1: {}   recall@3: {}   recall@5: {}   MRR: {}\n\n",
        fmt_pct_opt(metrics.recall_at_1),
        fmt_pct_opt(metrics.recall_at_3),
        fmt_pct_opt(metrics.recall_at_5),
        fmt_f64_opt(metrics.mrr, 4)
    ));

    s.push_str("--- Relation confusion (expected relation x actual relation, match cases) ---\n");
    if metrics.relation_confusion.is_empty() {
        s.push_str("  (no match-expected cases in this run)\n");
    } else {
        for entry in &metrics.relation_confusion {
            let actual = entry
                .actual
                .map(|r| format!("{r:?}"))
                .unwrap_or_else(|| "NOT-A-MATCH".to_string());
            s.push_str(&format!(
                "  expected={:?} actual={actual} count={}\n",
                entry.expected, entry.count
            ));
        }
    }
    s.push('\n');

    s.push_str("--- Novel family ---\n");
    s.push_str(&format!(
        "Recall: {}   Precision: {}\n\n",
        fmt_pct_opt(metrics.novel_family_recall),
        fmt_pct_opt(metrics.novel_family_precision)
    ));

    s.push_str("--- Gold-absent abstention ---\n");
    s.push_str(&format!(
        "Rate: {}\n\n",
        fmt_pct_opt(metrics.gold_absent_abstention_rate)
    ));

    s.push_str("--- Per-category worst-slice precision ---\n");
    for slice in &metrics.per_category_precision {
        s.push_str(&format!(
            "  {:?}: {} (n={})\n",
            slice.category,
            fmt_pct(slice.precision),
            slice.count
        ));
    }
    s.push_str(&format!(
        "  WORST SLICE: {}{}\n\n",
        fmt_pct_opt(metrics.worst_slice_precision),
        metrics
            .worst_slice_category
            .map(|c| format!(" ({c:?})"))
            .unwrap_or_default()
    ));

    s.push_str("--- Prompt-injection resistance ---\n");
    s.push_str(&format!(
        "{}  ({} injection-flagged case(s) in this run)\n\n",
        fmt_pct_opt(metrics.prompt_injection_resistance),
        metrics.prompt_injection_case_count
    ));

    s.push_str("--- Repair ---\n");
    s.push_str(&format!(
        "Repair rate: {}   Repair success rate: {}   Repairs/accepted: {}\n\n",
        fmt_pct(metrics.repair_rate),
        fmt_pct_opt(metrics.repair_success_rate),
        fmt_f64_opt(metrics.repairs_per_accepted, 3)
    ));

    s.push_str("--- Failure classes ---\n");
    s.push_str(&format!(
        "Timeout: {}   Provider-error: {}   Malformed: {}   Refusal: {}\n\n",
        fmt_pct(metrics.timeout_rate),
        fmt_pct(metrics.provider_error_rate),
        fmt_pct(metrics.malformed_rate),
        fmt_pct_opt(metrics.refusal_rate)
    ));

    s.push_str("--- Latency ---\n");
    s.push_str(&format!(
        "TTFT p50/p95: {} / {}   Total p50/p95: {} / {}\n",
        fmt_u64_opt(metrics.ttft_p50_ms, "ms"),
        fmt_u64_opt(metrics.ttft_p95_ms, "ms"),
        fmt_u64_opt(metrics.total_latency_p50_ms, "ms"),
        fmt_u64_opt(metrics.total_latency_p95_ms, "ms")
    ));
    s.push_str(&format!(
        "Prefill p50: {}   Decode p50: {}\n\n",
        fmt_u64_opt(metrics.prefill_latency_p50_ms, "ms"),
        fmt_u64_opt(metrics.decode_latency_p50_ms, "ms")
    ));

    s.push_str("--- Tokens / cost ---\n");
    s.push_str(&format!(
        "avg request tokens: {}   avg response tokens: {}   cost: {}\n\n",
        fmt_f64_opt(metrics.avg_request_tokens, 1),
        fmt_f64_opt(metrics.avg_response_tokens, 1),
        fmt_f64_opt(metrics.cost, 4)
    ));

    s.push_str("--- Cache / invocation-avoidance ---\n");
    s.push_str(&format!(
        "Cache-hit rate: {}\n\n",
        fmt_pct_opt(metrics.cache_hit_rate)
    ));

    s.push_str("--- Multi-run (filled only if the caller ran a second pass) ---\n");
    s.push_str(&format!(
        "Candidate-order sensitivity: {}   Repeatability: {}\n\n",
        fmt_pct_opt(metrics.candidate_order_sensitivity),
        fmt_pct_opt(metrics.repeatability)
    ));

    s.push_str("--- Deferred (no supporting data in this eval harness; see metrics.rs docs) ---\n");
    s.push_str(
        "  redaction_induced_accuracy_loss, human_label_iaa, \
         counterfactual_unsafe_acceptance_rate: need data outside the golden corpus schema \
         (paired raw/redacted cases, a second human labeler, and the Task 5 shadow log's \
         counterfactual_live_disposition field, respectively).\n",
    );
    s.push_str(
        "  refusal_rate, prefill/decode latency split, cost: no distinguishable signal / no \
         streaming support / no pricing table wired into this harness.\n",
    );
    s.push_str(
        "  prompt/model/quant regression delta: needs a SECOND Metrics run — see \
         crate::metrics::regression_delta.\n",
    );

    let json = serde_json::to_value(metrics).unwrap_or(serde_json::Value::Null);
    (s, json)
}
