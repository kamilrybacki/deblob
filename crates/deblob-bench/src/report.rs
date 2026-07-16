//! The bench reporter (spec §3.1): folds one scenario run's producer
//! stats, measurer accumulator, and mgmt-API probe samples into a
//! [`ScenarioResult`], and renders a full [`BenchReport`] (one or more
//! scenarios) as machine-readable JSON + a human summary. Pure — every
//! function here operates on already-collected in-memory data, so this
//! whole module is unit-testable on fixed inputs without a broker or a
//! management API.

use serde::Serialize;

use crate::histogram::LatencySummaryMs;
use crate::measurer::MeasureAccumulator;
use crate::outcome::TagOutcomeCounts;
use crate::prober::ProbeSample;
use crate::producer::ProduceStats;

/// One scenario's full result: everything the human summary and the JSON
/// report both render.
#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub scenario: String,
    pub produced: u64,
    pub produce_errors: u64,
    pub produce_wall_time_secs: f64,
    pub throughput_msgs_per_sec: f64,
    pub tagged_received: u64,
    pub missing_latency_header: u64,
    pub latency: Option<LatencySummaryMs>,
    pub tag_outcomes: TagOutcomeCounts,
    pub probes: Vec<ProbeSample>,
    pub notes: Vec<String>,
}

impl ScenarioResult {
    /// Builds a [`ScenarioResult`] from a completed run's raw
    /// producer/measurer/prober outputs. `throughput_msgs_per_sec` is `0.0`
    /// (never a divide-by-zero panic) when `produce_stats.wall_time` is
    /// zero — e.g. producer construction failed before any send was
    /// attempted.
    pub fn build(
        scenario: &str,
        produce_stats: ProduceStats,
        accumulator: MeasureAccumulator,
        probes: Vec<ProbeSample>,
        notes: Vec<String>,
    ) -> Self {
        let wall_secs = produce_stats.wall_time.as_secs_f64();
        let throughput = if wall_secs > 0.0 {
            produce_stats.sent as f64 / wall_secs
        } else {
            0.0
        };
        ScenarioResult {
            scenario: scenario.to_string(),
            produced: produce_stats.sent,
            produce_errors: produce_stats.send_errors,
            produce_wall_time_secs: wall_secs,
            throughput_msgs_per_sec: throughput,
            tagged_received: accumulator.received,
            missing_latency_header: accumulator.missing_latency,
            latency: accumulator.histogram.summary_ms(),
            tag_outcomes: accumulator.outcomes,
            probes,
            notes,
        }
    }
}

/// A full benchmark run: one or more [`ScenarioResult`]s plus when the
/// report was generated.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    pub generated_at_ms: i64,
    pub scenarios: Vec<ScenarioResult>,
}

impl BenchReport {
    pub fn new(scenarios: Vec<ScenarioResult>) -> Self {
        BenchReport {
            generated_at_ms: now_epoch_ms(),
            scenarios,
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Renders `report` as pretty-printed JSON — the `--out` file (spec §3.1's
/// "machine-readable JSON result"), which the controller renders into the
/// final visual report (spec §6).
pub fn render_json(report: &BenchReport) -> serde_json::Result<String> {
    serde_json::to_string_pretty(report)
}

/// Renders `report` as the human summary printed to stdout.
pub fn render_human(report: &BenchReport) -> String {
    let mut s = String::new();
    s.push_str("=== Deblob k3s Benchmark Report ===\n");
    s.push_str(&format!("scenarios: {}\n\n", report.scenarios.len()));

    for r in &report.scenarios {
        s.push_str(&format!("--- {} ---\n", r.scenario));
        s.push_str(&format!(
            "produced: {} (errors: {})   wall: {:.2}s   throughput: {:.1} msgs/s\n",
            r.produced, r.produce_errors, r.produce_wall_time_secs, r.throughput_msgs_per_sec
        ));
        s.push_str(&format!(
            "tagged received: {}   missing latency header: {}\n",
            r.tagged_received, r.missing_latency_header
        ));
        match &r.latency {
            Some(l) => s.push_str(&format!(
                "tag latency: p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms (n={})\n",
                l.p50_ms, l.p95_ms, l.p99_ms, l.max_ms, l.count
            )),
            None => s.push_str("tag latency: n/a (no tagged messages observed)\n"),
        }
        s.push_str(&format!(
            "tag outcomes: known={} provisional={} unresolved={} malformed={} tombstone={} unknown={}\n",
            r.tag_outcomes.known,
            r.tag_outcomes.provisional,
            r.tag_outcomes.unresolved,
            r.tag_outcomes.malformed,
            r.tag_outcomes.tombstone,
            r.tag_outcomes.unknown
        ));
        if !r.probes.is_empty() {
            s.push_str("mgmt-api probes:\n");
            for p in &r.probes {
                s.push_str(&format!(
                    "  {} -> {} in {:.2}ms\n",
                    p.op, p.status, p.latency_ms
                ));
            }
        }
        for note in &r.notes {
            s.push_str(&format!("note: {note}\n"));
        }
        s.push('\n');
    }

    s
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::histogram::LatencyHistogram;
    use crate::outcome::TagOutcome;

    fn sample_result(scenario: &str) -> ScenarioResult {
        let produce_stats = ProduceStats {
            sent: 100,
            send_errors: 1,
            wall_time: Duration::from_secs(2),
        };
        let mut accumulator = MeasureAccumulator::default();
        for ms in [10u64, 20, 30, 40, 50] {
            accumulator.record(crate::measurer::ProcessedMessage {
                latency_ns: Some(ms * 1_000_000),
                outcome: Some(TagOutcome::Known),
            });
        }
        accumulator.record(crate::measurer::ProcessedMessage {
            latency_ns: None,
            outcome: Some(TagOutcome::Unresolved),
        });
        let probes = vec![ProbeSample {
            op: "list_candidates".to_string(),
            status: 200,
            latency_ms: 12.5,
        }];
        ScenarioResult::build(
            scenario,
            produce_stats,
            accumulator,
            probes,
            vec!["a note".to_string()],
        )
    }

    #[test]
    fn build_computes_throughput_from_sent_and_wall_time() {
        let result = sample_result("throughput");
        assert_eq!(result.produced, 100);
        assert_eq!(result.produce_errors, 1);
        assert_eq!(result.throughput_msgs_per_sec, 50.0); // 100 / 2s
        assert_eq!(result.tagged_received, 6);
        assert_eq!(result.missing_latency_header, 1);
        assert_eq!(result.tag_outcomes.known, 5);
        assert_eq!(result.tag_outcomes.unresolved, 1);
        assert!(result.latency.is_some());
    }

    #[test]
    fn build_never_divides_by_zero_wall_time() {
        let produce_stats = ProduceStats {
            sent: 0,
            send_errors: 0,
            wall_time: Duration::ZERO,
        };
        let result = ScenarioResult::build(
            "empty",
            produce_stats,
            MeasureAccumulator::default(),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(result.throughput_msgs_per_sec, 0.0);
        assert!(result.latency.is_none());
    }

    #[test]
    fn render_json_round_trips_the_expected_shape() {
        let report = BenchReport::new(vec![
            sample_result("throughput"),
            sample_result("malformed"),
        ]);
        let json = render_json(&report).expect("serializes");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");

        assert!(value["generated_at_ms"].as_i64().unwrap() > 0);
        let scenarios = value["scenarios"].as_array().expect("scenarios array");
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0]["scenario"], "throughput");
        assert_eq!(scenarios[0]["produced"], 100);
        assert_eq!(scenarios[0]["tag_outcomes"]["known"], 5);
        assert_eq!(scenarios[0]["probes"][0]["op"], "list_candidates");
        assert!(scenarios[0]["latency"]["p50_ms"].is_number());
        assert_eq!(scenarios[1]["scenario"], "malformed");
    }

    #[test]
    fn render_human_includes_headline_numbers_and_notes() {
        let report = BenchReport::new(vec![sample_result("throughput")]);
        let human = render_human(&report);

        assert!(human.contains("--- throughput ---"));
        assert!(human.contains("produced: 100"));
        assert!(human.contains("throughput: 50.0 msgs/s"));
        assert!(human.contains("tag outcomes: known=5"));
        assert!(human.contains("list_candidates -> 200"));
        assert!(human.contains("note: a note"));
    }

    #[test]
    fn render_human_reports_no_latency_when_histogram_is_empty() {
        let report = BenchReport::new(vec![ScenarioResult::build(
            "empty",
            ProduceStats::default(),
            MeasureAccumulator::default(),
            Vec::new(),
            Vec::new(),
        )]);
        let human = render_human(&report);
        assert!(human.contains("tag latency: n/a"));
    }

    #[test]
    fn latency_histogram_default_is_empty() {
        // Sanity for MeasureAccumulator::default()'s histogram field —
        // proves the report's "n/a" path is reachable via the same
        // construction path scenarios.rs uses on a probing-only run.
        let h = LatencyHistogram::default();
        assert!(h.is_empty());
    }
}
