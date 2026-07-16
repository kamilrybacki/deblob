//! The scenario runner (spec §4): builds the record stream for the
//! requested scenario, drives it through the producer, measures the tagged
//! topic, and — for the scenarios that need it — times the management API,
//! folding everything into one [`crate::report::ScenarioResult`].
//!
//! [`run_scenario`] needs a LIVE broker (and, for `Semantic`/`Neighbors`/
//! `ColdLane`'s promote probe, a live management API) — it is exercised by
//! the controller's Docker-backed integration run, not this crate's unit
//! suite. [`build_stream`] and [`promote_body`] are pure and unit-tested
//! below.

use std::time::Duration;

use clap::ValueEnum;

use crate::config::{PayloadSize, SyntheticConfig};
use crate::fixtures::{real_world_stream, RealWorldKind};
use crate::generator::generate;
use crate::header::now_ns;
use crate::measurer::{measure_topic, MeasureAccumulator};
use crate::prober::{MgmtProber, ProbeSample};
use crate::producer::{build_producer, produce_stream, KeyDistribution, ProduceStats, RateLimit};
use crate::record::GeneratedRecord;
use crate::report::ScenarioResult;

/// Which spec §4 scenario to run. `RealWorld` is `--scenario real-world`
/// on the CLI (clap's kebab-case rename).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum ScenarioKind {
    /// §4.1: hot-path throughput/latency sweep.
    Throughput,
    /// §4.2: malformed/quarantine.
    Malformed,
    /// §4.3: cold-lane discovery (+ promote probe).
    ColdLane,
    /// §4.4: P2-D semantic annotation.
    Semantic,
    /// §4.4: P2-D semantic-neighbors query cost.
    Neighbors,
    /// §4.7: real-world mix.
    RealWorld,
}

impl ScenarioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ScenarioKind::Throughput => "throughput",
            ScenarioKind::Malformed => "malformed",
            ScenarioKind::ColdLane => "cold-lane",
            ScenarioKind::Semantic => "semantic",
            ScenarioKind::Neighbors => "neighbors",
            ScenarioKind::RealWorld => "real-world",
        }
    }
}

/// CLI-facing payload-size choice; maps 1:1 onto
/// [`crate::config::PayloadSize`] (kept separate so `clap::ValueEnum`'s
/// derive doesn't have to live on the already-published generator type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lowercase")]
pub enum PayloadArg {
    Small,
    Medium,
    Large,
}

impl From<PayloadArg> for PayloadSize {
    fn from(p: PayloadArg) -> Self {
        match p {
            PayloadArg::Small => PayloadSize::Small,
            PayloadArg::Medium => PayloadSize::Medium,
            PayloadArg::Large => PayloadSize::Large,
        }
    }
}

/// The resolved, run-ready parameters for one scenario invocation —
/// decoupled from `clap`'s `RunArgs` (`main.rs`) so this half is
/// independently testable/reusable.
#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub scenario: ScenarioKind,
    pub brokers: String,
    pub mgmt_url: Option<String>,
    pub api_token: Option<String>,
    pub raw_topic: String,
    pub tagged_topic: String,
    pub group_id: String,
    pub seed: u64,
    pub distinct_schemas: usize,
    pub count: usize,
    pub payload: PayloadSize,
    pub malformed_pct: f64,
    pub drift_rate: f64,
    pub optional_field_churn: f64,
    pub rate: RateLimit,
    pub key_distribution: KeyDistribution,
    pub measure_timeout: Duration,
    /// How long the measurer waits after the LAST observed tagged message
    /// before deciding the run is done (as opposed to `measure_timeout`,
    /// the hard overall backstop). See `crate::measurer::measure_stop_reason`.
    pub measure_idle_timeout: Duration,
    /// `k` for the `neighbors` scenario's `GET semantic-neighbors?k=`.
    pub neighbors_k: usize,
    /// `cold-lane` scenario: candidate id to promote. `None` skips the
    /// promote probe (a real run discovers this from a prior
    /// `list_candidates` call or a previous scenario's output — this CLI
    /// runs one scenario per invocation, so the controller wires the id
    /// through).
    pub target_candidate_id: Option<String>,
    /// `cold-lane` scenario: `"new"` or `"existing:<fam_id>"`.
    pub promote_family: String,
    pub promote_reason: String,
    /// `semantic`/`neighbors` scenario: target schema id.
    pub target_schema_id: Option<String>,
    /// `semantic` scenario: the full `PutSemanticRequest` JSON body.
    /// `deblob-bench` never guesses controlled-vocabulary content (spec
    /// §2's governance API validates a registered vocabulary this harness
    /// has no independent knowledge of) — the caller supplies it.
    pub semantic_body: Option<serde_json::Value>,
}

/// Builds the record stream for `cfg.scenario`. `RealWorld` cycles the full
/// fixture corpus (spec §4 item 7); every other scenario is the seeded
/// synthetic generator, parameterized by `cfg`'s knobs — `Malformed` and
/// `ColdLane` differ from `Throughput` only in which knob the CALLER set
/// non-zero before building this config (`malformed_pct` for `Malformed`;
/// a fresh, high `distinct_schemas` pool for `ColdLane`'s novel shapes), so
/// this function does not special-case them.
pub fn build_stream(cfg: &ScenarioConfig) -> Box<dyn Iterator<Item = GeneratedRecord> + Send> {
    if cfg.scenario == ScenarioKind::RealWorld {
        let kinds = [
            RealWorldKind::GitHubWebhook,
            RealWorldKind::K8sEvent,
            RealWorldKind::CloudEvent,
        ];
        Box::new(real_world_stream(&kinds, cfg.count, cfg.seed))
    } else {
        let synthetic = SyntheticConfig {
            seed: cfg.seed,
            distinct_schemas: cfg.distinct_schemas,
            optional_field_churn: cfg.optional_field_churn,
            drift_rate: cfg.drift_rate,
            malformed_pct: cfg.malformed_pct,
            payload_bytes: cfg.payload,
            count: cfg.count,
        };
        Box::new(generate(&synthetic))
    }
}

/// Builds the `POST .../promote` request body: `family` is either the
/// literal `"new"` or `"existing:<fam_id>"`, matching
/// `deblob::promote::FamilyChoice`'s wire representation (a bare string
/// for `New`, `{"existing": "fam_..."}` for `Existing`) without this crate
/// depending on that type.
pub fn promote_body(family: &str, reason: &str) -> serde_json::Value {
    let family_value = match family.strip_prefix("existing:") {
        Some(fam_id) => serde_json::json!({ "existing": fam_id }),
        None => serde_json::json!("new"),
    };
    serde_json::json!({
        "family": family_value,
        "name": serde_json::Value::Null,
        "reason": reason,
    })
}

/// Runs one scenario end to end: produce `cfg`'s stream onto the raw
/// topic while CONCURRENTLY measuring the tagged topic, optionally probe
/// the management API, and fold the result into a [`ScenarioResult`].
/// Every failure mode (producer construction, the measurer erroring, a
/// missing mgmt-API target) degrades to a `notes` entry rather than a
/// panic or an `Err` — a benchmark run must always finish with a report,
/// even a partial one, so the operator running it unattended on a k3s
/// worker never gets nothing back.
///
/// The measurer is spawned FIRST and runs alongside `produce_stream`,
/// rather than after it: a produce-then-consume ordering leaves every
/// early-tagged message sitting in the topic, already tagged, until the
/// whole produce loop finishes and the consumer even subscribes — which
/// measures "how long the backlog waited," not real end-to-end tag
/// latency (this was the smoke run's p50=40s exceeding its own 12s
/// produce wall-time). Each run also gets a fresh, per-run consumer group
/// id, so no earlier run's group state can ever cause this one to skip
/// messages.
pub async fn run_scenario(cfg: &ScenarioConfig) -> ScenarioResult {
    let mut notes = Vec::new();

    let producer = match build_producer(&cfg.brokers) {
        Ok(p) => Some(p),
        Err(err) => {
            notes.push(format!("producer construction failed: {err}"));
            None
        }
    };

    let group_id = format!("{}-{}", cfg.group_id, now_ns());
    let measure_handle = {
        let brokers = cfg.brokers.clone();
        let tagged_topic = cfg.tagged_topic.clone();
        // `cfg.count` (not `produce_stats.sent`, which doesn't exist yet
        // since production hasn't run) is the measurer's target — it's
        // known upfront and matches `build_stream`'s guaranteed output
        // length (see `build_stream_for_synthetic_scenarios_honors_count`
        // below). A shortfall from send errors still ends the run
        // promptly via the idle timeout rather than waiting out the full
        // deadline for messages that will never arrive.
        let expected = cfg.count as u64;
        let deadline = cfg.measure_timeout;
        let idle_timeout = cfg.measure_idle_timeout;
        tokio::spawn(async move {
            measure_topic(
                &brokers,
                &group_id,
                &tagged_topic,
                expected,
                deadline,
                idle_timeout,
            )
            .await
        })
    };
    // Yield once so the spawned measurer task gets a chance to actually
    // start (build its consumer, subscribe, begin polling) before this
    // task starts flooding the raw topic. Not a correctness requirement —
    // a fresh, `earliest`-reset consumer group never skips messages
    // regardless of exactly when it subscribes — just reduces how much of
    // the earliest backlog gets read in a single post-hoc catch-up burst.
    tokio::task::yield_now().await;

    let mut produce_stats = ProduceStats::default();
    if let Some(producer) = &producer {
        let stream = build_stream(cfg);
        produce_stats = produce_stream(
            producer,
            &cfg.raw_topic,
            stream,
            cfg.key_distribution,
            cfg.rate,
        )
        .await;
    }

    let accumulator = match measure_handle.await {
        Ok(Ok(acc)) => acc,
        Ok(Err(err)) => {
            notes.push(format!("measurer failed: {err}"));
            MeasureAccumulator::default()
        }
        Err(join_err) => {
            notes.push(format!("measurer task panicked: {join_err}"));
            MeasureAccumulator::default()
        }
    };

    if accumulator.received != produce_stats.sent {
        notes.push(format!(
            "measurer received {} of {} sent messages (--count was {}); see \
             tagged_expected/tagged_received and tag_outcomes for detail — a \
             mismatch after the idle timeout means either the run is still \
             draining or some sent records never reached the tagged topic",
            accumulator.received, produce_stats.sent, cfg.count
        ));
    }

    let probes = run_probes(cfg, &mut notes).await;

    ScenarioResult::build(
        cfg.scenario.as_str(),
        produce_stats,
        accumulator,
        probes,
        notes,
    )
}

/// Runs the management-API probes `cfg.scenario` needs (spec §4 items 3/4).
/// Pushes a human-readable note (never panics/errors out) whenever a probe
/// is skipped for lack of `mgmt_url`/`api_token`/a target id.
async fn run_probes(cfg: &ScenarioConfig, notes: &mut Vec<String>) -> Vec<ProbeSample> {
    if !matches!(
        cfg.scenario,
        ScenarioKind::ColdLane | ScenarioKind::Semantic | ScenarioKind::Neighbors
    ) {
        return Vec::new();
    }

    let (Some(base_url), Some(token)) = (&cfg.mgmt_url, &cfg.api_token) else {
        notes.push(
            "scenario calls for mgmt-API probing but --mgmt-url and/or DEBLOB_API_TOKEN \
             were not set; probing skipped"
                .to_string(),
        );
        return Vec::new();
    };

    let prober = match MgmtProber::new(base_url.clone(), token.clone(), Duration::from_secs(10)) {
        Ok(p) => p,
        Err(err) => {
            notes.push(format!("prober construction failed: {err}"));
            return Vec::new();
        }
    };

    let mut samples = Vec::new();
    match cfg.scenario {
        ScenarioKind::ColdLane => {
            match prober.list_candidates("provisional").await {
                Ok(s) => samples.push(s),
                Err(err) => notes.push(format!("list_candidates probe failed: {err}")),
            }
            if let Some(cand_id) = &cfg.target_candidate_id {
                let body = promote_body(&cfg.promote_family, &cfg.promote_reason);
                match prober.promote_candidate(cand_id, &body).await {
                    Ok(s) => samples.push(s),
                    Err(err) => notes.push(format!("promote_candidate probe failed: {err}")),
                }
            } else {
                notes.push(
                    "cold-lane scenario: no --target-candidate-id supplied, promote probe skipped"
                        .to_string(),
                );
            }
        }
        ScenarioKind::Semantic => match (&cfg.target_schema_id, &cfg.semantic_body) {
            (Some(sch_id), Some(body)) => match prober.put_semantic(sch_id, body).await {
                Ok(s) => samples.push(s),
                Err(err) => notes.push(format!("put_semantic probe failed: {err}")),
            },
            _ => notes.push(
                "semantic scenario: --target-schema-id and --semantic-body-file are both \
                 required to probe PUT .../semantic; skipped"
                    .to_string(),
            ),
        },
        ScenarioKind::Neighbors => match &cfg.target_schema_id {
            Some(sch_id) => match prober.semantic_neighbors(sch_id, cfg.neighbors_k).await {
                Ok(s) => samples.push(s),
                Err(err) => notes.push(format!("semantic_neighbors probe failed: {err}")),
            },
            None => notes.push(
                "neighbors scenario: no --target-schema-id supplied, probe skipped".to_string(),
            ),
        },
        _ => {}
    }

    samples
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg(scenario: ScenarioKind) -> ScenarioConfig {
        ScenarioConfig {
            scenario,
            brokers: "localhost:9092".to_string(),
            mgmt_url: None,
            api_token: None,
            raw_topic: "events.raw".to_string(),
            tagged_topic: "events.tagged".to_string(),
            group_id: "deblob-bench".to_string(),
            seed: 7,
            distinct_schemas: 10,
            count: 20,
            payload: PayloadSize::Small,
            malformed_pct: 0.0,
            drift_rate: 0.0,
            optional_field_churn: 0.0,
            rate: RateLimit::MaxThroughput,
            key_distribution: KeyDistribution::None,
            measure_timeout: Duration::from_secs(1),
            measure_idle_timeout: Duration::from_millis(200),
            neighbors_k: 10,
            target_candidate_id: None,
            promote_family: "new".to_string(),
            promote_reason: "test".to_string(),
            target_schema_id: None,
            semantic_body: None,
        }
    }

    #[test]
    fn build_stream_for_real_world_yields_exactly_count_records() {
        let cfg = base_cfg(ScenarioKind::RealWorld);
        let records: Vec<_> = build_stream(&cfg).collect();
        assert_eq!(records.len(), 20);
    }

    #[test]
    fn build_stream_for_synthetic_scenarios_honors_count_and_is_deterministic() {
        for kind in [
            ScenarioKind::Throughput,
            ScenarioKind::Malformed,
            ScenarioKind::ColdLane,
            ScenarioKind::Semantic,
            ScenarioKind::Neighbors,
        ] {
            let cfg = base_cfg(kind);
            let first: Vec<_> = build_stream(&cfg).map(|r| r.bytes).collect();
            let second: Vec<_> = build_stream(&cfg).map(|r| r.bytes).collect();
            assert_eq!(first.len(), 20, "{kind:?} produced wrong count");
            assert_eq!(first, second, "{kind:?} stream must be deterministic");
        }
    }

    #[test]
    fn malformed_scenario_config_actually_produces_malformed_records() {
        let mut cfg = base_cfg(ScenarioKind::Malformed);
        cfg.malformed_pct = 1.0;
        cfg.count = 5;
        let records: Vec<_> = build_stream(&cfg).collect();
        assert!(records
            .iter()
            .all(|r| matches!(r.expected, crate::record::RecordKind::Malformed)));
    }

    #[test]
    fn promote_body_new_family_is_bare_string() {
        let body = promote_body("new", "why");
        assert_eq!(body["family"], serde_json::json!("new"));
        assert_eq!(body["reason"], "why");
        assert!(body["name"].is_null());
    }

    #[test]
    fn promote_body_existing_family_is_a_single_key_object() {
        let body = promote_body("existing:fam_abc123", "why");
        assert_eq!(
            body["family"],
            serde_json::json!({ "existing": "fam_abc123" })
        );
    }

    #[test]
    fn scenario_kind_as_str_matches_the_cli_kebab_case_value() {
        assert_eq!(ScenarioKind::ColdLane.as_str(), "cold-lane");
        assert_eq!(ScenarioKind::RealWorld.as_str(), "real-world");
        assert_eq!(ScenarioKind::Throughput.as_str(), "throughput");
    }

    #[tokio::test]
    async fn run_probes_notes_when_scenario_needs_probing_but_mgmt_is_unset() {
        let cfg = base_cfg(ScenarioKind::ColdLane);
        let mut notes = Vec::new();
        let probes = run_probes(&cfg, &mut notes).await;
        assert!(probes.is_empty());
        assert!(notes.iter().any(|n| n.contains("mgmt-url")));
    }

    #[tokio::test]
    async fn run_probes_is_a_noop_for_throughput_scenario() {
        let cfg = base_cfg(ScenarioKind::Throughput);
        let mut notes = Vec::new();
        let probes = run_probes(&cfg, &mut notes).await;
        assert!(probes.is_empty());
        assert!(notes.is_empty());
    }
}
