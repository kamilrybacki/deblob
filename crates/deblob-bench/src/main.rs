//! `deblob-bench` binary — the k3s-benchmark client driving Deblob's relay
//! from outside: produces a generated/real-world JSON stream onto the
//! ingest topic, measures end-to-end tag latency/throughput off the tagged
//! topic, and (for `semantic`/`neighbors`/`cold-lane`) times the
//! management API. Spec `docs/superpowers/specs/
//! 2026-07-16-deblob-k3s-benchmark.md` §3.1/§4/§5.
//!
//! ## Deferred integration
//!
//! Running a scenario needs a LIVE Redpanda/Kafka broker (and, for
//! `semantic`/`neighbors`/`cold-lane`'s promote probe, a live Deblob
//! management API) — there is no Docker/testcontainers test for this
//! binary in this task; see `deblob_bench`'s crate-level docs for the
//! unit-vs-integration split.
//!
//! ## Secrets
//!
//! `DEBLOB_API_TOKEN` is read ONLY from the environment, never from a CLI
//! flag (a flag would land in shell history / `ps`), and is never printed
//! or included in the JSON report.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use deblob_bench::measurer::DEFAULT_IDLE_TIMEOUT;
use deblob_bench::producer::{KeyDistribution, RateLimit};
use deblob_bench::report::{self, BenchReport};
use deblob_bench::scenarios::{self, PayloadArg, ScenarioConfig, ScenarioKind};

const ENV_API_TOKEN: &str = "DEBLOB_API_TOKEN";

/// Deblob k3s-benchmark client. See `docs/superpowers/specs/
/// 2026-07-16-deblob-k3s-benchmark.md` for the scenarios this drives.
#[derive(Debug, Parser)]
#[command(name = "deblob-bench", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run one benchmark scenario end to end and emit a report.
    Run(RunArgs),
}

#[derive(Debug, Args, Clone)]
struct RunArgs {
    /// Kafka/Redpanda bootstrap servers, e.g. `redpanda:9092`.
    #[arg(long)]
    brokers: String,

    /// Deblob management API base URL, e.g. `http://deblob-mgmt:8081`.
    /// Required (alongside `DEBLOB_API_TOKEN`) for the `semantic`/
    /// `neighbors`/`cold-lane` scenarios' mgmt-API probes; unused
    /// otherwise.
    #[arg(long)]
    mgmt_url: Option<String>,

    /// Which spec §4 scenario to run.
    #[arg(long, value_enum)]
    scenario: ScenarioKind,

    /// Number of distinct synthetic schema families (ignored by
    /// `real-world`, which always cycles the fixed fixture corpus).
    #[arg(long, default_value_t = 1000)]
    distinct_schemas: usize,

    /// Total number of records to produce.
    #[arg(long, default_value_t = 100_000)]
    count: usize,

    /// Target serialized payload size class (ignored by `real-world`).
    #[arg(long, value_enum, default_value_t = PayloadArg::Medium)]
    payload: PayloadArg,

    #[arg(long, default_value = "events.raw")]
    raw_topic: String,

    #[arg(long, default_value = "events.tagged")]
    tagged_topic: String,

    #[arg(long, default_value = "deblob-bench")]
    group_id: String,

    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Probability a synthetic record is malformed (spec §4.2's
    /// `malformed` scenario knob).
    #[arg(long, default_value_t = 0.0)]
    malformed_pct: f64,

    /// Probability a synthetic record is a compatible-drift variant (spec
    /// §4.3's cold-lane novel-shape knob).
    #[arg(long, default_value_t = 0.0)]
    drift_rate: f64,

    #[arg(long, default_value_t = 0.0)]
    optional_field_churn: f64,

    /// Target produce rate in msgs/s; omit for max throughput.
    #[arg(long)]
    rate: Option<f64>,

    /// Cycle this many synthetic partition keys; omit (or `0`) for keyless
    /// (null-key) production.
    #[arg(long)]
    key_pool: Option<u32>,

    /// Hard overall backstop: how long the measurer waits in total before
    /// giving up regardless of progress, in seconds.
    #[arg(long, default_value_t = 60)]
    measure_timeout_secs: u64,

    /// How long the measurer waits after the LAST observed tagged message
    /// before deciding the run is done, in seconds. The primary stop
    /// condition in practice — `measure_timeout_secs` is only the
    /// backstop for a stalled/unreachable broker.
    #[arg(long, default_value_t = DEFAULT_IDLE_TIMEOUT.as_secs())]
    measure_idle_timeout_secs: u64,

    /// `k` for the `neighbors` scenario's `GET semantic-neighbors?k=`.
    #[arg(long, default_value_t = 10)]
    neighbors_k: usize,

    /// `cold-lane` scenario: candidate id to promote via the mgmt API
    /// (skips the promote probe if omitted).
    #[arg(long)]
    target_candidate_id: Option<String>,

    /// `cold-lane` scenario: `new` or `existing:<fam_id>`.
    #[arg(long, default_value = "new")]
    promote_family: String,

    #[arg(long, default_value = "k3s-benchmark scenario probe")]
    promote_reason: String,

    /// `semantic`/`neighbors` scenario: target schema id.
    #[arg(long)]
    target_schema_id: Option<String>,

    /// `semantic` scenario: path to a JSON file holding the
    /// `PUT .../semantic` request body (`{"metadata": ..., "reason": ...}`)
    /// — `deblob-bench` never guesses controlled-vocabulary content.
    #[arg(long)]
    semantic_body_file: Option<PathBuf>,

    /// Write the machine-readable JSON report here (in addition to the
    /// human summary printed to stdout).
    #[arg(long)]
    out: Option<PathBuf>,
}

fn key_distribution(key_pool: Option<u32>) -> KeyDistribution {
    match key_pool {
        Some(n) if n > 0 => KeyDistribution::RoundRobin(n),
        _ => KeyDistribution::None,
    }
}

fn rate_limit(rate: Option<f64>) -> RateLimit {
    match rate {
        Some(r) if r > 0.0 => RateLimit::PerSecond(r),
        _ => RateLimit::MaxThroughput,
    }
}

fn load_semantic_body(path: &PathBuf) -> Result<serde_json::Value, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("failed to parse {}: {e}", path.display()))
}

/// Resolves `args` (+ the env-only API token) into a [`ScenarioConfig`].
/// Never reads `std::env` itself beyond what the caller passes in as
/// `env_api_token` — kept a pure function so it's testable without
/// touching process-global environment state (mirrors `deblob-eval`'s
/// `resolve_slm_config`).
fn resolve_scenario_config(
    args: &RunArgs,
    env_api_token: Option<String>,
) -> Result<ScenarioConfig, String> {
    let semantic_body = match &args.semantic_body_file {
        Some(path) => Some(load_semantic_body(path)?),
        None => None,
    };
    Ok(ScenarioConfig {
        scenario: args.scenario,
        brokers: args.brokers.clone(),
        mgmt_url: args.mgmt_url.clone(),
        api_token: env_api_token,
        raw_topic: args.raw_topic.clone(),
        tagged_topic: args.tagged_topic.clone(),
        group_id: args.group_id.clone(),
        seed: args.seed,
        distinct_schemas: args.distinct_schemas,
        count: args.count,
        payload: args.payload.into(),
        malformed_pct: args.malformed_pct,
        drift_rate: args.drift_rate,
        optional_field_churn: args.optional_field_churn,
        rate: rate_limit(args.rate),
        key_distribution: key_distribution(args.key_pool),
        measure_timeout: Duration::from_secs(args.measure_timeout_secs),
        measure_idle_timeout: Duration::from_secs(args.measure_idle_timeout_secs),
        neighbors_k: args.neighbors_k,
        target_candidate_id: args.target_candidate_id.clone(),
        promote_family: args.promote_family.clone(),
        promote_reason: args.promote_reason.clone(),
        target_schema_id: args.target_schema_id.clone(),
        semantic_body,
    })
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let Command::Run(args) = cli.command;

    let cfg = match resolve_scenario_config(&args, std::env::var(ENV_API_TOKEN).ok()) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "running scenario {:?} against brokers={} raw_topic={} tagged_topic={} count={} \
         distinct_schemas={} payload={:?} (mgmt-api token {})",
        cfg.scenario,
        cfg.brokers,
        cfg.raw_topic,
        cfg.tagged_topic,
        cfg.count,
        cfg.distinct_schemas,
        cfg.payload,
        if cfg.api_token.is_some() {
            "configured, not logged"
        } else {
            "not set"
        }
    );

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to start async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    let result = runtime.block_on(scenarios::run_scenario(&cfg));
    let bench_report = BenchReport::new(vec![result]);

    println!("{}", report::render_human(&bench_report));

    if let Some(path) = &args.out {
        match report::render_json(&bench_report) {
            Ok(json) => {
                if let Err(err) = std::fs::write(path, json) {
                    eprintln!("failed to write JSON report to {}: {err}", path.display());
                    return ExitCode::FAILURE;
                }
                println!("wrote JSON report to {}", path.display());
            }
            Err(err) => {
                eprintln!("failed to render JSON report: {err}");
                return ExitCode::FAILURE;
            }
        }
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_run_subcommand_with_the_readme_example_flags() {
        let cli = Cli::try_parse_from([
            "deblob-bench",
            "run",
            "--brokers",
            "redpanda:9092",
            "--mgmt-url",
            "http://deblob-mgmt:8081",
            "--scenario",
            "throughput",
            "--distinct-schemas",
            "1000",
            "--count",
            "100000",
            "--payload",
            "medium",
        ])
        .expect("parses");
        let Command::Run(args) = cli.command;
        assert_eq!(args.brokers, "redpanda:9092");
        assert_eq!(args.mgmt_url.as_deref(), Some("http://deblob-mgmt:8081"));
        assert_eq!(args.scenario, ScenarioKind::Throughput);
        assert_eq!(args.distinct_schemas, 1000);
        assert_eq!(args.count, 100_000);
        assert_eq!(args.payload, PayloadArg::Medium);
    }

    #[test]
    fn scenario_kind_accepts_kebab_case_real_world_and_cold_lane() {
        for (flag, expected) in [
            ("real-world", ScenarioKind::RealWorld),
            ("cold-lane", ScenarioKind::ColdLane),
            ("malformed", ScenarioKind::Malformed),
            ("semantic", ScenarioKind::Semantic),
            ("neighbors", ScenarioKind::Neighbors),
        ] {
            let cli = Cli::try_parse_from([
                "deblob-bench",
                "run",
                "--brokers",
                "b:9092",
                "--scenario",
                flag,
            ])
            .unwrap_or_else(|e| panic!("scenario {flag} failed to parse: {e}"));
            let Command::Run(args) = cli.command;
            assert_eq!(args.scenario, expected);
        }
    }

    #[test]
    fn missing_required_brokers_flag_fails_to_parse() {
        let err = Cli::try_parse_from(["deblob-bench", "run", "--scenario", "throughput"])
            .expect_err("brokers is required");
        assert!(err.to_string().contains("brokers"));
    }

    #[test]
    fn missing_required_scenario_flag_fails_to_parse() {
        let err = Cli::try_parse_from(["deblob-bench", "run", "--brokers", "b:9092"])
            .expect_err("scenario is required");
        assert!(err.to_string().contains("scenario"));
    }

    #[test]
    fn defaults_match_the_documented_values() {
        let cli = Cli::try_parse_from([
            "deblob-bench",
            "run",
            "--brokers",
            "b:9092",
            "--scenario",
            "throughput",
        ])
        .expect("parses with defaults");
        let Command::Run(args) = cli.command;
        assert_eq!(args.distinct_schemas, 1000);
        assert_eq!(args.count, 100_000);
        assert_eq!(args.payload, PayloadArg::Medium);
        assert_eq!(args.raw_topic, "events.raw");
        assert_eq!(args.tagged_topic, "events.tagged");
        assert_eq!(args.seed, 42);
        assert_eq!(args.rate, None);
        assert_eq!(args.measure_timeout_secs, 60);
        assert_eq!(
            args.measure_idle_timeout_secs,
            DEFAULT_IDLE_TIMEOUT.as_secs()
        );
    }

    #[test]
    fn key_distribution_maps_pool_size() {
        assert_eq!(key_distribution(None), KeyDistribution::None);
        assert_eq!(key_distribution(Some(0)), KeyDistribution::None);
        assert_eq!(key_distribution(Some(4)), KeyDistribution::RoundRobin(4));
    }

    #[test]
    fn rate_limit_maps_optional_rate() {
        assert_eq!(rate_limit(None), RateLimit::MaxThroughput);
        assert_eq!(rate_limit(Some(0.0)), RateLimit::MaxThroughput);
        assert!(matches!(rate_limit(Some(500.0)), RateLimit::PerSecond(r) if r == 500.0));
    }

    fn base_run_args() -> RunArgs {
        RunArgs {
            brokers: "b:9092".to_string(),
            mgmt_url: None,
            scenario: ScenarioKind::Throughput,
            distinct_schemas: 10,
            count: 100,
            payload: PayloadArg::Small,
            raw_topic: "events.raw".to_string(),
            tagged_topic: "events.tagged".to_string(),
            group_id: "deblob-bench".to_string(),
            seed: 1,
            malformed_pct: 0.0,
            drift_rate: 0.0,
            optional_field_churn: 0.0,
            rate: None,
            key_pool: None,
            measure_timeout_secs: 60,
            measure_idle_timeout_secs: 10,
            neighbors_k: 10,
            target_candidate_id: None,
            promote_family: "new".to_string(),
            promote_reason: "reason".to_string(),
            target_schema_id: None,
            semantic_body_file: None,
            out: None,
        }
    }

    #[test]
    fn resolve_scenario_config_carries_env_api_token_never_a_cli_flag() {
        // Compile-time proof by construction: `RunArgs` has no `api_token`
        // field at all (see the struct above) — there is no flag to even
        // pass one via.
        let args = base_run_args();
        let cfg = resolve_scenario_config(&args, Some("shh".to_string())).expect("resolves");
        assert_eq!(cfg.api_token.as_deref(), Some("shh"));
    }

    #[test]
    fn resolve_scenario_config_without_semantic_body_file_leaves_it_none() {
        let args = base_run_args();
        let cfg = resolve_scenario_config(&args, None).expect("resolves");
        assert!(cfg.semantic_body.is_none());
    }

    #[test]
    fn resolve_scenario_config_errors_clearly_on_missing_semantic_body_file() {
        let mut args = base_run_args();
        args.semantic_body_file = Some(PathBuf::from("/nonexistent/path/does-not-exist.json"));
        let err = resolve_scenario_config(&args, None).expect_err("missing file must error");
        assert!(err.contains("failed to read"));
    }
}
