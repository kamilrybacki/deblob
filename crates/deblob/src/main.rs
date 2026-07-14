//! Entry point (Task 18): parses CLI args, loads config + env-only secrets,
//! connects to Redis (persistence-gated, spec §6), starts the runtime
//! health probe, wires up the hot-path matcher/cold lane/promoter/metrics,
//! spawns the management API and the Kafka relay, and waits for
//! SIGTERM/SIGINT to drain both before exiting.
//!
//! Kept thin on purpose: every non-trivial decision (config parsing, env
//! overlay, secret validation, the `--unsafe-volatile` → `RedisOpts`
//! mapping) lives in [`deblob::config`], which is unit-tested there
//! without a running Redis/Kafka. This file's own tests cover only the
//! CLI-parsing surface that's specific to `main`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use deblob::api::{self, ApiState, SecretToken};
use deblob::coldlane::ColdLane;
use deblob::config::{self, Config};
use deblob::discovery_consumer::{self, DiscoveryConsumerCfg};
use deblob::matcher::HotMatcher;
use deblob::metrics::{init_tracing, Metrics};
use deblob::policy::Promoter as ConcretePromoter;
use deblob::promote::Promoter as PromoterTrait;
use deblob_core::error::CoreError;
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_kafka::{Relay, RelayCfg};
use deblob_redis::{HealthGate, RedisEvidence, RedisEvidenceOpts, RedisRegistry};
use tokio_util::sync::CancellationToken;

/// How often the runtime persistence health probe re-checks Redis (spec
/// §6): production wants ~10s.
const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Exact-match LRU capacity for [`HotMatcher`]. Not yet exposed as a TOML
/// knob — spec §9's example config doesn't itemize it, so a fixed,
/// generous default is used until a future task promotes it to `Config`.
const HOT_MATCHER_LRU_CAPACITY: usize = 100_000;

#[derive(Parser, Debug)]
#[command(
    name = "deblob",
    about = "Schema-tagging hot/cold-lane relay (spec P1)"
)]
struct Cli {
    /// Path to the TOML config file (non-secret operational knobs only —
    /// see `deblob.example.toml`).
    #[arg(long, default_value = "deblob.toml")]
    config: PathBuf,

    /// Allow connecting to a Redis instance with AOF persistence disabled
    /// (spec §6: "refuse non-persistent Redis unless --unsafe-volatile").
    /// Off by default; an explicit, documented risk acceptance for
    /// ephemeral/test deployments only.
    #[arg(long)]
    unsafe_volatile: bool,
}

/// Every way [`run`] can fail before/during startup or while wiring the
/// runtime together. `Display` on each variant is safe to log verbatim —
/// none of them ever carry a secret VALUE (spec §9).
#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("redis error: {0}")]
    Redis(#[from] CoreError),
    #[error("invalid `management.addr` {addr:?}: {source}")]
    InvalidManagementAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    #[error("management API I/O error: {0}")]
    Io(#[from] std::io::Error),
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // Every `AppError` variant's `Display` is secret-value-free
            // (spec §9) — safe to log as-is.
            tracing::error!(error = %err, "deblob exiting");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let raw_config = Config::load(&cli.config)?;
    let app_config = config::apply_env_overlay(raw_config, &config::process_env);
    let secrets = config::validate_secrets(&config::process_env)?;
    let redis_opts = config::redis_opts(cli.unsafe_volatile);

    tracing::info!(
        config_path = %cli.config.display(),
        unsafe_volatile = cli.unsafe_volatile,
        management_addr = %app_config.management.addr,
        "starting deblob"
    );

    // --- Redis: registry (permanent schema vault) + evidence (candidate
    // lifecycle), both persistence-gated at connect time (spec §6). ---
    let health = HealthGate::new();
    let registry = RedisRegistry::connect(&secrets.redis_url, redis_opts)
        .await
        .map_err(AppError::Redis)?
        .with_health_gate(health.clone());
    let registry: Arc<dyn Registry> = Arc::new(registry);

    let evidence =
        RedisEvidence::connect(&secrets.redis_url, RedisEvidenceOpts::default(), redis_opts)
            .await
            .map_err(AppError::Redis)?;
    let evidence: Arc<dyn EvidenceStore> = Arc::new(evidence);

    // The health gate's background probe needs its OWN connection —
    // `RedisRegistry::conn()` is crate-private to `deblob-redis`, by
    // design (spec §6: the gate is a separate runtime concern from the
    // registry's own publish path, not a shared mutable handle).
    let probe_client = redis::Client::open(secrets.redis_url.as_str())
        .map_err(|e| AppError::Redis(CoreError::RegistryUnavailable(e.to_string())))?;
    let probe_conn = probe_client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| AppError::Redis(CoreError::RegistryUnavailable(e.to_string())))?;
    let probe_handle = health.spawn_probe(probe_conn, HEALTH_PROBE_INTERVAL);

    // --- Hot path / cold lane / promotion / metrics. ---
    let metrics = Metrics::new();
    let matcher = Arc::new(HotMatcher::new(
        registry.clone(),
        HOT_MATCHER_LRU_CAPACITY,
        metrics.clone(),
    ));
    // Fed by the discovery-topic consumer spawned below —
    // `deblob-kafka::Relay` PRODUCES `DiscoveryMsg`s to the discovery
    // topic; `discovery_consumer::run` is what actually drives
    // `ColdLane::ingest` for each one, so candidates accumulate and
    // promotion has something to promote.
    let cold_lane = Arc::new(ColdLane::with_metrics(evidence.clone(), metrics.clone()));
    let promoter: Arc<dyn PromoterTrait> = Arc::new(ConcretePromoter::with_policy(
        registry.clone(),
        evidence.clone(),
        app_config.promotion.to_policy(),
    ));

    // --- Management API: its OWN listen port, separate from Kafka ingest
    // (spec §8). ---
    let api_state = ApiState {
        registry: registry.clone(),
        evidence: evidence.clone(),
        health: health.clone(),
        token: SecretToken::new(&secrets.api_token),
        promoter,
        metrics: metrics.clone(),
    };
    let management_addr: SocketAddr =
        app_config
            .management
            .addr
            .parse()
            .map_err(|source| AppError::InvalidManagementAddr {
                addr: app_config.management.addr.clone(),
                source,
            })?;
    let listener = tokio::net::TcpListener::bind(management_addr).await?;
    let router = api::router(api_state);

    let shutdown = CancellationToken::new();
    let api_shutdown = shutdown.clone();
    let api_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { api_shutdown.cancelled().await })
            .await
    });

    // --- Kafka relay. ---
    let relay_cfg = RelayCfg {
        brokers: secrets.kafka_brokers.clone(),
        group_id: app_config.kafka.group_id.clone(),
        raw_topic: app_config.kafka.raw_topic.clone(),
        tagged_topic: app_config.kafka.tagged_topic.clone(),
        discovery_topic: app_config.kafka.discovery_topic.clone(),
        quarantine_topic: app_config.kafka.quarantine_topic.clone(),
        transactional_id: app_config.kafka.transactional_id.clone(),
        limits: app_config.limits.to_limits(),
        fault: None,
        metrics: metrics.clone(),
        sasl: secrets.kafka_sasl.clone(),
    };
    let relay_shutdown = shutdown.clone();
    let relay_matcher = matcher.clone();
    let relay_handle =
        tokio::spawn(async move { Relay::run(relay_cfg, relay_matcher, relay_shutdown).await });

    // --- Discovery-topic consumer: feeds `cold_lane` from what the relay
    // produces above. Without this task the cold lane never ingests
    // anything. ---
    let discovery_cfg = DiscoveryConsumerCfg {
        brokers: secrets.kafka_brokers.clone(),
        group_id: app_config.kafka.group_id.clone(),
        discovery_topic: app_config.kafka.discovery_topic.clone(),
        limits: app_config.limits.to_limits(),
        sasl: secrets.kafka_sasl.clone(),
    };
    let discovery_shutdown = shutdown.clone();
    let discovery_cold_lane = cold_lane.clone();
    let discovery_handle = tokio::spawn(async move {
        discovery_consumer::run(discovery_cfg, discovery_cold_lane, discovery_shutdown).await
    });

    wait_for_shutdown_signal().await;
    tracing::info!(
        "shutdown signal received; draining relay, discovery consumer, and management API"
    );
    shutdown.cancel();

    // Relay first: spec §3.2 wants any open Kafka transaction aborted/
    // drained before the process considers itself stopped.
    match relay_handle.await {
        Ok(Ok(())) => tracing::info!("relay drained cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "relay exited with error during shutdown"),
        Err(e) => tracing::error!(error = %e, "relay task panicked"),
    }
    match discovery_handle.await {
        Ok(Ok(())) => tracing::info!("discovery consumer drained cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "discovery consumer exited with error during shutdown")
        }
        Err(e) => tracing::error!(error = %e, "discovery consumer task panicked"),
    }
    match api_handle.await {
        Ok(Ok(())) => tracing::info!("management api shut down cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "management api exited with error"),
        Err(e) => tracing::error!(error = %e, "management api task panicked"),
    }

    // The probe loop has no graceful-shutdown protocol of its own (it's a
    // pure `tokio::time::interval` loop) — aborting it here is safe
    // because nothing depends on it completing, only on the `HealthGate`
    // it stops updating.
    probe_handle.abort();

    Ok(())
}

/// Waits for SIGTERM (the orchestrator's stop signal) or SIGINT/Ctrl-C
/// (interactive/dev use), whichever comes first.
#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = sigterm.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_to_deblob_toml_and_safe_redis() {
        let cli = Cli::parse_from(["deblob"]);
        assert_eq!(cli.config, PathBuf::from("deblob.toml"));
        assert!(!cli.unsafe_volatile);
    }

    #[test]
    fn cli_unsafe_volatile_flag_sets_true() {
        let cli = Cli::parse_from(["deblob", "--unsafe-volatile"]);
        assert!(cli.unsafe_volatile);
    }

    #[test]
    fn cli_config_flag_overrides_default_path() {
        let cli = Cli::parse_from(["deblob", "--config", "/etc/deblob/deblob.toml"]);
        assert_eq!(cli.config, PathBuf::from("/etc/deblob/deblob.toml"));
    }

    // The default (no flag) must map to `allow_volatile: false` — this is
    // the same assertion `deblob::config`'s own `volatile_without_flag_is_
    // rejected` test makes on `config::redis_opts` directly; repeated here
    // against the exact value `main`'s wiring passes, so a future edit to
    // `run()` that swaps the argument order can't silently invert it.
    #[test]
    fn default_cli_maps_to_non_volatile_redis_opts() {
        let cli = Cli::parse_from(["deblob"]);
        let opts = config::redis_opts(cli.unsafe_volatile);
        assert!(!opts.allow_volatile);
    }
}
