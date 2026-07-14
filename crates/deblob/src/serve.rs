//! Reusable runtime wiring (Task 18/19): connects Redis (registry +
//! evidence, persistence-gated), starts the runtime health probe, wires up
//! the hot-path matcher/cold lane/promoter/metrics, and spawns the
//! management API, the Kafka relay, and the discovery-topic consumer —
//! everything the `deblob` binary's `main.rs` needs at runtime.
//!
//! Split out of `main.rs` (Task 19) specifically so an end-to-end
//! acceptance test can call the SAME wiring the binary uses, instead of a
//! test-only stand-in that risks drifting from production: `serve` takes an
//! already-parsed [`Config`]/[`Secrets`]/[`RedisOpts`] and an
//! externally-owned [`CancellationToken`] — the caller (production `main`,
//! or a test) decides how those are built and when to cancel the token;
//! `serve` itself only owns what happens in between.
//!
//! `serve` returns once `shutdown` is cancelled AND every spawned task has
//! drained (relay first, per spec §3.2, then the discovery consumer, then
//! the management API) — the exact sequencing Task 18's `main.rs` run()
//! used to inline before this split.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use deblob_core::error::CoreError;
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_kafka::{Relay, RelayCfg};
use deblob_redis::{HealthGate, RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use tokio_util::sync::CancellationToken;

use crate::api::{self, ApiState, SecretToken};
use crate::coldlane::ColdLane;
use crate::config::{Config, Secrets};
use crate::discovery_consumer::{self, DiscoveryConsumerCfg};
use crate::matcher::HotMatcher;
use crate::metrics::Metrics;
use crate::policy::Promoter as ConcretePromoter;
use crate::promote::Promoter as PromoterTrait;

/// How often the runtime persistence health probe re-checks Redis (spec
/// §6): production wants ~10s.
const HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// Exact-match LRU capacity for [`HotMatcher`]. Not yet exposed as a TOML
/// knob — spec §9's example config doesn't itemize it, so a fixed,
/// generous default is used until a future task promotes it to `Config`.
const HOT_MATCHER_LRU_CAPACITY: usize = 100_000;

/// Every way [`serve`] can fail before/during startup or while wiring the
/// runtime together. `Display` on each variant is safe to log verbatim —
/// none of them ever carry a secret VALUE (spec §9).
#[derive(Debug, thiserror::Error)]
pub enum AppError {
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

/// Wires up and runs the full deblob runtime — Redis registry/evidence,
/// the runtime health probe, the hot-path matcher, the cold lane +
/// promoter, the management API, the Kafka relay, and the discovery-topic
/// consumer — until `shutdown` is cancelled, then drains every spawned
/// task (relay, discovery consumer, management API, in that order) before
/// returning.
///
/// Callers own `shutdown`: production's `main.rs` cancels it on
/// SIGTERM/SIGINT; an end-to-end test cancels it once its assertions are
/// done, or simply drops the `serve` future's `JoinHandle` at the end of
/// the test (the containers going away underneath it is enough to end the
/// process either way).
pub async fn serve(
    app_config: Config,
    secrets: Secrets,
    redis_opts: RedisOpts,
    shutdown: CancellationToken,
) -> Result<(), AppError> {
    tracing::info!(
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
    // registry's own publish path, not a shared mutable handle). It uses
    // the same `ConnectionManager` tuning as the registry/evidence
    // connections (Task 19 fix) so the probe itself recovers after a Redis
    // outage instead of permanently reporting the last state it saw before
    // the connection died.
    let probe_client = redis::Client::open(secrets.redis_url.as_str())
        .map_err(|e| AppError::Redis(CoreError::RegistryUnavailable(e.to_string())))?;
    let probe_conn = probe_client
        .get_connection_manager_with_config(deblob_redis::connection_manager_config())
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

    shutdown.cancelled().await;
    tracing::info!(
        "shutdown signal received; draining relay, discovery consumer, and management API"
    );

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
