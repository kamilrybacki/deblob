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
use deblob_slm::{HttpInferencer, SemanticInferencer, SlmHttpConfig};
use tokio_util::sync::CancellationToken;

use crate::api::{self, ApiState, SecretToken};
use crate::coldlane::ColdLane;
use crate::config::{Config, Secrets, SlmConfig};
use crate::discovery_consumer::{self, DiscoveryConsumerCfg};
use crate::matcher::HotMatcher;
use crate::metrics::Metrics;
use crate::policy::{Promoter as ConcretePromoter, PromotionPolicy};
use crate::promote::Promoter as PromoterTrait;
use crate::shadow::{
    run_shadow_sweep, ModelMeta, RedisShadowLog, ShadowClassifier, ShadowConfig, ShadowLog,
};

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
    /// Defensive only — [`crate::config::validate_secrets`] already
    /// guarantees `secrets.slm_api_token.is_some()` whenever
    /// `app_config.slm.enabled` is `true`, so this should be unreachable
    /// via `main.rs`'s normal startup path. Surfaced as an error (never a
    /// panic) in case a future caller ever constructs `Secrets` by hand
    /// with that invariant violated (e.g. a test).
    #[error("[slm].enabled is true but no DEBLOB_SLM_API_TOKEN was supplied")]
    MissingSlmToken,
}

/// Everything [`serve`] needs to construct the SLM shadow lane, computed
/// PURELY from `[slm]` config + secrets — no I/O, no Redis/HTTP client
/// construction. Kept separate from [`serve`] itself so the
/// enabled/disabled WIRING DECISION is unit-testable without Docker/Redis
/// — see the `shadow_lane_wiring_*` tests below.
#[derive(Debug)]
struct ShadowLaneWiring {
    slm_http: SlmHttpConfig,
    shadow_config: ShadowConfig,
    model: ModelMeta,
    sweep_interval: Duration,
}

/// `Ok(None)` iff `[slm].enabled` is `false` — in that case [`serve`]
/// constructs NO `HttpInferencer`, NO `RedisShadowLog`, and spawns NO
/// sweep task, so behavior is unchanged from before Task 5b. `Ok(Some(_))`
/// iff enabled and the required secret is present. `Err` only in the
/// defensive case described on [`AppError::MissingSlmToken`].
fn build_shadow_lane_wiring(
    slm: &SlmConfig,
    secrets: &Secrets,
) -> Result<Option<ShadowLaneWiring>, AppError> {
    if !slm.enabled {
        return Ok(None);
    }
    let api_token = secrets
        .slm_api_token
        .clone()
        .ok_or(AppError::MissingSlmToken)?;

    Ok(Some(ShadowLaneWiring {
        slm_http: SlmHttpConfig {
            base_url: slm.base_url.clone(),
            model: slm.model.clone(),
            api_token: Some(api_token),
            timeout_ms: slm.timeout_ms,
            max_concurrency: slm.max_concurrency,
        },
        shadow_config: ShadowConfig {
            eligibility: PromotionPolicy {
                min_samples: slm.min_samples,
                min_age_ms: slm.min_window_ms as i64,
            },
            // Keep the logged `InferenceBudget::timeout_ms` consistent
            // with the timeout actually enforced by `HttpInferencer`'s
            // `reqwest` client (built from `slm_http.timeout_ms` above) —
            // otherwise a `ShadowDecision`'s recorded budget would lie
            // about how long the call was really allowed to take.
            inference_timeout_ms: slm.timeout_ms,
            ..ShadowConfig::default()
        },
        model: ModelMeta {
            model_id: slm.model.clone(),
            ..ModelMeta::default()
        },
        sweep_interval: Duration::from_millis(slm.sweep_interval_ms),
    }))
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

    // --- SLM shadow lane (Task 5b): OFF unless `[slm].enabled` is true.
    // When disabled (the default), no `HttpInferencer`, no
    // `RedisShadowLog`, and no sweep task are constructed at all — this
    // block is then a no-op and `shadow_handle` stays `None`. ---
    let shadow_handle = match build_shadow_lane_wiring(&app_config.slm, &secrets)? {
        None => None,
        Some(wiring) => {
            let inferencer: Arc<dyn SemanticInferencer> =
                Arc::new(HttpInferencer::new(wiring.slm_http));
            let shadow_log: Arc<dyn ShadowLog> = Arc::new(
                RedisShadowLog::connect(&secrets.redis_url)
                    .await
                    .map_err(AppError::Redis)?,
            );
            let classifier = Arc::new(ShadowClassifier::new(
                evidence.clone(),
                registry.clone(),
                inferencer,
                shadow_log,
                wiring.model,
                wiring.shadow_config,
            ));
            let sweep_evidence = evidence.clone();
            let sweep_shutdown = shutdown.clone();
            let sweep_interval = wiring.sweep_interval;
            Some(tokio::spawn(async move {
                run_shadow_sweep(classifier, sweep_evidence, sweep_interval, sweep_shutdown).await;
            }))
        }
    };

    shutdown.cancelled().await;
    tracing::info!(
        "shutdown signal received; draining relay, discovery consumer, shadow sweep (if enabled), and management API"
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
    if let Some(shadow_handle) = shadow_handle {
        match shadow_handle.await {
            Ok(()) => tracing::info!("shadow sweep drained cleanly"),
            Err(e) => tracing::error!(error = %e, "shadow sweep task panicked"),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn secrets(slm_api_token: Option<&str>) -> Secrets {
        Secrets {
            api_token: "test-token".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            kafka_brokers: "localhost:9092".to_string(),
            kafka_sasl: None,
            slm_api_token: slm_api_token.map(str::to_string),
        }
    }

    /// `[slm].enabled=false` (the documented default) must wire up NO
    /// shadow lane at all — the exact behavior `serve()` had before Task
    /// 5b. This is the "unit-level assertion on the wiring decision"
    /// substitute for a bounded Docker sweep test (see the Task 5b
    /// report): it proves the enabled/disabled DECISION without spinning
    /// up Redis/an HTTP endpoint.
    #[test]
    fn shadow_lane_disabled_by_default_wires_nothing() {
        let slm = SlmConfig::default();
        assert!(!slm.enabled);

        let wiring =
            build_shadow_lane_wiring(&slm, &secrets(None)).expect("disabled slm must never error");
        assert!(
            wiring.is_none(),
            "disabled [slm] must construct no HttpInferencer/RedisShadowLog/sweep task"
        );
    }

    /// `[slm].enabled=true` with the token present constructs the full
    /// wiring (`SlmHttpConfig`/`ShadowConfig`/`ModelMeta`/sweep interval),
    /// correctly threading `[slm]`'s fields through — including the
    /// `min_samples`/`min_window_ms` stability thresholds into
    /// `ShadowConfig::eligibility` (the same shape `ShadowClassifier
    /// ::maybe_classify` gates on).
    #[test]
    fn shadow_lane_enabled_builds_full_wiring() {
        let slm = SlmConfig {
            enabled: true,
            base_url: "http://slm.internal:8000/v1".to_string(),
            model: "test-model".to_string(),
            timeout_ms: 1234,
            max_concurrency: 7,
            sweep_interval_ms: 5000,
            min_samples: 3,
            min_window_ms: 10_000,
        };

        let wiring = build_shadow_lane_wiring(&slm, &secrets(Some("slm-token")))
            .expect("enabled + token present must not error")
            .expect("enabled slm must construct Some(wiring)");

        assert_eq!(wiring.slm_http.base_url, "http://slm.internal:8000/v1");
        assert_eq!(wiring.slm_http.model, "test-model");
        assert_eq!(wiring.slm_http.api_token.as_deref(), Some("slm-token"));
        assert_eq!(wiring.slm_http.timeout_ms, 1234);
        assert_eq!(wiring.slm_http.max_concurrency, 7);

        assert_eq!(wiring.shadow_config.eligibility.min_samples, 3);
        assert_eq!(wiring.shadow_config.eligibility.min_age_ms, 10_000);

        assert_eq!(wiring.model.model_id, "test-model");
        assert_eq!(wiring.sweep_interval, Duration::from_millis(5000));
    }

    /// Defensive case: `[slm].enabled=true` but `secrets.slm_api_token` is
    /// `None` — should be unreachable via `validate_secrets` in normal
    /// startup, but `build_shadow_lane_wiring` must still fail loudly
    /// (never construct an `HttpInferencer` with no token) rather than
    /// panic or silently proceed unauthenticated.
    #[test]
    fn shadow_lane_enabled_without_token_errors() {
        let slm = SlmConfig {
            enabled: true,
            ..SlmConfig::default()
        };
        let err = build_shadow_lane_wiring(&slm, &secrets(None))
            .expect_err("enabled slm with no token must error, never panic or proceed");
        assert!(matches!(err, AppError::MissingSlmToken));
    }
}
