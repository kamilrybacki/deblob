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
//! the shadow sweep if `[slm].enabled`, then the management API, then the
//! HTTP push reverse proxy if `[http_proxy].enabled` — P2-C Task 4, no
//! ordering dependency on the others, so it drains last) — the exact
//! sequencing Task 18's `main.rs` run() used to inline before this split.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use deblob_core::error::CoreError;
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_http::{DiscoverySink, HttpProxy, HttpProxyCfg, IngestToken, KafkaDiscoverySink};
use deblob_kafka::{DiscoveryProducer, DiscoveryProducerCfg, DiscoveryProducerError};
use deblob_kafka::{Relay, RelayCfg};
use deblob_redis::{HealthGate, RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use deblob_slm::{HttpInferencer, SemanticInferencer, SlmHttpConfig};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::api::{self, ApiState, SecretToken};
use crate::coldlane::ColdLane;
use crate::config::{Config, HttpProxyConfig, Secrets, SlmConfig};
use crate::discovery_consumer::{self, DiscoveryConsumerCfg};
use crate::matcher::HotMatcher;
use crate::metrics::Metrics;
use crate::policy::{Promoter as ConcretePromoter, PromotionPolicy};
use crate::promote::Promoter as PromoterTrait;
use crate::semantic_store::SemanticStore;
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
    /// `[http_proxy].listen_addr` (P2-C Task 4) failed to parse as a socket
    /// address. Caught before any listener is bound — a malformed listen
    /// address is a config bug, not a runtime condition to degrade
    /// through.
    #[error("invalid `http_proxy.listen_addr` {addr:?}: {source}")]
    InvalidHttpProxyListenAddr {
        addr: String,
        source: std::net::AddrParseError,
    },
    /// A `[http_proxy].upstream_allowlist` entry or `[http_proxy].route`
    /// value failed to parse as a URL.
    #[error("invalid `http_proxy` URL {url:?}: {source}")]
    InvalidHttpProxyUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    /// `[http_proxy].route` is not a member of `[http_proxy]
    /// .upstream_allowlist` (spec §4: SSRF prevention). Caught before any
    /// listener is bound, mirroring
    /// [`deblob_http::HttpProxyError::RouteNotAllowlisted`] which
    /// `HttpProxy::run` itself would otherwise raise at construction —
    /// surfaced here first for a clearer startup error.
    #[error("[http_proxy].route is not a member of [http_proxy].upstream_allowlist")]
    HttpProxyRouteNotAllowlisted,
    /// The standalone discovery-topic producer backing
    /// [`deblob_http::KafkaDiscoverySink`] failed to build (P2-C Task 4).
    #[error("failed to build the HTTP proxy's discovery producer: {0}")]
    HttpProxyDiscoveryProducer(#[source] DiscoveryProducerError),
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

/// Everything [`serve`] needs to spawn the HTTP push reverse proxy,
/// computed PURELY from `[http_proxy]` config + `[kafka]` config +
/// secrets — no listener bound, no I/O beyond the (non-network,
/// non-blocking — see [`DiscoveryProducer::new`]'s own docs) construction
/// of the standalone discovery producer. Kept separate from [`serve`]
/// itself so the enabled/disabled WIRING DECISION is unit-testable
/// without Docker/Kafka — see the `http_proxy_wiring_*` tests below,
/// mirroring [`ShadowLaneWiring`]/[`build_shadow_lane_wiring`]'s own
/// pattern.
struct HttpProxyWiring {
    cfg: HttpProxyCfg,
    sink: KafkaDiscoverySink,
}

/// `Ok(None)` iff `[http_proxy].enabled` is `false` — in that case
/// [`serve`] constructs NO `HttpProxyCfg`, NO `KafkaDiscoverySink`, and
/// spawns NO proxy listener, so behavior is unchanged from before Task 4.
/// `Ok(Some(_))` iff enabled and every URL/address in `[http_proxy]`
/// parses and `route` is a member of `upstream_allowlist`.
fn build_http_proxy_wiring(
    http_proxy: &HttpProxyConfig,
    app_config: &Config,
    secrets: &Secrets,
) -> Result<Option<HttpProxyWiring>, AppError> {
    if !http_proxy.enabled {
        return Ok(None);
    }

    let listen_addr: SocketAddr =
        http_proxy
            .listen_addr
            .parse()
            .map_err(|source| AppError::InvalidHttpProxyListenAddr {
                addr: http_proxy.listen_addr.clone(),
                source,
            })?;

    let upstream_allowlist = http_proxy
        .upstream_allowlist
        .iter()
        .map(|raw| parse_http_proxy_url(raw))
        .collect::<Result<Vec<Url>, AppError>>()?;
    let route = parse_http_proxy_url(&http_proxy.route)?;

    if !is_http_proxy_route_allowlisted(&route, &upstream_allowlist) {
        return Err(AppError::HttpProxyRouteNotAllowlisted);
    }

    let cfg = HttpProxyCfg {
        listen_addr,
        upstream_allowlist,
        route,
        limits: app_config.limits.to_limits(),
        max_body_bytes: http_proxy.max_body_bytes,
        max_header_bytes: http_proxy.max_header_bytes,
        max_header_count: http_proxy.max_header_count,
        request_timeout: Duration::from_millis(http_proxy.request_timeout_ms),
        header_read_timeout: Duration::from_millis(http_proxy.header_read_timeout_ms),
        upstream_timeout: Duration::from_millis(http_proxy.upstream_timeout_ms),
        discovery_enqueue_timeout: Duration::from_millis(http_proxy.discovery_enqueue_timeout_ms),
        // `require_auth` now actually ENFORCES the bearer check (spec
        // §4/§8), not just validates the token present at startup: `Some`
        // when `require_auth` is true — `validate_secrets` already
        // guarantees `secrets.http_ingest_token.is_some()` in that case
        // (it errors at startup otherwise), so the `expect` below is
        // unreachable via `main.rs`'s normal startup path. `None` when
        // `require_auth` is false — unchanged, unauthenticated behavior.
        ingest_token: if http_proxy.require_auth {
            Some(IngestToken::new(secrets.http_ingest_token.as_deref().expect(
                "validate_secrets guarantees http_ingest_token is Some when require_auth is true",
            )))
        } else {
            None
        },
    };

    // Reuses the SAME discovery topic the Kafka relay/discovery-topic
    // consumer already read from (`app_config.kafka.discovery_topic`), so
    // HTTP-ingested unknowns land on the same durable discovery lane as
    // Kafka-ingested ones (spec §3.2).
    let producer = DiscoveryProducer::new(DiscoveryProducerCfg {
        brokers: secrets.kafka_brokers.clone(),
        discovery_topic: app_config.kafka.discovery_topic.clone(),
        sasl: secrets.kafka_sasl.clone(),
    })
    .map_err(AppError::HttpProxyDiscoveryProducer)?;
    let sink = KafkaDiscoverySink::new(producer);

    Ok(Some(HttpProxyWiring { cfg, sink }))
}

fn parse_http_proxy_url(raw: &str) -> Result<Url, AppError> {
    Url::parse(raw).map_err(|source| AppError::InvalidHttpProxyUrl {
        url: raw.to_string(),
        source,
    })
}

/// Mirrors [`deblob_http::proxy`]'s own (private) `is_allowlisted`: true if
/// `route`'s scheme+host+port matches at least one entry of `allowlist`
/// (spec §4: "compare scheme+host+port"). Duplicated here (rather than
/// exported from `deblob-http`) so `serve()` can raise a clear startup
/// error BEFORE `HttpProxy::run` would otherwise catch the same condition
/// at construction.
fn is_http_proxy_route_allowlisted(route: &Url, allowlist: &[Url]) -> bool {
    allowlist.iter().any(|allowed| {
        allowed.scheme() == route.scheme()
            && allowed.host_str() == route.host_str()
            && allowed.port_or_known_default() == route.port_or_known_default()
    })
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
    // Wrapped in `Arc<RedisRegistry>` first (rather than eagerly upcast to
    // `Arc<dyn Registry>`) so the SAME concrete instance — one connection,
    // one health gate — backs BOTH the structural `Registry` trait object
    // and the semantic-governance `SemanticStore` trait object (Task 6)
    // below via two independent unsized coercions, instead of connecting
    // twice.
    let redis_registry = Arc::new(
        RedisRegistry::connect(&secrets.redis_url, redis_opts)
            .await
            .map_err(AppError::Redis)?
            .with_health_gate(health.clone()),
    );
    let registry: Arc<dyn Registry> = redis_registry.clone();
    let semantic: Arc<dyn SemanticStore> = redis_registry.clone();

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
        semantic,
        // P2-D Task 8 follow-up (A1): seeded from the TOML `[semantic]`
        // section (`crate::config::SemanticConfig::to_registries`) — no
        // registration ENDPOINT exists (an operator edits the config file
        // and restarts), but the registries themselves are no longer
        // permanently empty. Absent `[semantic]` still yields
        // `Registries::default()` (both sets empty), so every strong-axis
        // annotation 422s exactly as it did before this wiring.
        semantic_registries: Arc::new(app_config.semantic.to_registries()),
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

    // --- HTTP push reverse proxy (P2-C Task 4): OFF unless
    // `[http_proxy].enabled` is true. When disabled (the default), no
    // `HttpProxyCfg`, no `KafkaDiscoverySink`, and no proxy listener are
    // constructed at all — this block is then a no-op and
    // `http_proxy_handle` stays `None`, matching the shadow lane's own
    // pattern above. Runs off the relay/hot path — a separate spawned
    // task with its own listener, never sharing the Kafka relay's or
    // management API's ports. ---
    let http_proxy_handle =
        match build_http_proxy_wiring(&app_config.http_proxy, &app_config, &secrets)? {
            None => None,
            Some(wiring) => {
                let sink: Arc<dyn DiscoverySink> = Arc::new(wiring.sink);
                let http_proxy_matcher = matcher.clone();
                let http_proxy_shutdown = shutdown.clone();
                Some(tokio::spawn(async move {
                    HttpProxy::run(
                        wiring.cfg,
                        http_proxy_matcher,
                        Some(sink),
                        http_proxy_shutdown,
                    )
                    .await
                }))
            }
        };

    shutdown.cancelled().await;
    tracing::info!(
        "shutdown signal received; draining relay, discovery consumer, shadow sweep (if enabled), http proxy (if enabled), and management API"
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
    // HTTP proxy drained last: it runs off the relay/hot path (its own
    // listener, no shared state with the Kafka relay/management API
    // beyond `matcher`/`shutdown`), so there's no ordering requirement
    // pulling it earlier the way the relay's open-transaction concern
    // does — draining it after the management API is simplest and
    // matches the shadow sweep's own "no ordering dependency" placement.
    if let Some(http_proxy_handle) = http_proxy_handle {
        match http_proxy_handle.await {
            Ok(Ok(())) => tracing::info!("http proxy drained cleanly"),
            Ok(Err(e)) => {
                tracing::error!(error = %e, "http proxy exited with error during shutdown")
            }
            Err(e) => tracing::error!(error = %e, "http proxy task panicked"),
        }
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
            http_ingest_token: None,
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

    /// A minimal `Config` (just the required `[kafka]` section) with
    /// `http_proxy` overridden — `build_http_proxy_wiring` reads
    /// `app_config.kafka.discovery_topic`/`app_config.limits` alongside
    /// `[http_proxy]` itself.
    fn test_config(http_proxy: HttpProxyConfig) -> Config {
        let mut config = Config::parse_toml(
            r#"
            [kafka]
            raw_topic = "r"
            tagged_topic = "t"
            discovery_topic = "d"
            quarantine_topic = "q"
            group_id = "g"
            transactional_id = "x"
            "#,
        )
        .expect("minimal config must parse");
        config.http_proxy = http_proxy;
        config
    }

    /// `[http_proxy].enabled=false` (the documented default) must wire up
    /// NO proxy at all — mirrors `shadow_lane_disabled_by_default_wires_
    /// nothing`.
    #[test]
    fn disabled_by_default_spawns_no_proxy() {
        let http_proxy = HttpProxyConfig::default();
        assert!(!http_proxy.enabled);
        let config = test_config(http_proxy.clone());

        let wiring = build_http_proxy_wiring(&http_proxy, &config, &secrets(None))
            .expect("disabled http_proxy must never error");
        assert!(
            wiring.is_none(),
            "disabled [http_proxy] must construct no HttpProxyCfg/KafkaDiscoverySink/listener"
        );
    }

    /// `[http_proxy].enabled=true` with a valid allowlist/route constructs
    /// the full wiring, correctly threading every `[http_proxy]` field
    /// (including the `_ms` timeout fields converted to `Duration`)
    /// through to `HttpProxyCfg`. An off-allowlist route must instead
    /// produce a clear startup error (spec §4: SSRF prevention).
    #[test]
    fn enabled_constructs_proxy_wiring() {
        let http_proxy = HttpProxyConfig {
            enabled: true,
            listen_addr: "127.0.0.1:9600".to_string(),
            upstream_allowlist: vec!["https://upstream.internal:8443".to_string()],
            route: "https://upstream.internal:8443/ingest".to_string(),
            max_body_bytes: 2_000_000,
            max_header_bytes: 30_000,
            max_header_count: 150,
            request_timeout_ms: 4000,
            header_read_timeout_ms: 4500,
            upstream_timeout_ms: 5000,
            discovery_enqueue_timeout_ms: 250,
            require_auth: false,
        };
        let config = test_config(http_proxy.clone());

        let wiring = build_http_proxy_wiring(&http_proxy, &config, &secrets(None))
            .expect("enabled + allowlisted route must not error")
            .expect("enabled http_proxy must construct Some(wiring)");

        assert_eq!(wiring.cfg.listen_addr.to_string(), "127.0.0.1:9600");
        assert_eq!(
            wiring.cfg.route.as_str(),
            "https://upstream.internal:8443/ingest"
        );
        assert_eq!(wiring.cfg.upstream_allowlist.len(), 1);
        assert_eq!(wiring.cfg.max_body_bytes, 2_000_000);
        assert_eq!(wiring.cfg.max_header_bytes, 30_000);
        assert_eq!(wiring.cfg.max_header_count, 150);
        assert_eq!(wiring.cfg.request_timeout, Duration::from_millis(4000));
        assert_eq!(wiring.cfg.header_read_timeout, Duration::from_millis(4500));
        assert_eq!(wiring.cfg.upstream_timeout, Duration::from_millis(5000));
        assert_eq!(
            wiring.cfg.discovery_enqueue_timeout,
            Duration::from_millis(250)
        );
        assert!(
            wiring.cfg.ingest_token.is_none(),
            "require_auth=false must construct no ingest_token — unauthenticated, unchanged behavior"
        );

        // An off-allowlist route must produce a clear startup error,
        // before any listener is bound.
        let off_allowlist = HttpProxyConfig {
            route: "https://not-allowed.internal:9999/ingest".to_string(),
            ..http_proxy
        };
        let config2 = test_config(off_allowlist.clone());
        // `HttpProxyWiring` (the `Ok` payload) intentionally has no
        // `Debug` impl — it embeds `KafkaDiscoverySink`, which wraps a
        // non-`Debug` `rdkafka` producer — so this asserts via `matches!`
        // rather than `expect_err` (which would require `Debug` on the
        // whole `Result`).
        let result = build_http_proxy_wiring(&off_allowlist, &config2, &secrets(None));
        assert!(
            matches!(result, Err(AppError::HttpProxyRouteNotAllowlisted)),
            "route outside the allowlist must error with HttpProxyRouteNotAllowlisted"
        );
    }

    /// Security fix: `[http_proxy].require_auth = true` must thread an
    /// actual `IngestToken` into `HttpProxyCfg` — `validate_secrets`
    /// already guarantees `secrets.http_ingest_token.is_some()` whenever
    /// `require_auth` is true (it errors at startup otherwise), so this
    /// asserts the wiring actually turns that guarantee into enforcement,
    /// rather than the config knob being validated-but-inert.
    #[test]
    fn require_auth_true_threads_ingest_token() {
        let http_proxy = HttpProxyConfig {
            enabled: true,
            listen_addr: "127.0.0.1:9600".to_string(),
            upstream_allowlist: vec!["https://upstream.internal:8443".to_string()],
            route: "https://upstream.internal:8443/ingest".to_string(),
            require_auth: true,
            ..HttpProxyConfig::default()
        };
        let config = test_config(http_proxy.clone());
        let secrets_with_token = Secrets {
            api_token: "test-token".to_string(),
            redis_url: "redis://localhost:6379".to_string(),
            kafka_brokers: "localhost:9092".to_string(),
            kafka_sasl: None,
            slm_api_token: None,
            http_ingest_token: Some("ingest-secret".to_string()),
        };

        let wiring = build_http_proxy_wiring(&http_proxy, &config, &secrets_with_token)
            .expect("enabled + require_auth=true with a token present must not error")
            .expect("enabled http_proxy must construct Some(wiring)");

        assert!(
            wiring.cfg.ingest_token.is_some(),
            "require_auth=true must construct Some(ingest_token)"
        );
    }
}
