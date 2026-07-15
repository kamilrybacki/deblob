//! The tag-and-forward core (spec §3.1, §3.3): an axum server that reads
//! a POSTed body, classifies it via the shared
//! [`deblob_match::matcher::HotMatcher`], tags it, and forwards the
//! UNMODIFIED body to a fixed, config-supplied upstream — never a
//! destination the client controls.
//!
//! Task 2 adds the hardening layer (spec §4/§6) on top of Task 1's
//! tag-and-forward core: request-smuggling framing guards, body/header
//! size limits enforced before the hot path ever sees a byte, malformed
//! bodies quarantined with a 422 instead of forwarded, allowlist
//! enforcement at both construction and request time, and bounded
//! inbound/outbound timeouts.
//!
//! Task 3 (this module's current scope) adds the discovery-lane feed +
//! idempotency-key contract on top of Task 2's hardening: a `Provisional`
//! classification now enqueues a `DiscoveryMsg` to the configured
//! `DiscoverySink` (see `enqueue_discovery`), run CONCURRENTLY with the
//! upstream forward via `tokio::join!` so it never adds serialized
//! latency and its own failure never fails the request; every request
//! forwarded downstream now also carries exactly one `Idempotency-Key`
//! (client-provided verbatim, or generated — `headers::
//! ensure_idempotency_key`); and `Unresolved` (registry-down, from
//! Task 2) is confirmed to never enqueue a discovery message, only
//! `Provisional` does. No `[http_proxy]` config/`serve()` wiring yet
//! (Task 4).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use deblob_core::envelope::SourceCursor;
use deblob_core::id::SchemaRef;
use deblob_fingerprint::Limits;
use deblob_match::discovery::DiscoveryMsg;
use deblob_match::matcher::HotMatcher;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as HyperConnBuilder;
use hyper_util::service::TowerToHyperService;
use tokio_util::sync::CancellationToken;
use tower_http::timeout::TimeoutLayer;
use url::Url;
use uuid::Uuid;

use crate::headers::{
    ensure_idempotency_key, strip_reserved_and_hop_by_hop, with_quarantine_reason, with_tag,
    SCHEMA_ID_HEADER,
};
use crate::limits::{check_content_length, check_framing, check_header_limits, payload_too_large};

/// Errors a [`DiscoverySink`] implementation can return when enqueueing a
/// discovery message. Backed in production by
/// [`crate::kafka_sink::KafkaDiscoverySink`] (reusing
/// `deblob-kafka`'s standalone discovery producer, spec §3.2). Never
/// carries a payload byte — only the sink's own bounded error text (spec
/// §9) — the ingest handler logs this at `debug` without ever attaching
/// the message that failed to enqueue.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("discovery sink unavailable: {0}")]
    Unavailable(String),
}

/// Feeds unknown-shape (`Provisional`) classifications to the durable
/// discovery lane, so HTTP-ingested unknowns reach the cold lane exactly
/// like Kafka-ingested ones (spec §3.2). `enqueue_discovery` calls
/// [`DiscoverySink::enqueue`] on every `Provisional` classification,
/// CONCURRENTLY with the upstream forward (never serialized behind it —
/// see `enqueue_discovery`'s own docs). An `Option<Arc<dyn
/// DiscoverySink>>` of `None` is the documented degraded mode (spec
/// §3.2): the classification is still tagged `cand_` and forwarded, it
/// simply isn't fed to the cold lane.
#[async_trait::async_trait]
pub trait DiscoverySink: Send + Sync {
    async fn enqueue(&self, msg: DiscoveryMsg) -> Result<(), DiscoveryError>;
}

/// Configuration for one [`HttpProxy::run`] instance (spec §3.3, §5).
#[derive(Debug, Clone)]
pub struct HttpProxyCfg {
    /// The ingest listener address — SEPARATE from the management API
    /// port (spec §8), like the Kafka relay's own listener concerns.
    pub listen_addr: SocketAddr,
    /// The fixed upstream allowlist (SSRF prevention, spec §4). `route`
    /// (and any future route-map target) MUST be a member of this list —
    /// [`HttpProxy::run`] validates that at construction, and the ingest
    /// handler re-checks it per-request as defense-in-depth.
    pub upstream_allowlist: Vec<Url>,
    /// The single upstream every request is forwarded to (Task 1). A
    /// later task may promote this to a real path -> upstream route map;
    /// for now this is the entire "route" concept.
    pub route: Url,
    /// Bounds enforced while parsing the body (shared with the Kafka
    /// relay's `HotMatcher::classify` call — spec §3.2 reuse).
    pub limits: Limits,
    /// Hard ceiling on request body bytes actually read (spec §4/§6).
    /// Enforced BOTH via a `Content-Length` precheck AND a streamed
    /// aggregate cap (`axum::body::to_bytes`) — a lying or absent
    /// `Content-Length` can never let an oversized body through.
    pub max_body_bytes: usize,
    /// Hard ceiling on total request header bytes (names + values, spec
    /// §4).
    pub max_header_bytes: usize,
    /// Hard ceiling on the number of request headers (spec §4).
    pub max_header_count: usize,
    /// Bounded read/request timeout on the inbound handler (spec §4/§6):
    /// a slow-BODY/slowloris client is bounded, not able to hold a
    /// connection (and the handler task behind it) open indefinitely.
    /// Enforced by [`tower_http::timeout::TimeoutLayer`] wrapping the
    /// axum `Service` — which hyper only invokes AFTER the full header
    /// block has been parsed, so this bounds body delivery, not header
    /// delivery (see `header_read_timeout` for that).
    pub request_timeout: Duration,
    /// Bounded timeout on reading the full HTTP header block during
    /// connection setup (spec §4: "bounded read, write, and HEADER
    /// timeouts") — the Slowloris defense: a client that dribbles header
    /// bytes without ever completing the block is disconnected after
    /// this long. Enforced directly by the `hyper_util` connection
    /// builder in [`HttpProxy::run`] (`Http1Builder::header_read_timeout`),
    /// BEFORE hyper ever hands a request to axum's `Service` — `
    /// request_timeout`/`TimeoutLayer` alone cannot bound this, since
    /// tower only sees the request once headers are fully parsed.
    pub header_read_timeout: Duration,
    /// Bounded timeout on the outbound forward to the upstream (spec
    /// §4/§6): a slow/hung upstream returns `504 Gateway Timeout` instead
    /// of hanging the request.
    pub upstream_timeout: Duration,
    /// Bounded timeout on the discovery-lane `enqueue` call ONLY (Task 4
    /// Part 2, spec §4 "enqueue must not block the hot path"): decouples
    /// the forwarded response's latency from a slow/unreachable discovery
    /// sink. Without this, a Kafka broker outage could add up to
    /// [`deblob_kafka::discovery_producer::DiscoveryProducer`]'s own
    /// `message.timeout.ms` (10s) to EVERY Provisional request's latency,
    /// since `enqueue_discovery` runs concurrently with — but is still
    /// `.await`ed alongside — the upstream forward via `tokio::join!`. On
    /// timeout the enqueue attempt is abandoned (logged at `debug`, no
    /// payload) and the request proceeds exactly as it does on an
    /// `enqueue` `Err`: tagged `cand_` and forwarded regardless — the
    /// discovery-lane feed is already documented as non-fatal, this just
    /// bounds how long a stalled attempt is allowed to add to the
    /// response.
    pub discovery_enqueue_timeout: Duration,
}

/// Every way [`HttpProxy::run`] can fail before/while serving. Never
/// carries a header value or payload byte — only bounded, derived
/// information (spec §9).
#[derive(Debug, thiserror::Error)]
pub enum HttpProxyError {
    #[error("http proxy I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Config validation failure (spec §4): the configured `route` is not
    /// a member of `upstream_allowlist`. Caught at construction, before
    /// any listener is bound — an off-allowlist route is a config bug,
    /// not a runtime condition to degrade through.
    #[error("configured route is not a member of the upstream allowlist")]
    RouteNotAllowlisted,
    /// The outbound `reqwest::Client` failed to build (e.g. an invalid
    /// timeout configuration).
    #[error("failed to build the outbound forward client: {0}")]
    ClientBuild(#[source] reqwest::Error),
}

/// True if `route`'s scheme+host+port matches at least one entry of
/// `allowlist` (spec §4: "compare scheme+host+port"). The path is
/// deliberately NOT compared — the allowlist authorizes a *destination*,
/// not a specific path on it.
fn is_allowlisted(route: &Url, allowlist: &[Url]) -> bool {
    allowlist.iter().any(|allowed| {
        allowed.scheme() == route.scheme()
            && allowed.host_str() == route.host_str()
            && allowed.port_or_known_default() == route.port_or_known_default()
    })
}

/// Shared, cloneable state for the ingest handler.
#[derive(Clone)]
struct ProxyState {
    matcher: Arc<HotMatcher>,
    discovery: Option<Arc<dyn DiscoverySink>>,
    /// Monotonically-increasing synthetic offset for the `SourceCursor`
    /// attached to every `DiscoveryMsg` this listener enqueues (spec §3.2:
    /// HTTP has no real Kafka offset, so `enqueue_discovery` mints
    /// `cursor = SourceCursor { topic: "http", partition: 0, offset }`
    /// from this counter — unique and ordered per listener, never reused,
    /// even though it carries no replay-recovery meaning the way a real
    /// Kafka offset does).
    discovery_offset: Arc<AtomicI64>,
    route: Url,
    /// Re-checked per-request as defense-in-depth alongside the
    /// construction-time check in [`HttpProxy::run`] (spec §4).
    upstream_allowlist: Vec<Url>,
    /// The `deblob-origin` prefix for this listener — `http/<listen_addr>`
    /// — combined with a per-request id to build the full origin value
    /// (spec §3.1: origin carries transport + source coordinates).
    origin_prefix: String,
    client: reqwest::Client,
    limits: Limits,
    max_body_bytes: usize,
    max_header_bytes: usize,
    max_header_count: usize,
    /// See [`HttpProxyCfg::discovery_enqueue_timeout`].
    discovery_enqueue_timeout: Duration,
}

/// The HTTP push reverse proxy (spec §3.3).
pub struct HttpProxy;

impl HttpProxy {
    /// Binds `cfg.listen_addr`, serves the ingest route until `shutdown`
    /// is cancelled, then returns once every in-flight connection has
    /// finished (each one is bounded by `header_read_timeout` +
    /// `request_timeout`, so this drain itself can never hang).
    ///
    /// Validates `cfg.route` against `cfg.upstream_allowlist` BEFORE
    /// binding any socket (spec §4: "reject/validate at construction if
    /// the configured route points outside the allowlist") — an
    /// off-allowlist route never gets a chance to serve a single request.
    ///
    /// Serves via a manually-configured `hyper_util` connection builder
    /// rather than `axum::serve` (spec §4: "bounded read, write, and
    /// HEADER timeouts") — `axum::serve` builds a bare
    /// `hyper_util::server::conn::auto::Builder` with no
    /// `header_read_timeout`, so a client that dribbles header bytes
    /// without ever completing the block can hold a connection open
    /// indefinitely (Slowloris). The tower `TimeoutLayer` on the router
    /// below is NOT a substitute: hyper only invokes the axum `Service`
    /// after the full header block has already been parsed, so it bounds
    /// slow-BODY delivery only. Configuring `header_read_timeout`
    /// directly on the connection builder closes that gap.
    pub async fn run(
        cfg: HttpProxyCfg,
        matcher: Arc<HotMatcher>,
        discovery: Option<Arc<dyn DiscoverySink>>,
        shutdown: CancellationToken,
    ) -> Result<(), HttpProxyError> {
        if !is_allowlisted(&cfg.route, &cfg.upstream_allowlist) {
            return Err(HttpProxyError::RouteNotAllowlisted);
        }

        let client = reqwest::Client::builder()
            .timeout(cfg.upstream_timeout)
            .build()
            .map_err(HttpProxyError::ClientBuild)?;

        let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
        let state = ProxyState {
            matcher,
            discovery,
            discovery_offset: Arc::new(AtomicI64::new(0)),
            route: cfg.route,
            upstream_allowlist: cfg.upstream_allowlist,
            origin_prefix: format!("http/{}", cfg.listen_addr),
            client,
            limits: cfg.limits,
            max_body_bytes: cfg.max_body_bytes,
            max_header_bytes: cfg.max_header_bytes,
            max_header_count: cfg.max_header_count,
            discovery_enqueue_timeout: cfg.discovery_enqueue_timeout,
        };
        let router = Router::new()
            .route("/ingest", post(ingest_handler))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                cfg.request_timeout,
            ))
            .with_state(state);

        // A `Timer` is REQUIRED for `header_read_timeout` to take effect
        // (hyper_util panics at connection-serve time if one is
        // configured without it) — `TokioTimer` is the timer backing.
        let mut conn_builder = HyperConnBuilder::new(TokioExecutor::new());
        conn_builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(cfg.header_read_timeout);

        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer_addr)) => {
                            let io = TokioIo::new(stream);
                            let hyper_service = TowerToHyperService::new(router.clone());
                            let conn_builder = conn_builder.clone();
                            // Spawned so one slow/stalled client never
                            // blocks `listener.accept()` for anyone else
                            // — each connection is bounded on its own by
                            // `header_read_timeout`/`request_timeout`.
                            connections.spawn(async move {
                                if let Err(error) =
                                    conn_builder.serve_connection(io, hyper_service).await
                                {
                                    tracing::debug!(?error, "connection ended with an error");
                                }
                            });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "failed to accept an inbound connection");
                        }
                    }
                }
            }
        }

        // Stop accepting new connections; drain what's already in
        // flight. Each one is individually bounded, so this can never
        // hang past `header_read_timeout` + `request_timeout` +
        // `upstream_timeout` (plus response-write time).
        while connections.join_next().await.is_some() {}
        Ok(())
    }
}

/// `POST /ingest` — the tag-and-forward core (spec §3.1) plus every
/// hardening rule spec §4/§6 requires:
///
/// 1. request-smuggling framing guard (both `Content-Length` AND
///    `Transfer-Encoding`, or duplicate `Content-Length` → 400) — before a
///    single body byte is read,
/// 2. header count/byte-weight guard → 431,
/// 3. `Content-Length` precheck against `max_body_bytes` → 413,
/// 4. read the body via a streamed `axum::body::to_bytes` cap — bounds the
///    bytes actually read regardless of what any header claims → 413,
/// 5. `HotMatcher::classify` it against the shared decision table,
/// 6. `Malformed` → 422 + `deblob-quarantine-reason`, NEVER forwarded,
/// 7. re-check `state.route` against `state.upstream_allowlist`
///    (defense-in-depth; `HttpProxy::run` already validated this at
///    construction) → 502 if somehow no longer a member,
/// 8. strip every inbound reserved/hop-by-hop header, then write exactly
///    one `deblob-schema-id` + `deblob-origin` pair, then ensure exactly
///    one `Idempotency-Key` (a client-provided value survives verbatim —
///    it is neither reserved nor hop-by-hop, so step 8's strip already
///    left it in place; an absent one is generated, spec §4),
/// 9. forward the UNMODIFIED body to `state.route` (never a
///    client-controlled destination), with `state.client`'s configured
///    forward timeout — a hung upstream → 504, not a hang — running
///    CONCURRENTLY with `enqueue_discovery` (Task 3, spec §3.2: a
///    `Provisional` classification's discovery-lane feed never serializes
///    behind the forward, and a `Malformed`/non-`Provisional`
///    classification never enqueues at all — see `enqueue_discovery`),
/// 10. return the upstream's response, with `deblob-schema-id` added so
///     the producer sees the tag too.
async fn ingest_handler(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let request_headers = parts.headers;

    if let Err(response) = check_framing(&request_headers) {
        return *response;
    }
    if let Err(response) = check_header_limits(
        &request_headers,
        state.max_header_bytes,
        state.max_header_count,
    ) {
        return *response;
    }
    if let Err(response) = check_content_length(&request_headers, state.max_body_bytes) {
        return *response;
    }

    let body = match axum::body::to_bytes(body, state.max_body_bytes).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(%error, "request body exceeded the streamed size cap");
            return *payload_too_large();
        }
    };

    let classification = state.matcher.classify(&body, &state.limits).await;

    if classification.schema_ref == SchemaRef::Malformed {
        let reason = classification
            .quarantine
            .expect("Malformed classification always carries a quarantine reason");
        let mut response_headers = HeaderMap::new();
        with_quarantine_reason(&mut response_headers, reason);
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            response_headers,
            "payload quarantined: malformed",
        )
            .into_response();
    }

    if !is_allowlisted(&state.route, &state.upstream_allowlist) {
        tracing::error!("configured route is no longer a member of the upstream allowlist");
        return (StatusCode::BAD_GATEWAY, "upstream not allowlisted").into_response();
    }

    let mut forward_headers = strip_reserved_and_hop_by_hop(&request_headers);
    let origin = format!("{}/{}", state.origin_prefix, Uuid::now_v7());
    with_tag(&mut forward_headers, &classification.schema_ref, &origin);
    ensure_idempotency_key(&mut forward_headers);

    // Kick off the discovery-lane enqueue and the upstream forward
    // TOGETHER (`tokio::join!`, not one `.await` after the other): the
    // discovery enqueue never serializes behind — and therefore never
    // adds latency on top of — the forward. `enqueue_discovery` itself is
    // a no-op (returns immediately) for every classification except
    // `Provisional`, and for `Provisional` with no `DiscoverySink`
    // configured (the documented degraded mode, spec §3.2).
    let discovery_future =
        enqueue_discovery(&state, classification.schema_ref.clone(), body.clone());
    let forward_future = state
        .client
        .post(state.route.clone())
        .headers(forward_headers)
        .body(body)
        .send();
    let (_, forward_result) = tokio::join!(discovery_future, forward_future);

    let upstream_response = match forward_result {
        Ok(response) => response,
        Err(error) => {
            if error.is_timeout() {
                tracing::error!(%error, "upstream request timed out");
                return (StatusCode::GATEWAY_TIMEOUT, "upstream request timed out").into_response();
            }
            tracing::error!(%error, "failed forwarding request to upstream");
            return (StatusCode::BAD_GATEWAY, "upstream request failed").into_response();
        }
    };

    let status = upstream_response.status();
    let mut response_headers = strip_reserved_and_hop_by_hop(upstream_response.headers());
    let upstream_body = match upstream_response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::error!(%error, "failed reading upstream response body");
            return (StatusCode::BAD_GATEWAY, "upstream response read failed").into_response();
        }
    };

    response_headers.insert(
        HeaderName::from_static(SCHEMA_ID_HEADER),
        HeaderValue::from_str(&classification.schema_ref.header_value())
            .expect("SchemaRef::header_value is always ASCII-safe"),
    );

    (status, response_headers, upstream_body).into_response()
}

/// Feeds a `Provisional` classification to the durable discovery lane
/// (spec §3.2), so an HTTP-ingested unknown shape reaches the cold lane
/// exactly like a Kafka-ingested one — the same `DiscoveryMsg` envelope,
/// carrying the discovery lane's own durable copy of the RAW body (this
/// is the discovery topic's established contract, spec §3.2: the Kafka
/// relay's own `Provisional` produce carries the same raw payload; the
/// cold lane already owns redaction/PII handling downstream of this
/// point — this is not, and must never be confused with, the separately
/// redacted SLM prompt).
///
/// A no-op for every OTHER classification (`Known`, `Unresolved`,
/// `Malformed`, `Tombstone`) — only an unknown shape is a discovery
/// candidate; `Unresolved` in particular must NEVER enqueue (spec §6: a
/// registry outage degrades to `unresolved`, it does not mint a
/// candidate, so there is nothing here for the cold lane to see).
///
/// Also a no-op when `state.discovery` is `None` — the documented
/// degraded mode (spec §3.2): "If Kafka isn't configured, HTTP unknowns
/// are tagged `cand_` and forwarded but not fed to the cold lane."
///
/// Called via `tokio::join!` alongside the upstream forward (never
/// `.await`ed serially before it), so a slow discovery sink adds no
/// latency on top of the forward itself; the `enqueue` call is ALSO
/// individually bounded by `state.discovery_enqueue_timeout` (Task 4 Part
/// 2) — without that bound, `tokio::join!` still waits for BOTH futures
/// to finish, so a slow/unreachable Kafka broker (e.g. stuck inside
/// `DiscoveryProducer`'s own 10s `message.timeout.ms`) would add up to
/// that long on top of every Provisional response even though the two
/// futures run concurrently. Both an `Err` and a timeout are caught and
/// logged at `debug` — WITHOUT the payload, only a bounded reason —
/// rather than propagated, because a discovery-lane outage or slowdown
/// must never fail (or meaningfully slow) the request: the message is
/// tagged `cand_` and forwarded regardless, same "degrade, don't block"
/// principle as the `Unresolved` outage path.
async fn enqueue_discovery(state: &ProxyState, schema_ref: SchemaRef, body: Bytes) {
    let SchemaRef::Provisional(cand_id) = schema_ref else {
        return;
    };
    let Some(discovery) = &state.discovery else {
        return;
    };

    let offset = state.discovery_offset.fetch_add(1, Ordering::Relaxed);
    let msg = DiscoveryMsg {
        cand_id: cand_id.as_str().to_string(),
        payload: body,
        source: state.origin_prefix.clone(),
        cursor: SourceCursor {
            topic: "http".to_string(),
            partition: 0,
            offset,
        },
    };

    match tokio::time::timeout(state.discovery_enqueue_timeout, discovery.enqueue(msg)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::debug!(
                %error,
                "discovery enqueue failed; the request is still tagged and forwarded"
            );
        }
        Err(_elapsed) => {
            tracing::debug!(
                timeout_ms = state.discovery_enqueue_timeout.as_millis() as u64,
                "discovery enqueue timed out; the request is still tagged and forwarded"
            );
        }
    }
}
