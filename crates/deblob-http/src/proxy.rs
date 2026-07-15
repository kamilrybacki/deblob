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
//! inbound/outbound timeouts. No discovery-lane enqueue yet (Task 3 — the
//! `DiscoverySink` trait is defined here so Task 3 can back it, but
//! nothing calls `enqueue` in this task), no `[http_proxy]`
//! config/`serve()` wiring yet (Task 4).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
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
    strip_reserved_and_hop_by_hop, with_quarantine_reason, with_tag, SCHEMA_ID_HEADER,
};
use crate::limits::{check_content_length, check_framing, check_header_limits, payload_too_large};

/// Errors a [`DiscoverySink`] implementation can return when enqueueing a
/// discovery message. Task 3 backs [`DiscoverySink`] with a Kafka
/// producer (reusing the relay's discovery topic) and defines the real
/// failure modes; this task only defines the trait shape.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("discovery sink unavailable: {0}")]
    Unavailable(String),
}

/// Feeds unknown-shape (`Provisional`) classifications to the durable
/// discovery lane, so HTTP-ingested unknowns reach the cold lane exactly
/// like Kafka-ingested ones (spec §3.2). Task 3 backs this with a Kafka
/// producer and wires the handler to call [`DiscoverySink::enqueue`] on a
/// `Provisional` classification; this task only defines the trait and
/// threads an `Option<Arc<dyn DiscoverySink>>` through
/// [`HttpProxy::run`]/the handler without calling it yet — a wiring seam,
/// not yet a behavior.
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
    #[allow(dead_code)] // wired up by Task 3
    discovery: Option<Arc<dyn DiscoverySink>>,
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
            route: cfg.route,
            upstream_allowlist: cfg.upstream_allowlist,
            origin_prefix: format!("http/{}", cfg.listen_addr),
            client,
            limits: cfg.limits,
            max_body_bytes: cfg.max_body_bytes,
            max_header_bytes: cfg.max_header_bytes,
            max_header_count: cfg.max_header_count,
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
///    one `deblob-schema-id` + `deblob-origin` pair,
/// 9. forward the UNMODIFIED body to `state.route` (never a
///    client-controlled destination), with `state.client`'s configured
///    forward timeout — a hung upstream → 504, not a hang,
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

    let upstream_response = match state
        .client
        .post(state.route.clone())
        .headers(forward_headers)
        .body(body)
        .send()
        .await
    {
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
