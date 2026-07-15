//! The tag-and-forward core (spec §3.1, §3.3): an axum server that reads
//! a POSTed body, classifies it via the shared
//! [`deblob_match::matcher::HotMatcher`], tags it, and forwards the
//! UNMODIFIED body to a fixed, config-supplied upstream — never a
//! destination the client controls.
//!
//! Task 1 scope only: no body/header size limits yet (Task 2), no
//! discovery-lane enqueue yet (Task 3 — the `DiscoverySink` trait is
//! defined here so Task 3 can back it, but nothing calls `enqueue` in
//! this task), no `[http_proxy]` config/`serve()` wiring yet (Task 4).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use deblob_fingerprint::Limits;
use deblob_match::discovery::DiscoveryMsg;
use deblob_match::matcher::HotMatcher;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::headers::{strip_reserved_and_hop_by_hop, with_tag, SCHEMA_ID_HEADER};

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
    /// The fixed upstream allowlist (SSRF prevention, spec §4). Task 1
    /// forwards unconditionally to `route`; Task 2 enforces that `route`
    /// (and any future route-map target) is actually a member of this
    /// list before forwarding.
    pub upstream_allowlist: Vec<Url>,
    /// The single upstream every request is forwarded to (Task 1). A
    /// later task may promote this to a real path -> upstream route map;
    /// for now this is the entire "route" concept.
    pub route: Url,
    /// Bounds enforced while parsing the body (shared with the Kafka
    /// relay's `HotMatcher::classify` call — spec §3.2 reuse).
    pub limits: Limits,
}

/// Every way [`HttpProxy::run`] can fail before/while serving. Never
/// carries a header value or payload byte — only bounded, derived
/// information (spec §9).
#[derive(Debug, thiserror::Error)]
pub enum HttpProxyError {
    #[error("http proxy I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Shared, cloneable state for the ingest handler.
#[derive(Clone)]
struct ProxyState {
    matcher: Arc<HotMatcher>,
    #[allow(dead_code)] // wired up by Task 3
    discovery: Option<Arc<dyn DiscoverySink>>,
    route: Url,
    /// The `deblob-origin` prefix for this listener — `http/<listen_addr>`
    /// — combined with a per-request id to build the full origin value
    /// (spec §3.1: origin carries transport + source coordinates).
    origin_prefix: String,
    client: reqwest::Client,
    limits: Limits,
}

/// The HTTP push reverse proxy (spec §3.3).
pub struct HttpProxy;

impl HttpProxy {
    /// Binds `cfg.listen_addr`, serves the ingest route until `shutdown`
    /// is cancelled, then returns once the listener has drained
    /// in-flight connections (axum's graceful shutdown).
    pub async fn run(
        cfg: HttpProxyCfg,
        matcher: Arc<HotMatcher>,
        discovery: Option<Arc<dyn DiscoverySink>>,
        shutdown: CancellationToken,
    ) -> Result<(), HttpProxyError> {
        let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
        let state = ProxyState {
            matcher,
            discovery,
            route: cfg.route,
            origin_prefix: format!("http/{}", cfg.listen_addr),
            client: reqwest::Client::new(),
            limits: cfg.limits,
        };
        let router = Router::new()
            .route("/ingest", post(ingest_handler))
            .with_state(state);

        axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await?;
        Ok(())
    }
}

/// `POST /ingest` — the tag-and-forward core (spec §3.1):
///
/// 1. read the body (bounded reading is Task 2's job),
/// 2. `HotMatcher::classify` it against the shared decision table,
/// 3. strip every inbound reserved/hop-by-hop header, then write exactly
///    one `deblob-schema-id` + `deblob-origin` pair,
/// 4. forward the UNMODIFIED body to `state.route` (never a
///    client-controlled destination),
/// 5. return the upstream's response, with `deblob-schema-id` added so
///    the producer sees the tag too.
async fn ingest_handler(
    State(state): State<ProxyState>,
    request_headers: HeaderMap,
    body: Bytes,
) -> Response {
    let classification = state.matcher.classify(&body, &state.limits).await;

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
