//! Integration tests for `HttpProxy::run` (Task 1 scope): tag-and-forward
//! and header hygiene, exercised end-to-end against an in-process axum
//! "upstream" test double — no Docker/wiremock needed since the upstream
//! contract here is just "an HTTP server that records what it received".

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, FamilyRef, Registry, SchemaRecord};
use deblob_fingerprint::Limits;
use deblob_http::{HttpProxy, HttpProxyCfg, HttpProxyError};
use deblob_match::matcher::HotMatcher;
use deblob_match::metrics::Metrics;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Generous hardening defaults for tests that don't care about a
/// specific limit — big enough that ordinary small JSON bodies/headers
/// never trip them, so each test only has to override the ONE knob it's
/// actually exercising.
fn generous_cfg(listen_addr: SocketAddr, upstream_allowlist: Vec<Url>, route: Url) -> HttpProxyCfg {
    HttpProxyCfg {
        listen_addr,
        upstream_allowlist,
        route,
        limits: Limits::default(),
        max_body_bytes: 1024 * 1024,
        max_header_bytes: 64 * 1024,
        max_header_count: 200,
        request_timeout: Duration::from_secs(10),
        header_read_timeout: Duration::from_secs(10),
        upstream_timeout: Duration::from_secs(10),
    }
}

/// A `Registry` fake exercising only the one method `HotMatcher::classify`
/// actually calls (`resolve_structural`) — every other method panics if
/// called, matching `deblob-match`'s own hot-path invariant: the tag-and-
/// forward core never publishes, never reads a schema by id, never lists
/// (spec §3.1).
struct FakeRegistry {
    /// `Some(id)` => every `resolve_structural` call answers `Known(id)`;
    /// `None` => answers `Provisional` (index miss).
    resolve: Option<SchemaId>,
}

#[async_trait::async_trait]
impl Registry for FakeRegistry {
    async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn resolve_structural(
        &self,
        _bucket_key: &str,
        _fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        Ok(self.resolve.clone())
    }

    async fn publish(
        &self,
        _record: SchemaRecord,
        _alias_from: &CandidateId,
        _bucket_key: &str,
        _variant_members: &[(String, String)],
        _actor: &str,
        _reason: &str,
    ) -> Result<FamilyVersion, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_schemas(
        &self,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_families_in_buckets(
        &self,
        _bucket_keys: &[String],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_families_by_band_depth(
        &self,
        _bands: &[u32],
        _depths: &[u32],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }
}

// Silence "never constructed"/"never called" warnings on the unused parts
// of the trait surface above — these exist only so `FakeRegistry` compiles
// as a full `Registry` impl, mirroring `deblob-match`'s own test fixture.
#[allow(dead_code)]
fn _unused_shape(_r: CandidateRecord) -> CandidateState {
    CandidateState::Provisional
}
#[allow(dead_code)]
fn _unused_family_id(_f: FamilyId) {}

fn matcher(resolve: Option<SchemaId>) -> Arc<HotMatcher> {
    let registry: Arc<dyn Registry> = Arc::new(FakeRegistry { resolve });
    Arc::new(HotMatcher::new(registry, 16, Metrics::new()))
}

/// One request the test upstream observed.
#[derive(Debug, Clone)]
struct CapturedRequest {
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Clone)]
struct UpstreamState {
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
}

async fn upstream_handler(
    State(state): State<UpstreamState>,
    headers: HeaderMap,
    body: Bytes,
) -> &'static str {
    state
        .captured
        .lock()
        .unwrap()
        .push(CapturedRequest { headers, body });
    "ok"
}

/// Spins up an in-process axum server standing in for the allowlisted
/// upstream, returning its bound address and a handle to every request it
/// received.
async fn spawn_test_upstream() -> (Url, Arc<Mutex<Vec<CapturedRequest>>>) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let state = UpstreamState {
        captured: captured.clone(),
    };
    let router = Router::new()
        .route("/upstream", post(upstream_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test upstream");
    let addr = listener.local_addr().expect("upstream local addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    let url = Url::parse(&format!("http://{addr}/upstream")).expect("valid upstream url");
    (url, captured)
}

/// Picks a free ephemeral port synchronously (bind-then-drop) so the
/// `HttpProxyCfg` under test can be built before `HttpProxy::run` is
/// spawned — `run` owns its own bind step and doesn't hand the bound
/// address back to the caller.
fn free_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr")
}

async fn wait_for_proxy(addr: SocketAddr) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("proxy at {addr} never became reachable");
}

#[tokio::test]
async fn known_or_provisional_shape_tagged_and_forwarded() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    // Index miss => `Provisional(cand_...)`, exercising the same
    // deterministic decision table the Kafka relay uses.
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let body = br#"{"a":1,"b":"two"}"#.to_vec();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .header("content-type", "application/json")
        .body(body.clone())
        .send()
        .await
        .expect("request to proxy succeeds");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let response_tag = response
        .headers()
        .get("deblob-schema-id")
        .expect("response carries the schema tag")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        response_tag.starts_with("sch_") || response_tag.starts_with("cand_"),
        "unexpected tag: {response_tag}"
    );
    let response_body = response.bytes().await.expect("read client response body");
    assert_eq!(response_body.as_ref(), b"ok");

    {
        // Scoped so the `MutexGuard` is dropped before the `.await`s below
        // (clippy::await_holding_lock) — the assertions themselves need no
        // lock held across an await point, only synchronous access.
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1, "upstream received exactly one request");
        let received = &captured[0];
        assert_eq!(
            received.body.as_ref(),
            body.as_slice(),
            "upstream received the SAME body bytes, unmutated"
        );
        let forwarded_tags: Vec<_> = received
            .headers
            .get_all("deblob-schema-id")
            .iter()
            .collect();
        assert_eq!(
            forwarded_tags.len(),
            1,
            "exactly one deblob-schema-id header was forwarded"
        );
        assert_eq!(forwarded_tags[0].to_str().unwrap(), response_tag);
        let forwarded_origin: Vec<_> = received.headers.get_all("deblob-origin").iter().collect();
        assert_eq!(forwarded_origin.len(), 1);
        assert!(forwarded_origin[0].to_str().unwrap().starts_with("http/"));
    }

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn spoofed_tag_stripped_then_replaced() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    let known_id = SchemaId::from_digest(&[3u8; 32]);
    let matcher = matcher(Some(known_id.clone()));
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .header("deblob-schema-id", "sch_forged")
        .header("Deblob-Origin", "evil/0/0")
        .body(br#"{"x":true}"#.to_vec())
        .send()
        .await
        .expect("request to proxy succeeds");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    {
        // Same MutexGuard-across-await scoping as above.
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        let received = &captured[0];

        let tags: Vec<_> = received
            .headers
            .get_all("deblob-schema-id")
            .iter()
            .collect();
        assert_eq!(
            tags.len(),
            1,
            "the forged header must not survive alongside the real one"
        );
        assert_eq!(
            tags[0].to_str().unwrap(),
            known_id.as_str(),
            "upstream must see deblob's OWN tag, never the forged value"
        );
        let origins: Vec<_> = received.headers.get_all("deblob-origin").iter().collect();
        assert_eq!(origins.len(), 1);
        assert_ne!(origins[0].to_str().unwrap(), "evil/0/0");
    }

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------
// Task 2: hardening (spec §4/§6) — each rule below is one test.
// ---------------------------------------------------------------------

/// A `Registry` fake whose `resolve_structural` always errors, standing
/// in for a Redis/registry outage (spec §6: "registry down → tag
/// `unresolved`, still forward"). Every other method panics, matching
/// `FakeRegistry`'s own hot-path-only invariant.
struct DownRegistry;

#[async_trait::async_trait]
impl Registry for DownRegistry {
    async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn resolve_structural(
        &self,
        _bucket_key: &str,
        _fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        Err(CoreError::RegistryUnavailable("registry down".into()))
    }

    async fn publish(
        &self,
        _record: SchemaRecord,
        _alias_from: &CandidateId,
        _bucket_key: &str,
        _variant_members: &[(String, String)],
        _actor: &str,
        _reason: &str,
    ) -> Result<FamilyVersion, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_schemas(
        &self,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_families_in_buckets(
        &self,
        _bucket_keys: &[String],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }

    async fn list_families_by_band_depth(
        &self,
        _bands: &[u32],
        _depths: &[u32],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        panic!("not exercised by HotMatcher::classify")
    }
}

fn matcher_with_registry_down() -> Arc<HotMatcher> {
    let registry: Arc<dyn Registry> = Arc::new(DownRegistry);
    Arc::new(HotMatcher::new(registry, 16, Metrics::new()))
}

/// Rule: body over `max_body_bytes` → 413, upstream NEVER called (spec
/// §4/§6).
#[tokio::test]
async fn oversize_body_413() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let mut cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    cfg.max_body_bytes = 16;
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let oversized_body = vec![b'a'; 1024];
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .body(oversized_body)
        .send()
        .await
        .expect("request to proxy succeeds");

    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        captured.lock().unwrap().len(),
        0,
        "an oversize body must never reach the upstream"
    );

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: `Malformed` classification → 422 +
/// `deblob-quarantine-reason: <reason>`, NEVER forwarded upstream (spec
/// §6). Body is a duplicate-key JSON object, exercising the exact
/// `QuarantineReason::DuplicateKey` path `deblob-fingerprint::parse_bounded`
/// rejects.
#[tokio::test]
async fn malformed_422_not_forwarded() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let malformed_body = br#"{"a":1,"a":2}"#.to_vec();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .header("content-type", "application/json")
        .body(malformed_body)
        .send()
        .await
        .expect("request to proxy succeeds");

    assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let reason = response
        .headers()
        .get("deblob-quarantine-reason")
        .expect("quarantine reason header present")
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(reason, "duplicate_key");
    assert_eq!(
        captured.lock().unwrap().len(),
        0,
        "a malformed body must never reach the upstream"
    );

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: a configured `route` that isn't a member of
/// `upstream_allowlist` must be rejected AT CONSTRUCTION — `HttpProxy::run`
/// never binds a listener, so no request can ever reach an off-allowlist
/// destination (spec §4: SSRF prevention).
#[tokio::test]
async fn disallowed_upstream_rejected() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let allowlisted_only = Url::parse("http://127.0.0.1:1/never-allowlisted")
        .expect("valid url — never actually dialed, construction fails first");
    // `route` (the real, reachable test upstream) is deliberately NOT a
    // member of `upstream_allowlist` (some unrelated, unreachable URL).
    let cfg = generous_cfg(listen_addr, vec![allowlisted_only], upstream_url);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();

    let result = HttpProxy::run(cfg, matcher, None, shutdown).await;

    assert!(
        matches!(result, Err(HttpProxyError::RouteNotAllowlisted)),
        "expected RouteNotAllowlisted, got {result:?}"
    );
    assert_eq!(
        captured.lock().unwrap().len(),
        0,
        "a proxy that failed to construct must never have forwarded anything"
    );
}

/// Rule: a request carrying BOTH `Content-Length` and `Transfer-Encoding`
/// → 400, never forwarded (spec §4/§6 request-smuggling defense). Written
/// against a raw `TcpStream` — deliberately bypassing `reqwest`'s own
/// header hygiene, which won't let a caller set an ambiguous framing pair
/// — so the exact wire-level request a hardened stack must reject is what
/// actually gets sent.
#[tokio::test]
async fn cl_and_te_rejected() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let raw_request = format!(
        "POST /ingest HTTP/1.1\r\n\
         Host: {listen_addr}\r\n\
         Content-Length: 10\r\n\
         Transfer-Encoding: chunked\r\n\
         Connection: close\r\n\
         \r\n\
         0123456789"
    );
    let response_text = send_raw_request(listen_addr, &raw_request).await;

    // Either our own explicit guard fires (400), or hyper/axum's own h1
    // framing layer already refuses to deliver the ambiguous request to
    // the handler (also surfaced as a 400, or the connection is closed
    // outright with no response at all) — either way it must NOT be a
    // 2xx, and the upstream must never see it.
    if let Some(status_line) = response_text.lines().next() {
        assert!(
            !status_line.contains(" 200 ") && !status_line.contains(" 2"),
            "ambiguous CL+TE framing must never succeed, got: {status_line}"
        );
    }
    assert_eq!(
        captured.lock().unwrap().len(),
        0,
        "an ambiguous-framing request must never reach the upstream"
    );

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: duplicate/conflicting `Content-Length` headers → 400, never
/// forwarded (spec §4/§6 request-smuggling defense). Same raw-socket
/// rationale as [`cl_and_te_rejected`].
#[tokio::test]
async fn duplicate_content_length_rejected() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let raw_request = format!(
        "POST /ingest HTTP/1.1\r\n\
         Host: {listen_addr}\r\n\
         Content-Length: 5\r\n\
         Content-Length: 10\r\n\
         Connection: close\r\n\
         \r\n\
         01234"
    );
    let response_text = send_raw_request(listen_addr, &raw_request).await;

    if let Some(status_line) = response_text.lines().next() {
        assert!(
            !status_line.contains(" 200 ") && !status_line.contains(" 2"),
            "duplicate Content-Length must never succeed, got: {status_line}"
        );
    }
    assert_eq!(
        captured.lock().unwrap().len(),
        0,
        "a duplicate-Content-Length request must never reach the upstream"
    );

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: total header bytes/count over the configured cap → 431, never
/// forwarded (spec §4).
#[tokio::test]
async fn oversized_headers_rejected() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let mut cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    cfg.max_header_bytes = 64;
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .header("x-oversized", "v".repeat(1024))
        .body(br#"{"a":1}"#.to_vec())
        .send()
        .await
        .expect("request to proxy succeeds");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
    );
    assert_eq!(captured.lock().unwrap().len(), 0);

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: an upstream that never responds within `upstream_timeout` → 504,
/// bounded — never a hang (spec §4/§6).
#[tokio::test]
async fn slow_upstream_times_out() {
    let listen_addr = free_addr();
    let slow_listener = StdTcpListener::bind("127.0.0.1:0").expect("bind slow upstream");
    slow_listener
        .set_nonblocking(true)
        .expect("set slow upstream listener non-blocking");
    let slow_addr = slow_listener.local_addr().expect("slow upstream addr");
    let slow_upstream_url =
        Url::parse(&format!("http://{slow_addr}/upstream")).expect("valid slow upstream url");

    let slow_router = Router::new().route(
        "/upstream",
        post(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            "too slow"
        }),
    );
    let tokio_listener = tokio::net::TcpListener::from_std(slow_listener).expect("tokio listener");
    tokio::spawn(async move {
        axum::serve(tokio_listener, slow_router).await.ok();
    });

    let mut cfg = generous_cfg(
        listen_addr,
        vec![slow_upstream_url.clone()],
        slow_upstream_url,
    );
    cfg.upstream_timeout = Duration::from_millis(200);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let started = std::time::Instant::now();
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .body(br#"{"a":1}"#.to_vec())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("request to proxy succeeds (the PROXY must respond, even if upstream is slow)");
    let elapsed = started.elapsed();

    assert_eq!(response.status(), reqwest::StatusCode::GATEWAY_TIMEOUT);
    assert!(
        elapsed < Duration::from_secs(2),
        "the forward timeout must bound the request, took {elapsed:?}"
    );

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: registry outage → `HotMatcher::classify` yields `Unresolved`
/// (never `cand_`), and the proxy STILL forwards — degrade, don't block
/// (spec §6).
#[tokio::test]
async fn registry_down_tags_unresolved_and_forwards() {
    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    let matcher = matcher_with_registry_down();
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .body(br#"{"a":1}"#.to_vec())
        .send()
        .await
        .expect("request to proxy succeeds");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let tag = response
        .headers()
        .get("deblob-schema-id")
        .expect("tag header present")
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(tag, "unresolved");

    {
        let captured = captured.lock().unwrap();
        assert_eq!(
            captured.len(),
            1,
            "a registry outage must still forward, not block"
        );
        let forwarded_tag = captured[0]
            .headers
            .get("deblob-schema-id")
            .expect("forwarded tag header present")
            .to_str()
            .unwrap();
        assert_eq!(forwarded_tag, "unresolved");
    }

    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Rule: a client that dribbles header bytes without ever completing the
/// header block is bounded by `header_read_timeout`, not able to hold a
/// connection (and the accept loop behind it) open indefinitely (spec
/// §4: "bounded read, write, and HEADER timeouts" — the Slowloris
/// defense specifically on the HEADER path, distinct from
/// `request_timeout`, which only bounds slow BODY delivery once headers
/// are already fully parsed).
#[tokio::test]
async fn slow_header_client_is_bounded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (upstream_url, captured) = spawn_test_upstream().await;
    let listen_addr = free_addr();
    let mut cfg = generous_cfg(listen_addr, vec![upstream_url.clone()], upstream_url);
    cfg.header_read_timeout = Duration::from_millis(500);
    let matcher = matcher(None);
    let shutdown = CancellationToken::new();
    let run_shutdown = shutdown.clone();
    let proxy_handle =
        tokio::spawn(async move { HttpProxy::run(cfg, matcher, None, run_shutdown).await });
    wait_for_proxy(listen_addr).await;

    // Open a raw connection, write a partial request line plus ONE
    // header byte, then stall — no blank line, no rest of the header
    // value, the header block is never completed.
    let mut slow_stream = tokio::net::TcpStream::connect(listen_addr)
        .await
        .expect("connect slow raw socket to proxy");
    slow_stream
        .write_all(b"POST /ingest HTTP/1.1\r\nHost: x\r\nX-Slow: a")
        .await
        .expect("write partial header block");

    // The server must close/error the stalled connection within
    // `header_read_timeout` + a generous margin — never hang
    // indefinitely waiting for the header block to complete.
    let read_result = tokio::time::timeout(Duration::from_secs(3), async {
        let mut buf = [0u8; 16];
        slow_stream.read(&mut buf).await
    })
    .await;
    assert!(
        read_result.is_ok(),
        "server did not close/respond to a stalled-header connection within \
         header_read_timeout + margin — the Slowloris connection is unbounded"
    );
    // Whatever happened on that read (clean EOF, an error response, a
    // reset) is fine — what matters is it happened, not what it was.

    // Prove the stalled connection didn't block or exhaust the server:
    // a well-formed request on a SEPARATE connection still succeeds
    // promptly.
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{listen_addr}/ingest"))
        .header("content-type", "application/json")
        .body(br#"{"a":1}"#.to_vec())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("well-formed request on a separate connection still succeeds");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        captured.lock().unwrap().len(),
        1,
        "the well-formed request forwarded normally; the stalled one never did"
    );

    drop(slow_stream);
    shutdown.cancel();
    proxy_handle.await.unwrap().unwrap();
}

/// Writes `raw_request` verbatim to a fresh `TcpStream` connected to
/// `addr`, reads whatever comes back (tolerating a connection close with
/// no bytes at all — some hardened stacks refuse ambiguous framing by
/// dropping the connection rather than responding), and returns it as a
/// lossy UTF-8 string.
async fn send_raw_request(addr: SocketAddr, raw_request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect raw socket to proxy");
    stream
        .write_all(raw_request.as_bytes())
        .await
        .expect("write raw request");

    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).into_owned()
}
