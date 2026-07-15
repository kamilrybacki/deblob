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
use deblob_http::{HttpProxy, HttpProxyCfg};
use deblob_match::matcher::HotMatcher;
use deblob_match::metrics::Metrics;
use tokio_util::sync::CancellationToken;
use url::Url;

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
    let cfg = HttpProxyCfg {
        listen_addr,
        upstream_allowlist: vec![upstream_url.clone()],
        route: upstream_url,
        limits: Limits::default(),
    };
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
    let cfg = HttpProxyCfg {
        listen_addr,
        upstream_allowlist: vec![upstream_url.clone()],
        route: upstream_url,
        limits: Limits::default(),
    };
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
