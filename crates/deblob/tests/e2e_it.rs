//! Task 19: end-to-end acceptance test exercising the FULL pipeline
//! against real Kafka + Redis containers, through the SAME `deblob::serve
//! ::serve` wiring the production binary runs (Task 19's split of
//! `main.rs`) — not a test-only stand-in.
//!
//! Scenario (spec + Task 19 brief), all in one ordered test since every
//! step depends on state built by the previous one (produced records,
//! cold-lane accumulation, a promoted schema id, an outage window):
//!
//!  1. produce 5 messages of shape A, 3 of shape B (= A + one optional
//!     field), 1 duplicate-key malformed, 1 tombstone;
//!  2. assert on `events.tagged`/quarantine: the 5 shape-A messages carry
//!     an IDENTICAL provisional (`cand_`) id; malformed routes to
//!     quarantine with `deblob-quarantine-reason: duplicate_key`; the
//!     tombstone tags `tombstone` with a null value;
//!  3. the cold lane (fed by the discovery-topic consumer) clusters shape
//!     A + shape B into ONE candidate, `sample_count == 8`;
//!  4. promote that candidate via the management API (bearer token) → 201
//!     + `Location`;
//!  5. produce 2 more shape-A messages → now tagged with the PROMOTED
//!     `sch_` id, not `cand_` — the promote→resolve round trip;
//!  6. stop the Redis container, produce one more message → tagged
//!     `unresolved` (never `cand_`, spec §10's outage-safe rule); restart
//!     Redis, produce another → tagging recovers to `sch_`. (Shape-B
//!     shaped, deliberately not shape-A — see the comment at that step for
//!     why reusing shape A would make the outage assertion vacuous.)
//!
//! Every assertion that depends on an asynchronous background effect
//! (discovery ingest, promotion propagating to the hot path, health
//! recovery) is deadline-based polling, never a bare `sleep` — see the
//! `poll_*`/`drain_*` helpers below, which mirror the same pattern already
//! used by `deblob-kafka`'s `relay_it.rs`/`chaos_it.rs`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use deblob::config::{
    Config, HttpProxyConfig, KafkaConfig, LimitsConfig, ManagementConfig, PromotionConfig, Secrets,
    SemanticConfig, SlmConfig,
};
use deblob::promote::{FamilyChoice, PromoteRequest};
use deblob::serve::serve;
use deblob_core::id::CandidateId;
use deblob_core::ports::EvidenceStore;
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde_json::Value;
use testcontainers_modules::kafka::apache;
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------
// Container / topic setup helpers.
// ---------------------------------------------------------------------

/// Single-node KRaft `apache/kafka` container with the internal
/// `__transaction_state` topic's replication factor forced down to 1 —
/// same rationale as `deblob-kafka`'s own `relay_it.rs::start_kafka`:
/// without this, `Producer::init_transactions` never completes against a
/// single-broker cluster.
async fn start_kafka() -> ContainerAsync<apache::Kafka> {
    apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container must start")
}

/// Host port `start_redis` pins the container's Redis port to. Testcontainers
/// normally assigns a fresh random host port every time a *stopped*
/// container is `start()`-ed again, which would silently move the address
/// out from under an already-running `serve()` that dialed the original
/// `redis_url` once at startup — step 6's outage-recovery assertion would
/// then be testing "did we reconnect to a Redis that isn't even listening
/// on the port we're connected to" rather than genuine recovery. Pinning a
/// fixed host port keeps `redis_url` identical across `stop()`/`start()`, so
/// the ONLY variable in step 6 is whether Redis itself is reachable.
///
/// Chosen as an uncommon high port unlikely to collide with any other
/// service already bound on this shared host (see the port-collision check
/// in the Task 19 report) — deliberately far from Redis's own default 6379.
const REDIS_FIXED_HOST_PORT: u16 = 16399;

/// AOF-enabled Redis container — required so `RedisRegistry::connect`'s
/// persistence gate passes WITHOUT `--unsafe-volatile` (spec §6), matching
/// how a real deployment is expected to run. AOF also matters across the
/// step-6 stop/start cycle: `stop()`/`start()` reuse the SAME container
/// (and its writable layer), so on restart Redis replays the AOF file and
/// the promoted schema + structural index the test already published are
/// still there — not just the port, the DATA survives too.
///
/// The host port is pinned via `with_mapped_port` (see
/// `REDIS_FIXED_HOST_PORT`) instead of left to testcontainers' default
/// random-port allocation, specifically so it stays identical across the
/// step-6 `stop()`/`start()` cycle.
async fn start_redis() -> ContainerAsync<Redis> {
    Redis::default()
        .with_cmd(["--appendonly", "yes"])
        .with_mapped_port(REDIS_FIXED_HOST_PORT, REDIS_PORT.tcp())
        .start()
        .await
        .expect("redis container must start")
}

async fn create_topics(brokers: &str, names: &[&str]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("admin client");
    let new_topics: Vec<NewTopic> = names
        .iter()
        .map(|n| NewTopic::new(n, 1, TopicReplication::Fixed(1)))
        .collect();
    let refs: Vec<&NewTopic> = new_topics.iter().collect();
    let results = admin
        .create_topics(refs, &AdminOptions::new())
        .await
        .expect("create_topics call");
    for r in results {
        r.expect("topic creation must succeed");
    }
}

fn raw_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("raw producer")
}

async fn produce_raw(
    producer: &FutureProducer,
    topic: &str,
    key: Option<&[u8]>,
    payload: Option<&[u8]>,
) {
    let mut record = FutureRecord::<[u8], [u8]>::to(topic).partition(0);
    if let Some(k) = key {
        record = record.key(k);
    }
    if let Some(p) = payload {
        record = record.payload(p);
    }
    producer
        .send(record, Duration::from_secs(5))
        .await
        .expect("produce raw record");
}

/// A verification consumer: `isolation.level=read_committed` so it can
/// NEVER observe a record from an aborted or still-open transaction (same
/// rationale as every other Kafka integration test in this workspace).
fn committed_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .create()
        .expect("verification consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    consumer
}

async fn recv_owned(consumer: &StreamConsumer, timeout: Duration) -> OwnedMessage {
    let msg = tokio::time::timeout(timeout, consumer.recv())
        .await
        .expect("message within timeout")
        .expect("no kafka error");
    msg.detach()
}

/// Drains exactly `n` messages from `consumer`, bounded by an overall
/// `deadline` (not `n` individual per-message timeouts) — a slow first
/// message (e.g. container/consumer-group warm-up) doesn't eat into the
/// budget for the rest.
async fn drain_n(consumer: &StreamConsumer, n: usize, deadline: Duration) -> Vec<OwnedMessage> {
    let mut out = Vec::with_capacity(n);
    let end = tokio::time::Instant::now() + deadline;
    while out.len() < n {
        let remaining = end.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out draining {n} messages: only {} arrived",
            out.len()
        );
        let msg = recv_owned(consumer, remaining.min(Duration::from_secs(15))).await;
        out.push(msg);
    }
    out
}

fn header_map(headers: Option<&OwnedHeaders>) -> HashMap<String, Option<Vec<u8>>> {
    let mut map = HashMap::new();
    if let Some(headers) = headers {
        for h in headers.iter() {
            map.insert(h.key.to_string(), h.value.map(|v| v.to_vec()));
        }
    }
    map
}

fn schema_id_header(msg: &OwnedMessage) -> String {
    let headers = header_map(msg.headers());
    String::from_utf8(
        headers
            .get("deblob-schema-id")
            .expect("deblob-schema-id header present")
            .clone()
            .expect("deblob-schema-id header has a value"),
    )
    .expect("deblob-schema-id header is utf8")
}

// ---------------------------------------------------------------------
// Minimal hand-rolled HTTP/1.1 client for the management API. No new
// dependency is worth pulling in for one authenticated POST + a couple of
// GETs; `serve()` binds a real `tokio::net::TcpListener` (the exact same
// axum server the production binary runs), so exercising it means talking
// real HTTP over a real socket, not calling the router in-process.
// ---------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Value,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn connect_with_retry(addr: SocketAddr, deadline: Duration) -> TcpStream {
    let start = tokio::time::Instant::now();
    loop {
        match TcpStream::connect(addr).await {
            Ok(s) => return s,
            Err(e) => {
                assert!(
                    start.elapsed() <= deadline,
                    "could not connect to management API at {addr} within {deadline:?}: {e}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&[u8]>,
) -> HttpResponse {
    let mut stream = connect_with_retry(addr, Duration::from_secs(20)).await;

    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(token) = bearer {
        req.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .expect("write request line/headers");
    if let Some(b) = body {
        stream.write_all(b).await.expect("write request body");
    }

    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = tokio::time::timeout(Duration::from_secs(15), stream.read(&mut chunk))
            .await
            .expect("reading response headers timed out")
            .expect("read error while waiting for response headers");
        assert!(n != 0, "connection closed before full headers received");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.lines();
    let status_line = lines.next().expect("status line present");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status line has a code")
        .parse()
        .expect("status code is numeric");

    let mut headers = HashMap::new();
    let mut content_length: Option<usize> = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let value = v.trim().to_string();
            if key == "content-length" {
                content_length = value.parse().ok();
            }
            headers.insert(key, value);
        }
    }

    let mut body_bytes = buf[header_end + 4..].to_vec();
    if let Some(len) = content_length {
        while body_bytes.len() < len {
            let n = tokio::time::timeout(Duration::from_secs(15), stream.read(&mut chunk))
                .await
                .expect("reading response body timed out")
                .expect("read error while waiting for response body");
            if n == 0 {
                break;
            }
            body_bytes.extend_from_slice(&chunk[..n]);
        }
    }

    let body: Value = if body_bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body_bytes).unwrap_or(Value::Null)
    };

    HttpResponse {
        status,
        headers,
        body,
    }
}

// ---------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------

#[tokio::test]
async fn full_pipeline_produce_tag_cluster_promote_and_recover_from_outage() {
    // --- Setup: real Kafka + Redis containers, topics, and the FULL
    // `serve()` wiring (relay + discovery consumer + cold lane + promoter
    // + management API), exactly as the production binary runs it. ---
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let redis = start_redis().await;
    // `start_redis` pins the host port to `REDIS_FIXED_HOST_PORT` (see its
    // doc comment), so this MUST equal that constant — assert it rather
    // than silently trusting the pin, since a drift here would silently
    // reintroduce the exact bug this fix addresses.
    let initial_redis_port = redis
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("mapped redis port");
    assert_eq!(
        initial_redis_port, REDIS_FIXED_HOST_PORT,
        "redis container did not bind the pinned fixed host port"
    );
    let redis_url = format!("redis://127.0.0.1:{initial_redis_port}");

    let raw_topic = "e2e-raw".to_string();
    let tagged_topic = "e2e-tagged".to_string();
    let discovery_topic = "e2e-discovery".to_string();
    let quarantine_topic = "e2e-quarantine".to_string();
    create_topics(
        &brokers,
        &[
            &raw_topic,
            &tagged_topic,
            &discovery_topic,
            &quarantine_topic,
        ],
    )
    .await;

    let management_port = {
        // Bind-then-drop to grab a free ephemeral port for `serve()`'s
        // listener; a brief TOCTOU window is an accepted, standard
        // integration-test pattern.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port probe");
        probe.local_addr().expect("local addr").port()
    };
    let management_addr: SocketAddr = format!("127.0.0.1:{management_port}")
        .parse()
        .expect("valid socket addr");

    const API_TOKEN: &str = "e2e-test-bearer-token";

    let config = Config {
        kafka: KafkaConfig {
            raw_topic: raw_topic.clone(),
            raw_topics: Vec::new(),
            tagged_topic: tagged_topic.clone(),
            discovery_topic: discovery_topic.clone(),
            quarantine_topic: quarantine_topic.clone(),
            group_id: "e2e-group".to_string(),
            transactional_id: "e2e-relay-txn".to_string(),
            // This test's stages depend on producing a small number of
            // records and observing their tags before producing the next
            // stage — the pre-batching per-record-transaction escape
            // hatch (batching spec §3) keeps that behavior exact rather
            // than depending on the linger timer's timing.
            max_batch_records: 1,
            max_batch_linger_ms: 100,
        },
        limits: LimitsConfig::default(),
        // Task 19 brief: promotion guards configured low enough for the
        // test's 8 accumulated samples to clear, and `min_age_ms` low
        // enough that the test doesn't have to wait out the production
        // 5-minute default.
        promotion: PromotionConfig {
            min_samples: 8,
            min_age_ms: 0,
        },
        management: ManagementConfig {
            addr: management_addr.to_string(),
        },
        // Task 5b: the SLM shadow lane is out of scope for this P1
        // pipeline test — `SlmConfig::default()` (`enabled: false`) means
        // `serve()` wires up no `HttpInferencer`/`RedisShadowLog`/sweep
        // task at all, so this e2e test's behavior is unaffected by the
        // shadow lane's existence.
        slm: SlmConfig::default(),
        // Task 4 (P2-C): the HTTP push reverse proxy is out of scope for
        // this P1 pipeline test — `HttpProxyConfig::default()`
        // (`enabled: false`) means `serve()` wires up no `HttpProxyCfg`/
        // `KafkaDiscoverySink`/listener at all, so this e2e test's
        // behavior is unaffected by the HTTP proxy's existence.
        http_proxy: HttpProxyConfig::default(),
        // P2-D Task 8 follow-up (A1): out of scope for this P1 pipeline
        // test — both lists empty means `serve()` seeds an empty
        // `Registries`, unchanged from every pre-A1 test.
        semantic: SemanticConfig::default(),
    umbrella: Default::default(),
    };
    let secrets = Secrets {
        api_token: API_TOKEN.to_string(),
        redis_url: redis_url.clone(),
        kafka_brokers: brokers.clone(),
        kafka_sasl: None,
        slm_api_token: None,
        http_ingest_token: None,
    };
    // AOF is on (see `start_redis`), so the persistence gate must pass
    // WITHOUT `--unsafe-volatile` — `allow_volatile: false` here is the
    // point of the assertion, not an oversight.
    let redis_opts = RedisOpts {
        allow_volatile: false,
    };

    let shutdown = CancellationToken::new();
    let serve_handle = tokio::spawn(serve(config, secrets, redis_opts, shutdown.clone()));

    // Sanity: the management API must actually come up (connect retries
    // internally up to 20s) before we lean on it later — a fast, cheap
    // signal that `serve()` didn't die on startup (e.g. a bad Redis
    // persistence gate, a bind failure).
    let boot = http_request(management_addr, "GET", "/healthz", None, None).await;
    assert_eq!(
        boot.status, 200,
        "management API /healthz must be reachable"
    );

    // ------------------------------------------------------------------
    // Step 1: produce 5xA, 3xB(=A+optional field), 1 duplicate-key
    // malformed, 1 tombstone.
    // ------------------------------------------------------------------
    let producer = raw_producer(&brokers);

    let shape_a: Vec<&str> = vec![
        r#"{"user":"u1","ts":1}"#,
        r#"{"user":"u2","ts":2}"#,
        r#"{"user":"u3","ts":3}"#,
        r#"{"user":"u4","ts":4}"#,
        r#"{"user":"u5","ts":5}"#,
    ];
    let shape_b: Vec<&str> = vec![
        r#"{"user":"u6","ts":6,"note":"a"}"#,
        r#"{"user":"u7","ts":7,"note":"b"}"#,
        r#"{"user":"u8","ts":8,"note":"c"}"#,
    ];
    let malformed: &str = r#"{"a":1,"a":2}"#;
    let tombstone_key: &[u8] = b"e2e-tombstone-key";

    for payload in &shape_a {
        produce_raw(&producer, &raw_topic, None, Some(payload.as_bytes())).await;
    }
    for payload in &shape_b {
        produce_raw(&producer, &raw_topic, None, Some(payload.as_bytes())).await;
    }
    produce_raw(&producer, &raw_topic, None, Some(malformed.as_bytes())).await;
    produce_raw(&producer, &raw_topic, Some(tombstone_key), None).await;

    // ------------------------------------------------------------------
    // Step 2: assert on `events.tagged` (read_committed) + quarantine.
    // ------------------------------------------------------------------
    let tagged_consumer = committed_consumer(&brokers, "e2e-tagged-verify", &tagged_topic);
    // 5 (A) + 3 (B) + 1 (tombstone) = 9 tagged records; the malformed one
    // routes to quarantine only.
    let tagged = drain_n(&tagged_consumer, 9, Duration::from_secs(90)).await;

    let mut a_schema_ids: Vec<String> = Vec::new();
    let mut b_count = 0usize;
    let mut tombstone_seen = false;

    for msg in &tagged {
        match msg.payload() {
            Some(payload_bytes) => {
                let text = std::str::from_utf8(payload_bytes).expect("tagged payload is utf8");
                if shape_a.contains(&text) {
                    a_schema_ids.push(schema_id_header(msg));
                } else if shape_b.contains(&text) {
                    b_count += 1;
                    let id = schema_id_header(msg);
                    assert!(
                        id.starts_with("cand_"),
                        "shape-B message must tag Provisional too: {id}"
                    );
                } else {
                    panic!("unexpected tagged payload: {text}");
                }
            }
            None => {
                assert_eq!(
                    msg.key(),
                    Some(tombstone_key),
                    "tombstone key must be preserved"
                );
                assert_eq!(schema_id_header(msg), "tombstone");
                tombstone_seen = true;
            }
        }
    }

    assert_eq!(
        a_schema_ids.len(),
        5,
        "all 5 shape-A messages must be tagged"
    );
    assert_eq!(b_count, 3, "all 3 shape-B messages must be tagged");
    assert!(tombstone_seen, "the tombstone record must be tagged");

    let cand_id_str = a_schema_ids[0].clone();
    assert!(
        cand_id_str.starts_with("cand_"),
        "shape-A must tag Provisional: {cand_id_str}"
    );
    for id in &a_schema_ids {
        assert_eq!(
            id, &cand_id_str,
            "every shape-A message must carry the IDENTICAL provisional id"
        );
    }
    eprintln!(
        "[e2e step 2] OK: 5x shape-A tagged identical {cand_id_str}, 3x shape-B tagged Provisional, tombstone tagged"
    );

    let quarantine_consumer =
        committed_consumer(&brokers, "e2e-quarantine-verify", &quarantine_topic);
    let quarantined = drain_n(&quarantine_consumer, 1, Duration::from_secs(30)).await;
    let q_headers = header_map(quarantined[0].headers());
    assert_eq!(
        q_headers.get("deblob-schema-id").unwrap().as_deref(),
        Some(b"malformed".as_slice())
    );
    assert_eq!(
        q_headers
            .get("deblob-quarantine-reason")
            .unwrap()
            .as_deref(),
        Some(b"duplicate_key".as_slice())
    );
    assert_eq!(
        quarantined[0].payload(),
        Some(malformed.as_bytes()),
        "malformed payload must be preserved, never silently dropped"
    );
    eprintln!(
        "[e2e step 2] OK: malformed record quarantined with reason duplicate_key, payload intact"
    );

    // ------------------------------------------------------------------
    // Step 3: cold lane (fed by the discovery-topic consumer) clusters
    // shape A + shape B into ONE candidate, sample_count == 8. Polled —
    // discovery ingestion is asynchronous relative to the tagged-topic
    // produce we just observed.
    // ------------------------------------------------------------------
    let evidence = RedisEvidence::connect(&redis_url, RedisEvidenceOpts::default(), redis_opts)
        .await
        .expect("connect evidence store for polling");
    let cand_id = CandidateId::parse(&cand_id_str).expect("valid cand_ id");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut last_seen_count = None;
    loop {
        let record = evidence
            .get_candidate(&cand_id)
            .await
            .expect("evidence store reachable");
        if let Some(rec) = &record {
            last_seen_count = Some(rec.sample_count);
            if rec.sample_count == 8 {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cold lane never accumulated sample_count == 8 within deadline; last seen: {last_seen_count:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!("[e2e step 3] OK: cold lane clustered shape A + shape B into candidate {cand_id_str} with sample_count == 8");

    // ------------------------------------------------------------------
    // Step 4: promote via the management API (bearer token) -> 201 +
    // Location.
    // ------------------------------------------------------------------
    let promote_req = PromoteRequest {
        family: FamilyChoice::New,
        name: Some("e2e.user_event".to_string()),
        reason: "e2e capstone promotion".to_string(),
    };
    let promote_body = serde_json::to_vec(&promote_req).expect("serialize promote request");
    let promote_path = format!("/api/v1/candidates/{cand_id_str}/promote");
    let promote_resp = http_request(
        management_addr,
        "POST",
        &promote_path,
        Some(API_TOKEN),
        Some(&promote_body),
    )
    .await;

    assert_eq!(
        promote_resp.status, 201,
        "promote must return 201, got body: {:?}",
        promote_resp.body
    );
    let location = promote_resp
        .headers
        .get("location")
        .expect("Location header present on 201")
        .clone();
    let promoted_schema_id = promote_resp.body["data"]["schema_id"]
        .as_str()
        .expect("response body carries data.schema_id")
        .to_string();
    assert_eq!(location, format!("/api/v1/schemas/{promoted_schema_id}"));
    assert!(
        promoted_schema_id.starts_with("sch_"),
        "promoted id must be a sch_ id: {promoted_schema_id}"
    );
    eprintln!("[e2e step 4] OK: promoted {cand_id_str} -> {promoted_schema_id}, 201 + Location");

    // ------------------------------------------------------------------
    // Step 5: produce 2 more shape-A messages -> now tagged with the
    // PROMOTED sch_ id, never cand_. This is the promote->resolve round
    // trip through the REAL hot path: per Task 11's design, the hot-path
    // LRU only ever caches a KNOWN (index-hit) result (see
    // `HotMatcher::classify` — the `Ok(Some(known))` arm is the only one
    // that calls `self.lru.lock().put(...)`), so the earlier Provisional
    // classifications of this exact shape were NEVER cached; the next
    // classify of the same raw shape is an LRU miss that goes straight to
    // `Registry::resolve_structural`, which now finds the promoted schema.
    // No explicit LRU-invalidation wiring was needed — verified here
    // against the real Redis-backed registry, not just the unit tests.
    // ------------------------------------------------------------------
    let more_a: Vec<&str> = vec![r#"{"user":"u9","ts":9}"#, r#"{"user":"u10","ts":10}"#];
    for payload in &more_a {
        produce_raw(&producer, &raw_topic, None, Some(payload.as_bytes())).await;
    }

    let after_promotion = drain_n(&tagged_consumer, 2, Duration::from_secs(60)).await;
    for msg in &after_promotion {
        let text =
            std::str::from_utf8(msg.payload().expect("payload present")).expect("utf8 payload");
        assert!(
            more_a.contains(&text),
            "unexpected payload after promotion: {text}"
        );
        let id = schema_id_header(msg);
        assert_eq!(
            id, promoted_schema_id,
            "post-promotion shape-A message must resolve to the promoted sch_ id, not re-mint cand_"
        );
    }
    eprintln!(
        "[e2e step 5] OK: post-promotion shape-A messages resolve to {promoted_schema_id} (no LRU invalidation needed)"
    );

    // ------------------------------------------------------------------
    // Step 6: outage. Stop Redis -> produce 1 more message -> tagged
    // `unresolved` (never cand_, spec §10). Restart Redis -> produce
    // another -> tagging recovers to sch_.
    //
    // Deliberately shape-B-shaped, NOT shape-A: step 5 just forced a
    // shape-A classify through `Registry::resolve_structural` to a
    // `Known` result, which — per `HotMatcher::classify`'s cache-on-Known-
    // only rule — permanently seeded the in-process LRU for shape A's raw
    // fingerprint. Any later shape-A message would be an LRU HIT
    // regardless of whether Redis is up, down, or mid-restart, which
    // would make this step vacuous (it would "pass" without ever touching
    // the registry at all — confirmed empirically: an earlier version of
    // this test reused shape A here and the outage message came back
    // tagged with `promoted_schema_id`, not `unresolved`, because the LRU
    // served it straight from cache).
    //
    // Shape B was never re-classified after promotion (steps 1-2 only
    // ever saw it as `Provisional`, which is never cached), so it's
    // guaranteed to still be an LRU miss here, forcing a REAL
    // `Registry::resolve_structural` round trip — genuinely exercising
    // the outage path. Shape B's concrete variant WAS recorded against
    // the candidate during cold-lane ingestion and replayed into the
    // structural index at promotion time (`Promoter::promote`), so once
    // Redis is back, this shape resolves to the SAME `promoted_schema_id`
    // — a genuine registry-reconnect recovery, not a cache artifact.
    // ------------------------------------------------------------------
    redis.stop().await.expect("stop redis container");

    let during_outage_payload = r#"{"user":"u11","ts":11,"note":"outage-probe"}"#;
    produce_raw(
        &producer,
        &raw_topic,
        None,
        Some(during_outage_payload.as_bytes()),
    )
    .await;

    let outage_msg = recv_owned(&tagged_consumer, Duration::from_secs(90)).await;
    assert_eq!(
        std::str::from_utf8(outage_msg.payload().expect("payload present")).unwrap(),
        during_outage_payload
    );
    assert_eq!(
        schema_id_header(&outage_msg),
        "unresolved",
        "a Redis outage must tag `unresolved`, NEVER mint a fresh cand_ (spec §10)"
    );
    eprintln!(
        "[e2e step 6a] OK: during Redis outage, tagging degrades to `unresolved`, never cand_"
    );

    redis.start().await.expect("restart redis container");

    // Confirm the restart actually landed on the SAME host port (the whole
    // point of pinning it) rather than silently proceeding against a
    // `redis_url` that no longer points at anything — a stale mapping here
    // would just turn this into a slower version of the original bug.
    let restarted_redis_port = redis
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("mapped redis port after restart");
    assert_eq!(
        restarted_redis_port, initial_redis_port,
        "redis restarted on a DIFFERENT host port ({restarted_redis_port}) than before the \
         outage ({initial_redis_port}); `serve()`'s already-open connection to `redis_url` can \
         never reach this new address, so recovery would be untestable"
    );

    // Recovery is polled with a FRESH probe message produced on every
    // iteration, each separated by a short pacing delay, rather than firing
    // one probe before the loop and hoping it lands after the exact
    // redis-rs reconnect moment: a single probe race-condition (produced
    // too soon after `start()`, before the registry's connection has
    // actually recovered) would previously make this step flaky rather
    // than robust. The deadline is generous (90s) to comfortably cover
    // redis-rs's reconnect-on-next-command behavior plus container
    // start-up time.
    let recovery_payload = r#"{"user":"u12","ts":12,"note":"recovery-probe"}"#;
    let recovery_deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let mut last_recovery_id: Option<String> = None;
    let recovered = loop {
        assert!(
            tokio::time::Instant::now() < recovery_deadline,
            "tagging never recovered to the promoted sch_ id after Redis restart; last observed: {last_recovery_id:?}"
        );

        produce_raw(
            &producer,
            &raw_topic,
            None,
            Some(recovery_payload.as_bytes()),
        )
        .await;

        let msg = recv_owned(&tagged_consumer, Duration::from_secs(15)).await;
        let text =
            std::str::from_utf8(msg.payload().expect("payload present")).expect("utf8 payload");
        assert_eq!(
            text, recovery_payload,
            "unexpected payload during recovery poll"
        );
        let id = schema_id_header(&msg);
        last_recovery_id = Some(id.clone());
        if id == promoted_schema_id {
            break msg;
        }
        // Not yet recovered — this pass's probe tagged something other
        // than the promoted id (e.g. still `unresolved` while the registry
        // connection is mid-reconnect). Pace the next attempt rather than
        // hammering Kafka/the registry back-to-back.
        tokio::time::sleep(Duration::from_secs(3)).await;
    };
    assert_eq!(schema_id_header(&recovered), promoted_schema_id);
    eprintln!("[e2e step 6b] OK: tagging recovered to {promoted_schema_id} after Redis restart");

    // ------------------------------------------------------------------
    // Teardown: cancel `serve()`'s shutdown and give it a bounded window
    // to drain — this is a correctness-adjacent sanity check (the relay/
    // discovery consumer/management API must all still be joinable), not
    // a new assertion under test.
    // ------------------------------------------------------------------
    shutdown.cancel();
    match tokio::time::timeout(Duration::from_secs(30), serve_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("serve() returned an error during shutdown: {e}"),
        Ok(Err(e)) => panic!("serve() task panicked: {e}"),
        Err(_) => panic!("serve() did not shut down within the deadline"),
    }
}
