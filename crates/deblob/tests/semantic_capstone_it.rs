//! P2-D Task 8 (capstone): one end-to-end test proving the WHOLE
//! semantic-fingerprint feature against real Kafka + Redis, through the
//! SAME `deblob::serve::serve` wiring the production binary runs — mirrors
//! `crates/deblob/tests/e2e_it.rs`'s harness style (real containers, the
//! hand-rolled HTTP/1.1 client, no test-only stand-in).
//!
//! Unlike `e2e_it.rs`, this scenario never needs to produce/consume a raw
//! Kafka message — every governance/diagnostic assertion here lives on the
//! management API, hit through the REAL running HTTP server. A real Kafka
//! container is still started and its topics created, exactly like
//! `e2e_it.rs`, so `serve()` runs its FULL production wiring (relay +
//! discovery consumer, both left idle) rather than a partial stand-in.
//!
//! ## Schemas are published directly ("via ... the store", brief Part B),
//! NOT through candidate promotion — and why
//!
//! The brief explicitly allows publishing this test's fixture schemas "via
//! the governance API or the store". This suite uses the store (a direct
//! `RedisRegistry::publish`, exactly like `semantic_drift_it.rs`/
//! `semantic_neighbors_it.rs` already do) rather than the real candidate ->
//! `POST /candidates/{id}/promote` path, because of a genuine, pre-existing
//! incompatibility this capstone surfaced: `Promoter::promote` (P1) ALWAYS
//! stores a promoted schema with `canonicalizer: "deblob-monoid-v1"`
//! (`deblob_monoid::GENERALIZER`) and `canonical:
//! Profile::generalized_canonical_json()` — a DIFFERENT JSON grammar
//! (`{"optional":..., ...}`) than the plain `"deblob-canon-v1"` shape
//! grammar (`{"t":..., "f":{...}}`) `deblob_semantic::path::
//! canonical_field_paths` understands. `api::semantic::put_semantic` calls
//! `canonical_field_paths(&record.canonical)` UNCONDITIONALLY (even for an
//! `event_type`-only annotation with zero field entries), so it 422s with
//! `MalformedShape` for EVERY schema actually published through real
//! promotion — this is not a corner case, it's every schema deblob will
//! ever promote in production today. Every prior P2-D task's test suite
//! (1-7, 9, 10) never caught this because each one hand-built a plain-
//! canonicalizer `SchemaRecord` directly, the same workaround this file
//! uses. This is a real, capstone-worthy finding, reported in
//! `docs/semantic-runbook.md` and the Task 8 report — fixing it (teaching
//! `canonical_field_paths`/`typed_paths` a second, generalized-profile
//! grammar, or gating annotation on `canonicalizer == "deblob-canon-v1"`)
//! is future work, out of THIS task's two explicitly-scoped wirings (A1
//! config-seeding, A2 drift/collision wiring).
//!
//! ## What this proves
//!
//! **A1 (config-seeded governance registries):** `[semantic]` in the TOML
//! config lists a `canonical_field_id` and an `event_type`; the test
//! constructs `Config` with that section and starts the REAL `serve()` —
//! not a hand-built `ApiState` — so a `PUT .../semantic` naming the
//! config-seeded ids must validate, and one naming an unregistered id must
//! still `422`.
//!
//! **A2 (drift/collision wired for real):** `deblob_semantic_drift_total`/
//! `deblob_semantic_collision_total` are read off the REAL `/metrics`
//! endpoint after the annotation writes that should fire them — not by
//! calling `crate::semantic_drift`'s orchestrators directly (that's
//! `semantic_drift_it.rs`'s job) — proving they are actually wired into the
//! production annotation path, not just correct-but-dead code.
//!
//! **The headline case + full posture (Part B):** two annotations of ONE
//! `sch_` (Celsius, then a governed re-annotation to Fahrenheit) produce
//! DIFFERENT `sem_`, both persisting as separately-readable revisions,
//! while the schema RECORD itself (`canonical`/`canonicalizer`/`schema_id`
//! — the wire tag) never changes; a compatible family re-version with a
//! different unit raises drift without splitting the family (checked
//! directly against the family hash, never via a "the family split" API —
//! there isn't one); two schemas sharing one `sem_` raise the collision
//! diagnostic with a strength, never a merge (`GET /semantic/{sem_id}`
//! keeps listing them as two distinct `sch_` ids); a `semantic-neighbors`
//! query over a rename-similar pair ranks it above an unrelated schema,
//! diagnostic-only, the response carrying each neighbor's revision id.
//!
//! ## Why "two schemas with identical structure" is built as ONE `sch_`
//! going through two revisions
//!
//! `sch_id = base32(sha256(canonical))` is a PURE function of structure
//! (spec P1). Two payloads that are genuinely, byte-for-byte structurally
//! identical are — by construction, and this is the entire point of P2-D's
//! headline case — the exact SAME `sch_id`; `Registry::publish` is
//! idempotent on that identity, so there is no way to mint two DIFFERENT
//! `SchemaRecord`s from one canonical shape. What DOES let an operator
//! distinguish "this schema, read as Celsius" from "this schema, read as
//! Fahrenheit" is the append-only semantic-revision history on that ONE
//! `sch_id`: a governed re-annotation (`PUT` with the correct `If-Match` +
//! a reason) appends revision 2 with a different `sem_`, while revision 1
//! stays intact and independently readable — precisely "same structure,
//! different meaning, both provable" without ever claiming two schema
//! records exist where the digest says there is one.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use deblob::config::{
    Config, HttpProxyConfig, KafkaConfig, LimitsConfig, ManagementConfig, PromotionConfig, Secrets,
    SemanticConfig, SlmConfig,
};
use deblob::serve::serve;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_redis::{RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use serde_json::{json, Value};
use testcontainers_modules::kafka::apache;
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "capstone-it-token";
const REGISTERED_CFID: &str = "temp.ambient";
const REGISTERED_EVENT: &str = "device.reading";
const UNREGISTERED_CFID: &str = "cfid.never.registered.in.toml";

// ---------------------------------------------------------------------
// Container setup (mirrors e2e_it.rs).
// ---------------------------------------------------------------------

async fn start_kafka() -> ContainerAsync<apache::Kafka> {
    apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container must start")
}

async fn start_redis() -> ContainerAsync<Redis> {
    Redis::default()
        .with_cmd(["--appendonly", "yes"])
        .start()
        .await
        .expect("redis container must start")
}

async fn create_topics(brokers: &str, names: &[&str]) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::ClientConfig;

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

// ---------------------------------------------------------------------
// Minimal hand-rolled HTTP/1.1 client for the management API (mirrors
// e2e_it.rs, extended with an optional `If-Match` header and a raw-bytes
// body field so the plain-text `/metrics` response can be read without
// forcing it through JSON parsing).
// ---------------------------------------------------------------------

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Value,
    raw_body: Vec<u8>,
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

#[allow(clippy::too_many_arguments)]
async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    if_match: Option<&str>,
    body: Option<&[u8]>,
) -> HttpResponse {
    let mut stream = connect_with_retry(addr, Duration::from_secs(20)).await;

    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(token) = bearer {
        req.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    if let Some(etag) = if_match {
        req.push_str(&format!("If-Match: {etag}\r\n"));
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
        raw_body: body_bytes,
    }
}

async fn get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> HttpResponse {
    http_request(addr, "GET", path, bearer, None, None).await
}

async fn put(
    addr: SocketAddr,
    path: &str,
    bearer: &str,
    if_match: Option<&str>,
    json_body: &Value,
) -> HttpResponse {
    let body = serde_json::to_vec(json_body).expect("serialize PUT body");
    http_request(addr, "PUT", path, Some(bearer), if_match, Some(&body)).await
}

/// The bare numeric value trailing a Prometheus text-exposition line
/// matching `name` (no labels) or `name{label="value"}` (one label) —
/// parses `state.metrics.gather_text()`'s REAL output over the wire
/// (`GET /metrics`), never a value read from an in-process `Metrics`
/// handle the test doesn't have access to (that handle lives inside
/// `serve()`).
fn metric_value(text: &str, name: &str, label: Option<(&str, &str)>) -> f64 {
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let matches = match label {
            None => line.starts_with(name) && line[name.len()..].starts_with(' '),
            Some((k, v)) => {
                let wanted = format!("{name}{{");
                line.starts_with(&wanted) && line.contains(&format!("{k}=\"{v}\""))
            }
        };
        if matches {
            return line
                .split_whitespace()
                .last()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or_else(|| panic!("could not parse metric value from line: {line}"));
        }
    }
    0.0
}

async fn fetch_metrics_text(addr: SocketAddr) -> String {
    let resp = get(addr, "/metrics", None).await;
    assert_eq!(resp.status, 200, "/metrics must be reachable");
    String::from_utf8(resp.raw_body).expect("/metrics body is utf8")
}

// ---------------------------------------------------------------------
// Schema publishing — direct-to-store (see the module doc comment for why:
// a real GENERALIZER-canonicalized promoted schema can never be annotated
// at all, a pre-existing gap this capstone surfaced but does not fix).
// Mirrors `semantic_drift_it.rs`/`semantic_neighbors_it.rs`'s own
// `publish_schema` helper, writing through the SAME `RedisRegistry::publish`
// the real `Promoter` calls — a real atomic publication, just with a
// hand-built plain `"deblob-canon-v1"` record instead of a generalized one.
// ---------------------------------------------------------------------

async fn publish_schema(
    reg: &RedisRegistry,
    family_id: FamilyId,
    json_payload: &str,
    seed: u8,
) -> SchemaId {
    let node = parse_bounded(json_payload.as_bytes(), &Limits::default())
        .expect("fixture payload must parse under the bounded parser");
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).expect("canonical bytes are utf8");
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id,
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "semantic_capstone_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    };
    let bucket = format!("bucket:capstone-it:{seed}");
    let cand = CandidateId::from_digest(&[seed; 32]);
    reg.publish(record, &cand, &bucket, &[], "kamil", "capstone e2e seed")
        .await
        .expect("publish fixture schema");
    schema_id
}

async fn family_of(addr: SocketAddr, sch_id: &SchemaId) -> FamilyId {
    let resp = get(
        addr,
        &format!("/api/v1/schemas/{}", sch_id.as_str()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(resp.status, 200);
    let family_str = resp.body["data"]["family_id"]
        .as_str()
        .expect("schema record carries family_id");
    FamilyId::parse(family_str).expect("valid fam_ id")
}

// ---------------------------------------------------------------------
// Semantic-metadata JSON builders (mirrors api_it.rs's `semantic_body`,
// generalized to an arbitrary field path/cfid/unit/event_type).
// ---------------------------------------------------------------------

fn field_entry(path_key: &str, cfid: Option<&str>, unit_code: Option<&str>) -> Value {
    json!({
        "path": [{"key": path_key}],
        "semantics": {
            "canonical_field_id": cfid,
            "unit": unit_code.map(|c| json!({"system": "ucum", "code": c})),
        }
    })
}

fn metadata(event_type: Option<&str>, fields: Vec<Value>) -> Value {
    json!({ "event_type": event_type, "fields": fields })
}

fn put_body(metadata: Value, reason: &str) -> Value {
    json!({ "metadata": metadata, "reason_code": "correction", "reason": reason })
}

fn semantic_uri(sch_id: &SchemaId) -> String {
    format!("/api/v1/schemas/{}/semantic", sch_id.as_str())
}

// ---------------------------------------------------------------------
// Redis snapshot helper (mirrors semantic_drift_it.rs / semantic_neighbors_it.rs)
// for the "diagnostics never mutate schema/family state" proof.
// ---------------------------------------------------------------------

async fn snapshot(url: &str, patterns: &[&str]) -> HashMap<String, HashMap<String, String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut out = HashMap::new();
    for pattern in patterns {
        let keys: Vec<String> = conn.keys(*pattern).await.unwrap();
        for key in keys {
            // Type-appropriate read: `deblob:schema:*`/`deblob:family:*`/
            // `deblob:sem-active:*`/`deblob:sem-rev:*` are HASHes, but
            // `deblob:sem-index:*`/`deblob:sem-sig:*` are SETs — HGETALL on
            // a SET key errors WRONGTYPE, which would silently snapshot as
            // an empty map (a real mutation blind spot) if not handled.
            let key_type: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap();
            let fields: HashMap<String, String> = match key_type.as_str() {
                "hash" => conn.hgetall(&key).await.unwrap_or_default(),
                "set" => {
                    let mut members: Vec<String> = conn.smembers(&key).await.unwrap();
                    members.sort();
                    HashMap::from([("__set_members__".to_string(), members.join(","))])
                }
                "string" => {
                    let v: String = conn.get(&key).await.unwrap_or_default();
                    HashMap::from([("__string__".to_string(), v)])
                }
                other => HashMap::from([("__unhandled_type__".to_string(), other.to_string())]),
            };
            out.insert(key, fields);
        }
    }
    out
}

// ---------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------

#[tokio::test]
async fn p2d_capstone_full_semantic_fingerprint_posture() {
    // --- Setup: real Kafka + Redis containers, topics, and the FULL
    // `serve()` wiring, exactly as the production binary runs it. ---
    let kafka = start_kafka().await;
    let brokers = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("mapped kafka port")
    );
    let redis = start_redis().await;
    let redis_port = redis
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("mapped redis port");
    let redis_url = format!("redis://127.0.0.1:{redis_port}");

    let raw_topic = "capstone-raw".to_string();
    let tagged_topic = "capstone-tagged".to_string();
    let discovery_topic = "capstone-discovery".to_string();
    let quarantine_topic = "capstone-quarantine".to_string();
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
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port probe");
        probe.local_addr().expect("local addr").port()
    };
    let management_addr: SocketAddr = format!("127.0.0.1:{management_port}")
        .parse()
        .expect("valid socket addr");

    // --- A1: `[semantic]` config-seeds the governance registries. ---
    let config = Config {
        kafka: KafkaConfig {
            raw_topic: raw_topic.clone(),
            raw_topics: Vec::new(),
            tagged_topic: tagged_topic.clone(),
            discovery_topic: discovery_topic.clone(),
            quarantine_topic: quarantine_topic.clone(),
            group_id: "capstone-group".to_string(),
            transactional_id: "capstone-relay-txn".to_string(),
            // Pre-batching per-record-transaction escape hatch (batching
            // spec §3) — this test isn't about batching mechanics, keep
            // its existing produce/observe timing exact.
            max_batch_records: 1,
            max_batch_linger_ms: 100,
        },
        limits: LimitsConfig::default(),
        promotion: PromotionConfig::default(),
        management: ManagementConfig {
            addr: management_addr.to_string(),
        },
        slm: SlmConfig::default(),
        http_proxy: HttpProxyConfig::default(),
        semantic: SemanticConfig {
            canonical_field_ids: vec![REGISTERED_CFID.to_string()],
            event_types: vec![REGISTERED_EVENT.to_string()],
        },
        umbrella: Default::default(),
    };
    let secrets = Secrets {
        api_token: TOKEN.to_string(),
        redis_url: redis_url.clone(),
        kafka_brokers: brokers.clone(),
        kafka_sasl: None,
        slm_api_token: None,
        http_ingest_token: None,
    };
    let redis_opts = RedisOpts {
        allow_volatile: false,
    };

    let shutdown = CancellationToken::new();
    let serve_handle = tokio::spawn(serve(config, secrets, redis_opts, shutdown.clone()));

    let boot = get(management_addr, "/healthz", None).await;
    assert_eq!(
        boot.status, 200,
        "management API /healthz must be reachable"
    );
    eprintln!("[capstone] serve() up with [semantic] config-seeded via a real TOML-shaped Config");

    // An independent connection to the SAME Redis `serve()` itself connected
    // to — used to publish this suite's fixture schemas directly (see the
    // module doc comment) and for read-only assertions the management API
    // doesn't expose over HTTP (family-version adjacency, raw key
    // snapshots). Writes through this handle land in the exact same
    // storage `serve()`'s own `ApiState` reads from, so every `PUT
    // .../semantic` call below is exercising REAL shared state, not a
    // second, disconnected Redis.
    let reg = RedisRegistry::connect(&redis_url, redis_opts)
        .await
        .expect("connect direct registry handle");

    // ==================================================================
    // A1: config-seeded field-id/event-type validates through PUT; an
    // unregistered one still 422s.
    // ==================================================================
    let sch_a1 = publish_schema(&reg, FamilyId::new_v7(), r#"{"amount":5}"#, 1).await;

    let ok_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry("amount", Some(REGISTERED_CFID), Some("Cel"))],
        ),
        "config-seeded ids must validate",
    );
    let ok_resp = put(
        management_addr,
        &semantic_uri(&sch_a1),
        TOKEN,
        None,
        &ok_body,
    )
    .await;
    assert_eq!(
        ok_resp.status, 201,
        "a config-seeded canonical_field_id/event_type must validate through PUT, got: {:?}",
        ok_resp.body
    );

    let sch_a1_neg = publish_schema(&reg, FamilyId::new_v7(), r#"{"balance":5}"#, 2).await;
    let bad_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry("balance", Some(UNREGISTERED_CFID), None)],
        ),
        "unregistered id must 422",
    );
    let bad_resp = put(
        management_addr,
        &semantic_uri(&sch_a1_neg),
        TOKEN,
        None,
        &bad_body,
    )
    .await;
    assert_eq!(
        bad_resp.status, 422,
        "an UNREGISTERED canonical_field_id must still 422, got: {:?}",
        bad_resp.body
    );
    let bad_message = bad_resp.body["error"]["message"]
        .as_str()
        .expect("422 body carries error.message");
    assert!(
        bad_message.contains(UNREGISTERED_CFID),
        "422 must name the offending unregistered token, got: {bad_message}"
    );
    eprintln!("[capstone] A1 OK: config-seeded id validates (201); unregistered id still 422s naming the token");

    // ==================================================================
    // Headline case + governed re-annotation (Part B items 1-2): ONE sch_
    // (S1), annotated Celsius then governed-re-annotated Fahrenheit.
    // ==================================================================
    let s1 = publish_schema(&reg, FamilyId::new_v7(), r#"{"temperature":20}"#, 3).await;

    let s1_record_before = get(
        management_addr,
        &format!("/api/v1/schemas/{}", s1.as_str()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(s1_record_before.status, 200);

    let cel_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry(
                "temperature",
                Some(REGISTERED_CFID),
                Some("Cel"),
            )],
        ),
        "initial reading in Celsius",
    );
    let cel_resp = put(management_addr, &semantic_uri(&s1), TOKEN, None, &cel_body).await;
    assert_eq!(
        cel_resp.status, 201,
        "first annotation must be 201: {:?}",
        cel_resp.body
    );
    let sem_a = cel_resp.body["data"]["semantic_fingerprint"]
        .as_str()
        .expect("PUT response carries semantic_fingerprint")
        .to_string();
    let etag1 = cel_resp
        .headers
        .get("etag")
        .expect("PUT response carries an ETag header")
        .clone();
    assert_eq!(etag1, "\"1\"");

    // Governed re-annotation: correct If-Match + a reason -> appends
    // revision 2, advances the active pointer.
    let fahrenheit_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry(
                "temperature",
                Some(REGISTERED_CFID),
                Some("[degF]"),
            )],
        ),
        "convert to Fahrenheit for the US feed",
    );
    let f_resp = put(
        management_addr,
        &semantic_uri(&s1),
        TOKEN,
        Some(&etag1),
        &fahrenheit_body,
    )
    .await;
    assert_eq!(
        f_resp.status, 201,
        "governed re-annotation with correct If-Match must be 201: {:?}",
        f_resp.body
    );
    let sem_b = f_resp.body["data"]["semantic_fingerprint"]
        .as_str()
        .expect("PUT response carries semantic_fingerprint")
        .to_string();
    let etag2 = f_resp
        .headers
        .get("etag")
        .expect("PUT response carries an ETag header")
        .clone();
    assert_eq!(etag2, "\"2\"");

    assert_ne!(
        sem_a, sem_b,
        "identical structure, Celsius vs Fahrenheit MUST produce different sem_ (the headline case)"
    );
    eprintln!(
        "[capstone] headline OK: sem_a={sem_a} != sem_b={sem_b} on the SAME sch_ ({})",
        s1.as_str()
    );

    // Both revisions persist, oldest first, independently readable.
    let revisions_resp = get(
        management_addr,
        &format!("{}/revisions", semantic_uri(&s1)),
        Some(TOKEN),
    )
    .await;
    assert_eq!(revisions_resp.status, 200);
    let revisions = revisions_resp.body["data"]
        .as_array()
        .expect("revisions response carries data[]");
    assert_eq!(revisions.len(), 2, "both revisions must persist");
    assert_eq!(revisions[0]["sem_id"].as_str().unwrap(), sem_a);
    assert_eq!(revisions[1]["sem_id"].as_str().unwrap(), sem_b);
    assert_eq!(
        revisions[0]["metadata"]["fields"][0]["semantics"]["unit"]["code"], "Cel",
        "prior revision must still be readable with its ORIGINAL unit"
    );

    // The sch_ schema-record bytes (the wire tag) are UNCHANGED by
    // annotation.
    let s1_record_after = get(
        management_addr,
        &format!("/api/v1/schemas/{}", s1.as_str()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(s1_record_after.status, 200);
    assert_eq!(
        s1_record_before.body["data"]["canonical"], s1_record_after.body["data"]["canonical"],
        "sch_ canonical bytes must be byte-identical before/after annotation"
    );
    assert_eq!(
        s1_record_before.body["data"]["canonicalizer"],
        s1_record_after.body["data"]["canonicalizer"]
    );
    assert_eq!(
        s1_record_before.body["data"]["schema_id"], s1_record_after.body["data"]["schema_id"],
        "the wire tag (sch_) must be unchanged by semantic annotation"
    );

    // `schemas_by_semantic` separates the two sem_s: the OLD sem_ no longer
    // carries S1 (unlinked on re-annotation), the NEW sem_ does.
    let by_sem_a = get(
        management_addr,
        &format!("/api/v1/semantic/{sem_a}"),
        Some(TOKEN),
    )
    .await;
    assert_eq!(by_sem_a.status, 200);
    let sem_a_members: Vec<String> = by_sem_a.body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(
        !sem_a_members.contains(&s1.as_str().to_string()),
        "the SUPERSEDED sem_ must no longer list S1: {sem_a_members:?}"
    );
    let by_sem_b = get(
        management_addr,
        &format!("/api/v1/semantic/{sem_b}"),
        Some(TOKEN),
    )
    .await;
    assert_eq!(by_sem_b.status, 200);
    let sem_b_members: Vec<String> = by_sem_b.body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(
        sem_b_members.contains(&s1.as_str().to_string()),
        "the ACTIVE sem_ must list S1: {sem_b_members:?}"
    );
    eprintln!("[capstone] Part B items 1-2 OK: revisions/ETag/If-Match/sch_-immutability/reverse-index all verified");

    // ==================================================================
    // Part B item 3: a compatible family re-version with a changed unit
    // raises deblob_semantic_drift_total WITHOUT splitting the family.
    // ==================================================================
    let family2 = FamilyId::new_v7();
    let s2 = publish_schema(
        &reg,
        family2.clone(),
        r#"{"temperature":20,"reading_id":"x"}"#,
        4,
    )
    .await;

    let s2_cel_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry(
                "temperature",
                Some(REGISTERED_CFID),
                Some("Cel"),
            )],
        ),
        "s2 initial Celsius reading",
    );
    let s2_resp = put(
        management_addr,
        &semantic_uri(&s2),
        TOKEN,
        None,
        &s2_cel_body,
    )
    .await;
    assert_eq!(s2_resp.status, 201, "{:?}", s2_resp.body);
    let sem_a_v2 = s2_resp.body["data"]["semantic_fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        sem_a_v2, sem_a,
        "identical metadata bytes must hash to the SAME sem_ across unrelated schemas"
    );

    // A structurally-COMPATIBLE new version in the SAME family (superset of
    // S2's fields).
    let s3 = publish_schema(
        &reg,
        family2.clone(),
        r#"{"temperature":20,"reading_id":"x","extra":true}"#,
        5,
    )
    .await;

    // Snapshot the schema/family key space right after BOTH S2 and S3 are
    // published (ordinary promotion writes) but BEFORE the annotation
    // write below that fires drift + collision — isolates "did the
    // DIAGNOSTICS mutate schema/family state" from "did promotion/
    // annotation itself" (both of the latter are expected, legitimate
    // writes; `crate::semantic_store`'s own docs: the schema hash is never
    // touched by anything in the semantic-revision module, so schema/
    // family keys are already stable by this point in the test and stay
    // stable through every remaining annotation/diagnostic call).
    let schema_family_before = snapshot(&redis_url, &["deblob:schema:*", "deblob:family:*"]).await;

    let drift_before = metric_value(
        &fetch_metrics_text(management_addr).await,
        "deblob_semantic_drift_total",
        None,
    );
    let collision_medium_before = metric_value(
        &fetch_metrics_text(management_addr).await,
        "deblob_semantic_collision_total",
        Some(("strength", "medium")),
    );

    // Annotate S3 with the SAME metadata S1's ACTIVE revision now carries
    // (Fahrenheit) — this single write is deliberately built to fire BOTH
    // diagnostics at once: (a) drift, since S2 (adjacent, prior version)
    // is still Celsius; (b) collision, since sem_b will now be shared by
    // S1 AND S3 (see Part B item 4 below).
    let s3_f_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry(
                "temperature",
                Some(REGISTERED_CFID),
                Some("[degF]"),
            )],
        ),
        "s3 compatible re-version, converted to Fahrenheit",
    );
    let s3_resp = put(management_addr, &semantic_uri(&s3), TOKEN, None, &s3_f_body).await;
    assert_eq!(s3_resp.status, 201, "{:?}", s3_resp.body);
    let sem_b_v3 = s3_resp.body["data"]["semantic_fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        sem_b_v3, sem_b,
        "identical metadata bytes must hash to the SAME sem_ as S1's Fahrenheit revision"
    );

    let metrics_after_s3 = fetch_metrics_text(management_addr).await;
    let drift_after = metric_value(&metrics_after_s3, "deblob_semantic_drift_total", None);
    assert_eq!(
        drift_after,
        drift_before + 1.0,
        "annotating a compatible re-version with a changed unit must increment deblob_semantic_drift_total by exactly 1"
    );

    // "Without splitting the family": the family hash still resolves
    // EXACTLY v:1 -> S2 and v:2 -> S3 — checked directly against storage,
    // since there is no "did a split happen" API (there is no split
    // capability at all in P2-D, by design).
    let v1_schema = reg
        .family_version_schema(&family2, FamilyVersion(1))
        .await
        .expect("read family v:1")
        .expect("family v:1 must resolve");
    let v2_schema = reg
        .family_version_schema(&family2, FamilyVersion(2))
        .await
        .expect("read family v:2")
        .expect("family v:2 must resolve");
    assert_eq!(v1_schema, s2, "family v:1 must still be S2");
    assert_eq!(v2_schema, s3, "family v:2 must still be S3");
    let s2_family = family_of(management_addr, &s2).await;
    let s3_family = family_of(management_addr, &s3).await;
    assert_eq!(s2_family, family2, "S2's family_id must be unchanged");
    assert_eq!(
        s3_family, family2,
        "S3 must remain in the SAME family as S2 — no split"
    );
    eprintln!(
        "[capstone] Part B item 3 OK: drift_total +1, family {} still exactly v1={},v2={}",
        family2.as_str(),
        s2.as_str(),
        s3.as_str()
    );

    // ==================================================================
    // Part B item 4: two schemas sharing one sem_ raise the collision
    // diagnostic with a strength, never a merge.
    // ==================================================================
    let collision_medium_after = metric_value(
        &metrics_after_s3,
        "deblob_semantic_collision_total",
        Some(("strength", "medium")),
    );
    assert!(
        collision_medium_after > collision_medium_before,
        "annotating S3 onto the SAME sem_ S1 carries must increment \
         deblob_semantic_collision_total{{strength=\"medium\"}} (S1 has 1/1 leaf-field coverage, \
         S3 has 1/3 => the pair's min coverage is well under 80%, so Medium not Strong): \
         before={collision_medium_before} after={collision_medium_after}"
    );

    let by_sem_b_final = get(
        management_addr,
        &format!("/api/v1/semantic/{sem_b}"),
        Some(TOKEN),
    )
    .await;
    let sem_b_final_members: Vec<String> = by_sem_b_final.body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(
        sem_b_final_members.contains(&s1.as_str().to_string())
            && sem_b_final_members.contains(&s3.as_str().to_string()),
        "GET /semantic/{{sem_b}} must list BOTH S1 and S3 as DISTINCT sch_ ids — a shared sem_ \
         is a diagnostic finding, never a merge into one record: {sem_b_final_members:?}"
    );
    assert_ne!(
        s1, s3,
        "S1 and S3 remain two distinct sch_ ids, never merged"
    );
    eprintln!("[capstone] Part B item 4 OK: collision counter incremented; S1/S3 remain distinct sch_ ids under the shared sem_");

    // "Diagnostics never mutate schema/family state": the schema/family key
    // space is byte-identical to its snapshot from before either
    // diagnostic-firing write above (drift + collision both fired on S3's
    // annotation; only sem-active/sem-rev/sem-index/metrics may have
    // changed — proven separately above — never deblob:schema:*/
    // deblob:family:*).
    let schema_family_after = snapshot(&redis_url, &["deblob:schema:*", "deblob:family:*"]).await;
    assert_eq!(
        schema_family_before, schema_family_after,
        "firing the drift/collision diagnostics must change NO deblob:schema:*/deblob:family:* key \
         beyond the ordinary S2/S3 promotion writes already captured in both snapshots"
    );

    // ==================================================================
    // Part B item 5: semantic-neighbors ranks a rename-similar pair above
    // an unrelated schema, diagnostic-only, carrying revision ids.
    // ==================================================================
    let s4 = publish_schema(&reg, FamilyId::new_v7(), r#"{"ambient_temp":20}"#, 6).await;
    let s4_body = put_body(
        metadata(
            Some(REGISTERED_EVENT),
            vec![field_entry(
                "ambient_temp",
                Some(REGISTERED_CFID),
                Some("[degF]"),
            )],
        ),
        "s4 renamed field, same meaning as S1/S3's Fahrenheit annotation",
    );
    let s4_resp = put(management_addr, &semantic_uri(&s4), TOKEN, None, &s4_body).await;
    assert_eq!(s4_resp.status, 201, "{:?}", s4_resp.body);

    let s5 = publish_schema(&reg, FamilyId::new_v7(), r#"{"payment_amount":5}"#, 7).await;
    // Shares ONLY the bare unit code with S4/S1/S3 (no canonical_field_id,
    // no event_type) — a genuinely weak, low-weight overlap, never an
    // anchor (`has_anchor` requires event_type/cfid/namespace), so it must
    // rank at the BOTTOM (strength "insufficient") of any list it appears
    // in at all.
    let s5_body = put_body(
        metadata(
            None,
            vec![field_entry("payment_amount", None, Some("[degF]"))],
        ),
        "s5 unrelated schema, shares only a bare unit code",
    );
    let s5_resp = put(management_addr, &semantic_uri(&s5), TOKEN, None, &s5_body).await;
    assert_eq!(s5_resp.status, 201, "{:?}", s5_resp.body);

    let neighbors_snapshot_before = snapshot(
        &redis_url,
        &[
            "deblob:schema:*",
            "deblob:family:*",
            "deblob:sem-active:*",
            "deblob:sem-index:*",
            "deblob:sem-rev:*",
            "deblob:sem-sig:*",
        ],
    )
    .await;

    let neighbors_resp = get(
        management_addr,
        &format!("/api/v1/schemas/{}/semantic-neighbors", s4.as_str()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(neighbors_resp.status, 200, "{:?}", neighbors_resp.body);
    let data = &neighbors_resp.body["data"];
    assert_eq!(data["query_schema"].as_str().unwrap(), s4.as_str());
    assert_eq!(data["authority"].as_str().unwrap(), "diagnostic_only");
    let neighbors = data["neighbors"].as_array().expect("neighbors[] present");
    assert!(
        !neighbors.is_empty(),
        "expected at least one neighbor candidate"
    );

    // Every neighbor entry carries a revision id ("the response carries the
    // versions").
    for n in neighbors {
        let rev = n["semantic_revision_id"]
            .as_str()
            .expect("each neighbor carries semantic_revision_id");
        assert!(
            rev.starts_with("rev_"),
            "unexpected revision id shape: {rev}"
        );
        assert_ne!(
            n["schema_id"].as_str().unwrap(),
            s4.as_str(),
            "the query schema must never neighbor itself"
        );
    }

    let strong_entry = neighbors
        .iter()
        .find(|n| n["schema_id"].as_str() == Some(s1.as_str()) || n["schema_id"].as_str() == Some(s3.as_str()))
        .expect("the rename-similar pair (S1 or S3, sharing S4's exact cfid+event_type+unit) must appear");
    assert_eq!(strong_entry["strength"].as_str().unwrap(), "strong");

    let unrelated_entry = neighbors
        .iter()
        .find(|n| n["schema_id"].as_str() == Some(s5.as_str()));
    if let Some(unrelated) = unrelated_entry {
        assert_eq!(
            unrelated["strength"].as_str().unwrap(),
            "insufficient",
            "the unrelated schema, if present at all, must rank at the weakest strength tier"
        );
        let strong_idx = neighbors
            .iter()
            .position(|n| std::ptr::eq(n, strong_entry))
            .unwrap();
        let unrelated_idx = neighbors
            .iter()
            .position(|n| std::ptr::eq(n, unrelated))
            .unwrap();
        assert!(
            strong_idx < unrelated_idx,
            "the rename-similar neighbor must rank ABOVE the unrelated schema in the response"
        );
    }
    // Whether or not S5 clears the bounded posting-key union to even
    // appear, it can NEVER outrank the true near-neighbor — either
    // assertion path proves "ranked above an unrelated schema".

    let neighbors_snapshot_after = snapshot(
        &redis_url,
        &[
            "deblob:schema:*",
            "deblob:family:*",
            "deblob:sem-active:*",
            "deblob:sem-index:*",
            "deblob:sem-rev:*",
            "deblob:sem-sig:*",
        ],
    )
    .await;
    assert_eq!(
        neighbors_snapshot_before, neighbors_snapshot_after,
        "a semantic-neighbors query must be diagnostic-only: zero storage mutation"
    );
    eprintln!("[capstone] Part B item 5 OK: rename-similar pair ranks strong, above/instead-of the unrelated schema, diagnostic-only");

    // ------------------------------------------------------------------
    // Teardown.
    // ------------------------------------------------------------------
    shutdown.cancel();
    match tokio::time::timeout(Duration::from_secs(30), serve_handle).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => panic!("serve() returned an error during shutdown: {e}"),
        Ok(Err(e)) => panic!("serve() task panicked: {e}"),
        Err(_) => panic!("serve() did not shut down within the deadline"),
    }
}
