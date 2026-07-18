//! P2-D Task 10: `GET /api/v1/schemas/{sch_id}/semantic-neighbors` against a
//! REAL (AOF-enabled) Redis via testcontainers — same harness style as
//! `deblob-redis`'s own `semantic_it.rs` and this crate's
//! `semantic_drift_it.rs`. Exercises the full HTTP surface (auth, response
//! shape, exclude-self, no-anchor, `signature_too_broad`) plus the SECOND
//! half of the Task 10 checkpoint: a rebuild must produce IDENTICAL
//! neighbor ordering to the pre-rebuild incremental state (the postings
//! byte-identity half is `deblob-redis/tests/semantic_it.rs`'s job).

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deblob::api::{self, ApiState, SecretToken};
use deblob::metrics::Metrics;
use deblob::promote::{PromoteRequest, Promoter};
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore, Registry, SchemaRecord};
use deblob_core::revision::ReasonCode;
use deblob_core::semantic::{
    CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment, SemanticMetadata, Unit, UnitSystem,
};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_redis::health::HealthGate;
use deblob_redis::{RedisOpts, RedisRegistry};
use deblob_umbrella::store::InMemoryUmbrellaStore;
use http_body_util::BodyExt;
use redis::AsyncCommands;
use serde_json::Value;
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};
use tower::ServiceExt;

const TOKEN: &str = "neighbors-it-token";

// ---------------------------------------------------------------------
// Minimal fakes for the two dependencies this suite never exercises.
// ---------------------------------------------------------------------

#[derive(Default)]
struct UnusedEvidence;

#[async_trait::async_trait]
impl EvidenceStore for UnusedEvidence {
    async fn upsert_candidate(&self, _rec: CandidateRecord) -> Result<(), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn get_candidate(&self, _id: &CandidateId) -> Result<Option<CandidateRecord>, CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn list_candidates(
        &self,
        _state: CandidateState,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn append_evidence(
        &self,
        _id: &CandidateId,
        _stats: serde_json::Value,
    ) -> Result<(), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn set_state(&self, _id: &CandidateId, _state: CandidateState) -> Result<(), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn get_cluster(&self, _gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn set_cluster(&self, _gen_fp: &str, _cand_id: &CandidateId) -> Result<(), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn add_variant(
        &self,
        _cand_id: &CandidateId,
        _bucket_key: &str,
        _fp_b32: &str,
    ) -> Result<(), CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
    async fn get_variants(
        &self,
        _cand_id: &CandidateId,
    ) -> Result<Vec<(String, String)>, CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
}

#[derive(Default)]
struct UnusedPromoter;

#[async_trait::async_trait]
impl Promoter for UnusedPromoter {
    async fn promote(
        &self,
        _cand: &CandidateId,
        _req: PromoteRequest,
        _actor: &str,
    ) -> Result<SchemaRecord, CoreError> {
        unimplemented!("not exercised by the semantic-neighbors suite")
    }
}

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

/// Publishes a real, structurally-canonicalized schema (mirrors
/// `deblob-redis`'s `semantic_it.rs::publish_schema`).
async fn publish_schema(reg: &RedisRegistry, json: &[u8], cand_seed: u8) -> SchemaId {
    let node = parse_bounded(json, &Limits::default()).unwrap();
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).unwrap();
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id: FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "semantic_neighbors_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
    };
    let bucket = format!("bucket:neighbors-it:{cand_seed}");
    let cand = CandidateId::from_digest(&[cand_seed; 32]);
    reg.publish(record, &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();
    schema_id
}

fn metadata_with_cfid(cfid: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
            semantics: FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new(cfid)),
                identifier_namespace: None,
                unit: None,
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

fn metadata_with_unit_only(code: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
            semantics: FieldSemantics {
                canonical_field_id: None,
                identifier_namespace: None,
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: code.to_string(),
                }),
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

fn canon(metadata: &SemanticMetadata) -> (Vec<u8>, deblob_core::id::SemanticId) {
    let bytes = deblob_semantic::canonical_semantic_bytes(metadata).unwrap();
    let fp = deblob_semantic::semantic_fingerprint(metadata)
        .unwrap()
        .unwrap();
    (bytes, fp.0)
}

/// Annotates `sch_id` with `metadata`, going straight through
/// `RedisRegistry::append_revision` (mirrors every other `*_it.rs` file in
/// this workspace) — bypassing the `PUT .../semantic` HTTP endpoint (and
/// its Task 2 controlled-vocabulary validation against `semantic_
/// registries`, which is empty by default) since this suite only exercises
/// the READ-side `semantic-neighbors` endpoint.
async fn annotate(reg: &RedisRegistry, sch_id: &SchemaId, metadata: &SemanticMetadata, seed: i64) {
    // `expected_etag` is derived from whatever is CURRENTLY active (`None`
    // for a never-annotated schema, else its current etag) — a real caller
    // would do a compare-and-swap the same way; this fixture just always
    // "wins" the race since it's the only writer.
    let expected_etag = reg
        .active_semantic(sch_id)
        .await
        .unwrap()
        .map(|(_, _, etag)| etag);
    let (bytes, sem_id) = canon(metadata);
    reg.append_revision(
        sch_id,
        metadata,
        &bytes,
        &sem_id,
        "kamil",
        ReasonCode::Correction,
        "fixture annotation",
        seed,
        seed,
        expected_etag,
    )
    .await
    .unwrap();
}

async fn connect() -> (ContainerAsync<Redis>, RedisRegistry, RedisRegistry, String) {
    let node = Redis::default()
        .with_cmd(["--appendonly", "yes"])
        .start()
        .await
        .unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let opts = RedisOpts {
        allow_volatile: false,
    };
    // Two independent connections to the same Redis instance: `ApiState`
    // wants `Arc<dyn Registry>` and `Arc<dyn SemanticStore>` as separate
    // trait-object handles, and `RedisRegistry` isn't `Clone` — connecting
    // twice is cheap and keeps both handles fully independent, same as any
    // two real clients talking to the same server would be.
    let reg = RedisRegistry::connect(&url, opts).await.unwrap();
    let sem = RedisRegistry::connect(&url, opts).await.unwrap();
    (node, reg, sem, url)
}

fn state(reg: RedisRegistry, sem: RedisRegistry) -> ApiState {
    ApiState {
        registry: Arc::new(reg),
        evidence: Arc::new(UnusedEvidence),
        health: HealthGate::new(),
        token: SecretToken::new(TOKEN),
        promoter: Arc::new(UnusedPromoter),
        metrics: Metrics::new(),
        semantic: Arc::new(sem),
        semantic_registries: Arc::new(deblob_semantic::Registries::default()),
        umbrellas: Arc::new(InMemoryUmbrellaStore::new()),
        stream_tx: tokio::sync::broadcast::channel(16).0,
    }
}

fn get(uri: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn neighbors_uri(sch_id: &SchemaId) -> String {
    format!("/api/v1/schemas/{}/semantic-neighbors", sch_id.as_str())
}

/// Full snapshot of every `deblob:`-prefixed key touched by semantic
/// annotation/indexing, for the "never mutates" invariant test — mirrors
/// `semantic_drift_it.rs::snapshot_all`.
async fn snapshot_all(url: &str) -> HashMap<String, HashMap<String, String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut out = HashMap::new();
    for pattern in [
        "deblob:schema:*",
        "deblob:family:*",
        "deblob:sem-active:*",
        "deblob:sem-index:*",
        "deblob:sem-rev:*",
        "deblob:sem-sig:*",
        "deblob:alias:*",
    ] {
        let keys: Vec<String> = conn.keys(pattern).await.unwrap();
        for key in keys {
            let fields: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
            if !fields.is_empty() {
                out.insert(key, fields);
                continue;
            }
            let key_type: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap();
            let rendered = match key_type.as_str() {
                "set" => {
                    let mut members: Vec<String> = conn.smembers(&key).await.unwrap();
                    members.sort();
                    members.join(",")
                }
                "string" => conn.get(&key).await.unwrap_or_default(),
                other => format!("<unhandled type {other}>"),
            };
            out.insert(key, HashMap::from([("__raw__".to_string(), rendered)]));
        }
    }
    out
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[tokio::test]
async fn requires_auth() {
    let (_node, reg, sem, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 1).await;
    annotate(&reg, &sch_id, &metadata_with_cfid("device.temperature"), 1).await;

    let app = api::router(state(reg, sem));

    let resp = app
        .clone()
        .oneshot(get(&neighbors_uri(&sch_id), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp2 = app
        .oneshot(get(&neighbors_uri(&sch_id), Some("wrong-token")))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unannotated_schema_is_404() {
    let (_node, reg, sem, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 2).await;

    let app = api::router(state(reg, sem));
    let resp = app
        .oneshot(get(&neighbors_uri(&sch_id), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn no_anchor_features_returns_empty_neighbors_with_reason() {
    let (_node, reg, sem, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 3).await;
    // A unit-only annotation carries NO anchor feature (no cfid/event/idns).
    annotate(&reg, &sch_id, &metadata_with_unit_only("Cel"), 1).await;

    let app = api::router(state(reg, sem));
    let resp = app
        .oneshot(get(&neighbors_uri(&sch_id), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["data"]["neighbors"], serde_json::json!([]));
    assert_eq!(body["data"]["reason"], "no_anchor_features");
    assert_eq!(body["data"]["authority"], "diagnostic_only");
}

#[tokio::test]
async fn excludes_self_and_ranks_feature_sharing_schema_above_unrelated() {
    let (_node, reg, sem, _url) = connect().await;
    let query = publish_schema(&reg, br#"{"temperature":1}"#, 4).await;
    let related = publish_schema(&reg, br#"{"temperature":1,"meta":{}}"#, 5).await;
    let unrelated = publish_schema(&reg, br#"{"other":1}"#, 6).await;

    annotate(&reg, &query, &metadata_with_cfid("device.temperature"), 1).await;
    annotate(&reg, &related, &metadata_with_cfid("device.temperature"), 1).await;
    annotate(&reg, &unrelated, &metadata_with_cfid("device.humidity"), 1).await;

    let app = api::router(state(reg, sem));
    let resp = app
        .oneshot(get(&neighbors_uri(&query), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;

    assert_eq!(body["data"]["query_schema"], query.as_str());
    assert_eq!(
        body["data"]["signature_version"],
        "deblob-semantic-signature-v1"
    );
    assert_eq!(
        body["data"]["weights_version"],
        "deblob-semantic-signature-weights-v1"
    );
    assert_eq!(body["data"]["authority"], "diagnostic_only");

    let neighbors = body["data"]["neighbors"].as_array().unwrap();
    assert_eq!(
        neighbors.len(),
        1,
        "the unrelated schema must never appear, and the query must exclude itself: {neighbors:?}"
    );
    let neighbor = &neighbors[0];
    assert_eq!(neighbor["schema_id"], related.as_str());
    assert_eq!(neighbor["strength"], "medium");
    assert!(neighbor["semantic_revision_id"].is_string());
    assert!(neighbor["score"]["numerator"].is_number());
    assert!(neighbor["score"]["denominator"].is_number());
    assert!(neighbor["score"]["decimal"].is_string());
    assert_eq!(neighbor["shared_anchor_count"], 1);
    assert_eq!(
        neighbor["matched_feature_classes"],
        serde_json::json!(["canonical_field_id"])
    );
}

#[tokio::test]
async fn signature_too_broad_is_422() {
    let (_node, reg, sem, url) = connect().await;
    let query = publish_schema(&reg, br#"{"temperature":1}"#, 7).await;
    let metadata = metadata_with_cfid("device.temperature");
    annotate(&reg, &query, &metadata, 1).await;

    let feature_hex = deblob_semantic::signature::semantic_signature(&metadata)
        .feature_keys_hex()
        .into_iter()
        .next()
        .unwrap();

    // Pathologically inflate ONE of the query's own posting sets past the
    // 20,000-candidate bound — a legitimate boundary-condition fixture (see
    // `deblob-redis/tests/semantic_it.rs`'s equivalent storage-layer test).
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let key = format!("deblob:sem-sig:{feature_hex}");
    let synthetic: Vec<String> = (0..=deblob_core::revision::MAX_SIGNATURE_CANDIDATES)
        .map(|i| format!("sch_synthetic{i}"))
        .collect();
    for chunk in synthetic.chunks(5000) {
        let _: () = conn.sadd(&key, chunk).await.unwrap();
    }

    let app = api::router(state(reg, sem));
    let resp = app
        .oneshot(get(&neighbors_uri(&query), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "signature_too_broad");
}

#[tokio::test]
async fn neighbors_query_never_mutates_any_deblob_key() {
    let (_node, reg, sem, url) = connect().await;
    let query = publish_schema(&reg, br#"{"temperature":1}"#, 8).await;
    let related = publish_schema(&reg, br#"{"temperature":1,"meta":{}}"#, 9).await;
    annotate(&reg, &query, &metadata_with_cfid("device.temperature"), 1).await;
    annotate(&reg, &related, &metadata_with_cfid("device.temperature"), 1).await;

    let app = api::router(state(reg, sem));

    let before = snapshot_all(&url).await;
    let resp = app
        .oneshot(get(&neighbors_uri(&query), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let after = snapshot_all(&url).await;

    assert_eq!(
        before, after,
        "a diagnostic-only neighbors query must never mutate any deblob: key at all"
    );
}

/// CHECKPOINT (spec §5.12), HTTP-level half: a rebuild must produce
/// IDENTICAL neighbor ordering to the pre-rebuild incremental state, not
/// just identical raw postings (`deblob-redis/tests/semantic_it.rs` proves
/// the postings-byte-identity half).
#[tokio::test]
async fn rebuild_produces_identical_neighbor_ordering() {
    let (_node, reg, sem, url) = connect().await;
    let query = publish_schema(&reg, br#"{"temperature":1}"#, 10).await;
    let a = publish_schema(&reg, br#"{"temperature":1,"a":1}"#, 11).await;
    let b = publish_schema(&reg, br#"{"temperature":1,"b":1}"#, 12).await;

    annotate(&reg, &query, &metadata_with_cfid("device.temperature"), 1).await;
    // `a` and `b` both share the query's field — re-annotate `a` once, to
    // exercise the SREM/SADD swap before the rebuild.
    annotate(&reg, &a, &metadata_with_cfid("device.humidity"), 1).await;
    annotate(&reg, &a, &metadata_with_cfid("device.temperature"), 2).await;
    annotate(&reg, &b, &metadata_with_cfid("device.temperature"), 1).await;

    // A THIRD, independent connection, held separately so it survives
    // `reg`/`sem` being moved into the router below — used ONLY to invoke
    // the offline `rebuild_semantic_index` maintenance call, mirroring how
    // a real operator would run it against the same Redis instance the API
    // is serving from.
    let rebuild_reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: false,
        },
    )
    .await
    .unwrap();

    let app = api::router(state(reg, sem));

    let before_resp = app
        .clone()
        .oneshot(get(&neighbors_uri(&query), Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(before_resp.status(), StatusCode::OK);
    let before_body = body_json(before_resp).await;

    let rebuilt = rebuild_reg.rebuild_semantic_index().await.unwrap();
    assert!(rebuilt >= 3);

    let after_resp = app
        .oneshot(get(&neighbors_uri(&query), Some(TOKEN)))
        .await
        .unwrap();
    let after_body = body_json(after_resp).await;
    assert_eq!(
        before_body, after_body,
        "neighbor ordering must be identical before/after a rebuild"
    );
}
