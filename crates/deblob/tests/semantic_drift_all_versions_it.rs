//! P2-D polish Task 1: `PUT /api/v1/schemas/{sch_id}/semantic`'s drift
//! diagnostic must compare a newly-annotated family version against EVERY
//! prior version that carries an active `sem_`, not only the adjacent
//! (`version - 1`) one — against a REAL (AOF-enabled) Redis via
//! testcontainers, same harness style as `semantic_drift_it.rs`/
//! `semantic_drift_monoid_promoted_it.rs`, but calling
//! `api::semantic::put_semantic` directly (no HTTP/Kafka needed: it's a
//! plain `async fn` over a real `ApiState`) so the exact production loop
//! this task adds is what's under test, not just the orchestrator it
//! calls.
//!
//! Before this fix: a family reaching version 3 whose version 2 was never
//! annotated (or was annotated with the SAME `sem_` as version 3) would
//! never detect that version 1 carries a DIFFERENT, structurally-compatible
//! `sem_` — `put_semantic` only ever compared against `version - 1`. This
//! suite proves the fix: annotating version 3 now fires
//! `deblob_semantic_drift_total` against version 1 even with version 2
//! unannotated in between, and stays silent when every annotated version
//! shares one `sem_`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use deblob::api::semantic::{put_semantic, PutSemanticRequest};
use deblob::api::{ApiState, SecretToken};
use deblob::metrics::Metrics;
use deblob::promote::{PromoteRequest, Promoter};
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore, Registry, SchemaRecord};
use deblob_core::revision::ReasonCode;
use deblob_core::semantic::{
    FieldEntry, FieldSemantics, PathSegment, SemanticMetadata, Unit, UnitSystem,
};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_redis::health::HealthGate;
use deblob_redis::{RedisOpts, RedisRegistry};
use deblob_umbrella::store::InMemoryUmbrellaStore;
use redis::AsyncCommands;
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

const TOKEN: &str = "drift-all-versions-it-token";

// ---------------------------------------------------------------------
// Minimal fakes for the two dependencies `put_semantic` never touches
// (mirrors `semantic_neighbors_it.rs`/`semantic_monoid_promoted_it.rs`).
// ---------------------------------------------------------------------

#[derive(Default)]
struct UnusedEvidence;

#[async_trait::async_trait]
impl EvidenceStore for UnusedEvidence {
    async fn upsert_candidate(&self, _rec: CandidateRecord) -> Result<(), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn get_candidate(&self, _id: &CandidateId) -> Result<Option<CandidateRecord>, CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn list_candidates(
        &self,
        _state: CandidateState,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn append_evidence(
        &self,
        _id: &CandidateId,
        _stats: serde_json::Value,
    ) -> Result<(), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn set_state(&self, _id: &CandidateId, _state: CandidateState) -> Result<(), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn get_cluster(&self, _gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn set_cluster(&self, _gen_fp: &str, _cand_id: &CandidateId) -> Result<(), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn add_variant(
        &self,
        _cand_id: &CandidateId,
        _bucket_key: &str,
        _fp_b32: &str,
    ) -> Result<(), CoreError> {
        unimplemented!("not exercised by put_semantic")
    }
    async fn get_variants(
        &self,
        _cand_id: &CandidateId,
    ) -> Result<Vec<(String, String)>, CoreError> {
        unimplemented!("not exercised by put_semantic")
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
        unimplemented!("not exercised by put_semantic")
    }
}

// ---------------------------------------------------------------------
// Setup: real Redis, two independent RedisRegistry handles (one as
// `Registry`, one as `SemanticStore` — mirrors `semantic_neighbors_it.rs`'s
// `connect`/`state`).
// ---------------------------------------------------------------------

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
    let reg = RedisRegistry::connect(&url, opts).await.unwrap();
    let sem = RedisRegistry::connect(&url, opts).await.unwrap();
    (node, reg, sem, url)
}

fn state(reg: RedisRegistry, sem: RedisRegistry, metrics: Arc<Metrics>) -> ApiState {
    ApiState {
        registry: Arc::new(reg),
        evidence: Arc::new(UnusedEvidence),
        health: HealthGate::new(),
        token: SecretToken::new(TOKEN),
        promoter: Arc::new(UnusedPromoter),
        metrics,
        semantic: Arc::new(sem),
        semantic_registries: Arc::new(deblob_semantic::Registries::default()),
        umbrellas: Arc::new(InMemoryUmbrellaStore::new()),
    }
}

/// Publishes a real, structurally-canonicalized schema into `family_id`
/// (mirrors `semantic_drift_it.rs::publish_schema`). `Registry::publish`
/// auto-assigns the next `FamilyVersion` for `family_id` regardless of the
/// `version` field on the record passed in, so three calls with the SAME
/// `family_id` land as v1/v2/v3 in publish order.
async fn publish_schema(
    reg: &RedisRegistry,
    family_id: FamilyId,
    json: &[u8],
    cand_seed: u8,
) -> SchemaId {
    let node = parse_bounded(json, &Limits::default()).unwrap();
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).unwrap();
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id,
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "semantic_drift_all_versions_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
    };
    let bucket = format!("bucket:drift-all-versions-it:{cand_seed}");
    let cand = CandidateId::from_digest(&[cand_seed; 32]);
    reg.publish(record, &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();
    schema_id
}

fn metadata_with_unit(field: &str, code: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key(field.to_string())],
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

fn put_body(metadata: SemanticMetadata, reason: &str) -> PutSemanticRequest {
    PutSemanticRequest {
        metadata,
        reason_code: Some(ReasonCode::Correction),
        reason: Some(reason.to_string()),
    }
}

async fn annotate(state: &ApiState, sch_id: &SchemaId, req: PutSemanticRequest) -> StatusCode {
    let resp = put_semantic(
        State(state.clone()),
        Path(sch_id.as_str().to_string()),
        HeaderMap::new(),
        Json(req),
    )
    .await
    .expect("put_semantic must not error for a well-formed annotation");
    resp.status()
}

fn drift_total(metrics: &Metrics) -> f64 {
    metrics
        .registry()
        .gather()
        .iter()
        .find(|f| f.get_name() == "deblob_semantic_drift_total")
        .unwrap()
        .get_metric()[0]
        .get_counter()
        .get_value()
}

/// Snapshot of `deblob:schema:*`/`deblob:family:*` — mirrors
/// `semantic_capstone_it.rs::snapshot`, used to prove the drift loop never
/// touches schema/family state beyond the legitimate annotation write it
/// runs alongside.
async fn snapshot(url: &str, patterns: &[&str]) -> HashMap<String, HashMap<String, String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut out = HashMap::new();
    for pattern in patterns {
        let keys: Vec<String> = conn.keys(*pattern).await.unwrap();
        for key in keys {
            let fields: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
            out.insert(key, fields);
        }
    }
    out
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The headline case: family versions 1/2/3, v1 and v3 annotated with
/// DIFFERENT compatible `sem_`, v2 left unannotated. Annotating v3 must
/// detect drift against the NON-ADJACENT v1 — the old adjacent-only code
/// only ever checked v2 (unannotated => no drift), so this is a genuine
/// regression test for the loop.
#[tokio::test]
async fn annotating_v3_detects_drift_against_non_adjacent_v1_with_v2_unannotated() {
    let (_node, reg, sem, url) = connect().await;
    let family_id = FamilyId::new_v7();

    let v1 = publish_schema(&reg, family_id.clone(), br#"{"x":1}"#, 1).await;
    let v2 = publish_schema(&reg, family_id.clone(), br#"{"x":1,"y":2}"#, 2).await;
    let v3 = publish_schema(&reg, family_id.clone(), br#"{"x":1,"y":2,"z":3}"#, 3).await;

    let metrics = Metrics::new();
    let state = state(reg, sem, metrics.clone());

    // Sanity: publish() really did auto-assign sequential versions.
    for (sch, want) in [(&v1, 1), (&v2, 2), (&v3, 3)] {
        let record = state.registry.get_schema(sch).await.unwrap().unwrap();
        assert_eq!(
            record.version,
            FamilyVersion(want),
            "sanity: version assignment"
        );
    }

    // v1: annotated Celsius.
    let status = annotate(
        &state,
        &v1,
        put_body(metadata_with_unit("x", "Cel"), "v1 initial"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // v2: deliberately left UNANNOTATED (this is the gap the old adjacent-
    // only code fell into: comparing v3 against v2 alone finds nothing).

    let drift_before = drift_total(&metrics);
    let v1_sem_before = state.semantic.active_semantic(&v1).await.unwrap();
    let v2_sem_before = state.semantic.active_semantic(&v2).await.unwrap();
    assert!(v2_sem_before.is_none(), "sanity: v2 must be unannotated");
    let schema_family_before = snapshot(&url, &["deblob:schema:*", "deblob:family:*"]).await;

    // v3: annotated Kelvin — different sem_ than v1's Celsius, but the SAME
    // structurally-compatible family (v3 is a superset of v1's fields).
    let status = annotate(
        &state,
        &v3,
        put_body(
            metadata_with_unit("x", "K"),
            "v3, non-adjacent drift vs v1 with v2 unannotated",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let drift_after = drift_total(&metrics);
    assert_eq!(
        drift_after,
        drift_before + 1.0,
        "annotating v3 must fire drift against the non-adjacent v1 exactly once, \
         even though v2 (the adjacent version) was never annotated"
    );

    // Zero mutation: v1's and v2's own sem_ state (read during the loop)
    // must be byte-identical before/after, and no schema/family key may
    // have changed beyond v3's own legitimate annotation write.
    let v1_sem_after = state.semantic.active_semantic(&v1).await.unwrap();
    let v2_sem_after = state.semantic.active_semantic(&v2).await.unwrap();
    assert_eq!(
        v1_sem_before, v1_sem_after,
        "v1's sem_ state must be untouched by the drift loop"
    );
    assert_eq!(
        v2_sem_before, v2_sem_after,
        "v2's sem_ state must be untouched by the drift loop"
    );
    let schema_family_after = snapshot(&url, &["deblob:schema:*", "deblob:family:*"]).await;
    assert_eq!(
        schema_family_before, schema_family_after,
        "the drift loop must never mutate deblob:schema:*/deblob:family:* state"
    );

    // No split: the family hash still resolves exactly v:1->v1, v:2->v2,
    // v:3->v3.
    for (version, want) in [(1, &v1), (2, &v2), (3, &v3)] {
        let resolved = state
            .registry
            .family_version_schema(&family_id, FamilyVersion(version))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            &resolved, want,
            "family v:{version} must still resolve correctly"
        );
    }
}

/// Companion case: v1 and v3 share the SAME `sem_`, v2 unannotated —
/// annotating v3 must NOT fire drift (same-`sem_` is never drift, per
/// `detect_semantic_drift`'s own contract, regardless of how many prior
/// versions the loop now checks).
#[tokio::test]
async fn annotating_v3_with_the_same_sem_as_v1_does_not_fire_drift() {
    let (_node, reg, sem, _url) = connect().await;
    let family_id = FamilyId::new_v7();

    let v1 = publish_schema(&reg, family_id.clone(), br#"{"x":1}"#, 4).await;
    let _v2 = publish_schema(&reg, family_id.clone(), br#"{"x":1,"y":2}"#, 5).await;
    let v3 = publish_schema(&reg, family_id.clone(), br#"{"x":1,"y":2,"z":3}"#, 6).await;

    let metrics = Metrics::new();
    let state = state(reg, sem, metrics.clone());

    let status = annotate(
        &state,
        &v1,
        put_body(metadata_with_unit("x", "Cel"), "v1 initial"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let drift_before = drift_total(&metrics);

    // v3 annotated with the SAME unit -> same sem_ as v1.
    let status = annotate(
        &state,
        &v3,
        put_body(metadata_with_unit("x", "Cel"), "v3, same sem_ as v1"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let drift_after = drift_total(&metrics);
    assert_eq!(
        drift_after, drift_before,
        "identical sem_ across all annotated versions must never fire drift, \
         no matter how many prior versions the loop checks"
    );
}
