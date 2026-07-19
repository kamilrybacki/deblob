//! P2-D Task 8 follow-up: `PUT /api/v1/schemas/{sch_id}/semantic` against a
//! schema that was ACTUALLY published through real candidate promotion
//! (`deblob::coldlane::ColdLane::ingest` -> `deblob::policy::Promoter::promote`,
//! the exact production path `POST /candidates/{id}/promote` runs), driven
//! over the real HTTP router (`deblob::api::router`) against a REAL Redis
//! (Docker via testcontainers) — never fakes, never a hand-built
//! `deblob-canon-v1` stand-in.
//!
//! Before the fix documented in `docs/semantic-runbook.md` (and this
//! suite's own module doc comment mirrors it): `Promoter::promote` ALWAYS
//! stores a promoted `SchemaRecord` with `canonicalizer: "deblob-monoid-v1"`
//! and a `canonical` string in the generalized-field grammar
//! (`deblob_monoid::Profile::generalized_canonical_json`'s
//! `{"optional":...,"types":[...],"children":{...},"elem":...}` shape,
//! bare — NOT wrapped in `{"gen":...,"fields":...}`, which only appears in
//! the hash preimage, never the persisted `canonical` string). `PUT
//! .../semantic` called `deblob_semantic::path::canonical_field_paths`
//! UNCONDITIONALLY, which only understood the plain `"deblob-canon-v1"`
//! shape grammar — so EVERY schema ever actually promoted 422'd on every
//! annotation attempt. This suite proves the fix: `put_semantic` now
//! dispatches on the schema record's OWN `canonicalizer` via
//! `canonical_field_paths_for`, so a genuinely-promoted schema annotates
//! successfully, while an absent path (or an unrecognized canonicalizer)
//! still 422s.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deblob::api::{self, ApiState, SecretToken};
use deblob::coldlane::{ColdLane, SampleMeta};
use deblob::metrics::Metrics;
use deblob::policy::{Promoter, PromotionPolicy};
use deblob::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{EvidenceStore, Registry, SchemaRecord};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits, Node};
use deblob_redis::health::HealthGate;
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use deblob_umbrella::store::InMemoryUmbrellaStore;
use http_body_util::BodyExt;
use serde_json::Value;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};
use tower::ServiceExt;

const TOKEN: &str = "monoid-semantic-it-token";

// ---------------------------------------------------------------------
// Setup: real Redis, real Registry + Evidence + a second Registry handle
// used as the `SemanticStore` (mirrors `semantic_neighbors_it.rs::connect`
// / `promote_resolve_it.rs::setup`).
// ---------------------------------------------------------------------

async fn setup() -> (
    Arc<RedisRegistry>,
    Arc<RedisEvidence>,
    RedisRegistry,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let opts = RedisOpts {
        allow_volatile: true,
    };
    let registry = Arc::new(RedisRegistry::connect(&url, opts).await.unwrap());
    let evidence = Arc::new(
        RedisEvidence::connect(&url, RedisEvidenceOpts::default(), opts)
            .await
            .unwrap(),
    );
    // Independent connection used as the `SemanticStore` handle in
    // `ApiState` — same pattern as `semantic_neighbors_it.rs::connect`.
    let semantic = RedisRegistry::connect(&url, opts).await.unwrap();
    (registry, evidence, semantic, node)
}

fn node_of(json: &[u8]) -> Node {
    parse_bounded(json, &Limits::default()).unwrap()
}

fn cand_id_of(json: &[u8]) -> CandidateId {
    let node = node_of(json);
    CandidateId::from_digest(&fingerprint(&shape_of(&node)))
}

fn meta(source: &str) -> SampleMeta {
    SampleMeta {
        source: source.to_string(),
        cursor: None,
    }
}

/// Both promotion guards disabled — this suite cares about the
/// annotation-vs-grammar wiring, not the guard thresholds (already covered
/// by `crates/deblob/src/policy.rs`'s own unit tests).
fn no_guard_policy() -> PromotionPolicy {
    PromotionPolicy {
        min_samples: 1,
        min_age_ms: 0,
    }
}

fn promote_request(name: &str) -> PromoteRequest {
    PromoteRequest {
        family: FamilyChoice::New,
        name: Some(name.to_string()),
        reason: "semantic_monoid_promoted_it fixture".to_string(),
    }
}

/// Ingests one concrete `payload` as a fresh candidate and promotes it
/// through the REAL `deblob::policy::Promoter` (no HTTP involved yet) —
/// the exact `ColdLane::ingest` -> `Promoter::promote` pipeline
/// `POST /candidates/{id}/promote` runs in production. Returns the
/// resulting `SchemaRecord`, whose `canonicalizer` is always
/// `"deblob-monoid-v1"` (`deblob_monoid::GENERALIZER`) — never
/// `"deblob-canon-v1"` — by construction of `Promoter::promote`.
async fn promote_real_schema(
    registry: &Arc<RedisRegistry>,
    evidence: &Arc<RedisEvidence>,
    payload: &[u8],
    name: &str,
) -> SchemaRecord {
    let lane = ColdLane::new(evidence.clone());
    let cand_id = cand_id_of(payload);
    lane.ingest(cand_id.clone(), &node_of(payload), meta("monoid-it"))
        .await
        .unwrap();

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    promoter
        .promote(&cand_id, promote_request(name), "tester")
        .await
        .unwrap()
}

fn state(registry: Arc<RedisRegistry>, semantic: RedisRegistry) -> ApiState {
    ApiState {
        registry,
        evidence: Arc::new(UnusedEvidence),
        health: HealthGate::new(),
        token: SecretToken::new(TOKEN),
        promoter: Arc::new(UnusedPromoter),
        metrics: Metrics::new(),
        semantic: Arc::new(semantic),
        semantic_registries: Arc::new(deblob_semantic::Registries::default()),
        umbrellas: Arc::new(InMemoryUmbrellaStore::new()),
        sources: Arc::new(deblob_core::ports::InMemorySourceRegistry::default()),
        value_profiles: Arc::new(deblob_core::ports::InMemoryValueProfileStore::default()),
        enforce_value_guard: false,
        stream_tx: tokio::sync::broadcast::channel(16).0,
    }
}

// ---------------------------------------------------------------------
// Fakes for the two dependencies this suite never exercises over HTTP
// (promotion happens directly against `deblob::policy::Promoter` above,
// never through `POST /candidates/{id}/promote`; evidence reads/writes are
// entirely `ColdLane`'s job before the HTTP router is ever touched).
// ---------------------------------------------------------------------

#[derive(Default)]
struct UnusedEvidence;

#[async_trait::async_trait]
impl EvidenceStore for UnusedEvidence {
    async fn upsert_candidate(
        &self,
        _rec: deblob_core::ports::CandidateRecord,
    ) -> Result<(), deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn get_candidate(
        &self,
        _id: &CandidateId,
    ) -> Result<Option<deblob_core::ports::CandidateRecord>, deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn list_candidates(
        &self,
        _state: deblob_core::ports::CandidateState,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<
        (Vec<deblob_core::ports::CandidateRecord>, Option<String>),
        deblob_core::error::CoreError,
    > {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn append_evidence(
        &self,
        _id: &CandidateId,
        _stats: serde_json::Value,
    ) -> Result<(), deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn set_state(
        &self,
        _id: &CandidateId,
        _state: deblob_core::ports::CandidateState,
    ) -> Result<(), deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn get_cluster(
        &self,
        _gen_fp: &str,
    ) -> Result<Option<CandidateId>, deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn set_cluster(
        &self,
        _gen_fp: &str,
        _cand_id: &CandidateId,
    ) -> Result<(), deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn add_variant(
        &self,
        _cand_id: &CandidateId,
        _bucket_key: &str,
        _fp_b32: &str,
    ) -> Result<(), deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
    async fn get_variants(
        &self,
        _cand_id: &CandidateId,
    ) -> Result<Vec<(String, String)>, deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
}

#[derive(Default)]
struct UnusedPromoter;

#[async_trait::async_trait]
impl PromoterTrait for UnusedPromoter {
    async fn promote(
        &self,
        _cand: &CandidateId,
        _req: PromoteRequest,
        _actor: &str,
    ) -> Result<SchemaRecord, deblob_core::error::CoreError> {
        unimplemented!("not exercised over HTTP by this suite")
    }
}

// ---------------------------------------------------------------------
// HTTP request builders (mirrors `api_it.rs`).
// ---------------------------------------------------------------------

fn get(uri: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn put_json(uri: &str, bearer: &str, if_match: Option<&str>, json: &Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {bearer}"));
    if let Some(etag) = if_match {
        builder = builder.header("if-match", etag);
    }
    builder
        .body(Body::from(serde_json::to_vec(json).unwrap()))
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn semantic_uri(sch_id: &SchemaId) -> String {
    format!("/api/v1/schemas/{}/semantic", sch_id.as_str())
}

/// Builds a `PutSemanticRequest` body annotating each `path` in
/// `path_keys` with a REAL, non-empty assertion (a bare UCUM unit code, "1"
/// — a registered-free system, unlike `canonical_field_id`/`event_type`
/// which default to an empty operator-registered vocabulary). An
/// all-`null` `FieldSemantics` normalizes to "no assertion", which
/// `semantic_fingerprint` would then reject wholesale as
/// `"no semantic assertions were provided"` BEFORE path validation ever
/// runs — this helper must give every field entry a real assertion so
/// Task 4's path-existence check is actually exercised.
fn key_path_body(path_keys: &[Value], reason: &str) -> Value {
    let fields: Vec<Value> = path_keys
        .iter()
        .map(|path| {
            serde_json::json!({
                "path": path,
                "semantics": {
                    "canonical_field_id": null,
                    "identifier_namespace": null,
                    "unit": {"system": "ucum", "code": "1"},
                    "numeric_scale": null,
                    "temporal": null,
                    "enum_semantics": null
                }
            })
        })
        .collect();
    serde_json::json!({
        "metadata": { "event_type": null, "fields": fields },
        "reason_code": "correction",
        "reason": reason
    })
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The headline fix: a schema published through REAL candidate promotion
/// (`canonicalizer == "deblob-monoid-v1"`) can now be annotated on a path
/// that actually exists in its generalized field structure — this used to
/// 422 unconditionally before the fix.
#[tokio::test]
async fn promoted_monoid_schema_annotates_successfully_on_an_existing_path() {
    let (registry, evidence, semantic, _node) = setup().await;
    let schema = promote_real_schema(
        &registry,
        &evidence,
        br#"{"amount":5,"currency":"USD"}"#,
        "payments.charged",
    )
    .await;
    assert_eq!(
        schema.canonicalizer, "deblob-monoid-v1",
        "sanity: real promotion must use the generalized-profile canonicalizer"
    );

    let app = api::router(state(registry, semantic));
    let body = key_path_body(
        &[serde_json::json!([{"key": "amount"}])],
        "amount is a real field",
    );

    let resp = app
        .oneshot(put_json(
            &semantic_uri(&schema.schema_id),
            TOKEN,
            None,
            &body,
        ))
        .await
        .unwrap();

    let status = resp.status();
    let payload = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "annotating an existing path on a REAL promoted (deblob-monoid-v1) schema must succeed, got: {payload:?}"
    );
    assert!(payload["data"]["semantic_fingerprint"]
        .as_str()
        .unwrap()
        .starts_with("sem_"));
}

/// A path that does NOT exist in the promoted schema's generalized field
/// structure must still `422` — the fix teaches the enumerator the real
/// grammar, it does not turn validation off.
#[tokio::test]
async fn promoted_monoid_schema_still_422s_on_an_absent_path() {
    let (registry, evidence, semantic, _node) = setup().await;
    let schema = promote_real_schema(
        &registry,
        &evidence,
        br#"{"amount":5,"currency":"USD"}"#,
        "payments.charged",
    )
    .await;

    let app = api::router(state(registry, semantic));
    let body = key_path_body(
        &[serde_json::json!([{"key": "does_not_exist"}])],
        "this field was never observed",
    );

    let resp = app
        .oneshot(put_json(
            &semantic_uri(&schema.schema_id),
            TOKEN,
            None,
            &body,
        ))
        .await
        .unwrap();

    let status = resp.status();
    let payload = body_json(resp).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{payload:?}");
    let message = payload["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("does_not_exist"),
        "422 must name the offending path: {message}"
    );
}

/// A promoted schema with a nested object inside an array (`items[].sku`)
/// must enumerate BOTH the `Key("items")` path and the `Wildcard` inside it
/// reached through `"elem"` — proving the monoid walker's array handling,
/// not just its flat-object handling.
#[tokio::test]
async fn promoted_monoid_schema_annotates_successfully_through_a_wildcard_path() {
    let (registry, evidence, semantic, _node) = setup().await;
    let schema = promote_real_schema(
        &registry,
        &evidence,
        br#"{"items":[{"sku":"A1"}]}"#,
        "orders.created",
    )
    .await;
    assert_eq!(schema.canonicalizer, "deblob-monoid-v1");

    let app = api::router(state(registry, semantic));
    let body = key_path_body(
        &[serde_json::json!([
            {"key": "items"},
            "wildcard",
            {"key": "sku"}
        ])],
        "sku is inside every item",
    );

    let resp = app
        .oneshot(put_json(
            &semantic_uri(&schema.schema_id),
            TOKEN,
            None,
            &body,
        ))
        .await
        .unwrap();

    let status = resp.status();
    let payload = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a path reached through the monoid grammar's \"elem\" wildcard must validate, got: {payload:?}"
    );
}

/// Regression: a schema hand-published with the PLAIN `deblob-canon-v1`
/// grammar (never promoted) must keep annotating exactly as before the
/// dispatch was introduced — the fix must be additive, never a behavior
/// change for the existing grammar.
#[tokio::test]
async fn canon_v1_schema_annotation_still_works_unchanged() {
    let (registry, _evidence, semantic, _node) = setup().await;

    let payload = br#"{"temperature":20}"#;
    let node = node_of(payload);
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
        provenance: serde_json::json!({"source": "semantic_monoid_promoted_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    };
    let cand = CandidateId::from_digest(&[42u8; 32]);
    registry
        .publish(
            record,
            &cand,
            "bucket:monoid-it:canon-v1",
            &[],
            "kamil",
            "seed",
        )
        .await
        .unwrap();

    let app = api::router(state(registry, semantic));
    let body = key_path_body(
        &[serde_json::json!([{"key": "temperature"}])],
        "plain canon-v1 schema still annotates",
    );

    let resp = app
        .oneshot(put_json(&semantic_uri(&schema_id), TOKEN, None, &body))
        .await
        .unwrap();

    let status = resp.status();
    let payload = body_json(resp).await;
    assert_eq!(status, StatusCode::CREATED, "{payload:?}");
}

/// An unrecognized `canonicalizer` on a schema record must still `422` —
/// never a silent accept of an unknown grammar.
#[tokio::test]
async fn unknown_canonicalizer_reports_422_not_silent_accept() {
    let (registry, _evidence, semantic, _node) = setup().await;

    let schema_id = SchemaId::from_digest(&[7u8; 32]);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id: FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: "{}".to_string(),
        canonicalizer: "deblob-canon-v2-from-the-future".to_string(),
        provenance: serde_json::json!({"source": "semantic_monoid_promoted_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    };
    let cand = CandidateId::from_digest(&[8u8; 32]);
    registry
        .publish(
            record,
            &cand,
            "bucket:monoid-it:unknown",
            &[],
            "kamil",
            "seed",
        )
        .await
        .unwrap();

    let app = api::router(state(registry, semantic));
    let body = key_path_body(&[], "event-type-only annotation");

    let resp = app
        .oneshot(put_json(&semantic_uri(&schema_id), TOKEN, None, &body))
        .await
        .unwrap();

    let status = resp.status();
    let payload = body_json(resp).await;
    // With zero field entries, `validate_paths` never actually inspects
    // `valid_paths` — so this proves the DISPATCH itself rejects the
    // unknown canonicalizer up front, not merely that no offending path
    // happened to be checked.
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{payload:?}");
}

/// Sanity check exercised alongside the above: `GET` the freshly-promoted
/// schema back over HTTP to confirm the record's `canonicalizer`/`canonical`
/// the API surfaces match what promotion actually wrote (never silently
/// rewritten by the annotation path).
#[tokio::test]
async fn promoted_schema_record_is_unchanged_by_annotation() {
    let (registry, evidence, semantic, _node) = setup().await;
    let schema =
        promote_real_schema(&registry, &evidence, br#"{"amount":5}"#, "payments.charged").await;

    let app = api::router(state(registry, semantic));

    let before = app
        .clone()
        .oneshot(get(
            &format!("/api/v1/schemas/{}", schema.schema_id.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();
    let before_body = body_json(before).await;

    let put_body = key_path_body(&[serde_json::json!([{"key": "amount"}])], "annotate amount");
    let put_resp = app
        .clone()
        .oneshot(put_json(
            &semantic_uri(&schema.schema_id),
            TOKEN,
            None,
            &put_body,
        ))
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::CREATED);

    let after = app
        .oneshot(get(
            &format!("/api/v1/schemas/{}", schema.schema_id.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();
    let after_body = body_json(after).await;

    assert_eq!(
        before_body["data"]["canonical"], after_body["data"]["canonical"],
        "annotation must never rewrite the schema record's own canonical bytes"
    );
    assert_eq!(
        before_body["data"]["canonicalizer"],
        after_body["data"]["canonicalizer"]
    );
    assert_eq!(after_body["data"]["canonicalizer"], "deblob-monoid-v1");
}
