//! Integration tests for the management API (`deblob::api`), spec §8.
//! Drives the axum `Router` in-process via `tower::ServiceExt::oneshot` —
//! no real Redis/Kafka, only fake `Registry`/`EvidenceStore`/`Promoter`
//! implementations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deblob::api::{self, ApiState, SecretToken};
use deblob::metrics::Metrics;
use deblob::promote::{FamilyChoice, PromoteRequest, Promoter};
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore, Registry, SchemaRecord};
use deblob_redis::health::HealthGate;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const TOKEN: &str = "test-token-abc123";

// ---------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------

/// In-memory `Registry` fake. `list_schemas` paginates over a fixed,
/// insertion-ordered `Vec` using a decimal-index cursor (opaque to the API
/// layer, which base64-wraps whatever string the registry hands back).
struct FakeRegistry {
    schemas: Vec<SchemaRecord>,
}

impl FakeRegistry {
    fn new(schemas: Vec<SchemaRecord>) -> Self {
        Self { schemas }
    }
}

#[async_trait::async_trait]
impl Registry for FakeRegistry {
    async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        Ok(self.schemas.iter().find(|s| &s.schema_id == id).cloned())
    }

    async fn resolve_structural(
        &self,
        _bucket_key: &str,
        _fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        unimplemented!("not exercised by the management API")
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
        unimplemented!("promotion goes through Promoter, not Registry::publish, in this API")
    }

    async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
        unimplemented!("not exercised by the management API")
    }

    async fn list_schemas(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        let start: usize = match cursor {
            Some(c) => c
                .parse()
                .map_err(|_| CoreError::Conflict("bad cursor".into()))?,
            None => 0,
        };
        let end = (start + limit).min(self.schemas.len());
        let page = self.schemas[start.min(self.schemas.len())..end].to_vec();
        let next = if end < self.schemas.len() {
            Some(end.to_string())
        } else {
            None
        };
        Ok((page, next))
    }

    async fn list_families_in_buckets(
        &self,
        _bucket_keys: &[String],
    ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
        unimplemented!("not exercised by the management API")
    }

    async fn list_families_by_band_depth(
        &self,
        _bands: &[u32],
        _depths: &[u32],
    ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
        unimplemented!("not exercised by the management API")
    }
}

/// In-memory `EvidenceStore` fake, keyed by `CandidateId`.
#[derive(Default)]
struct FakeEvidence {
    candidates: StdMutex<HashMap<CandidateId, CandidateRecord>>,
}

#[async_trait::async_trait]
impl EvidenceStore for FakeEvidence {
    async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError> {
        self.candidates
            .lock()
            .unwrap()
            .insert(rec.candidate_id.clone(), rec);
        Ok(())
    }

    async fn get_candidate(&self, id: &CandidateId) -> Result<Option<CandidateRecord>, CoreError> {
        Ok(self.candidates.lock().unwrap().get(id).cloned())
    }

    async fn list_candidates(
        &self,
        state: CandidateState,
        _cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
        let items: Vec<_> = self
            .candidates
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.state == state)
            .take(limit)
            .cloned()
            .collect();
        Ok((items, None))
    }

    async fn append_evidence(
        &self,
        _id: &CandidateId,
        _stats: serde_json::Value,
    ) -> Result<(), CoreError> {
        Ok(())
    }

    async fn set_state(&self, id: &CandidateId, state: CandidateState) -> Result<(), CoreError> {
        if let Some(rec) = self.candidates.lock().unwrap().get_mut(id) {
            rec.state = state;
        }
        Ok(())
    }

    async fn get_cluster(&self, _gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
        unimplemented!("not exercised by the management API")
    }

    async fn set_cluster(&self, _gen_fp: &str, _cand_id: &CandidateId) -> Result<(), CoreError> {
        unimplemented!("not exercised by the management API")
    }

    async fn add_variant(
        &self,
        _cand_id: &CandidateId,
        _bucket_key: &str,
        _fp_b32: &str,
    ) -> Result<(), CoreError> {
        unimplemented!("not exercised by the management API")
    }

    async fn get_variants(
        &self,
        _cand_id: &CandidateId,
    ) -> Result<Vec<(String, String)>, CoreError> {
        unimplemented!("not exercised by the management API")
    }
}

/// Configurable `Promoter` fake: returns whatever `outcome` was constructed
/// with on every call.
struct FakePromoter {
    outcome: StdMutex<Option<Result<SchemaRecord, CoreErrorClone>>>,
}

/// `CoreError` isn't `Clone`; this newtype lets the fake stash one
/// reusable error variant without re-deriving `Clone` on the real type.
enum CoreErrorClone {
    Conflict(String),
    PolicyRejected(String),
}

impl From<CoreErrorClone> for CoreError {
    fn from(e: CoreErrorClone) -> Self {
        match e {
            CoreErrorClone::Conflict(msg) => CoreError::Conflict(msg),
            CoreErrorClone::PolicyRejected(msg) => CoreError::PolicyRejected(msg),
        }
    }
}

impl FakePromoter {
    fn ok(schema: SchemaRecord) -> Self {
        Self {
            outcome: StdMutex::new(Some(Ok(schema))),
        }
    }

    fn conflict(msg: &str) -> Self {
        Self {
            outcome: StdMutex::new(Some(Err(CoreErrorClone::Conflict(msg.to_string())))),
        }
    }

    fn policy_rejected(msg: &str) -> Self {
        Self {
            outcome: StdMutex::new(Some(Err(CoreErrorClone::PolicyRejected(msg.to_string())))),
        }
    }
}

#[async_trait::async_trait]
impl Promoter for FakePromoter {
    async fn promote(
        &self,
        _cand: &CandidateId,
        _req: PromoteRequest,
        _actor: &str,
    ) -> Result<SchemaRecord, CoreError> {
        match self.outcome.lock().unwrap().take() {
            Some(Ok(schema)) => Ok(schema),
            Some(Err(e)) => Err(e.into()),
            None => Err(CoreError::Conflict("fake promoter called twice".into())),
        }
    }
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn sample_schema(seed: u8) -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[seed; 32]),
        family_id: deblob_core::id::FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: "{}".to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({}),
        semantic: None,
        semantic_fingerprint: None,
    }
}

fn make_state(
    registry: FakeRegistry,
    evidence: FakeEvidence,
    promoter: FakePromoter,
    health: HealthGate,
) -> ApiState {
    ApiState {
        registry: Arc::new(registry),
        evidence: Arc::new(evidence),
        health,
        token: SecretToken::new(TOKEN),
        promoter: Arc::new(promoter),
        metrics: Metrics::new(),
    }
}

fn empty_state() -> ApiState {
    make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
    )
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn get(uri: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn post_json(uri: &str, bearer: Option<&str>, json: &Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from(serde_json::to_vec(json).unwrap()))
        .unwrap()
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[tokio::test]
async fn rejects_missing_bearer() {
    let app = api::router(empty_state());

    let resp = app.oneshot(get("/api/v1/schemas", None)).await.unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "unauthorized");
}

#[tokio::test]
async fn wrong_token_rejected() {
    let app = api::router(empty_state());

    let resp = app
        .oneshot(get("/api/v1/schemas", Some("not-the-token")))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "unauthorized");
}

#[tokio::test]
async fn lists_schemas_with_cursor() {
    let schemas = vec![sample_schema(1), sample_schema(2), sample_schema(3)];
    let state = make_state(
        FakeRegistry::new(schemas.clone()),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
    );
    let app = api::router(state);

    let resp = app
        .clone()
        .oneshot(get("/api/v1/schemas?limit=2", Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["data"].as_array().unwrap().len(), 2);
    let next_cursor = body["next_cursor"].as_str().unwrap().to_string();

    let resp2 = app
        .oneshot(get(
            &format!("/api/v1/schemas?limit=2&cursor={next_cursor}"),
            Some(TOKEN),
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = body_json(resp2).await;
    assert_eq!(body2["data"].as_array().unwrap().len(), 1);
    assert!(body2["next_cursor"].is_null());
}

#[tokio::test]
async fn promote_returns_201_location() {
    let schema = sample_schema(9);
    let state = make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::ok(schema.clone()),
        HealthGate::new(),
    );
    let app = api::router(state);

    let cand_id = CandidateId::from_digest(&[5u8; 32]);
    let req_body = serde_json::to_value(PromoteRequest {
        family: FamilyChoice::New,
        name: Some("orders.created".to_string()),
        reason: "looks right".to_string(),
    })
    .unwrap();

    let resp = app
        .oneshot(post_json(
            &format!("/api/v1/candidates/{}/promote", cand_id.as_str()),
            Some(TOKEN),
            &req_body,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let expected_location = format!("/api/v1/schemas/{}", schema.schema_id.as_str());
    let location = resp
        .headers()
        .get("location")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(location, expected_location);

    let body = body_json(resp).await;
    assert_eq!(body["data"]["schema_id"], schema.schema_id.as_str());
}

#[tokio::test]
async fn promote_conflict_409() {
    let state = make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::conflict("already promoted"),
        HealthGate::new(),
    );
    let app = api::router(state);

    let cand_id = CandidateId::from_digest(&[6u8; 32]);
    let req_body = serde_json::to_value(PromoteRequest {
        family: FamilyChoice::New,
        name: Some("orders.created".to_string()),
        reason: "looks right".to_string(),
    })
    .unwrap();

    let resp = app
        .oneshot(post_json(
            &format!("/api/v1/candidates/{}/promote", cand_id.as_str()),
            Some(TOKEN),
            &req_body,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "conflict");
}

/// Task 14: `CoreError::PolicyRejected` (a candidate that hasn't crossed
/// the promotion guards) maps to 422, distinct from `Conflict`'s 409.
#[tokio::test]
async fn promote_policy_rejected_422() {
    let state = make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::policy_rejected("candidate has 1 sample(s), below the minimum of 10"),
        HealthGate::new(),
    );
    let app = api::router(state);

    let cand_id = CandidateId::from_digest(&[8u8; 32]);
    let req_body = serde_json::to_value(PromoteRequest {
        family: FamilyChoice::New,
        name: Some("orders.created".to_string()),
        reason: "too early".to_string(),
    })
    .unwrap();

    let resp = app
        .oneshot(post_json(
            &format!("/api/v1/candidates/{}/promote", cand_id.as_str()),
            Some(TOKEN),
            &req_body,
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "unprocessable_entity");
}

#[tokio::test]
async fn readyz_503_when_degraded() {
    let degraded_gate = HealthGate::new();
    degraded_gate.force_degraded_for_test();
    let degraded_app = api::router(make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        degraded_gate,
    ));
    let resp = degraded_app.oneshot(get("/readyz", None)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    let healthy_app = api::router(empty_state());
    let resp2 = healthy_app.oneshot(get("/readyz", None)).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
}

#[tokio::test]
async fn healthz_always_200() {
    let app = api::router(empty_state());
    let resp = app.oneshot(get("/healthz", None)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_200() {
    let app = api::router(empty_state());
    let resp = app.oneshot(get("/metrics", None)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_endpoint_exposes_text() {
    let app = api::router(empty_state());
    let resp = app.oneshot(get("/metrics", None)).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert_eq!(content_type, "text/plain; version=0.0.4");

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(
        body.contains("deblob_messages_total"),
        "expected deblob_messages_total in exposition text:\n{body}"
    );
}
