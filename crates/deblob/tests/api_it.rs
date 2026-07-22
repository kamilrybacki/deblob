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
use deblob::semantic_store::SemanticStore;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyVersion, SchemaId, SemanticId};
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore, Registry, SchemaRecord};
use deblob_core::revision::{
    AppendOutcome, Etag, ReasonCode, Revision, RevisionStatus, SemError, SignatureCandidates,
};
use deblob_core::semantic::SemanticMetadata;
use deblob_redis::health::HealthGate;
use deblob_umbrella::store::{InMemoryUmbrellaStore, UmbrellaStore};
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

    async fn family_version_schema(
        &self,
        family_id: &deblob_core::id::FamilyId,
        version: FamilyVersion,
    ) -> Result<Option<SchemaId>, CoreError> {
        Ok(self
            .schemas
            .iter()
            .find(|s| &s.family_id == family_id && s.version == version)
            .map(|s| s.schema_id.clone()))
    }

    async fn get_family(
        &self,
        family_id: &deblob_core::id::FamilyId,
    ) -> Result<Option<deblob_core::ports::FamilyRecord>, CoreError> {
        let current = self
            .schemas
            .iter()
            .filter(|s| &s.family_id == family_id)
            .map(|s| s.version.0)
            .max();
        Ok(current.map(|v| deblob_core::ports::FamilyRecord {
            family_id: family_id.clone(),
            current_version: FamilyVersion(v),
        }))
    }

    async fn list_family_versions(
        &self,
        family_id: &deblob_core::id::FamilyId,
    ) -> Result<Vec<FamilyVersion>, CoreError> {
        let mut versions: Vec<FamilyVersion> = self
            .schemas
            .iter()
            .filter(|s| &s.family_id == family_id)
            .map(|s| s.version)
            .collect();
        versions.sort();
        Ok(versions)
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

/// In-memory `SemanticStore` fake (Task 6). Reimplements
/// `SEM_APPEND_SCRIPT`'s semantics (`deblob-redis/src/lua.rs`) faithfully
/// enough for the management-API tests below: idempotency-by-bytes checked
/// FIRST (bypassing reason/etag), then a missing `reason` -> `MissingReason`,
/// then a CAS check against the per-schema etag -> `EtagConflict`, otherwise
/// append + advance the pointer + relink the reverse index.
#[derive(Default)]
struct FakeSemanticState {
    revisions: HashMap<SchemaId, Vec<Revision>>,
    etags: HashMap<SchemaId, u64>,
    index: HashMap<SemanticId, std::collections::HashSet<SchemaId>>,
}

#[derive(Default)]
struct FakeSemanticStore {
    state: StdMutex<FakeSemanticState>,
}

#[async_trait::async_trait]
impl SemanticStore for FakeSemanticStore {
    async fn append_revision(
        &self,
        sch_id: &SchemaId,
        metadata: &SemanticMetadata,
        canonical_bytes: &[u8],
        sem_id: &SemanticId,
        actor: &str,
        reason_code: ReasonCode,
        reason: &str,
        recorded_at: i64,
        effective_from: i64,
        expected_etag: Option<Etag>,
    ) -> Result<AppendOutcome, SemError> {
        let mut state = self.state.lock().unwrap();

        if let Some(active) = state
            .revisions
            .get(sch_id)
            .and_then(|history| history.last())
        {
            if active.canonical_semantic_bytes == canonical_bytes {
                let etag = *state.etags.get(sch_id).unwrap_or(&0);
                return Ok(AppendOutcome::AlreadyActive {
                    revision: active.clone(),
                    etag: Etag(etag),
                });
            }
        }

        if reason.is_empty() {
            return Err(SemError::MissingReason);
        }

        let current_etag = *state.etags.get(sch_id).unwrap_or(&0);
        let expected = expected_etag.map(|e| e.0).unwrap_or(0);
        if expected != current_etag {
            return Err(SemError::EtagConflict {
                expected: expected_etag,
                current: Etag(current_etag),
            });
        }

        let previous = state
            .revisions
            .get(sch_id)
            .and_then(|history| history.last());
        let previous_revision_id = previous.map(|r| r.revision_id.clone());
        let old_sem_id = previous.map(|r| r.sem_id.clone());

        let revision = Revision {
            revision_id: deblob_core::id::RevisionId::new_v7(),
            sch_id: sch_id.clone(),
            sem_id: sem_id.clone(),
            metadata: metadata.clone(),
            canonical_semantic_bytes: canonical_bytes.to_vec(),
            previous_revision_id,
            actor: actor.to_string(),
            reason_code,
            reason: reason.to_string(),
            recorded_at,
            effective_from,
            status: RevisionStatus::Active,
        };

        state
            .revisions
            .entry(sch_id.clone())
            .or_default()
            .push(revision.clone());
        let new_etag = current_etag + 1;
        state.etags.insert(sch_id.clone(), new_etag);

        if let Some(old) = &old_sem_id {
            if old != sem_id {
                if let Some(set) = state.index.get_mut(old) {
                    set.remove(sch_id);
                }
            }
        }
        state
            .index
            .entry(sem_id.clone())
            .or_default()
            .insert(sch_id.clone());

        Ok(AppendOutcome::Appended {
            revision,
            etag: Etag(new_etag),
        })
    }

    async fn active_semantic(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(SemanticMetadata, SemanticId, Etag)>, SemError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .revisions
            .get(sch_id)
            .and_then(|history| history.last())
            .map(|r| {
                let etag = *state.etags.get(sch_id).unwrap_or(&0);
                (r.metadata.clone(), r.sem_id.clone(), Etag(etag))
            }))
    }

    async fn active_revision(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(Revision, Etag)>, SemError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .revisions
            .get(sch_id)
            .and_then(|history| history.last())
            .map(|r| {
                let etag = *state.etags.get(sch_id).unwrap_or(&0);
                (r.clone(), Etag(etag))
            }))
    }

    async fn revisions(&self, sch_id: &SchemaId) -> Result<Vec<Revision>, SemError> {
        let state = self.state.lock().unwrap();
        Ok(state.revisions.get(sch_id).cloned().unwrap_or_default())
    }

    async fn schemas_by_semantic(&self, sem_id: &SemanticId) -> Result<Vec<SchemaId>, SemError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .index
            .get(sem_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default())
    }

    /// Task 10: brute-force in-memory postings lookup — every ACTIVE
    /// (latest-revision) schema whose signature shares at least one of
    /// `feature_keys_hex` with the query. Faithful to the real
    /// `deblob-redis` behavior (may include the query schema itself;
    /// bounded the same way), just without an actual Redis SET behind it —
    /// fine for the management-API tests, which don't exercise the
    /// 20,000-candidate bound (that has real coverage in
    /// `deblob-redis/tests/semantic_it.rs`).
    async fn signature_candidates(
        &self,
        feature_keys_hex: &[String],
    ) -> Result<SignatureCandidates, SemError> {
        let wanted: std::collections::HashSet<&String> = feature_keys_hex.iter().collect();
        let state = self.state.lock().unwrap();
        let ids: Vec<SchemaId> = state
            .revisions
            .iter()
            .filter_map(|(sch_id, history)| {
                let active = history.last()?;
                let sig = deblob_semantic::signature::semantic_signature(&active.metadata);
                sig.feature_keys_hex()
                    .iter()
                    .any(|k| wanted.contains(k))
                    .then(|| sch_id.clone())
            })
            .collect();
        if ids.len() > deblob_core::revision::MAX_SIGNATURE_CANDIDATES {
            return Ok(SignatureCandidates::TooBroad);
        }
        Ok(SignatureCandidates::Bounded(ids))
    }

    async fn idf_stats(&self, feature_keys_hex: &[String]) -> Result<(u64, Vec<u64>), SemError> {
        // Saturating stats (see the FixtureStore note in semantic_neighbors.rs):
        // these API tests assert orchestration, not corpus-relative IDF demotion.
        Ok((u64::from(u32::MAX), vec![1; feature_keys_hex.len()]))
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
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

fn make_state(
    registry: FakeRegistry,
    evidence: FakeEvidence,
    promoter: FakePromoter,
    health: HealthGate,
    semantic: Arc<dyn SemanticStore>,
) -> ApiState {
    ApiState {
        registry: Arc::new(registry),
        evidence: Arc::new(evidence),
        health,
        token: SecretToken::new(TOKEN),
        promoter: Arc::new(promoter),
        metrics: Metrics::new(),
        semantic,
        semantic_registries: Arc::new(deblob_semantic::Registries::default()),
        umbrellas: Arc::new(InMemoryUmbrellaStore::new()),
        sources: Arc::new(deblob_core::ports::InMemorySourceRegistry::default()),
        value_profiles: Arc::new(deblob_core::ports::InMemoryValueProfileStore::default()),
        enforce_value_guard: false,
        domain_gate_enforce: false,
        samples: None,
        samples_read_token: None,
        umbrella_min_support: 30,
        stream_tx: tokio::sync::broadcast::channel(16).0,
    }
}

fn empty_state() -> ApiState {
    make_state(
        FakeRegistry::new(vec![]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    )
}

/// A `SchemaRecord` published as a specific `version` within a caller-given
/// `family_id` — needed for the family-endpoint tests, which require
/// multiple schemas sharing one family at distinct versions (unlike
/// `sample_schema`, which mints a fresh family per call).
fn family_schema(seed: u8, family_id: deblob_core::id::FamilyId, version: u32) -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[seed; 32]),
        family_id,
        version: FamilyVersion(version),
        canonical: "{}".to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

/// A schema with a real `deblob-canon-v1` shape (one top-level numeric
/// field, `"temperature"`) — needed for the semantic-annotation tests,
/// unlike `sample_schema`'s `"{}"` (which has no field paths at all, so
/// `validate_paths` would reject every field-level annotation).
fn semantic_schema(seed: u8) -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[seed; 32]),
        family_id: deblob_core::id::FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: r#"{"t":"obj","f":{"temperature":{"t":"num"}}}"#.to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

/// A `PutSemanticRequest` JSON body annotating `"temperature"` with a UCUM
/// unit `code` — swap `code` between calls to produce genuinely different
/// canonical bytes / `sem_` identities, mirroring
/// `deblob-redis/tests/semantic_it.rs`'s own `metadata_with_unit` fixture.
fn semantic_body(code: &str, reason_code: Option<&str>, reason: Option<&str>) -> Value {
    let mut body = serde_json::json!({
        "metadata": {
            "event_type": null,
            "fields": [
                {
                    "path": [{"key": "temperature"}],
                    "semantics": {
                        "canonical_field_id": null,
                        "identifier_namespace": null,
                        "unit": {"system": "ucum", "code": code},
                        "numeric_scale": null,
                        "temporal": null,
                        "enum_semantics": null
                    }
                }
            ]
        }
    });
    let obj = body.as_object_mut().unwrap();
    if let Some(rc) = reason_code {
        obj.insert("reason_code".to_string(), Value::String(rc.to_string()));
    }
    if let Some(r) = reason {
        obj.insert("reason".to_string(), Value::String(r.to_string()));
    }
    body
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

fn put_json(
    uri: &str,
    bearer: Option<&str>,
    if_match: Option<&str>,
    json: &Value,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    if let Some(etag) = if_match {
        builder = builder.header("if-match", etag);
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
        Arc::new(FakeSemanticStore::default()),
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
        Arc::new(FakeSemanticStore::default()),
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
        Arc::new(FakeSemanticStore::default()),
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
        Arc::new(FakeSemanticStore::default()),
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
        Arc::new(FakeSemanticStore::default()),
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

// ---------------------------------------------------------------------
// Semantic governance API (P2-D Task 6)
// ---------------------------------------------------------------------

fn semantic_uri(sch_id: &SchemaId) -> String {
    format!("/api/v1/schemas/{}/semantic", sch_id.as_str())
}

#[tokio::test]
async fn put_first_annotation_returns_201_with_sem_and_etag() {
    let schema = semantic_schema(20);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store.clone(),
    );
    let app = api::router(state);

    let body = semantic_body("Cel", Some("correction"), Some("initial annotation"));
    let resp = app
        .oneshot(put_json(&semantic_uri(&sch_id), Some(TOKEN), None, &body))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let etag = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(etag, "\"1\"");

    let json = body_json(resp).await;
    let sem = json["data"]["semantic_fingerprint"].as_str().unwrap();
    assert!(sem.starts_with("sem_"), "expected sem_ prefix, got {sem}");
    assert_eq!(
        json["data"]["metadata"]["fields"][0]["semantics"]["unit"]["code"],
        "Cel"
    );

    // The response `ETag` header and the response body's `sem_` must
    // describe the SAME revision, because both are threaded through from
    // the ONE atomic `append_revision` call (`AppendOutcome::etag()`) —
    // never from a separate re-read of the store's active pointer.
    let (_, active_sem, active_etag) = semantic_store
        .active_semantic(&sch_id)
        .await
        .unwrap()
        .expect("must be annotated after the PUT");
    assert_eq!(active_etag, Etag(1));
    assert_eq!(
        active_sem.as_str(),
        sem,
        "ETag header's revision and the response body's sem_ must be the SAME atomic result"
    );
}

#[tokio::test]
async fn put_identical_bytes_replay_is_200_no_new_revision() {
    let schema = semantic_schema(21);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store.clone(),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);
    let body = semantic_body("Cel", Some("correction"), Some("initial annotation"));

    let first = app
        .clone()
        .oneshot(put_json(&uri, Some(TOKEN), None, &body))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);

    let second = app
        .oneshot(put_json(&uri, Some(TOKEN), None, &body))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let etag = second
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(etag, "\"1\"", "idempotent replay must not advance the etag");

    let history = semantic_store.revisions(&sch_id).await.unwrap();
    assert_eq!(
        history.len(),
        1,
        "idempotent replay must not create a new revision"
    );
}

#[tokio::test]
async fn put_different_bytes_without_reason_is_400() {
    let schema = semantic_schema(22);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store.clone(),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let first = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);

    // Different unit code, no reason/reason_code at all.
    let changed = semantic_body("K", None, None);
    let resp = app
        .oneshot(put_json(&uri, Some(TOKEN), Some("\"1\""), &changed))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "bad_request");

    // Nothing must have been written.
    assert_eq!(semantic_store.revisions(&sch_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn put_different_bytes_with_stale_or_missing_etag_is_409() {
    let schema = semantic_schema(23);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store.clone(),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let first = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);

    let changed = semantic_body("K", Some("correction"), Some("fix the unit"));

    // Stale etag.
    let resp = app
        .clone()
        .oneshot(put_json(&uri, Some(TOKEN), Some("\"999\""), &changed))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "conflict");

    // Missing If-Match on an already-annotated schema.
    let resp2 = app
        .oneshot(put_json(&uri, Some(TOKEN), None, &changed))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::CONFLICT);

    assert_eq!(semantic_store.revisions(&sch_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn put_different_bytes_with_reason_and_correct_etag_is_201_with_new_sem_and_history() {
    let schema = semantic_schema(24);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store.clone(),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let first = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);
    let first_json = body_json(first).await;
    let first_sem = first_json["data"]["semantic_fingerprint"]
        .as_str()
        .unwrap()
        .to_string();

    let changed = semantic_body("K", Some("correction"), Some("fix the unit"));
    let resp = app
        .oneshot(put_json(&uri, Some(TOKEN), Some("\"1\""), &changed))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
    let etag = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(etag, "\"2\"");
    let json = body_json(resp).await;
    let second_sem = json["data"]["semantic_fingerprint"].as_str().unwrap();
    assert_ne!(second_sem, first_sem);

    let history = semantic_store.revisions(&sch_id).await.unwrap();
    assert_eq!(history.len(), 2, "both revisions must be retained");
}

#[tokio::test]
async fn put_unknown_unit_code_is_422_naming_the_token() {
    let schema = semantic_schema(25);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let body = semantic_body(
        "not-a-real-unit",
        Some("correction"),
        Some("initial annotation"),
    );
    let resp = app
        .oneshot(put_json(&semantic_uri(&sch_id), Some(TOKEN), None, &body))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = body_json(resp).await;
    assert_eq!(json["error"]["code"], "unprocessable_entity");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("not-a-real-unit"),
        "422 must name the offending token, got: {message}"
    );
}

#[tokio::test]
async fn put_absent_path_is_422_naming_the_path() {
    let schema = semantic_schema(26);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let mut body = semantic_body("Cel", Some("correction"), Some("initial annotation"));
    body["metadata"]["fields"][0]["path"][0]["key"] = Value::String("nonexistent".to_string());

    let resp = app
        .oneshot(put_json(&semantic_uri(&sch_id), Some(TOKEN), None, &body))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let json = body_json(resp).await;
    assert_eq!(json["error"]["code"], "unprocessable_entity");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("nonexistent"),
        "422 must name the offending path, got: {message}"
    );
}

#[tokio::test]
async fn get_semantic_returns_404_when_never_annotated() {
    let schema = semantic_schema(27);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let resp = app
        .oneshot(get(&semantic_uri(&sch_id), Some(TOKEN)))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_semantic_returns_active_and_etag() {
    let schema = semantic_schema(28);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store,
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let put_resp = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::CREATED);

    let resp = app.oneshot(get(&uri, Some(TOKEN))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = resp
        .headers()
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(etag, "\"1\"");
    let json = body_json(resp).await;
    assert_eq!(
        json["data"]["metadata"]["fields"][0]["semantics"]["unit"]["code"],
        "Cel"
    );
}

/// P2-D polish Task 3: `enum_semantics` is now a LIST of typed
/// `{value, meaning}` entries (never a `value -> meaning` JSON object) —
/// verifies the PUT wire shape end to end, not just the pure crate-level
/// canon/digest tests.
#[tokio::test]
async fn put_semantic_accepts_typed_enum_value_list_shape() {
    let schema = semantic_schema(41);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let mut body = semantic_body("Cel", Some("correction"), Some("initial annotation"));
    body["metadata"]["fields"][0]["semantics"]["enum_semantics"] = serde_json::json!([
        {
            "value": {"type": "string", "v": "ACTIVE"},
            "meaning": {"vocabulary": "deblob/order-status/v1", "code": "pending"}
        },
        {
            "value": {"type": "bool", "v": true},
            "meaning": {"vocabulary": "deblob/order-status/v1", "code": "confirmed"}
        }
    ]);

    let resp = app
        .oneshot(put_json(&uri, Some(TOKEN), None, &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json = body_json(resp).await;
    assert_eq!(
        json["data"]["metadata"]["fields"][0]["semantics"]["enum_semantics"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

/// The pre-P2D-polish wire shape (`enum_semantics` as a `value -> meaning`
/// JSON object) is now a hard rejection — never silently accepted/coerced.
/// Valid JSON that doesn't match the target Rust shape surfaces as axum's
/// `JsonRejection::JsonDataError`, which axum itself maps to `422` (`400`
/// is reserved for actually-malformed JSON syntax) — this handler never
/// even runs, since `Json<PutSemanticRequest>` extraction fails first.
#[tokio::test]
async fn put_semantic_rejects_legacy_object_shaped_enum_semantics() {
    let schema = semantic_schema(42);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let mut body = semantic_body("Cel", Some("correction"), Some("initial annotation"));
    body["metadata"]["fields"][0]["semantics"]["enum_semantics"] = serde_json::json!({
        "ACTIVE": {"vocabulary": "deblob/order-status/v1", "code": "pending"}
    });

    let resp = app
        .oneshot(put_json(&uri, Some(TOKEN), None, &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

/// An `enum_semantics[].value` with an unrecognized `"type"` tag is a hard
/// rejection (`422`, see the previous test's doc comment), never silently
/// coerced into one of the known variants.
#[tokio::test]
async fn put_semantic_rejects_unknown_enum_value_type_tag() {
    let schema = semantic_schema(43);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let mut body = semantic_body("Cel", Some("correction"), Some("initial annotation"));
    body["metadata"]["fields"][0]["semantics"]["enum_semantics"] = serde_json::json!([
        {
            "value": {"type": "array", "v": []},
            "meaning": {"vocabulary": "deblob/order-status/v1", "code": "pending"}
        }
    ]);

    let resp = app
        .oneshot(put_json(&uri, Some(TOKEN), None, &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn get_semantic_revisions_lists_history() {
    let schema = semantic_schema(29);
    let sch_id = schema.schema_id.clone();
    let semantic_store = Arc::new(FakeSemanticStore::default());
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        semantic_store,
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let first = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);

    let second = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            Some("\"1\""),
            &semantic_body("K", Some("correction"), Some("fix the unit")),
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::CREATED);

    let revisions_uri = format!("/api/v1/schemas/{}/semantic/revisions", sch_id.as_str());
    let resp = app.oneshot(get(&revisions_uri, Some(TOKEN))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn get_schemas_by_semantic_lists_the_schema() {
    let schema = semantic_schema(30);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);
    let uri = semantic_uri(&sch_id);

    let put_resp = app
        .clone()
        .oneshot(put_json(
            &uri,
            Some(TOKEN),
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::CREATED);
    let put_json_body = body_json(put_resp).await;
    let sem_id = put_json_body["data"]["semantic_fingerprint"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .oneshot(get(&format!("/api/v1/semantic/{sem_id}"), Some(TOKEN)))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    let schemas = json["data"].as_array().unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0], sch_id.as_str());
}

#[tokio::test]
async fn semantic_endpoints_require_bearer() {
    let schema = semantic_schema(31);
    let sch_id = schema.schema_id.clone();
    let state = make_state(
        FakeRegistry::new(vec![schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let resp = app
        .clone()
        .oneshot(get(&semantic_uri(&sch_id), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp2 = app
        .oneshot(put_json(
            &semantic_uri(&sch_id),
            None,
            None,
            &semantic_body("Cel", Some("correction"), Some("initial annotation")),
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------
// Family reads (P2-D polish Task 2)
// ---------------------------------------------------------------------

#[tokio::test]
async fn get_family_returns_record_with_current_version() {
    let family_id = deblob_core::id::FamilyId::new_v7();
    let s1 = family_schema(40, family_id.clone(), 1);
    let s2 = family_schema(41, family_id.clone(), 2);
    let state = make_state(
        FakeRegistry::new(vec![s1, s2]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let resp = app
        .oneshot(get(
            &format!("/api/v1/families/{}", family_id.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["data"]["family_id"], family_id.as_str());
    assert_eq!(body["data"]["current_version"], 2);
}

#[tokio::test]
async fn get_family_404_when_unknown() {
    let app = api::router(empty_state());
    let unknown = deblob_core::id::FamilyId::new_v7();

    let resp = app
        .oneshot(get(
            &format!("/api/v1/families/{}", unknown.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn get_family_versions_returns_all_versions_in_order() {
    let family_id = deblob_core::id::FamilyId::new_v7();
    let s1 = family_schema(42, family_id.clone(), 1);
    let s2 = family_schema(43, family_id.clone(), 2);
    let state = make_state(
        FakeRegistry::new(vec![s1, s2]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    let app = api::router(state);

    let resp = app
        .oneshot(get(
            &format!("/api/v1/families/{}/versions", family_id.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let versions: Vec<u64> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    assert_eq!(versions, vec![1, 2]);
}

#[tokio::test]
async fn get_family_versions_404_when_unknown() {
    let app = api::router(empty_state());
    let unknown = deblob_core::id::FamilyId::new_v7();

    let resp = app
        .oneshot(get(
            &format!("/api/v1/families/{}/versions", unknown.as_str()),
            Some(TOKEN),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn family_endpoints_require_bearer() {
    let family_id = deblob_core::id::FamilyId::new_v7();
    let app = api::router(empty_state());

    let resp = app
        .clone()
        .oneshot(get(
            &format!("/api/v1/families/{}", family_id.as_str()),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp2 = app
        .oneshot(get(
            &format!("/api/v1/families/{}/versions", family_id.as_str()),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------
// Candidate reindex (backfill endpoint)
// ---------------------------------------------------------------------

#[tokio::test]
async fn reindex_calls_through_evidence_store() {
    let app = api::router(empty_state());

    let resp = app
        .oneshot(post_json(
            "/api/v1/candidates/reindex",
            Some(TOKEN),
            &serde_json::json!({}),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    // `FakeEvidence` doesn't override `rebuild_candidate_index`, so it runs
    // the trait's default `Ok(0)` — still proves the route reaches
    // `EvidenceStore::rebuild_candidate_index` end to end.
    assert_eq!(body["data"]["reindexed"], 0);
}

#[tokio::test]
async fn reindex_requires_bearer() {
    let app = api::router(empty_state());

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/candidates/reindex")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------
// Umbrella governance: approve + lineage assertion
// ---------------------------------------------------------------------

/// A minimal, monoid-canonical child schema whose one top-level field
/// (`"dt"`, a number) is the source of the transform built by
/// [`approve_fixture`].
fn lineage_child_schema() -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[42u8; 32]),
        family_id: deblob_core::id::FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: r#"{"optional":false,"types":["object"],"children":{"dt":{"optional":false,"types":["number"]}}}"#.to_string(),
        canonicalizer: deblob_monoid::GENERALIZER.to_string(),
        provenance: serde_json::json!({}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

/// A provisional umbrella + its one statically-sound transform, sourced
/// from [`lineage_child_schema`] — everything `approve` needs to reach
/// `promote_bundle`/`put_lineage_assertion` without hitting any
/// verification issue.
fn approve_fixture() -> (
    deblob_umbrella::types::UmbrellaSchema,
    deblob_umbrella::types::ChildTransform,
) {
    use deblob_umbrella::types::{
        Binding, Cardinality, ChildTransform, FieldType, JsonPath, OnError, OnMissing, ScalarType,
        UmbrellaField, UmbrellaSchema,
    };

    let child_id = lineage_child_schema().schema_id.as_str().to_string();

    let umbrella = UmbrellaSchema {
        umbrella_id: "umb_test".into(),
        label: "test".into(),
        version: 1,
        fields: vec![UmbrellaField {
            canonical_field_id: deblob_core::semantic::CanonicalFieldId::new("event_time"),
            path: JsonPath::parse("$.event_time").unwrap(),
            name: "event_time".into(),
            ty: FieldType::Scalar(ScalarType::Decimal),
            unit: None,
            cardinality: Cardinality::Required,
        }],
    };

    let transform = ChildTransform {
        child_schema_id: child_id.clone(),
        umbrella_id: "umb_test".into(),
        child_revision: format!("{child_id}@1"),
        umbrella_revision: "umb_test@1".into(),
        bindings: vec![Binding {
            source: JsonPath::parse("$.dt").unwrap(),
            target: JsonPath::parse("$.event_time").unwrap(),
            ops: vec![],
            on_missing: OnMissing::Reject,
            on_error: OnError::Reject,
        }],
        unmapped_source_paths: vec![],
    };

    (umbrella, transform)
}

#[tokio::test]
async fn approve_writes_lineage_assertion() {
    use deblob_umbrella::store::UmbrellaState;

    let child_schema = lineage_child_schema();
    let child_id = child_schema.schema_id.as_str().to_string();
    let (umbrella, transform) = approve_fixture();

    let umbrella_store = InMemoryUmbrellaStore::new();
    umbrella_store
        .put_umbrella(&umbrella, UmbrellaState::Provisional)
        .await
        .unwrap();
    umbrella_store.put_transform(&transform).await.unwrap();

    let mut state = make_state(
        FakeRegistry::new(vec![child_schema]),
        FakeEvidence::default(),
        FakePromoter::conflict("unused"),
        HealthGate::new(),
        Arc::new(FakeSemanticStore::default()),
    );
    state.umbrellas = Arc::new(umbrella_store);
    let app = api::router(state);

    // Not yet approved: no lineage assertion exists.
    let resp = app
        .clone()
        .oneshot(get("/api/v1/umbrellas/umb_test/lineage", Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = app
        .clone()
        .oneshot(post_json(
            "/api/v1/umbrellas/umb_test/approve",
            Some(TOKEN),
            &serde_json::json!({"reason": "consolidating weather sources"}),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{:?}", body_json(resp).await);

    let resp = app
        .oneshot(get("/api/v1/umbrellas/umb_test/lineage", Some(TOKEN)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["data"]["umbrella_id"], "umb_test");
    assert_eq!(body["data"]["umbrella_version"], 1);
    assert_eq!(
        body["data"]["approved_reason"],
        "consolidating weather sources"
    );
    assert_eq!(body["data"]["members"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"]["members"][0]["child_schema_id"], child_id);
    assert_eq!(body["data"]["members"][0]["transform_present"], true);
}

#[tokio::test]
async fn lineage_404_for_unknown_umbrella() {
    let app = api::router(empty_state());

    let resp = app
        .oneshot(get("/api/v1/umbrellas/umb_missing/lineage", Some(TOKEN)))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
