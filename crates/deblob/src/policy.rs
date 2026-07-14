//! Promotion policy guards + the concrete [`Promoter`] implementation
//! (spec §5, §6, §8): a candidate becomes an authoritative `SchemaRecord`
//! only after crossing an evidentiary bar — never on a single sample, and
//! never the instant it's first observed.

use std::sync::Arc;

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, EvidenceStore, Registry, SchemaRecord};
use deblob_fingerprint::{bucket_key, ShapeSummary, CANONICALIZER};
use deblob_monoid::{FieldNode, Profile};

use crate::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};

/// Default minimum number of observed samples before a candidate may be
/// promoted (spec §5/§6).
pub const DEFAULT_MIN_SAMPLES: u64 = 10;

/// Default minimum age (last-seen minus first-seen), in milliseconds,
/// before a candidate may be promoted: 5 minutes (spec §5/§6). Guards
/// against a burst of identical traffic in the first second rushing a
/// schema to publication.
pub const DEFAULT_MIN_AGE_MS: i64 = 5 * 60 * 1000;

/// Promotion guard thresholds. Immutable, `Copy`-able config — construct
/// with [`PromotionPolicy::default`] for the spec defaults, or override
/// either field directly for tests/tuning.
#[derive(Debug, Clone, Copy)]
pub struct PromotionPolicy {
    pub min_samples: u64,
    pub min_age_ms: i64,
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        Self {
            min_samples: DEFAULT_MIN_SAMPLES,
            min_age_ms: DEFAULT_MIN_AGE_MS,
        }
    }
}

impl PromotionPolicy {
    /// `Ok(())` iff `cand` has crossed both guards; otherwise `Err` with a
    /// human-readable reason, surfaced verbatim to the API caller as the
    /// 422 response's error message.
    pub fn check(&self, cand: &CandidateRecord) -> Result<(), String> {
        if cand.sample_count < self.min_samples {
            return Err(format!(
                "candidate has {} sample(s), below the minimum of {}",
                cand.sample_count, self.min_samples
            ));
        }
        let age_ms = cand.last_seen_ms - cand.first_seen_ms;
        if age_ms < self.min_age_ms {
            return Err(format!(
                "candidate has been observed for {age_ms}ms, below the minimum age of {}ms",
                self.min_age_ms
            ));
        }
        Ok(())
    }
}

/// Concrete [`crate::promote::Promoter`]: guards a candidate against
/// [`PromotionPolicy`], builds a [`SchemaRecord`] from its GENERALIZED
/// profile, and publishes it through [`Registry::publish`].
pub struct Promoter {
    registry: Arc<dyn Registry>,
    evidence: Arc<dyn EvidenceStore>,
    policy: PromotionPolicy,
}

impl Promoter {
    /// Build a `Promoter` with the default [`PromotionPolicy`].
    pub fn new(registry: Arc<dyn Registry>, evidence: Arc<dyn EvidenceStore>) -> Self {
        Self::with_policy(registry, evidence, PromotionPolicy::default())
    }

    /// Build a `Promoter` with an explicit [`PromotionPolicy`] (tests /
    /// non-default deployments).
    pub fn with_policy(
        registry: Arc<dyn Registry>,
        evidence: Arc<dyn EvidenceStore>,
        policy: PromotionPolicy,
    ) -> Self {
        Self {
            registry,
            evidence,
            policy,
        }
    }
}

#[async_trait]
impl PromoterTrait for Promoter {
    async fn promote(
        &self,
        cand: &CandidateId,
        req: PromoteRequest,
        actor: &str,
    ) -> Result<SchemaRecord, CoreError> {
        let record = self
            .evidence
            .get_candidate(cand)
            .await?
            .ok_or(CoreError::NotFound)?;

        self.policy
            .check(&record)
            .map_err(CoreError::PolicyRejected)?;

        let profile: Profile = serde_json::from_value(record.profile.clone()).map_err(|e| {
            CoreError::RegistryUnavailable(format!("corrupt candidate profile: {e}"))
        })?;

        let schema_id = SchemaId::from_digest(&profile.generalized_fingerprint());
        let family_id = match req.family {
            FamilyChoice::New => FamilyId::new_v7(),
            FamilyChoice::Existing(id) => id,
        };

        let provenance = serde_json::json!({
            "candidate_id": cand.as_str(),
            "sample_count": record.sample_count,
            "first_seen_ms": record.first_seen_ms,
            "last_seen_ms": record.last_seen_ms,
            "name": req.name,
            "promoted_by": actor,
        });

        // `version` here is only ever a caller-side guess (spec §6,
        // `Registry::publish` docs) — the registry is the sole authority
        // and overwrites it below with the value it actually allocated.
        let draft = SchemaRecord {
            schema_id,
            family_id,
            version: FamilyVersion(0),
            canonical: profile.generalized_canonical_json(),
            canonicalizer: CANONICALIZER.to_string(),
            provenance,
        };

        let bucket = bucket_key(&generalized_shape_summary(&profile));

        // Task 14 fix: replay every CONCRETE shape observed for this
        // candidate (recorded by `ColdLane::ingest` via
        // `EvidenceStore::add_variant`) into the structural index alongside
        // the schema itself, atomically. `schema_id` above is derived from
        // the GENERALIZED profile — a different fingerprint domain than any
        // single concrete observation (spec §5) — so without this, a
        // hot-path lookup for a message that's actually been seen can never
        // match `schema_id`'s own digest. An empty vec (a candidate
        // promoted with no ingest history) is valid: it just means nothing
        // extra gets indexed, never a hard failure.
        let variants = self.evidence.get_variants(cand).await?;

        let version = self
            .registry
            .publish(draft.clone(), cand, &bucket, &variants, actor, &req.reason)
            .await?;

        Ok(SchemaRecord { version, ..draft })
    }
}

/// Derives a [`ShapeSummary`] from a candidate's GENERALIZED `Profile` so
/// `Registry::publish` can index the schema under the same structural
/// index the hot-path matcher/`bucket_key` scheme uses. `Profile` doesn't
/// retain a concrete `Shape` (it tracks type-union statistics per field,
/// which a `Shape` can't represent at object-field granularity), so this
/// reconstructs the summary directly from the profile's root `FieldNode`:
/// top-level field count and sorted key set come from `root.children`
/// (populated only when the root was ever observed as an object, matching
/// `deblob_fingerprint::summarize`'s non-object-root convention of `0`
/// fields), and `depth` walks children/array-element nesting the same way
/// `deblob_fingerprint::shape::shape_depth` does (scalar/empty container
/// depth 1, `1 + max(child depths)` otherwise).
fn generalized_shape_summary(profile: &Profile) -> ShapeSummary {
    let root = &profile.root;
    ShapeSummary {
        top_level_fields: root.children.len(),
        depth: field_depth(root),
        top_keys_sorted: root.children.keys().cloned().collect(),
    }
}

fn field_depth(field: &FieldNode) -> u32 {
    let mut max_child = 0u32;
    for child in field.children.values() {
        max_child = max_child.max(field_depth(child));
    }
    if let Some(elem) = &field.array_elem {
        max_child = max_child.max(field_depth(elem));
    }
    1 + max_child
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::CandidateId as CandId;
    use deblob_core::ports::{CandidateState, SchemaRecord as CoreSchemaRecord};
    use deblob_fingerprint::{fingerprint, parse_bounded, shape_of, Limits};
    use deblob_monoid::Profile as MonoidProfile;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct FakeEvidence {
        candidates: StdMutex<HashMap<CandId, CandidateRecord>>,
        clusters: StdMutex<HashMap<String, CandId>>,
        variants: StdMutex<HashMap<CandId, Vec<(String, String)>>>,
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
        async fn get_candidate(&self, id: &CandId) -> Result<Option<CandidateRecord>, CoreError> {
            Ok(self.candidates.lock().unwrap().get(id).cloned())
        }
        async fn list_candidates(
            &self,
            _state: CandidateState,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn append_evidence(
            &self,
            _id: &CandId,
            _stats: serde_json::Value,
        ) -> Result<(), CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn set_state(&self, _id: &CandId, _state: CandidateState) -> Result<(), CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandId>, CoreError> {
            Ok(self.clusters.lock().unwrap().get(gen_fp).cloned())
        }
        async fn set_cluster(&self, gen_fp: &str, cand_id: &CandId) -> Result<(), CoreError> {
            self.clusters
                .lock()
                .unwrap()
                .insert(gen_fp.to_string(), cand_id.clone());
            Ok(())
        }
        async fn add_variant(
            &self,
            cand_id: &CandId,
            bucket_key: &str,
            fp_b32: &str,
        ) -> Result<(), CoreError> {
            self.variants
                .lock()
                .unwrap()
                .entry(cand_id.clone())
                .or_default()
                .push((bucket_key.to_string(), fp_b32.to_string()));
            Ok(())
        }
        async fn get_variants(&self, cand_id: &CandId) -> Result<Vec<(String, String)>, CoreError> {
            Ok(self
                .variants
                .lock()
                .unwrap()
                .get(cand_id)
                .cloned()
                .unwrap_or_default())
        }
    }

    /// One recorded `Registry::publish` call, captured verbatim so tests
    /// can assert exactly what the promoter sent.
    struct PublishCall {
        record: CoreSchemaRecord,
        alias_from: CandId,
        bucket_key: String,
        variant_members: Vec<(String, String)>,
        actor: String,
        reason: String,
    }

    /// Records every `publish` call so tests can assert what the promoter
    /// sent, and returns a configurable authoritative `FamilyVersion` —
    /// standing in for the registry's `HINCRBY`-allocated version (spec
    /// §6: the registry is the sole authority, never the caller's guess).
    struct FakeRegistry {
        authoritative_version: FamilyVersion,
        published: StdMutex<Vec<PublishCall>>,
    }

    impl FakeRegistry {
        fn new(authoritative_version: u32) -> Self {
            Self {
                authoritative_version: FamilyVersion(authoritative_version),
                published: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Registry for FakeRegistry {
        async fn get_schema(&self, _id: &SchemaId) -> Result<Option<CoreSchemaRecord>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn publish(
            &self,
            record: CoreSchemaRecord,
            alias_from: &CandId,
            bucket_key: &str,
            variant_members: &[(String, String)],
            actor: &str,
            reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            self.published.lock().unwrap().push(PublishCall {
                record,
                alias_from: alias_from.clone(),
                bucket_key: bucket_key.to_string(),
                variant_members: variant_members.to_vec(),
                actor: actor.to_string(),
                reason: reason.to_string(),
            });
            Ok(self.authoritative_version)
        }
        async fn get_alias(&self, _id: &CandId) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<CoreSchemaRecord>, Option<String>), CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
    }

    fn profile_of(json: &str) -> MonoidProfile {
        let node = parse_bounded(json.as_bytes(), &Limits::default()).unwrap();
        MonoidProfile::from_node(&node)
    }

    fn candidate_record(
        id: CandId,
        sample_count: u64,
        first_seen_ms: i64,
        last_seen_ms: i64,
    ) -> CandidateRecord {
        let profile = profile_of(r#"{"a":1,"b":"x"}"#);
        CandidateRecord {
            candidate_id: id,
            profile: serde_json::to_value(&profile).unwrap(),
            sample_count,
            first_seen_ms,
            last_seen_ms,
            state: CandidateState::Provisional,
        }
    }

    fn some_cand_id() -> CandId {
        let node = parse_bounded(br#"{"a":1,"b":"x"}"#, &Limits::default()).unwrap();
        CandId::from_digest(&fingerprint(&shape_of(&node)))
    }

    fn request() -> PromoteRequest {
        PromoteRequest {
            family: FamilyChoice::New,
            name: Some("orders.created".to_string()),
            reason: "manually reviewed".to_string(),
        }
    }

    #[tokio::test]
    async fn promote_missing_candidate_is_not_found() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(1));
        let promoter = Promoter::new(registry, evidence);

        let err = promoter
            .promote(&some_cand_id(), request(), "alice")
            .await
            .unwrap_err();

        assert!(matches!(err, CoreError::NotFound));
    }

    #[tokio::test]
    async fn promote_below_min_samples_rejected() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(1));
        let cand_id = some_cand_id();
        evidence
            .upsert_candidate(candidate_record(cand_id.clone(), 1, 0, 10_000_000))
            .await
            .unwrap();
        let promoter = Promoter::new(registry.clone(), evidence);

        let err = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap_err();

        assert!(matches!(err, CoreError::PolicyRejected(_)));
        assert!(registry.published.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn promote_below_min_age_rejected() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(1));
        let cand_id = some_cand_id();
        // Enough samples, but first_seen == last_seen (age 0).
        evidence
            .upsert_candidate(candidate_record(cand_id.clone(), 50, 1_000, 1_000))
            .await
            .unwrap();
        let promoter = Promoter::new(registry.clone(), evidence);

        let err = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap_err();

        assert!(matches!(err, CoreError::PolicyRejected(_)));
    }

    #[tokio::test]
    async fn promote_after_threshold_publishes() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(7));
        let cand_id = some_cand_id();
        let first_seen = 0i64;
        let last_seen = DEFAULT_MIN_AGE_MS + 1;
        evidence
            .upsert_candidate(candidate_record(
                cand_id.clone(),
                DEFAULT_MIN_SAMPLES,
                first_seen,
                last_seen,
            ))
            .await
            .unwrap();
        // Task 14 fix: two concrete variants recorded against this
        // candidate by (the real) `ColdLane::ingest` before promotion.
        evidence
            .add_variant(&cand_id, "deblob:index:2:2:aaaaaaaa", "rawfpvariantone")
            .await
            .unwrap();
        evidence
            .add_variant(&cand_id, "deblob:index:4:2:bbbbbbbb", "rawfpvarianttwo")
            .await
            .unwrap();
        let promoter = Promoter::new(registry.clone(), evidence);

        let schema = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap();

        assert_eq!(schema.version, FamilyVersion(7));

        let published = registry.published.lock().unwrap();
        assert_eq!(published.len(), 1);
        let call = &published[0];
        assert_eq!(call.alias_from, cand_id);
        assert_eq!(call.actor, "alice");
        assert_eq!(call.reason, "manually reviewed");
        assert_eq!(call.record.canonicalizer, CANONICALIZER);
        assert_eq!(call.record.schema_id, schema.schema_id);
        assert!(call.bucket_key.starts_with("deblob:index:"));

        // The candidate's recorded concrete variants must be threaded
        // through to `Registry::publish` verbatim — this is what lets the
        // registry index each of them onto the promoted schema id.
        let mut variants = call.variant_members.clone();
        variants.sort();
        let mut expected = vec![
            (
                "deblob:index:2:2:aaaaaaaa".to_string(),
                "rawfpvariantone".to_string(),
            ),
            (
                "deblob:index:4:2:bbbbbbbb".to_string(),
                "rawfpvarianttwo".to_string(),
            ),
        ];
        expected.sort();
        assert_eq!(variants, expected);
    }

    /// Task 14 fix: a candidate promoted with NO recorded variants (e.g.
    /// seeded without ingest history) must still promote successfully —
    /// `get_variants` returning empty is a valid, non-error case that just
    /// means nothing extra gets indexed.
    #[tokio::test]
    async fn promote_with_no_recorded_variants_still_publishes() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(1));
        let cand_id = some_cand_id();
        evidence
            .upsert_candidate(candidate_record(
                cand_id.clone(),
                DEFAULT_MIN_SAMPLES,
                0,
                DEFAULT_MIN_AGE_MS + 1,
            ))
            .await
            .unwrap();
        let promoter = Promoter::new(registry.clone(), evidence);

        let schema = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap();

        assert_eq!(schema.version, FamilyVersion(1));
        let published = registry.published.lock().unwrap();
        assert!(published[0].variant_members.is_empty());
    }

    #[test]
    fn policy_check_reports_both_guards() {
        let policy = PromotionPolicy::default();
        let too_few = candidate_record(some_cand_id(), 1, 0, DEFAULT_MIN_AGE_MS * 2);
        assert!(policy.check(&too_few).unwrap_err().contains("sample"));

        let too_young = candidate_record(some_cand_id(), DEFAULT_MIN_SAMPLES, 0, 10);
        assert!(policy.check(&too_young).unwrap_err().contains("age"));

        let ok = candidate_record(
            some_cand_id(),
            DEFAULT_MIN_SAMPLES,
            0,
            DEFAULT_MIN_AGE_MS + 1,
        );
        assert!(policy.check(&ok).is_ok());
    }
}
