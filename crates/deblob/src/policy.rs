//! Promotion policy guards + the concrete [`Promoter`] implementation
//! (spec §5, §6, §8): a candidate becomes an authoritative `SchemaRecord`
//! only after crossing an evidentiary bar — never on a single sample, and
//! never the instant it's first observed.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore, Registry, SchemaRecord};
use deblob_fingerprint::{bucket_key, ShapeSummary};
use deblob_monoid::{FieldNode, Profile, GENERALIZER};

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

/// Current wall-clock time in epoch milliseconds. Used by the promotion guards
/// to measure a candidate's real AGE (now - first_seen) rather than its
/// observation SPAN (last_seen - first_seen), so burst sources aren't starved.
pub(crate) fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl PromotionPolicy {
    /// `Ok(())` iff `cand` has crossed both guards; otherwise `Err` with a
    /// human-readable reason, surfaced verbatim to the API caller as the
    /// 422 response's error message.
    pub fn check(&self, cand: &CandidateRecord, now_ms: i64) -> Result<(), String> {
        if cand.sample_count < self.min_samples {
            return Err(format!(
                "candidate has {} sample(s), below the minimum of {}",
                cand.sample_count, self.min_samples
            ));
        }
        // WALL-CLOCK age (now - first_seen), NOT the observation span
        // (last_seen - first_seen). A burst source that emits its whole shape in
        // one poll has a ~0 span but is still genuinely old; the span-based check
        // permanently starved such candidates (auto-promote incident 2026-07-21).
        let age_ms = now_ms - cand.first_seen_ms;
        if age_ms < self.min_age_ms {
            return Err(format!(
                "candidate first seen {age_ms}ms ago, below the minimum age of {}ms",
                self.min_age_ms
            ));
        }
        Ok(())
    }
}

/// Default AUTOMATIC-promotion thresholds — deliberately STRICTER than the
/// manual [`PromotionPolicy`] because no human reviews the result: more
/// samples, longer observation, plus a shape-stability guard.
pub const DEFAULT_AUTO_MIN_SAMPLES: u64 = 50;
pub const DEFAULT_AUTO_MIN_AGE_MS: i64 = 10 * 60 * 1000; // 10 minutes
/// Minimum number of REQUIRED LEAF fields (a scalar present in every sample,
/// at any nesting depth). Two, not one, so a single-key envelope like
/// `{"data": <churning>}` — which has exactly one always-present top key but no
/// stable backbone underneath — can never clear the bar.
pub const DEFAULT_AUTO_MIN_REQUIRED_FIELDS: usize = 2;
pub const DEFAULT_AUTO_MIN_REQUIRED_RATIO: f64 = 0.5;

/// Walks a generalized [`FieldNode`] subtree, counting `(required_leaves,
/// total_leaves)`. A LEAF is a field with no children and no array element (a
/// scalar). `denom` is the number of observations in which THIS node's parent
/// held the container type that could contain it (`types.object` for object
/// children, `types.array` for the array element) — matching
/// `deblob_monoid::profile::write_generalized_field`'s own optionality
/// denominator. A leaf is REQUIRED iff it was present in every such observation
/// (`present == denom`, `denom > 0`); `present > denom` is corrupt data and is
/// NOT counted as required (fail closed).
fn count_leaves(node: &FieldNode, denom: u64, required: &mut usize, total: &mut usize) {
    if node.children.is_empty() && node.array_elem.is_none() {
        *total += 1;
        if denom > 0 && node.present == denom {
            *required += 1;
        }
        return;
    }
    for child in node.children.values() {
        count_leaves(child, node.types.object, required, total);
    }
    if let Some(elem) = &node.array_elem {
        count_leaves(elem, node.types.array, required, total);
    }
}

/// The deterministic bar a NEWLY-DISCOVERED candidate must clear before the
/// auto-promote sweep ([`crate::auto_promote`]) publishes it to a NEW family
/// WITHOUT a human in the loop. It is purposely a superset of
/// [`PromotionPolicy`]: the same sample/age guards PLUS a shape-stability
/// check. A novel candidate has no existing family to corroborate against, so
/// "the statistics are good" is expressed structurally — the generalized shape
/// must have settled into a real REQUIRED backbone (fields present in every
/// observed sample) rather than a still-churning bag of optionals. The model
/// still never decides: it only proposes the candidate; this deterministic
/// policy, on deterministic evidence, is what promotes.
#[derive(Debug, Clone, Copy)]
pub struct AutoPromotePolicy {
    pub min_samples: u64,
    pub min_age_ms: i64,
    /// Minimum number of REQUIRED LEAF fields (a scalar present in every
    /// sample, at any depth). A shape with no required backbone — including a
    /// single-key wrapper whose contents churn — is never confident enough.
    pub min_required_fields: usize,
    /// Minimum fraction (0.0–1.0) of LEAF fields that must be REQUIRED. A shape
    /// dominated by flapping optionals has not settled yet.
    pub min_required_ratio: f64,
}

impl Default for AutoPromotePolicy {
    fn default() -> Self {
        Self {
            min_samples: DEFAULT_AUTO_MIN_SAMPLES,
            min_age_ms: DEFAULT_AUTO_MIN_AGE_MS,
            min_required_fields: DEFAULT_AUTO_MIN_REQUIRED_FIELDS,
            min_required_ratio: DEFAULT_AUTO_MIN_REQUIRED_RATIO,
        }
    }
}

impl AutoPromotePolicy {
    /// `Ok(())` iff `cand` is statistically solid enough to promote with no
    /// human review; otherwise `Err` with a human-readable reason (logged by
    /// the sweep at debug level — this path has no API caller to surface it to).
    pub fn eligible(&self, cand: &CandidateRecord, now_ms: i64) -> Result<(), String> {
        // 1. The manual sample/age guards apply verbatim.
        PromotionPolicy {
            min_samples: self.min_samples,
            min_age_ms: self.min_age_ms,
        }
        .check(cand, now_ms)?;
        // 2. Shape stability from the candidate's GENERALIZED profile: walk to
        //    the LEAF fields (any depth) and require a real backbone of leaves
        //    present in EVERY sample. This looks past a single-key envelope
        //    (`{"data": <churn>}`) whose one always-present top key hides a
        //    still-churning interior.
        let profile: Profile = serde_json::from_value(cand.profile.clone())
            .map_err(|e| format!("corrupt candidate profile: {e}"))?;
        if profile.count == 0 {
            return Err("candidate profile has zero observations".to_string());
        }
        let (mut required, mut total) = (0usize, 0usize);
        // The root is the whole document; its own denominator is the total
        // observation count (`profile.count`).
        count_leaves(&profile.root, profile.count, &mut required, &mut total);
        if total == 0 {
            return Err("shape has no leaf fields — not settled".to_string());
        }
        if required < self.min_required_fields {
            return Err(format!(
                "shape has {required} required leaf field(s), below the minimum of {}",
                self.min_required_fields
            ));
        }
        // `- 1e-9` so an exact boundary (e.g. 2/4 == 0.5 against min 0.5)
        // isn't rejected by float representation noise; anything genuinely
        // below the threshold still fails.
        let ratio = required as f64 / total as f64;
        if ratio < self.min_required_ratio - 1e-9 {
            return Err(format!(
                "shape is {:.0}% required leaf fields, below the minimum {:.0}% — not settled",
                ratio * 100.0,
                self.min_required_ratio * 100.0
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
    /// Optional durable value-profile sidecar store (joint design
    /// `dc-umbrella-signals-1907`, Stage 1). When set, `promote` captures an
    /// immutable value-profile snapshot from the candidate's profile and
    /// references it from the published `SchemaRecord`. `None` (the default)
    /// keeps the pre-existing behavior verbatim — no capture — so every
    /// current caller/test is unaffected until it opts in.
    value_profiles: Option<Arc<dyn deblob_core::ports::ValueProfileStore>>,
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
            value_profiles: None,
        }
    }

    /// Opt in to durable value-profile capture at promotion.
    pub fn with_value_profiles(
        mut self,
        store: Arc<dyn deblob_core::ports::ValueProfileStore>,
    ) -> Self {
        self.value_profiles = Some(store);
        self
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

        // Promotion is a one-way transition that only a Provisional candidate
        // may take. Guarding here (not only in the API layer) closes the window
        // where the auto-promote sweep — or any concurrent caller — acts on a
        // candidate a human already rejected, and (together with the
        // `set_state(Staged)` below) stops an already-published candidate from
        // being re-promoted on every sweep tick. Residual: this state read and
        // the `set_state` write below are not one atomic transaction, so a
        // reject landing in between still races; the window is narrowed to a
        // single promote call, not eliminated (a fully atomic flip belongs in
        // the publish Lua script — tracked as a follow-up).
        if record.state != CandidateState::Provisional {
            return Err(CoreError::PolicyRejected(format!(
                "candidate is {:?}; only Provisional candidates can be promoted",
                record.state
            )));
        }

        self.policy
            .check(&record, now_epoch_ms())
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

        let canonical = profile.generalized_canonical_json();

        // Stage 1 (joint design dc-umbrella-signals-1907): capture an
        // immutable value-profile snapshot from the candidate's profile and
        // persist it to the sidecar store BEFORE publishing the schema that
        // references it, so a referenced profile is always already durable.
        // Only when a store is wired (`with_value_profiles`); otherwise the
        // schema is published with `value_profile_ref: None`, exactly as
        // before. Excluded from the `sch_` identity digest — `schema_id`
        // above is already fixed from the generalized fingerprint.
        let (value_profile_ref, value_profile_summary) = if let Some(store) = &self.value_profiles {
            let snapshot = crate::value_profile::build_snapshot(
                &schema_id,
                &canonical,
                cand,
                &profile,
                now_ms(),
            );
            let summary = snapshot.summary();
            store.put_value_profile(&snapshot).await?;
            (Some(snapshot.profile_id), Some(summary))
        } else {
            (None, None)
        };

        // `version` here is only ever a caller-side guess (spec §6,
        // `Registry::publish` docs) — the registry is the sole authority
        // and overwrites it below with the value it actually allocated.
        let draft = SchemaRecord {
            schema_id,
            family_id,
            version: FamilyVersion(0),
            canonical,
            canonicalizer: GENERALIZER.to_string(),
            provenance,
            semantic: None,
            semantic_fingerprint: None,
            privacy_class: None,
            value_profile_ref,
            value_profile_summary,
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

        // Move the candidate out of the Provisional scan set now that its
        // schema is published. There is no dedicated `Promoted` state; `Staged`
        // is the terminal "acted-upon, no longer a raw provisional candidate"
        // state (never set anywhere else in the codebase). Best-effort: the
        // publish is already committed, so a transient state-write failure must
        // not turn a successful promotion into an error the caller retries. If
        // it does fail, the state guard above plus the registry's write-once
        // alias semantics still prevent a duplicate schema on the next attempt.
        if let Err(err) = self.evidence.set_state(cand, CandidateState::Staged).await {
            tracing::warn!(
                candidate_id = %cand.as_str(),
                error = %err,
                "promote: schema published but candidate state transition to Staged failed"
            );
        }

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
/// `pub(crate)`: reused by `crate::retrieval` (deblob-p2ab Task 3) to
/// derive the same structural-index bucket a candidate's generalized
/// profile would be published under, without duplicating this logic.
pub(crate) fn generalized_shape_summary(profile: &Profile) -> ShapeSummary {
    let root = &profile.root;
    ShapeSummary {
        top_level_fields: root.children.len(),
        depth: field_depth(root),
        top_keys_sorted: root.children.keys().cloned().collect(),
    }
}

pub(crate) fn field_depth(field: &FieldNode) -> u32 {
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
        async fn set_state(&self, id: &CandId, state: CandidateState) -> Result<(), CoreError> {
            if let Some(rec) = self.candidates.lock().unwrap().get_mut(id) {
                rec.state = state;
            }
            Ok(())
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

        async fn list_families_in_buckets(
            &self,
            _bucket_keys: &[String],
        ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn list_families_by_band_depth(
            &self,
            _bands: &[u32],
            _depths: &[u32],
        ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }
        async fn family_version_schema(
            &self,
            _family_id: &FamilyId,
            _version: FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }

        async fn get_family(
            &self,
            _family_id: &FamilyId,
        ) -> Result<Option<deblob_core::ports::FamilyRecord>, CoreError> {
            unimplemented!("not exercised by promoter tests")
        }

        async fn list_family_versions(
            &self,
            _family_id: &FamilyId,
        ) -> Result<Vec<FamilyVersion>, CoreError> {
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
            source: None,
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
        // Enough samples, but first_seen is NOW (wall-clock age ~0 < min_age).
        let fresh = now_epoch_ms();
        evidence
            .upsert_candidate(candidate_record(cand_id.clone(), 50, fresh, fresh))
            .await
            .unwrap();
        let promoter = Promoter::new(registry.clone(), evidence);

        let err = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap_err();

        assert!(matches!(err, CoreError::PolicyRejected(_)));
    }

    // ---- AutoPromotePolicy (automatic promotion bar) ----

    #[test]
    fn auto_promote_below_min_samples_rejected() {
        // 10 samples < default auto min (50); old enough + settled shape.
        let cand = candidate_record(some_cand_id(), 10, 0, 700_000);
        let err = AutoPromotePolicy::default()
            .eligible(&cand, cand.last_seen_ms)
            .unwrap_err();
        assert!(err.contains("sample"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_below_min_age_rejected() {
        // Enough samples, but observed for 0ms (first_seen == last_seen).
        let cand = candidate_record(some_cand_id(), 60, 1_000, 1_000);
        let err = AutoPromotePolicy::default()
            .eligible(&cand, cand.last_seen_ms)
            .unwrap_err();
        assert!(err.contains("age"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_settled_shape_is_eligible() {
        // candidate_record's `{"a":1,"b":"x"}` profile => 2 required top-level
        // fields (present == count). 60 samples over 700s => eligible.
        let cand = candidate_record(some_cand_id(), 60, 0, 700_000);
        assert!(AutoPromotePolicy::default()
            .eligible(&cand, cand.last_seen_ms)
            .is_ok());
    }

    #[test]
    fn auto_promote_burst_source_eligible_by_wall_clock_not_span() {
        // Regression for the 2026-07-21 incident: a burst source emits its whole
        // shape in one poll, so the observation SPAN (last_seen - first_seen) is
        // ~0, yet the candidate is genuinely OLD by wall clock. Span-based age
        // permanently starved these; wall-clock age promotes them. Uses an
        // EXPLICIT synthetic `now` (not last_seen) so it actually exercises the
        // wall-clock-vs-span distinction — a silent revert to the span formula
        // would flip the first assert to Err.
        let now = 2_000_000_000_000i64;
        // first seen 20 min ago (> 10 min default), but span is only 3 ms.
        let first_seen = now - 20 * 60 * 1000;
        let burst = candidate_record(some_cand_id(), 60, first_seen, first_seen + 3);
        assert!(
            AutoPromotePolicy::default().eligible(&burst, now).is_ok(),
            "burst source (span 3ms, wall-clock age 20min) must be eligible"
        );
        // The age guard still bites a genuinely young candidate (5 ms old).
        let young = candidate_record(some_cand_id(), 60, now - 5, now);
        let err = AutoPromotePolicy::default()
            .eligible(&young, now)
            .unwrap_err();
        assert!(err.contains("age"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_requires_a_required_backbone() {
        // Same well-formed candidate, but the policy demands 5 required fields
        // and the shape only has 2 — not confident enough.
        let cand = candidate_record(some_cand_id(), 60, 0, 700_000);
        let policy = AutoPromotePolicy {
            min_required_fields: 5,
            ..AutoPromotePolicy::default()
        };
        let err = policy.eligible(&cand, cand.last_seen_ms).unwrap_err();
        assert!(err.contains("required leaf"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_rejects_a_shapeless_scalar() {
        // A candidate whose observations are bare scalars has a single leaf and
        // no field backbone — below the default 2-required-leaf minimum.
        let mut cand = candidate_record(some_cand_id(), 60, 0, 700_000);
        cand.profile = serde_json::to_value(profile_of("5")).unwrap();
        let err = AutoPromotePolicy::default()
            .eligible(&cand, cand.last_seen_ms)
            .unwrap_err();
        assert!(err.contains("required leaf"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_rejects_single_key_envelope() {
        // {"data": {"x": <varies>}} — one always-present top key, but the
        // interior churns, so the recursion finds no stable leaf backbone.
        // `profile_of` merges two differently-shaped observations of the same
        // wrapper so the inner field is optional (present in 1 of 2).
        let a = profile_of(r#"{"data":{"x":1}}"#);
        let b = profile_of(r#"{"data":{"y":2}}"#);
        let merged = MonoidProfile::merge(&a, &b);
        let mut cand = candidate_record(some_cand_id(), 60, 0, 700_000);
        cand.profile = serde_json::to_value(&merged).unwrap();
        let err = AutoPromotePolicy::default()
            .eligible(&cand, cand.last_seen_ms)
            .unwrap_err();
        assert!(err.contains("required leaf"), "reason was: {err}");
    }

    #[test]
    fn auto_promote_ratio_guard_rejects_optional_heavy_shape() {
        // One required leaf + three optionals => ratio 1/4 = 0.25 < 0.5.
        // Merge a full record with a minimal one so three fields go optional.
        let full = profile_of(r#"{"id":"a","p":1,"q":2,"r":3}"#);
        let min = profile_of(r#"{"id":"b"}"#);
        let merged = MonoidProfile::merge(&full, &min);
        let mut cand = candidate_record(some_cand_id(), 60, 0, 700_000);
        cand.profile = serde_json::to_value(&merged).unwrap();
        // Lower the field-count floor so the RATIO guard is what fires.
        let policy = AutoPromotePolicy {
            min_required_fields: 1,
            ..AutoPromotePolicy::default()
        };
        let err = policy.eligible(&cand, cand.last_seen_ms).unwrap_err();
        assert!(err.contains("%"), "reason was: {err}");
    }

    #[tokio::test]
    async fn promote_transitions_candidate_out_of_provisional() {
        // Root fix: a published candidate must leave the Provisional scan set,
        // else the auto-promote sweep re-promotes it forever.
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
        let promoter = Promoter::new(registry, evidence.clone());

        promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap();

        let after = evidence.get_candidate(&cand_id).await.unwrap().unwrap();
        assert_eq!(after.state, CandidateState::Staged);
    }

    #[tokio::test]
    async fn promote_rejects_non_provisional_candidate() {
        // Root fix: a candidate a human already Rejected can never be published
        // (closes the sweep-vs-reject race at the promote call).
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
        evidence
            .set_state(&cand_id, CandidateState::Rejected)
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
    async fn promote_after_threshold_publishes() {
        let evidence = Arc::new(FakeEvidence::default());
        let registry = Arc::new(FakeRegistry::new(7));
        let cand_id = some_cand_id();
        // Wall-clock age: first_seen must be JUST past the min-age threshold
        // relative to real `now` (promote() reads now_epoch_ms()). Epoch-0
        // (1970) would make this vacuous — always "old enough" regardless of
        // the threshold — so it's anchored to now instead.
        let now = now_epoch_ms();
        let first_seen = now - (DEFAULT_MIN_AGE_MS + 1);
        let last_seen = now;
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
        assert_eq!(call.record.canonicalizer, GENERALIZER);
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

    #[tokio::test]
    async fn promote_captures_value_profile_when_store_wired() {
        use deblob_core::ports::{InMemoryValueProfileStore, ValueProfileStore};

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

        let vp_store = Arc::new(InMemoryValueProfileStore::default());
        let promoter =
            Promoter::new(registry.clone(), evidence).with_value_profiles(vp_store.clone());

        let schema = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap();

        // The published schema references a durable value profile...
        let vp_ref = schema
            .value_profile_ref
            .clone()
            .expect("value_profile_ref set when store wired");
        let summary = schema.value_profile_summary.clone().expect("summary set");
        assert_eq!(summary.profile_id, vp_ref);
        assert_eq!(summary.leaf_count, 2); // {"a":1,"b":"x"} -> a, b

        // ...and it was persisted to the sidecar store before publish, with
        // the coarse per-leaf evidence but NO raw values.
        let snap = vp_store
            .get_value_profile(&vp_ref)
            .await
            .unwrap()
            .expect("snapshot persisted");
        assert_eq!(snap.candidate_id, cand_id);
        assert_eq!(snap.leaves.len(), 2);
        let a = snap.leaves.iter().find(|l| l.path == "a").unwrap();
        // Counts come from the candidate PROFILE (built from one parsed doc
        // here), not the candidate's `sample_count` metadata.
        assert_eq!(a.type_counts.number, 1);
        assert_eq!(snap.observation_count, 1);
        // value 1 falls in (0,10] -> small_positive bit.
        assert_eq!(
            a.numeric_bucket_mask,
            deblob_core::ports::value_bucket::SMALL_POSITIVE
        );
        let b = snap.leaves.iter().find(|l| l.path == "b").unwrap();
        assert_eq!(b.numeric_bucket_mask, 0); // string leaf, no numeric buckets
        assert!(b.type_counts.string > 0);
    }

    #[tokio::test]
    async fn promote_without_store_leaves_value_profile_none() {
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
        let promoter = Promoter::new(registry, evidence);
        let schema = promoter
            .promote(&cand_id, request(), "alice")
            .await
            .unwrap();
        assert!(schema.value_profile_ref.is_none());
        assert!(schema.value_profile_summary.is_none());
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
        assert!(policy
            .check(&too_few, too_few.last_seen_ms)
            .unwrap_err()
            .contains("sample"));

        let too_young = candidate_record(some_cand_id(), DEFAULT_MIN_SAMPLES, 0, 10);
        assert!(policy
            .check(&too_young, too_young.last_seen_ms)
            .unwrap_err()
            .contains("age"));

        let ok = candidate_record(
            some_cand_id(),
            DEFAULT_MIN_SAMPLES,
            0,
            DEFAULT_MIN_AGE_MS + 1,
        );
        assert!(policy.check(&ok, ok.last_seen_ms).is_ok());
    }
}
