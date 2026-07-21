//! Port traits for registry, evidence store, and schema matching. Spec §6.

use crate::{
    error::CoreError,
    id::*,
    semantic::{PrivacyClass, SemanticMetadata},
};
use async_trait::async_trait;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaRecord {
    pub schema_id: SchemaId,
    pub family_id: FamilyId,
    pub version: FamilyVersion,
    pub canonical: String,     // canonical shape JSON
    pub canonicalizer: String, // "deblob-canon-v1"
    pub provenance: serde_json::Value,
    /// Controlled semantic metadata for this schema's fields (P2-D). `None`
    /// means no semantic annotations were ever supplied — distinct from an
    /// annotated-but-empty map. `#[serde(default)]` so pre-P2-D serialized
    /// records (which lack this field entirely) still deserialize.
    #[serde(default)]
    pub semantic: Option<SemanticMetadata>,
    /// The `sem_` identity computed from `semantic` (Task 3 — not computed
    /// by this task). `#[serde(default)]` for the same back-compat reason.
    #[serde(default)]
    pub semantic_fingerprint: Option<SemanticId>,
    /// Data-sensitivity classification. Governance metadata (Hermes review
    /// §1/§3): deliberately SEPARATE from `semantic`/`semantic_fingerprint`
    /// — it never enters the `sem_` digest preimage, since privacy
    /// classification can change (jurisdiction/tenant/policy-version)
    /// without the field's meaning changing. `#[serde(default)]` for the
    /// same back-compat reason as the other two.
    #[serde(default)]
    pub privacy_class: Option<PrivacyClass>,
    /// Reference to this schema's durable value-profile snapshot (a
    /// SIDECAR blob in the [`ValueProfileStore`], NOT embedded here — see
    /// its docs). `None` for legacy schemas promoted before value-profile
    /// capture existed, and NEVER a synthetic empty profile. Excluded from
    /// the `sch_`/`sem_` identity digests (it is observability/governance
    /// evidence, not shape identity), so back-filling it never changes a
    /// schema's id. `#[serde(default)]` for back-compat with pre-existing
    /// serialized records.
    #[serde(default)]
    pub value_profile_ref: Option<ValueProfileId>,
    /// A tiny inline summary of the referenced value profile, cheap enough
    /// to carry on every list/get without lazy-loading the full sidecar
    /// (leaf count + observation count + capture time). `None` iff
    /// `value_profile_ref` is `None`.
    #[serde(default)]
    pub value_profile_summary: Option<ValueProfileSummary>,
}

/// The five coarse, non-reversible numeric magnitude buckets, packed into a
/// bitmask (mirrors `deblob_monoid::NumericBuckets`, kept here so
/// `deblob-core` needn't depend on the monoid crate). Bit layout is stable
/// and versioned by [`ValueProfileSnapshot::bucket_boundaries_version`].
pub mod value_bucket {
    pub const NEGATIVE: u8 = 1 << 0;
    pub const ZERO: u8 = 1 << 1;
    pub const SMALL_POSITIVE: u8 = 1 << 2; // (0, 10]
    pub const MEDIUM_POSITIVE: u8 = 1 << 3; // (10, 100]
    pub const LARGE_POSITIVE: u8 = 1 << 4; // > 100
    /// All bits that can legitimately be set — used to validate a stored mask.
    pub const ALL: u8 = NEGATIVE | ZERO | SMALL_POSITIVE | MEDIUM_POSITIVE | LARGE_POSITIVE;
}

/// Per-type observation counts for one leaf (mirror of
/// `deblob_monoid::TypeCounts`; duplicated so `deblob-core` stays monoid-free).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeafTypeCounts {
    pub null: u64,
    pub bool: u64,
    pub number: u64,
    pub string: u64,
    pub array: u64,
    pub object: u64,
}

/// One leaf's durable value evidence within a [`ValueProfileSnapshot`].
/// `path` is the exact canonical field reference (dotted object-key path,
/// mirroring the schema-canonical walk) the value profile is bound to — a
/// mis-attached leaf is worse than none, so this must be produced from the
/// same path semantics the consolidation join uses.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeafValueProfile {
    pub path: String,
    pub present_count: u64,
    pub explicit_null_count: u64,
    pub type_counts: LeafTypeCounts,
    /// The [`value_bucket`] bitmask observed for this leaf's numbers. OR-merged
    /// booleans, NOT a distribution — an overlapping mask can never *prove*
    /// compatibility, only a disjoint one can flag suspicion (see the joint
    /// design doc's "one-sided negative" guard semantics).
    pub numeric_bucket_mask: u8,
    pub int_only: bool,
    pub neg_zero_seen: bool,
}

/// An immutable, versioned value-profile snapshot captured atomically at
/// PROMOTION time from the candidate's monoid profile (spec §9 lineage /
/// joint design `dc-umbrella-signals-1907`). Stored as a compact sidecar
/// referenced by [`SchemaRecord::value_profile_ref`]; NEVER embedded on the
/// record and NEVER part of any identity digest. Bound to the exact inputs
/// that produced it (`canonicalizer`, `schema_canonical_digest`,
/// `candidate_id`, `candidate_profile_digest`) so a mis-attachment is
/// detectable, and preserves `observation_count`/`captured_at_ms` so the
/// guard can enforce minimum-support and reason about staleness.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ValueProfileSnapshot {
    pub profile_id: ValueProfileId,
    /// Snapshot schema version (`1` = value-profile-v1).
    pub profile_version: u32,
    /// Version of the bucket boundaries used (`1` = the (0,10]/(10,100]/>100
    /// scheme). Persisted so a later boundary change never silently
    /// reinterprets an old mask.
    pub bucket_boundaries_version: u32,
    pub canonicalizer: String,
    pub schema_canonical_digest: String,
    pub candidate_id: CandidateId,
    pub candidate_profile_digest: String,
    pub observation_count: u64,
    pub captured_at_ms: i64,
    pub leaves: Vec<LeafValueProfile>,
}

/// The tiny inline companion to a [`ValueProfileSnapshot`], carried on the
/// `SchemaRecord` itself so listings need not lazy-load the sidecar.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ValueProfileSummary {
    pub profile_id: ValueProfileId,
    pub leaf_count: u32,
    pub observation_count: u64,
    pub captured_at_ms: i64,
}

impl ValueProfileSnapshot {
    /// The tiny summary to stamp onto the `SchemaRecord`.
    pub fn summary(&self) -> ValueProfileSummary {
        ValueProfileSummary {
            profile_id: self.profile_id.clone(),
            leaf_count: self.leaves.len() as u32,
            observation_count: self.observation_count,
            captured_at_ms: self.captured_at_ms,
        }
    }
}

/// Durable sidecar store for [`ValueProfileSnapshot`]s (one compact blob per
/// profile — never one key per leaf). Snapshots are immutable: `put` writes
/// once, keyed by the content-addressed `profile_id`.
#[async_trait]
pub trait ValueProfileStore: Send + Sync {
    async fn put_value_profile(&self, snapshot: &ValueProfileSnapshot) -> Result<(), CoreError>;
    async fn get_value_profile(
        &self,
        id: &ValueProfileId,
    ) -> Result<Option<ValueProfileSnapshot>, CoreError>;
}

/// Process-local [`ValueProfileStore`] for tests / in-memory deployments.
#[derive(Debug, Default)]
pub struct InMemoryValueProfileStore {
    inner: std::sync::Mutex<std::collections::HashMap<String, ValueProfileSnapshot>>,
}

#[async_trait]
impl ValueProfileStore for InMemoryValueProfileStore {
    async fn put_value_profile(&self, snapshot: &ValueProfileSnapshot) -> Result<(), CoreError> {
        self.inner
            .lock()
            .expect("poisoned")
            .insert(snapshot.profile_id.as_str().to_string(), snapshot.clone());
        Ok(())
    }
    async fn get_value_profile(
        &self,
        id: &ValueProfileId,
    ) -> Result<Option<ValueProfileSnapshot>, CoreError> {
        Ok(self.inner.lock().expect("poisoned").get(id.as_str()).cloned())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CandidateRecord {
    pub candidate_id: CandidateId,
    pub profile: serde_json::Value, // serialized monoid profile
    pub sample_count: u64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub state: CandidateState,
    /// The Kafka topic (or other source identity) this candidate's most
    /// recent observation was ingested from (Hermes review gap 2: real
    /// per-record source, spec §4/§9). Threaded through from
    /// `deblob::coldlane::SampleMeta::source`, which is itself now the
    /// ACTUAL consumed record's topic rather than a static config value
    /// (see `deblob-kafka::relay`'s `DiscoveryMsg.source` fix). This FIELD
    /// itself is provenance/observability only, surfaced by `GET
    /// /api/v1/candidates` — never read back as a key by this crate.
    ///
    /// CANDIDATE clustering IS source-scoped (Hermes lineage gap 3, fixed):
    /// the same `meta.source` value is folded into the candidate id mint
    /// (`deblob_match::matcher::HotMatcher::classify` via
    /// [`CandidateId::from_source_and_digest`]) and into the
    /// generalized-fingerprint cluster-map key
    /// (`deblob::coldlane::scoped_gen_fp`) — via those two SEPARATE code
    /// paths, not via this field — so two different sources sharing the
    /// exact same shape never converge onto one candidate. KNOWN-schema
    /// structural retrieval (`Registry::resolve_structural`) stays
    /// GLOBAL/source-blind for now; widening it the same way is a
    /// documented follow-up, not done here.
    /// `#[serde(default)]` so every pre-existing `CandidateRecord` JSON in
    /// storage (which lacks this field entirely) still deserializes,
    /// yielding `None`.
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateState {
    Provisional,
    Staged,
    Rejected,
}

/// One schema found in a structural-index bucket, as returned by
/// [`Registry::list_families_in_buckets`] (deblob-p2ab Task 3: deterministic
/// structural-distance retrieval). Carries the same generalized-canonical
/// JSON [`SchemaRecord::canonical`] holds, so the caller can score
/// structural distance against it without a second per-schema round trip.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FamilyRef {
    pub family_id: FamilyId,
    pub schema_id: SchemaId,
    pub version: FamilyVersion,
    pub canonical: String,
}

/// The family metadata that lives at `deblob:family:<fam_id>` (spec §6),
/// as returned by [`Registry::get_family`]. The P1 family record is
/// minimal: `crate::lua::PUBLISH_SCRIPT`'s `HINCRBY` only ever writes a
/// `next_version` counter (plus a `v:<n>` entry per version and an echoed
/// `family_id` field) onto that hash — there is no `name`/`state`/`compat`
/// field stored anywhere for a family, so this struct exposes exactly what
/// the write path actually populates, not more.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FamilyRecord {
    pub family_id: FamilyId,
    /// The highest version ever allocated to this family — the same
    /// `next_version` field `HINCRBY` maintains. Versions are allocated
    /// 1.. and are contiguous (never sparse), so this also doubles as the
    /// count of versions that exist.
    pub current_version: FamilyVersion,
}

/// Outcome of [`Registry::set_schema_name`] — the display-name write is
/// governed (a `human` name is never clobbered by an automatic write), so the
/// call reports which of the three terminal states it reached rather than a
/// bare `()`. `jr-schema-naming-211140`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NameWriteOutcome {
    /// The name (and its `name_source`) were written to provenance.
    Applied,
    /// The schema already carries a `human` name and the incoming write was
    /// `slm`/`heuristic` — refused atomically, the human name kept. This is a
    /// benign no-op (the automatic namer must NOT treat it as a failure).
    SkippedHumanProtected,
    /// No schema exists for the given id.
    NotFound,
}

#[async_trait]
pub trait Registry: Send + Sync {
    async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError>;

    /// Patch the display NAME on a schema's provenance in place: sets
    /// `provenance.label` = `label`, `provenance.name_source` = `source`
    /// (`human`|`slm`|`heuristic`), `provenance.name_meta` = `meta`, and
    /// `provenance.name_updated_ms` = now. Never touches the schema's identity
    /// digest (`schema_id` is content-addressed over SHAPE, not provenance) or
    /// its version — this is display metadata, so the fingerprint domain is
    /// untouched. SLM-proposed, human-editable schema names
    /// (`jr-schema-naming-211140`).
    ///
    /// GOVERNANCE (human override always wins): when `source != "human"` and
    /// the record already has `name_source == "human"`, the write is refused
    /// and [`NameWriteOutcome::SkippedHumanProtected`] returned. Implementations
    /// MUST enforce this ATOMICALLY (read-guard-write under a single
    /// optimistic transaction), so a human edit landing between an automatic
    /// namer's read and write can never be clobbered.
    ///
    /// Default impl: unsupported — only a durable registry persists names. The
    /// in-memory test fakes that exercise the naming endpoint override this.
    async fn set_schema_name(
        &self,
        id: &SchemaId,
        label: &str,
        source: &str,
        meta: Option<serde_json::Value>,
    ) -> Result<NameWriteOutcome, CoreError> {
        let _ = (id, label, source, meta);
        Err(CoreError::RegistryUnavailable(
            "set_schema_name is not supported by this registry".to_string(),
        ))
    }

    async fn resolve_structural(
        &self,
        bucket_key: &str,
        fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError>;
    /// Atomic publication: schema + family version + index + alias + audit (§6).
    ///
    /// Returns the authoritative `FamilyVersion` allocated by the registry.
    /// The `version` field on the passed-in `record` is only ever a
    /// caller-side guess; implementations must never trust it for storage
    /// or echo it back — the registry (e.g. Redis `HINCRBY` on the family
    /// key) is the sole authority for version numbers.
    ///
    /// `variant_members` (Task 14 fix): every CONCRETE shape observed while
    /// this candidate was accumulating evidence, as `(bucket_key, fp_b32)`
    /// pairs — `fp_b32` is the base32 body of that concrete observation's
    /// OWN raw `deblob-fingerprint::fingerprint` digest, NOT `record`'s
    /// generalized `schema_id` digest. `record.schema_id` is derived from
    /// the candidate's GENERALIZED profile (a different fingerprint domain
    /// than any single concrete observation, spec §5) — a hot-path lookup
    /// for a concrete message can therefore never match `record.schema_id`'s
    /// own digest. Implementations must additionally index each
    /// `variant_members` pair so `resolve_structural(bucket_key,
    /// SchemaId::from_digest(fp))` finds `record.schema_id` for every
    /// concrete shape that was actually observed, not just the one the
    /// candidate happened to be seeded from. An empty slice is valid (a
    /// candidate promoted without any recorded variants indexes nothing
    /// extra, but must not fail).
    async fn publish(
        &self,
        record: SchemaRecord,
        alias_from: &CandidateId,
        bucket_key: &str,
        variant_members: &[(String, String)],
        actor: &str,
        reason: &str,
    ) -> Result<FamilyVersion, CoreError>;
    async fn get_alias(&self, id: &CandidateId) -> Result<Option<SchemaId>, CoreError>;
    async fn list_schemas(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError>;

    /// Every schema whose structural-index membership lives in any of
    /// `bucket_keys` (deblob-p2ab Task 3 retrieval), de-duplicated by
    /// `schema_id` across buckets. Each bucket lookup is a bounded per-bucket
    /// scan — buckets are small by construction (spec §6) — never a scan
    /// over `deblob:schema:*`. An empty `bucket_keys` slice is valid and
    /// returns `Ok(vec![])`; this is the gold-ABSENT case (no nearby
    /// families for a candidate), never an error.
    async fn list_families_in_buckets(
        &self,
        bucket_keys: &[String],
    ) -> Result<Vec<FamilyRef>, CoreError>;

    /// Every schema whose structural-index bucket falls under ANY `(band,
    /// depth)` pair in `bands` × `depths` — i.e. `deblob:index:{band}:{depth}:*`
    /// — regardless of the bucket's `reqhash8` suffix (deblob-p2ab Task 3
    /// recall fix). `reqhash8` is a hash of a schema's own top-level key
    /// NAMES, so a family whose top-level fields were merely
    /// renamed/case-changed (e.g. `widgetCount` -> `widget_count`) lands in
    /// a DIFFERENT bucket at the SAME field-count band and depth — an
    /// exact-key lookup via [`Registry::list_families_in_buckets`] can
    /// never find it, but this widened, name-blind discovery can.
    /// De-duplicated by `schema_id` across every discovered bucket.
    ///
    /// Still bounded, never a global scan: `bands`/`depths` are the
    /// caller's small local neighborhood (a candidate's own field-count
    /// band plus its immediate neighbors, and nearby depths), so this is
    /// one bounded `SCAN MATCH` per `(band, depth)` pair to discover that
    /// prefix's bucket keys, followed by one bounded per-bucket scan for
    /// each discovered bucket — the cold retrieval path, run once per
    /// candidate cluster, never per record. An empty `bands` or `depths`
    /// slice is valid and returns `Ok(vec![])`, never an error.
    async fn list_families_by_band_depth(
        &self,
        bands: &[u32],
        depths: &[u32],
    ) -> Result<Vec<FamilyRef>, CoreError>;

    /// The schema id published as `family_id`'s `version` (spec §6: family
    /// versions are allocated 1.. via `HINCRBY` at `publish` time, and are
    /// otherwise immutable) — `None` if that exact version was never
    /// published, including a `family_id` that doesn't exist at all. Never
    /// an error for a not-found version (mirrors [`Registry::
    /// list_families_in_buckets`]'s "empty/absent is a valid answer, not a
    /// failure" posture).
    ///
    /// P2-D Task 8 follow-up: lets a caller find a family's ADJACENT
    /// version so `crate::semantic_drift::check_family_version_drift` (in
    /// the `deblob` bin) can compare its active `sem_` against a
    /// newly-annotated version's — the ONLY reason this method exists; nothing
    /// else in this trait needed per-version lookup before.
    async fn family_version_schema(
        &self,
        family_id: &FamilyId,
        version: FamilyVersion,
    ) -> Result<Option<SchemaId>, CoreError>;

    /// The family record stored at `deblob:family:<fam_id>` (spec §6) —
    /// `None` if nothing has ever been published to `family_id` (including
    /// a `family_id` that was never minted at all). Never an error for a
    /// not-found family (mirrors [`Registry::list_families_in_buckets`]'s
    /// "empty/absent is a valid answer, not a failure" posture).
    ///
    /// P2-D polish Task 2: backs `GET /api/v1/families/{fam_id}`.
    async fn get_family(&self, family_id: &FamilyId) -> Result<Option<FamilyRecord>, CoreError>;

    /// Every version ever published to `family_id`, ascending. Family
    /// versions are allocated 1.. via `HINCRBY` at `publish` time and are
    /// contiguous — never sparse (spec §6) — so a correct implementation is
    /// always `1..=get_family(family_id)?.current_version`, derived rather
    /// than independently stored/enumerated. `Ok(vec![])` for an unknown
    /// family, never an error; callers that need to distinguish "unknown
    /// family" from "family with zero versions" (which the contiguity
    /// invariant above makes impossible for a family that exists) should
    /// call [`Registry::get_family`] first.
    ///
    /// P2-D polish Task 2: backs `GET /api/v1/families/{fam_id}/versions`.
    async fn list_family_versions(
        &self,
        family_id: &FamilyId,
    ) -> Result<Vec<FamilyVersion>, CoreError>;
}

#[async_trait]
pub trait EvidenceStore: Send + Sync {
    async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError>;
    async fn get_candidate(&self, id: &CandidateId) -> Result<Option<CandidateRecord>, CoreError>;
    async fn list_candidates(
        &self,
        state: CandidateState,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError>;
    async fn append_evidence(
        &self,
        id: &CandidateId,
        stats: serde_json::Value,
    ) -> Result<(), CoreError>;
    async fn set_state(&self, id: &CandidateId, state: CandidateState) -> Result<(), CoreError>;
    /// Cold-lane clustering (Task 14, spec §4): looks up which candidate a
    /// *generalized* fingerprint (hex-encoded `Profile::generalized_fingerprint`)
    /// currently clusters onto, so optional-field variants of one emerging
    /// schema converge onto ONE candidate even when the hot path's raw
    /// shape digest mints a different `cand_` id per variant.
    ///
    /// `gen_fp` is an OPAQUE key as far as this trait is concerned — this
    /// trait's signature is deliberately unchanged by Hermes lineage gap 3
    /// (source-scoped candidate clustering): the sole production caller,
    /// `deblob::coldlane::ColdLane::ingest`, is responsible for
    /// source-scoping the key it passes here (via `deblob::coldlane::
    /// scoped_gen_fp`, prefixing the source before the hex fingerprint) so
    /// two different sources sharing the exact same generalized shape never
    /// share a cluster entry. An implementation must simply treat `gen_fp`
    /// as an opaque string key — it must never itself try to parse a
    /// source back out of it.
    async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError>;
    /// Records (or refreshes) that `gen_fp` clusters onto `cand_id`. See
    /// [`EvidenceStore::get_cluster`]'s docs on `gen_fp` being an opaque,
    /// caller-scoped key.
    async fn set_cluster(&self, gen_fp: &str, cand_id: &CandidateId) -> Result<(), CoreError>;

    /// Records (idempotently, de-duplicated) that a CONCRETE shape observed
    /// for `cand_id` belongs to structural bucket `bucket_key` and has raw
    /// fingerprint base32-body `fp_b32` (Task 14 fix). `ColdLane::ingest`
    /// calls this once per distinct observed shape so `Promoter::promote`
    /// can later replay every one of them into the structural index
    /// (`Registry::publish`'s `variant_members`), bridging the hot path's
    /// raw-shape lookups to the promoted schema's generalized identity.
    async fn add_variant(
        &self,
        cand_id: &CandidateId,
        bucket_key: &str,
        fp_b32: &str,
    ) -> Result<(), CoreError>;

    /// Every `(bucket_key, fp_b32)` pair recorded for `cand_id` via
    /// [`EvidenceStore::add_variant`] so far, de-duplicated. Empty if none
    /// were ever recorded (e.g. a candidate promoted without ingest
    /// history) — implementations must return `Ok(vec![])`, never an error,
    /// for that case.
    async fn get_variants(&self, cand_id: &CandidateId)
        -> Result<Vec<(String, String)>, CoreError>;

    /// Backfill/repair the per-state candidate-listing index (operator
    /// path, e.g. `deblob-redis::RedisEvidence::rebuild_candidate_index`):
    /// reconstructs whatever index a store maintains for
    /// [`EvidenceStore::list_candidates`] from its own authoritative
    /// records, and returns the number of candidates reindexed.
    ///
    /// Defaults to a no-op returning `Ok(0)` so every implementation that
    /// doesn't maintain such an index (in-memory fakes, test doubles) keeps
    /// compiling unchanged; only a store with a real backfill path (Redis)
    /// needs to override this.
    async fn rebuild_candidate_index(&self) -> Result<u64, CoreError> {
        Ok(0)
    }
}

/// One redacted troubleshooting sample (joint design `dc-samples-dlp-1907`).
/// Built ONLY from DLP-redacted output — `document` never contains a raw
/// payload value. Persisted opaquely by the [`SampleStore`]; `redaction_counts`
/// is carried as JSON so `deblob-core` needn't depend on `deblob-dlp`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SampleRecord {
    /// Idempotency key = hash(source_id, topic, partition, offset). An
    /// at-least-once consumer replay of the same coordinate must NOT create a
    /// duplicate; the store dedups on this.
    pub sample_id: String,
    pub source_id: String,
    pub candidate_id: CandidateId,
    pub captured_at_ms: i64,
    /// The DLP detector-set version that produced `document` (readers re-run
    /// the current detector and can tell what generated the stored form).
    pub dlp_version: String,
    /// Per-finding counts (`{sensitive_key, secret_pattern, …}`), opaque JSON.
    pub redaction_counts: serde_json::Value,
    /// Whether structure-aware size limiting dropped fields/array tails.
    pub truncated: bool,
    /// The REDACTED document — visible markers in place of any secret/PII.
    pub document: serde_json::Value,
}

/// Bounded, idempotent, age+count-pruned store of redacted troubleshooting
/// samples per candidate (joint design `dc-samples-dlp-1907`). Backed by a
/// DEDICATED, VOLATILE Redis instance — never the permanent vault's (whose
/// RDB/AOF/backups would outlive the retention TTL). Off the hot path.
#[async_trait]
pub trait SampleStore: Send + Sync {
    /// Idempotently store one redacted sample under its RESOLVED candidate id,
    /// then prune by age and count. Returns `true` if newly stored, `false` if
    /// it was a replay duplicate (dedup on `sample.sample_id`).
    async fn put_sample(&self, sample: &SampleRecord) -> Result<bool, CoreError>;
    /// Most-recent-first samples for a candidate, up to `limit`.
    async fn list_samples(
        &self,
        candidate_id: &CandidateId,
        limit: usize,
    ) -> Result<Vec<SampleRecord>, CoreError>;
}

/// Process-local [`SampleStore`] for tests: idempotent + count-bounded (age
/// pruning is time-based and exercised against the Redis impl).
#[derive(Debug)]
pub struct InMemorySampleStore {
    max_per_candidate: usize,
    inner: std::sync::Mutex<std::collections::HashMap<String, Vec<SampleRecord>>>,
}

impl InMemorySampleStore {
    pub fn new(max_per_candidate: usize) -> Self {
        Self { max_per_candidate, inner: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }
}

#[async_trait]
impl SampleStore for InMemorySampleStore {
    async fn put_sample(&self, sample: &SampleRecord) -> Result<bool, CoreError> {
        let mut map = self.inner.lock().expect("poisoned");
        let v = map.entry(sample.candidate_id.as_str().to_string()).or_default();
        if v.iter().any(|s| s.sample_id == sample.sample_id) {
            return Ok(false); // replay dup
        }
        v.push(sample.clone());
        v.sort_by_key(|s| s.captured_at_ms);
        let excess = v.len().saturating_sub(self.max_per_candidate);
        if excess > 0 {
            v.drain(0..excess); // drop oldest
        }
        Ok(true)
    }
    async fn list_samples(
        &self,
        candidate_id: &CandidateId,
        limit: usize,
    ) -> Result<Vec<SampleRecord>, CoreError> {
        let map = self.inner.lock().expect("poisoned");
        Ok(map
            .get(candidate_id.as_str())
            .map(|v| v.iter().rev().take(limit).cloned().collect())
            .unwrap_or_default())
    }
}

#[async_trait]
pub trait SchemaMatcher: Send + Sync {
    /// Pure lookup: never creates anything. Returns Known / Provisional(candidate fp) / Unresolved.
    async fn match_shape(
        &self,
        bucket_key: &str,
        fingerprint_digest: &[u8; 32],
    ) -> crate::id::SchemaRef;
}

/// One registered data SOURCE (spec §9 lineage). Identity (`source_id`) is a
/// pure function of `name` ([`SourceId::from_source`]), so registration is
/// idempotent and every observer derives the same id. `first_seen_ms`/
/// `last_seen_ms` bound the window this source has been observed over;
/// provenance/observability only, never read back as a key by this crate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceRecord {
    pub source_id: SourceId,
    pub name: String,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
}

/// A durable registry of the data sources the service has observed, keyed by
/// the content-addressed [`SourceId`] (Hermes lineage gap: give every source
/// a stable id the UI/lineage can reference instead of a raw topic string).
///
/// Deliberately NOT on the hot path: registration happens off-path (cold-lane
/// ingest and the `POST /sources/reconcile` backfill that scans candidates'
/// `source` fields), never from the deterministic relay — the same posture as
/// the stream tap's `family_id`/matched-vs-new enrichment.
#[async_trait]
pub trait SourceRegistry: Send + Sync {
    /// Idempotent upsert of a source by `name`: mints/returns its stable
    /// [`SourceId`], sets `first_seen_ms` on first registration and never
    /// moves it backward, and advances `last_seen_ms` to `max(existing,
    /// observed_at_ms)`. Re-run safe: registering the same name twice yields
    /// the same record (only `last_seen_ms` may advance).
    async fn register_source(
        &self,
        name: &str,
        observed_at_ms: i64,
    ) -> Result<SourceRecord, CoreError>;
    async fn get_source(&self, id: &SourceId) -> Result<Option<SourceRecord>, CoreError>;
    /// Every registered source, unordered (callers sort for display).
    async fn list_sources(&self) -> Result<Vec<SourceRecord>, CoreError>;
}

/// A process-local [`SourceRegistry`] for tests and in-memory deployments —
/// same idempotent-upsert / monotonic-timestamp semantics as the Redis
/// backend, without a store. Keyed by the content-addressed [`SourceId`].
#[derive(Debug, Default)]
pub struct InMemorySourceRegistry {
    inner: std::sync::Mutex<std::collections::HashMap<String, SourceRecord>>,
}

#[async_trait]
impl SourceRegistry for InMemorySourceRegistry {
    async fn register_source(
        &self,
        name: &str,
        observed_at_ms: i64,
    ) -> Result<SourceRecord, CoreError> {
        let id = SourceId::from_source(name);
        let mut map = self.inner.lock().expect("poisoned");
        let rec = map
            .entry(id.as_str().to_string())
            .and_modify(|r| {
                r.first_seen_ms = r.first_seen_ms.min(observed_at_ms);
                r.last_seen_ms = r.last_seen_ms.max(observed_at_ms);
            })
            .or_insert_with(|| SourceRecord {
                source_id: id.clone(),
                name: name.to_string(),
                first_seen_ms: observed_at_ms,
                last_seen_ms: observed_at_ms,
            });
        Ok(rec.clone())
    }

    async fn get_source(&self, id: &SourceId) -> Result<Option<SourceRecord>, CoreError> {
        Ok(self.inner.lock().expect("poisoned").get(id.as_str()).cloned())
    }

    async fn list_sources(&self) -> Result<Vec<SourceRecord>, CoreError> {
        Ok(self.inner.lock().expect("poisoned").values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal executor: `InMemorySourceRegistry`'s futures have no real
    /// await points (their bodies are synchronous), so a single poll under a
    /// no-op waker always yields `Ready` — no runtime dependency needed.
    fn block_on<F: std::future::Future>(mut fut: F) -> F::Output {
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        fn noop(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        // Safety: `fut` is owned and never moved after being pinned here.
        let mut fut = unsafe { std::pin::Pin::new_unchecked(&mut fut) };
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[test]
    fn in_memory_source_registry_is_idempotent_and_monotonic() {
        let reg = InMemorySourceRegistry::default();
        let name = "events.grid.carbonintensity";

        let first = block_on(reg.register_source(name, 100)).unwrap();
        assert!(first.source_id.as_str().starts_with("src_"));
        assert_eq!((first.first_seen_ms, first.last_seen_ms), (100, 100));

        // Later sighting advances last_seen, never first_seen; same id.
        let later = block_on(reg.register_source(name, 250)).unwrap();
        assert_eq!(later.source_id, first.source_id);
        assert_eq!((later.first_seen_ms, later.last_seen_ms), (100, 250));

        // An EARLIER sighting lowers first_seen, never last_seen.
        let earlier = block_on(reg.register_source(name, 40)).unwrap();
        assert_eq!((earlier.first_seen_ms, earlier.last_seen_ms), (40, 250));

        // A distinct source is a distinct record; both are listed.
        block_on(reg.register_source("events.compute.azure", 500)).unwrap();
        let all = block_on(reg.list_sources()).unwrap();
        assert_eq!(all.len(), 2);
        let got = block_on(reg.get_source(&first.source_id)).unwrap().unwrap();
        assert_eq!(got.name, name);
    }

    /// Pre-P2-D serialized `SchemaRecord` JSON (lacking `semantic`,
    /// `semantic_fingerprint`, AND `privacy_class` entirely) must still
    /// deserialize, yielding `None` for all three — every one of them is
    /// `#[serde(default)]` precisely so old records in storage keep loading
    /// after this ships.
    #[test]
    fn schema_record_deserializes_pre_p2d_json_with_none_semantics() {
        let schema_id = SchemaId::from_digest(&[7u8; 32]);
        let family_id = FamilyId::new_v7();
        let json = serde_json::json!({
            "schema_id": schema_id.as_str(),
            "family_id": family_id.as_str(),
            "version": 1,
            "canonical": "{}",
            "canonicalizer": "deblob-canon-v1",
            "provenance": {},
        });

        let record: SchemaRecord = serde_json::from_value(json).unwrap();
        assert_eq!(record.semantic, None);
        assert_eq!(record.semantic_fingerprint, None);
        assert_eq!(record.privacy_class, None);
    }

    /// Pre-existing serialized `CandidateRecord` JSON (lacking `source`
    /// entirely) must still deserialize, yielding `None` for it —
    /// `#[serde(default)]` exists precisely so every candidate already in
    /// storage before this field shipped keeps loading (Hermes review gap
    /// 2 follow-up, mirrors `schema_record_deserializes_pre_p2d_json_with_
    /// none_semantics` above).
    #[test]
    fn candidate_record_deserializes_pre_source_json() {
        let candidate_id = CandidateId::from_digest(&[9u8; 32]);
        let json = serde_json::json!({
            "candidate_id": candidate_id.as_str(),
            "profile": {},
            "sample_count": 3,
            "first_seen_ms": 1_000,
            "last_seen_ms": 2_000,
            "state": "provisional",
        });

        let record: CandidateRecord = serde_json::from_value(json).unwrap();
        assert_eq!(record.source, None);
    }
}
