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
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CandidateRecord {
    pub candidate_id: CandidateId,
    pub profile: serde_json::Value, // serialized monoid profile
    pub sample_count: u64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub state: CandidateState,
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

#[async_trait]
pub trait Registry: Send + Sync {
    async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError>;
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
    async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError>;
    /// Records (or refreshes) that `gen_fp` clusters onto `cand_id`.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
