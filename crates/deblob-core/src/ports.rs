//! Port traits for registry, evidence store, and schema matching. Spec §6.

use crate::{error::CoreError, id::*};
use async_trait::async_trait;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaRecord {
    pub schema_id: SchemaId,
    pub family_id: FamilyId,
    pub version: FamilyVersion,
    pub canonical: String,     // canonical shape JSON
    pub canonicalizer: String, // "deblob-canon-v1"
    pub provenance: serde_json::Value,
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
