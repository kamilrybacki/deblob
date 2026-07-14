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
    async fn publish(
        &self,
        record: SchemaRecord,
        alias_from: &CandidateId,
        bucket_key: &str,
        actor: &str,
        reason: &str,
    ) -> Result<FamilyVersion, CoreError>;
    async fn get_alias(&self, id: &CandidateId) -> Result<Option<SchemaId>, CoreError>;
    async fn list_schemas(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError>;
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
