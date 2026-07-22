//! The `SemanticStore` seam (P2-D Task 6, `deblob-p2d-hermes-review.md`
//! §4): the management API's semantic-governance endpoints need
//! `RedisRegistry`'s `append_revision`/`active_semantic`/`revisions`/
//! `schemas_by_semantic` (Task 5), but those are inherent methods on the
//! concrete `deblob_redis::RedisRegistry`, deliberately NOT a
//! `deblob_core::ports` trait — Task 5's own docs: "Task 6 (the HTTP
//! surface, out of scope here) is free to wrap these in a trait later if it
//! needs to mock them." This module is that wrapping: it defines the trait
//! so `api::router` can depend on an abstract `Arc<dyn SemanticStore>`
//! (mirroring `crate::promote::Promoter`'s own seam) and implements it for
//! `RedisRegistry` by delegating straight to its existing inherent methods
//! — no new storage logic, per this task's scope.

use async_trait::async_trait;
use deblob_core::id::{SchemaId, SemanticId};
use deblob_core::revision::{
    AppendOutcome, Etag, ReasonCode, Revision, SemError, SignatureCandidates,
};
use deblob_core::semantic::SemanticMetadata;
use deblob_redis::RedisRegistry;

/// Abstract seam over the append-only semantic-revision store (Task 5).
/// Every method mirrors `RedisRegistry`'s own inherent semantic-storage
/// methods exactly — this trait adds no new behavior, only mockability for
/// the management API's tests (`crates/deblob/tests/api_it.rs`'s
/// `FakeSemanticStore`, mirroring `FakeRegistry`/`FakeEvidence`/
/// `FakePromoter`).
#[async_trait]
pub trait SemanticStore: Send + Sync {
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<AppendOutcome, SemError>;

    async fn active_semantic(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(SemanticMetadata, SemanticId, Etag)>, SemError>;

    /// The schema's current FULL active [`Revision`] (with `revision_id`)
    /// plus its [`Etag`] — Task 10 needs `revision_id`
    /// (`semantic_revision_id` in the neighbors API response), which
    /// `active_semantic` discards.
    async fn active_revision(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(Revision, Etag)>, SemError>;

    async fn revisions(&self, sch_id: &SchemaId) -> Result<Vec<Revision>, SemError>;

    async fn schemas_by_semantic(&self, sem_id: &SemanticId) -> Result<Vec<SchemaId>, SemError>;

    /// Task 10: the bounded feature-postings union for `feature_keys_hex`
    /// (the query signature's own posting keys) — the raw candidate lookup
    /// `crate::semantic_neighbors::neighbors` scores/ranks/truncates.
    async fn signature_candidates(
        &self,
        feature_keys_hex: &[String],
    ) -> Result<SignatureCandidates, SemError>;

    /// Task 10 IDF (`jr-deblob-similarity-idf-221040`): the active-annotated
    /// population `N` and the document frequency of each `feature_keys_hex`
    /// posting (aligned to input order), read as one atomic snapshot. Powers the
    /// `idf_multiplier` the neighbor handler injects into the weighted score /
    /// strength so corpus-common features stop earning false-close neighbors.
    async fn idf_stats(&self, feature_keys_hex: &[String]) -> Result<(u64, Vec<u64>), SemError>;
}

/// Delegates straight to `RedisRegistry`'s own inherent methods (Rust's
/// inherent-method-first resolution means `self.append_revision(..)` below
/// calls the INHERENT method, not this trait method recursively — the
/// standard delegation idiom).
#[async_trait]
impl SemanticStore for RedisRegistry {
    #[allow(clippy::too_many_arguments)]
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
        self.append_revision(
            sch_id,
            metadata,
            canonical_bytes,
            sem_id,
            actor,
            reason_code,
            reason,
            recorded_at,
            effective_from,
            expected_etag,
        )
        .await
    }

    async fn active_semantic(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(SemanticMetadata, SemanticId, Etag)>, SemError> {
        self.active_semantic(sch_id).await
    }

    async fn active_revision(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(Revision, Etag)>, SemError> {
        self.active_revision(sch_id).await
    }

    async fn revisions(&self, sch_id: &SchemaId) -> Result<Vec<Revision>, SemError> {
        self.revisions(sch_id).await
    }

    async fn schemas_by_semantic(&self, sem_id: &SemanticId) -> Result<Vec<SchemaId>, SemError> {
        self.schemas_by_semantic(sem_id).await
    }

    async fn signature_candidates(
        &self,
        feature_keys_hex: &[String],
    ) -> Result<SignatureCandidates, SemError> {
        self.signature_candidates(feature_keys_hex).await
    }

    async fn idf_stats(&self, feature_keys_hex: &[String]) -> Result<(u64, Vec<u64>), SemError> {
        self.idf_stats(feature_keys_hex).await
    }
}
