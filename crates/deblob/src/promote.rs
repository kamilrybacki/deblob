//! The `Promoter` seam (spec §8, §14): the management API's promotion
//! endpoint needs to turn a candidate into a published schema, but the
//! concrete clustering/guard logic (sample-count threshold, minimum age,
//! generalized-profile → `SchemaRecord` construction) is Task 14's
//! `coldlane`/`policy` work. This module defines the trait and request DTOs
//! so `api::router` can depend on an abstract `Arc<dyn Promoter>` today;
//! Task 14 implements this trait and reuses `PromoteRequest`/`FamilyChoice`
//! verbatim rather than inventing a second copy.

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId};
use deblob_core::ports::SchemaRecord;
use serde::{Deserialize, Serialize};

/// Which family a promoted candidate joins. Serializes/deserializes via
/// serde's default externally-tagged representation: the unit variant is a
/// bare string (`"new"`), the tuple variant is a single-key object
/// (`{"existing": "fam_..."}`) — matching the request body in spec §8's
/// promote example.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FamilyChoice {
    New,
    Existing(FamilyId),
}

/// Request body for `POST /api/v1/candidates/{cand_id}/promote`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromoteRequest {
    pub family: FamilyChoice,
    pub name: Option<String>,
    pub reason: String,
}

/// Turns a provisional/staged candidate into a published `SchemaRecord`.
/// Promotion is an administrative security boundary (spec §8): the `actor`
/// string is recorded in the immutable audit trail alongside `req.reason`
/// and the candidate's previous state — implementations must not silently
/// drop it.
#[async_trait]
pub trait Promoter: Send + Sync {
    async fn promote(
        &self,
        cand: &CandidateId,
        req: PromoteRequest,
        actor: &str,
    ) -> Result<SchemaRecord, CoreError>;
}
