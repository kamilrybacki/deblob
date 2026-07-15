//! Append-only semantic-assertion revisions + mutable active pointer
//! (P2-D Task 5, `deblob-p2d-hermes-review.md` §4).
//!
//! `sch_` (spec §5's structural identity) is immutable by construction — a
//! new canonical shape mints a new `sch_`. Meaning is different: it must be
//! CORRECTABLE (an operator fixing a wrong `unit`/`enum_semantics` entry
//! must not have to mint a new schema identity for a physically-unchanged
//! record), but never *silently rewritten* (an auditor must always be able
//! to see every semantic assertion a schema ever carried, in order, with
//! who/why/when). Append-only revisions plus a separately-advancing mutable
//! pointer give both properties at once: every [`Revision`] this module
//! defines is written once and never touched again; only the active
//! pointer (owned by `deblob-redis`'s storage layer, not this module) moves.
//!
//! This module holds the shapes + controlled vocabulary + error taxonomy
//! only — no I/O. Storage (the actual Redis keys, the atomic Lua
//! transition) is `deblob-redis::semantic` (Task 5); the HTTP surface that
//! will drive `expected_etag`/`reason` from `If-Match`/request-body fields
//! is Task 6.

use crate::id::{RevisionId, SchemaId, SemanticId};
use crate::semantic::SemanticMetadata;

/// Controlled reason a semantic assertion changed (Hermes review §4). The
/// free-form `reason` string on [`Revision`] is still required alongside
/// this — the code is what a caller can filter/aggregate/alert on, the
/// prose is for a human reviewing the audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonCode {
    /// Fixing a wrong prior annotation (the common case).
    Correction,
    /// The controlled vocabulary itself changed (Task 2's tables gained a
    /// more precise code for something previously coarser).
    OntologyUpgrade,
    /// A governance/compliance review changed the assessed meaning.
    PolicyReview,
    /// The upstream source's own contract changed (e.g. a producer started
    /// emitting epoch-milliseconds instead of epoch-seconds for a field
    /// whose physical shape — `sch_` — didn't change).
    SourceContractChange,
    /// A human operator overrode the assertion outside any of the above,
    /// e.g. an emergency fix — always auditable via `actor`/`reason`.
    OperatorOverride,
}

/// Monotonically increasing version stamped on the mutable active-revision
/// pointer (`deblob:sem-active:<sch_id>` in `deblob-redis`). Conceptually
/// `0` before a schema has ever been annotated; the first successful
/// [`crate::error::CoreError`]-free `append_revision` call sets it to `1`,
/// and every subsequent successful call increments it by exactly one. This
/// is the compare-and-swap token `append_revision`'s `expected_etag`
/// argument checks against — analogous to an HTTP `If-Match`/`ETag` pair —
/// so two concurrent correctors racing against the same schema can never
/// both believe their edit landed cleanly.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Etag(pub u64);

/// Lifecycle marker stamped on a revision AT CREATION TIME ONLY. P2-D mints
/// every revision as `Active` and never mutates it afterward — mutating a
/// stored revision's `status` post-hoc would itself violate the "revisions
/// are immutable" invariant this module exists to guarantee. Whether a
/// given revision is the CURRENTLY pointed-to one is derived by comparing
/// its `revision_id` against the active pointer's `revision_id`, never by
/// reading this field. It exists now (rather than being added later) so a
/// P4 feature that mints a revision in some other lifecycle state (e.g. a
/// disputed/retracted correction) doesn't need a storage-shape migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RevisionStatus {
    Active,
}

/// One immutable semantic-assertion revision. Never deleted, never
/// overwritten, once written — see module docs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Revision {
    pub revision_id: RevisionId,
    pub sch_id: SchemaId,
    pub sem_id: SemanticId,
    /// The typed metadata this revision asserts. Stored alongside
    /// `canonical_semantic_bytes` (not reconstructed from it): the
    /// byte-level protocol (`deblob-semantic::canon`) is a one-way,
    /// injective HASH PREIMAGE, deliberately not designed to be decoded
    /// back into a typed value — round-tripping goes through this field's
    /// ordinary `serde_json` representation instead, the same way
    /// `SchemaRecord::semantic` already round-trips (`deblob-core::ports`).
    pub metadata: SemanticMetadata,
    /// The exact canonical bytes `deblob_semantic::canonical_semantic_bytes`
    /// produced for `metadata` — stored verbatim so a replay's idempotency
    /// check (`append_revision`, same-bytes ⇒ `AlreadyActive`) is a byte
    /// compare against what was ACTUALLY persisted, never a recomputation
    /// that could silently drift from what this revision claims to assert.
    pub canonical_semantic_bytes: Vec<u8>,
    /// `None` only for a schema's very first revision.
    pub previous_revision_id: Option<RevisionId>,
    pub actor: String,
    pub reason_code: ReasonCode,
    pub reason: String,
    /// Caller-supplied epoch-ms timestamp — recording when this revision
    /// was appended. Deliberately NOT computed via `SystemTime::now()`
    /// inside `deblob-redis`: unlike `RedisRegistry::publish`'s own
    /// operational audit-log timestamp (which records "when did the Redis
    /// write happen" and is legitimately server-local), a semantic
    /// correction's timestamps are themselves part of the auditable
    /// assertion being recorded and must be reproducible/testable/
    /// caller-attributable, matching `effective_from` below.
    pub recorded_at: i64,
    /// When this assertion is effective from — caller-supplied, may be
    /// backdated for a retroactive correction (P2-D stores this; it does
    /// NOT build the ingestion-time-aware `sch_` resolver that would
    /// consume it — that's P4, since `sem_` isn't on the wire yet).
    pub effective_from: i64,
    pub status: RevisionStatus,
}

/// What [`append_revision`]-shaped storage calls report back: whether a
/// genuinely new revision was appended, or the call was an idempotent
/// no-op because the supplied bytes already matched the active revision
/// (the brief's "identical `canonical_bytes` → idempotent no-op returning
/// `AlreadyActive`" case). Modeled as an explicit outcome rather than
/// folding both cases into a bare `Revision` so a caller (Task 6's
/// `PUT /schemas/{sch}/semantic`, which must return `200` for the
/// idempotent case without minting anything) can distinguish them without
/// comparing revision counts before/after.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AppendOutcome {
    /// A new immutable revision was written and the active pointer moved.
    /// `etag` is the AUTHORITATIVE post-write etag `SEM_APPEND_SCRIPT`
    /// itself computed and returned inside the same atomic transition —
    /// never a value separately re-read afterward, so a caller building an
    /// HTTP response never needs a second round trip, and can never observe
    /// an `ETag` header that describes a DIFFERENT revision than the one in
    /// `revision` (see `deblob-redis::semantic::append_revision`'s docs).
    Appended { revision: Revision, etag: Etag },
    /// `canonical_bytes` matched the currently active revision exactly;
    /// nothing was written. Carries that (pre-existing) active revision and
    /// its CURRENT etag — likewise read by `SEM_APPEND_SCRIPT` atomically,
    /// inside the very idempotency-check branch that decided this was a
    /// replay, never via a separate re-read.
    AlreadyActive { revision: Revision, etag: Etag },
}

impl AppendOutcome {
    /// The revision either newly appended or already active — the thing
    /// that is now (or still) the schema's current semantic assertion,
    /// regardless of which case produced it.
    pub fn into_revision(self) -> Revision {
        match self {
            AppendOutcome::Appended { revision, .. }
            | AppendOutcome::AlreadyActive { revision, .. } => revision,
        }
    }

    pub fn revision(&self) -> &Revision {
        match self {
            AppendOutcome::Appended { revision, .. }
            | AppendOutcome::AlreadyActive { revision, .. } => revision,
        }
    }

    /// The authoritative current etag, straight from `SEM_APPEND_SCRIPT`'s
    /// own atomic reply — the whole reason this type carries it at all: a
    /// caller never needs a separate `active_semantic` read to learn the
    /// etag that goes with `revision()`.
    pub fn etag(&self) -> Etag {
        match self {
            AppendOutcome::Appended { etag, .. } | AppendOutcome::AlreadyActive { etag, .. } => {
                *etag
            }
        }
    }

    pub fn was_appended(&self) -> bool {
        matches!(self, AppendOutcome::Appended { .. })
    }
}

/// Errors from the append-only semantic-revision store (Task 5). Distinct
/// from [`crate::error::CoreError`] (the structural-vault taxonomy) because
/// `MissingReason`/`EtagConflict` are decided BEFORE any write is even
/// attempted, purely from the caller's request shape versus the currently
/// active revision — a different failure class than the structural vault's
/// immutability/alias-conflict/registry-unavailable guards, and one Task 6
/// needs to map onto its own status codes (`400`/`409`) independently of
/// `CoreError`'s mapping.
#[derive(Debug, thiserror::Error)]
pub enum SemError {
    /// The supplied bytes differ from the active revision's, but no
    /// `reason` was supplied. Brief §4: `PUT .../semantic` → `400`.
    #[error("a reason is required to change an existing semantic assertion")]
    MissingReason,
    /// The supplied bytes differ from the active revision's, but
    /// `expected_etag` did not match the current pointer. `current` is the
    /// actual current etag (`0` if the schema was never annotated) so the
    /// caller can retry with fresh state. Brief §4: `409`.
    #[error("etag conflict: expected {expected:?}, current is {current:?}")]
    EtagConflict {
        expected: Option<Etag>,
        current: Etag,
    },
    /// The underlying store could not be reached or returned malformed
    /// data. Mirrors `CoreError::RegistryUnavailable`'s wording/intent but
    /// stays a separate variant so this error type has no dependency on
    /// `CoreError` at all.
    #[error("semantic revision store unavailable: {0}")]
    StoreUnavailable(String),
    /// Stored revision/pointer data could not be parsed back into typed
    /// values (corrupt hash, unknown `reason_code`/`status` string, bad
    /// hex, etc.) — a data-integrity problem, never a normal outcome of a
    /// well-formed call.
    #[error("corrupt semantic revision data: {0}")]
    Corrupt(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_code_round_trips_snake_case() {
        assert_eq!(
            serde_json::to_string(&ReasonCode::Correction).unwrap(),
            "\"correction\""
        );
        assert_eq!(
            serde_json::to_string(&ReasonCode::OntologyUpgrade).unwrap(),
            "\"ontology_upgrade\""
        );
        assert_eq!(
            serde_json::to_string(&ReasonCode::PolicyReview).unwrap(),
            "\"policy_review\""
        );
        assert_eq!(
            serde_json::to_string(&ReasonCode::SourceContractChange).unwrap(),
            "\"source_contract_change\""
        );
        assert_eq!(
            serde_json::to_string(&ReasonCode::OperatorOverride).unwrap(),
            "\"operator_override\""
        );
        let round: ReasonCode = serde_json::from_str("\"correction\"").unwrap();
        assert_eq!(round, ReasonCode::Correction);
    }

    #[test]
    fn append_outcome_into_revision_unwraps_either_variant() {
        let rev = Revision {
            revision_id: RevisionId::new_v7(),
            sch_id: SchemaId::from_digest(&[1u8; 32]),
            sem_id: SemanticId::from_digest(&[2u8; 32]),
            metadata: SemanticMetadata {
                event_type: None,
                fields: vec![],
            },
            canonical_semantic_bytes: vec![1, 2, 3],
            previous_revision_id: None,
            actor: "kamil".to_string(),
            reason_code: ReasonCode::Correction,
            reason: "fix".to_string(),
            recorded_at: 1,
            effective_from: 1,
            status: RevisionStatus::Active,
        };

        let appended = AppendOutcome::Appended {
            revision: rev.clone(),
            etag: Etag(1),
        };
        assert!(appended.was_appended());
        assert_eq!(appended.etag(), Etag(1));
        assert_eq!(appended.into_revision(), rev.clone());

        let already = AppendOutcome::AlreadyActive {
            revision: rev.clone(),
            etag: Etag(1),
        };
        assert!(!already.was_appended());
        assert_eq!(already.etag(), Etag(1));
        assert_eq!(already.into_revision(), rev);
    }
}
