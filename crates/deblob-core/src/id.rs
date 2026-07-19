//! Identity types. Spec §5.

use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdError {
    #[error("wrong prefix, expected {expected}")]
    WrongPrefix { expected: &'static str },
    #[error("invalid base32 body")]
    InvalidBody,
}

macro_rules! digest_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn from_digest(digest: &[u8; 32]) -> Self {
                Self(format!(
                    "{}{}",
                    $prefix,
                    BASE32_NOPAD.encode(digest).to_ascii_lowercase()
                ))
            }
            pub fn parse(s: &str) -> Result<Self, IdError> {
                let body = s
                    .strip_prefix($prefix)
                    .ok_or(IdError::WrongPrefix { expected: $prefix })?;
                let up = body.to_ascii_uppercase();
                let bytes = BASE32_NOPAD
                    .decode(up.as_bytes())
                    .map_err(|_| IdError::InvalidBody)?;
                if bytes.len() != 32 {
                    return Err(IdError::InvalidBody);
                }
                Ok(Self(s.to_string()))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
digest_id!(SchemaId, "sch_");
digest_id!(CandidateId, "cand_");
digest_id!(SemanticId, "sem_");
digest_id!(SourceId, "src_");
digest_id!(ValueProfileId, "vp_");

impl SourceId {
    /// Stable content-addressed identity for a data SOURCE (the Kafka topic
    /// — or other source identity, e.g. the HTTP proxy's `origin_prefix` — a
    /// record was consumed from). A pure function of the source string only
    /// (no clock, no random), so the SAME source name always maps to the
    /// IDENTICAL `src_` id: the `SourceRegistry` (spec §9 lineage) can
    /// therefore register idempotently and every observer derives the same
    /// id without coordinating.
    ///
    /// Domain-separated from [`CandidateId::from_source_and_digest`]'s use of
    /// the same source string: there the source folds into a candidate's
    /// SHAPE digest; here it is the whole preimage, tagged with a distinct
    /// prefix byte so a `src_` id can never collide with the source-folded
    /// half of a `cand_` preimage.
    pub fn from_source(source: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"s"); // domain tag: source-identity preimage
        hasher.update(source.as_bytes());
        let digest: [u8; 32] = hasher.finalize().into();
        Self::from_digest(&digest)
    }
}

impl CandidateId {
    /// Source-scoped candidate identity (Hermes lineage gap 3, spec §4/§9):
    /// folds `source` (the Kafka topic — or other source identity, e.g. the
    /// HTTP proxy's `origin_prefix` — a record was consumed from) into the
    /// digest preimage alongside the raw shape `digest`, so two records
    /// from DIFFERENT sources that happen to share the exact same
    /// structural shape mint DIFFERENT `cand_` ids. "Source co-occurrence
    /// is provenance, not semantic evidence" — it must never be grounds for
    /// merging two sources' candidates into one.
    ///
    /// Pure function of `(source, digest)` only — no clock, no random —
    /// so the SAME source observing the SAME raw shape always mints the
    /// IDENTICAL `cand_` id (replay-safe, spec §3.2), while a DIFFERENT
    /// source never collides with it even when `digest` is identical.
    ///
    /// This is the canonical way to mint a `cand_` id from a raw shape
    /// fingerprint plus its source; every such mint site (currently
    /// `deblob_match::matcher::HotMatcher::classify`) should go through
    /// this rather than [`Self::from_digest`] directly, so source-scoping
    /// can never silently regress at a new call site.
    pub fn from_source_and_digest(source: &str, digest: &[u8; 32]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        // Domain-separates `source`'s bytes from `digest`'s: without this,
        // a source `"ab"` + digest `[0xCD, ..]` could theoretically be
        // constructed to alias a source `"a"` + a different digest sharing
        // the same concatenated byte stream. `source` is never attacker-
        // supplied raw payload (it's the consumed topic / configured
        // origin prefix), but this is a one-byte cost for an unconditional
        // guarantee rather than a "never happens in practice" assumption.
        hasher.update([0u8]);
        hasher.update(digest);
        let folded: [u8; 32] = hasher.finalize().into();
        Self::from_digest(&folded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct FamilyId(String);
impl FamilyId {
    pub fn new_v7() -> Self {
        Self(format!("fam_{}", uuid::Uuid::now_v7()))
    }
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let body = s
            .strip_prefix("fam_")
            .ok_or(IdError::WrongPrefix { expected: "fam_" })?;
        uuid::Uuid::parse_str(body).map_err(|_| IdError::InvalidBody)?;
        Ok(Self(s.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct FamilyVersion(pub u32);

/// A semantic-assertion revision id (P2-D Task 5, Hermes review §4). Like
/// [`FamilyId`], this is a fresh time-ordered UUIDv7 minted per revision —
/// NOT content-addressed, since two revisions can legitimately carry
/// identical `canonical_semantic_bytes` at different points in a schema's
/// history (e.g. a correction that reverts to a prior value) and must still
/// be distinct, individually-addressable records. UUIDv7's leading 48 bits
/// are a millisecond timestamp, so lexicographically sorting `RevisionId`
/// strings sorts them chronologically — `RedisRegistry::revisions` relies on
/// this instead of maintaining a separate ordered index.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct RevisionId(String);
impl RevisionId {
    pub fn new_v7() -> Self {
        Self(format!("rev_{}", uuid::Uuid::now_v7()))
    }
    pub fn parse(s: &str) -> Result<Self, IdError> {
        let body = s
            .strip_prefix("rev_")
            .ok_or(IdError::WrongPrefix { expected: "rev_" })?;
        uuid::Uuid::parse_str(body).map_err(|_| IdError::InvalidBody)?;
        Ok(Self(s.to_string()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaRef {
    Known(SchemaId),
    Provisional(CandidateId),
    Unresolved,
    Malformed,
    Tombstone,
}
impl SchemaRef {
    pub fn header_value(&self) -> String {
        match self {
            SchemaRef::Known(id) => id.as_str().to_string(),
            SchemaRef::Provisional(id) => id.as_str().to_string(),
            SchemaRef::Unresolved => "unresolved".into(),
            SchemaRef::Malformed => "malformed".into(),
            SchemaRef::Tombstone => "tombstone".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Hermes lineage gap 3: same source + same raw digest always mints the
    // identical cand_ id (deterministic/replay-safe — spec §3.2).
    #[test]
    fn candidate_id_from_source_and_digest_is_deterministic_per_source() {
        let d = [7u8; 32];
        let a1 = CandidateId::from_source_and_digest("topic-a", &d);
        let a2 = CandidateId::from_source_and_digest("topic-a", &d);
        assert_eq!(a1, a2);
        assert!(a1.as_str().starts_with("cand_"));
    }

    // Hermes lineage gap 3 (the core fix): the SAME raw shape digest from
    // DIFFERENT sources must mint DIFFERENT candidate ids — "source
    // co-occurrence is provenance, not semantic evidence."
    #[test]
    fn candidate_id_from_source_and_digest_differs_across_sources() {
        let d = [7u8; 32];
        let a = CandidateId::from_source_and_digest("topic-a", &d);
        let b = CandidateId::from_source_and_digest("topic-b", &d);
        assert_ne!(a, b);
    }

    // Source-scoping must not collapse back onto the plain source-blind
    // `from_digest` mint — folding `source` in actually changes the digest
    // preimage, not just tag along unused.
    #[test]
    fn candidate_id_from_source_and_digest_differs_from_source_blind_digest() {
        let d = [7u8; 32];
        let scoped = CandidateId::from_source_and_digest("topic-a", &d);
        let blind = CandidateId::from_digest(&d);
        assert_ne!(scoped, blind);
    }

    #[test]
    fn schema_id_from_digest_roundtrips() {
        let d = [0xABu8; 32];
        let id = SchemaId::from_digest(&d);
        assert!(id.as_str().starts_with("sch_"));
        assert_eq!(SchemaId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn schema_id_encodes_full_256_bits_base32_lower_nopad() {
        let d = [0u8; 32];
        let id = SchemaId::from_digest(&d);
        // 32 bytes → 52 base32 chars unpadded
        assert_eq!(id.as_str().len(), "sch_".len() + 52);
        assert!(id.as_str()[4..]
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn parse_rejects_wrong_prefix_and_garbage() {
        assert!(SchemaId::parse("cand_abc").is_err());
        assert!(SchemaId::parse("sch_!!!").is_err());
        assert!(CandidateId::parse("sch_abc").is_err());
    }

    #[test]
    fn source_id_from_source_is_deterministic_and_distinct() {
        let a1 = SourceId::from_source("events.grid.carbonintensity");
        let a2 = SourceId::from_source("events.grid.carbonintensity");
        let b = SourceId::from_source("events.compute.azure");
        assert!(a1.as_str().starts_with("src_"));
        assert_eq!(a1, a2, "same source name -> identical id");
        assert_ne!(a1, b, "different sources -> different ids");
        assert_eq!(SourceId::parse(a1.as_str()).unwrap(), a1);
        // Domain separation: a src_ id must not parse as another dimension.
        assert!(SchemaId::parse(a1.as_str()).is_err());
        assert!(SourceId::parse("sch_abc").is_err());
    }

    #[test]
    fn semantic_id_from_digest_roundtrips() {
        let d = [0xABu8; 32];
        let id = SemanticId::from_digest(&d);
        assert!(id.as_str().starts_with("sem_"));
        assert_eq!(SemanticId::parse(id.as_str()).unwrap(), id);
    }

    #[test]
    fn parse_rejects_semantic_prefix_domain_separation() {
        // sem_ must never parse as sch_/cand_, and vice versa (domain
        // separation between the three identity dimensions, spec P2-D).
        assert!(SemanticId::parse("sch_abc").is_err());
        assert!(SemanticId::parse("cand_abc").is_err());
        assert!(SemanticId::parse("sem_!!!").is_err());
        assert!(SchemaId::parse("sem_abc").is_err());
        assert!(CandidateId::parse("sem_abc").is_err());
    }

    #[test]
    fn revision_id_new_v7_roundtrips_and_sorts_chronologically() {
        let a = RevisionId::new_v7();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = RevisionId::new_v7();
        assert!(a.as_str().starts_with("rev_"));
        assert_eq!(RevisionId::parse(a.as_str()).unwrap(), a);
        assert!(
            a.as_str() < b.as_str(),
            "UUIDv7 string ordering must be chronological: {a:?} then {b:?}"
        );
    }

    #[test]
    fn revision_id_parse_rejects_wrong_prefix_and_garbage() {
        assert!(RevisionId::parse("sch_abc").is_err());
        assert!(RevisionId::parse("rev_not-a-uuid").is_err());
    }

    #[test]
    fn schema_ref_header_values() {
        let d = [1u8; 32];
        assert!(SchemaRef::Known(SchemaId::from_digest(&d))
            .header_value()
            .starts_with("sch_"));
        assert!(SchemaRef::Provisional(CandidateId::from_digest(&d))
            .header_value()
            .starts_with("cand_"));
        assert_eq!(SchemaRef::Unresolved.header_value(), "unresolved");
        assert_eq!(SchemaRef::Malformed.header_value(), "malformed");
        assert_eq!(SchemaRef::Tombstone.header_value(), "tombstone");
    }
}
