//! `sem_` digest (P2-D Task 3, `deblob-p2d-hermes-review.md` §2/§3): hashes
//! [`canon::canonical_semantic_bytes`] output behind a domain-separation
//! tag, and enforces the "unannotated is a true `None`" rule — a
//! [`SemanticMetadata`] with no surviving canonical assertion never gets a
//! `sem_` at all (no bytes are encoded, no hash is computed, no sentinel
//! value is returned).
//!
//! `sch_` is never referenced anywhere in this module, directly or via the
//! preimage: [`SemanticMetadata`] carries no schema-id field, so two
//! differently-structured schemas that carry identical semantic metadata
//! hash to the identical `sem_` by construction (the §5
//! same-semantic-different-structure diagnostic signal this is meant to
//! enable is a later task; this module just has to not break it).

use sha2::{Digest, Sha256};

use deblob_core::id::SemanticId;
use deblob_core::semantic::{SemanticFingerprint, SemanticMetadata};

use crate::canon::{self, encode_normalized, normalize, CanonError};
use crate::vocab::VOCAB_VERSION;

/// Computes `metadata`'s `sem_`, if it has one.
///
/// Preimage is exactly `VOCAB_VERSION || 0x00 || canonical_semantic_bytes`
/// (the same `"deblob-semantic-v1"` version tag the vocabulary tables in
/// `vocab.rs` are keyed by, so a hash and the vocabulary it was checked
/// against always travel together) — never `sch_`, which would defeat the
/// "same semantics, different structure" detection this identity dimension
/// exists to enable.
///
/// Returns `Ok(None)` — no bytes ever get encoded and no hash is ever
/// computed in this branch — when `metadata` carries no `event_type` and
/// every field entry normalizes away to nothing (no attributes, or only
/// attributes that themselves normalize to absent). This is a real
/// `Option::None`, not an all-zero digest or a hash of empty bytes: two
/// unannotated schemas must never compare equal via `sem_`.
pub fn semantic_fingerprint(
    metadata: &SemanticMetadata,
) -> Result<Option<SemanticFingerprint>, CanonError> {
    let normalized = normalize(metadata)?;
    if normalized.is_empty() {
        return Ok(None);
    }

    let bytes = encode_normalized(&normalized);
    let mut hasher = Sha256::new();
    hasher.update(VOCAB_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(&bytes);
    let digest: [u8; 32] = hasher.finalize().into();

    Ok(Some(SemanticFingerprint(SemanticId::from_digest(&digest))))
}

/// Re-exported for callers (Task 5 storage) that want the canonical bytes
/// alongside the fingerprint for byte-compare on replay, without
/// recomputing normalization.
pub use canon::canonical_semantic_bytes;

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{FieldEntry, FieldSemantics, PathSegment};

    fn empty_semantics() -> FieldSemantics {
        FieldSemantics {
            canonical_field_id: None,
            identifier_namespace: None,
            unit: None,
            numeric_scale: None,
            temporal: None,
            enum_semantics: None,
        }
    }

    #[test]
    fn no_event_type_and_no_fields_is_none() {
        let meta = SemanticMetadata {
            event_type: None,
            fields: vec![],
        };
        assert_eq!(semantic_fingerprint(&meta).unwrap(), None);
    }

    #[test]
    fn fields_present_but_all_semantics_empty_is_still_none() {
        let meta = SemanticMetadata {
            event_type: None,
            fields: vec![FieldEntry {
                path: vec![PathSegment::Key("a".to_string())],
                semantics: empty_semantics(),
            }],
        };
        assert_eq!(semantic_fingerprint(&meta).unwrap(), None);
    }

    #[test]
    fn none_is_not_hash_of_empty_bytes() {
        // Guard against a regression that would make "empty" silently
        // resolve to some sentinel hash instead of a true None: compute
        // what sha256(domain_tag || canonical_bytes_of_empty) *would* be,
        // and confirm the real function never surfaces that as `Some`.
        let empty_meta = SemanticMetadata {
            event_type: None,
            fields: vec![],
        };
        let result = semantic_fingerprint(&empty_meta).unwrap();
        assert!(result.is_none());

        let would_be_bytes = canonical_semantic_bytes(&empty_meta).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(VOCAB_VERSION.as_bytes());
        hasher.update([0u8]);
        hasher.update(&would_be_bytes);
        let sentinel_digest: [u8; 32] = hasher.finalize().into();
        let sentinel_id = SemanticId::from_digest(&sentinel_digest);
        // The real function must not have quietly returned this as Some.
        assert_ne!(Some(SemanticFingerprint(sentinel_id)), result);
    }

    #[test]
    fn event_type_only_produces_some() {
        let meta = SemanticMetadata {
            event_type: Some(deblob_core::semantic::CanonicalEventTypeId::new(
                "user.created",
            )),
            fields: vec![],
        };
        assert!(semantic_fingerprint(&meta).unwrap().is_some());
    }

    #[test]
    fn determinism_independent_of_field_order() {
        let field_a = FieldEntry {
            path: vec![PathSegment::Key("a".to_string())],
            semantics: FieldSemantics {
                numeric_scale: Some(1),
                ..empty_semantics()
            },
        };
        let field_b = FieldEntry {
            path: vec![PathSegment::Key("b".to_string())],
            semantics: FieldSemantics {
                numeric_scale: Some(2),
                ..empty_semantics()
            },
        };
        let forward = SemanticMetadata {
            event_type: None,
            fields: vec![field_a.clone(), field_b.clone()],
        };
        let reverse = SemanticMetadata {
            event_type: None,
            fields: vec![field_b, field_a],
        };
        assert_eq!(
            semantic_fingerprint(&forward).unwrap(),
            semantic_fingerprint(&reverse).unwrap()
        );
    }

    #[test]
    fn sem_id_never_parses_as_sch_id_domain_separation() {
        let meta = SemanticMetadata {
            event_type: Some(deblob_core::semantic::CanonicalEventTypeId::new(
                "user.created",
            )),
            fields: vec![],
        };
        let fp = semantic_fingerprint(&meta).unwrap().unwrap();
        let sem_str = fp.0.as_str();
        assert!(sem_str.starts_with("sem_"));
        assert!(deblob_core::id::SchemaId::parse(sem_str).is_err());
        assert!(deblob_core::id::CandidateId::parse(sem_str).is_err());
    }

    #[test]
    fn duplicate_path_error_propagates() {
        let meta = SemanticMetadata {
            event_type: None,
            fields: vec![
                FieldEntry {
                    path: vec![PathSegment::Key("a".to_string())],
                    semantics: FieldSemantics {
                        numeric_scale: Some(1),
                        ..empty_semantics()
                    },
                },
                FieldEntry {
                    path: vec![PathSegment::Key("a".to_string())],
                    semantics: FieldSemantics {
                        numeric_scale: Some(2),
                        ..empty_semantics()
                    },
                },
            ],
        };
        assert!(matches!(
            semantic_fingerprint(&meta),
            Err(CanonError::DuplicatePath { .. })
        ));
    }
}
