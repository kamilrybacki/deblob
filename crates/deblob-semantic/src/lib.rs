//! `deblob-semantic-v1`: version-addressed controlled vocabulary tables +
//! validation for the `deblob_core::semantic` types (P2-D Task 2,
//! `deblob-p2d-hermes-review.md` §1/§2).
//!
//! Scope is deliberately narrow: this crate only holds the immutable
//! vocabulary tables and a pure validation function that checks the
//! *controlled metadata tokens* (unit codes, namespace codes, registered
//! ids, meaning-code vocabularies) against them. It does NOT compute a
//! digest, does NOT define canonical byte serialization (Task 3), and has
//! NO storage, API, or signature concerns (later tasks). No I/O, no async.

pub mod canon;
pub mod digest;
pub mod path;
pub mod vocab;

pub use canon::{canonical_semantic_bytes, CanonError};
pub use digest::semantic_fingerprint;
pub use path::{canonical_field_paths, validate_paths, PathError};
pub use vocab::{
    validate_metadata, CanonicalEventTypeIdRegistry, CanonicalFieldIdRegistry, Registries,
    VocabError, ISO4217_CURRENCIES, MEANING_VOCABULARIES, NAMESPACE_CODES, UCUM_UNITS,
    VOCAB_VERSION,
};
