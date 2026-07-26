//! Schema monoid: mergeable structural profiles over parsed JSON
//! documents. `Profile::merge` is associative, commutative, and has
//! `Profile::identity()` as its neutral element (proven by proptest in
//! `merge`), so profiles from independently observed documents can be
//! combined in any order to approximate a generalized schema. Spec §4/§6.

pub mod merge;
pub mod profile;
pub mod simhash;

pub use profile::{FieldNode, NumericBuckets, Profile, TypeCounts, GENERALIZER};
pub use simhash::{
    structural_hamming, structural_simhash, NEAR_DUPLICATE_HAMMING_MAX,
    STRUCTURAL_SIMHASH_VERSION,
};
