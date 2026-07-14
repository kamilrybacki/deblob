//! Schema monoid: mergeable structural profiles over parsed JSON
//! documents. `Profile::merge` is associative, commutative, and has
//! `Profile::identity()` as its neutral element (proven by proptest in
//! `merge`), so profiles from independently observed documents can be
//! combined in any order to approximate a generalized schema. Spec §4/§6.

pub mod merge;
pub mod profile;

pub use profile::{FieldNode, NumericBuckets, Profile, TypeCounts, GENERALIZER};
