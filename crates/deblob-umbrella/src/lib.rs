//! # deblob-umbrella
//!
//! Gold-tier **consolidation / umbrella schemas** for Deblob — the medallion
//! `bronze → silver → gold` progression's gold layer. A gold [`UmbrellaSchema`] is
//! one canonical event contract over N semantically-similar source schemas, and a
//! [`ChildTransform`] is a pinned, **executable** projection from one child into it.
//!
//! This crate is the **deterministic safety core** of the feature (design's
//! "determinism disposes"): the closed transform DSL, its executor, the unit
//! registry, and the static + execution verification that any proposal — SLM- or
//! human-authored — must survive. It contains no SLM, clustering, persistence, or
//! API; those are separate slices that all check against what lives here.
//!
//! Joint design: `docs/umbrella-schema-design.md` (Claude × Hermes, jr-umbrella-181605).

pub mod executor;
pub mod path;
pub mod types;
pub mod units;
pub mod verify;

pub use types::{
    Binding, Cardinality, CastMode, ChildTransform, FieldType, JsonPath, Op, OnError, OnMissing,
    Relation, ScalarType, UmbrellaField, UmbrellaSchema,
};
