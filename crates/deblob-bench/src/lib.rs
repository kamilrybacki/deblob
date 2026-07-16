//! Dataset generation for the Deblob k3s benchmark: a seeded synthetic JSON
//! stream generator plus embedded real-world fixtures. Purely additive —
//! this crate has no dependency on, and makes no changes to, any shipped
//! Deblob product crate. Spec `docs/superpowers/specs/
//! 2026-07-16-deblob-k3s-benchmark.md` §3.1.
//!
//! The producer/measurer/mgmt-prober halves of the bench (which need a
//! live Kafka-compatible broker and a running Deblob) are a later task;
//! this crate only builds and validates the dataset.

pub mod config;
pub mod fields;
pub mod fixtures;
pub mod generator;
pub mod malform;
pub mod padding;
pub mod record;
pub mod schema;

pub use config::{PayloadSize, SyntheticConfig};
pub use fixtures::{all_fixtures, real_world_stream, RealWorldKind, RealWorldStream};
pub use generator::{generate, SyntheticGenerator};
pub use record::{GeneratedRecord, RecordKind};
