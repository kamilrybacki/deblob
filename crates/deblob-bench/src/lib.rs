//! Dataset generation + client harness for the Deblob k3s benchmark: a
//! seeded synthetic JSON stream generator, embedded real-world fixtures,
//! an `rdkafka` producer/measurer pair, an authenticated management-API
//! prober, a scenario runner, and a JSON/human reporter. Purely additive —
//! this crate has no dependency on, and makes no changes to, any shipped
//! Deblob product crate; it observes Deblob only through the same Kafka
//! wire contract and HTTP API a real operator would. Spec
//! `docs/superpowers/specs/2026-07-16-deblob-k3s-benchmark.md` §3.1/§4/§5.
//!
//! Every module whose logic needs a live broker or management API
//! (`producer::produce_stream`, `measurer::measure_topic`,
//! `prober::MgmtProber`'s methods, `scenarios::run_scenario`) is exercised
//! by the controller's Docker-backed integration run, not this crate's own
//! `cargo test` — see the crate's Task 4 report for the exact split. The
//! PURE parts of each of those modules (header codec, tag classification,
//! histogram percentiles, `ClientConfig` builders, Prometheus-text
//! parsing, report shape, CLI parsing) are unit-tested per-module.

pub mod config;
pub mod fields;
pub mod fixtures;
pub mod generator;
pub mod header;
pub mod histogram;
pub mod malform;
pub mod measurer;
pub mod outcome;
pub mod padding;
pub mod prober;
pub mod producer;
pub mod record;
pub mod report;
pub mod scenarios;
pub mod schema;

pub use config::{PayloadSize, SyntheticConfig};
pub use fixtures::{all_fixtures, real_world_stream, RealWorldKind, RealWorldStream};
pub use generator::{generate, SyntheticGenerator};
pub use record::{GeneratedRecord, RecordKind};
