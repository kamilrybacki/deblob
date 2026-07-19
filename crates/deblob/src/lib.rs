//! `deblob` — the hot/cold-lane relay binary. Spec §3.
//!
//! This crate's `lib.rs` exists so the per-message decision logic (the "hot
//! path", spec §3.1) is unit-testable without a running Kafka/Redis stack.
//! Real wiring (config, adapters, binding the management API to its own
//! listen port) lands in Task 18.
//!
//! `matcher` and `metrics` are re-exported from the `deblob-match` crate
//! rather than owned here: Task 18's `main.rs` wires `deblob-kafka::Relay
//! ::run` into this crate's binary, and `deblob-kafka` itself depends on
//! `HotMatcher`/`Metrics`. Keeping those two modules inside `deblob` would
//! make `deblob-kafka -> deblob -> deblob-kafka` a cyclic package
//! dependency (Cargo rejects this at the package level regardless of which
//! target — lib or bin — actually needs the dependency); splitting them
//! into `deblob-match` breaks the cycle while these re-exports keep every
//! pre-Task-18 `deblob::matcher`/`deblob::metrics` import path working
//! unchanged.

pub mod api;
pub mod coldlane;
pub mod config;
pub mod discovery_consumer;
pub mod feedback;
pub mod model_registry;
pub mod policy;
pub mod promote;
pub mod retrain;
pub mod retrieval;
pub mod semantic_drift;
pub mod semantic_neighbors;
pub mod semantic_store;
pub mod serve;
pub mod shadow;
pub mod trusted;
pub mod umbrella_controller;
pub mod umbrella_guard;
pub mod value_profile;

pub use deblob_match::{matcher, metrics};
