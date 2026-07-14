//! `deblob` — the hot/cold-lane relay binary. Spec §3.
//!
//! This crate's `lib.rs` exists so the per-message decision logic (the "hot
//! path", spec §3.1) is unit-testable without a running Kafka/Redis stack.
//! Real wiring (config, adapters, binding the management API to its own
//! listen port) lands in Task 18.

pub mod api;
pub mod coldlane;
pub mod matcher;
pub mod policy;
pub mod promote;
