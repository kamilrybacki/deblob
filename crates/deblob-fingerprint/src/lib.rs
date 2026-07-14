//! Bounded, hand-rolled JSON parser. This crate is the security boundary
//! for untrusted input: `parse_bounded` must never panic, OOM, or hang —
//! every failure mode is surfaced as a `QuarantineReason`. Spec §4.

pub mod limits;
pub mod parse;

pub use limits::Limits;
pub use parse::{parse_bounded, Node};
