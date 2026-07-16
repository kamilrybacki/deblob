//! The four-layer metric decomposition (spec §3) — each layer its own
//! module/function over `(arm decisions, gold sidecar)`, per spec §8:
//! "metrics (`metrics/`) — the 4 layers + McNemar/paired-bootstrap +
//! rule-of-three CI + risk-coverage curve."
//!
//! - [`l1_retrieval`] — deterministic retrieval capability, independent of
//!   any arm's decision.
//! - [`l2_raw`] — raw (ungated) decider capability against the external
//!   label.
//! - [`l3_gate`] — how much the trust gate contains/costs, comparing a raw
//!   decider to its gated counterpart.
//! - [`l4_utility`] — B1-vs-A1 incremental system utility, with McNemar +
//!   paired bootstrap significance.
//! - [`stats`] — the shared statistical primitives (rule-of-three,
//!   McNemar, bootstrap) the above layers build on.

pub mod l1_retrieval;
pub mod l2_raw;
pub mod l3_gate;
pub mod l4_utility;
pub mod stats;

pub use l1_retrieval::{compute_l1, L1CaseView, RetrievalMetrics};
pub use l2_raw::{compute_l2, L2CaseView, L2Metrics};
pub use l3_gate::{compute_l3, GateContainmentMetrics, L3CaseView};
pub use l4_utility::{compute_l4, Contingency, L4CaseView, UtilityReport};
