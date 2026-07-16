//! `deblob-experiment`: the comparative-experiment harness core (spec
//! `docs/superpowers/specs/2026-07-16-deblob-experiment.md`).
//!
//! This crate answers the motivating question — does the SLM intelligence
//! lane buy more *safe automation* than the deterministic lane alone, at
//! the same zero-false-merge bound — with ground truth EXTERNAL to the
//! trust gate (§2's anti-tautology core) and a four-layer metric
//! decomposition (§3) so the gate cannot hide model errors behind its own
//! filtering.
//!
//! This task covers everything runnable OFFLINE on the SYNTHETIC corpus:
//! arms A0/A1/B0/B1/B2 ([`arms`]), the leak-strip guard ([`labels`]), the
//! four metric layers ([`metrics`]), and a deterministic-by-seed runner
//! ([`run`]) + reporter ([`reporter`]). Real corpus ingestion (spec §6b)
//! and live model endpoint adapters (spec §5) are later tasks — [`arms`]
//! leaves the exact seam (`deblob_slm::SemanticInferencer`) those tasks
//! plug into.
//!
//! Additive only: no product-crate (`deblob`) decision logic is changed or
//! reimplemented here — the trust gate is reused verbatim via
//! `deblob::shadow::evaluate_policy`.

pub mod arms;
pub mod labels;
pub mod metrics;
pub mod reporter;
pub mod run;

pub use arms::{Arm, ArmDecision, ArmId};
pub use labels::{split_case, split_corpus, GoldSidecar, InferenceInput};
pub use run::{run_experiment, ArmReport, ExperimentReport, RunConfig};
