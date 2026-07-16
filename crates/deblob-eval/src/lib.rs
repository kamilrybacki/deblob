//! `deblob-eval`: the OFFLINE eval harness for the SLM discovery lane.
//!
//! P2-A/B Task 6 (`corpus`): the golden corpus FORMAT + a loader +
//! hand-authored seed cases. Task 7 (`metrics`, `report`): drives a
//! configured `SemanticInferencer` against the corpus and computes the
//! full metric set (recall@k, MRR, false-merge/false-split rate,
//! wrong-valid rate tracked apart from schema-valid rate, etc.) plus a
//! human + machine report. See
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` §
//! "Tasks 6-7 — eval metrics + corpus" (authoritative over
//! `docs/superpowers/plans/2026-07-14-deblob-p2ab.md` §§ Task 6-7).
//!
//! This crate scores a CONFIGURED `deblob_slm::SemanticInferencer`
//! endpoint against the corpus loaded here — it never talks to the cold
//! lane, the registry, or any live Deblob state; the golden corpus is the
//! only ground truth it consumes. Task 8 adds a wiremock self-test + CI
//! wiring on top of this module.

pub mod corpus;
pub mod generate;
pub mod metrics;
pub mod report;

pub use corpus::{load_corpus, Category, EvalCase, EvalError, Expected, Partition};
pub use generate::{
    format_summary, generate_corpus, render_finetune_jsonl, write_corpus, GenerateConfig,
    GeneratedCorpus, GenerationSummary,
};
pub use metrics::{
    compute_metrics, measure_candidate_order_sensitivity, measure_repeatability, regression_delta,
    run_eval, CallFailure, CaseResult, CategoryPrecision, EvalRun, Metrics, RegressionDelta,
    RelationConfusionEntry,
};
pub use report::report;
