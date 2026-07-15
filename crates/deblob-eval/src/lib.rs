//! `deblob-eval`: the OFFLINE eval harness for the SLM discovery lane.
//!
//! P2-A/B Task 6 (this crate's foundation): the golden corpus FORMAT + a
//! loader + hand-authored seed cases. See
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` §
//! "Tasks 6-7 — eval metrics + corpus" (authoritative over
//! `docs/superpowers/plans/2026-07-14-deblob-p2ab.md` § Task 6).
//!
//! This crate scores a CONFIGURED `deblob_slm::SemanticInferencer`
//! endpoint against the corpus loaded here — it never talks to the cold
//! lane, the registry, or any live Deblob state; the golden corpus is the
//! only ground truth it consumes. Task 7 adds metric computation
//! (recall@k, MRR, false-merge/false-split rate, wrong-valid rate, etc.)
//! and a report; Task 8 adds a wiremock self-test + CI wiring.

pub mod corpus;

pub use corpus::{load_corpus, Category, EvalCase, EvalError, Expected, Partition};
