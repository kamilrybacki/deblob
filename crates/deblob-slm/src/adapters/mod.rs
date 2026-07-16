//! Real backend adapters behind [`crate::contract::SemanticInferencer`]
//! (spec `2026-07-16-deblob-experiment.md` §5: "A `SemanticInferencer` port
//! already exists; add thin backend adapters").
//!
//! ## The shared OpenAI-compat core
//!
//! [`crate::http::HttpInferencer`] (Task 2) IS the shared OpenAI-compat
//! core: forced tool-calling against `submit_semantic_decision`, one
//! mechanical repair retry on a malformed/unknown-field response, a safe
//! [`crate::contract::InferenceDecision::Abstain`] fallback when that
//! repair also fails (never a spurious match), telemetry, and a decision
//! cache. Every adapter here is a THIN wrapper around one
//! `HttpInferencer` instance, configured with backend-specific defaults
//! (base URL / model id conventions) plus a [`crate::runtime::RuntimeInfo`]
//! tag — no HTTP/parse logic is duplicated three times.
//!
//! ## The outer bounded retry-with-backoff
//!
//! `HttpInferencer` already retries once, immediately, on a
//! malformed-or-transport failure (Task 2's "one mechanical repair"). That
//! is a same-request retry aimed at a flaky single response. This module
//! adds one more layer ON TOP, shared by every adapter
//! ([`classify_with_retry`]): a small, BOUNDED number of additional
//! `classify()` attempts, each separated by an exponential-with-cap
//! backoff sleep, applied only to [`crate::contract::InferenceError`]
//! (total transport/timeout failure — never to a parse failure, which
//! `HttpInferencer` already converts to a safe `Abstain` outcome, not an
//! `Err`). This targets real network flakiness (a model server mid-restart,
//! a cold `cactus serve` process) without an unbounded retry loop: total
//! HTTP attempts per `classify()` call are capped at
//! `(RetryPolicy::max_retries + 1) * 2` (each outer attempt may itself
//! retry once internally). Exhausting the bounded retries still returns
//! `Err(InferenceError)`, never a panic and never a fabricated decision —
//! the caller ([`crate::contract::SemanticInferencer`]'s existing
//! contract, and `deblob-experiment`'s `SemanticArm`) already maps that to
//! a safe abstain outcome.

pub mod cactus;
pub mod llama_cpp;
pub mod ollama;

pub use cactus::{CactusConfig, CactusInferencer};
pub use llama_cpp::{LlamaCppConfig, LlamaCppInferencer};
pub use ollama::{OllamaConfig, OllamaInferencer};

use std::time::Duration;

use crate::contract::{InferenceError, InferenceOutcome, InferenceRequest, SemanticInferencer};
use crate::http::HttpInferencer;

/// Bounded exponential backoff policy for the outer retry layer (see the
/// module docs). `max_retries: 0` disables the outer layer entirely (only
/// `HttpInferencer`'s own internal repair retry runs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl RetryPolicy {
    /// No outer retry at all.
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            base_backoff_ms: 0,
            max_backoff_ms: 0,
        }
    }

    pub fn bounded(max_retries: u32, base_backoff_ms: u64, max_backoff_ms: u64) -> Self {
        Self {
            max_retries,
            base_backoff_ms,
            max_backoff_ms,
        }
    }

    /// Backoff delay before the (0-indexed) `attempt`th retry: doubles each
    /// attempt, capped at `max_backoff_ms`. `attempt` is clamped to avoid a
    /// shift overflow on `1u64 << attempt` for a pathologically large
    /// `max_retries`.
    fn backoff_for(&self, attempt: u32) -> u64 {
        let shift = attempt.min(20);
        let exp = self.base_backoff_ms.saturating_mul(1u64 << shift);
        exp.min(self.max_backoff_ms)
    }
}

impl Default for RetryPolicy {
    /// One bounded outer retry, 150ms then capped growth to 1s — generous
    /// enough to ride out a brief local-runtime hiccup, bounded enough to
    /// never hang the eval harness on a dead endpoint.
    fn default() -> Self {
        Self::bounded(1, 150, 1_000)
    }
}

/// Shared retry-with-backoff loop every adapter's `classify()` delegates
/// to. See the module docs for exactly what this does and does not retry.
pub(crate) async fn classify_with_retry(
    inner: &HttpInferencer,
    req: &InferenceRequest,
    policy: RetryPolicy,
) -> Result<InferenceOutcome, InferenceError> {
    let mut attempt = 0u32;
    loop {
        match inner.classify(req.clone()).await {
            Ok(outcome) => return Ok(outcome),
            Err(err) => {
                if attempt >= policy.max_retries {
                    return Err(err);
                }
                let delay = policy.backoff_for(attempt);
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_then_caps() {
        let policy = RetryPolicy::bounded(5, 100, 1_000);
        assert_eq!(policy.backoff_for(0), 100);
        assert_eq!(policy.backoff_for(1), 200);
        assert_eq!(policy.backoff_for(2), 400);
        assert_eq!(policy.backoff_for(3), 800);
        assert_eq!(policy.backoff_for(4), 1_000, "capped at max_backoff_ms");
        assert_eq!(policy.backoff_for(30), 1_000, "large attempt stays capped");
    }

    #[test]
    fn none_policy_never_delays() {
        let policy = RetryPolicy::none();
        assert_eq!(policy.backoff_for(0), 0);
        assert_eq!(policy.max_retries, 0);
    }
}
