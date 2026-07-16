//! Cactus adapter (spec §5: "Cactus adapter — Needle 26M via `cactus
//! serve` (OpenAI-compat)."), and the ONE adapter in this module tagged
//! with [`ModelFamily::NeedleContinualUpdate`] rather than
//! [`ModelFamily::StandardForwardPass`].
//!
//! Per spec §5's roster, `cactus serve` in this harness fronts exactly one
//! model — Needle 26M — whose serving path is a continual/on-device
//! adaptation loop, not a plain static forward pass. The instructions are
//! explicit that this must be surfaced distinctly ("its update path is not
//! the same as the others") so a downstream reporter never conflates
//! Needle's numbers with the other (static) roster models'. If `cactus
//! serve` is ever pointed at a non-Needle, plain-forward-pass model,
//! [`CactusConfig::family`] can be overridden — it defaults to
//! [`ModelFamily::NeedleContinualUpdate`], not hard-coded, precisely so
//! that override is possible without editing this file.

use async_trait::async_trait;

use crate::adapters::{classify_with_retry, RetryPolicy};
use crate::contract::{InferenceError, InferenceOutcome, InferenceRequest, SemanticInferencer};
use crate::http::{HttpInferencer, SlmHttpConfig};
use crate::runtime::{Backend, ModelFamily, RuntimeInfo};

/// `cactus serve`'s default local OpenAI-compat base URL.
pub const DEFAULT_CACTUS_BASE_URL: &str = "http://localhost:8081/v1";

/// Configuration for [`CactusInferencer`].
#[derive(Debug, Clone)]
pub struct CactusConfig {
    pub base_url: String,
    /// Model id as `cactus serve` reports/expects it, e.g. `"needle-26m"`.
    pub model: String,
    pub quantization: Option<String>,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
    pub retry: RetryPolicy,
    /// See the module docs. Defaults to
    /// [`ModelFamily::NeedleContinualUpdate`] — the roster's only Cactus
    /// model (Needle 26M) uses a continual/on-device update path, not a
    /// static forward pass.
    pub family: ModelFamily,
}

impl CactusConfig {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            base_url: DEFAULT_CACTUS_BASE_URL.to_string(),
            model: model.into(),
            quantization: None,
            timeout_ms: 30_000,
            max_concurrency: 4,
            retry: RetryPolicy::default(),
            family: ModelFamily::NeedleContinualUpdate,
        }
    }
}

/// The `SemanticInferencer` for a `cactus serve`-hosted model (Needle 26M
/// in this roster). Delegates entirely to an internal [`HttpInferencer`]
/// plus the shared outer retry layer; see the module docs for the
/// [`ModelFamily`] tagging this adapter defaults to.
pub struct CactusInferencer {
    inner: HttpInferencer,
    retry: RetryPolicy,
    runtime: RuntimeInfo,
}

impl CactusInferencer {
    pub fn new(cfg: CactusConfig) -> Self {
        let runtime = RuntimeInfo {
            backend: Backend::Cactus,
            model_id: cfg.model.clone(),
            quantization: cfg.quantization.clone(),
            endpoint: cfg.base_url.clone(),
            family: cfg.family,
        };
        let inner = HttpInferencer::new(SlmHttpConfig {
            base_url: cfg.base_url,
            model: cfg.model,
            api_token: None,
            timeout_ms: cfg.timeout_ms,
            max_concurrency: cfg.max_concurrency,
        });
        Self {
            inner,
            retry: cfg.retry,
            runtime,
        }
    }

    pub fn runtime_info(&self) -> &RuntimeInfo {
        &self.runtime
    }
}

#[async_trait]
impl SemanticInferencer for CactusInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        classify_with_retry(&self.inner, &req, self.retry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_needle_continual_update_family() {
        let adapter = CactusInferencer::new(CactusConfig::new("needle-26m"));
        let runtime = adapter.runtime_info();
        assert_eq!(runtime.backend, Backend::Cactus);
        assert_eq!(runtime.family, ModelFamily::NeedleContinualUpdate);
        assert_ne!(runtime.family, ModelFamily::StandardForwardPass);
    }

    #[test]
    fn family_is_overridable_for_a_non_needle_cactus_model() {
        let mut cfg = CactusConfig::new("some-other-model");
        cfg.family = ModelFamily::StandardForwardPass;
        let adapter = CactusInferencer::new(cfg);
        assert_eq!(
            adapter.runtime_info().family,
            ModelFamily::StandardForwardPass
        );
    }
}
