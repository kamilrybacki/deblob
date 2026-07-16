//! `llama.cpp` server adapter (spec §5: "llama.cpp adapter — FunctionGemma
//! 270M (GGUF), optionally Qwen. llama.cpp server OpenAI-compat
//! `/v1/chat/completions` (+ its function-calling/grammar mode). May share
//! most of the Ollama client — factor a common OpenAI-compat core, thin
//! per-backend differences.").
//!
//! Same shared core as [`crate::adapters::ollama`] — see
//! [`crate::adapters`]'s module docs — differing only in default base URL
//! and the presence of an explicit `quantization` field (GGUF quantization,
//! e.g. `"Q4_K_M"`, is a first-class, caller-supplied fact for this
//! backend, unlike Ollama's opaque model-tag management).

use async_trait::async_trait;

use crate::adapters::{classify_with_retry, RetryPolicy};
use crate::contract::{InferenceError, InferenceOutcome, InferenceRequest, SemanticInferencer};
use crate::http::{HttpInferencer, SlmHttpConfig};
use crate::runtime::{Backend, ModelFamily, RuntimeInfo};

/// `llama.cpp server`'s default local OpenAI-compat base URL.
pub const DEFAULT_LLAMA_CPP_BASE_URL: &str = "http://localhost:8080/v1";

/// Configuration for [`LlamaCppInferencer`].
#[derive(Debug, Clone)]
pub struct LlamaCppConfig {
    pub base_url: String,
    /// Model id as the `llama.cpp` server reports/expects it, e.g.
    /// `"functiongemma-270m"`.
    pub model: String,
    /// GGUF quantization scheme, e.g. `Some("Q4_K_M".to_string())`. `None`
    /// if unquantized or unknown.
    pub quantization: Option<String>,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
    pub retry: RetryPolicy,
}

impl LlamaCppConfig {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            base_url: DEFAULT_LLAMA_CPP_BASE_URL.to_string(),
            model: model.into(),
            quantization: None,
            timeout_ms: 30_000,
            max_concurrency: 4,
            retry: RetryPolicy::default(),
        }
    }
}

/// The `SemanticInferencer` for a `llama.cpp server`-hosted GGUF model
/// (FunctionGemma 270M; optionally Qwen). Delegates entirely to an internal
/// [`HttpInferencer`] plus the shared outer retry layer.
pub struct LlamaCppInferencer {
    inner: HttpInferencer,
    retry: RetryPolicy,
    runtime: RuntimeInfo,
}

impl LlamaCppInferencer {
    pub fn new(cfg: LlamaCppConfig) -> Self {
        let runtime = RuntimeInfo {
            backend: Backend::LlamaCpp,
            model_id: cfg.model.clone(),
            quantization: cfg.quantization.clone(),
            endpoint: cfg.base_url.clone(),
            family: ModelFamily::StandardForwardPass,
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
impl SemanticInferencer for LlamaCppInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        classify_with_retry(&self.inner, &req, self.retry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_info_carries_quantization() {
        let mut cfg = LlamaCppConfig::new("functiongemma-270m");
        cfg.quantization = Some("Q4_K_M".to_string());
        let adapter = LlamaCppInferencer::new(cfg);
        let runtime = adapter.runtime_info();
        assert_eq!(runtime.backend, Backend::LlamaCpp);
        assert_eq!(runtime.quantization.as_deref(), Some("Q4_K_M"));
        assert_eq!(runtime.family, ModelFamily::StandardForwardPass);
    }
}
