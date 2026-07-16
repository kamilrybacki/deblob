//! Ollama adapter (spec §5: "Ollama adapter — Granite 3.1-MoE 1B, Qwen2.5
//! 1.5B-Instruct. OpenAI-compat `/v1/chat/completions` with tools.").
//!
//! A thin wrapper around [`crate::http::HttpInferencer`] (the shared
//! OpenAI-compat core — see [`crate::adapters`]'s module docs) with
//! Ollama's default local base URL and this crate's outer bounded
//! retry-with-backoff layered on top.

use async_trait::async_trait;

use crate::adapters::{classify_with_retry, RetryPolicy};
use crate::contract::{InferenceError, InferenceOutcome, InferenceRequest, SemanticInferencer};
use crate::http::{HttpInferencer, SlmHttpConfig};
use crate::runtime::{Backend, ModelFamily, RuntimeInfo};

/// Ollama's default local OpenAI-compat base URL.
pub const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434/v1";

/// Configuration for [`OllamaInferencer`].
#[derive(Debug, Clone)]
pub struct OllamaConfig {
    /// Base URL of the Ollama OpenAI-compat endpoint, e.g.
    /// `http://localhost:11434/v1`.
    pub base_url: String,
    /// Ollama model tag, e.g. `"granite3.1-moe:1b"` or
    /// `"qwen2.5:1.5b-instruct"`.
    pub model: String,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
    pub retry: RetryPolicy,
}

impl OllamaConfig {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            base_url: DEFAULT_OLLAMA_BASE_URL.to_string(),
            model: model.into(),
            timeout_ms: 30_000,
            max_concurrency: 4,
            retry: RetryPolicy::default(),
        }
    }
}

/// The default `SemanticInferencer` for Ollama-served models (Granite
/// 3.1-MoE 1B, Qwen2.5 1.5B-Instruct). Delegates entirely to an internal
/// [`HttpInferencer`] plus the shared outer retry layer — see
/// [`crate::adapters`]'s module docs.
pub struct OllamaInferencer {
    inner: HttpInferencer,
    retry: RetryPolicy,
    runtime: RuntimeInfo,
}

impl OllamaInferencer {
    pub fn new(cfg: OllamaConfig) -> Self {
        let runtime = RuntimeInfo {
            backend: Backend::Ollama,
            model_id: cfg.model.clone(),
            // Ollama manages quantization opaquely per model tag; not
            // independently observable at this layer without a separate
            // `/api/show` call this adapter does not make.
            quantization: None,
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
impl SemanticInferencer for OllamaInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        classify_with_retry(&self.inner, &req, self.retry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Backend;

    #[test]
    fn runtime_info_reflects_config() {
        let mut cfg = OllamaConfig::new("granite3.1-moe:1b");
        cfg.base_url = "http://ollama.local:11434/v1".to_string();
        let adapter = OllamaInferencer::new(cfg);
        let runtime = adapter.runtime_info();
        assert_eq!(runtime.backend, Backend::Ollama);
        assert_eq!(runtime.model_id, "granite3.1-moe:1b");
        assert_eq!(runtime.endpoint, "http://ollama.local:11434/v1");
        assert_eq!(runtime.family, ModelFamily::StandardForwardPass);
    }
}
