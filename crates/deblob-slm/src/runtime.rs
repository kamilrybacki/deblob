//! Runtime/backend metadata for a [`crate::contract::SemanticInferencer`]
//! implementation (spec `2026-07-16-deblob-experiment.md` §5: "record
//! runtime as part of the composite bundle" — "the experiment/report needs
//! which runtime produced a result").
//!
//! This module does NOT touch [`crate::contract::SemanticInferencer`] or
//! [`crate::contract::InferenceTelemetry`] — both are depended on by
//! product crates (`deblob`, `deblob-eval`) and other trait implementors
//! (test mocks in `deblob-experiment`), so widening either would ripple
//! into files this task is explicitly not scoped to touch. Instead,
//! [`RuntimeInfo`] is exposed as an inherent `runtime_info()` accessor on
//! each concrete adapter (see [`crate::adapters`]), and [`ModelBundle`] is
//! an optional, purely additive pairing a caller MAY use to carry an
//! `Arc<dyn SemanticInferencer>` and its `RuntimeInfo` together (e.g. the
//! model-roster wiring a later task builds).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::contract::SemanticInferencer;

/// Which backend served a model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    /// Ollama's OpenAI-compatible `/v1/chat/completions` endpoint.
    Ollama,
    /// A `llama.cpp` server's OpenAI-compatible endpoint (GGUF models).
    LlamaCpp,
    /// `cactus serve`'s OpenAI-compatible endpoint.
    Cactus,
}

impl Backend {
    pub fn label(self) -> &'static str {
        match self {
            Backend::Ollama => "ollama",
            Backend::LlamaCpp => "llama.cpp",
            Backend::Cactus => "cactus",
        }
    }
}

/// The inference METHOD a model uses to produce a decision — distinct from
/// [`Backend`] (the transport/server). Spec §5 requires Needle be labeled
/// distinctly: "LABEL Needle's method/runtime DISTINCTLY (its update path
/// is not the same as the others) — surface a family/method tag so the
/// reporter never conflates it." Every roster model EXCEPT Needle (served
/// via [`Backend::Cactus`]) is a plain, static forward-pass classifier
/// behind the shared OpenAI-compat tool-calling contract; Needle's serving
/// path (`cactus serve`) fronts a continual/on-device-adapting model, so a
/// downstream reporter must never lump its numbers in with the static
/// forward-pass models as if the "method" producing them were identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelFamily {
    /// A static, non-adapting forward-pass classifier (Granite/Qwen/
    /// FunctionGemma) — the OpenAI-compat tool-call contract's default
    /// assumption.
    StandardForwardPass,
    /// Needle's continual/on-device update path (`cactus serve`) — NOT a
    /// plain static forward pass. Kept as its own variant (rather than a
    /// bool on [`Backend::Cactus`]) so a future non-Needle model served via
    /// Cactus is not silently mislabeled.
    NeedleContinualUpdate,
}

/// Runtime metadata for one [`crate::contract::SemanticInferencer`]
/// implementation — "which runtime produced a result" (spec §5). Every
/// concrete adapter in [`crate::adapters`] exposes one of these via
/// `runtime_info()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub backend: Backend,
    /// The model id as configured for this adapter (e.g.
    /// `"granite3.1-moe:1b"`, `"qwen2.5:1.5b-instruct"`,
    /// `"functiongemma-270m-q4_k_m"`, `"needle-26m"`).
    pub model_id: String,
    /// Quantization scheme, if known/configured (e.g. `"Q4_K_M"`). `None`
    /// when the backend manages quantization opaquely (e.g. an Ollama
    /// model tag whose quant isn't independently observable here) or when
    /// running unquantized.
    pub quantization: Option<String>,
    /// The OpenAI-compatible base URL this adapter was configured against.
    pub endpoint: String,
    /// See [`ModelFamily`].
    pub family: ModelFamily,
}

/// An `Arc<dyn SemanticInferencer>` paired with the [`RuntimeInfo`] that
/// describes it — the "composite model bundle" spec §5 asks for so a
/// report can say which runtime produced which result. Purely additive:
/// nothing in this crate or `deblob-experiment` is required to construct
/// one; it exists as the seam a model-roster wiring (a later task) plugs
/// into.
#[derive(Clone)]
pub struct ModelBundle {
    pub inferencer: Arc<dyn SemanticInferencer>,
    pub runtime: RuntimeInfo,
}

impl ModelBundle {
    pub fn new(inferencer: Arc<dyn SemanticInferencer>, runtime: RuntimeInfo) -> Self {
        Self {
            inferencer,
            runtime,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_labels_are_stable() {
        assert_eq!(Backend::Ollama.label(), "ollama");
        assert_eq!(Backend::LlamaCpp.label(), "llama.cpp");
        assert_eq!(Backend::Cactus.label(), "cactus");
    }

    #[test]
    fn family_serializes_snake_case_and_distinguishes_needle() {
        assert_eq!(
            serde_json::to_string(&ModelFamily::StandardForwardPass).unwrap(),
            "\"standard_forward_pass\""
        );
        assert_eq!(
            serde_json::to_string(&ModelFamily::NeedleContinualUpdate).unwrap(),
            "\"needle_continual_update\""
        );
        assert_ne!(
            ModelFamily::StandardForwardPass,
            ModelFamily::NeedleContinualUpdate
        );
    }
}
