//! `HttpInferencer`: the default OpenAI-compatible `SemanticInferencer`
//! implementation (spec §Task 2; authoritative shape per
//! `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 2 —
//! structured output").
//!
//! ## Request shape: model-native tool calling (the production default)
//!
//! Every call is a single `/chat/completions` request with:
//! - `temperature: 0` (deterministic decoding).
//! - one REQUIRED tool, `submit_semantic_decision`, whose JSON-schema
//!   `parameters` is the 3-way discriminated union from
//!   [`crate::contract::InferenceDecision`] (`match_schema` /
//!   `new_candidate` / `abstain`), `additionalProperties: false`, enums for
//!   `relation` / `novelty` / `cause`, and `schema_id` constrained to the
//!   exact retrieved top-k ids. No `rationale` or `confidence` field is ever
//!   requested.
//! - `tool_choice` forcing that tool (the model cannot answer in plain
//!   text).
//! - a bounded final-call token budget (`max_tokens` = [`MAX_TOOL_ARG_TOKENS`],
//!   32) — the tool ARGS are the only thing being decoded under this budget.
//!
//! This is "reason free, constrain late" collapsed to its DEFAULT form per
//! the Hermes review: direct tool calling, no separate unconstrained
//! reasoning pass. The review describes an optional five-step variant
//! (monoid stats + candidates → short unconstrained comparison ≤48 tokens or
//! the provider's private reasoning channel → forced tool call → strict
//! decoding on the tool args only → discard/never-log the private
//! reasoning) as an EXPERIMENT, not the production default, and explicitly
//! says not to double-prefill by default. If that two-pass variant is ever
//! implemented, it belongs as an alternate code path here (e.g. a
//! `reasoning_pass: bool` on [`SlmHttpConfig`] or a second `Inferencer`
//! impl) that (1) issues an unconstrained, capped completion first, (2)
//! discards/never logs its content, then (3) proceeds through the same
//! tool-call + validate + repair pipeline below. It is NOT implemented here.
//!
//! ## Parse / validate / repair
//!
//! The tool call's `arguments` are extracted and run through
//! [`crate::contract::validate_decision`] against the request's retrieved
//! top-k ids. Any failure that is mechanically detectable without ground
//! truth — malformed JSON, no tool call in the response, an unknown field,
//! or a `schema_id` outside the top-k — triggers exactly ONE retry of the
//! full request. If the retry's response also fails validation, the call
//! returns a safe [`InferenceDecision::Abstain`] with
//! [`crate::contract::AbstainCause::Ambiguous`] — never an error, and never
//! an attempt to rewrite/guess a corrected decision. A response that is
//! syntactically and contractually valid but simply *wrong* (e.g. names an
//! allowed id but the wrong one) is NOT detectable here — there is no
//! ground truth at inference time — so it passes through as the returned
//! decision; catching that class of error is what the eval harness
//! (Tasks 6-8) and shadow-log wrong-valid tracking (Task 5) are for.
//!
//! A transport/timeout failure — i.e. no HTTP response was ever obtained —
//! is never retried here and never becomes an `Abstain`; it surfaces as
//! [`InferenceError::Timeout`] or [`InferenceError::Transport`], which the
//! caller (the shadow classifier, Task 5) maps to a shadow "unavailable"
//! outcome.
//!
//! ## Decision cache
//!
//! See [`crate::cache`]. An identical `(model, contract_version, retrieved
//! set, prompt)` request is served from the in-memory cache without an HTTP
//! call.
//!
//! ## Prompt (placeholder pending Task 4)
//!
//! This module consumes [`InferenceRequest::prompt`] verbatim as the sole
//! user-message content. It does not build, redact, or validate prompts —
//! that is Task 4's PII-safe prompt builder. Tests here use a fixed
//! placeholder string.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use deblob_core::id::SchemaId;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::cache::{cache_key, DecisionCache};
use crate::contract::{
    validate_decision, AbstainCause, ContractError, EndpointStatus, InferenceDecision,
    InferenceError, InferenceOutcome, InferenceRequest, InferenceTelemetry, SemanticInferencer,
};

/// Tool name the model is required to call.
const TOOL_NAME: &str = "submit_semantic_decision";
/// Bounded final-call token budget for the tool ARGS (constraint-tax
/// avoidance: the schema is small, 32 tokens is generous for `{"decision":
/// "match_schema","schema_id":"sch_...","relation":"compatible_drift"}`).
const MAX_TOOL_ARG_TOKENS: u32 = 32;
/// Default in-memory decision cache capacity.
const DEFAULT_CACHE_CAPACITY: usize = 1024;

/// Token usage extracted from an OpenAI-compatible response's `usage`
/// object, if the endpoint reported one. Not every OpenAI-compatible server
/// includes `usage` — both fields are `None` when absent, never guessed.
#[derive(Debug, Clone, Copy, Default)]
struct CallUsage {
    request_tokens: Option<u32>,
    response_tokens: Option<u32>,
}

/// Result of a single HTTP call attempt, distinguishing parse errors from
/// transport errors so the caller can decide whether to retry. Carries
/// [`CallUsage`] on every branch where a 200 response body was actually
/// parsed (`Success` and `Malformed`) — a malformed tool call can still
/// arrive alongside a populated `usage` object.
#[derive(Debug)]
enum CallResult {
    /// Success: 200 response with a valid tool call.
    Success(String, CallUsage),
    /// 200 response but malformed (no tool call, unparseable JSON).
    Malformed(CallUsage),
    /// Non-2xx HTTP status or network failure (retriable).
    TransportError(String),
}

/// Configuration for [`HttpInferencer`].
///
/// The API token is supplied by the caller — this crate never reads
/// environment variables itself (`DEBLOB_SLM_API_TOKEN` is read at the app
/// layer, per the plan's global constraints). `Debug` redacts the token so
/// it can never leak into logs via `{:?}`.
pub struct SlmHttpConfig {
    /// Base URL of an OpenAI-compatible endpoint, e.g.
    /// `http://localhost:8000/v1`. `HttpInferencer` POSTs to
    /// `{base_url}/chat/completions`.
    pub base_url: String,
    pub model: String,
    /// Bearer token, if the endpoint requires auth. Never logged.
    pub api_token: Option<String>,
    pub timeout_ms: u64,
    pub max_concurrency: usize,
}

impl fmt::Debug for SlmHttpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlmHttpConfig")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_token", &self.api_token.as_ref().map(|_| "<redacted>"))
            .field("timeout_ms", &self.timeout_ms)
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

/// The default `SemanticInferencer`: an OpenAI-compatible `/chat/completions`
/// endpoint driven with forced tool calling.
pub struct HttpInferencer {
    cfg: SlmHttpConfig,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
    cache: DecisionCache,
}

impl HttpInferencer {
    pub fn new(cfg: SlmHttpConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms))
            .build()
            .expect("reqwest client configuration is valid");
        let concurrency = Arc::new(Semaphore::new(cfg.max_concurrency.max(1)));
        Self {
            cfg,
            client,
            concurrency,
            cache: DecisionCache::new(DEFAULT_CACHE_CAPACITY),
        }
    }

    fn endpoint_url(&self) -> String {
        format!(
            "{}/chat/completions",
            self.cfg.base_url.trim_end_matches('/')
        )
    }

    fn tool_parameters_schema(&self, allowed_ids: &[SchemaId]) -> Value {
        let allowed_id_strs: Vec<&str> = allowed_ids.iter().map(SchemaId::as_str).collect();
        json!({
            "type": "object",
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "decision": {"const": "match_schema"},
                        "schema_id": {"type": "string", "enum": allowed_id_strs},
                        "relation": {
                            "type": "string",
                            "enum": ["exact", "compatible_drift", "incompatible_similarity"]
                        }
                    },
                    "required": ["decision", "schema_id", "relation"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": {
                        "decision": {"const": "new_candidate"},
                        "novelty": {"type": "string", "enum": ["structural", "semantic"]}
                    },
                    "required": ["decision", "novelty"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": {
                        "decision": {"const": "abstain"},
                        "cause": {
                            "type": "string",
                            "enum": ["ambiguous", "insufficient_evidence", "candidate_missing"]
                        }
                    },
                    "required": ["decision", "cause"],
                    "additionalProperties": false
                }
            ]
        })
    }

    fn build_body(&self, req: &InferenceRequest, allowed_ids: &[SchemaId]) -> Value {
        json!({
            "model": self.cfg.model,
            "temperature": 0,
            "max_tokens": MAX_TOOL_ARG_TOKENS,
            "tool_choice": {"type": "function", "function": {"name": TOOL_NAME}},
            "tools": [{
                "type": "function",
                "function": {
                    "name": TOOL_NAME,
                    "description": "Submit the 3-way schema-tagging decision for the candidate cluster.",
                    "parameters": self.tool_parameters_schema(allowed_ids),
                }
            }],
            "messages": [
                {
                    "role": "system",
                    "content": "You classify a structural candidate cluster against a retrieved \
                                 top-k of known schemas. Call submit_semantic_decision exactly \
                                 once with your decision. Never invent a schema_id outside the \
                                 provided candidates."
                },
                {"role": "user", "content": req.prompt}
            ]
        })
    }

    /// Issue one HTTP call and extract the tool call's raw `arguments` string.
    ///
    /// Returns `CallResult::Success(arguments)` for a valid 200 response with a tool call.
    /// Returns `CallResult::Malformed` for a 200 response but no/bad tool call.
    /// Returns `CallResult::TransportError(msg)` for non-2xx status or network failure.
    async fn call_once(&self, req: &InferenceRequest, allowed_ids: &[SchemaId]) -> CallResult {
        let body = self.build_body(req, allowed_ids);

        let mut builder = self.client.post(self.endpoint_url()).json(&body);
        if let Some(token) = &self.cfg.api_token {
            builder = builder.bearer_auth(token);
        }

        let response = match builder.send().await {
            Ok(r) => r,
            Err(err) => {
                let msg = if err.is_timeout() {
                    "timeout".to_string()
                } else {
                    format!("send failed: {}", err)
                };
                return CallResult::TransportError(msg);
            }
        };

        // Non-2xx status is a transport-class failure (provider unavailable/error).
        if !response.status().is_success() {
            return CallResult::TransportError(format!("HTTP {}", response.status()));
        }

        let payload: Value = match response.json().await {
            Ok(v) => v,
            Err(_) => return CallResult::Malformed(CallUsage::default()), // 200 but unparseable body
        };

        let usage = CallUsage {
            request_tokens: payload
                .get("usage")
                .and_then(|u| u.get("prompt_tokens"))
                .and_then(Value::as_u64)
                .map(|v| v as u32),
            response_tokens: payload
                .get("usage")
                .and_then(|u| u.get("completion_tokens"))
                .and_then(Value::as_u64)
                .map(|v| v as u32),
        };

        let arguments = payload
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("tool_calls"))
            .and_then(|tc| tc.get(0))
            .and_then(|tc| tc.get("function"))
            .and_then(|f| f.get("arguments"))
            .and_then(|a| a.as_str())
            .map(str::to_string);

        match arguments {
            Some(args) => CallResult::Success(args, usage),
            None => CallResult::Malformed(usage),
        }
    }
}

#[async_trait]
impl SemanticInferencer for HttpInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        let allowed_ids: Vec<SchemaId> =
            req.retrieved.iter().map(|c| c.schema_id.clone()).collect();

        let key = cache_key(
            &self.cfg.model,
            req.contract_version,
            &req.retrieved,
            &req.prompt,
        );
        if let Some(cached) = self.cache.get(&key) {
            // Minimal "cached" telemetry: no HTTP call was made, so latency
            // and token counts are unobservable here (not `0` — genuinely
            // unknown for this call). `repair_count: 0` because no retry
            // logic ran either.
            return Ok(InferenceOutcome {
                decision: cached,
                telemetry: InferenceTelemetry {
                    request_tokens: None,
                    response_tokens: None,
                    ttft_ms: None,
                    total_latency_ms: None,
                    repair_count: 0,
                    endpoint_status: EndpointStatus::Ok,
                    parse_error: false,
                    schema_validation_error: false,
                    model_id: Some(self.cfg.model.clone()),
                },
            });
        }

        let _permit = self
            .concurrency
            .acquire()
            .await
            .map_err(|err| InferenceError::Transport(err.to_string()))?;

        let call_started = Instant::now();

        // Telemetry accumulated across the (at most two) call_once attempts
        // below. `repair_count`/`parse_error`/`schema_validation_error`
        // reflect the FINAL fate of this classify() call, not every
        // transient error observed along the way — see
        // `InferenceTelemetry`'s field docs.
        let mut repair_count: u32 = 0;
        let mut parse_error = false;
        let mut schema_validation_error = false;
        // No default: every code path below either assigns `usage` from a
        // response it actually received, or returns early on a total
        // transport failure (never reaching the telemetry construction that
        // reads `usage`) — the compiler's definite-assignment check enforces
        // this invariant instead of a placeholder default that would always
        // be silently overwritten.
        let usage: CallUsage;

        // First call attempt. Process based on result type.
        let first_result = self.call_once(&req, &allowed_ids).await;
        let decision = match first_result {
            CallResult::Success(args, first_usage) => {
                // Successful response: validate the decision and handle errors.
                match validate_decision(&args, &allowed_ids) {
                    Ok(decision) => {
                        usage = first_usage;
                        decision
                    }
                    Err(err) => {
                        // Validation failed. Branch on error kind:
                        // - IdNotAllowed: semantic error, no retry (saves prefill).
                        // - Malformed/UnknownField: retriable syntax errors.
                        match err {
                            ContractError::IdNotAllowed => {
                                usage = first_usage;
                                schema_validation_error = true;
                                InferenceDecision::Abstain {
                                    cause: AbstainCause::Ambiguous,
                                }
                            }
                            ContractError::Malformed(_) | ContractError::UnknownField => {
                                // Retry the full call once. `first_usage` is
                                // intentionally discarded here — `usage` is
                                // always overwritten by the retry's own
                                // result below, whichever branch it takes.
                                repair_count = 1;
                                match self.call_once(&req, &allowed_ids).await {
                                    CallResult::Success(second_args, second_usage) => {
                                        usage = second_usage;
                                        match validate_decision(&second_args, &allowed_ids) {
                                            Ok(d) => d,
                                            Err(retry_err) => {
                                                mark_unrecovered(
                                                    &retry_err,
                                                    &mut parse_error,
                                                    &mut schema_validation_error,
                                                );
                                                InferenceDecision::Abstain {
                                                    cause: AbstainCause::Ambiguous,
                                                }
                                            }
                                        }
                                    }
                                    CallResult::Malformed(second_usage) => {
                                        usage = second_usage;
                                        parse_error = true;
                                        InferenceDecision::Abstain {
                                            cause: AbstainCause::Ambiguous,
                                        }
                                    }
                                    CallResult::TransportError(msg) => {
                                        return Err(InferenceError::Transport(msg));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            CallResult::Malformed(_first_usage) => {
                // 200 response but malformed body. Retry once. `_first_usage`
                // is unused: this branch always retries, and `usage` is
                // always overwritten by the retry's own result below.
                repair_count = 1;
                match self.call_once(&req, &allowed_ids).await {
                    CallResult::Success(second_args, second_usage) => {
                        usage = second_usage;
                        match validate_decision(&second_args, &allowed_ids) {
                            Ok(d) => d,
                            Err(retry_err) => {
                                mark_unrecovered(
                                    &retry_err,
                                    &mut parse_error,
                                    &mut schema_validation_error,
                                );
                                InferenceDecision::Abstain {
                                    cause: AbstainCause::Ambiguous,
                                }
                            }
                        }
                    }
                    CallResult::Malformed(second_usage) => {
                        usage = second_usage;
                        parse_error = true;
                        InferenceDecision::Abstain {
                            cause: AbstainCause::Ambiguous,
                        }
                    }
                    CallResult::TransportError(msg) => {
                        return Err(InferenceError::Transport(msg));
                    }
                }
            }
            CallResult::TransportError(msg) => {
                // Non-2xx status or network failure. Retry once.
                repair_count = 1;
                match self.call_once(&req, &allowed_ids).await {
                    CallResult::Success(second_args, second_usage) => {
                        usage = second_usage;
                        match validate_decision(&second_args, &allowed_ids) {
                            Ok(d) => d,
                            Err(retry_err) => {
                                mark_unrecovered(
                                    &retry_err,
                                    &mut parse_error,
                                    &mut schema_validation_error,
                                );
                                InferenceDecision::Abstain {
                                    cause: AbstainCause::Ambiguous,
                                }
                            }
                        }
                    }
                    CallResult::Malformed(second_usage) => {
                        usage = second_usage;
                        parse_error = true;
                        InferenceDecision::Abstain {
                            cause: AbstainCause::Ambiguous,
                        }
                    }
                    CallResult::TransportError(second_msg) => {
                        // Both attempts failed with transport errors.
                        return Err(InferenceError::Transport(format!(
                            "first: {}; retry: {}",
                            msg, second_msg
                        )));
                    }
                }
            }
        };

        // Only reached once we have a usable decision — the endpoint
        // answered (possibly after one repair), so `endpoint_status` is
        // always `Ok` here; see `EndpointStatus`'s docs.
        let total_latency_ms = call_started.elapsed().as_millis() as u64;
        let telemetry = InferenceTelemetry {
            request_tokens: usage.request_tokens,
            response_tokens: usage.response_tokens,
            // No streaming support in this client — TTFT is approximated
            // by the full call latency (see `InferenceTelemetry::ttft_ms`'s
            // docs).
            ttft_ms: Some(total_latency_ms),
            total_latency_ms: Some(total_latency_ms),
            repair_count,
            endpoint_status: EndpointStatus::Ok,
            parse_error,
            schema_validation_error,
            model_id: Some(self.cfg.model.clone()),
        };

        self.cache.put(key, decision.clone());
        Ok(InferenceOutcome {
            decision,
            telemetry,
        })
    }
}

/// Sets the appropriate telemetry flag for a [`ContractError`] observed on
/// the SECOND (repair) attempt that still failed to validate — the decision
/// is falling back to `Abstain` and will not be retried again (only one
/// repair ever runs).
fn mark_unrecovered(
    err: &ContractError,
    parse_error: &mut bool,
    schema_validation_error: &mut bool,
) {
    match err {
        ContractError::IdNotAllowed => *schema_validation_error = true,
        ContractError::Malformed(_) | ContractError::UnknownField => *parse_error = true,
    }
}
