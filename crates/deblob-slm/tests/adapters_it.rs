//! Integration tests for the Task-3 backend adapters (Ollama/llama.cpp/
//! Cactus) against a mocked OpenAI-compatible endpoint (wiremock, already a
//! dev-dependency of this crate via `http_it.rs` — no new mock library
//! added). No real model/backend endpoint is ever contacted.

use deblob_core::id::{FamilyId, SchemaId};
use deblob_fingerprint::{parse_bounded, Limits};
use deblob_monoid::Profile;
use deblob_slm::{
    build_prompt, AbstainCause, Backend, CactusConfig, CactusInferencer, CandidateProfileView,
    FamilyCandidate, InferenceBudget, InferenceDecision, InferenceError, InferenceRequest,
    LlamaCppConfig, LlamaCppInferencer, ModelFamily, Novelty, OllamaConfig, OllamaInferencer,
    Relation, RetryPolicy, SemanticInferencer,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn candidate(id: &SchemaId, rank: u32) -> FamilyCandidate {
    FamilyCandidate {
        family_id: FamilyId::new_v7(),
        schema_id: id.clone(),
        version: 1,
        distance: 0.05,
        rank,
    }
}

fn request(retrieved: Vec<FamilyCandidate>, prompt: String) -> InferenceRequest {
    InferenceRequest {
        candidate: CandidateProfileView::from_profile(&Profile::identity()),
        retrieved,
        contract_version: 1,
        budget: InferenceBudget {
            max_prompt_tokens: 512,
            timeout_ms: 2_000,
        },
        prompt,
    }
}

fn tool_call_response(arguments: &str) -> serde_json::Value {
    json!({
        "choices": [{
            "message": {
                "tool_calls": [{
                    "function": {
                        "name": "submit_semantic_decision",
                        "arguments": arguments
                    }
                }]
            }
        }]
    })
}

// --- Well-formed tool-call responses parse to the correct 3-way decision,
// exercised across the three different adapters (proving the shared core
// behaves identically regardless of which backend wraps it) --------------

#[tokio::test]
async fn ollama_well_formed_match_schema_parses() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[1u8; 32]);
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        id.as_str()
    );
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(&args)))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = OllamaConfig::new("granite3.1-moe:1b");
    cfg.base_url = server.uri();
    let adapter = OllamaInferencer::new(cfg);

    let outcome = adapter
        .classify(request(vec![candidate(&id, 0)], "prompt".to_string()))
        .await
        .expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::Exact,
        }
    );
    assert_eq!(adapter.runtime_info().backend, Backend::Ollama);
    assert_eq!(
        adapter.runtime_info().family,
        ModelFamily::StandardForwardPass
    );
}

#[tokio::test]
async fn llama_cpp_well_formed_new_candidate_parses() {
    let server = MockServer::start().await;
    let args = r#"{"decision":"new_candidate","novelty":"structural"}"#;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(args)))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = LlamaCppConfig::new("functiongemma-270m");
    cfg.base_url = server.uri();
    cfg.quantization = Some("Q4_K_M".to_string());
    let adapter = LlamaCppInferencer::new(cfg);

    let outcome = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await
        .expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::NewCandidate {
            novelty: Novelty::Structural
        }
    );
    assert_eq!(adapter.runtime_info().backend, Backend::LlamaCpp);
    assert_eq!(
        adapter.runtime_info().quantization.as_deref(),
        Some("Q4_K_M")
    );
}

#[tokio::test]
async fn cactus_well_formed_abstain_parses() {
    let server = MockServer::start().await;
    let args = r#"{"decision":"abstain","cause":"ambiguous"}"#;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(args)))
        .expect(1)
        .mount(&server)
        .await;

    let mut cfg = CactusConfig::new("needle-26m");
    cfg.base_url = server.uri();
    let adapter = CactusInferencer::new(cfg);

    let outcome = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await
        .expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous
        }
    );
    // The distinct Needle tag, asserted end-to-end through a real classify()
    // call (not just at construction) — the spec requires the reporter be
    // able to tell this apart from the other (StandardForwardPass) backends.
    assert_eq!(adapter.runtime_info().backend, Backend::Cactus);
    assert_eq!(
        adapter.runtime_info().family,
        ModelFamily::NeedleContinualUpdate
    );
    assert_ne!(
        adapter.runtime_info().family,
        ModelFamily::StandardForwardPass
    );
}

// --- Malformed / non-JSON / missing-tool-call response -> abstain, NEVER
// a spurious match, with the parse-failure flag set for the L2 parse-rate
// metric --------------------------------------------------------------------

#[tokio::test]
async fn malformed_response_is_abstain_not_match_with_parse_error_flagged() {
    let server = MockServer::start().await;
    // No tool_calls in the message at all, on every attempt (including the
    // one internal repair `HttpInferencer` runs) -> unrecoverable malformed.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"choices": [{"message": {}}]})),
        )
        .expect(2)
        .mount(&server)
        .await;

    let mut cfg = OllamaConfig::new("granite3.1-moe:1b");
    cfg.base_url = server.uri();
    let adapter = OllamaInferencer::new(cfg);

    let outcome = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await
        .expect("a malformed-but-recovered-response call still returns Ok(..) with an Abstain");

    assert_eq!(
        outcome.decision,
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous
        },
        "a malformed response must NEVER be reported as a match"
    );
    assert!(
        !outcome.decision.is_accepted_match(),
        "malformed response must not be an accepted match"
    );
    assert!(
        outcome.telemetry.parse_error,
        "the parse failure must be flagged for the L2 parse-rate metric"
    );
}

#[tokio::test]
async fn non_json_body_is_abstain_not_match() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("not json at all")
                .insert_header("content-type", "text/plain"),
        )
        .expect(2)
        .mount(&server)
        .await;

    let mut cfg = LlamaCppConfig::new("functiongemma-270m");
    cfg.base_url = server.uri();
    let adapter = LlamaCppInferencer::new(cfg);

    let outcome = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await
        .expect("classify succeeds with a safe-abstain fallback");

    assert_eq!(
        outcome.decision,
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous
        }
    );
    assert!(!outcome.decision.is_accepted_match());
    assert!(outcome.telemetry.parse_error);
}

// --- Timeout / persistent 5xx -> the outer bounded retry-with-backoff
// runs, then a safe error (never a panic, never a fabricated decision) ------

#[tokio::test]
async fn persistent_5xx_triggers_the_bounded_outer_retry_then_a_safe_error_no_panic() {
    let server = MockServer::start().await;
    // No `.expect(..)` here: the exact count is asserted explicitly below,
    // which gives a clearer failure message than a mount-time panic would.
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let mut cfg = OllamaConfig::new("granite3.1-moe:1b");
    cfg.base_url = server.uri();
    // Fast, deterministic, still bounded: one outer retry on top of
    // HttpInferencer's own one internal repair retry.
    cfg.retry = RetryPolicy::bounded(1, 1, 5);
    let adapter = OllamaInferencer::new(cfg);

    let result = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await;

    assert!(
        matches!(result, Err(InferenceError::Transport(_))),
        "persistent 5xx must surface as InferenceError::Transport after the bounded retries, got {result:?}"
    );

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(
        received.len(),
        4,
        "bounded outer retry (2 outer attempts) x HttpInferencer's own internal \
         repair retry (2 HTTP calls per attempt) = 4 total, never unbounded"
    );
}

#[tokio::test]
async fn timeout_triggers_bounded_retry_then_a_safe_error_no_panic() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(tool_call_response(
                    r#"{"decision":"abstain","cause":"ambiguous"}"#,
                ))
                .set_delay(std::time::Duration::from_millis(300)),
        )
        .mount(&server)
        .await;

    let mut cfg = CactusConfig::new("needle-26m");
    cfg.base_url = server.uri();
    cfg.timeout_ms = 20;
    cfg.retry = RetryPolicy::bounded(1, 1, 5);
    let adapter = CactusInferencer::new(cfg);

    // Must not panic and must not hang; a bound on total wall time is
    // implicit in the small timeout/backoff values above.
    let result = adapter
        .classify(request(vec![], "prompt".to_string()))
        .await;
    assert!(
        matches!(
            result,
            Err(InferenceError::Timeout) | Err(InferenceError::Transport(_))
        ),
        "expected a safe error after bounded retries, got {result:?}"
    );
}

// --- No raw values ever reach the adapter's HTTP request: the prompt is
// rendered through the PII-safe builder, and the adapter forwards it
// verbatim -- reusing the redaction assertion pattern from `prompt.rs` -----

#[tokio::test]
async fn adapter_request_carries_no_raw_values_only_the_redacted_prompt() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(
            r#"{"decision":"abstain","cause":"ambiguous"}"#,
        )))
        .expect(1)
        .mount(&server)
        .await;

    let email = "attacker@evil.example";
    let token = "FAKELEAKCANARY_not_a_real_secret_0123456789ABCDEF";
    let payload = format!(
        r#"{{"contact_email":"{email}","api_token":"{token}","balance":4111111111111111}}"#
    );
    let node = parse_bounded(payload.as_bytes(), &Limits::default()).unwrap();
    let profile = Profile::from_node(&node);
    let view = CandidateProfileView::from_profile(&profile);
    let prompt = build_prompt(&view, &[], &[]).text;
    assert!(
        !prompt.contains(email) && !prompt.contains(token),
        "sanity: the prompt builder itself must not leak (see prompt.rs's own tests)"
    );

    let mut cfg = OllamaConfig::new("granite3.1-moe:1b");
    cfg.base_url = server.uri();
    let adapter = OllamaInferencer::new(cfg);

    adapter
        .classify(request(vec![], prompt))
        .await
        .expect("classify succeeds");

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(received.len(), 1);
    let sent_body = String::from_utf8(received[0].body.clone()).expect("utf8 request body");
    assert!(
        !sent_body.contains(email),
        "raw email leaked into the wire request body: {sent_body}"
    );
    assert!(
        !sent_body.contains(token),
        "raw token leaked into the wire request body: {sent_body}"
    );
    assert!(
        !sent_body.contains("4111111111111111"),
        "raw number leaked into the wire request body: {sent_body}"
    );
    // The field NAMES (redacted, escaped) are expected to appear.
    assert!(sent_body.contains("contact_email"));
}
