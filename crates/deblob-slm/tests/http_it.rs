//! Integration tests for `HttpInferencer` against a mocked OpenAI-compatible
//! endpoint (wiremock). No real model is ever contacted.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use deblob_core::id::{FamilyId, SchemaId};
use deblob_monoid::Profile;
use deblob_slm::contract::{
    AbstainCause, CandidateProfileView, EndpointStatus, FamilyCandidate, InferenceBudget,
    InferenceDecision, InferenceError, InferenceRequest, Relation, SemanticInferencer,
};
use deblob_slm::http::{HttpInferencer, SlmHttpConfig};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn candidate(id: &SchemaId, rank: u32) -> FamilyCandidate {
    FamilyCandidate {
        family_id: FamilyId::new_v7(),
        schema_id: id.clone(),
        version: 1,
        distance: 0.05,
        rank,
    }
}

fn request(retrieved: Vec<FamilyCandidate>) -> InferenceRequest {
    InferenceRequest {
        candidate: CandidateProfileView::from_profile(&Profile::identity()),
        retrieved,
        contract_version: 1,
        budget: InferenceBudget {
            max_prompt_tokens: 512,
            timeout_ms: 2_000,
        },
        prompt: "placeholder prompt (Task 4 builds the real PII-safe prompt)".to_string(),
    }
}

fn cfg(base_url: String) -> SlmHttpConfig {
    SlmHttpConfig {
        base_url,
        model: "test-model".to_string(),
        api_token: None,
        timeout_ms: 2_000,
        max_concurrency: 4,
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

/// Returns a distinct `ResponseTemplate` per call, in order. The last
/// template repeats if the mock is called more times than provided.
struct Sequenced {
    calls: AtomicUsize,
    responses: Vec<ResponseTemplate>,
}

impl Sequenced {
    fn new(responses: Vec<ResponseTemplate>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            responses,
        }
    }
}

impl Respond for Sequenced {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        let idx = self.calls.fetch_add(1, Ordering::SeqCst);
        self.responses.get(idx).cloned().unwrap_or_else(|| {
            self.responses
                .last()
                .expect("at least one response")
                .clone()
        })
    }
}

#[tokio::test]
async fn well_formed_tool_call_parses_to_decision() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[1u8; 32]);
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"compatible_drift"}}"#,
        id.as_str()
    );

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(&args)))
        .expect(1)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&id, 0)]);

    let outcome = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::CompatibleDrift,
        }
    );
    assert_eq!(
        outcome.telemetry.repair_count, 0,
        "no retry ran on a well-formed first response"
    );
    assert_eq!(outcome.telemetry.endpoint_status, EndpointStatus::Ok);
    assert!(!outcome.telemetry.parse_error);
    assert!(!outcome.telemetry.schema_validation_error);
    assert_eq!(outcome.telemetry.model_id.as_deref(), Some("test-model"));
}

#[tokio::test]
async fn id_outside_topk_then_abstain() {
    // IdNotAllowed is a semantic contract violation (schema_id not in allow-list).
    // Per the repair rule, semantic errors must NOT retry — we abstain immediately
    // to avoid a wasted second HTTP call. This test asserts exactly ONE request.
    let server = MockServer::start().await;
    let allowed = SchemaId::from_digest(&[2u8; 32]);
    let outside = SchemaId::from_digest(&[3u8; 32]);

    let first_args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        outside.as_str()
    );

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(tool_call_response(&first_args)))
        .expect(1)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&allowed, 0)]);

    let outcome = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        }
    );
    assert_eq!(
        outcome.telemetry.repair_count, 0,
        "IdNotAllowed must not retry"
    );
    assert!(
        outcome.telemetry.schema_validation_error,
        "an unrecovered IdNotAllowed must flag schema_validation_error"
    );
    assert!(!outcome.telemetry.parse_error);

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(
        received.len(),
        1,
        "IdNotAllowed is semantic, must not retry (saves wasted prefill)"
    );
}

#[tokio::test]
async fn malformed_then_repaired() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[4u8; 32]);
    let good_args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        id.as_str()
    );

    let responder = Sequenced::new(vec![
        // 1st: no tool call at all -> falls back to "null" -> Malformed.
        ResponseTemplate::new(200).set_body_json(json!({"choices": [{"message": {}}]})),
        // 2nd: valid.
        ResponseTemplate::new(200).set_body_json(tool_call_response(&good_args)),
    ]);

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(responder)
        .expect(2)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&id, 0)]);

    let outcome = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        outcome.decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::Exact,
        }
    );
    assert_eq!(
        outcome.telemetry.repair_count, 1,
        "one repair ran and the decision was recovered"
    );
    assert!(
        !outcome.telemetry.parse_error,
        "a successfully repaired decision must not flag parse_error"
    );
    assert!(!outcome.telemetry.schema_validation_error);

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(received.len(), 2, "one repair should have run");
}

/// Task 5b: `repair_count` must reflect exactly one repair for a
/// malformed-then-valid response sequence — the eval harness (Tasks 6-8)
/// computes repair rate from this field.
#[tokio::test]
async fn telemetry_repair_count_reflects_one_repair() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[8u8; 32]);
    let good_args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        id.as_str()
    );

    let responder = Sequenced::new(vec![
        // 1st: no tool call at all -> Malformed.
        ResponseTemplate::new(200).set_body_json(json!({"choices": [{"message": {}}]})),
        // 2nd: valid.
        ResponseTemplate::new(200).set_body_json(tool_call_response(&good_args)),
    ]);

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(responder)
        .expect(2)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&id, 0)]);

    let outcome = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(outcome.telemetry.repair_count, 1);
    assert_eq!(
        outcome.decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::Exact,
        }
    );
}

/// Task 5b: `request_tokens`/`response_tokens` must be populated from an
/// OpenAI-compatible response's `usage` object when the endpoint reports
/// one.
#[tokio::test]
async fn telemetry_tokens_from_usage() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[9u8; 32]);
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        id.as_str()
    );

    let mut body = tool_call_response(&args);
    body["usage"] = json!({"prompt_tokens": 210, "completion_tokens": 14});

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&id, 0)]);

    let outcome = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(outcome.telemetry.request_tokens, Some(210));
    assert_eq!(outcome.telemetry.response_tokens, Some(14));
}

#[tokio::test]
async fn persistent_5xx_is_transport_error() {
    // A persistent HTTP 500 (or any non-2xx status) on every call is a
    // transport-class failure, not a malformed-response case. After one retry,
    // if both attempts return non-2xx, we should surface InferenceError::Transport,
    // NOT silently abstain.
    let server = MockServer::start().await;
    let responder = Sequenced::new(vec![ResponseTemplate::new(500), ResponseTemplate::new(500)]);

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(responder)
        .expect(2)
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let id = SchemaId::from_digest(&[7u8; 32]);
    let req = request(vec![candidate(&id, 0)]);

    let err = inferencer
        .classify(req)
        .await
        .expect_err("should fail with transport error (no InferenceOutcome for a total failure)");
    assert!(
        matches!(err, InferenceError::Transport(_)),
        "expected InferenceError::Transport, got {err:?}"
    );

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(received.len(), 2, "should retry once on non-2xx status");
}

#[tokio::test]
async fn timeout_is_inference_error() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[5u8; 32]);
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        id.as_str()
    );

    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(tool_call_response(&args))
                .set_delay(Duration::from_millis(500)),
        )
        .mount(&server)
        .await;

    let mut inferencer_cfg = cfg(server.uri());
    inferencer_cfg.timeout_ms = 50;
    let inferencer = HttpInferencer::new(inferencer_cfg);
    let req = request(vec![candidate(&id, 0)]);

    let err = inferencer.classify(req).await.expect_err("should time out");
    assert!(
        matches!(err, InferenceError::Timeout | InferenceError::Transport(_)),
        "expected Timeout or Transport, got {err:?}"
    );
}

#[tokio::test]
async fn cache_hit_skips_endpoint() {
    let server = MockServer::start().await;
    let id = SchemaId::from_digest(&[6u8; 32]);
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

    let inferencer = HttpInferencer::new(cfg(server.uri()));
    let req = request(vec![candidate(&id, 0)]);

    let first = inferencer
        .classify(req.clone())
        .await
        .expect("first call succeeds");
    let second = inferencer
        .classify(req)
        .await
        .expect("second call served from cache");

    assert_eq!(first.decision, second.decision);
    assert_eq!(
        second.telemetry.total_latency_ms, None,
        "a cache hit makes no HTTP call, so latency is unobservable"
    );
    assert_eq!(second.telemetry.ttft_ms, None);
    assert_eq!(second.telemetry.repair_count, 0);

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(
        received.len(),
        1,
        "identical request should hit endpoint once"
    );
}

#[test]
fn token_never_logged() {
    let cfg = SlmHttpConfig {
        base_url: "http://example.invalid".to_string(),
        model: "test-model".to_string(),
        api_token: Some("super-secret-token-xyz".to_string()),
        timeout_ms: 1_000,
        max_concurrency: 1,
    };

    let debug_output = format!("{cfg:?}");
    assert!(
        !debug_output.contains("super-secret-token-xyz"),
        "token leaked into Debug output: {debug_output}"
    );
    assert!(debug_output.contains("redacted"));
}
