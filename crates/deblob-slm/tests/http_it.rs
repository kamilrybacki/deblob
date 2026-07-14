//! Integration tests for `HttpInferencer` against a mocked OpenAI-compatible
//! endpoint (wiremock). No real model is ever contacted.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use deblob_core::id::{FamilyId, SchemaId};
use deblob_monoid::Profile;
use deblob_slm::contract::{
    AbstainCause, CandidateProfileView, FamilyCandidate, InferenceBudget, InferenceDecision,
    InferenceError, InferenceRequest, Relation, SemanticInferencer,
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

    let decision = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::CompatibleDrift,
        }
    );
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

    let decision = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        decision,
        InferenceDecision::Abstain {
            cause: AbstainCause::Ambiguous,
        }
    );

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

    let decision = inferencer.classify(req).await.expect("classify succeeds");

    assert_eq!(
        decision,
        InferenceDecision::MatchSchema {
            schema_id: id,
            relation: Relation::Exact,
        }
    );

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(received.len(), 2, "one repair should have run");
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
        .expect_err("should fail with transport error");
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

    assert_eq!(first, second);

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
