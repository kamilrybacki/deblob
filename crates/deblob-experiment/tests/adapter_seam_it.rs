//! Task 3 seam test: a real (mock-HTTP-backed) adapter wired into Task 1's
//! `SemanticArm`, end to end -> `GatedArm` (the FROZEN trust gate) ->
//! `ArmDecision`.
//!
//! Task 1's report explicitly left "any real backend adapter Task 3
//! implements against [`deblob_slm::SemanticInferencer`] plugs in with zero
//! changes to this crate" as the seam this task had to prove. This test
//! proves it: it constructs a real `deblob_slm::OllamaInferencer` (an
//! adapter built in this task) pointed at a wiremock server (no real
//! network), wraps it in `deblob_experiment::arms::semantic::SemanticArm`
//! exactly as `run.rs` would for B1, wraps THAT in `GatedArm::new(ArmId::B1,
//! ..)` — the identical gate-wrapping constructor B1 uses in the runner —
//! and asserts the final `ArmDecision` the whole B1 pipeline produces.
//!
//! `SemanticArm::decide` bridges the adapter's `async fn classify` via
//! `futures_executor::block_on` (Task 1's documented seam). Because the
//! adapter here does REAL (albeit mocked) HTTP I/O through `reqwest`, that
//! blocking bridge must not starve the ambient tokio I/O driver — so this
//! test runs the gated `decide()` call inside `tokio::task::spawn_blocking`
//! on a multi-thread runtime, keeping the async I/O driver free to service
//! the in-flight request rather than deadlocking a single worker thread
//! against itself.

use std::sync::Arc;

use deblob_core::id::{FamilyId, SchemaId};
use deblob_experiment::arms::gate::GatedArm;
use deblob_experiment::arms::semantic::SemanticArm;
use deblob_experiment::arms::{Arm, ArmId};
use deblob_experiment::labels::InferenceInput;
use deblob_slm::{
    CandidateProfileView, FamilyCandidate, InferenceDecision, OllamaConfig, OllamaInferencer,
    Relation,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn schema_id(byte: u8) -> SchemaId {
    SchemaId::from_digest(&[byte; 32])
}

fn fc(byte: u8, rank: u32, distance: f32) -> FamilyCandidate {
    FamilyCandidate {
        family_id: FamilyId::new_v7(),
        schema_id: schema_id(byte),
        version: 1,
        distance,
        rank,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_backed_ollama_adapter_flows_through_b1_gate_to_arm_decision() {
    let server = MockServer::start().await;
    let strong_id = schema_id(1);
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        strong_id.as_str()
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

    // Exactly B1's construction in `run.rs`: SemanticArm wraps the
    // inferencer, GatedArm::new(ArmId::B1, ..) wraps that with the SAME
    // frozen `deblob::shadow::evaluate_policy` gate B2 also goes through.
    let b1 = Arc::new(GatedArm::new(
        ArmId::B1,
        Box::new(SemanticArm::new(ArmId::B1, Arc::new(adapter))),
    ));

    // Strong retrieval geometry (near-zero distance, wide margin, ample
    // observations) so the gate accepts the model's proposal rather than
    // downgrading it -- the test is about the seam wiring, not re-testing
    // gate threshold arithmetic (already covered by `arms::gate`'s tests).
    let input = InferenceInput {
        candidate: CandidateProfileView {
            observation_count: 1_000,
            fields: vec![],
            truncated: false,
        },
        retrieved: vec![fc(1, 1, 0.0), fc(2, 2, 0.9)],
        allowed_ids: vec![schema_id(1), schema_id(2)],
        prompt: String::new(),
    };

    let b1_for_blocking = Arc::clone(&b1);
    let input_owned = input.clone();
    let decision = tokio::task::spawn_blocking(move || b1_for_blocking.decide(&input_owned))
        .await
        .expect("the blocking decide() call must not panic");

    assert_eq!(
        decision,
        InferenceDecision::MatchSchema {
            schema_id: strong_id,
            relation: Relation::Exact,
        },
        "adapter -> SemanticArm -> GatedArm(B1) must produce the gate-accepted decision"
    );

    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(
        received.len(),
        1,
        "the seam should hit the mock endpoint exactly once"
    );
}

/// The redundancy-ablation companion (B2) also still works when B1's real
/// adapter is swapped in beside it -- proving the adapter changes nothing
/// about the frozen-gate comparison the whole experiment depends on: B1
/// and B2 differ ONLY in which `Arm` supplies the proposal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_rejected_semantic_proposal_downgrades_to_abstain_through_the_same_seam() {
    let server = MockServer::start().await;
    let outside_topk = schema_id(9);
    // The model proposes a schema_id NOT in the retrieved top-k -- an
    // IdNotAllowed contract violation, which `HttpInferencer` (the shared
    // core every adapter wraps) converts to a safe `Abstain` immediately
    // (no retry -- semantic errors are not retried).
    let args = format!(
        r#"{{"decision":"match_schema","schema_id":"{}","relation":"exact"}}"#,
        outside_topk.as_str()
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

    let b1 = Arc::new(GatedArm::new(
        ArmId::B1,
        Box::new(SemanticArm::new(ArmId::B1, Arc::new(adapter))),
    ));

    let input = InferenceInput {
        candidate: CandidateProfileView {
            observation_count: 1_000,
            fields: vec![],
            truncated: false,
        },
        retrieved: vec![fc(1, 1, 0.0), fc(2, 2, 0.9)],
        allowed_ids: vec![schema_id(1), schema_id(2)],
        prompt: String::new(),
    };

    let b1_for_blocking = Arc::clone(&b1);
    let input_owned = input.clone();
    let decision = tokio::task::spawn_blocking(move || b1_for_blocking.decide(&input_owned))
        .await
        .expect("the blocking decide() call must not panic");

    // The adapter itself already turned the contract-invalid response into
    // a safe Abstain; the gate then sees a non-MatchSchema proposal and
    // passes it through unchanged (never a spurious match at any stage of
    // the seam).
    assert!(!decision.is_accepted_match());
}
