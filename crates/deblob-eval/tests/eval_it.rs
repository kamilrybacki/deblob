//! End-to-end self-test of the eval harness (P2-A/B Task 8) against a
//! wiremock-mocked OpenAI-compatible endpoint. NO REAL MODEL IS EVER
//! CONTACTED — this proves `load_corpus` → `run_eval` → `compute_metrics`
//! → `report` compose correctly and produce the metrics a scripted set of
//! decisions predicts, so CI can gate on the whole pipeline without a live
//! endpoint.
//!
//! ## Scripting strategy
//!
//! The mock responds to every request with the CORRECT
//! (`expected.decision`) answer for that corpus case, taken straight from
//! the loaded golden corpus — EXCEPT three deliberate, hand-picked
//! deviations chosen to exercise the harness's headline failure-mode
//! counters with known, hand-computed answers:
//!
//! 1. `known_exact_basic` (not a merge/split trap): the mock answers with
//!    the CORRECT `schema_id` but the WRONG `relation` (`compatible_drift`
//!    instead of `exact`). Schema-valid, semantically wrong, same family
//!    → exercises `wrong_valid` WITHOUT touching `false_merge`.
//! 2. `drift_plausible_wrong_high_freq_family` (`false_merge_trap: true`):
//!    the mock ACCEPTS a match to the "popular" distractor family (rank 1)
//!    instead of the gold family (rank 2) → the ONE deliberately scripted
//!    false merge.
//! 3. `drift_renamed_fields_snake_camel` (`false_split_trap: true`): the
//!    mock calls `new_candidate` instead of accepting the match it should
//!    → the ONE deliberately scripted false split.
//!
//! Every other case (22 of 25) echoes the corpus's own `expected.decision`
//! verbatim, so the harness's baseline is "correct" and the three
//! deviations above are the ENTIRE source of every non-trivial count
//! below. The request is matched to a case by the exact (ordered) list of
//! `schema_id`s offered in the tool-call schema's `enum` — each corpus
//! case's retrieved top-k is a distinct ordered sequence (verified by a
//! `#[test]` in this file), so this is a reliable, non-fragile key.
//!
//! Every number asserted below was hand/script-computed directly from
//! `crates/deblob-eval/corpus/*.json` (see the Task 8 report for the
//! derivation) — this test is the proof that the harness reproduces them.

use std::collections::HashMap;

use deblob_eval::{compute_metrics, load_corpus, report, run_eval};
use deblob_slm::{HttpInferencer, InferenceDecision, Relation, SlmHttpConfig};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn corpus_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// The exact, ordered list of `schema_id`s a case's `retrieved` top-k
/// offers the model — used both as the scripted-response lookup key and
/// (mirrored inside `HttpInferencer::tool_parameters_schema`) as the tool
/// call's `schema_id` enum, in the same order. Two cases only collide on
/// this key if they retrieve the identical schema_id SEQUENCE (not just
/// the same set) — see `retrieval_key_is_unique_per_case_or_shares_the_same_expected_answer`.
fn retrieval_key(retrieved: &[deblob_slm::FamilyCandidate]) -> String {
    retrieved
        .iter()
        .map(|c| c.schema_id.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn extract_allowed_ids(body: &Value) -> Vec<String> {
    body.pointer("/tools/0/function/parameters/oneOf/0/properties/schema_id/enum")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Applies the three deliberate deviations described in the module docs;
/// every other case answers with its own `expected.decision`.
fn scripted_decision_for(case: &deblob_eval::EvalCase) -> InferenceDecision {
    match case.name.as_str() {
        "known_exact_basic" => {
            let schema_id = case.retrieved[0].schema_id.clone();
            InferenceDecision::MatchSchema {
                schema_id,
                relation: Relation::CompatibleDrift, // wrong relation; expected is Exact.
            }
        }
        "drift_plausible_wrong_high_freq_family" => {
            let wrong_family = case
                .retrieved
                .iter()
                .find(|c| c.rank == 1) // rank 1 = the "popular" distractor, NOT gold (rank 2).
                .expect("case has a rank-1 candidate")
                .schema_id
                .clone();
            InferenceDecision::MatchSchema {
                schema_id: wrong_family,
                relation: Relation::CompatibleDrift,
            }
        }
        "drift_renamed_fields_snake_camel" => InferenceDecision::NewCandidate {
            novelty: deblob_slm::Novelty::Structural,
        },
        _ => case.expected.decision.clone(),
    }
}

struct ScriptedResponder {
    /// retrieval_key -> tool-call `arguments` JSON string.
    script: HashMap<String, String>,
}

impl Respond for ScriptedResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let body: Value = request.body_json().expect("request body is JSON");
        let allowed_ids = extract_allowed_ids(&body);
        let key = allowed_ids.join(",");
        let arguments = self
            .script
            .get(&key)
            .unwrap_or_else(|| panic!("no scripted response for retrieval key {key:?}"));

        ResponseTemplate::new(200).set_body_json(json!({
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "submit_semantic_decision",
                            "arguments": arguments
                        }
                    }]
                }
            }],
            // Fixed, deterministic usage on every call (no repairs are
            // scripted, so every case makes exactly one call) — lets this
            // test also assert exact avg_request_tokens/avg_response_tokens.
            "usage": {"prompt_tokens": 100, "completion_tokens": 10}
        }))
    }
}

/// Sanity check on the scripting strategy itself (not the harness): every
/// corpus case's retrieval key is either unique, or shared only with
/// another case that expects the identical answer (the two
/// order-permutation cases, which intentionally retrieve the same TWO ids
/// in swapped rank order — a different ordered sequence, hence a different
/// key in practice, but this test tolerates a same-key collision too as
/// long as the scripted answer would agree).
#[test]
fn retrieval_key_is_unique_per_case_or_shares_the_same_expected_answer() {
    let corpus = load_corpus(corpus_dir()).expect("seed corpus loads");
    let mut seen: HashMap<String, &deblob_eval::EvalCase> = HashMap::new();
    for case in &corpus {
        let key = retrieval_key(&case.retrieved);
        if let Some(prior) = seen.get(&key) {
            assert_eq!(
                scripted_decision_for(prior),
                scripted_decision_for(case),
                "cases {:?} and {:?} share retrieval key {key:?} but would get \
                 different scripted answers",
                prior.name,
                case.name
            );
        } else {
            seen.insert(key, case);
        }
    }
}

fn build_script(corpus: &[deblob_eval::EvalCase]) -> HashMap<String, String> {
    corpus
        .iter()
        .map(|case| {
            let key = retrieval_key(&case.retrieved);
            let decision = scripted_decision_for(case);
            let arguments = serde_json::to_string(&decision).expect("InferenceDecision serializes");
            (key, arguments)
        })
        .collect()
}

#[tokio::test]
async fn full_harness_pipeline_matches_hand_computed_metrics() {
    let corpus = load_corpus(corpus_dir()).expect("seed corpus loads");
    assert_eq!(corpus.len(), 25, "this test's hand-computed numbers assume the 25-case seed corpus; update both if the corpus grows");

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ScriptedResponder {
            script: build_script(&corpus),
        })
        .mount(&server)
        .await;

    let inferencer = HttpInferencer::new(SlmHttpConfig {
        base_url: server.uri(),
        model: "eval-self-test-model".to_string(),
        api_token: None,
        timeout_ms: 5_000,
        max_concurrency: 8,
    });

    // --- Full pipeline: load_corpus (above) -> run_eval -> compute_metrics -> report ---
    let run = run_eval(&inferencer, &corpus).await;
    let metrics = compute_metrics(&run, &corpus);
    let (human, json) = report(&metrics);

    // Every request actually hit the mock endpoint (no repairs were
    // scripted, no case's request is identical to another's, so no
    // decision-cache hit either) — proves this is truly end-to-end, not a
    // silently-skipped pipeline.
    let received = server.received_requests().await.expect("requests recorded");
    assert_eq!(
        received.len(),
        25,
        "expected exactly one HTTP call per corpus case (no repairs, no cache hits scripted)"
    );

    // --- Parse / schema-valid: every scripted response is well-formed. ---
    assert_eq!(metrics.total_cases, 25);
    assert_eq!(metrics.json_parse_rate, 1.0);
    assert_eq!(metrics.schema_valid_rate, 1.0);
    assert_eq!(metrics.id_constraint_violations, 0);
    assert_eq!(metrics.timeout_rate, 0.0);
    assert_eq!(metrics.provider_error_rate, 0.0);
    assert_eq!(metrics.malformed_rate, 0.0);
    assert_eq!(
        metrics.repair_rate, 0.0,
        "nothing was scripted to need repair"
    );
    assert_eq!(metrics.repair_success_rate, None);

    // --- The headline: wrong-valid, tracked apart from schema-valid. ---
    // 3 deliberate deviations, all schema-valid-but-wrong, out of 25.
    assert_eq!(metrics.wrong_valid_count, 3);
    assert!((metrics.wrong_valid_rate - 3.0 / 25.0).abs() < 1e-9);
    assert!((metrics.exact_semantic_accuracy - 22.0 / 25.0).abs() < 1e-9);
    // Only the false-split deviation changes the decision KIND
    // (match_schema -> new_candidate); the other two keep the same kind.
    assert!((metrics.decision_choice_accuracy - 24.0 / 25.0).abs() < 1e-9);

    // --- The hard gate: false-merge, tracked apart from generic wrong-valid. ---
    // 5 false_merge_trap cases in the seed corpus; exactly 1 (the
    // plausible-but-wrong-family deviation) is an ACCEPTED wrong-family
    // match. The other 4 correctly echo a non-accepted expected answer
    // (incompatible_similarity x3, abstain x1) and so are NOT false merges.
    assert_eq!(metrics.false_merge_trap_count, 5);
    assert_eq!(metrics.false_merge_count, 1);
    assert_eq!(metrics.false_merge_rate, Some(0.2));

    // --- False-split, tracked apart from false-merge. ---
    // 6 false_split_trap cases; exactly 1 (the new_candidate deviation)
    // fails to accept the match it should have.
    assert_eq!(metrics.false_split_trap_count, 6);
    assert_eq!(metrics.false_split_count, 1);
    assert!((metrics.false_split_rate.unwrap() - 1.0 / 6.0).abs() < 1e-9);

    // --- Abstention: all 7 abstain-expected cases echo correctly. ---
    assert_eq!(metrics.abstention_precision, Some(1.0));
    assert_eq!(metrics.abstention_recall, Some(1.0));

    // --- Retrieval quality (recall@k / MRR): pure function of the
    // corpus's own `expected.gold_rank`, independent of what the mock
    // answered — see `crate::metrics::compute_metrics`'s docs. 13 of 25
    // cases carry a gold schema id; gold appears at rank 1 in 9, rank <=3
    // in 12, rank <=5 in 12 (no case retrieves more than 3), and one
    // gold-bearing case has no rank at all (the mandatory gold-absent
    // case, MRR contribution 0).
    assert!((metrics.recall_at_1.unwrap() - 9.0 / 13.0).abs() < 1e-9);
    assert!((metrics.recall_at_3.unwrap() - 12.0 / 13.0).abs() < 1e-9);
    assert!((metrics.recall_at_5.unwrap() - 12.0 / 13.0).abs() < 1e-9);
    let expected_mrr = (9.0 * 1.0 + 2.0 * 0.5 + 1.0 * (1.0 / 3.0) + 0.0) / 13.0;
    assert!((metrics.mrr.unwrap() - expected_mrr).abs() < 1e-9);

    // --- Novel family: 3 legitimate new_family cases (all echoed
    // correctly) plus 1 deviation that ALSO called new_candidate (but was
    // wrong to) --------------------------------------------------------
    assert_eq!(metrics.novel_family_recall, Some(1.0));
    assert!((metrics.novel_family_precision.unwrap() - 0.75).abs() < 1e-9);

    // --- Gold-absent abstention: the one mandatory case, echoed correctly. ---
    assert_eq!(metrics.gold_absent_abstention_rate, Some(1.0));

    // --- Tokens: fixed usage on every one of the 25 single-call cases. ---
    assert_eq!(metrics.avg_request_tokens, Some(100.0));
    assert_eq!(metrics.avg_response_tokens, Some(10.0));
    // Every case made a real (mocked) HTTP call — no case was served from
    // the decision cache.
    assert_eq!(metrics.cache_hit_rate, Some(0.0));

    // --- Human report surfaces the headline figures. ---
    assert!(human.contains("Wrong-valid rate:"));
    assert!(
        human.contains("12.00%"),
        "wrong-valid = 3/25 = 12.00%:\n{human}"
    );
    assert!(human.contains("False-merge rate:"));
    assert!(
        human.contains("20.00%"),
        "false-merge = 1/5 = 20.00%:\n{human}"
    );
    assert!(human.contains("False-split rate:"));
    assert!(
        human.contains("16.67%"),
        "false-split = 1/6 = 16.67%:\n{human}"
    );

    // --- Machine (JSON) report round-trips the same numbers. ---
    assert_eq!(json["total_cases"], serde_json::json!(25));
    assert_eq!(json["wrong_valid_count"], serde_json::json!(3));
    assert_eq!(json["false_merge_count"], serde_json::json!(1));
    assert_eq!(json["false_merge_rate"], serde_json::json!(0.2));
    assert_eq!(json["false_split_count"], serde_json::json!(1));
}
