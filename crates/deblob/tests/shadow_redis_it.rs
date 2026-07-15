//! Task 5: `RedisShadowLog` against a REAL Redis (Docker via
//! testcontainers) — proves the `deblob:shadow:<candidate_id>` stream is
//! actually written to via `XADD` and actually bounded via `MAXLEN ~`, the
//! same pattern `deblob-redis`'s own `evidence_it.rs::evidence_stream_
//! trimmed` proves for the evidence stream. Kept intentionally small (300
//! appends, not 1500) — bounded test runtime/host disk, per the task brief.

use deblob::shadow::{RedisShadowLog, ShadowLog};
use deblob_core::id::{CandidateId, SchemaId};
use deblob_slm::Relation;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// A minimal, valid `ShadowDecision` for stream-mechanics tests — the exact
/// field values don't matter here, only that `append` serializes and
/// `XADD`s it.
fn sample_decision(cand_id: &CandidateId, n: u32) -> deblob::shadow::ShadowDecision {
    use deblob::shadow::{EndpointStatus, LiveDisposition, PolicyOutcome};

    deblob::shadow::ShadowDecision {
        decision_id: format!("dec-{n}"),
        cluster_id: cand_id.clone(),
        source_id: "src-a".to_string(),
        observation_count: n as u64,
        observation_window_ms: 1_000,
        canonicalizer_version: "deblob-canon-v1".to_string(),
        monoid_version: "deblob-monoid-v1".to_string(),
        redaction_policy_version: "deblob-slm-redact-v1".to_string(),
        structural_evidence_hash: "deadbeef".to_string(),
        retrieval_algorithm_version: 2,
        retrieved: vec![],
        top1_top2_margin: 0.0,
        candidate_set_hash: "cafebabe".to_string(),
        retrieval_latency_ms: 1,
        prompt_template_version: 1,
        rendered_prompt_hash: "feedface".to_string(),
        model_id: "test-model".to_string(),
        model_digest: None,
        server_runtime_version: None,
        quantization: None,
        temperature: None,
        seed: None,
        max_tokens: None,
        structured_output_backend: None,
        request_tokens: None,
        response_tokens: None,
        raw_model_response: None,
        parsed_decision: None,
        parse_error: None,
        schema_validation_error: None,
        repair_count: 0,
        ttft_ms: None,
        total_latency_ms: 1,
        endpoint_status: EndpointStatus::Available,
        provider_error: None,
        deterministic_compatibility_result: true,
        policy_outcome: PolicyOutcome {
            would_accept: true,
            gate_reasons: vec![],
        },
        counterfactual_live_disposition: LiveDisposition::WouldAcceptMatch {
            schema_id: SchemaId::from_digest(&[1u8; 32]),
            relation: Relation::Exact,
        },
        human_label: None,
        correct_schema_id: None,
        correct_family_id: None,
        correct_relation: None,
        labeler_id: None,
        adjudication_version: None,
        logged_at_ms: 0,
    }
}

#[tokio::test]
async fn append_writes_to_the_per_candidate_stream() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let log = RedisShadowLog::connect(&url).await.unwrap();

    let cand_id = CandidateId::from_digest(&[7u8; 32]);
    log.append(&cand_id, &sample_decision(&cand_id, 1))
        .await
        .unwrap();

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let len: u64 = redis::cmd("XLEN")
        .arg(format!("deblob:shadow:{}", cand_id.as_str()))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 1, "one XADD must produce exactly one stream entry");
}

#[tokio::test]
async fn shadow_stream_trimmed() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let log = RedisShadowLog::connect(&url).await.unwrap();
    let cand_id = CandidateId::from_digest(&[9u8; 32]);

    // Exceeds `SHADOW_STREAM_MAXLEN` (1000) by 200 — same magnitude as
    // `deblob-redis`'s own `evidence_it.rs::evidence_stream_trimmed`
    // (1500 appends against a 1000 cap) — to actually observe the
    // approximate `XTRIM` behavior, not just infer it.
    for n in 0..1200u32 {
        log.append(&cand_id, &sample_decision(&cand_id, n))
            .await
            .unwrap();
    }

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let len: u64 = redis::cmd("XLEN")
        .arg(format!("deblob:shadow:{}", cand_id.as_str()))
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        len < 1200,
        "stream must be trimmed well below the 1200 entries appended, got {len}"
    );
    assert!(
        len >= 1000,
        "approximate MAXLEN trim should not drop below the 1000 cap, got {len}"
    );
}
