//! `RedisModelRegistry` against a REAL Redis (Docker via testcontainers) —
//! mirrors `crates/deblob-redis/tests/registry_it.rs`'s setup. Proves the
//! governed, gated model-promotion invariant (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §4):
//!
//!   - a candidate with `false_merge_rate > 0` is NEVER promoted (audited
//!     `Rejected`, active pointer untouched);
//!   - a candidate that passes the gate AND improves on the current active
//!     IS promoted;
//!   - a candidate WORSE than the current active is rejected even though it
//!     independently passes the gate's own thresholds;
//!   - `rollback` restores the prior active, and the promoted-then-rolled-
//!     back model is marked `RolledBack`.

use deblob::model_registry::{
    EvalMetricsSummary, GoLiveGate, ModelRegistry, ModelState, ModelVersion, PromotionOutcome,
    RedisModelRegistry,
};
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

async fn connect() -> (
    RedisModelRegistry,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let registry = RedisModelRegistry::connect(&url).await.unwrap();
    (registry, node)
}

fn metrics(
    false_merge_rate: Option<f64>,
    wrong_valid_rate: f64,
    accepted_precision: f64,
    exact_semantic_accuracy: f64,
) -> EvalMetricsSummary {
    EvalMetricsSummary {
        total_cases: 200,
        false_merge_rate,
        wrong_valid_rate,
        accepted_precision,
        exact_semantic_accuracy,
    }
}

fn version(id: &str, eval_metrics: EvalMetricsSummary, recorded_at: i64) -> ModelVersion {
    ModelVersion {
        model_id: id.to_string(),
        digest: format!("sha256:{id}"),
        trained_from: "feedback+synthetic seed".to_string(),
        eval_metrics,
        recorded_at,
        state: ModelState::Candidate,
    }
}

#[tokio::test]
async fn a_candidate_with_any_false_merge_is_never_promoted() {
    let (registry, _node) = connect().await;
    let gate = GoLiveGate::default();

    let candidate = version(
        "model-false-merge",
        metrics(Some(0.001), 0.001, 0.999, 0.95),
        1000,
    );
    registry
        .register_candidate(candidate.clone())
        .await
        .unwrap();

    let outcome = registry.promote_if_gated(candidate, &gate).await.unwrap();

    match outcome {
        PromotionOutcome::Rejected { reasons, candidate } => {
            assert!(
                reasons.iter().any(|r| r.contains("false_merge_rate")),
                "expected a false-merge gate reason, got {reasons:?}"
            );
            assert_eq!(candidate.state, ModelState::Rejected);
        }
        other => panic!("a false-merging candidate must never be Promoted, got {other:?}"),
    }

    assert!(
        registry.get_active().await.unwrap().is_none(),
        "the active pointer must remain untouched by a rejected promotion"
    );

    // Audited: the rejection is visible in history with the Rejected state.
    let history = registry.history().await.unwrap();
    let recorded = history
        .iter()
        .find(|v| v.model_id == "model-false-merge")
        .expect("rejected candidate must still be recorded in history (audited)");
    assert_eq!(recorded.state, ModelState::Rejected);
}

#[tokio::test]
async fn a_candidate_that_passes_the_gate_and_improves_is_promoted() {
    let (registry, _node) = connect().await;
    let gate = GoLiveGate::default();

    // No active model yet: a gate-passing candidate is promoted
    // unconditionally (nothing to regress against).
    let first = version("model-first", metrics(Some(0.0), 0.001, 0.999, 0.9), 1000);
    registry.register_candidate(first.clone()).await.unwrap();
    let outcome = registry.promote_if_gated(first, &gate).await.unwrap();
    assert!(matches!(outcome, PromotionOutcome::Promoted(_)));
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-first"
    );

    // A second candidate that BOTH passes the gate and improves on the
    // first must be promoted, displacing the first as active.
    let second = version(
        "model-second",
        metrics(Some(0.0), 0.0005, 0.9995, 0.95),
        2000,
    );
    registry.register_candidate(second.clone()).await.unwrap();
    let outcome = registry.promote_if_gated(second, &gate).await.unwrap();
    match outcome {
        PromotionOutcome::Promoted(v) => assert_eq!(v.model_id, "model-second"),
        other => panic!("expected Promoted, got {other:?}"),
    }
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-second"
    );
}

#[tokio::test]
async fn a_candidate_worse_than_the_active_model_is_rejected_even_if_it_passes_the_gate_alone() {
    let (registry, _node) = connect().await;
    let gate = GoLiveGate::default();

    let active = version("model-active", metrics(Some(0.0), 0.001, 0.999, 0.9), 1000);
    registry.register_candidate(active.clone()).await.unwrap();
    let outcome = registry.promote_if_gated(active, &gate).await.unwrap();
    assert!(matches!(outcome, PromotionOutcome::Promoted(_)));

    // This candidate independently PASSES the go-live gate thresholds
    // (wrong_valid_rate/accepted_precision both comfortably inside the
    // default bounds) but has LOWER exact_semantic_accuracy than the
    // active model — it must still be rejected as a regression.
    let worse = version("model-worse", metrics(Some(0.0), 0.001, 0.999, 0.5), 2000);
    registry.register_candidate(worse.clone()).await.unwrap();
    let outcome = registry.promote_if_gated(worse, &gate).await.unwrap();

    match outcome {
        PromotionOutcome::Rejected { reasons, .. } => {
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("regresses exact_semantic_accuracy")),
                "expected a regression reason, got {reasons:?}"
            );
        }
        other => panic!("a regressing candidate must never be Promoted, got {other:?}"),
    }

    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-active",
        "the worse candidate must never displace the still-active better model"
    );
}

#[tokio::test]
async fn rollback_restores_the_prior_active_and_marks_the_current_one_rolled_back() {
    let (registry, _node) = connect().await;
    let gate = GoLiveGate::default();

    let first = version(
        "model-rb-first",
        metrics(Some(0.0), 0.001, 0.999, 0.9),
        1000,
    );
    registry.register_candidate(first.clone()).await.unwrap();
    registry.promote_if_gated(first, &gate).await.unwrap();

    let second = version(
        "model-rb-second",
        metrics(Some(0.0), 0.0005, 0.9995, 0.95),
        2000,
    );
    registry.register_candidate(second.clone()).await.unwrap();
    let outcome = registry.promote_if_gated(second, &gate).await.unwrap();
    assert!(matches!(outcome, PromotionOutcome::Promoted(_)));
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-rb-second"
    );

    let restored = registry.rollback("ops:kamil").await.unwrap();
    assert_eq!(restored.model_id, "model-rb-first");
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-rb-first",
        "rollback must restore the prior active model as the current active"
    );

    let history = registry.history().await.unwrap();
    let rolled_back = history
        .iter()
        .find(|v| v.model_id == "model-rb-second")
        .unwrap();
    assert_eq!(
        rolled_back.state,
        ModelState::RolledBack,
        "the superseded-then-rolled-back model must be marked RolledBack"
    );
}

#[tokio::test]
async fn rollback_without_a_prior_active_is_a_conflict() {
    let (registry, _node) = connect().await;
    let gate = GoLiveGate::default();

    let only = version("model-solo", metrics(Some(0.0), 0.001, 0.999, 0.9), 1000);
    registry.register_candidate(only.clone()).await.unwrap();
    registry.promote_if_gated(only, &gate).await.unwrap();

    let err = registry.rollback("ops:kamil").await.unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
}

#[tokio::test]
async fn registering_the_same_model_id_twice_is_rejected() {
    let (registry, _node) = connect().await;
    let v = version("dup-model", metrics(Some(0.0), 0.001, 0.999, 0.9), 1000);
    registry.register_candidate(v.clone()).await.unwrap();
    let err = registry.register_candidate(v).await.unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
}
