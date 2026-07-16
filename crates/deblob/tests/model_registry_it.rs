//! `RedisModelRegistry` against a REAL Redis (Docker via testcontainers) —
//! mirrors `crates/deblob-redis/tests/registry_it.rs`'s setup. Proves the
//! governed, gated model-promotion invariants (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §4,
//! §B6-B8, §B11):
//!
//!   - a candidate with `false_merge_count > 0` is NEVER promoted (audited
//!     `Rejected`, active pointer untouched);
//!   - `attach_evidence` alone NEVER produces `Active` — only
//!     `ShadowCandidate` (pass) or `Rejected` (fail) — spec §B7/§B11;
//!   - `promote` is a SEPARATE action: it requires the candidate to
//!     already be `ShadowCandidate`, an attached evidence bundle, and (per
//!     `GateConfig::require_explicit_approval`) an explicit approval —
//!     without approval it is refused even though the candidate already
//!     passed its offline gate;
//!   - a candidate WORSE than the current active is rejected by
//!     `attach_evidence` even though it independently passes the gate's
//!     own thresholds;
//!   - `rollback` restores the prior active's WHOLE artifact bundle (spec
//!     §B8) — not just a digest — and marks the superseded model
//!     `RolledBack`.

use deblob::model_registry::{
    ArtifactBundle, FamilySlice, GateConfig, GateDecision, GateEvidence, ModelRegistry, ModelState,
    ModelVersion, PromotionApproval, TrainedFrom,
};
use deblob_core::id::FamilyId;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

async fn connect() -> (
    deblob::model_registry::RedisModelRegistry,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let registry = deblob::model_registry::RedisModelRegistry::connect(&url)
        .await
        .unwrap();
    (registry, node)
}

/// Permissive gate for IT plumbing tests — the gate MATH itself is
/// exhaustively unit-tested in `deblob::model_registry`; these tests only
/// need it to actually pass/fail predictably over a tiny synthetic
/// evidence bundle.
fn test_gate() -> GateConfig {
    GateConfig {
        min_test_n: 1,
        per_family_min_n: 1,
        per_family_precision_floor: 0.0,
        max_false_merge_upper_ci: 1.0,
        min_shadow_hold_ms: 0,
        ..GateConfig::default()
    }
}

fn evidence(
    false_merge_count: usize,
    false_merge_trap_count: usize,
    wrong_valid_rate: f64,
    accepted_precision: f64,
    exact_semantic_accuracy: f64,
) -> GateEvidence {
    GateEvidence {
        aggregate: deblob::model_registry::EvalMetricsSummary {
            total_cases: 200,
            false_merge_rate: if false_merge_trap_count > 0 {
                Some(false_merge_count as f64 / false_merge_trap_count as f64)
            } else {
                None
            },
            false_merge_count,
            false_merge_trap_count,
            wrong_valid_rate,
            accepted_precision,
            exact_semantic_accuracy,
            oracle_retrieval_exact_accuracy: Some(exact_semantic_accuracy),
            retrieval_recall_at_5: Some(0.99),
        },
        per_family: vec![],
        false_merge_upper_ci: if false_merge_trap_count > 0 {
            Some(false_merge_count as f64 / false_merge_trap_count as f64)
        } else {
            None
        },
        computed_at: 0,
    }
}

fn bundle(weights_digest: &str) -> ArtifactBundle {
    ArtifactBundle {
        weights_digest: weights_digest.to_string(),
        tokenizer: "tok-v1".to_string(),
        prompt_template_version: "prompt-v1".to_string(),
        runtime: "vllm-0.9".to_string(),
        quantization: "int8".to_string(),
        retrieval_index_version: "idx-v1".to_string(),
        grammar: "grammar-v1".to_string(),
        catalog: "catalog-v1".to_string(),
    }
}

fn version(id: &str, recorded_at: i64) -> ModelVersion {
    ModelVersion {
        model_id: id.to_string(),
        bundle: bundle(&format!("sha256:quant-{id}")),
        training_checkpoint_digest: format!("sha256:ckpt-{id}"),
        trained_from: TrainedFrom {
            base_snapshot_id: "base-snapshot-v0".to_string(),
            feedback_cursor: "feedback_examples=0".to_string(),
            corpus_seed: "synthetic_train_cases=1 synthetic_holdout_cases=1".to_string(),
        },
        evidence: None,
        recorded_at,
        shadow_since: None,
        state: ModelState::Candidate,
    }
}

fn approved(actor: &str) -> PromotionApproval {
    PromotionApproval {
        approved: true,
        actor: actor.to_string(),
    }
}

#[tokio::test]
async fn a_candidate_with_any_false_merge_is_never_promoted() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let candidate = version("model-false-merge", 1000);
    registry
        .register_candidate(candidate.clone())
        .await
        .unwrap();

    let decision = registry
        .attach_evidence(
            "model-false-merge",
            evidence(1, 200, 0.001, 0.999, 0.95),
            &gate,
        )
        .await
        .unwrap();

    match decision {
        GateDecision::Rejected { reasons, candidate } => {
            assert!(
                reasons.iter().any(|r| r.contains("false_merge_count")),
                "expected a false-merge gate reason, got {reasons:?}"
            );
            assert_eq!(candidate.state, ModelState::Rejected);
        }
        other => {
            panic!("a false-merging candidate must never enter ShadowCandidate, got {other:?}")
        }
    }

    assert!(
        registry.get_active().await.unwrap().is_none(),
        "the active pointer must remain untouched by a rejected evidence attachment"
    );

    // Audited: the rejection is visible in history with the Rejected state.
    let history = registry.history().await.unwrap();
    let recorded = history
        .iter()
        .find(|v| v.model_id == "model-false-merge")
        .expect("rejected candidate must still be recorded in history (audited)");
    assert_eq!(recorded.state, ModelState::Rejected);
}

/// Spec §B7/§B11 — the headline separation-of-duties + two-stage-canary
/// invariant: `attach_evidence` passing the offline gate produces
/// `ShadowCandidate`, NEVER `Active`. Only a SEPARATE, explicitly
/// approved `promote` call ever moves the active alias.
#[tokio::test]
async fn attach_evidence_passing_the_gate_yields_shadow_candidate_never_directly_active() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let candidate = version("model-shadow", 1000);
    registry.register_candidate(candidate).await.unwrap();

    let decision = registry
        .attach_evidence("model-shadow", evidence(0, 200, 0.001, 0.999, 0.95), &gate)
        .await
        .unwrap();

    match decision {
        GateDecision::EnteredShadow(v) => {
            assert_eq!(v.state, ModelState::ShadowCandidate);
            assert!(v.evidence.is_some(), "evidence must be attached");
            assert!(v.shadow_since.is_some());
        }
        other => panic!("expected EnteredShadow, got {other:?}"),
    }

    assert!(
        registry.get_active().await.unwrap().is_none(),
        "passing the offline gate alone must never activate the candidate"
    );
    let stored = registry.get("model-shadow").await.unwrap().unwrap();
    assert_eq!(stored.state, ModelState::ShadowCandidate);

    // The SEPARATE, explicitly-approved promote call is what actually
    // moves the alias.
    let promoted = registry
        .promote("model-shadow", approved("ops:kamil"), &gate)
        .await
        .unwrap();
    assert_eq!(promoted.state, ModelState::Active);
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-shadow"
    );
}

/// Spec §B7: `promote` requires an explicit approval when
/// `GateConfig::require_explicit_approval` is set — even for a candidate
/// that already passed the offline gate and is sitting in
/// `ShadowCandidate`.
#[tokio::test]
async fn promote_without_explicit_approval_is_refused_even_after_the_gate_passed() {
    let (registry, _node) = connect().await;
    let gate = test_gate();
    assert!(gate.require_explicit_approval);

    let candidate = version("model-needs-approval", 1000);
    registry.register_candidate(candidate).await.unwrap();
    let decision = registry
        .attach_evidence(
            "model-needs-approval",
            evidence(0, 200, 0.001, 0.999, 0.95),
            &gate,
        )
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));

    let err = registry
        .promote(
            "model-needs-approval",
            PromotionApproval {
                approved: false,
                actor: "ops:kamil".to_string(),
            },
            &gate,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        deblob_core::error::CoreError::PolicyRejected(_)
    ));
    assert!(
        registry.get_active().await.unwrap().is_none(),
        "an unapproved promote must never move the active alias"
    );
}

/// Spec §B7: `promote` refuses a candidate that never went through
/// `attach_evidence` at all — it must still be `Candidate`, not
/// `ShadowCandidate`.
#[tokio::test]
async fn promote_refuses_a_bare_candidate_that_never_passed_attach_evidence() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let candidate = version("model-bare", 1000);
    registry.register_candidate(candidate).await.unwrap();

    let err = registry
        .promote("model-bare", approved("ops:kamil"), &gate)
        .await
        .unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
    assert!(registry.get_active().await.unwrap().is_none());
}

#[tokio::test]
async fn a_candidate_that_passes_the_gate_and_improves_is_promoted() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    // No active model yet: a gate-passing candidate reaches
    // ShadowCandidate unconditionally (nothing to regress against), then
    // is explicitly promoted.
    let first = version("model-first", 1000);
    registry.register_candidate(first).await.unwrap();
    let decision = registry
        .attach_evidence("model-first", evidence(0, 200, 0.001, 0.999, 0.9), &gate)
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));
    registry
        .promote("model-first", approved("ops:kamil"), &gate)
        .await
        .unwrap();
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-first"
    );

    // A second candidate that BOTH passes the gate and improves on the
    // first must reach ShadowCandidate and then, on promote, displace the
    // first as active.
    let second = version("model-second", 2000);
    registry.register_candidate(second).await.unwrap();
    let decision = registry
        .attach_evidence(
            "model-second",
            evidence(0, 200, 0.0005, 0.9995, 0.95),
            &gate,
        )
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));
    let promoted = registry
        .promote("model-second", approved("ops:kamil"), &gate)
        .await
        .unwrap();
    assert_eq!(promoted.model_id, "model-second");
    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-second"
    );
}

#[tokio::test]
async fn a_candidate_worse_than_the_active_model_is_rejected_even_if_it_passes_the_gate_alone() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let active = version("model-active", 1000);
    registry.register_candidate(active).await.unwrap();
    let decision = registry
        .attach_evidence("model-active", evidence(0, 200, 0.001, 0.999, 0.9), &gate)
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));
    registry
        .promote("model-active", approved("ops:kamil"), &gate)
        .await
        .unwrap();

    // This candidate independently PASSES the go-live gate thresholds
    // (wrong_valid_rate/accepted_precision both comfortably inside the
    // default bounds) but has LOWER exact_semantic_accuracy than the
    // active model — it must still be rejected as a regression.
    let worse = version("model-worse", 2000);
    registry.register_candidate(worse).await.unwrap();
    let decision = registry
        .attach_evidence("model-worse", evidence(0, 200, 0.001, 0.999, 0.5), &gate)
        .await
        .unwrap();

    match decision {
        GateDecision::Rejected { reasons, .. } => {
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("regresses exact_semantic_accuracy")),
                "expected a regression reason, got {reasons:?}"
            );
        }
        other => panic!("a regressing candidate must never enter ShadowCandidate, got {other:?}"),
    }

    assert_eq!(
        registry.get_active().await.unwrap().unwrap().model_id,
        "model-active",
        "the worse candidate must never displace the still-active better model"
    );
}

/// Spec §B6 acceptance: a candidate that passes every AGGREGATE number
/// but regresses a per-family slice below the floor (with sufficient N)
/// must be rejected by `attach_evidence`.
#[tokio::test]
async fn a_candidate_passing_aggregate_but_failing_a_per_family_slice_is_rejected() {
    let (registry, _node) = connect().await;
    let gate = GateConfig {
        per_family_min_n: 10,
        per_family_precision_floor: 0.99,
        min_test_n: 1,
        max_false_merge_upper_ci: 1.0,
        min_shadow_hold_ms: 0,
        ..GateConfig::default()
    };

    let candidate = version("model-slice-bad", 1000);
    registry.register_candidate(candidate).await.unwrap();

    let mut ev = evidence(0, 0, 0.001, 0.999, 0.95); // strong aggregate
    ev.per_family = vec![FamilySlice {
        family_id: FamilyId::new_v7(),
        n: 50,
        correct: 20, // 40% precision, well under the floor, plenty of N
        precision: 0.4,
    }];

    let decision = registry
        .attach_evidence("model-slice-bad", ev, &gate)
        .await
        .unwrap();
    match decision {
        GateDecision::Rejected { reasons, .. } => {
            assert!(
                reasons.iter().any(|r| r.contains("family")),
                "expected a per-family rejection reason, got {reasons:?}"
            );
        }
        other => panic!("expected Rejected on a bad per-family slice, got {other:?}"),
    }
}

/// Spec §B6 acceptance: a candidate below `min_test_n` is inconclusive —
/// not promotable — even with otherwise-perfect metrics.
#[tokio::test]
async fn a_candidate_below_min_test_n_is_inconclusive_not_promotable() {
    let (registry, _node) = connect().await;
    let gate = GateConfig {
        min_test_n: 500,
        max_false_merge_upper_ci: 1.0,
        per_family_min_n: 1,
        per_family_precision_floor: 0.0,
        min_shadow_hold_ms: 0,
        ..GateConfig::default()
    };

    let candidate = version("model-too-small", 1000);
    registry.register_candidate(candidate).await.unwrap();
    let mut ev = evidence(0, 0, 0.0, 1.0, 1.0);
    ev.aggregate.total_cases = 50; // below min_test_n

    let decision = registry
        .attach_evidence("model-too-small", ev, &gate)
        .await
        .unwrap();
    match decision {
        GateDecision::Rejected { reasons, candidate } => {
            assert!(
                reasons.iter().any(|r| r.contains("INCONCLUSIVE")),
                "expected an inconclusive reason, got {reasons:?}"
            );
            assert_eq!(candidate.state, ModelState::Rejected);
        }
        other => panic!("expected Rejected (inconclusive), got {other:?}"),
    }
}

/// Spec §B8 acceptance: `rollback` restores the WHOLE artifact bundle
/// (weights + tokenizer + prompt template + runtime + quantization +
/// retrieval index + grammar + catalog) — not just a weights digest.
#[tokio::test]
async fn rollback_restores_the_whole_composite_artifact_bundle() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let mut first = version("model-rb-first", 1000);
    first.bundle = ArtifactBundle {
        weights_digest: "sha256:quant-v1".to_string(),
        tokenizer: "tok-v1".to_string(),
        prompt_template_version: "prompt-v1".to_string(),
        runtime: "vllm-0.9".to_string(),
        quantization: "int8".to_string(),
        retrieval_index_version: "idx-v1".to_string(),
        grammar: "grammar-v1".to_string(),
        catalog: "catalog-v1".to_string(),
    };
    registry.register_candidate(first.clone()).await.unwrap();
    let decision = registry
        .attach_evidence("model-rb-first", evidence(0, 200, 0.001, 0.999, 0.9), &gate)
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));
    registry
        .promote("model-rb-first", approved("ops:kamil"), &gate)
        .await
        .unwrap();

    let mut second = version("model-rb-second", 2000);
    second.bundle = ArtifactBundle {
        weights_digest: "sha256:quant-v2".to_string(),
        tokenizer: "tok-v2".to_string(), // a DIFFERENT tokenizer than v1
        prompt_template_version: "prompt-v2".to_string(),
        runtime: "vllm-0.10".to_string(),
        quantization: "int4".to_string(),
        retrieval_index_version: "idx-v2".to_string(),
        grammar: "grammar-v2".to_string(),
        catalog: "catalog-v2".to_string(),
    };
    registry.register_candidate(second.clone()).await.unwrap();
    let decision = registry
        .attach_evidence(
            "model-rb-second",
            evidence(0, 200, 0.0005, 0.9995, 0.95),
            &gate,
        )
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));
    registry
        .promote("model-rb-second", approved("ops:kamil"), &gate)
        .await
        .unwrap();
    assert_eq!(
        registry
            .get_active()
            .await
            .unwrap()
            .unwrap()
            .bundle
            .tokenizer,
        "tok-v2"
    );

    let restored = registry.rollback("ops:kamil").await.unwrap();
    assert_eq!(restored.model_id, "model-rb-first");
    assert_eq!(
        restored.bundle, first.bundle,
        "rollback must restore the WHOLE prior bundle, byte for byte"
    );

    let active = registry.get_active().await.unwrap().unwrap();
    assert_eq!(active.model_id, "model-rb-first");
    assert_eq!(
        active.bundle, first.bundle,
        "the restored active model's bundle must match the original v1 bundle exactly \
         (tokenizer/prompt_template_version/runtime/quantization/retrieval_index_version/\
         grammar/catalog all restored together, not just the weights digest)"
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
    let gate = test_gate();

    let only = version("model-solo", 1000);
    registry.register_candidate(only).await.unwrap();
    registry
        .attach_evidence("model-solo", evidence(0, 200, 0.001, 0.999, 0.9), &gate)
        .await
        .unwrap();
    registry
        .promote("model-solo", approved("ops:kamil"), &gate)
        .await
        .unwrap();

    let err = registry.rollback("ops:kamil").await.unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
}

#[tokio::test]
async fn registering_the_same_model_id_twice_is_rejected() {
    let (registry, _node) = connect().await;
    let v = version("dup-model", 1000);
    registry.register_candidate(v.clone()).await.unwrap();
    let err = registry.register_candidate(v).await.unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
}

/// Spec §B7: evidence is attached exactly once — a second
/// `attach_evidence` call against an already-decided (non-`Candidate`)
/// model is refused.
#[tokio::test]
async fn attach_evidence_twice_on_the_same_candidate_is_a_conflict() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let candidate = version("model-double-evidence", 1000);
    registry.register_candidate(candidate).await.unwrap();
    registry
        .attach_evidence(
            "model-double-evidence",
            evidence(0, 200, 0.001, 0.999, 0.9),
            &gate,
        )
        .await
        .unwrap();

    let err = registry
        .attach_evidence(
            "model-double-evidence",
            evidence(0, 200, 0.001, 0.999, 0.9),
            &gate,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, deblob_core::error::CoreError::Conflict(_)));
}

/// Spec §B7 separation of duties, made STRUCTURAL: `register_candidate`
/// must FORCE the persisted version to a bare `Candidate` (`evidence:
/// None`, `shadow_since: None`) even when the caller passes a version
/// that's already marked `ShadowCandidate` with forged evidence attached.
/// Without this, a caller could register a version that already looks
/// gate-passed and call `promote` directly, bypassing `attach_evidence`
/// (and the whole statistical gate) entirely.
#[tokio::test]
async fn register_candidate_forces_a_bare_candidate_even_if_caller_passes_forged_evidence() {
    let (registry, _node) = connect().await;
    let gate = test_gate();

    let mut forged = version("model-forged", 1000);
    forged.state = ModelState::ShadowCandidate;
    forged.evidence = Some(evidence(0, 200, 0.001, 0.999, 0.99));
    forged.shadow_since = Some(1000);

    registry.register_candidate(forged).await.unwrap();

    let stored = registry.get("model-forged").await.unwrap().unwrap();
    assert_eq!(
        stored.state,
        ModelState::Candidate,
        "register_candidate must force the persisted state to Candidate regardless of \
         what the caller passed in"
    );
    assert!(
        stored.evidence.is_none(),
        "register_candidate must strip any forged evidence the caller passed in"
    );
    assert!(
        stored.shadow_since.is_none(),
        "register_candidate must clear any forged shadow_since the caller passed in"
    );

    // A forced-bare candidate must still go through attach_evidence before
    // promote is even eligible — promote must refuse it exactly like any
    // other bare Candidate, proving the forged fields never took effect.
    let err = registry
        .promote("model-forged", approved("ops:kamil"), &gate)
        .await
        .unwrap_err();
    assert!(
        matches!(err, deblob_core::error::CoreError::Conflict(_)),
        "a forced-bare candidate must be refused by promote (not ShadowCandidate), got {err:?}"
    );
    assert!(registry.get_active().await.unwrap().is_none());
}

/// Spec §B11: `promote` must reject a candidate whose
/// `GateConfig::min_shadow_hold_ms` has not yet elapsed since
/// `shadow_since`. Every other test in this suite uses
/// `min_shadow_hold_ms: 0` (no hold), so the `elapsed < hold` rejection
/// branch was previously never exercised.
///
/// `model_registry.rs`'s internal `now_ms()` reads the real wall clock
/// with no injectable test seam (no `Clock`/`MockClock` abstraction exists
/// anywhere in this crate), so this test asserts ONLY the rejection
/// branch — deterministically, not via a race: `attach_evidence` sets
/// `shadow_since` to "now" and `promote` is called immediately after with
/// an hour-long hold, so `elapsed < hold` is true by construction with no
/// wall-clock flakiness.
#[tokio::test]
async fn promote_is_rejected_before_the_shadow_hold_elapses() {
    let (registry, _node) = connect().await;
    let gate = GateConfig {
        min_test_n: 1,
        per_family_min_n: 1,
        per_family_precision_floor: 0.0,
        max_false_merge_upper_ci: 1.0,
        min_shadow_hold_ms: 3_600_000, // 1 hour — cannot elapse within a test run
        ..GateConfig::default()
    };

    let candidate = version("model-shadow-hold", 1000);
    registry.register_candidate(candidate).await.unwrap();
    let decision = registry
        .attach_evidence(
            "model-shadow-hold",
            evidence(0, 200, 0.001, 0.999, 0.95),
            &gate,
        )
        .await
        .unwrap();
    assert!(matches!(decision, GateDecision::EnteredShadow(_)));

    let err = registry
        .promote("model-shadow-hold", approved("ops:kamil"), &gate)
        .await
        .unwrap_err();
    match err {
        deblob_core::error::CoreError::PolicyRejected(msg) => {
            assert!(
                msg.contains("shadow hold"),
                "expected a shadow-hold rejection message, got {msg:?}"
            );
        }
        other => panic!("expected PolicyRejected on an unelapsed shadow hold, got {other:?}"),
    }
    assert!(
        registry.get_active().await.unwrap().is_none(),
        "a promote rejected for an unelapsed shadow hold must never move the active alias"
    );
}
