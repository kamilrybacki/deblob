//! Arm C prequential loop against REAL Redis (Docker via testcontainers) —
//! mirrors `crates/deblob/tests/model_registry_it.rs`'s setup. Proves:
//!
//! - data -> submit -> artifact -> eval -> gate -> promote runs end-to-end
//!   with `FakeBackend`: a candidate with a false-merge is REJECTED, a
//!   clean candidate enters `ShadowCandidate` — NEVER directly `Active`;
//! - separation of duties: nothing in the fine-tune hook / the
//!   `PrequentialRunner` loop ever moves the active alias, even across
//!   multiple rounds, verified against a REAL `RedisModelRegistry`.
//!
//! The gate here is deliberately permissive on every ABLATABLE threshold
//! (`min_test_n`/per-family floor/`wrong_valid_rate`/`accepted_precision`)
//! so the test isolates the ONE UNCONDITIONAL axis that must never be
//! weakened: `false_merge_count > 0` always fails, regardless of every
//! other threshold (`deblob::model_registry::gate_reasons`, reused
//! verbatim, never touched by this crate). `AlwaysWrongFamily` reliably
//! trips it: `deblob_eval::generate_corpus` assigns ~20% of families to the
//! `Partition::Test` holdout (spec: "never split siblings"), and every
//! family carries exactly one `Category::IncompatibleUnsafe` variant with
//! `expected.false_merge_trap = true` — so a dev corpus with at least 5-6
//! families is guaranteed at least one such held-out case.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use deblob::model_registry::{BundleTemplate, GateConfig, ModelRegistry, RedisModelRegistry};
use deblob::retrain::CurationConfig;
use deblob_core::id::SchemaId;
use deblob_experiment::continual::{
    Budget, BudgetPolicy, FakeBackend, FixedJobParams, LoraParams, PrequentialConfig,
    PrequentialRunner, TrainingBackendFineTuneHook, TrainingMethod,
};
use deblob_redis::RedisFeedbackStore;
use deblob_slm::runtime::{Backend, ModelBundle, ModelFamily, RuntimeInfo};
use deblob_slm::{
    AbstainCause, EndpointStatus, InferenceDecision, InferenceError, InferenceOutcome,
    InferenceRequest, InferenceTelemetry, Relation, SemanticInferencer,
};
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

fn telemetry() -> InferenceTelemetry {
    InferenceTelemetry {
        request_tokens: None,
        response_tokens: None,
        ttft_ms: None,
        total_latency_ms: None,
        repair_count: 0,
        endpoint_status: EndpointStatus::Ok,
        parse_error: false,
        schema_validation_error: false,
        model_id: None,
    }
}

/// Never an accepted match -> `false_merge_count` can never become nonzero
/// for this candidate, on ANY corpus (see the module docs).
struct AlwaysAbstain;

#[async_trait]
impl SemanticInferencer for AlwaysAbstain {
    async fn classify(&self, _req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        Ok(InferenceOutcome {
            decision: InferenceDecision::Abstain {
                cause: AbstainCause::InsufficientEvidence,
            },
            telemetry: telemetry(),
        })
    }
}

/// Always proposes the SAME fixed, bogus family as an accepted `Exact`
/// match — guaranteed to register as a false merge on every
/// `false_merge_trap` case (see the module docs).
struct AlwaysWrongFamily;

#[async_trait]
impl SemanticInferencer for AlwaysWrongFamily {
    async fn classify(&self, _req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        Ok(InferenceOutcome {
            decision: InferenceDecision::MatchSchema {
                schema_id: SchemaId::from_digest(&[251; 32]),
                relation: Relation::Exact,
            },
            telemetry: telemetry(),
        })
    }
}

fn model_bundle(inferencer: Arc<dyn SemanticInferencer>) -> ModelBundle {
    ModelBundle::new(
        inferencer,
        RuntimeInfo {
            backend: Backend::Cactus,
            model_id: "it-test-model".to_string(),
            quantization: None,
            endpoint: "http://test".to_string(),
            family: ModelFamily::StandardForwardPass,
        },
    )
}

/// Maximally permissive on every ABLATABLE axis; the hard false-merge
/// check is NEVER ablatable (`deblob::model_registry::gate_reasons`), so
/// this is exactly the axis this test suite isolates.
fn permissive_gate() -> GateConfig {
    GateConfig {
        min_test_n: 1,
        per_family_min_n: 1,
        per_family_precision_floor: 0.0,
        max_wrong_valid_rate: 1.0,
        min_accepted_precision: 0.0,
        min_retrieval_recall_at_5: 0.0,
        non_inferiority_margin: 1.0,
        retrieval_non_inferiority_margin: 1.0,
        max_false_merge_upper_ci: 1.0,
        min_shadow_hold_ms: 0,
        ..GateConfig::default()
    }
}

fn bundle_template() -> BundleTemplate {
    BundleTemplate {
        tokenizer: "tok-v1".to_string(),
        prompt_template_version: "prompt-v1".to_string(),
        runtime: "vllm-0.9".to_string(),
        quantization: "int8".to_string(),
        retrieval_index_version: "idx-v1".to_string(),
        grammar: "grammar-v1".to_string(),
        catalog: "catalog-v1".to_string(),
    }
}

fn cfg(seed: u64) -> PrequentialConfig {
    PrequentialConfig {
        seed,
        num_rounds: 1,
        round_batch_size: 4,
        round_stream_families: 6,
        round_stream_variants_per_family: 8,
        dev_families: 8,
        dev_variants_per_family: 8,
        audit_families: 4,
        audit_variants_per_family: 6,
    }
}

fn hook(seed: u64) -> TrainingBackendFineTuneHook<FakeBackend> {
    TrainingBackendFineTuneHook::new(
        Arc::new(FakeBackend::new()),
        BudgetPolicy {
            max_usd_ceiling: 1000.0,
        },
        FixedJobParams {
            trainer_image_digest: "sha256:trainer".to_string(),
            method: TrainingMethod::LoraSft,
            lora: LoraParams::default(),
            seed,
            requested_budget: Budget {
                max_usd: 1.0,
                max_runtime_minutes: 10,
            },
            output_uri: "s3://out".to_string(),
        },
    )
}

#[tokio::test]
async fn a_clean_candidate_enters_shadow_never_active_a_false_merging_one_is_rejected() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let registry = RedisModelRegistry::connect(&url).await.unwrap();
    let feedback = RedisFeedbackStore::connect(&url).await.unwrap();

    // -- clean candidate: data -> submit -> artifact -> eval -> gate ------
    let good_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(|_round| model_bundle(Arc::new(AlwaysAbstain)));
    let mut good_runner = PrequentialRunner::new(
        &cfg(500),
        "base-snapshot-clean",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        model_bundle(Arc::new(AlwaysAbstain)),
        good_factory,
    )
    .unwrap();
    let good_hook = hook(1);
    let good_record = good_runner
        .run_round(&feedback, &registry, &good_hook, "harness:it")
        .await
        .unwrap();
    assert!(
        good_record.gate_passed,
        "clean candidate must pass the gate, reasons={:?}",
        good_record.gate_reasons
    );

    // Separation of duties (spec §B7/§B11, reused verbatim): passing the
    // offline gate enters ShadowCandidate, NEVER Active — and nothing in
    // this loop ever calls `promote`.
    assert!(
        registry.get_active().await.unwrap().is_none(),
        "the fine-tune hook / prequential loop must never move the active alias"
    );
    let history = registry.history().await.unwrap();
    assert!(
        history
            .iter()
            .any(|v| v.state == deblob::model_registry::ModelState::ShadowCandidate),
        "the clean candidate must be recorded as ShadowCandidate, history={history:?}"
    );

    // -- bad candidate: false-merge trap -> rejected ----------------------
    let bad_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(|_round| model_bundle(Arc::new(AlwaysWrongFamily)));
    let mut bad_runner = PrequentialRunner::new(
        &cfg(501),
        "base-snapshot-bad",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        model_bundle(Arc::new(AlwaysAbstain)),
        bad_factory,
    )
    .unwrap();
    let bad_hook = hook(2);
    let bad_record = bad_runner
        .run_round(&feedback, &registry, &bad_hook, "harness:it")
        .await
        .unwrap();
    assert!(
        !bad_record.gate_passed,
        "a false-merging candidate must be rejected by the UNCONDITIONAL hard gate"
    );
    assert!(
        bad_record
            .gate_reasons
            .iter()
            .any(|r| r.contains("false_merge_count")),
        "expected a false-merge rejection reason, got {:?}",
        bad_record.gate_reasons
    );

    // The active pointer must STILL be untouched — a rejected candidate
    // (or a gate-passing one sitting in ShadowCandidate) never moves it.
    assert!(registry.get_active().await.unwrap().is_none());
}

/// Spec §7/§B7: over MULTIPLE rounds — including rounds whose candidate
/// passes the gate and becomes the next round's model — the active alias
/// is untouched throughout. `PrequentialRunner`/`RetrainPlan` hold no path
/// to `ModelRegistry::promote` at all.
#[tokio::test]
async fn multiple_rounds_never_move_the_active_alias() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let registry = RedisModelRegistry::connect(&url).await.unwrap();
    let feedback = RedisFeedbackStore::connect(&url).await.unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> = Arc::new(move |_round| {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        model_bundle(Arc::new(AlwaysAbstain))
    });

    let mut cfg = cfg(700);
    cfg.num_rounds = 2;
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-multi",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        model_bundle(Arc::new(AlwaysAbstain)),
        factory,
    )
    .unwrap();

    let h = hook(3);
    for _ in 0..cfg.num_rounds {
        let record = runner
            .run_round(&feedback, &registry, &h, "harness:it")
            .await
            .unwrap();
        assert!(record.gate_passed, "reasons={:?}", record.gate_reasons);
        assert!(
            registry.get_active().await.unwrap().is_none(),
            "active alias moved mid-trajectory — separation of duties violated"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), cfg.num_rounds);
    assert!(registry.get_active().await.unwrap().is_none());

    let frozen = runner.freeze().unwrap();
    assert_eq!(frozen.rounds().len(), cfg.num_rounds);
}
