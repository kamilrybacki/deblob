//! Unit tests for [`super::PrequentialRunner`] against IN-MEMORY fakes
//! (fast, no Docker) — mirrors `deblob::retrain`'s own private test fakes.
//! The real-Redis separation-of-duties/e2e-gate proof lives in
//! `tests/continual_prequential_it.rs` (testcontainers), per the task
//! brief's "Docker up (real-Redis IT for feedback/registry)".

use std::collections::BTreeMap as StdBTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use deblob::model_registry::{
    BundleTemplate, GateConfig, GateDecision, ModelRegistry, ModelState, ModelVersion,
    PromotionApproval,
};
use deblob::retrain::CurationConfig;
use deblob_core::error::CoreError;
use deblob_core::id::{FamilyId, SchemaId};
use deblob_redis::{ExportManifest, FeedbackStore};
use deblob_slm::runtime::{Backend, ModelBundle, ModelFamily, RuntimeInfo};
use deblob_slm::{
    AbstainCause, InferenceDecision, InferenceError, InferenceOutcome, InferenceRequest,
    InferenceTelemetry, Relation, SemanticInferencer, TrainingExample,
};

use crate::continual::datasets::{self, PrequentialConfig};
use crate::continual::training_job::{
    Budget, FakeBackend, FixedJobParams, LoraParams, TrainingBackendFineTuneHook, TrainingMethod,
};

use super::{PrequentialError, PrequentialRunner};

fn telemetry() -> InferenceTelemetry {
    InferenceTelemetry {
        request_tokens: None,
        response_tokens: None,
        ttft_ms: None,
        total_latency_ms: None,
        repair_count: 0,
        endpoint_status: deblob_slm::EndpointStatus::Ok,
        parse_error: false,
        schema_validation_error: false,
        model_id: None,
    }
}

/// A STATELESS "always correct" test double: a lookup table keyed by the
/// exact rendered prompt (built via the real `deblob_slm::build_prompt`,
/// same as every real case) -> `expected.decision`, covering every case in
/// every dataset the runner will ever throw at it (dev corpus, round-
/// stream batches, and the audit set). Deliberately NOT a call-order
/// script: this SAME model instance is invoked from many different
/// contexts across a run (a round's own predict step, `RetrainPlan`'s
/// holdout gate evaluation, adaptation/retention probes on OTHER batches,
/// and — via `FrozenTrajectory` — the sealed audit set), so a linear
/// script would silently misalign the moment it's asked about a case
/// outside whatever sequence it expected.
struct LookupInferencer {
    by_prompt: StdBTreeMap<String, InferenceDecision>,
}

impl LookupInferencer {
    fn always_correct(cfg: &PrequentialConfig) -> Self {
        let mut cases = datasets::dev_corpus(cfg);
        cases.extend(datasets::round_batches(cfg).unwrap().into_iter().flatten());
        cases.extend(datasets::audit_set(cfg).cases);
        let mut by_prompt = StdBTreeMap::new();
        for case in &cases {
            let allowed: Vec<SchemaId> =
                case.retrieved.iter().map(|c| c.schema_id.clone()).collect();
            let prompt = deblob_slm::build_prompt(&case.candidate, &case.retrieved, &allowed).text;
            by_prompt.insert(prompt, case.expected.decision.clone());
        }
        Self { by_prompt }
    }
}

#[async_trait]
impl SemanticInferencer for LookupInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        let decision =
            self.by_prompt
                .get(&req.prompt)
                .cloned()
                .unwrap_or(InferenceDecision::Abstain {
                    cause: AbstainCause::Ambiguous,
                });
        Ok(InferenceOutcome {
            decision,
            telemetry: telemetry(),
        })
    }
}

/// Always proposes a fixed, WRONG family — guaranteed to fail
/// `wrong_valid_rate`/`accepted_precision` regardless of the held-out
/// corpus's content.
struct AlwaysWrongFamily;

#[async_trait]
impl SemanticInferencer for AlwaysWrongFamily {
    async fn classify(&self, _req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        Ok(InferenceOutcome {
            decision: InferenceDecision::MatchSchema {
                schema_id: SchemaId::from_digest(&[250; 32]),
                relation: Relation::Exact,
            },
            telemetry: telemetry(),
        })
    }
}

fn model_bundle(inferencer: Arc<dyn SemanticInferencer>, family: ModelFamily) -> ModelBundle {
    ModelBundle::new(
        inferencer,
        RuntimeInfo {
            backend: Backend::Cactus,
            model_id: "test-model".to_string(),
            quantization: None,
            endpoint: "http://test".to_string(),
            family,
        },
    )
}

fn small_cfg(seed: u64, num_rounds: usize) -> PrequentialConfig {
    PrequentialConfig {
        seed,
        num_rounds,
        round_batch_size: 4,
        round_stream_families: 6,
        round_stream_variants_per_family: 8,
        dev_families: 6,
        dev_variants_per_family: 8,
        audit_families: 4,
        audit_variants_per_family: 6,
    }
}

fn permissive_gate() -> GateConfig {
    GateConfig {
        min_test_n: 1,
        per_family_min_n: 1,
        per_family_precision_floor: 0.0,
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

fn hook(policy_ceiling: f64, method: TrainingMethod) -> TrainingBackendFineTuneHook<FakeBackend> {
    TrainingBackendFineTuneHook::new(
        Arc::new(FakeBackend::new()),
        crate::continual::training_job::BudgetPolicy {
            max_usd_ceiling: policy_ceiling,
        },
        FixedJobParams {
            trainer_image_digest: "sha256:trainer".to_string(),
            method,
            lora: LoraParams::default(),
            seed: 1,
            requested_budget: Budget {
                max_usd: 1.0,
                max_runtime_minutes: 10,
            },
            output_uri: "s3://out".to_string(),
        },
    )
}

// -- in-memory fakes (mirrors deblob::retrain's own private test fakes) --

#[derive(Default)]
struct FakeFeedbackStore {
    examples: Mutex<Vec<TrainingExample>>,
}

#[async_trait]
impl FeedbackStore for FakeFeedbackStore {
    async fn append(&self, example: &TrainingExample) -> Result<(), CoreError> {
        self.examples.lock().unwrap().push(example.clone());
        Ok(())
    }
    async fn export_jsonl(
        &self,
        writer: &mut (dyn std::io::Write + Send),
        _partition: Option<&FamilyId>,
    ) -> Result<usize, CoreError> {
        let examples = self.examples.lock().unwrap();
        let mut count = 0;
        for ex in examples.iter() {
            let allowed: Vec<SchemaId> = ex.retrieved.iter().map(|c| c.schema_id.clone()).collect();
            let prompt = deblob_slm::build_prompt(&ex.candidate, &ex.retrieved, &allowed);
            let line = serde_json::json!({
                "prompt": prompt.text,
                "gold_tool_call": serde_json::to_value(&ex.gold).unwrap(),
                "partition_key": ex.partition_key.as_str(),
            });
            writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
            count += 1;
        }
        Ok(count)
    }
    async fn iter_by_partition(
        &self,
    ) -> Result<StdBTreeMap<String, Vec<TrainingExample>>, CoreError> {
        Ok(StdBTreeMap::new())
    }
    async fn quarantine_actor(&self, _actor: &str) -> Result<(), CoreError> {
        Ok(())
    }
    async fn quarantined_actors(&self) -> Result<std::collections::BTreeSet<String>, CoreError> {
        Ok(std::collections::BTreeSet::new())
    }
    async fn export_snapshot(&self, _dir: &std::path::Path) -> Result<ExportManifest, CoreError> {
        unimplemented!("not exercised by these tests")
    }
}

#[derive(Default)]
struct FakeModelRegistry {
    models: Mutex<std::collections::HashMap<String, ModelVersion>>,
    active: Mutex<Option<String>>,
}

#[async_trait]
impl ModelRegistry for FakeModelRegistry {
    async fn register_candidate(&self, mut version: ModelVersion) -> Result<(), CoreError> {
        version.state = ModelState::Candidate;
        version.evidence = None;
        version.shadow_since = None;
        let mut models = self.models.lock().unwrap();
        if models.contains_key(&version.model_id) {
            return Err(CoreError::Conflict("already registered".into()));
        }
        models.insert(version.model_id.clone(), version);
        Ok(())
    }
    async fn get_active(&self) -> Result<Option<ModelVersion>, CoreError> {
        let active = self.active.lock().unwrap().clone();
        Ok(active.and_then(|id| self.models.lock().unwrap().get(&id).cloned()))
    }
    async fn get(&self, model_id: &str) -> Result<Option<ModelVersion>, CoreError> {
        Ok(self.models.lock().unwrap().get(model_id).cloned())
    }
    async fn attach_evidence(
        &self,
        model_id: &str,
        evidence: deblob::model_registry::GateEvidence,
        gate: &GateConfig,
    ) -> Result<GateDecision, CoreError> {
        let mut candidate = self
            .models
            .lock()
            .unwrap()
            .get(model_id)
            .cloned()
            .ok_or(CoreError::NotFound)?;
        let active = self.get_active().await?;
        let mut reasons = deblob::model_registry::gate_reasons(&evidence, gate);
        if let Some(active_version) = &active {
            if let Some(active_evidence) = &active_version.evidence {
                reasons.extend(deblob::model_registry::regression_reasons(
                    &evidence,
                    active_evidence,
                    gate,
                ));
            }
        }
        candidate.evidence = Some(evidence);
        let decision = if reasons.is_empty() {
            candidate.state = ModelState::ShadowCandidate;
            candidate.shadow_since = Some(0);
            GateDecision::EnteredShadow(candidate.clone())
        } else {
            candidate.state = ModelState::Rejected;
            GateDecision::Rejected {
                reasons,
                candidate: candidate.clone(),
            }
        };
        self.models
            .lock()
            .unwrap()
            .insert(candidate.model_id.clone(), candidate);
        Ok(decision)
    }
    async fn promote(
        &self,
        model_id: &str,
        approval: PromotionApproval,
        gate: &GateConfig,
    ) -> Result<ModelVersion, CoreError> {
        let mut candidate = self
            .models
            .lock()
            .unwrap()
            .get(model_id)
            .cloned()
            .ok_or(CoreError::NotFound)?;
        if candidate.state != ModelState::ShadowCandidate {
            return Err(CoreError::Conflict("not ShadowCandidate".into()));
        }
        if gate.require_explicit_approval && !approval.approved {
            return Err(CoreError::PolicyRejected("approval required".into()));
        }
        candidate.state = ModelState::Active;
        self.models
            .lock()
            .unwrap()
            .insert(candidate.model_id.clone(), candidate.clone());
        *self.active.lock().unwrap() = Some(candidate.model_id.clone());
        Ok(candidate)
    }
    async fn rollback(&self, _actor: &str) -> Result<ModelVersion, CoreError> {
        unimplemented!("not exercised by these tests")
    }
    async fn history(&self) -> Result<Vec<ModelVersion>, CoreError> {
        Ok(self.models.lock().unwrap().values().cloned().collect())
    }
}

// ------------------------------------------------------------------------

#[tokio::test]
async fn a_clean_candidate_enters_shadow_and_becomes_the_next_rounds_model() {
    let cfg = small_cfg(11, 2);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> = {
        let script = script.clone();
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::StandardForwardPass))
    };
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::LoraSft);
    let record = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap();
    assert!(record.gate_passed, "reasons={:?}", record.gate_reasons);
    assert_eq!(record.method, "lora-sft");
    assert!(
        registry.get_active().await.unwrap().is_none(),
        "attach_evidence passing the gate must never itself activate a candidate"
    );
}

#[tokio::test]
async fn a_bad_candidate_is_rejected_and_the_model_does_not_advance() {
    let cfg = small_cfg(12, 2);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script, ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> = Arc::new(|_round| {
        model_bundle(
            Arc::new(AlwaysWrongFamily),
            ModelFamily::StandardForwardPass,
        )
    });
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::LoraSft);
    let record = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap();
    assert!(!record.gate_passed);
    assert!(!record.gate_reasons.is_empty());
    assert!(registry.get_active().await.unwrap().is_none());
}

#[tokio::test]
async fn budget_ceiling_rejects_the_round_before_any_submit() {
    let cfg = small_cfg(13, 1);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::StandardForwardPass));
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    // Ceiling of $0.01 vs the hook's own requested $1.00 budget -> rejected
    // before `submit` is ever reached.
    let h = hook(0.01, TrainingMethod::LoraSft);
    let err = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap_err();
    assert!(matches!(err, PrequentialError::Retrain(_)));
}

#[tokio::test]
async fn needle_model_family_is_labeled_needle_custom_in_the_round_record() {
    let cfg = small_cfg(14, 1);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::NeedleContinualUpdate);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::NeedleContinualUpdate));
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::NeedleCustom);
    let record = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap();
    assert_eq!(record.method, "needle-custom");
    assert_ne!(record.method, "lora-sft");
}

/// Spec §7: model-r is evaluated on round r's batch BEFORE any label in
/// that batch is revealed. Reads `FakeFeedbackStore`'s example count as of
/// the moment of the call — same module, private field access is fine.
fn appended_count(store: &FakeFeedbackStore) -> usize {
    store.examples.lock().unwrap().len()
}

/// A model that, when asked to `decide`, records how many feedback
/// examples had been durably appended AT THE MOMENT of the call. If the
/// runner ever revealed round r's labels before finishing round r's
/// predictions, a call made during round r's OWN predict step would see a
/// count that already includes round r's own batch — this test asserts it
/// never does.
struct CountingSpyInferencer {
    inner: Arc<dyn SemanticInferencer>,
    appended_at_call: Arc<Mutex<Vec<usize>>>,
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl SemanticInferencer for CountingSpyInferencer {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceOutcome, InferenceError> {
        self.appended_at_call
            .lock()
            .unwrap()
            .push(self.counter.load(Ordering::SeqCst));
        self.inner.classify(req).await
    }
}

#[tokio::test]
async fn round_predictions_never_observe_that_rounds_own_revealed_feedback() {
    let cfg = small_cfg(21, 2);
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script: Arc<dyn SemanticInferencer> = Arc::new(LookupInferencer::always_correct(&cfg));
    let appended_at_call = Arc::new(Mutex::new(Vec::new()));
    let spy: Arc<dyn SemanticInferencer> = Arc::new(CountingSpyInferencer {
        inner: script.clone(),
        appended_at_call: appended_at_call.clone(),
        counter: counter.clone(),
    });
    let b_v0 = model_bundle(spy.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> = {
        let spy = spy.clone();
        Arc::new(move |_round| model_bundle(spy.clone(), ModelFamily::StandardForwardPass))
    };
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::LoraSft);
    // Increment the SAME counter every time feedback is appended, wired via
    // a thin wrapper below.
    for round in 0..cfg.num_rounds {
        let before_calls = appended_at_call.lock().unwrap().len();
        runner
            .run_round(&feedback, &registry, &h, "harness:test")
            .await
            .unwrap();
        counter.store(appended_count(&feedback), Ordering::SeqCst);
        let calls_this_round = &appended_at_call.lock().unwrap()[before_calls..];
        // Every predict-time observation made DURING this round must have
        // seen a count STRICTLY LESS than what it is now (after this
        // round's own reveal) — i.e. none of them already included this
        // round's own batch.
        let after = appended_count(&feedback);
        for seen in calls_this_round {
            assert!(
                *seen < after || round == 0 && after == 0,
                "round {round}: a prediction observed revealed-count {seen}, but this round's \
                 own reveal brought the total to {after} — a label leaked into pre-eval"
            );
        }
    }
}

#[tokio::test]
async fn freeze_before_all_rounds_complete_is_an_error() {
    let cfg = small_cfg(30, 2);
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::StandardForwardPass));
    let runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    match runner.freeze() {
        Err(PrequentialError::TrajectoryNotComplete {
            completed: 0,
            total: 2,
        }) => {}
        Err(other) => panic!("expected TrajectoryNotComplete{{0,2}}, got {other:?}"),
        Ok(_) => panic!("freeze must not succeed before every round has run"),
    }
}

#[tokio::test]
async fn a_completed_trajectory_freezes_and_scores_c_final_vs_b_v0_deterministically() {
    let cfg = small_cfg(31, 2);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::StandardForwardPass));
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::LoraSft);
    for _ in 0..cfg.num_rounds {
        runner
            .run_round(&feedback, &registry, &h, "harness:test")
            .await
            .unwrap();
    }
    let frozen = runner.freeze().unwrap();
    assert_eq!(frozen.rounds().len(), cfg.num_rounds);

    // Spec §7: no argmax-over-audit API exists -- this method takes no
    // round-index/Arm argument, and calling it twice with the same
    // arguments always scores the SAME fixed pair of models (no hidden
    // search over rounds).
    let a = frozen.c_final_vs_b_v0(1, 200);
    let b = frozen.c_final_vs_b_v0(1, 200);
    assert_eq!(
        serde_json::to_string(&a.contingency).unwrap(),
        serde_json::to_string(&b.contingency).unwrap()
    );
    assert_eq!(
        a.contingency.n,
        cfg.audit_families * cfg.audit_variants_per_family
    );
}

#[tokio::test]
async fn adaptation_gain_and_retention_loss_are_present_at_the_right_rounds() {
    let cfg = small_cfg(40, 3);
    let feedback = FakeFeedbackStore::default();
    let registry = FakeModelRegistry::default();
    let script = Arc::new(LookupInferencer::always_correct(&cfg));
    let b_v0 = model_bundle(script.clone(), ModelFamily::StandardForwardPass);
    let candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync> =
        Arc::new(move |_round| model_bundle(script.clone(), ModelFamily::StandardForwardPass));
    let mut runner = PrequentialRunner::new(
        &cfg,
        "base-snapshot-v0",
        bundle_template(),
        CurationConfig::default(),
        permissive_gate(),
        b_v0,
        candidate_factory,
    )
    .unwrap();

    let h = hook(1000.0, TrainingMethod::LoraSft);
    let r0 = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap()
        .clone();
    assert!(r0.adaptation_gain.is_some(), "round 0 has a future batch");
    assert!(
        r0.retention_loss.is_none(),
        "round 0 has no frozen history yet"
    );

    let r1 = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap()
        .clone();
    assert!(r1.adaptation_gain.is_some());
    assert!(r1.retention_loss.is_some());

    let r2 = runner
        .run_round(&feedback, &registry, &h, "harness:test")
        .await
        .unwrap()
        .clone();
    assert!(
        r2.adaptation_gain.is_none(),
        "the final round has no future batch to measure adaptation against"
    );
    assert!(r2.retention_loss.is_some());
}
