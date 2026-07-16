//! Retrain-and-gate orchestrator (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §3).
//!
//! [`RetrainPlan::run`] is the ONLY place these pieces meet: it (1) pulls
//! durable feedback (`crate::feedback_store::FeedbackStore`, wired here via
//! `deblob_redis::FeedbackStore`) and the family-partitioned synthetic
//! corpus (`deblob_eval::EvalCase`, `Partition::Train`) into one training
//! JSONL export, (2) hands that JSONL to an EXTERNAL [`FineTuneHook`] —
//! Deblob never trains a gradient step itself — (3) evaluates the
//! returned candidate model against the corpus's `Partition::Test`
//! held-out slice via `deblob_eval::{run_eval, compute_metrics}`, and (4)
//! calls `crate::model_registry::ModelRegistry::promote_if_gated`, which
//! is the ONLY place a model ever actually becomes `Active`. Every step
//! before (4) is pure data movement — no product-crate behavior, no
//! trust-gate change, nothing that could let a worse or un-gated model
//! reach `Active` by a side door.

use async_trait::async_trait;
use deblob_core::id::FamilyId;
use deblob_eval::{compute_metrics, run_eval, EvalCase, Partition};
use deblob_redis::FeedbackStore;
use deblob_slm::SemanticInferencer;

use crate::model_registry::{
    EvalMetricsSummary, GoLiveGate, ModelRegistry, ModelState, ModelVersion, PromotionOutcome,
};

/// The artifact an external fine-tune hook produces: enough identity to
/// register a [`ModelVersion`] candidate, never the weights themselves
/// (Deblob does not serve or store models).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelArtifact {
    pub model_id: String,
    pub digest: String,
}

/// Failures from an external [`FineTuneHook`] invocation.
#[derive(Debug, thiserror::Error)]
pub enum FineTuneError {
    #[error("fine-tune hook process/transport error: {0}")]
    Process(String),
    #[error("fine-tune hook produced an unparsable artifact: {0}")]
    Parse(String),
}

/// The external hook boundary: Deblob NEVER runs a gradient step. Every
/// implementation (the real shell-out [`ShellFineTuneHook`] and any test
/// fake) turns a training JSONL blob into a [`ModelArtifact`] and nothing
/// more — this is the one place in the whole loop where "did the model
/// actually get better" is someone else's job.
#[async_trait]
pub trait FineTuneHook: Send + Sync {
    async fn train(&self, training_jsonl: &str) -> Result<ModelArtifact, FineTuneError>;
}

/// Real [`FineTuneHook`]: writes `training_jsonl` to a temp file and
/// shells out to a configured command (e.g. a Needle `finetune` / HF
/// wrapper script), appending the temp file's path as the final argument.
/// The command's stdout MUST be exactly one line of
/// `{"model_id": "...", "digest": "..."}` JSON — anything else is a
/// [`FineTuneError::Parse`].
pub struct ShellFineTuneHook {
    command: String,
    args: Vec<String>,
}

impl ShellFineTuneHook {
    pub fn new(command: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            command: command.into(),
            args,
        }
    }
}

#[async_trait]
impl FineTuneHook for ShellFineTuneHook {
    async fn train(&self, training_jsonl: &str) -> Result<ModelArtifact, FineTuneError> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = std::env::temp_dir().join(format!(
            "deblob-retrain-{}-{nanos}.jsonl",
            std::process::id()
        ));
        tokio::fs::write(&tmp, training_jsonl)
            .await
            .map_err(|e| FineTuneError::Process(format!("write training jsonl: {e}")))?;

        let output = tokio::process::Command::new(&self.command)
            .args(&self.args)
            .arg(&tmp)
            .output()
            .await
            .map_err(|e| FineTuneError::Process(format!("spawn {}: {e}", self.command)));

        let _ = tokio::fs::remove_file(&tmp).await;
        let output = output?;

        if !output.status.success() {
            return Err(FineTuneError::Process(format!(
                "{} exited with {:?}: {}",
                self.command,
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(stdout.trim())
            .map_err(|e| FineTuneError::Parse(format!("{e}: stdout was {stdout:?}")))
    }
}

/// Failures from [`RetrainPlan::run`].
#[derive(Debug, thiserror::Error)]
pub enum RetrainError {
    #[error(
        "the synthetic corpus carries no Partition::Test (held-out) case — nothing to gate against"
    )]
    NoHoldout,
    #[error("feedback store error: {0}")]
    Store(#[from] deblob_core::error::CoreError),
    #[error("fine-tune hook error: {0}")]
    FineTune(#[from] FineTuneError),
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Orchestrates one retrain-and-gate cycle. See the module docs for the
/// step-by-step boundary each argument owns.
pub struct RetrainPlan;

impl RetrainPlan {
    /// Runs one full cycle:
    ///
    /// 1. Combine `feedback` (every recorded [`deblob_slm::TrainingExample`])
    ///    with `synthetic_corpus`'s `Partition::Train` cases into a training
    ///    JSONL export (family-partitioned by construction — see the
    ///    `feedback_store`/`generate` modules this reuses).
    /// 2. Hand that JSONL to `fine_tune_hook` — external, no gradient step
    ///    runs in this process.
    /// 3. Evaluate the returned [`ModelArtifact`] via `eval_endpoint`
    ///    against `synthetic_corpus`'s `Partition::Test` (held-out) slice,
    ///    using the SAME `deblob_eval::{run_eval, compute_metrics}` the
    ///    offline eval harness uses.
    /// 4. Register the candidate and call
    ///    `registry.promote_if_gated` — the ONLY step that can ever make a
    ///    model `Active`.
    ///
    /// `Err(RetrainError::NoHoldout)` before touching `feedback`,
    /// `fine_tune_hook`, or `registry` at all if `synthetic_corpus` has no
    /// `Partition::Test` case — there would be nothing to gate the
    /// candidate against, and promoting without a held-out check is
    /// exactly the un-gated promotion this whole module exists to prevent.
    pub async fn run(
        feedback: &dyn FeedbackStore,
        synthetic_corpus: &[EvalCase],
        fine_tune_hook: &dyn FineTuneHook,
        eval_endpoint: &dyn SemanticInferencer,
        registry: &dyn ModelRegistry,
        gate: &GoLiveGate,
    ) -> Result<PromotionOutcome, RetrainError> {
        let train_cases: Vec<EvalCase> = synthetic_corpus
            .iter()
            .filter(|c| c.partition == Partition::Train)
            .cloned()
            .collect();
        let holdout_cases: Vec<EvalCase> = synthetic_corpus
            .iter()
            .filter(|c| c.partition == Partition::Test)
            .cloned()
            .collect();
        if holdout_cases.is_empty() {
            return Err(RetrainError::NoHoldout);
        }

        // 1. Combined, family-partitioned training export: synthetic
        // Partition::Train cases (deblob-eval's own generator already
        // partitions by family — see generate/mod.rs's
        // `partition_by_family_holds` test) + every durable feedback
        // example, in the identical `{prompt, gold_tool_call, ...}` JSONL
        // shape.
        let mut training_jsonl = deblob_eval::render_finetune_jsonl(&train_cases);
        let mut feedback_buf: Vec<u8> = Vec::new();
        let feedback_count = feedback.export_jsonl(&mut feedback_buf, None).await?;
        training_jsonl.push_str(
            &String::from_utf8(feedback_buf)
                .expect("export_jsonl always writes valid UTF-8 JSON lines"),
        );

        // 2. External hook — no gradient step runs here.
        let artifact = fine_tune_hook.train(&training_jsonl).await?;

        // 3. Evaluate against the held-out gate corpus only — the
        // candidate never saw these families during step 1's export.
        let run = run_eval(eval_endpoint, &holdout_cases).await;
        let metrics = compute_metrics(&run, &holdout_cases);
        let eval_metrics = EvalMetricsSummary::from_eval(&run, &metrics);

        let candidate = ModelVersion {
            model_id: artifact.model_id,
            digest: artifact.digest,
            trained_from: format!(
                "feedback_examples={feedback_count} synthetic_train_cases={} \
                 synthetic_holdout_cases={}",
                train_cases.len(),
                holdout_cases.len()
            ),
            eval_metrics,
            recorded_at: now_ms(),
            state: ModelState::Candidate,
        };
        registry.register_candidate(candidate.clone()).await?;

        // 4. The ONLY step that can ever move a model to Active.
        Ok(registry.promote_if_gated(candidate, gate).await?)
    }
}

/// Convenience: derive the [`FamilyId`] a synthetic [`EvalCase`]'s
/// `retrieved` top-k or `gold_schema_id` most directly represents, for
/// callers building feedback examples that should share a synthetic
/// case's family partition. Not used by [`RetrainPlan::run`] itself (which
/// only reads `partition_key`/`partition` as already assigned) — kept here
/// as a documented seam for a future caller wiring live feedback capture
/// into the same partition space the synthetic generator uses.
pub fn family_of(case: &EvalCase) -> Option<FamilyId> {
    case.retrieved.first().map(|c| c.family_id.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use deblob_core::id::SchemaId;
    use deblob_eval::{Category, Expected};
    use deblob_slm::{
        CandidateProfileView, FamilyCandidate, InferenceDecision, InferenceError, InferenceOutcome,
        InferenceRequest, InferenceTelemetry, Relation, TrainingExample,
    };
    use std::collections::BTreeMap;

    use crate::model_registry::{ModelRegistry, PromotionOutcome};

    use super::*;

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn family() -> FamilyId {
        FamilyId::new_v7()
    }

    fn candidate_view() -> CandidateProfileView {
        CandidateProfileView {
            observation_count: 50,
            fields: vec![],
            truncated: false,
        }
    }

    fn fc(schema: &SchemaId, family_id: FamilyId, rank: u32, distance: f32) -> FamilyCandidate {
        FamilyCandidate {
            family_id,
            schema_id: schema.clone(),
            version: 1,
            distance,
            rank,
        }
    }

    /// A minimal synthetic corpus: one Train case, one Test (held-out)
    /// case, distinct families — mirrors the real generator's family
    /// separation without depending on it.
    fn tiny_corpus() -> Vec<EvalCase> {
        let train_id = schema_id(1);
        let train_family = family();
        let test_id = schema_id(2);
        let test_family = family();

        vec![
            EvalCase {
                name: "train_case".to_string(),
                category: Category::KnownExact,
                candidate: candidate_view(),
                retrieved: vec![fc(&train_id, train_family, 1, 0.0)],
                expected: Expected {
                    decision: InferenceDecision::MatchSchema {
                        schema_id: train_id.clone(),
                        relation: Relation::Exact,
                    },
                    gold_schema_id: Some(train_id),
                    gold_rank: Some(1),
                    false_merge_trap: false,
                    false_split_trap: false,
                },
                partition: Partition::Train,
            },
            EvalCase {
                name: "holdout_case".to_string(),
                category: Category::KnownExact,
                candidate: candidate_view(),
                retrieved: vec![fc(&test_id, test_family, 1, 0.0)],
                expected: Expected {
                    decision: InferenceDecision::MatchSchema {
                        schema_id: test_id.clone(),
                        relation: Relation::Exact,
                    },
                    gold_schema_id: Some(test_id),
                    gold_rank: Some(1),
                    false_merge_trap: false,
                    false_split_trap: false,
                },
                partition: Partition::Test,
            },
        ]
    }

    // -- in-memory fakes, used only by this module's tests -------------

    #[derive(Default)]
    struct FakeFeedbackStore {
        examples: Mutex<Vec<TrainingExample>>,
    }

    #[async_trait]
    impl FeedbackStore for FakeFeedbackStore {
        async fn append(
            &self,
            example: &TrainingExample,
        ) -> Result<(), deblob_core::error::CoreError> {
            self.examples.lock().unwrap().push(example.clone());
            Ok(())
        }
        async fn export_jsonl(
            &self,
            writer: &mut (dyn std::io::Write + Send),
            partition: Option<&FamilyId>,
        ) -> Result<usize, deblob_core::error::CoreError> {
            let examples = self.examples.lock().unwrap();
            let mut count = 0;
            for ex in examples.iter() {
                if let Some(p) = partition {
                    if &ex.partition_key != p {
                        continue;
                    }
                }
                let allowed: Vec<SchemaId> =
                    ex.retrieved.iter().map(|c| c.schema_id.clone()).collect();
                let prompt = deblob_slm::build_prompt(&ex.candidate, &ex.retrieved, &allowed);
                let line = serde_json::json!({
                    "prompt": prompt.text,
                    "gold_tool_call": serde_json::to_value(&ex.gold).unwrap(),
                });
                writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
                count += 1;
            }
            Ok(count)
        }
        async fn iter_by_partition(
            &self,
        ) -> Result<BTreeMap<String, Vec<TrainingExample>>, deblob_core::error::CoreError> {
            let mut map: BTreeMap<String, Vec<TrainingExample>> = BTreeMap::new();
            for ex in self.examples.lock().unwrap().iter() {
                map.entry(ex.partition_key.as_str().to_string())
                    .or_default()
                    .push(ex.clone());
            }
            Ok(map)
        }
    }

    struct FakeFineTuneHook {
        artifact: ModelArtifact,
        calls: AtomicUsize,
        last_jsonl: Mutex<String>,
    }

    impl FakeFineTuneHook {
        fn new(artifact: ModelArtifact) -> Self {
            Self {
                artifact,
                calls: AtomicUsize::new(0),
                last_jsonl: Mutex::new(String::new()),
            }
        }
    }

    #[async_trait]
    impl FineTuneHook for FakeFineTuneHook {
        async fn train(&self, training_jsonl: &str) -> Result<ModelArtifact, FineTuneError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_jsonl.lock().unwrap() = training_jsonl.to_string();
            Ok(self.artifact.clone())
        }
    }

    /// Scripted fake inferencer: echoes the corpus's own `expected.decision`
    /// for a "good" model, or a fixed wrong/unsafe answer for a "bad" one.
    struct ScriptedInferencer {
        mode: ScriptedMode,
    }

    enum ScriptedMode {
        AlwaysCorrect,
        FalseMerge(SchemaId),
    }

    #[async_trait]
    impl SemanticInferencer for ScriptedInferencer {
        async fn classify(
            &self,
            req: InferenceRequest,
        ) -> Result<InferenceOutcome, InferenceError> {
            let decision = match &self.mode {
                ScriptedMode::AlwaysCorrect => {
                    // The test corpus always expects an Exact match to the
                    // sole retrieved candidate.
                    InferenceDecision::MatchSchema {
                        schema_id: req.retrieved[0].schema_id.clone(),
                        relation: Relation::Exact,
                    }
                }
                ScriptedMode::FalseMerge(wrong_id) => InferenceDecision::MatchSchema {
                    schema_id: wrong_id.clone(),
                    relation: Relation::Exact,
                },
            };
            Ok(InferenceOutcome {
                decision,
                telemetry: InferenceTelemetry {
                    request_tokens: None,
                    response_tokens: None,
                    ttft_ms: None,
                    total_latency_ms: None,
                    repair_count: 0,
                    endpoint_status: deblob_slm::EndpointStatus::Ok,
                    parse_error: false,
                    schema_validation_error: false,
                    model_id: None,
                },
            })
        }
    }

    #[derive(Default)]
    struct FakeModelRegistry {
        models: Mutex<std::collections::HashMap<String, ModelVersion>>,
        active: Mutex<Option<String>>,
    }

    #[async_trait]
    impl ModelRegistry for FakeModelRegistry {
        async fn register_candidate(
            &self,
            version: ModelVersion,
        ) -> Result<(), deblob_core::error::CoreError> {
            let mut models = self.models.lock().unwrap();
            if models.contains_key(&version.model_id) {
                return Err(deblob_core::error::CoreError::Conflict(
                    "already registered".into(),
                ));
            }
            models.insert(version.model_id.clone(), version);
            Ok(())
        }

        async fn get_active(&self) -> Result<Option<ModelVersion>, deblob_core::error::CoreError> {
            let active = self.active.lock().unwrap().clone();
            Ok(active.and_then(|id| self.models.lock().unwrap().get(&id).cloned()))
        }

        async fn promote_if_gated(
            &self,
            mut candidate: ModelVersion,
            gate: &GoLiveGate,
        ) -> Result<PromotionOutcome, deblob_core::error::CoreError> {
            let active = self.get_active().await?;
            let mut reasons = crate::model_registry::gate_reasons(&candidate.eval_metrics, gate);
            if let Some(active_version) = &active {
                reasons.extend(crate::model_registry::regression_reasons(
                    &candidate.eval_metrics,
                    &active_version.eval_metrics,
                ));
            }
            if reasons.is_empty() {
                candidate.state = ModelState::Active;
                self.models
                    .lock()
                    .unwrap()
                    .insert(candidate.model_id.clone(), candidate.clone());
                *self.active.lock().unwrap() = Some(candidate.model_id.clone());
                Ok(PromotionOutcome::Promoted(candidate))
            } else {
                candidate.state = ModelState::Rejected;
                self.models
                    .lock()
                    .unwrap()
                    .insert(candidate.model_id.clone(), candidate.clone());
                Ok(PromotionOutcome::Rejected { reasons, candidate })
            }
        }

        async fn rollback(
            &self,
            _actor: &str,
        ) -> Result<ModelVersion, deblob_core::error::CoreError> {
            unimplemented!("not exercised by RetrainPlan tests")
        }

        async fn history(&self) -> Result<Vec<ModelVersion>, deblob_core::error::CoreError> {
            Ok(self.models.lock().unwrap().values().cloned().collect())
        }
    }

    #[tokio::test]
    async fn end_to_end_pipeline_promotes_a_passing_candidate_with_a_fake_finetune_hook() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        // Seed one hard-negative feedback example to prove step 1 combines
        // both sources.
        let rejected = schema_id(9);
        let example = crate::feedback::capture_trusted_proposal_rejected(
            candidate_view(),
            vec![],
            &rejected,
            None,
            family(),
            1,
        );
        feedback.append(&example).await.unwrap();

        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-v1".to_string(),
            digest: "sha256:aaaa".to_string(),
        });
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let registry = FakeModelRegistry::default();
        let gate = GoLiveGate::default();

        let outcome = RetrainPlan::run(&feedback, &corpus, &hook, &inferencer, &registry, &gate)
            .await
            .unwrap();

        match outcome {
            PromotionOutcome::Promoted(v) => {
                assert_eq!(v.model_id, "model-v1");
                assert_eq!(v.eval_metrics.exact_semantic_accuracy, 1.0);
            }
            other => panic!("expected Promoted, got {other:?}"),
        }
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        // The training export must have combined BOTH the synthetic Train
        // case and the feedback example.
        let jsonl = hook.last_jsonl.lock().unwrap().clone();
        assert_eq!(
            jsonl.lines().count(),
            2,
            "1 synthetic train case + 1 feedback example"
        );

        let active = registry.get_active().await.unwrap().unwrap();
        assert_eq!(active.model_id, "model-v1");
    }

    #[tokio::test]
    async fn a_gate_failing_candidate_is_rejected_and_the_active_model_is_unchanged() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-bad".to_string(),
            digest: "sha256:bbbb".to_string(),
        });
        // The held-out case's only retrieved candidate has schema_id
        // schema_id(2); a false-merge inferencer names a DIFFERENT wrong
        // family. `false_merge_trap` isn't set on this tiny corpus, so this
        // exercises the `wrong_valid_rate`/`accepted_precision` gate axes
        // rather than the hard false-merge gate directly.
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::FalseMerge(schema_id(200)),
        };
        let registry = FakeModelRegistry::default();
        let gate = GoLiveGate::default();

        let outcome = RetrainPlan::run(&feedback, &corpus, &hook, &inferencer, &registry, &gate)
            .await
            .unwrap();

        match outcome {
            PromotionOutcome::Rejected { reasons, candidate } => {
                assert!(!reasons.is_empty());
                assert_eq!(candidate.state, ModelState::Rejected);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert!(
            registry.get_active().await.unwrap().is_none(),
            "a rejected candidate must never become active"
        );
    }

    #[tokio::test]
    async fn a_second_candidate_worse_than_the_first_active_model_is_rejected() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        let registry = FakeModelRegistry::default();
        let gate = GoLiveGate::default();

        // First run: a good model becomes active.
        let good_hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-good".to_string(),
            digest: "sha256:good".to_string(),
        });
        let good_inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let first = RetrainPlan::run(
            &feedback,
            &corpus,
            &good_hook,
            &good_inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();
        assert!(matches!(first, PromotionOutcome::Promoted(_)));

        // Second run: a worse model (wrong on the held-out case) must be
        // rejected, and the active model must stay the first one.
        let worse_hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-worse".to_string(),
            digest: "sha256:worse".to_string(),
        });
        let worse_inferencer = ScriptedInferencer {
            mode: ScriptedMode::FalseMerge(schema_id(201)),
        };
        let second = RetrainPlan::run(
            &feedback,
            &corpus,
            &worse_hook,
            &worse_inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();
        assert!(matches!(second, PromotionOutcome::Rejected { .. }));

        let active = registry.get_active().await.unwrap().unwrap();
        assert_eq!(
            active.model_id, "model-good",
            "the worse candidate must never displace the still-active good model"
        );
    }

    #[tokio::test]
    async fn no_holdout_case_is_a_hard_error_before_any_side_effect() {
        let train_only = vec![tiny_corpus().remove(0)]; // only the Train case
        let feedback = FakeFeedbackStore::default();
        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "should-not-run".to_string(),
            digest: "sha256:none".to_string(),
        });
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let registry = FakeModelRegistry::default();

        let err = RetrainPlan::run(
            &feedback,
            &train_only,
            &hook,
            &inferencer,
            &registry,
            &GoLiveGate::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, RetrainError::NoHoldout));
        assert_eq!(
            hook.calls.load(Ordering::SeqCst),
            0,
            "the fine-tune hook must never run when there is no held-out gate corpus"
        );
    }
}
