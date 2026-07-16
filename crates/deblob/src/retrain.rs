//! Retrain-and-gate orchestrator (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §3,
//! §B7/§B9/§B10 — "Amendments from joint research").
//!
//! [`RetrainPlan::run`] is the ONLY place these pieces meet: it (1)
//! CURATES an active-learning replay set (spec §B10) from durable
//! feedback (`crate::feedback_store::FeedbackStore`, wired here via
//! `deblob_redis::FeedbackStore`) and the family-partitioned synthetic
//! corpus (`deblob_eval::EvalCase`, `Partition::Train`), stratified per
//! spec §B9 (immutable-golden / historical-replay / recent-correction /
//! rare-adversarial / no-call), (2) hands that replay set — plus a FIXED,
//! caller-supplied `base_snapshot` (never the latest promoted adapter,
//! spec §B9) — to an EXTERNAL [`FineTuneHook`] — Deblob never trains a
//! gradient step itself — (3) evaluates the returned candidate's
//! QUANTIZED artifact against the corpus's `Partition::Test` held-out
//! slice via `deblob_eval::{run_eval, compute_metrics}`, building a
//! [`crate::model_registry::GateEvidence`] bundle, and (4) registers the
//! candidate and attaches that evidence via
//! `crate::model_registry::ModelRegistry::attach_evidence`.
//!
//! # This module can NEVER promote (spec §B7)
//!
//! `attach_evidence` can only ever produce `ShadowCandidate` or
//! `Rejected` — never `Active` (see `crate::model_registry`'s module
//! docs). `RetrainPlan::run` calls `register_candidate` and
//! `attach_evidence` and NOTHING else on the registry: it holds no
//! `PromotionApproval`, never constructs one, and never calls
//! `ModelRegistry::promote`. Moving the active alias is a separate,
//! human/controller-attributed action outside this module entirely — see
//! `retrain_plan_never_moves_the_active_alias_even_when_the_offline_gate_passes`
//! in this module's tests for the proof.

use std::collections::BTreeMap;

use async_trait::async_trait;
use deblob_core::id::FamilyId;
use deblob_eval::{compute_metrics, run_eval, Category, EvalCase, Partition};
use deblob_redis::FeedbackStore;
use deblob_slm::{InferenceDecision, SemanticInferencer};

use crate::model_registry::{
    ArtifactBundle, BundleTemplate, GateConfig, GateDecision, GateEvidence, ModelRegistry,
    ModelState, ModelVersion, TrainedFrom,
};

// ---------------------------------------------------------------------
// Active-learning curation (spec §B9/§B10)
// ---------------------------------------------------------------------

/// Which replay stratum (spec §B9) one training line belongs to.
/// `ImmutableGolden`/`RareAdversarial`/`NoCall`/`RecentCorrection` are
/// NEVER capped by [`curate_synthetic`] — only `HistoricalReplay` ("easy
/// positives") is subject to [`CurationConfig::max_easy_positives_per_family`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayStratum {
    /// Safety-critical synthetic cases: false-merge/false-split traps and
    /// `Category::IncompatibleUnsafe` — must never be dropped by
    /// curation.
    ImmutableGolden,
    /// Ordinary synthetic `Partition::Train` cases — "easy positives",
    /// capped per family (spec §B10: "cap redundant easy positives").
    HistoricalReplay,
    /// Every durable `FeedbackStore` example — by definition a human
    /// correction/confirmation captured since the last retrain. Always
    /// included uncapped here: `FeedbackStore::export_jsonl` already
    /// applies the anti-poisoning per-(family, label_source) cap (spec
    /// amendment A3/A4) upstream of this module.
    RecentCorrection,
    /// `Category::AmbiguousAdversarial` synthetic cases — rare by
    /// construction, always included uncapped.
    RareAdversarial,
    /// Synthetic cases whose gold decision is `Abstain` — training
    /// signal for "the right call is not to call", always included
    /// uncapped.
    NoCall,
}

impl ReplayStratum {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ImmutableGolden => "immutable_golden",
            Self::HistoricalReplay => "historical_replay",
            Self::RecentCorrection => "recent_correction",
            Self::RareAdversarial => "rare_adversarial",
            Self::NoCall => "no_call",
        }
    }
}

/// One curated training line, tagged with its [`ReplayStratum`] and (if
/// known) which family it evidences — the unit [`ReplaySet::to_jsonl`]
/// renders and [`ReplaySet::families_represented`] counts over.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayLine {
    pub stratum: ReplayStratum,
    pub family_id: Option<String>,
    /// A single well-formed JSON object (no trailing newline) — the same
    /// `{prompt, gold_tool_call, ...}` shape `deblob_eval`/`FeedbackStore`
    /// already render.
    pub json_line: String,
}

/// The curated set [`RetrainPlan::run`] hands to [`FineTuneHook::train`]
/// (spec §B9: "Provide a replay set to the fine-tune hook with strata").
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplaySet {
    pub lines: Vec<ReplayLine>,
}

impl ReplaySet {
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Renders every line as JSONL, one `replay_stratum` field merged
    /// into each JSON object so the external fine-tune hook can weight/
    /// sample by stratum without a second lookup.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        for line in &self.lines {
            let mut value: serde_json::Value = serde_json::from_str(&line.json_line)
                .expect("ReplaySet lines are always well-formed JSON (built by this module)");
            if let serde_json::Value::Object(map) = &mut value {
                map.insert(
                    "replay_stratum".to_string(),
                    serde_json::Value::String(line.stratum.as_str().to_string()),
                );
            }
            out.push_str(
                &serde_json::to_string(&value).expect("a JSON Value always re-serializes"),
            );
            out.push('\n');
        }
        out
    }

    pub fn stratum_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for line in &self.lines {
            *counts.entry(line.stratum.as_str().to_string()).or_insert(0) += 1;
        }
        counts
    }

    pub fn families_represented(&self) -> usize {
        self.lines
            .iter()
            .filter_map(|l| l.family_id.as_deref())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }
}

/// Ablatable curation knobs (spec §B10). **Unvalidated — ablate.**
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CurationConfig {
    /// Max `HistoricalReplay` ("easy positive") lines kept per family,
    /// prioritizing the smallest top1/top2 retrieval margin (nearest the
    /// decision boundary — the most informative easy positives). Every
    /// other stratum is uncapped.
    pub max_easy_positives_per_family: usize,
}

impl Default for CurationConfig {
    fn default() -> Self {
        Self {
            max_easy_positives_per_family: 3,
        }
    }
}

/// What [`curate_synthetic`]/[`RetrainPlan::run`] curated — spec §B10:
/// "Report the curated mix".
#[derive(Debug, Clone, PartialEq)]
pub struct CurationReport {
    pub stratum_counts: BTreeMap<String, usize>,
    /// How many `HistoricalReplay` candidates were dropped by the
    /// per-family cap (never a safety/correction/adversarial/no-call
    /// line — those strata are never capped).
    pub capped_easy_positive_count: usize,
    pub families_represented: usize,
    pub total_curated: usize,
}

/// Convenience: derive the [`FamilyId`] a synthetic [`EvalCase`]'s
/// `retrieved` top-k most directly represents — the unit
/// [`curate_synthetic`]'s per-family cap groups on.
pub fn family_of(case: &EvalCase) -> Option<FamilyId> {
    case.retrieved.first().map(|c| c.family_id.clone())
}

fn classify_stratum(case: &EvalCase) -> ReplayStratum {
    if case.expected.false_merge_trap
        || case.expected.false_split_trap
        || case.category == Category::IncompatibleUnsafe
    {
        ReplayStratum::ImmutableGolden
    } else if case.category == Category::AmbiguousAdversarial {
        ReplayStratum::RareAdversarial
    } else if matches!(case.expected.decision, InferenceDecision::Abstain { .. }) {
        ReplayStratum::NoCall
    } else {
        ReplayStratum::HistoricalReplay
    }
}

/// Top1/top2 retrieval margin — smaller means nearer the decision
/// boundary, i.e. more informative (spec §B10: "prioritize informative
/// examples ... near the decision boundary"). A case with fewer than two
/// retrieved candidates has no margin to compute; treated as the LEAST
/// informative (capped first) since a single-candidate case is
/// structurally the easiest possible positive.
fn margin_of(case: &EvalCase) -> f32 {
    if case.retrieved.len() >= 2 {
        (case.retrieved[1].distance - case.retrieved[0].distance).abs()
    } else {
        f32::MAX
    }
}

fn render_case_line(case: &EvalCase) -> String {
    deblob_eval::render_finetune_jsonl(std::slice::from_ref(case))
        .trim_end()
        .to_string()
}

/// Spec §B9/§B10: stratifies `train_cases` and applies the
/// `HistoricalReplay` per-family cap, prioritizing the smallest retrieval
/// margin (see [`margin_of`]) within each family. Pure and synchronous —
/// no I/O, independently unit-testable from the async pipeline.
pub fn curate_synthetic(
    train_cases: &[EvalCase],
    config: &CurationConfig,
) -> (Vec<ReplayLine>, usize) {
    let mut lines = Vec::new();
    let mut easy_by_family: BTreeMap<String, Vec<(&EvalCase, f32)>> = BTreeMap::new();

    for case in train_cases {
        let stratum = classify_stratum(case);
        if stratum == ReplayStratum::HistoricalReplay {
            let key = family_of(case)
                .map(|f| f.as_str().to_string())
                .unwrap_or_default();
            easy_by_family
                .entry(key)
                .or_default()
                .push((case, margin_of(case)));
            continue;
        }
        lines.push(ReplayLine {
            stratum,
            family_id: family_of(case).map(|f| f.as_str().to_string()),
            json_line: render_case_line(case),
        });
    }

    let mut capped_count = 0usize;
    for cases in easy_by_family.values_mut() {
        cases.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        for (i, (case, _margin)) in cases.iter().enumerate() {
            if i < config.max_easy_positives_per_family {
                lines.push(ReplayLine {
                    stratum: ReplayStratum::HistoricalReplay,
                    family_id: family_of(case).map(|f| f.as_str().to_string()),
                    json_line: render_case_line(case),
                });
            } else {
                capped_count += 1;
            }
        }
    }

    (lines, capped_count)
}

fn curation_report(lines: &[ReplayLine], capped_easy_positive_count: usize) -> CurationReport {
    let set = ReplaySet {
        lines: lines.to_vec(),
    };
    CurationReport {
        stratum_counts: set.stratum_counts(),
        capped_easy_positive_count,
        families_represented: set.families_represented(),
        total_curated: set.len(),
    }
}

// ---------------------------------------------------------------------
// External fine-tune hook boundary
// ---------------------------------------------------------------------

/// The artifact an external fine-tune hook produces: enough identity to
/// register a [`ModelVersion`] candidate, never the weights themselves
/// (Deblob does not serve or store models). Spec §B8: the TRAINING
/// checkpoint and the QUANTIZED artifact are separate digests — the gate
/// evaluates the latter.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ModelArtifact {
    pub model_id: String,
    pub training_checkpoint_digest: String,
    pub quantized_weights_digest: String,
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
/// fake) turns `base_snapshot` (spec §B9: a fixed, reproducible base —
/// never "whatever is currently active") + a [`ReplaySet`] into a
/// [`ModelArtifact`] and nothing more — this is the one place in the
/// whole loop where "did the model actually get better" is someone
/// else's job.
#[async_trait]
pub trait FineTuneHook: Send + Sync {
    async fn train(
        &self,
        base_snapshot: &str,
        replay_set: &ReplaySet,
    ) -> Result<ModelArtifact, FineTuneError>;
}

/// Real [`FineTuneHook`]: writes `replay_set.to_jsonl()` to a temp file
/// and shells out to a configured command (e.g. a Needle `finetune` / HF
/// wrapper script), appending `base_snapshot` then the temp file's path
/// as the final two arguments. The command's stdout MUST be exactly one
/// line of `{"model_id": "...", "training_checkpoint_digest": "...",
/// "quantized_weights_digest": "..."}` JSON — anything else is a
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
    async fn train(
        &self,
        base_snapshot: &str,
        replay_set: &ReplaySet,
    ) -> Result<ModelArtifact, FineTuneError> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = std::env::temp_dir().join(format!(
            "deblob-retrain-{}-{nanos}.jsonl",
            std::process::id()
        ));
        tokio::fs::write(&tmp, replay_set.to_jsonl())
            .await
            .map_err(|e| FineTuneError::Process(format!("write replay set jsonl: {e}")))?;

        let output = tokio::process::Command::new(&self.command)
            .args(&self.args)
            .arg(base_snapshot)
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

// ---------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------

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

/// What [`RetrainPlan::run`] hands back — spec §B7: `gate_decision` can
/// only ever be `EnteredShadow` or `Rejected`, NEVER a promotion, because
/// this module never calls `ModelRegistry::promote`. `curation_report` is
/// spec §B10's "report the curated mix".
#[derive(Debug, Clone, PartialEq)]
pub struct RetrainOutcome {
    pub gate_decision: GateDecision,
    pub curation_report: CurationReport,
}

/// Orchestrates one retrain-and-gate cycle. See the module docs for the
/// step-by-step boundary each argument owns.
pub struct RetrainPlan;

impl RetrainPlan {
    /// Runs one full cycle:
    ///
    /// 1. Curate an active-learning [`ReplaySet`] (spec §B9/§B10) from
    ///    `synthetic_corpus`'s `Partition::Train` cases (stratified,
    ///    `HistoricalReplay` capped per family) plus every durable
    ///    `feedback` example (all `RecentCorrection`, uncapped here — the
    ///    anti-poisoning cap already ran upstream in `FeedbackStore`).
    /// 2. Hand `base_snapshot` (spec §B9: FIXED, never derived from
    ///    whatever is currently active/shadow) + the replay set to
    ///    `fine_tune_hook` — external, no gradient step runs in this
    ///    process.
    /// 3. Evaluate the returned [`ModelArtifact`]'s QUANTIZED weights
    ///    (spec §B8) via `eval_endpoint` against `synthetic_corpus`'s
    ///    `Partition::Test` (held-out) slice, using the SAME
    ///    `deblob_eval::{run_eval, compute_metrics}` the offline eval
    ///    harness uses, and build a
    ///    `crate::model_registry::GateEvidence` bundle from it.
    /// 4. Register the candidate (`ModelState::Candidate`, bare — spec
    ///    §B7) and call `registry.attach_evidence` — the ONLY registry
    ///    call this function ever makes beyond `register_candidate`. It
    ///    never calls `registry.promote`.
    ///
    /// `Err(RetrainError::NoHoldout)` before touching `feedback`,
    /// `fine_tune_hook`, or `registry` at all if `synthetic_corpus` has no
    /// `Partition::Test` case — there would be nothing to gate the
    /// candidate against.
    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        feedback: &dyn FeedbackStore,
        synthetic_corpus: &[EvalCase],
        base_snapshot: &str,
        bundle_template: &BundleTemplate,
        curation: &CurationConfig,
        fine_tune_hook: &dyn FineTuneHook,
        eval_endpoint: &dyn SemanticInferencer,
        registry: &dyn ModelRegistry,
        gate: &GateConfig,
    ) -> Result<RetrainOutcome, RetrainError> {
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

        // 1. Curate: synthetic Partition::Train, stratified + capped,
        // plus every durable feedback example (RecentCorrection).
        let (mut lines, capped_easy_positive_count) = curate_synthetic(&train_cases, curation);
        let mut feedback_buf: Vec<u8> = Vec::new();
        let feedback_count = feedback.export_jsonl(&mut feedback_buf, None).await?;
        let feedback_text = String::from_utf8(feedback_buf)
            .expect("export_jsonl always writes valid UTF-8 JSON lines");
        for raw_line in feedback_text.lines() {
            if raw_line.trim().is_empty() {
                continue;
            }
            let family_id = serde_json::from_str::<serde_json::Value>(raw_line)
                .ok()
                .and_then(|v| {
                    v.get("partition_key")
                        .and_then(|p| p.as_str().map(|s| s.to_string()))
                });
            lines.push(ReplayLine {
                stratum: ReplayStratum::RecentCorrection,
                family_id,
                json_line: raw_line.to_string(),
            });
        }
        let report = curation_report(&lines, capped_easy_positive_count);
        let replay_set = ReplaySet { lines };

        // 2. External hook — no gradient step runs here. `base_snapshot`
        // is exactly the caller-supplied constant, never derived from
        // `registry.get_active()` — this is the structural proof that
        // retraining never recursively mutates "the latest adapter"
        // (spec §B9).
        let artifact = fine_tune_hook.train(base_snapshot, &replay_set).await?;

        // 3. Evaluate the QUANTIZED artifact against the held-out gate
        // corpus only — the candidate never saw these families during
        // step 1's curated export.
        let run = run_eval(eval_endpoint, &holdout_cases).await;
        let metrics = compute_metrics(&run, &holdout_cases);
        let evidence = GateEvidence::from_eval(&run, &metrics, now_ms(), gate.confidence_z);

        let bundle =
            ArtifactBundle::new(artifact.quantized_weights_digest.clone(), bundle_template);
        let candidate = ModelVersion {
            model_id: artifact.model_id,
            bundle,
            training_checkpoint_digest: artifact.training_checkpoint_digest,
            trained_from: TrainedFrom {
                base_snapshot_id: base_snapshot.to_string(),
                feedback_cursor: format!("feedback_examples={feedback_count}"),
                corpus_seed: format!(
                    "synthetic_train_cases={} synthetic_holdout_cases={} curated_lines={}",
                    train_cases.len(),
                    holdout_cases.len(),
                    replay_set.len()
                ),
            },
            evidence: None,
            recorded_at: now_ms(),
            shadow_since: None,
            state: ModelState::Candidate,
        };
        registry.register_candidate(candidate.clone()).await?;

        // 4. Evaluation evidence only — NEVER a promotion. See the
        // module docs and this file's
        // `retrain_plan_never_moves_the_active_alias_even_when_the_offline_gate_passes`
        // test.
        let gate_decision = registry
            .attach_evidence(&candidate.model_id, evidence, gate)
            .await?;

        Ok(RetrainOutcome {
            gate_decision,
            curation_report: report,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use deblob_core::id::SchemaId;
    use deblob_eval::{Category as EvalCategory, Expected};
    use deblob_slm::{
        CandidateProfileView, FamilyCandidate, InferenceDecision, InferenceError, InferenceOutcome,
        InferenceRequest, InferenceTelemetry, Relation, TrainingExample,
    };
    use std::collections::BTreeMap as StdBTreeMap;

    use crate::model_registry::{
        GateConfig, GateDecision, ModelRegistry, ModelState, PromotionApproval,
    };

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

    /// A gate permissive enough for pipeline-plumbing tests (the gate
    /// MATH itself is exhaustively unit-tested in `model_registry`): a
    /// 1-case held-out corpus can't clear the default `min_test_n`/
    /// `per_family_min_n`/`false_merge_upper_ci` bars, so those are
    /// relaxed here to isolate what this module's tests actually assert.
    fn permissive_gate() -> GateConfig {
        GateConfig {
            min_test_n: 1,
            per_family_min_n: 1,
            per_family_precision_floor: 0.0,
            max_false_merge_upper_ci: 1.0,
            ..GateConfig::default()
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
                category: EvalCategory::KnownExact,
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
                category: EvalCategory::KnownExact,
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
        quarantined: Mutex<std::collections::BTreeSet<String>>,
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
                    "partition_key": ex.partition_key.as_str(),
                });
                writeln!(writer, "{}", serde_json::to_string(&line).unwrap()).unwrap();
                count += 1;
            }
            Ok(count)
        }
        async fn iter_by_partition(
            &self,
        ) -> Result<StdBTreeMap<String, Vec<TrainingExample>>, deblob_core::error::CoreError>
        {
            let mut map: StdBTreeMap<String, Vec<TrainingExample>> = StdBTreeMap::new();
            for ex in self.examples.lock().unwrap().iter() {
                map.entry(ex.partition_key.as_str().to_string())
                    .or_default()
                    .push(ex.clone());
            }
            Ok(map)
        }

        async fn quarantine_actor(&self, actor: &str) -> Result<(), deblob_core::error::CoreError> {
            self.quarantined.lock().unwrap().insert(actor.to_string());
            Ok(())
        }

        async fn quarantined_actors(
            &self,
        ) -> Result<std::collections::BTreeSet<String>, deblob_core::error::CoreError> {
            Ok(self.quarantined.lock().unwrap().clone())
        }

        async fn export_snapshot(
            &self,
            _dir: &std::path::Path,
        ) -> Result<deblob_redis::ExportManifest, deblob_core::error::CoreError> {
            unimplemented!("not exercised by RetrainPlan tests")
        }
    }

    struct FakeFineTuneHook {
        artifact: ModelArtifact,
        calls: AtomicUsize,
        last_base_snapshot: Mutex<String>,
        last_replay_set: Mutex<ReplaySet>,
    }

    impl FakeFineTuneHook {
        fn new(artifact: ModelArtifact) -> Self {
            Self {
                artifact,
                calls: AtomicUsize::new(0),
                last_base_snapshot: Mutex::new(String::new()),
                last_replay_set: Mutex::new(ReplaySet::default()),
            }
        }
    }

    #[async_trait]
    impl FineTuneHook for FakeFineTuneHook {
        async fn train(
            &self,
            base_snapshot: &str,
            replay_set: &ReplaySet,
        ) -> Result<ModelArtifact, FineTuneError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_base_snapshot.lock().unwrap() = base_snapshot.to_string();
            *self.last_replay_set.lock().unwrap() = replay_set.clone();
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

        async fn get(
            &self,
            model_id: &str,
        ) -> Result<Option<ModelVersion>, deblob_core::error::CoreError> {
            Ok(self.models.lock().unwrap().get(model_id).cloned())
        }

        async fn attach_evidence(
            &self,
            model_id: &str,
            evidence: crate::model_registry::GateEvidence,
            gate: &GateConfig,
        ) -> Result<GateDecision, deblob_core::error::CoreError> {
            let mut candidate = self
                .models
                .lock()
                .unwrap()
                .get(model_id)
                .cloned()
                .ok_or(deblob_core::error::CoreError::NotFound)?;
            if candidate.state != ModelState::Candidate {
                return Err(deblob_core::error::CoreError::Conflict(
                    "not in Candidate state".into(),
                ));
            }
            let active = self.get_active().await?;
            let mut reasons = crate::model_registry::gate_reasons(&evidence, gate);
            if let Some(active_version) = &active {
                if let Some(active_evidence) = &active_version.evidence {
                    reasons.extend(crate::model_registry::regression_reasons(
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
        ) -> Result<ModelVersion, deblob_core::error::CoreError> {
            let mut candidate = self
                .models
                .lock()
                .unwrap()
                .get(model_id)
                .cloned()
                .ok_or(deblob_core::error::CoreError::NotFound)?;
            if candidate.state != ModelState::ShadowCandidate {
                return Err(deblob_core::error::CoreError::Conflict(
                    "not in ShadowCandidate state".into(),
                ));
            }
            if gate.require_explicit_approval && !approval.approved {
                return Err(deblob_core::error::CoreError::PolicyRejected(
                    "approval required".into(),
                ));
            }
            candidate.state = ModelState::Active;
            self.models
                .lock()
                .unwrap()
                .insert(candidate.model_id.clone(), candidate.clone());
            *self.active.lock().unwrap() = Some(candidate.model_id.clone());
            Ok(candidate)
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

    async fn seed_hard_negative_feedback(feedback: &FakeFeedbackStore) {
        let rejected = schema_id(9);
        let ctx = crate::feedback::CaptureContext {
            actor: "operator:seed".to_string(),
            source_trust_level: deblob_slm::SourceTrustLevel::Standard,
            tool_schema_version: 1,
            dedup_cluster: String::new(),
            weights: crate::feedback::FeedbackWeights::default(),
            partition_key: family(),
            recorded_at: 1,
        };
        let example = crate::feedback::capture_trusted_proposal_rejected(
            candidate_view(),
            vec![],
            &rejected,
            deblob_slm::RejectionReason::WrongFamily,
            None,
            &ctx,
        )
        .expect("WrongFamily is a generator fault, must be emitted");
        feedback.append(&example).await.unwrap();
    }

    #[tokio::test]
    async fn end_to_end_pipeline_produces_a_shadow_candidate_never_directly_active() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        // Seed one hard-negative feedback example to prove step 1 combines
        // both sources.
        seed_hard_negative_feedback(&feedback).await;

        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-v1".to_string(),
            training_checkpoint_digest: "sha256:ckpt-aaaa".to_string(),
            quantized_weights_digest: "sha256:quant-aaaa".to_string(),
        });
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let registry = FakeModelRegistry::default();
        let gate = permissive_gate();
        let template = bundle_template();
        let curation = CurationConfig::default();

        let outcome = RetrainPlan::run(
            &feedback,
            &corpus,
            "base-snapshot-v0",
            &template,
            &curation,
            &hook,
            &inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();

        match &outcome.gate_decision {
            GateDecision::EnteredShadow(v) => {
                assert_eq!(v.model_id, "model-v1");
                assert_eq!(v.state, ModelState::ShadowCandidate);
                assert_eq!(
                    v.bundle.weights_digest, "sha256:quant-aaaa",
                    "the bundle must carry the QUANTIZED digest, not the training checkpoint"
                );
                assert_eq!(v.training_checkpoint_digest, "sha256:ckpt-aaaa");
                assert_eq!(v.trained_from.base_snapshot_id, "base-snapshot-v0");
                let evidence = v.evidence.as_ref().expect("evidence must be attached");
                assert_eq!(evidence.aggregate.exact_semantic_accuracy, 1.0);
            }
            other => panic!("expected EnteredShadow, got {other:?}"),
        }
        assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
        assert_eq!(*hook.last_base_snapshot.lock().unwrap(), "base-snapshot-v0");
        // The curated replay set must have combined BOTH the synthetic
        // Train case and the feedback example.
        let replay = hook.last_replay_set.lock().unwrap().clone();
        assert_eq!(
            replay.len(),
            2,
            "1 synthetic train case + 1 feedback example"
        );
        assert_eq!(
            *replay
                .stratum_counts()
                .get(ReplayStratum::RecentCorrection.as_str())
                .unwrap(),
            1
        );

        // Spec §B7/§B11: an offline-gate pass alone must NEVER move the
        // active alias.
        assert!(
            registry.get_active().await.unwrap().is_none(),
            "attach_evidence passing the gate must not itself activate the candidate"
        );

        assert_eq!(
            outcome.curation_report.total_curated, 2,
            "curation report must account for both curated lines"
        );
    }

    /// Spec §B7 acceptance: `RetrainPlan` never holds (or constructs) a
    /// path to `ModelRegistry::promote` — only an explicit, separately-
    /// invoked `promote` call (with approval) ever moves the active
    /// alias.
    #[tokio::test]
    async fn retrain_plan_never_moves_the_active_alias_even_when_the_offline_gate_passes() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-sep".to_string(),
            training_checkpoint_digest: "sha256:ckpt-sep".to_string(),
            quantized_weights_digest: "sha256:quant-sep".to_string(),
        });
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let registry = FakeModelRegistry::default();
        let gate = permissive_gate();
        let template = bundle_template();
        let curation = CurationConfig::default();

        let outcome = RetrainPlan::run(
            &feedback,
            &corpus,
            "base-snapshot-v0",
            &template,
            &curation,
            &hook,
            &inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();
        assert!(matches!(
            outcome.gate_decision,
            GateDecision::EnteredShadow(_)
        ));
        assert!(registry.get_active().await.unwrap().is_none());

        // Only an EXPLICIT, separately-invoked promote (with approval)
        // ever moves the alias.
        let promoted = registry
            .promote(
                "model-sep",
                PromotionApproval {
                    approved: true,
                    actor: "ops:kamil".to_string(),
                },
                &gate,
            )
            .await
            .unwrap();
        assert_eq!(promoted.state, ModelState::Active);
        assert_eq!(
            registry.get_active().await.unwrap().unwrap().model_id,
            "model-sep"
        );
    }

    #[tokio::test]
    async fn a_gate_failing_candidate_is_rejected_and_the_active_model_is_unchanged() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        let hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-bad".to_string(),
            training_checkpoint_digest: "sha256:ckpt-bad".to_string(),
            quantized_weights_digest: "sha256:quant-bad".to_string(),
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
        let gate = permissive_gate();
        let template = bundle_template();
        let curation = CurationConfig::default();

        let outcome = RetrainPlan::run(
            &feedback,
            &corpus,
            "base-snapshot-v0",
            &template,
            &curation,
            &hook,
            &inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();

        match outcome.gate_decision {
            GateDecision::Rejected { reasons, candidate } => {
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
        let gate = permissive_gate();
        let template = bundle_template();
        let curation = CurationConfig::default();

        // First run: a good model enters shadow, then is explicitly
        // promoted to active.
        let good_hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-good".to_string(),
            training_checkpoint_digest: "sha256:ckpt-good".to_string(),
            quantized_weights_digest: "sha256:quant-good".to_string(),
        });
        let good_inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let first = RetrainPlan::run(
            &feedback,
            &corpus,
            "base-snapshot-v0",
            &template,
            &curation,
            &good_hook,
            &good_inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();
        assert!(matches!(
            first.gate_decision,
            GateDecision::EnteredShadow(_)
        ));
        registry
            .promote(
                "model-good",
                PromotionApproval {
                    approved: true,
                    actor: "ops:kamil".to_string(),
                },
                &gate,
            )
            .await
            .unwrap();

        // Second run: a worse model (wrong on the held-out case) must be
        // rejected, and the active model must stay the first one.
        let worse_hook = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-worse".to_string(),
            training_checkpoint_digest: "sha256:ckpt-worse".to_string(),
            quantized_weights_digest: "sha256:quant-worse".to_string(),
        });
        let worse_inferencer = ScriptedInferencer {
            mode: ScriptedMode::FalseMerge(schema_id(201)),
        };
        let second = RetrainPlan::run(
            &feedback,
            &corpus,
            "base-snapshot-v0",
            &template,
            &curation,
            &worse_hook,
            &worse_inferencer,
            &registry,
            &gate,
        )
        .await
        .unwrap();
        assert!(matches!(
            second.gate_decision,
            GateDecision::Rejected { .. }
        ));

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
            training_checkpoint_digest: "sha256:none".to_string(),
            quantized_weights_digest: "sha256:none".to_string(),
        });
        let inferencer = ScriptedInferencer {
            mode: ScriptedMode::AlwaysCorrect,
        };
        let registry = FakeModelRegistry::default();
        let template = bundle_template();
        let curation = CurationConfig::default();

        let err = RetrainPlan::run(
            &feedback,
            &train_only,
            "base-snapshot-v0",
            &template,
            &curation,
            &hook,
            &inferencer,
            &registry,
            &GateConfig::default(),
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

    #[tokio::test]
    async fn repeated_runs_record_the_same_caller_supplied_base_snapshot_not_a_derived_one() {
        let corpus = tiny_corpus();
        let feedback = FakeFeedbackStore::default();
        let registry = FakeModelRegistry::default();
        let gate = permissive_gate();
        let template = bundle_template();
        let curation = CurationConfig::default();

        let hook_a = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-a".to_string(),
            training_checkpoint_digest: "sha256:ckpt-a".to_string(),
            quantized_weights_digest: "sha256:quant-a".to_string(),
        });
        let outcome_a = RetrainPlan::run(
            &feedback,
            &corpus,
            "fixed-base-snapshot",
            &template,
            &curation,
            &hook_a,
            &ScriptedInferencer {
                mode: ScriptedMode::AlwaysCorrect,
            },
            &registry,
            &gate,
        )
        .await
        .unwrap();
        registry
            .promote(
                "model-a",
                PromotionApproval {
                    approved: true,
                    actor: "ops:kamil".to_string(),
                },
                &gate,
            )
            .await
            .unwrap();

        let hook_b = FakeFineTuneHook::new(ModelArtifact {
            model_id: "model-b".to_string(),
            training_checkpoint_digest: "sha256:ckpt-b".to_string(),
            quantized_weights_digest: "sha256:quant-b".to_string(),
        });
        let outcome_b = RetrainPlan::run(
            &feedback,
            &corpus,
            "fixed-base-snapshot",
            &template,
            &curation,
            &hook_b,
            &ScriptedInferencer {
                mode: ScriptedMode::AlwaysCorrect,
            },
            &registry,
            &gate,
        )
        .await
        .unwrap();

        let base_of = |decision: &GateDecision| match decision {
            GateDecision::EnteredShadow(v) => v.trained_from.base_snapshot_id.clone(),
            GateDecision::Rejected { candidate, .. } => {
                candidate.trained_from.base_snapshot_id.clone()
            }
        };
        assert_eq!(base_of(&outcome_a.gate_decision), "fixed-base-snapshot");
        assert_eq!(
            base_of(&outcome_b.gate_decision),
            "fixed-base-snapshot",
            "base_snapshot must stay the caller-supplied constant across runs — RetrainPlan \
             must never derive it from the model that got promoted/rejected last time"
        );
    }

    // -- curation ---------------------------------------------------------

    fn case_with_margin(
        name: &str,
        category: EvalCategory,
        gold: InferenceDecision,
        family_id: FamilyId,
        margin: f32,
        traps: (bool, bool),
    ) -> EvalCase {
        let sid = SchemaId::from_digest(&[margin.to_bits() as u8; 32]);
        EvalCase {
            name: name.to_string(),
            category,
            candidate: candidate_view(),
            retrieved: vec![
                fc(&sid, family_id.clone(), 1, 0.0),
                fc(&SchemaId::from_digest(&[9; 32]), family_id, 2, margin),
            ],
            expected: Expected {
                decision: gold.clone(),
                gold_schema_id: match &gold {
                    InferenceDecision::MatchSchema { schema_id, .. } => Some(schema_id.clone()),
                    _ => None,
                },
                gold_rank: Some(1),
                false_merge_trap: traps.0,
                false_split_trap: traps.1,
            },
            partition: Partition::Train,
        }
    }

    #[test]
    fn easy_positives_are_capped_per_family_keeping_the_smallest_margin_first() {
        let fam = family();
        let mut cases = Vec::new();
        for i in 0..5u8 {
            let sid = SchemaId::from_digest(&[100 + i; 32]);
            cases.push(case_with_margin(
                &format!("easy-{i}"),
                EvalCategory::KnownExact,
                InferenceDecision::MatchSchema {
                    schema_id: sid,
                    relation: Relation::Exact,
                },
                fam.clone(),
                f32::from(i) * 0.1, // increasing margin: case 0 is most informative
                (false, false),
            ));
        }
        let config = CurationConfig {
            max_easy_positives_per_family: 2,
        };
        let (lines, capped) = curate_synthetic(&cases, &config);
        assert_eq!(capped, 3, "5 easy positives capped to 2 → 3 dropped");
        assert_eq!(lines.len(), 2);
        assert!(lines
            .iter()
            .all(|l| l.stratum == ReplayStratum::HistoricalReplay));
        let names: Vec<String> = lines
            .iter()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(&l.json_line)
                    .unwrap()
                    .get("case_name")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert!(
            names.contains(&"easy-0".to_string()),
            "smallest-margin cases must be kept: {names:?}"
        );
        assert!(
            names.contains(&"easy-1".to_string()),
            "smallest-margin cases must be kept: {names:?}"
        );
    }

    #[test]
    fn safety_critical_and_rare_and_no_call_strata_are_never_capped() {
        let fam = family();
        let sid = SchemaId::from_digest(&[7; 32]);
        let golden = case_with_margin(
            "golden",
            EvalCategory::KnownExact,
            InferenceDecision::MatchSchema {
                schema_id: sid.clone(),
                relation: Relation::Exact,
            },
            fam.clone(),
            0.0,
            (true, false), // false_merge_trap
        );
        let adversarial = case_with_margin(
            "adversarial",
            EvalCategory::AmbiguousAdversarial,
            InferenceDecision::MatchSchema {
                schema_id: sid.clone(),
                relation: Relation::Exact,
            },
            fam.clone(),
            0.0,
            (false, false),
        );
        let no_call = case_with_margin(
            "no-call",
            EvalCategory::KnownExact,
            InferenceDecision::Abstain {
                cause: deblob_slm::AbstainCause::Ambiguous,
            },
            fam,
            0.0,
            (false, false),
        );
        let config = CurationConfig {
            max_easy_positives_per_family: 0, // would cap ANY historical-replay line
        };
        let (lines, capped) = curate_synthetic(
            &[golden.clone(), adversarial.clone(), no_call.clone()],
            &config,
        );
        assert_eq!(capped, 0);
        assert_eq!(lines.len(), 3);
        let strata: Vec<ReplayStratum> = lines.iter().map(|l| l.stratum).collect();
        assert!(strata.contains(&ReplayStratum::ImmutableGolden));
        assert!(strata.contains(&ReplayStratum::RareAdversarial));
        assert!(strata.contains(&ReplayStratum::NoCall));
    }

    #[test]
    fn replay_set_to_jsonl_tags_every_line_with_its_stratum() {
        let set = ReplaySet {
            lines: vec![ReplayLine {
                stratum: ReplayStratum::RecentCorrection,
                family_id: Some("fam_x".to_string()),
                json_line: r#"{"prompt":"p","gold_tool_call":{}}"#.to_string(),
            }],
        };
        let jsonl = set.to_jsonl();
        let value: serde_json::Value = serde_json::from_str(jsonl.trim()).unwrap();
        assert_eq!(value["replay_stratum"], "recent_correction");
    }
}
