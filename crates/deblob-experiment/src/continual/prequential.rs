//! The prequential test-then-train loop (spec §7, verbatim):
//!
//! ```text
//! Round r:
//!   1. Evaluate model-r on the NEXT chronological batch BEFORE revealing labels.
//!   2. Record predictions permanently.
//!   3. Reveal feedback for that batch -> feedback store.
//!   4. Train model-(r+1) from the STABLE BASE using cumulative/replay data
//!      (not recursively merged weights).
//!   5. Evaluate retention on FROZEN historical slices.
//! ```
//!
//! This module ORCHESTRATES `deblob::feedback`/`deblob::retrain`/
//! `deblob::model_registry` — every one of those is reused VERBATIM (never
//! reimplemented, never weakened): [`RetrainPlan::run`] still holds no path
//! to `ModelRegistry::promote`, the statistical gate still runs unmodified,
//! and `capture_semantic_annotation` (an EXISTING `deblob::feedback`
//! constructor — "a controlled-vocabulary annotation supplies gold
//! directly") is the reveal-feedback primitive, since a prequential round's
//! label is the external ground truth, not a trust-gate accept/reject
//! verdict.
//!
//! Three datasets are kept STRICTLY separate (spec §7): the chronological
//! `round stream` (sliced into `PrequentialConfig::num_rounds` batches), the
//! `development set` (fed to `RetrainPlan::run` every round as its
//! curation-source-and-gate-holdout corpus — hyperparameters/replay ratios/
//! promotion thresholds are tuned against this, never the audit set), and
//! the SEALED final audit set — a private field with no accessor anywhere
//! in this module except [`PrequentialRunner::freeze`], reachable only
//! after every round has run.

use std::sync::Arc;

use deblob::feedback::{capture_semantic_annotation, CaptureContext, FeedbackWeights};
use deblob::model_registry::{BundleTemplate, GateConfig, GateDecision, ModelRegistry};
use deblob::retrain::{family_of, CurationConfig, FineTuneHook, RetrainPlan};
use deblob_core::error::CoreError;
use deblob_core::id::FamilyId;
use deblob_eval::EvalCase;
use deblob_redis::FeedbackStore;
use deblob_slm::runtime::ModelBundle;
use deblob_slm::SourceTrustLevel;

use crate::arms::gate::GatedArm;
use crate::arms::semantic::SemanticArm;
use crate::arms::{Arm, ArmDecision, ArmId};
use crate::continual::datasets::{self, PrequentialConfig};
use crate::continual::metrics::accuracy;
use crate::continual::training_job::TrainingMethod;
use crate::labels::split_case;
use crate::metrics::l4_utility::{compute_l4, L4CaseView, UtilityReport};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// One round's permanent, never-mutated-after-the-fact record.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RoundRecord {
    pub round: usize,
    /// Model-r's decisions on this round's batch, recorded BEFORE feedback
    /// was revealed (spec §7 step 1/2) — permanent, never recomputed.
    pub predictions: Vec<ArmDecision>,
    pub gate_passed: bool,
    pub gate_reasons: Vec<String>,
    pub feedback_revealed: usize,
    /// Spec §7: performance on the NEXT chronological batch, model-(r+1)
    /// minus model-r — `None` for the final round (no future batch yet).
    pub adaptation_gain: Option<f64>,
    /// Spec §7: regression on every FROZEN historical batch, model-r minus
    /// model-(r+1) (positive = forgetting) — `None` for round 0.
    pub retention_loss: Option<f64>,
    /// Spec §8 reporter wiring: which training method/runtime this round's
    /// model uses — reuses Task 3's `RuntimeInfo`/`ModelFamily` tag, so
    /// Needle is labeled `needle-custom` distinctly, never lumped in with
    /// `lora-sft`.
    pub method: String,
    pub runtime: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PrequentialError {
    #[error("round stream has only {available} cases, need at least {needed}")]
    InsufficientRoundStream { needed: usize, available: usize },
    #[error("all rounds already completed")]
    AllRoundsComplete,
    #[error("cannot freeze: {completed}/{total} rounds completed")]
    TrajectoryNotComplete { completed: usize, total: usize },
    #[error("feedback store error: {0}")]
    Store(#[from] CoreError),
    #[error("retrain error: {0}")]
    Retrain(#[from] deblob::retrain::RetrainError),
}

/// Runs the spec §7 loop round by round. See the module docs for the
/// dataset-separation + reuse discipline.
pub struct PrequentialRunner {
    round_batches: Vec<Vec<EvalCase>>,
    dev_corpus: Vec<EvalCase>,
    audit: datasets::SealedAuditSet,
    /// FIXED across every round (spec §7/§B9): never derived from a prior
    /// round's own candidate.
    base_snapshot: String,
    bundle_template: BundleTemplate,
    curation: CurationConfig,
    gate: GateConfig,
    current_round: usize,
    total_rounds: usize,
    current_model: ModelBundle,
    b_v0_model: ModelBundle,
    /// Produces round `r+1`'s candidate model, given `r+1`. A deterministic
    /// function of the round index — real deployments would instead read
    /// back whatever endpoint the trained adapter now serves; this harness
    /// keeps that pluggable exactly like Task 1/3's `SemanticInferencer`
    /// seam.
    candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync>,
    /// Every batch already revealed — the FROZEN retention-probe pool.
    seen_batches: Vec<Vec<EvalCase>>,
    history: Vec<RoundRecord>,
}

fn runtime_label(bundle: &ModelBundle) -> String {
    format!(
        "{}:{}",
        bundle.runtime.backend.label(),
        bundle.runtime.model_id
    )
}

impl PrequentialRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: &PrequentialConfig,
        base_snapshot: impl Into<String>,
        bundle_template: BundleTemplate,
        curation: CurationConfig,
        gate: GateConfig,
        b_v0_model: ModelBundle,
        candidate_factory: Arc<dyn Fn(usize) -> ModelBundle + Send + Sync>,
    ) -> Result<Self, PrequentialError> {
        let round_batches = datasets::round_batches(cfg)?;
        let dev_corpus = datasets::dev_corpus(cfg);
        let audit = datasets::audit_set(cfg);
        Ok(Self {
            round_batches,
            dev_corpus,
            audit,
            base_snapshot: base_snapshot.into(),
            bundle_template,
            curation,
            gate,
            current_round: 0,
            total_rounds: cfg.num_rounds,
            current_model: b_v0_model.clone(),
            b_v0_model,
            candidate_factory,
            seen_batches: Vec::new(),
            history: Vec::new(),
        })
    }

    pub fn rounds_completed(&self) -> &[RoundRecord] {
        &self.history
    }

    fn score(&self, model: &ModelBundle, cases: &[EvalCase]) -> f64 {
        if cases.is_empty() {
            return 0.0;
        }
        let arm = SemanticArm::new(ArmId::B1, Arc::clone(&model.inferencer));
        let mut decisions = Vec::with_capacity(cases.len());
        let mut expecteds = Vec::with_capacity(cases.len());
        for case in cases {
            let (input, sidecar) = split_case(case);
            decisions.push(arm.decide(&input));
            expecteds.push(sidecar.expected);
        }
        accuracy(&decisions, &expecteds)
    }

    /// Runs exactly one round of the spec §7 loop. Steps 1/2 (evaluate,
    /// THEN reveal) are two separate, sequential loops over `batch` so no
    /// label can possibly reach the model before every prediction in this
    /// round is already recorded.
    pub async fn run_round(
        &mut self,
        feedback: &dyn FeedbackStore,
        registry: &dyn ModelRegistry,
        fine_tune_hook: &dyn FineTuneHook,
        actor: &str,
    ) -> Result<&RoundRecord, PrequentialError> {
        if self.current_round >= self.total_rounds {
            return Err(PrequentialError::AllRoundsComplete);
        }
        let round = self.current_round;
        let batch = self.round_batches[round].clone();
        let model_before = self.current_model.clone();

        // Step 1: evaluate model-r on `batch` BEFORE any label in it is
        // revealed. `split_case` (Task 1's leak guard, reused verbatim)
        // never lets the gold decision reach `input`.
        let arm = GatedArm::new(
            ArmId::C {
                round: round as u32,
            },
            Box::new(SemanticArm::new(
                ArmId::B1,
                Arc::clone(&model_before.inferencer),
            )),
        );
        let mut predictions = Vec::with_capacity(batch.len());
        let mut expecteds = Vec::with_capacity(batch.len());
        for case in &batch {
            let (input, sidecar) = split_case(case);
            predictions.push(arm.decide_with_gate(&input).0);
            expecteds.push(sidecar.expected);
        }
        // `predictions` is now the PERMANENT record for round `round` —
        // nothing below ever mutates it.

        // Step 2: reveal feedback, now that every prediction above is
        // already captured.
        let mut feedback_revealed = 0usize;
        for case in &batch {
            let (input, sidecar) = split_case(case);
            let ctx = CaptureContext {
                actor: actor.to_string(),
                source_trust_level: SourceTrustLevel::Standard,
                tool_schema_version: 1,
                dedup_cluster: String::new(),
                weights: FeedbackWeights::default(),
                partition_key: family_of(case).unwrap_or_else(FamilyId::new_v7),
                recorded_at: now_ms(),
            };
            let example = capture_semantic_annotation(
                input.candidate.clone(),
                input.retrieved.clone(),
                sidecar.expected.decision.clone(),
                &ctx,
            );
            feedback.append(&example).await?;
            feedback_revealed += 1;
        }

        // Step 3: train model-(r+1) from the FIXED base via the external
        // hook, gated by the UNCHANGED statistical gate + two-stage
        // canary. `RetrainPlan` holds no path to `promote` — see its own
        // module docs; this orchestrator calls nothing beyond `run` here.
        let candidate_model = (self.candidate_factory)(round + 1);
        let outcome = RetrainPlan::run(
            feedback,
            &self.dev_corpus,
            &self.base_snapshot,
            &self.bundle_template,
            &self.curation,
            fine_tune_hook,
            candidate_model.inferencer.as_ref(),
            registry,
            &self.gate,
        )
        .await?;

        let (gate_passed, gate_reasons) = match &outcome.gate_decision {
            GateDecision::EnteredShadow(_) => {
                self.current_model = candidate_model.clone();
                (true, Vec::new())
            }
            GateDecision::Rejected { reasons, .. } => (false, reasons.clone()),
        };

        // Step 4/5: adaptation gain (future-slice) + retention loss
        // (frozen-slice regression) — analysis-only, never fed back into
        // either model.
        let adaptation_gain = if round + 1 < self.total_rounds {
            let next_batch = self.round_batches[round + 1].clone();
            let acc_new = self.score(&self.current_model, &next_batch);
            let acc_old = self.score(&model_before, &next_batch);
            Some(acc_new - acc_old)
        } else {
            None
        };
        let retention_loss = if self.seen_batches.is_empty() {
            None
        } else {
            let frozen: Vec<EvalCase> = self.seen_batches.iter().flatten().cloned().collect();
            let acc_old = self.score(&model_before, &frozen);
            let acc_new = self.score(&self.current_model, &frozen);
            Some(acc_old - acc_new)
        };

        self.seen_batches.push(batch);
        self.current_round += 1;

        let method = TrainingMethod::from_model_family(model_before.runtime.family);
        self.history.push(RoundRecord {
            round,
            predictions,
            gate_passed,
            gate_reasons,
            feedback_revealed,
            adaptation_gain,
            retention_loss,
            method: method.as_str().to_string(),
            runtime: runtime_label(&model_before),
        });
        Ok(self.history.last().expect("just pushed"))
    }

    /// Consumes the runner and returns a [`FrozenTrajectory`] — the ONLY
    /// path that ever exposes the sealed audit set, and only once every
    /// round has run.
    pub fn freeze(self) -> Result<FrozenTrajectory, PrequentialError> {
        if self.current_round != self.total_rounds {
            return Err(PrequentialError::TrajectoryNotComplete {
                completed: self.current_round,
                total: self.total_rounds,
            });
        }
        Ok(FrozenTrajectory {
            rounds: self.history,
            audit_cases: self.audit.cases,
            c_final_model: self.current_model,
            b_v0_model: self.b_v0_model,
        })
    }
}

/// A completed, sealed prequential trajectory. `audit_cases` is private and
/// has exactly ONE reader in this whole type: [`Self::c_final_vs_b_v0`].
pub struct FrozenTrajectory {
    rounds: Vec<RoundRecord>,
    audit_cases: Vec<EvalCase>,
    c_final_model: ModelBundle,
    b_v0_model: ModelBundle,
}

impl FrozenTrajectory {
    pub fn rounds(&self) -> &[RoundRecord] {
        &self.rounds
    }

    /// The ONLY way this type's sealed audit cases are ever read (spec §7:
    /// "never retrospectively pick the best round from the sealed
    /// trajectory"). Scores the frozen `C_final` model (whatever the last
    /// round settled on) against the frozen `B_v0` model (the model before
    /// any round ran), both through the SAME `GatedArm` every other arm in
    /// this crate reuses, and returns the SAME `UtilityReport`
    /// (contingency + McNemar + paired bootstrap CI)
    /// `metrics::l4_utility::compute_l4` produces for B1-vs-A1. There is
    /// deliberately no method here taking a round index or an arbitrary
    /// `Arm` — only these two fixed, already-decided models are ever
    /// scored against the audit set, and calling this twice with the same
    /// arguments always scores the identical pair (no hidden search over
    /// rounds).
    pub fn c_final_vs_b_v0(
        &self,
        bootstrap_seed: u64,
        bootstrap_iterations: usize,
    ) -> UtilityReport {
        let b_v0_arm = GatedArm::new(
            ArmId::C { round: 0 },
            Box::new(SemanticArm::new(
                ArmId::B1,
                Arc::clone(&self.b_v0_model.inferencer),
            )),
        );
        let c_final_arm = GatedArm::new(
            ArmId::C {
                round: self.rounds.len() as u32,
            },
            Box::new(SemanticArm::new(
                ArmId::B1,
                Arc::clone(&self.c_final_model.inferencer),
            )),
        );

        let mut a_decisions = Vec::with_capacity(self.audit_cases.len());
        let mut b_decisions = Vec::with_capacity(self.audit_cases.len());
        let mut expecteds = Vec::with_capacity(self.audit_cases.len());
        for case in &self.audit_cases {
            let (input, sidecar) = split_case(case);
            a_decisions.push(b_v0_arm.decide_with_gate(&input).0);
            b_decisions.push(c_final_arm.decide_with_gate(&input).0);
            expecteds.push(sidecar.expected);
        }
        let views: Vec<L4CaseView> = (0..self.audit_cases.len())
            .map(|i| L4CaseView {
                a_decision: &a_decisions[i],
                b_decision: &b_decisions[i],
                expected: &expecteds[i],
            })
            .collect();
        compute_l4(&views, bootstrap_seed, bootstrap_iterations)
    }
}

#[cfg(test)]
mod tests;
