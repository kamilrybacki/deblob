//! Governed model registry + gated promotion (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §4).
//!
//! Applies the SAME evidence discipline the schema registry
//! (`deblob_core::ports::Registry`) already holds for schema promotion to
//! MODEL VERSIONS: immutable records, an atomic + audited state
//! transition, and — the headline invariant — a candidate becomes `Active`
//! ONLY IF it both passes the go-live gate
//! (`docs/shadow-golive-gate.md`) on its own held-out metrics AND does not
//! regress against the current `Active` model on the SAME held-out set.
//! Everything else (a gate-failing OR regressing candidate) lands in
//! `Rejected`, audited with the exact reasons — never silently dropped,
//! never partially applied. `rollback` restores the immediately prior
//! `Active` model.
//!
//! # Why this lives in `deblob`, not `deblob-redis`
//!
//! `EvalMetricsSummary` is derived from `deblob_eval::{EvalRun, Metrics}` —
//! `deblob-redis` has (and should have) no dependency on the eval harness.
//! Keeping the trait + the Redis-backed implementation together here
//! mirrors how `crate::trusted`/`crate::policy` already hold
//! `Arc<dyn Registry>`/`Arc<dyn EvidenceStore>` from `deblob-core` while
//! implementing their OWN governed logic in the `deblob` crate; `redis` is
//! already a direct dependency of this crate for exactly this shape of
//! narrow, self-contained governed store.

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_eval::{CaseResult, EvalRun, Metrics};
use redis::Client;
use serde::{Deserialize, Serialize};

/// The `actor` string every retrain-driven registry write is attributed to
/// in the audit trail — distinct from a human operator string, mirroring
/// `crate::trusted::TRUSTED_ACTOR`'s convention.
pub const RETRAIN_ACTOR: &str = "retrain:v1";

/// Lifecycle state of one [`ModelVersion`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelState {
    /// Registered, not yet evaluated against the gate.
    Candidate,
    /// The currently (or, historically, once) promoted model. Exactly one
    /// [`ModelVersion`] is the registry's CURRENT active pointer at a
    /// time — see [`ModelRegistry::get_active`].
    Active,
    /// Failed the go-live gate or regressed vs the active model at
    /// promotion time. Audited with reasons; never becomes `Active`
    /// without a fresh, passing `promote_if_gated` call.
    Rejected,
    /// Was `Active`, then superseded by [`ModelRegistry::rollback`].
    RolledBack,
}

/// Gate-relevant metrics computed from a candidate's HELD-OUT evaluation
/// run (spec §4; go-live thresholds from `docs/shadow-golive-gate.md`).
/// `false_merge_rate: None` means the held-out corpus carried no
/// false-merge-trap case — see [`gate_reasons`]'s docs for how that's
/// treated (never fabricated as `0.0`).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EvalMetricsSummary {
    pub total_cases: usize,
    pub false_merge_rate: Option<f64>,
    pub wrong_valid_rate: f64,
    /// Fraction of ACCEPTED matches (`InferenceDecision::is_accepted_match`)
    /// that were exactly correct — "of what the model was willing to
    /// merge, how much was right" (go-live gate: "accepted precision").
    /// `1.0` (vacuously) if the run accepted no match at all.
    pub accepted_precision: f64,
    pub exact_semantic_accuracy: f64,
}

impl EvalMetricsSummary {
    /// Builds a summary from a [`deblob_eval::EvalRun`]/[`Metrics`] pair
    /// produced by evaluating a candidate against the held-out gate
    /// corpus. `accepted_precision` isn't a field `Metrics` itself
    /// exposes, so it's derived here directly from the run's records.
    pub fn from_eval(run: &EvalRun, metrics: &Metrics) -> Self {
        Self {
            total_cases: metrics.total_cases,
            false_merge_rate: metrics.false_merge_rate,
            wrong_valid_rate: metrics.wrong_valid_rate,
            accepted_precision: accepted_precision(run),
            exact_semantic_accuracy: metrics.exact_semantic_accuracy,
        }
    }
}

fn accepted_precision(run: &EvalRun) -> f64 {
    let mut accepted = 0usize;
    let mut correct = 0usize;
    for record in &run.records {
        let CaseResult {
            outcome, expected, ..
        } = record;
        if let Ok(outcome) = outcome {
            if outcome.decision.is_accepted_match() {
                accepted += 1;
                if outcome.decision == expected.decision {
                    correct += 1;
                }
            }
        }
    }
    if accepted == 0 {
        1.0
    } else {
        correct as f64 / accepted as f64
    }
}

/// Go-live gate thresholds (spec §4; `docs/shadow-golive-gate.md`).
/// `false_merge_rate == 0` is NOT a field here — it is a HARD, non-
/// configurable requirement enforced unconditionally by [`gate_reasons`],
/// exactly mirroring `crate::trusted`'s no-false-merge invariant: no
/// threshold anywhere in this codebase may ever relax it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GoLiveGate {
    /// Go-live gate: "wrong-valid rate ≤ 0.5%".
    pub max_wrong_valid_rate: f64,
    /// Go-live gate: "accepted precision ≥ 99.5%".
    pub min_accepted_precision: f64,
}

impl Default for GoLiveGate {
    fn default() -> Self {
        Self {
            max_wrong_valid_rate: 0.005,
            min_accepted_precision: 0.995,
        }
    }
}

/// Every reason `candidate` fails the go-live gate on its own held-out
/// metrics — empty iff it passes. `false_merge_rate` is checked FIRST and
/// unconditionally (spec: "the hard gate"): any nonzero measured rate
/// fails regardless of every other number. `None` (no false-merge-trap
/// case in the held-out corpus) is NOT treated as a failure — there is no
/// evidence of a false merge, so there is nothing to fail on; this is a
/// property of the held-out corpus's composition, not a fabricated `0.0`.
pub fn gate_reasons(candidate: &EvalMetricsSummary, gate: &GoLiveGate) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(rate) = candidate.false_merge_rate {
        if rate > 0.0 {
            reasons.push(format!(
                "false_merge_rate {rate:.4} > 0 (HARD gate — zero false merges required)"
            ));
        }
    }
    if candidate.wrong_valid_rate > gate.max_wrong_valid_rate {
        reasons.push(format!(
            "wrong_valid_rate {:.4} > {:.4}",
            candidate.wrong_valid_rate, gate.max_wrong_valid_rate
        ));
    }
    if candidate.accepted_precision < gate.min_accepted_precision {
        reasons.push(format!(
            "accepted_precision {:.4} < {:.4}",
            candidate.accepted_precision, gate.min_accepted_precision
        ));
    }
    reasons
}

/// Every reason `candidate` regresses against `active` on the SAME
/// held-out set — empty iff it does not regress. A candidate that passes
/// the gate but regresses is still rejected (spec §4: "does NOT regress vs
/// the current active").
pub fn regression_reasons(
    candidate: &EvalMetricsSummary,
    active: &EvalMetricsSummary,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if candidate.wrong_valid_rate > active.wrong_valid_rate {
        reasons.push(format!(
            "regresses wrong_valid_rate: candidate {:.4} > active {:.4}",
            candidate.wrong_valid_rate, active.wrong_valid_rate
        ));
    }
    if candidate.exact_semantic_accuracy < active.exact_semantic_accuracy {
        reasons.push(format!(
            "regresses exact_semantic_accuracy: candidate {:.4} < active {:.4}",
            candidate.exact_semantic_accuracy, active.exact_semantic_accuracy
        ));
    }
    if let (Some(c), Some(a)) = (candidate.false_merge_rate, active.false_merge_rate) {
        if c > a {
            reasons.push(format!(
                "regresses false_merge_rate: candidate {c:.4} > active {a:.4}"
            ));
        }
    }
    reasons
}

/// One governed model version — the audited unit [`ModelRegistry`]
/// manages. `trained_from` is a human-readable provenance string (feedback
/// cursor + corpus seed description), not a foreign key: model
/// provenance is descriptive audit metadata, same posture as
/// `deblob_core::ports::SchemaRecord::provenance`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelVersion {
    pub model_id: String,
    pub digest: String,
    pub trained_from: String,
    pub eval_metrics: EvalMetricsSummary,
    pub recorded_at: i64,
    pub state: ModelState,
}

/// Outcome of [`ModelRegistry::promote_if_gated`].
#[derive(Debug, Clone, PartialEq)]
pub enum PromotionOutcome {
    /// The candidate passed the gate and did not regress — now `Active`.
    Promoted(ModelVersion),
    /// The candidate failed the gate and/or regressed — now `Rejected`,
    /// with every failing reason (gate + regression combined).
    Rejected {
        reasons: Vec<String>,
        candidate: ModelVersion,
    },
}

/// Governed, immutable, audited registry of model versions. See the module
/// docs for the promotion invariant every implementation must uphold.
#[async_trait]
pub trait ModelRegistry: Send + Sync {
    /// Registers a NEW candidate (state `Candidate`). `Err(Conflict)` if
    /// `model_id` is already registered — a model version's identity is
    /// write-once.
    async fn register_candidate(&self, version: ModelVersion) -> Result<(), CoreError>;

    /// The current active model, if any.
    async fn get_active(&self) -> Result<Option<ModelVersion>, CoreError>;

    /// Evaluates `candidate` against `gate` AND, if there is a current
    /// active model, against it for regression. Atomically transitions
    /// `candidate` to `Active` (and updates the active pointer) on success,
    /// or to `Rejected` (pointer untouched) on failure — either way,
    /// audited with `actor = "retrain:v1"`. Never partially applied.
    async fn promote_if_gated(
        &self,
        candidate: ModelVersion,
        gate: &GoLiveGate,
    ) -> Result<PromotionOutcome, CoreError>;

    /// Restores the model that was active immediately before the current
    /// one — the current active transitions to `RolledBack`, audited with
    /// `actor`. `Err(Conflict)` if there is no prior active to restore
    /// (nothing has ever been promoted, or a rollback already consumed the
    /// only prior).
    async fn rollback(&self, actor: &str) -> Result<ModelVersion, CoreError>;

    /// Every registered model version, oldest first — the audit-readable
    /// history.
    async fn history(&self) -> Result<Vec<ModelVersion>, CoreError>;
}

fn model_key(id: &str) -> String {
    format!("deblob:slm-models:model:{id}")
}
const ACTIVE_KEY: &str = "deblob:slm-models:active";
const PRIOR_ACTIVE_KEY: &str = "deblob:slm-models:prior-active";
const INDEX_KEY: &str = "deblob:slm-models:index";
const AUDIT_KEY: &str = "deblob:slm-models:audit";
const AUDIT_STREAM_MAXLEN: u64 = 10_000;

fn redis_err(e: redis::RedisError) -> CoreError {
    CoreError::RegistryUnavailable(e.to_string())
}

/// Redis-backed [`ModelRegistry`]. All gate/regression math runs in Rust
/// (pure, over already-fetched [`EvalMetricsSummary`] values) BEFORE any
/// Redis write — every write this type issues is therefore a determinate,
/// already-decided state transition, applied atomically via
/// `redis::pipe().atomic()` (MULTI/EXEC), the same pattern
/// `deblob-redis::evidence` uses for its own multi-key writes. This is a
/// periodic-batch orchestration surface (one retrain run at a time is the
/// documented deployment model), not a high-concurrency hot path, so a
/// simple read-then-decide-then-write sequence (rather than a
/// compare-and-swap Lua script like the schema registry's `publish`) is
/// sufficient here.
pub struct RedisModelRegistry {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisModelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisModelRegistry").finish_non_exhaustive()
    }
}

impl RedisModelRegistry {
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(deblob_redis::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self { conn })
    }

    fn conn(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }

    async fn get_model(&self, id: &str) -> Result<Option<ModelVersion>, CoreError> {
        let mut conn = self.conn();
        let json: Option<String> = redis::cmd("GET")
            .arg(model_key(id))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        json.map(|j| {
            serde_json::from_str(&j)
                .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt model record: {e}")))
        })
        .transpose()
    }

    async fn write_model(&self, version: &ModelVersion) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let json = serde_json::to_string(version)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize model: {e}")))?;
        let _: () = redis::cmd("SET")
            .arg(model_key(&version.model_id))
            .arg(&json)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn audit(
        &self,
        action: &str,
        model_id: &str,
        actor: &str,
        reasons: &[String],
    ) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let reasons_joined = reasons.join("; ");
        let _: String = redis::cmd("XADD")
            .arg(AUDIT_KEY)
            .arg("MAXLEN")
            .arg("~")
            .arg(AUDIT_STREAM_MAXLEN)
            .arg("*")
            .arg("model_id")
            .arg(model_id)
            .arg("action")
            .arg(action)
            .arg("actor")
            .arg(actor)
            .arg("reasons")
            .arg(&reasons_joined)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }
}

#[async_trait]
impl ModelRegistry for RedisModelRegistry {
    async fn register_candidate(&self, version: ModelVersion) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let key = model_key(&version.model_id);
        let exists: bool = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if exists {
            return Err(CoreError::Conflict(format!(
                "model {} is already registered",
                version.model_id
            )));
        }

        let json = serde_json::to_string(&version)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize model: {e}")))?;
        let _: () = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(&key)
            .arg(&json)
            .ignore()
            .cmd("SADD")
            .arg(INDEX_KEY)
            .arg(&version.model_id)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        self.audit("register", &version.model_id, RETRAIN_ACTOR, &[])
            .await
    }

    async fn get_active(&self) -> Result<Option<ModelVersion>, CoreError> {
        let mut conn = self.conn();
        let id: Option<String> = redis::cmd("GET")
            .arg(ACTIVE_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match id {
            None => Ok(None),
            Some(id) => self.get_model(&id).await,
        }
    }

    async fn promote_if_gated(
        &self,
        mut candidate: ModelVersion,
        gate: &GoLiveGate,
    ) -> Result<PromotionOutcome, CoreError> {
        let active = self.get_active().await?;

        let mut reasons = gate_reasons(&candidate.eval_metrics, gate);
        if let Some(active_version) = &active {
            reasons.extend(regression_reasons(
                &candidate.eval_metrics,
                &active_version.eval_metrics,
            ));
        }

        if reasons.is_empty() {
            candidate.state = ModelState::Active;
            self.write_model(&candidate).await?;

            let mut conn = self.conn();
            let mut pipe = redis::pipe();
            pipe.atomic();
            pipe.cmd("SET")
                .arg(ACTIVE_KEY)
                .arg(&candidate.model_id)
                .ignore();
            match &active {
                Some(prior) => {
                    pipe.cmd("SET")
                        .arg(PRIOR_ACTIVE_KEY)
                        .arg(&prior.model_id)
                        .ignore();
                }
                None => {
                    pipe.cmd("DEL").arg(PRIOR_ACTIVE_KEY).ignore();
                }
            }
            let _: () = pipe.query_async(&mut conn).await.map_err(redis_err)?;

            self.audit("promote", &candidate.model_id, RETRAIN_ACTOR, &[])
                .await?;
            Ok(PromotionOutcome::Promoted(candidate))
        } else {
            candidate.state = ModelState::Rejected;
            self.write_model(&candidate).await?;
            self.audit("reject", &candidate.model_id, RETRAIN_ACTOR, &reasons)
                .await?;
            Ok(PromotionOutcome::Rejected { reasons, candidate })
        }
    }

    async fn rollback(&self, actor: &str) -> Result<ModelVersion, CoreError> {
        let mut conn = self.conn();
        let active_id: Option<String> = redis::cmd("GET")
            .arg(ACTIVE_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let prior_id: Option<String> = redis::cmd("GET")
            .arg(PRIOR_ACTIVE_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let active_id =
            active_id.ok_or_else(|| CoreError::Conflict("no active model to roll back".into()))?;
        let prior_id = prior_id.ok_or_else(|| {
            CoreError::Conflict("no prior active model recorded to roll back to".into())
        })?;

        let mut active_version = self
            .get_model(&active_id)
            .await?
            .ok_or(CoreError::NotFound)?;
        active_version.state = ModelState::RolledBack;
        self.write_model(&active_version).await?;

        let restored = self
            .get_model(&prior_id)
            .await?
            .ok_or(CoreError::NotFound)?;

        let _: () = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(ACTIVE_KEY)
            .arg(&prior_id)
            .ignore()
            .cmd("DEL")
            .arg(PRIOR_ACTIVE_KEY)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        self.audit("rollback", &restored.model_id, actor, &[])
            .await?;
        Ok(restored)
    }

    async fn history(&self) -> Result<Vec<ModelVersion>, CoreError> {
        let mut conn = self.conn();
        let ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(INDEX_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(v) = self.get_model(&id).await? {
                out.push(v);
            }
        }
        out.sort_by_key(|v| v.recorded_at);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing_metrics() -> EvalMetricsSummary {
        EvalMetricsSummary {
            total_cases: 100,
            false_merge_rate: Some(0.0),
            wrong_valid_rate: 0.001,
            accepted_precision: 0.999,
            exact_semantic_accuracy: 0.9,
        }
    }

    #[test]
    fn any_nonzero_false_merge_rate_fails_the_hard_gate() {
        let mut candidate = passing_metrics();
        candidate.false_merge_rate = Some(0.0001);
        let reasons = gate_reasons(&candidate, &GoLiveGate::default());
        assert!(
            reasons.iter().any(|r| r.contains("false_merge_rate")),
            "expected a false_merge_rate failure reason, got {reasons:?}"
        );
    }

    #[test]
    fn no_false_merge_trap_cases_is_not_treated_as_a_failure() {
        let mut candidate = passing_metrics();
        candidate.false_merge_rate = None;
        let reasons = gate_reasons(&candidate, &GoLiveGate::default());
        assert!(
            reasons.is_empty(),
            "an absent false_merge_rate (no trap cases in the held-out corpus) must not fail \
             the gate: {reasons:?}"
        );
    }

    #[test]
    fn wrong_valid_rate_above_threshold_fails() {
        let mut candidate = passing_metrics();
        candidate.wrong_valid_rate = 0.05;
        let reasons = gate_reasons(&candidate, &GoLiveGate::default());
        assert!(reasons.iter().any(|r| r.contains("wrong_valid_rate")));
    }

    #[test]
    fn accepted_precision_below_threshold_fails() {
        let mut candidate = passing_metrics();
        candidate.accepted_precision = 0.5;
        let reasons = gate_reasons(&candidate, &GoLiveGate::default());
        assert!(reasons.iter().any(|r| r.contains("accepted_precision")));
    }

    #[test]
    fn fully_passing_metrics_has_no_gate_reasons() {
        let reasons = gate_reasons(&passing_metrics(), &GoLiveGate::default());
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    #[test]
    fn a_candidate_that_regresses_accuracy_is_flagged() {
        let active = passing_metrics();
        let mut candidate = passing_metrics();
        candidate.exact_semantic_accuracy = active.exact_semantic_accuracy - 0.2;
        let reasons = regression_reasons(&candidate, &active);
        assert!(reasons
            .iter()
            .any(|r| r.contains("exact_semantic_accuracy")));
    }

    #[test]
    fn a_candidate_that_improves_never_regresses() {
        let active = passing_metrics();
        let mut candidate = passing_metrics();
        candidate.exact_semantic_accuracy = active.exact_semantic_accuracy + 0.05;
        candidate.wrong_valid_rate = active.wrong_valid_rate / 2.0;
        let reasons = regression_reasons(&candidate, &active);
        assert!(reasons.is_empty(), "{reasons:?}");
    }
}
