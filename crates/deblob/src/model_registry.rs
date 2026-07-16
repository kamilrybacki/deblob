//! Governed model registry + gated promotion (spec:
//! `docs/superpowers/specs/2026-07-16-slm-continual-learning.md` §4, §B6-8,
//! §B11-12 — "Amendments from joint research").
//!
//! Applies the SAME evidence discipline the schema registry
//! (`deblob_core::ports::Registry`) already holds for schema promotion to
//! MODEL VERSIONS: immutable records, atomic + audited state transitions.
//! Three amendments from the joint-research review reshape how that
//! discipline is expressed here:
//!
//! - **§B6 statistical gate.** The gate is not "zero failures": it
//!   requires a minimum test-N ([`GateConfig::min_test_n`]), per-family
//!   precision floors ([`GateConfig::per_family_precision_floor`]) checked
//!   with a Wilson-score confidence bound (not a bare point estimate), and
//!   a non-inferiority margin vs the active model
//!   ([`GateConfig::non_inferiority_margin`]). The `false_merge` hard gate
//!   stays absolute (`false_merge_count > 0` fails unconditionally,
//!   independent of every threshold below), but is now ALSO backed by an
//!   upper confidence bound given N
//!   ([`GateEvidence::false_merge_upper_ci`]) — zero observed false
//!   merges out of a tiny N is inconclusive, not proof of safety.
//! - **§B7 separation of duties.** No single call can ever move the
//!   active alias. [`ModelRegistry::register_candidate`] produces a bare
//!   [`ModelVersion`] in [`ModelState::Candidate`] with `evidence: None`.
//!   [`ModelRegistry::attach_evidence`] is the ONLY place gate math runs —
//!   it writes the [`GateEvidence`] bundle and transitions the candidate
//!   to [`ModelState::ShadowCandidate`] (pass) or [`ModelState::Rejected`]
//!   (fail); it can NEVER produce [`ModelState::Active`].
//!   [`ModelRegistry::promote`] is a SEPARATE controller action: it only
//!   accepts a candidate already in `ShadowCandidate` state, requires the
//!   evidence bundle to already be attached, and — per
//!   [`GateConfig::require_explicit_approval`] — an explicit
//!   [`PromotionApproval`]. `crate::retrain::RetrainPlan` calls
//!   `register_candidate` + `attach_evidence` only; it holds no path to
//!   `promote` at all (see that module's tests).
//! - **§B11 live-shadow canary.** `attach_evidence` passing the offline
//!   gate is NOT full promotion — it is entry into
//!   [`ModelState::ShadowCandidate`], the same shadow lane
//!   `crate::shadow` already runs on real traffic. `promote` additionally
//!   enforces [`GateConfig::min_shadow_hold_ms`] has elapsed since
//!   `shadow_since` before it will move the alias.
//!
//! `rollback` restores the immediately prior `Active` model **in full**
//! (§B8: the whole [`ArtifactBundle`], not just a weights digest) — see
//! that method's docs.
//!
//! # Why this lives in `deblob`, not `deblob-redis`
//!
//! `EvalMetricsSummary`/[`GateEvidence`] are derived from
//! `deblob_eval::{EvalRun, Metrics}` — `deblob-redis` has (and should
//! have) no dependency on the eval harness. Keeping the trait + the
//! Redis-backed implementation together here mirrors how
//! `crate::trusted`/`crate::policy` already hold
//! `Arc<dyn Registry>`/`Arc<dyn EvidenceStore>` from `deblob-core` while
//! implementing their OWN governed logic in the `deblob` crate.

use std::collections::HashMap;

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::FamilyId;
use deblob_eval::{CaseResult, EvalRun, Metrics};
use redis::Client;
use serde::{Deserialize, Serialize};

/// The `actor` string every retrain-driven registry write (registration,
/// evidence attachment) is attributed to in the audit trail — distinct
/// from a human/controller operator string. [`ModelRegistry::promote`] is
/// audited under `approval.actor` instead (spec §B7: promotion is a
/// SEPARATE, human/controller-attributed action, never this system actor).
pub const RETRAIN_ACTOR: &str = "retrain:v1";

/// 95% two-sided Wilson-score `z`. [`GateConfig::confidence_z`] defaults
/// to this but is itself ablatable — see that field's docs.
pub const Z_95: f64 = 1.959_963_985;

/// Lifecycle state of one [`ModelVersion`]. Spec §B7/§B11: the state
/// machine is `Candidate -> ShadowCandidate -> Active`, plus terminal
/// `Rejected`/`RolledBack` — no transition skips a stage, and only
/// [`ModelRegistry::promote`] ever produces `Active`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelState {
    /// Registered by `register_candidate`; no [`GateEvidence`] attached
    /// yet.
    Candidate,
    /// Passed [`ModelRegistry::attach_evidence`]'s offline gate — now
    /// eligible for the live-shadow lane (spec §B11). NOT active; a
    /// worse-than-active OR un-held candidate can sit here indefinitely.
    ShadowCandidate,
    /// The currently (or, historically, once) promoted model. Exactly one
    /// [`ModelVersion`] is the registry's CURRENT active pointer at a
    /// time — see [`ModelRegistry::get_active`]. Reachable ONLY via
    /// [`ModelRegistry::promote`].
    Active,
    /// Failed `attach_evidence`'s gate/regression check. Audited with
    /// reasons; never becomes `ShadowCandidate`/`Active` without a fresh
    /// candidate + a fresh, passing `attach_evidence` call.
    Rejected,
    /// Was `Active`, then superseded by [`ModelRegistry::rollback`].
    RolledBack,
}

// ---------------------------------------------------------------------
// Composite artifact bundle (spec §B8)
// ---------------------------------------------------------------------

/// The WHOLE inference bundle a [`ModelVersion`] versions — not just the
/// weights (spec §B8). `rollback` restores every one of these fields at
/// once, atomically, because the whole record (not a bare digest) is what
/// gets swapped back onto the active pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBundle {
    /// Digest of the QUANTIZED weights — the artifact the gate actually
    /// evaluates (spec §B8: "the gate evaluates the QUANTIZED artifact,
    /// recorded separately from the training checkpoint" — see
    /// [`ModelVersion::training_checkpoint_digest`] for the other side of
    /// that separation).
    pub weights_digest: String,
    pub tokenizer: String,
    pub prompt_template_version: String,
    pub runtime: String,
    pub quantization: String,
    pub retrieval_index_version: String,
    pub grammar: String,
    pub catalog: String,
}

impl ArtifactBundle {
    /// Builds a full bundle from a fixed [`BundleTemplate`] (the
    /// non-weights identity: tokenizer/runtime/grammar/etc, usually
    /// unchanged run over run) plus the one field that DOES change per
    /// candidate — the quantized weights digest.
    pub fn new(weights_digest: String, template: &BundleTemplate) -> Self {
        Self {
            weights_digest,
            tokenizer: template.tokenizer.clone(),
            prompt_template_version: template.prompt_template_version.clone(),
            runtime: template.runtime.clone(),
            quantization: template.quantization.clone(),
            retrieval_index_version: template.retrieval_index_version.clone(),
            grammar: template.grammar.clone(),
            catalog: template.catalog.clone(),
        }
    }
}

/// Every [`ArtifactBundle`] field EXCEPT the weights digest — the parts of
/// the inference bundle a retrain run does not itself change (spec §B8).
/// `RetrainPlan` callers supply this once (deployment config); the
/// per-candidate weights digest is merged in via [`ArtifactBundle::new`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleTemplate {
    pub tokenizer: String,
    pub prompt_template_version: String,
    pub runtime: String,
    pub quantization: String,
    pub retrieval_index_version: String,
    pub grammar: String,
    pub catalog: String,
}

/// Reproducible training provenance (spec §B9): `base_snapshot_id` is a
/// FIXED, caller-supplied base checkpoint identity — `RetrainPlan` never
/// derives it from whatever the previous `Active`/`ShadowCandidate` model
/// happened to be, so retraining never recursively mutates "the latest
/// adapter". `feedback_cursor`/`corpus_seed` remain descriptive audit
/// metadata, not foreign keys (same posture as
/// `deblob_core::ports::SchemaRecord::provenance`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainedFrom {
    pub base_snapshot_id: String,
    pub feedback_cursor: String,
    pub corpus_seed: String,
}

// ---------------------------------------------------------------------
// Gate evidence (spec §B6, §B12)
// ---------------------------------------------------------------------

/// Gate-relevant metrics computed from a candidate's HELD-OUT evaluation
/// run (spec §4/§B6; go-live thresholds from `docs/shadow-golive-gate.md`).
/// `false_merge_rate: None` means the held-out corpus carried no
/// false-merge-trap case; `retrieval_recall_at_5: None` means no case
/// carried a gold schema id — neither is ever fabricated as `0.0`/`1.0`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EvalMetricsSummary {
    pub total_cases: usize,
    pub false_merge_rate: Option<f64>,
    pub false_merge_count: usize,
    pub false_merge_trap_count: usize,
    pub wrong_valid_rate: f64,
    /// Fraction of ACCEPTED matches (`InferenceDecision::is_accepted_match`)
    /// that were exactly correct — "of what the model was willing to
    /// merge, how much was right" (go-live gate: "accepted precision").
    /// `1.0` (vacuously) if the run accepted no match at all.
    pub accepted_precision: f64,
    /// End-to-end exact-match accuracy over EVERY case (retrieval misses
    /// included) — the generator's real-world number.
    pub exact_semantic_accuracy: f64,
    /// Spec §B12: generator exact-match accuracy restricted to cases
    /// where the gold schema WAS present in the retrieved top-k
    /// (`gold_rank.is_some()`) — isolates the generator's own error rate
    /// from retrieval failure the generator structurally cannot fix.
    /// `None` if no case in the run carries a gold rank at all.
    pub oracle_retrieval_exact_accuracy: Option<f64>,
    /// Spec §B12: retrieval recall@5, tracked and gated SEPARATELY from
    /// generator accuracy — a regression here is its own failure mode
    /// (see [`regression_reasons`]), never folded into the accuracy
    /// checks above. `None` if no case carries a gold schema id.
    pub retrieval_recall_at_5: Option<f64>,
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

/// Spec §B12: generator accuracy restricted to gold-retrieved cases —
/// isolates generator error from retrieval error. See
/// [`EvalMetricsSummary::oracle_retrieval_exact_accuracy`]'s docs.
fn oracle_retrieval_exact_accuracy(run: &EvalRun) -> Option<f64> {
    let mut n = 0usize;
    let mut correct = 0usize;
    for record in &run.records {
        if record.expected.gold_rank.is_some() {
            n += 1;
            if let Ok(outcome) = &record.outcome {
                if outcome.decision == record.expected.decision {
                    correct += 1;
                }
            }
        }
    }
    if n == 0 {
        None
    } else {
        Some(correct as f64 / n as f64)
    }
}

/// One family's exact-match precision slice over a held-out run — the
/// unit spec §B6's per-family floor is checked against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FamilySlice {
    pub family_id: FamilyId,
    pub n: usize,
    pub correct: usize,
    pub precision: f64,
}

fn per_family_slices(run: &EvalRun) -> Vec<FamilySlice> {
    let mut agg: HashMap<FamilyId, (usize, usize)> = HashMap::new();
    for record in &run.records {
        let Some(family_id) = record.retrieved.first().map(|c| c.family_id.clone()) else {
            continue;
        };
        let entry = agg.entry(family_id).or_insert((0, 0));
        entry.0 += 1;
        if let Ok(outcome) = &record.outcome {
            if outcome.decision == record.expected.decision {
                entry.1 += 1;
            }
        }
    }
    let mut out: Vec<FamilySlice> = agg
        .into_iter()
        .map(|(family_id, (n, correct))| FamilySlice {
            family_id,
            n,
            correct,
            precision: if n == 0 {
                1.0
            } else {
                correct as f64 / n as f64
            },
        })
        .collect();
    out.sort_by(|a, b| a.family_id.as_str().cmp(b.family_id.as_str()));
    out
}

/// Two-sided Wilson score interval bound for a binomial proportion
/// `successes/n` at confidence `z` (spec §B6: "confidence intervals").
/// `n == 0` returns the maximally uninformative bound (`1.0` upper / `0.0`
/// lower) — no evidence means no confidence, never a fabricated midpoint.
fn wilson_bound(successes: usize, n: usize, z: f64, upper: bool) -> f64 {
    if n == 0 {
        return if upper { 1.0 } else { 0.0 };
    }
    let n = n as f64;
    let p = (successes as f64 / n).clamp(0.0, 1.0);
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = p + z2 / (2.0 * n);
    let margin = z * ((p * (1.0 - p) / n) + (z2 / (4.0 * n * n))).max(0.0).sqrt();
    let bound = if upper {
        (center + margin) / denom
    } else {
        (center - margin) / denom
    };
    bound.clamp(0.0, 1.0)
}

/// The full gate evidence bundle [`ModelRegistry::attach_evidence`] writes
/// onto a candidate (spec §B6/§B7/§B12) — never present on a bare
/// `Candidate` produced by `register_candidate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateEvidence {
    pub aggregate: EvalMetricsSummary,
    /// Spec §B6: per-family slices — a candidate can pass every aggregate
    /// number and still be rejected here.
    pub per_family: Vec<FamilySlice>,
    /// Spec §B6: upper confidence bound on the true false-merge rate,
    /// given `aggregate.false_merge_count`/`aggregate.false_merge_trap_count`.
    /// `None` iff the held-out corpus carried no false-merge-trap case at
    /// all (nothing to bound).
    pub false_merge_upper_ci: Option<f64>,
    pub computed_at: i64,
}

impl GateEvidence {
    /// Builds evidence from a [`deblob_eval::EvalRun`]/[`Metrics`] pair —
    /// the candidate's held-out gate-corpus evaluation. `z` is
    /// [`GateConfig::confidence_z`] (kept as a parameter, not read from
    /// `GateConfig` directly, so evidence-building stays decoupled from
    /// which specific gate it will later be checked against).
    pub fn from_eval(run: &EvalRun, metrics: &Metrics, now_ms: i64, z: f64) -> Self {
        let aggregate = EvalMetricsSummary {
            total_cases: metrics.total_cases,
            false_merge_rate: metrics.false_merge_rate,
            false_merge_count: metrics.false_merge_count,
            false_merge_trap_count: metrics.false_merge_trap_count,
            wrong_valid_rate: metrics.wrong_valid_rate,
            accepted_precision: accepted_precision(run),
            exact_semantic_accuracy: metrics.exact_semantic_accuracy,
            oracle_retrieval_exact_accuracy: oracle_retrieval_exact_accuracy(run),
            retrieval_recall_at_5: metrics.recall_at_5,
        };
        let false_merge_upper_ci = if metrics.false_merge_trap_count > 0 {
            Some(wilson_bound(
                metrics.false_merge_count,
                metrics.false_merge_trap_count,
                z,
                true,
            ))
        } else {
            None
        };
        Self {
            aggregate,
            per_family: per_family_slices(run),
            false_merge_upper_ci,
            computed_at: now_ms,
        }
    }
}

// ---------------------------------------------------------------------
// Gate config + reasons (spec §B6)
// ---------------------------------------------------------------------

/// Statistical gate thresholds (spec §B6) — replaces the old flat
/// "zero-failures" `GoLiveGate`. Every field except the hard false-merge
/// check is a config parameter, explicitly ablatable, mirroring
/// `crate::feedback::FeedbackWeights`'s convention. Defaults are the
/// go-live numbers from `docs/shadow-golive-gate.md` where that document
/// specifies one, and are documented "unvalidated — ablate" otherwise.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GateConfig {
    /// Statistical power floor over the WHOLE held-out run — below this,
    /// the candidate is INCONCLUSIVE (not promotable), not merely
    /// "unproven". `docs/shadow-golive-gate.md`'s full go-live gate uses
    /// 3000 (shadow-log accepted decisions); this offline per-retrain gate
    /// defaults lower since it runs on the eval harness's held-out corpus,
    /// not live shadow traffic — raise it once the corpus is large enough
    /// to support it.
    pub min_test_n: usize,
    /// Go-live gate: "wrong-valid rate ≤ 0.5%".
    pub max_wrong_valid_rate: f64,
    /// Go-live gate: "accepted precision ≥ 99.5%".
    pub min_accepted_precision: f64,
    /// A family slice below `per_family_min_n` cases is exempt from
    /// [`per_family_precision_floor`] — not enough evidence to judge it
    /// either way (mirrors `docs/shadow-golive-gate.md`'s "no slice of
    /// ≥ 100 examples below 99% precision").
    pub per_family_min_n: usize,
    pub per_family_precision_floor: f64,
    /// Spec §B12: retrieval recall@5 floor, checked independently of
    /// every generator-accuracy number above.
    pub min_retrieval_recall_at_5: f64,
    /// Spec §B6: how much WORSE a candidate's `exact_semantic_accuracy`/
    /// `wrong_valid_rate` may be than the active model's before it counts
    /// as a regression — a true non-inferiority margin, not "strictly no
    /// worse at all". **Unvalidated — ablate.**
    pub non_inferiority_margin: f64,
    /// Spec §B12: the SAME kind of margin, applied to
    /// `retrieval_recall_at_5` — its own regression check, never folded
    /// into `non_inferiority_margin`.
    pub retrieval_non_inferiority_margin: f64,
    /// Spec §B6: the false-merge hard gate is ALSO backed by this upper
    /// confidence bound on the true rate (given
    /// `false_merge_trap_count`) — a candidate with 0 observed false
    /// merges over a tiny N still fails here as INCONCLUSIVE.
    pub max_false_merge_upper_ci: f64,
    /// `z` for every Wilson-score bound this gate computes. Defaults to
    /// [`Z_95`] (95% two-sided) — ablatable.
    pub confidence_z: f64,
    /// Spec §B7: whether [`ModelRegistry::promote`] requires
    /// `approval.approved == true`. `true` in every deployed
    /// configuration; `false` exists only for a test/dev registry where
    /// the approval ceremony itself is out of scope.
    pub require_explicit_approval: bool,
    /// Spec §B11: minimum wall-clock hold in `ShadowCandidate` (live
    /// canary lane) before `promote` will accept it, measured from
    /// `ModelVersion::shadow_since`. `0` (test default) means no hold —
    /// operators MUST configure a real hold period in production.
    pub min_shadow_hold_ms: i64,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            min_test_n: 200,
            max_wrong_valid_rate: 0.005,
            min_accepted_precision: 0.995,
            per_family_min_n: 20,
            per_family_precision_floor: 0.99,
            min_retrieval_recall_at_5: 0.95,
            non_inferiority_margin: 0.01,
            retrieval_non_inferiority_margin: 0.02,
            max_false_merge_upper_ci: 0.01,
            confidence_z: Z_95,
            require_explicit_approval: true,
            min_shadow_hold_ms: 0,
        }
    }
}

/// Every reason `evidence` fails its OWN offline gate (spec §B6) —
/// empty iff it passes. `false_merge_count` is checked FIRST and
/// unconditionally (the hard gate): any nonzero measured count fails
/// regardless of every other number. A candidate below `min_test_n`, or
/// whose false-merge upper confidence bound is too wide given N, is
/// prefixed `INCONCLUSIVE` (not `REJECTED` for cause) — see this
/// function's callers for how the two are handled identically (neither
/// is promotable) but reported distinctly.
pub fn gate_reasons(evidence: &GateEvidence, gate: &GateConfig) -> Vec<String> {
    let mut reasons = Vec::new();
    let agg = &evidence.aggregate;

    if agg.total_cases < gate.min_test_n {
        reasons.push(format!(
            "INCONCLUSIVE: total_cases {} < min_test_n {} (statistical power floor)",
            agg.total_cases, gate.min_test_n
        ));
    }

    // The hard gate: any measured false merge fails, unconditionally.
    if agg.false_merge_count > 0 {
        reasons.push(format!(
            "false_merge_count {} > 0 (HARD gate — zero false merges required)",
            agg.false_merge_count
        ));
    }
    // Statistical backing for the hard gate: even 0 observed false
    // merges is not proof of safety if N is too small to bound the true
    // rate tightly.
    if let Some(upper) = evidence.false_merge_upper_ci {
        if upper > gate.max_false_merge_upper_ci {
            reasons.push(format!(
                "INCONCLUSIVE: false_merge upper confidence bound {:.4} > {:.4} \
                 (N={} too small to certify zero false merges)",
                upper, gate.max_false_merge_upper_ci, agg.false_merge_trap_count
            ));
        }
    }

    if agg.wrong_valid_rate > gate.max_wrong_valid_rate {
        reasons.push(format!(
            "wrong_valid_rate {:.4} > {:.4}",
            agg.wrong_valid_rate, gate.max_wrong_valid_rate
        ));
    }
    if agg.accepted_precision < gate.min_accepted_precision {
        reasons.push(format!(
            "accepted_precision {:.4} < {:.4}",
            agg.accepted_precision, gate.min_accepted_precision
        ));
    }

    // Spec §B12: retrieval recall is its OWN gate axis, independent of
    // every generator-accuracy check above.
    if let Some(recall) = agg.retrieval_recall_at_5 {
        if recall < gate.min_retrieval_recall_at_5 {
            reasons.push(format!(
                "retrieval_recall_at_5 {:.4} < floor {:.4} (retrieval gate — independent of \
                 generator accuracy)",
                recall, gate.min_retrieval_recall_at_5
            ));
        }
    }

    // Spec §B6: per-family precision floor — checked with a Wilson lower
    // bound (not the bare point estimate) so a slice only fails when
    // there is enough evidence to be confident it is actually bad, but a
    // slice below `per_family_min_n` is exempt entirely (not enough
    // evidence to judge either way).
    for slice in &evidence.per_family {
        if slice.n < gate.per_family_min_n {
            continue;
        }
        let lower = wilson_bound(slice.correct, slice.n, gate.confidence_z, false);
        if lower < gate.per_family_precision_floor {
            reasons.push(format!(
                "family {} precision {:.4} (n={}, wilson_lower={:.4}) < floor {:.4} — \
                 aggregate can pass while this slice fails",
                slice.family_id.as_str(),
                slice.precision,
                slice.n,
                lower,
                gate.per_family_precision_floor
            ));
        }
    }

    reasons
}

/// Every reason `candidate` regresses against `active` on the SAME
/// held-out set — empty iff it does not regress beyond
/// [`GateConfig::non_inferiority_margin`]/[`GateConfig::retrieval_non_inferiority_margin`].
/// A candidate that passes its own gate but regresses is still rejected
/// (spec §4/§B6: "does NOT regress vs the current active", now expressed
/// as non-inferiority rather than strict improvement).
pub fn regression_reasons(
    candidate: &GateEvidence,
    active: &GateEvidence,
    gate: &GateConfig,
) -> Vec<String> {
    let mut reasons = Vec::new();
    let c = &candidate.aggregate;
    let a = &active.aggregate;

    // False merges must never regress — no margin, ever.
    if c.false_merge_count > a.false_merge_count {
        reasons.push(format!(
            "regresses false_merge_count: candidate {} > active {}",
            c.false_merge_count, a.false_merge_count
        ));
    }

    if c.exact_semantic_accuracy + gate.non_inferiority_margin < a.exact_semantic_accuracy {
        reasons.push(format!(
            "regresses exact_semantic_accuracy beyond non_inferiority_margin {:.4}: candidate \
             {:.4} < active {:.4}",
            gate.non_inferiority_margin, c.exact_semantic_accuracy, a.exact_semantic_accuracy
        ));
    }
    if c.wrong_valid_rate > a.wrong_valid_rate + gate.non_inferiority_margin {
        reasons.push(format!(
            "regresses wrong_valid_rate beyond non_inferiority_margin {:.4}: candidate {:.4} > \
             active {:.4}",
            gate.non_inferiority_margin, c.wrong_valid_rate, a.wrong_valid_rate
        ));
    }

    // Spec §B12: retrieval recall regression is its OWN gate — a
    // candidate can have flat-or-better generator accuracy and still be
    // flagged here.
    if let (Some(cr), Some(ar)) = (c.retrieval_recall_at_5, a.retrieval_recall_at_5) {
        if cr + gate.retrieval_non_inferiority_margin < ar {
            reasons.push(format!(
                "regresses retrieval_recall_at_5 beyond retrieval_non_inferiority_margin {:.4}: \
                 candidate {:.4} < active {:.4} (retrieval gate — independent of generator \
                 accuracy)",
                gate.retrieval_non_inferiority_margin, cr, ar
            ));
        }
    }

    reasons
}

// ---------------------------------------------------------------------
// ModelVersion + registry (spec §B7/§B8/§B11)
// ---------------------------------------------------------------------

/// One governed model version — the audited unit [`ModelRegistry`]
/// manages. Spec §B8: versions the WHOLE inference bundle, not just
/// weights. Spec §B7: `evidence` is `None` on every freshly-registered
/// candidate — it is written exactly once, by `attach_evidence`, never by
/// `register_candidate`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelVersion {
    pub model_id: String,
    pub bundle: ArtifactBundle,
    /// Digest of the TRAINING checkpoint, kept separate from
    /// `bundle.weights_digest` (the quantized artifact the gate actually
    /// evaluates) — spec §B8.
    pub training_checkpoint_digest: String,
    pub trained_from: TrainedFrom,
    /// `None` until `attach_evidence` runs — spec §B7's separation of
    /// duties made structural: a bare `Candidate` cannot even carry gate
    /// evidence.
    pub evidence: Option<GateEvidence>,
    pub recorded_at: i64,
    /// Set when `attach_evidence` transitions this version into
    /// `ShadowCandidate` — the clock `promote`'s
    /// `GateConfig::min_shadow_hold_ms` check reads from. `None` until
    /// then.
    pub shadow_since: Option<i64>,
    pub state: ModelState,
}

/// Outcome of [`ModelRegistry::attach_evidence`] — spec §B7/§B11: this is
/// the ONLY place gate math runs, and it can produce
/// [`ModelState::ShadowCandidate`] or [`ModelState::Rejected`], NEVER
/// [`ModelState::Active`].
#[derive(Debug, Clone, PartialEq)]
pub enum GateDecision {
    /// The candidate passed its own gate and did not regress vs the
    /// current active (if any) — now `ShadowCandidate`, eligible for live
    /// canary evaluation and, later, an explicit `promote`.
    EnteredShadow(ModelVersion),
    /// The candidate failed the gate and/or regressed — now `Rejected`,
    /// with every failing/inconclusive reason (gate + regression
    /// combined).
    Rejected {
        reasons: Vec<String>,
        candidate: ModelVersion,
    },
}

/// Spec §B7: the explicit, human/controller-attributed approval
/// `ModelRegistry::promote` requires when
/// [`GateConfig::require_explicit_approval`] is set. `actor` is who/what
/// approved it — attributed in the audit trail instead of
/// [`RETRAIN_ACTOR`], since promotion is deliberately NOT a system-actor
/// action.
#[derive(Debug, Clone, PartialEq)]
pub struct PromotionApproval {
    pub approved: bool,
    pub actor: String,
}

/// Governed, immutable, audited registry of model versions. See the
/// module docs for the state machine every implementation must uphold —
/// in particular: `attach_evidence` never produces `Active`, and
/// `promote` is the ONLY method that ever does.
#[async_trait]
pub trait ModelRegistry: Send + Sync {
    /// Registers a NEW candidate (state `Candidate`, `evidence: None`).
    /// `Err(Conflict)` if `model_id` is already registered — a model
    /// version's identity is write-once. Spec §B7 separation of duties is
    /// STRUCTURAL, not a caller convention: whatever `state`/`evidence`/
    /// `shadow_since` the caller passed in on `version` are forcibly
    /// overwritten to the bare `Candidate` defaults before the write — a
    /// candidate can never be born already carrying (forged or otherwise)
    /// gate evidence.
    async fn register_candidate(&self, version: ModelVersion) -> Result<(), CoreError>;

    /// The current active model, if any.
    async fn get_active(&self) -> Result<Option<ModelVersion>, CoreError>;

    /// One registered model version by id, if any.
    async fn get(&self, model_id: &str) -> Result<Option<ModelVersion>, CoreError>;

    /// Spec §B6/§B7: writes `evidence` onto the `model_id` candidate and
    /// evaluates it (own gate + regression vs the current active, if
    /// any). `Err(Conflict)` if `model_id` is not currently in
    /// `ModelState::Candidate` (evidence is attached exactly once).
    /// Atomically transitions the candidate to `ShadowCandidate` (pass)
    /// or `Rejected` (fail) — audited with `actor = RETRAIN_ACTOR`. NEVER
    /// produces `Active`.
    async fn attach_evidence(
        &self,
        model_id: &str,
        evidence: GateEvidence,
        gate: &GateConfig,
    ) -> Result<GateDecision, CoreError>;

    /// Spec §B7/§B11: the ONLY method that can ever move the active
    /// alias. `Err(Conflict)` if `model_id` is not currently
    /// `ShadowCandidate` (skips straight from `Candidate`, or promoting
    /// an already-terminal version, are both rejected the same way).
    /// `Err(PolicyRejected)` if `gate.require_explicit_approval &&
    /// !approval.approved`, or if `gate.min_shadow_hold_ms` has not yet
    /// elapsed since `shadow_since`. On success: atomically transitions
    /// to `Active`, updates the active pointer, retains the previous
    /// active for `rollback` — audited with `actor = approval.actor`.
    async fn promote(
        &self,
        model_id: &str,
        approval: PromotionApproval,
        gate: &GateConfig,
    ) -> Result<ModelVersion, CoreError>;

    /// Restores the model that was active immediately before the current
    /// one — the current active transitions to `RolledBack`, audited with
    /// `actor`. Restores the WHOLE prior [`ModelVersion`] record
    /// (`bundle` included — spec §B8), since the active pointer simply
    /// moves back to that record's id rather than reconstructing any
    /// field piecemeal. `Err(Conflict)` if there is no prior active to
    /// restore (nothing has ever been promoted, or a rollback already
    /// consumed the only prior).
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

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Redis-backed [`ModelRegistry`]. All gate/regression math runs in Rust
/// (pure, over already-fetched [`GateEvidence`] values) BEFORE any Redis
/// write — every write this type issues is therefore a determinate,
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
    async fn register_candidate(&self, mut version: ModelVersion) -> Result<(), CoreError> {
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

        // Spec §B7 separation of duties, made STRUCTURAL: a candidate is
        // always born bare. Force this rather than trusting the caller —
        // overwrite whatever `state`/`evidence`/`shadow_since` were passed
        // in, so a forged `evidence: Some(...)` on a caller-supplied
        // `ShadowCandidate`/`Active` version can never slip past this
        // write-once EXISTS guard and later reach `promote` without ever
        // going through `attach_evidence`.
        version.state = ModelState::Candidate;
        version.evidence = None;
        version.shadow_since = None;

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

    async fn get(&self, model_id: &str) -> Result<Option<ModelVersion>, CoreError> {
        self.get_model(model_id).await
    }

    async fn attach_evidence(
        &self,
        model_id: &str,
        evidence: GateEvidence,
        gate: &GateConfig,
    ) -> Result<GateDecision, CoreError> {
        let mut candidate = self.get_model(model_id).await?.ok_or(CoreError::NotFound)?;
        if candidate.state != ModelState::Candidate {
            return Err(CoreError::Conflict(format!(
                "model {model_id} is not in Candidate state (found {:?}) — evidence is \
                 attached exactly once",
                candidate.state
            )));
        }

        let active = self.get_active().await?;

        let mut reasons = gate_reasons(&evidence, gate);
        if let Some(active_version) = &active {
            if let Some(active_evidence) = &active_version.evidence {
                reasons.extend(regression_reasons(&evidence, active_evidence, gate));
            }
        }

        candidate.evidence = Some(evidence);

        if reasons.is_empty() {
            candidate.state = ModelState::ShadowCandidate;
            candidate.shadow_since = Some(now_ms());
            self.write_model(&candidate).await?;
            self.audit("attach_evidence:shadow", model_id, RETRAIN_ACTOR, &[])
                .await?;
            Ok(GateDecision::EnteredShadow(candidate))
        } else {
            candidate.state = ModelState::Rejected;
            self.write_model(&candidate).await?;
            self.audit("attach_evidence:reject", model_id, RETRAIN_ACTOR, &reasons)
                .await?;
            Ok(GateDecision::Rejected { reasons, candidate })
        }
    }

    async fn promote(
        &self,
        model_id: &str,
        approval: PromotionApproval,
        gate: &GateConfig,
    ) -> Result<ModelVersion, CoreError> {
        let mut candidate = self.get_model(model_id).await?.ok_or(CoreError::NotFound)?;
        if candidate.state != ModelState::ShadowCandidate {
            return Err(CoreError::Conflict(format!(
                "model {model_id} is not in ShadowCandidate state (found {:?}) — \
                 attach_evidence must pass before promote is even eligible",
                candidate.state
            )));
        }
        if candidate.evidence.is_none() {
            return Err(CoreError::PolicyRejected(format!(
                "model {model_id} has no gate evidence attached — cannot promote"
            )));
        }
        if gate.require_explicit_approval && !approval.approved {
            return Err(CoreError::PolicyRejected(
                "promote requires an explicit approval (require_explicit_approval=true)".into(),
            ));
        }
        if let Some(since) = candidate.shadow_since {
            let elapsed = now_ms() - since;
            if elapsed < gate.min_shadow_hold_ms {
                return Err(CoreError::PolicyRejected(format!(
                    "shadow hold period not yet elapsed: {elapsed}ms < {}ms",
                    gate.min_shadow_hold_ms
                )));
            }
        }

        let active = self.get_active().await?;

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

        self.audit("promote", &candidate.model_id, &approval.actor, &[])
            .await?;
        Ok(candidate)
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

        // The FULL prior ModelVersion record — bundle included (spec
        // §B8) — is what gets restored; nothing here reconstructs any
        // field piecemeal.
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

    fn family(byte: u8) -> FamilyId {
        FamilyId::parse(&format!("fam_00000000-0000-7000-8000-0000000000{byte:02x}")).unwrap()
    }

    fn passing_metrics() -> EvalMetricsSummary {
        EvalMetricsSummary {
            total_cases: 200,
            false_merge_rate: Some(0.0),
            false_merge_count: 0,
            false_merge_trap_count: 200,
            wrong_valid_rate: 0.001,
            accepted_precision: 0.999,
            exact_semantic_accuracy: 0.9,
            oracle_retrieval_exact_accuracy: Some(0.95),
            retrieval_recall_at_5: Some(0.98),
        }
    }

    fn passing_evidence() -> GateEvidence {
        GateEvidence {
            aggregate: passing_metrics(),
            per_family: vec![],
            false_merge_upper_ci: Some(0.005),
            computed_at: 0,
        }
    }

    #[test]
    fn any_nonzero_false_merge_count_fails_the_hard_gate() {
        let mut evidence = passing_evidence();
        evidence.aggregate.false_merge_count = 1;
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons.iter().any(|r| r.contains("false_merge_count")),
            "expected a false_merge_count failure reason, got {reasons:?}"
        );
    }

    #[test]
    fn zero_false_merges_over_too_small_n_is_inconclusive_not_a_pass() {
        // 0 observed false merges out of only 5 trap cases: the point
        // estimate is 0.0, but the Wilson upper bound is wide — this must
        // NOT be treated as "proven safe".
        let mut evidence = passing_evidence();
        evidence.aggregate.false_merge_trap_count = 5;
        evidence.aggregate.total_cases = 200;
        evidence.false_merge_upper_ci = Some(wilson_bound(0, 5, Z_95, true));
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("INCONCLUSIVE") && r.contains("false_merge")),
            "expected an inconclusive false-merge-CI reason, got {reasons:?}"
        );
    }

    #[test]
    fn no_false_merge_trap_cases_is_not_treated_as_a_failure() {
        let mut evidence = passing_evidence();
        evidence.aggregate.false_merge_rate = None;
        evidence.aggregate.false_merge_trap_count = 0;
        evidence.false_merge_upper_ci = None;
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons.is_empty(),
            "an absent false_merge_rate (no trap cases in the held-out corpus) must not fail \
             the gate: {reasons:?}"
        );
    }

    #[test]
    fn below_min_test_n_is_inconclusive() {
        let mut evidence = passing_evidence();
        evidence.aggregate.total_cases = 10;
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("INCONCLUSIVE") && r.contains("min_test_n")),
            "expected an inconclusive min_test_n reason, got {reasons:?}"
        );
    }

    #[test]
    fn wrong_valid_rate_above_threshold_fails() {
        let mut evidence = passing_evidence();
        evidence.aggregate.wrong_valid_rate = 0.05;
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(reasons.iter().any(|r| r.contains("wrong_valid_rate")));
    }

    #[test]
    fn accepted_precision_below_threshold_fails() {
        let mut evidence = passing_evidence();
        evidence.aggregate.accepted_precision = 0.5;
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(reasons.iter().any(|r| r.contains("accepted_precision")));
    }

    #[test]
    fn retrieval_recall_below_floor_fails_independently() {
        let mut evidence = passing_evidence();
        evidence.aggregate.retrieval_recall_at_5 = Some(0.5);
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(reasons.iter().any(|r| r.contains("retrieval_recall_at_5")));
    }

    #[test]
    fn fully_passing_evidence_has_no_gate_reasons() {
        let reasons = gate_reasons(&passing_evidence(), &GateConfig::default());
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    #[test]
    fn per_family_slice_below_floor_with_sufficient_n_is_rejected_even_if_aggregate_passes() {
        let mut evidence = passing_evidence();
        // Aggregate is fine (see passing_metrics), but one family slice
        // has plenty of N and low precision.
        evidence.per_family = vec![
            FamilySlice {
                family_id: family(1),
                n: 50,
                correct: 30, // 60% — well under the 99% floor
                precision: 0.6,
            },
            FamilySlice {
                family_id: family(2),
                n: 50,
                correct: 50,
                precision: 1.0,
            },
        ];
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons
                .iter()
                .any(|r| r.contains("family") && r.contains(family(1).as_str())),
            "expected a per-family rejection reason for the bad slice, got {reasons:?}"
        );
    }

    #[test]
    fn per_family_slice_below_floor_with_insufficient_n_is_exempt() {
        let mut evidence = passing_evidence();
        evidence.per_family = vec![FamilySlice {
            family_id: family(1),
            n: 3, // well under per_family_min_n (20)
            correct: 0,
            precision: 0.0,
        }];
        let reasons = gate_reasons(&evidence, &GateConfig::default());
        assert!(
            reasons.is_empty(),
            "a tiny slice must not fail the gate on insufficient evidence: {reasons:?}"
        );
    }

    #[test]
    fn a_candidate_that_regresses_accuracy_beyond_the_margin_is_flagged() {
        let active = passing_evidence();
        let mut candidate = passing_evidence();
        candidate.aggregate.exact_semantic_accuracy =
            active.aggregate.exact_semantic_accuracy - 0.2;
        let reasons = regression_reasons(&candidate, &active, &GateConfig::default());
        assert!(reasons
            .iter()
            .any(|r| r.contains("exact_semantic_accuracy")));
    }

    #[test]
    fn a_tiny_regression_within_the_non_inferiority_margin_is_not_flagged() {
        let active = passing_evidence();
        let mut candidate = passing_evidence();
        candidate.aggregate.exact_semantic_accuracy =
            active.aggregate.exact_semantic_accuracy - 0.001; // well within default margin 0.01
        let reasons = regression_reasons(&candidate, &active, &GateConfig::default());
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    #[test]
    fn a_candidate_that_improves_never_regresses() {
        let active = passing_evidence();
        let mut candidate = passing_evidence();
        candidate.aggregate.exact_semantic_accuracy =
            active.aggregate.exact_semantic_accuracy + 0.05;
        candidate.aggregate.wrong_valid_rate = active.aggregate.wrong_valid_rate / 2.0;
        let reasons = regression_reasons(&candidate, &active, &GateConfig::default());
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    /// Spec §B12 acceptance: a candidate with GOOD (even improved)
    /// generator accuracy but a retrieval-recall regression must still be
    /// flagged — by the retrieval gate specifically, not folded into (or
    /// hidden by) the generator-accuracy checks.
    #[test]
    fn good_generator_accuracy_does_not_mask_a_retrieval_recall_regression() {
        let active = passing_evidence();
        let mut candidate = passing_evidence();
        candidate.aggregate.exact_semantic_accuracy =
            active.aggregate.exact_semantic_accuracy + 0.03;
        candidate.aggregate.oracle_retrieval_exact_accuracy =
            active.aggregate.oracle_retrieval_exact_accuracy;
        candidate.aggregate.retrieval_recall_at_5 = Some(0.5); // active is 0.98
        let reasons = regression_reasons(&candidate, &active, &GateConfig::default());
        assert!(
            reasons.iter().any(|r| r.contains("retrieval_recall_at_5")),
            "expected the retrieval gate to flag the regression independently: {reasons:?}"
        );
        assert!(
            !reasons
                .iter()
                .any(|r| r.contains("exact_semantic_accuracy")),
            "the (improved) generator-accuracy axis must not itself be flagged: {reasons:?}"
        );
    }

    #[test]
    fn wilson_bound_widens_as_n_shrinks() {
        let wide = wilson_bound(0, 5, Z_95, true);
        let tight = wilson_bound(0, 5000, Z_95, true);
        assert!(
            wide > tight,
            "a smaller N must produce a wider (less confident) upper bound: {wide} vs {tight}"
        );
    }

    #[test]
    fn wilson_bound_handles_zero_n() {
        assert_eq!(wilson_bound(0, 0, Z_95, true), 1.0);
        assert_eq!(wilson_bound(0, 0, Z_95, false), 0.0);
    }
}
