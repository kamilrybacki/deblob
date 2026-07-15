//! Eval metric computation (deblob-p2ab Task 7; authoritative metric set
//! per `docs/superpowers/plans/deblob-p2ab-hermes-review.md` §
//! "Tasks 6-7 — eval metrics + corpus", which overrides the corresponding
//! "AMEND" marker in `docs/superpowers/plans/2026-07-14-deblob-p2ab.md`
//! § Task 7).
//!
//! [`run_eval`] drives a configured `SemanticInferencer` against every
//! [`EvalCase`] in a loaded corpus (Task 6) and collects the raw
//! (expected, actual, telemetry, retrieval) records into an [`EvalRun`].
//! [`compute_metrics`] is a PURE function over that run: it never talks
//! to a model itself, so every number in a [`Metrics`] report is
//! reproducible from the same `EvalRun`.
//!
//! ## The headline: wrong-valid tracked apart from schema-valid
//!
//! Per the Hermes review (Task 2 amendment): "100% schema-valid is NOT a
//! success criterion." [`Metrics::schema_valid_rate`] answers "did the
//! output conform to the 3-way contract" (no parse/schema-validation
//! error). [`Metrics::wrong_valid_rate`] answers a DIFFERENT question:
//! "of ALL cases, how many produced a contract-conformant answer that was
//! nonetheless the WRONG answer" — a case can be schema-valid AND
//! wrong-valid at once; it can never be wrong-valid without also being
//! schema-valid. These two rates are computed from independent counters
//! in [`compute_metrics`] specifically so a high schema-valid rate can
//! never mask a high wrong-valid rate (see `wrong_valid_counted_apart_from_schema_valid`
//! in this module's tests).
//!
//! [`Metrics::false_merge_rate`] is Hermes' hard go-live gate (§ Task 5:
//! "false merges corrupt identity") and is likewise tracked separately
//! from [`Metrics::false_split_rate`] and from generic wrong-valid error.
//!
//! ## Deferred metrics
//!
//! Several metrics on Hermes' list need data this crate's golden-corpus
//! eval structurally cannot produce from a single `run_eval` pass, or at
//! all. Each such field on [`Metrics`] is `None`/absent — NEVER a
//! fabricated `0.0` — with its own doc comment explaining why:
//!
//! - **Needs a second `classify()` pass, not a single `EvalRun`:**
//!   candidate-order sensitivity ([`measure_candidate_order_sensitivity`])
//!   and temp-0 repeatability ([`measure_repeatability`]) are separate
//!   async functions a caller runs and folds into a [`Metrics`] value
//!   after [`compute_metrics`]; a prompt/model/quant regression delta
//!   needs TWO `Metrics` values to diff ([`regression_delta`]).
//! - **Needs data outside the golden corpus schema entirely:** redaction-
//!   induced accuracy loss (no paired raw/redacted case exists — raw
//!   values structurally never reach this layer), human-label
//!   inter-annotator agreement (needs a second independent human
//!   annotation pass, not the corpus's single adjudicated `expected`),
//!   counterfactual unsafe-acceptance rate (needs
//!   `ShadowDecision::counterfactual_live_disposition`, a Task 5
//!   shadow-log field the `EvalCase` schema does not carry), a
//!   distinguishable "refusal" signal (the 3-way contract has none
//!   separate from `Abstain`/parse failure), prefill/decode latency
//!   split (the current `HttpInferencer` is request/response, not
//!   streaming — see `InferenceTelemetry::ttft_ms`'s docs), and cost
//!   (no pricing table is wired in; [`Metrics::avg_request_tokens`] /
//!   [`Metrics::avg_response_tokens`] are the inputs an operator applies
//!   externally).

use std::collections::HashMap;

use deblob_core::id::SchemaId;
use deblob_slm::{
    build_prompt, AbstainCause, CandidateProfileView, FamilyCandidate, InferenceBudget,
    InferenceDecision, InferenceError, InferenceOutcome, InferenceRequest, Relation,
    SemanticInferencer,
};
use serde::Serialize;

use crate::corpus::{Category, EvalCase, Expected};

/// Contract version this harness speaks (Task 1). Not model-supplied —
/// stamped by the caller, per Hermes' "store OUTSIDE the model output"
/// note on `InferenceRequest::contract_version`.
pub const CONTRACT_VERSION: u32 = 1;

/// Budget applied uniformly to every case's `InferenceRequest`. Only
/// shapes the request scaffolding a real endpoint would see — never
/// scored by the harness itself.
pub const DEFAULT_BUDGET: InferenceBudget = InferenceBudget {
    max_prompt_tokens: 4096,
    timeout_ms: 30_000,
};

/// Minimum case count a [`Category`] slice must have before it is
/// eligible for [`Metrics::worst_slice_precision`]. Hermes: "min
/// precision over slices with ≥ a threshold count; with the seed corpus,
/// per-Category worst-slice is fine" — set to `1` so every category the
/// (small) seed corpus actually covers is eligible; a larger corpus can
/// raise this bar without changing the computation.
const MIN_SLICE_COUNT: usize = 1;

// --- Run collection ---------------------------------------------------------

/// [`InferenceError`] mirrored into a `Clone + PartialEq` shape so a
/// total `classify()` failure can be stored alongside the successful
/// [`InferenceOutcome`] case in [`CaseResult::outcome`]. `InferenceError`
/// itself only derives `thiserror::Error` (+`Debug`), not `Clone`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallFailure {
    Transport(String),
    Timeout,
    Parse(String),
}

impl From<&InferenceError> for CallFailure {
    fn from(err: &InferenceError) -> Self {
        match err {
            InferenceError::Transport(msg) => CallFailure::Transport(msg.clone()),
            InferenceError::Timeout => CallFailure::Timeout,
            InferenceError::Parse(msg) => CallFailure::Parse(msg.clone()),
        }
    }
}

/// One corpus case's ground truth paired with what the configured
/// inferencer actually produced. Self-contained (carries its own copy of
/// the case's `category`/`candidate`/`retrieved`/`expected`) so
/// [`compute_metrics`] never needs to re-look-up the source [`EvalCase`]
/// by name.
#[derive(Debug, Clone)]
pub struct CaseResult {
    pub case_name: String,
    pub category: Category,
    pub candidate: CandidateProfileView,
    pub retrieved: Vec<FamilyCandidate>,
    pub expected: Expected,
    /// `Ok` for every case that reached a decision, whether on the first
    /// attempt or after the inferencer's own internal repair — see
    /// `SemanticInferencer::classify`'s docs: a contract-invalid-but-
    /// unrecoverable response is `Ok(InferenceOutcome{decision: Abstain{..}, ..})`
    /// with `telemetry.parse_error`/`schema_validation_error` set, NOT
    /// this `Err` branch. `Err` is reserved for a TOTAL transport/timeout/
    /// parse failure with no outcome or telemetry at all.
    pub outcome: Result<InferenceOutcome, CallFailure>,
}

/// The raw collected results of one [`run_eval`] pass, one [`CaseResult`]
/// per corpus case in corpus order.
#[derive(Debug, Clone, Default)]
pub struct EvalRun {
    pub records: Vec<CaseResult>,
}

/// Drives `inferencer` against every case in `corpus`, sequentially and in
/// corpus order (deterministic — required for [`measure_repeatability`]
/// and any caller diffing two runs). For each case: builds the exact
/// `InferenceRequest` shape a real endpoint would see (redacted candidate,
/// retrieved top-k, and the real PII-safe prompt via
/// `deblob_slm::build_prompt`), calls `inferencer.classify`, and records
/// the (expected, actual, telemetry, retrieval) tuple as a [`CaseResult`].
pub async fn run_eval(inferencer: &dyn SemanticInferencer, corpus: &[EvalCase]) -> EvalRun {
    let mut records = Vec::with_capacity(corpus.len());
    for case in corpus {
        records.push(run_one(inferencer, case).await);
    }
    EvalRun { records }
}

async fn run_one(inferencer: &dyn SemanticInferencer, case: &EvalCase) -> CaseResult {
    let allowed_ids: Vec<SchemaId> = case.retrieved.iter().map(|c| c.schema_id.clone()).collect();
    let prompt = build_prompt(&case.candidate, &case.retrieved, &allowed_ids).text;
    let request = InferenceRequest {
        candidate: case.candidate.clone(),
        retrieved: case.retrieved.clone(),
        contract_version: CONTRACT_VERSION,
        budget: DEFAULT_BUDGET,
        prompt,
    };
    let outcome = inferencer
        .classify(request)
        .await
        .map_err(|err| CallFailure::from(&err));
    CaseResult {
        case_name: case.name.clone(),
        category: case.category,
        candidate: case.candidate.clone(),
        retrieved: case.retrieved.clone(),
        expected: case.expected.clone(),
        outcome,
    }
}

// --- Metrics ------------------------------------------------------------

/// One (expected relation, actual relation) pair observed over cases
/// where `expected.decision` was `MatchSchema` — Hermes' "relation
/// confusion matrix (expected relation × actual relation over match
/// cases)". `actual: None` means the actual decision was NOT a
/// `MatchSchema` at all (e.g. the model abstained or called it new when a
/// match was expected).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RelationConfusionEntry {
    pub expected: Relation,
    pub actual: Option<Relation>,
    pub count: u32,
}

/// Exact-decision precision for one [`Category`] slice, and how many
/// cases backed it — see [`Metrics::worst_slice_precision`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CategoryPrecision {
    pub category: Category,
    pub precision: f64,
    pub count: usize,
}

/// The full metric report computed by [`compute_metrics`] over one
/// [`EvalRun`]. See the module docs for which fields are `None` by
/// construction (never a fabricated value) and why.
#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    pub total_cases: usize,

    // -- parse / schema-valid / semantic correctness ------------------------
    /// Fraction of cases whose final response parsed as JSON at all (no
    /// `telemetry.parse_error`, and no total `CallFailure`).
    pub json_parse_rate: f64,
    /// Fraction of cases whose final response conformed to the 3-way
    /// contract: parsed AND passed id-allow-list validation (no
    /// `parse_error`, no `schema_validation_error`). NOT a success
    /// criterion on its own — see `wrong_valid_rate`.
    pub schema_valid_rate: f64,
    /// Fraction of cases where the actual decision equals the expected
    /// decision EXACTLY (same top-level kind, same family/relation for a
    /// match, same cause for an abstain).
    pub exact_semantic_accuracy: f64,
    /// Fraction of cases where only the top-level decision kind
    /// (match/new/abstain) matches expected, ignoring relation/novelty/
    /// cause detail.
    pub decision_choice_accuracy: f64,
    /// THE HEADLINE METRIC. Fraction of ALL cases that were schema-valid
    /// but semantically WRONG (`schema_valid && actual != expected`).
    /// Computed from a counter independent of `schema_valid_rate` — see
    /// the module docs.
    pub wrong_valid_rate: f64,
    pub wrong_valid_count: usize,

    // -- abstention -----------------------------------------------------------
    /// Of the cases where the model actually abstained, the fraction
    /// where abstaining was the correct call. `None` if the model never
    /// abstained.
    pub abstention_precision: Option<f64>,
    /// Of the cases where the model SHOULD have abstained, the fraction
    /// where it did. `None` if no case called for an abstain.
    pub abstention_recall: Option<f64>,

    // -- id-constraint ----------------------------------------------------------
    /// Count of `MatchSchema` decisions naming a `schema_id` outside that
    /// case's retrieved top-k. Should be ~0 given Task 2's id allow-list
    /// validation on a real `HttpInferencer`; nonzero here means the
    /// configured `SemanticInferencer` under test bypassed it.
    pub id_constraint_violations: usize,

    // -- retrieval quality (recall@k / MRR) --------------------------------------
    /// Fraction of gold-bearing cases (`expected.gold_schema_id.is_some()`)
    /// whose gold schema appeared in the retrieved top-k at rank ≤ 1/3/5.
    /// `None` if no case in the run carries a gold schema id.
    pub recall_at_1: Option<f64>,
    pub recall_at_3: Option<f64>,
    pub recall_at_5: Option<f64>,
    /// Mean reciprocal rank of the gold schema (`1/gold_rank`, `0` if the
    /// gold schema was not retrieved) over gold-bearing cases.
    pub mrr: Option<f64>,

    // -- false-merge / false-split: SEPARATE from generic wrong-valid ------------
    /// Of the `false_merge_trap` cases, the fraction where the model
    /// ACCEPTED a match (`is_accepted_match()`) to the WRONG family.
    /// Hermes' hard go-live gate. `None` if the corpus has no
    /// false-merge-trap case.
    pub false_merge_rate: Option<f64>,
    pub false_merge_count: usize,
    pub false_merge_trap_count: usize,
    /// Of the `false_split_trap` cases, the fraction where the model
    /// failed to accept the match it should have (new_candidate,
    /// abstain, or incompatible_similarity instead). `None` if the
    /// corpus has no false-split-trap case.
    pub false_split_rate: Option<f64>,
    pub false_split_count: usize,
    pub false_split_trap_count: usize,

    // -- relation confusion -------------------------------------------------------
    pub relation_confusion: Vec<RelationConfusionEntry>,

    // -- novel family --------------------------------------------------------------
    /// Of the `NewFamily`-category cases, the fraction correctly called
    /// `new_candidate`. `None` if the run has no `NewFamily` case.
    pub novel_family_recall: Option<f64>,
    /// Of the cases where the model called `new_candidate`, the fraction
    /// where that was actually correct. `None` if the model never called
    /// `new_candidate`.
    pub novel_family_precision: Option<f64>,

    // -- gold-absent abstention ------------------------------------------------------
    /// Of the mandatory gold-absent cases (`gold_schema_id` set,
    /// `gold_rank` absent — the true family exists but retrieval missed
    /// it), the fraction that correctly abstained with
    /// `AbstainCause::CandidateMissing`. `None` if the run has no such
    /// case.
    pub gold_absent_abstention_rate: Option<f64>,

    // -- per-category worst-slice precision ---------------------------------------------
    pub per_category_precision: Vec<CategoryPrecision>,
    /// Minimum `exact_semantic_accuracy`-style precision over
    /// [`Category`] slices with at least [`MIN_SLICE_COUNT`] cases.
    /// `None` if no slice met the threshold.
    pub worst_slice_precision: Option<f64>,
    pub worst_slice_category: Option<Category>,

    // -- prompt-injection resistance -------------------------------------------------------
    /// Of the cases carrying at least one injection-flagged field name
    /// (`deblob_slm::detect_injection`, surfaced on
    /// `CandidateProfileView::fields[..].path[..].injection_flagged`),
    /// the fraction the model still got exactly right. `None` if the run
    /// has no injection-flagged case.
    pub prompt_injection_resistance: Option<f64>,
    pub prompt_injection_case_count: usize,

    // -- repair -----------------------------------------------------------------------------
    /// Fraction of cases where the inferencer's internal repair ran
    /// (`telemetry.repair_count > 0`).
    pub repair_rate: f64,
    /// Of the repaired cases, the fraction that ended up schema-valid.
    /// `None` if no case was repaired.
    pub repair_success_rate: Option<f64>,
    /// `sum(repair_count) / count(accepted matches)` — Task 7's
    /// plan-level "repairs-per-accepted". `None` if no case was an
    /// accepted match.
    pub repairs_per_accepted: Option<f64>,

    // -- failure classes (from CallFailure / telemetry) ----------------------------------------
    /// Fraction of cases whose `classify()` call returned
    /// `InferenceError::Timeout`.
    pub timeout_rate: f64,
    /// Fraction of cases whose `classify()` call returned
    /// `InferenceError::Transport`.
    pub provider_error_rate: f64,
    /// Fraction of cases whose FINAL outcome was malformed: either a
    /// total `InferenceError::Parse` failure, or an `Ok` outcome with
    /// `telemetry.parse_error` set (repair could not recover a parseable
    /// response).
    pub malformed_rate: f64,
    /// Deferred — see the module docs' "needs data outside the golden
    /// corpus schema entirely" section.
    pub refusal_rate: Option<f64>,

    // -- latency ------------------------------------------------------------------------------
    pub ttft_p50_ms: Option<u64>,
    pub ttft_p95_ms: Option<u64>,
    pub total_latency_p50_ms: Option<u64>,
    pub total_latency_p95_ms: Option<u64>,
    /// Deferred — see module docs.
    pub prefill_latency_p50_ms: Option<u64>,
    /// Deferred — see module docs.
    pub decode_latency_p50_ms: Option<u64>,

    // -- tokens (separate from latency) --------------------------------------------------------
    pub avg_request_tokens: Option<f64>,
    pub avg_response_tokens: Option<f64>,
    /// Deferred — see module docs.
    pub cost: Option<f64>,

    // -- whole-lane cache-hit / invocation-avoidance -------------------------------------------
    /// Fraction of cases served from the inferencer's decision cache
    /// (heuristic: `telemetry.total_latency_ms.is_none()` on an `Ok`
    /// outcome — the only documented reason it's unset, per
    /// `InferenceTelemetry::total_latency_ms`'s docs).
    pub cache_hit_rate: Option<f64>,

    // -- multi-run metrics: filled by the caller from a SECOND pass, not by
    // `compute_metrics` itself (a single `EvalRun` cannot answer these) -----------------------
    /// See [`measure_candidate_order_sensitivity`]. `None` until a caller
    /// runs that separate pass and assigns the result here.
    pub candidate_order_sensitivity: Option<f64>,
    /// See [`measure_repeatability`]. `None` until a caller runs that
    /// separate pass and assigns the result here.
    pub repeatability: Option<f64>,

    // -- needs data this crate structurally never has (see module docs) -----------------------
    pub redaction_induced_accuracy_loss: Option<f64>,
    pub human_label_iaa: Option<f64>,
    pub counterfactual_unsafe_acceptance_rate: Option<f64>,
}

fn is_gold_absent_case(expected: &Expected) -> bool {
    expected.gold_schema_id.is_some() && expected.gold_rank.is_none()
}

fn decision_kind_matches(a: &InferenceDecision, b: &InferenceDecision) -> bool {
    matches!(
        (a, b),
        (
            InferenceDecision::MatchSchema { .. },
            InferenceDecision::MatchSchema { .. }
        ) | (
            InferenceDecision::NewCandidate { .. },
            InferenceDecision::NewCandidate { .. }
        ) | (
            InferenceDecision::Abstain { .. },
            InferenceDecision::Abstain { .. }
        )
    )
}

fn candidate_has_injection_flag(candidate: &CandidateProfileView) -> bool {
    candidate
        .fields
        .iter()
        .any(|field| field.path.iter().any(|segment| segment.injection_flagged))
}

fn bump_relation_confusion(
    matrix: &mut Vec<RelationConfusionEntry>,
    expected: Relation,
    actual: Option<Relation>,
) {
    if let Some(entry) = matrix
        .iter_mut()
        .find(|entry| entry.expected == expected && entry.actual == actual)
    {
        entry.count += 1;
    } else {
        matrix.push(RelationConfusionEntry {
            expected,
            actual,
            count: 1,
        });
    }
}

fn safe_rate(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn safe_rate_opt(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn avg_u32(values: &[u32]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().map(|v| f64::from(*v)).sum::<f64>() / values.len() as f64)
    }
}

/// Nearest-rank percentile over `values` (consumed, sorted internally).
/// `p` in `[0, 100]`. `None` if `values` is empty.
fn percentile_u64(mut values: Vec<u64>, p: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let rank = ((p / 100.0) * (values.len() as f64 - 1.0)).round() as usize;
    values.get(rank.min(values.len() - 1)).copied()
}

/// Computes every metric in Hermes' Task 6-7 list that a single
/// [`EvalRun`] over `corpus` supports. See the module docs for the
/// (documented, never fabricated) `None` fields.
///
/// `corpus` is accepted (rather than deriving everything from `run`
/// alone) to keep the function's contract explicit about what ground
/// truth it scores against, and so a future metric needing corpus-level
/// context beyond what `CaseResult` self-contains (e.g. total corpus
/// composition) has it available without changing the signature again.
pub fn compute_metrics(run: &EvalRun, corpus: &[EvalCase]) -> Metrics {
    debug_assert_eq!(
        run.records.len(),
        corpus.len(),
        "an EvalRun produced by run_eval(inferencer, corpus) always has exactly one CaseResult \
         per corpus case, in corpus order"
    );

    let total = run.records.len();

    let mut json_parsed = 0usize;
    let mut schema_valid = 0usize;
    let mut exact_correct = 0usize;
    let mut decision_choice_correct = 0usize;
    let mut wrong_valid = 0usize;
    let mut id_violations = 0usize;

    let mut abstain_true_positive = 0usize;
    let mut abstain_actual = 0usize;
    let mut abstain_should = 0usize;

    let mut gold_present = 0usize;
    let mut recall1 = 0usize;
    let mut recall3 = 0usize;
    let mut recall5 = 0usize;
    let mut reciprocal_rank_sum = 0.0f64;

    let mut false_merge_trap_count = 0usize;
    let mut false_merge_count = 0usize;
    let mut false_split_trap_count = 0usize;
    let mut false_split_count = 0usize;

    let mut relation_confusion: Vec<RelationConfusionEntry> = Vec::new();

    let mut new_family_total = 0usize;
    let mut new_family_recalled = 0usize;
    let mut predicted_new_total = 0usize;
    let mut predicted_new_correct = 0usize;

    let mut gold_absent_total = 0usize;
    let mut gold_absent_correct = 0usize;

    let mut category_totals: HashMap<Category, (usize, usize)> = HashMap::new();

    let mut injection_total = 0usize;
    let mut injection_correct = 0usize;

    let mut repaired = 0usize;
    let mut repaired_and_valid = 0usize;
    let mut repair_count_sum: u64 = 0;
    let mut accepted_count = 0usize;

    let mut timeout_count = 0usize;
    let mut provider_error_count = 0usize;
    let mut malformed_count = 0usize;

    let mut ttft_values: Vec<u64> = Vec::new();
    let mut latency_values: Vec<u64> = Vec::new();
    let mut request_tokens: Vec<u32> = Vec::new();
    let mut response_tokens: Vec<u32> = Vec::new();
    let mut cache_hits = 0usize;

    for record in &run.records {
        let expected_decision = &record.expected.decision;

        // Retrieval quality: independent of whether classify() succeeded.
        if record.expected.gold_schema_id.is_some() {
            gold_present += 1;
            if let Some(rank) = record.expected.gold_rank {
                if rank <= 1 {
                    recall1 += 1;
                }
                if rank <= 3 {
                    recall3 += 1;
                }
                if rank <= 5 {
                    recall5 += 1;
                }
                reciprocal_rank_sum += 1.0 / f64::from(rank);
            }
        }

        if record.expected.false_merge_trap {
            false_merge_trap_count += 1;
        }
        if record.expected.false_split_trap {
            false_split_trap_count += 1;
        }
        if record.category == Category::NewFamily {
            new_family_total += 1;
        }
        if is_gold_absent_case(&record.expected) {
            gold_absent_total += 1;
        }

        let category_entry = category_totals.entry(record.category).or_insert((0, 0));
        category_entry.1 += 1;

        match &record.outcome {
            Ok(outcome) => {
                let telemetry = &outcome.telemetry;
                let actual = &outcome.decision;
                let is_exact = actual == expected_decision;

                if telemetry.parse_error {
                    malformed_count += 1;
                } else {
                    json_parsed += 1;
                }
                let is_schema_valid = !telemetry.parse_error && !telemetry.schema_validation_error;
                if is_schema_valid {
                    schema_valid += 1;
                }
                if is_exact {
                    exact_correct += 1;
                    category_entry.0 += 1;
                } else if is_schema_valid {
                    wrong_valid += 1;
                }
                if decision_kind_matches(actual, expected_decision) {
                    decision_choice_correct += 1;
                }

                if let InferenceDecision::MatchSchema { schema_id, .. } = actual {
                    if !record.retrieved.iter().any(|c| &c.schema_id == schema_id) {
                        id_violations += 1;
                    }
                }

                let did_abstain = matches!(actual, InferenceDecision::Abstain { .. });
                let should_abstain = matches!(expected_decision, InferenceDecision::Abstain { .. });
                if did_abstain {
                    abstain_actual += 1;
                    if should_abstain {
                        abstain_true_positive += 1;
                    }
                }
                if should_abstain {
                    abstain_should += 1;
                }

                if record.expected.false_merge_trap && actual.is_accepted_match() && !is_exact {
                    false_merge_count += 1;
                }
                if record.expected.false_split_trap && !actual.is_accepted_match() {
                    false_split_count += 1;
                }

                if let InferenceDecision::MatchSchema {
                    relation: expected_relation,
                    ..
                } = expected_decision
                {
                    let actual_relation = match actual {
                        InferenceDecision::MatchSchema { relation, .. } => Some(*relation),
                        _ => None,
                    };
                    bump_relation_confusion(
                        &mut relation_confusion,
                        *expected_relation,
                        actual_relation,
                    );
                }

                let predicted_new = matches!(actual, InferenceDecision::NewCandidate { .. });
                if record.category == Category::NewFamily && predicted_new {
                    new_family_recalled += 1;
                }
                if predicted_new {
                    predicted_new_total += 1;
                    if matches!(expected_decision, InferenceDecision::NewCandidate { .. }) {
                        predicted_new_correct += 1;
                    }
                }

                if is_gold_absent_case(&record.expected)
                    && matches!(
                        actual,
                        InferenceDecision::Abstain {
                            cause: AbstainCause::CandidateMissing
                        }
                    )
                {
                    gold_absent_correct += 1;
                }

                if candidate_has_injection_flag(&record.candidate) {
                    injection_total += 1;
                    if is_exact {
                        injection_correct += 1;
                    }
                }

                if telemetry.repair_count > 0 {
                    repaired += 1;
                    if is_schema_valid {
                        repaired_and_valid += 1;
                    }
                }
                repair_count_sum += u64::from(telemetry.repair_count);
                if actual.is_accepted_match() {
                    accepted_count += 1;
                }

                if let Some(ttft) = telemetry.ttft_ms {
                    ttft_values.push(ttft);
                }
                if let Some(latency) = telemetry.total_latency_ms {
                    latency_values.push(latency);
                } else {
                    cache_hits += 1;
                }
                if let Some(tokens) = telemetry.request_tokens {
                    request_tokens.push(tokens);
                }
                if let Some(tokens) = telemetry.response_tokens {
                    response_tokens.push(tokens);
                }
            }
            Err(failure) => {
                match failure {
                    CallFailure::Timeout => timeout_count += 1,
                    CallFailure::Transport(_) => provider_error_count += 1,
                    CallFailure::Parse(_) => malformed_count += 1,
                }
                // A total failure never accepts a match, so it always
                // counts as a split (never a merge) against a trap case.
                if record.expected.false_split_trap {
                    false_split_count += 1;
                }
            }
        }
    }

    let mut per_category_precision: Vec<CategoryPrecision> = Vec::new();
    for category in [
        Category::KnownExact,
        Category::CompatibleDrift,
        Category::IncompatibleUnsafe,
        Category::NewFamily,
        Category::AmbiguousAdversarial,
    ] {
        if let Some((correct, count)) = category_totals.get(&category) {
            if *count > 0 {
                per_category_precision.push(CategoryPrecision {
                    category,
                    precision: *correct as f64 / *count as f64,
                    count: *count,
                });
            }
        }
    }
    let worst_slice = per_category_precision
        .iter()
        .filter(|slice| slice.count >= MIN_SLICE_COUNT)
        .min_by(|a, b| {
            a.precision
                .partial_cmp(&b.precision)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    let worst_slice_precision = worst_slice.map(|slice| slice.precision);
    let worst_slice_category = worst_slice.map(|slice| slice.category);

    Metrics {
        total_cases: total,

        json_parse_rate: safe_rate(json_parsed, total),
        schema_valid_rate: safe_rate(schema_valid, total),
        exact_semantic_accuracy: safe_rate(exact_correct, total),
        decision_choice_accuracy: safe_rate(decision_choice_correct, total),
        wrong_valid_rate: safe_rate(wrong_valid, total),
        wrong_valid_count: wrong_valid,

        abstention_precision: safe_rate_opt(abstain_true_positive, abstain_actual),
        abstention_recall: safe_rate_opt(abstain_true_positive, abstain_should),

        id_constraint_violations: id_violations,

        recall_at_1: safe_rate_opt(recall1, gold_present),
        recall_at_3: safe_rate_opt(recall3, gold_present),
        recall_at_5: safe_rate_opt(recall5, gold_present),
        mrr: if gold_present == 0 {
            None
        } else {
            Some(reciprocal_rank_sum / gold_present as f64)
        },

        false_merge_rate: safe_rate_opt(false_merge_count, false_merge_trap_count),
        false_merge_count,
        false_merge_trap_count,
        false_split_rate: safe_rate_opt(false_split_count, false_split_trap_count),
        false_split_count,
        false_split_trap_count,

        relation_confusion,

        novel_family_recall: safe_rate_opt(new_family_recalled, new_family_total),
        novel_family_precision: safe_rate_opt(predicted_new_correct, predicted_new_total),

        gold_absent_abstention_rate: safe_rate_opt(gold_absent_correct, gold_absent_total),

        per_category_precision,
        worst_slice_precision,
        worst_slice_category,

        prompt_injection_resistance: safe_rate_opt(injection_correct, injection_total),
        prompt_injection_case_count: injection_total,

        repair_rate: safe_rate(repaired, total),
        repair_success_rate: safe_rate_opt(repaired_and_valid, repaired),
        repairs_per_accepted: if accepted_count == 0 {
            None
        } else {
            Some(repair_count_sum as f64 / accepted_count as f64)
        },

        timeout_rate: safe_rate(timeout_count, total),
        provider_error_rate: safe_rate(provider_error_count, total),
        malformed_rate: safe_rate(malformed_count, total),
        refusal_rate: None,

        ttft_p50_ms: percentile_u64(ttft_values.clone(), 50.0),
        ttft_p95_ms: percentile_u64(ttft_values, 95.0),
        total_latency_p50_ms: percentile_u64(latency_values.clone(), 50.0),
        total_latency_p95_ms: percentile_u64(latency_values, 95.0),
        prefill_latency_p50_ms: None,
        decode_latency_p50_ms: None,

        avg_request_tokens: avg_u32(&request_tokens),
        avg_response_tokens: avg_u32(&response_tokens),
        cost: None,

        cache_hit_rate: safe_rate_opt(cache_hits, total),

        candidate_order_sensitivity: None,
        repeatability: None,

        redaction_induced_accuracy_loss: None,
        human_label_iaa: None,
        counterfactual_unsafe_acceptance_rate: None,
    }
}

// --- Multi-run metrics (separate passes; fold into a Metrics by hand) ------

/// Hermes: "candidate-order sensitivity (optional/if feasible: same case
/// with permuted retrieved order → decision changed?)". Re-runs every
/// case with ≥2 retrieved candidates a SECOND time with the top-two
/// candidates' rank/distance swapped (a genuine order permutation — note
/// `deblob_slm::build_prompt` already renders `retrieved` in a canonical
/// rank-sorted order regardless of caller-supplied `Vec` ordering, so
/// merely reversing the `Vec` would not change what the model sees; the
/// rank/distance VALUES must actually change) and reports the fraction
/// whose decision changed. `None` if no case in `corpus` has ≥2 retrieved
/// candidates to permute.
///
/// This is a second `classify()` pass per eligible case, so it cannot be
/// folded into [`compute_metrics`] (which is pure over an already-
/// collected [`EvalRun`]); a caller assigns the result to
/// `Metrics::candidate_order_sensitivity` by hand.
pub async fn measure_candidate_order_sensitivity(
    inferencer: &dyn SemanticInferencer,
    corpus: &[EvalCase],
) -> Option<f64> {
    let eligible: Vec<&EvalCase> = corpus.iter().filter(|c| c.retrieved.len() >= 2).collect();
    if eligible.is_empty() {
        return None;
    }

    let mut changed = 0usize;
    for case in &eligible {
        let original = run_one(inferencer, case).await;

        let mut permuted_case = (*case).clone();
        permuted_case.retrieved.sort_by_key(|c| c.rank);
        let (rank0, distance0) = (
            permuted_case.retrieved[0].rank,
            permuted_case.retrieved[0].distance,
        );
        let (rank1, distance1) = (
            permuted_case.retrieved[1].rank,
            permuted_case.retrieved[1].distance,
        );
        permuted_case.retrieved[0].rank = rank1;
        permuted_case.retrieved[0].distance = distance1;
        permuted_case.retrieved[1].rank = rank0;
        permuted_case.retrieved[1].distance = distance0;

        let permuted = run_one(inferencer, &permuted_case).await;

        if !outcomes_agree(&original.outcome, &permuted.outcome) {
            changed += 1;
        }
    }
    Some(changed as f64 / eligible.len() as f64)
}

/// Hermes: "repeatability across 3 temp-0 runs (if the inferencer is
/// deterministic/temp-0 — a fake will be)". Runs the full corpus through
/// `inferencer` `runs` times and reports the fraction of cases where
/// EVERY run produced the same decision. `None` if `runs < 2` or `corpus`
/// is empty (nothing to compare).
pub async fn measure_repeatability(
    inferencer: &dyn SemanticInferencer,
    corpus: &[EvalCase],
    runs: usize,
) -> Option<f64> {
    if runs < 2 || corpus.is_empty() {
        return None;
    }

    let mut all_runs: Vec<EvalRun> = Vec::with_capacity(runs);
    for _ in 0..runs {
        all_runs.push(run_eval(inferencer, corpus).await);
    }

    let mut agree = 0usize;
    for i in 0..corpus.len() {
        let baseline = &all_runs[0].records[i].outcome;
        if all_runs
            .iter()
            .all(|run| outcomes_agree(&run.records[i].outcome, baseline))
        {
            agree += 1;
        }
    }
    Some(agree as f64 / corpus.len() as f64)
}

fn outcomes_agree(
    a: &Result<InferenceOutcome, CallFailure>,
    b: &Result<InferenceOutcome, CallFailure>,
) -> bool {
    match (a, b) {
        (Ok(x), Ok(y)) => x.decision == y.decision,
        (Err(_), Err(_)) => true,
        _ => false,
    }
}

/// Hermes: "prompt/model/quant regression delta". Inherently a two-run
/// comparison — a single [`compute_metrics`] call has no baseline to diff
/// against, so this is a free function taking two already-computed
/// [`Metrics`] values rather than a field `compute_metrics` populates.
#[derive(Debug, Clone, Serialize)]
pub struct RegressionDelta {
    pub exact_semantic_accuracy_delta: f64,
    pub wrong_valid_rate_delta: f64,
    pub false_merge_rate_delta: Option<f64>,
    pub false_split_rate_delta: Option<f64>,
}

pub fn regression_delta(baseline: &Metrics, candidate: &Metrics) -> RegressionDelta {
    RegressionDelta {
        exact_semantic_accuracy_delta: candidate.exact_semantic_accuracy
            - baseline.exact_semantic_accuracy,
        wrong_valid_rate_delta: candidate.wrong_valid_rate - baseline.wrong_valid_rate,
        false_merge_rate_delta: match (baseline.false_merge_rate, candidate.false_merge_rate) {
            (Some(b), Some(c)) => Some(c - b),
            _ => None,
        },
        false_split_rate_delta: match (baseline.false_split_rate, candidate.false_split_rate) {
            (Some(b), Some(c)) => Some(c - b),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use deblob_core::id::FamilyId;
    use deblob_slm::{EndpointStatus, InferenceTelemetry};

    use crate::corpus::Partition;

    use super::*;

    fn schema_id(byte: u8) -> SchemaId {
        SchemaId::from_digest(&[byte; 32])
    }

    fn candidate(observation_count: u64) -> CandidateProfileView {
        CandidateProfileView {
            observation_count,
            fields: vec![],
            truncated: false,
        }
    }

    fn fc(byte: u8, rank: u32, distance: f32) -> FamilyCandidate {
        FamilyCandidate {
            family_id: FamilyId::new_v7(),
            schema_id: schema_id(byte),
            version: 1,
            distance,
            rank,
        }
    }

    fn base_telemetry() -> InferenceTelemetry {
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

    fn eval_case(
        name: &str,
        category: Category,
        retrieved: Vec<FamilyCandidate>,
        expected: Expected,
    ) -> EvalCase {
        EvalCase {
            name: name.to_string(),
            category,
            candidate: candidate(10),
            retrieved,
            expected,
            partition: Partition::Test,
        }
    }

    fn expected(
        decision: InferenceDecision,
        gold_schema_id: Option<SchemaId>,
        gold_rank: Option<u32>,
        false_merge_trap: bool,
        false_split_trap: bool,
    ) -> Expected {
        Expected {
            decision,
            gold_schema_id,
            gold_rank,
            false_merge_trap,
            false_split_trap,
        }
    }

    /// A programmable fake `SemanticInferencer`: returns the next
    /// scripted `InferenceOutcome` (or a fixed telemetry default with the
    /// scripted decision) on each `classify()` call, in call order. This
    /// lets every test drive `compute_metrics` against KNOWN (expected,
    /// actual) pairs without a real model.
    struct FakeInferencer {
        script: Vec<InferenceOutcome>,
        calls: AtomicUsize,
    }

    impl FakeInferencer {
        fn new(script: Vec<InferenceOutcome>) -> Self {
            Self {
                script,
                calls: AtomicUsize::new(0),
            }
        }

        fn simple(decisions: Vec<InferenceDecision>) -> Self {
            Self::new(
                decisions
                    .into_iter()
                    .map(|decision| InferenceOutcome {
                        decision,
                        telemetry: base_telemetry(),
                    })
                    .collect(),
            )
        }
    }

    #[async_trait]
    impl SemanticInferencer for FakeInferencer {
        async fn classify(
            &self,
            _req: InferenceRequest,
        ) -> Result<InferenceOutcome, InferenceError> {
            let idx = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.script[idx].clone())
        }
    }

    // -- 1. wrong-valid tracked apart from schema-valid ----------------------

    #[tokio::test]
    async fn wrong_valid_counted_apart_from_schema_valid() {
        let id1 = schema_id(1);
        let id2 = schema_id(2);

        // Case A: schema-valid (no parse/schema errors) but semantically
        // WRONG — the model named an allowed id, just the wrong one.
        let case_a = eval_case(
            "wrong_but_valid",
            Category::CompatibleDrift,
            vec![fc(1, 1, 0.05), fc(2, 2, 0.2)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::Exact,
                },
                Some(id1.clone()),
                Some(1),
                false,
                false,
            ),
        );
        // Case B: schema-valid AND correct.
        let case_b = eval_case(
            "correct",
            Category::KnownExact,
            vec![fc(1, 1, 0.0)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::Exact,
                },
                Some(id1.clone()),
                Some(1),
                false,
                false,
            ),
        );
        let corpus = vec![case_a, case_b];

        let fake = FakeInferencer::simple(vec![
            InferenceDecision::MatchSchema {
                schema_id: id2,
                relation: Relation::Exact,
            },
            InferenceDecision::MatchSchema {
                schema_id: id1,
                relation: Relation::Exact,
            },
        ]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(
            metrics.schema_valid_rate, 1.0,
            "both cases were schema-valid"
        );
        assert_eq!(
            metrics.wrong_valid_count, 1,
            "only the wrong-but-valid case counts toward wrong-valid"
        );
        assert_eq!(metrics.wrong_valid_rate, 0.5);
        assert_eq!(
            metrics.exact_semantic_accuracy, 0.5,
            "the correct case must not be double-counted into the wrong bucket"
        );
    }

    // -- 2. false-merge rate --------------------------------------------------

    #[tokio::test]
    async fn false_merge_rate() {
        let wrong_family = schema_id(1);
        let expected_decision = InferenceDecision::Abstain {
            cause: AbstainCause::InsufficientEvidence,
        };

        let merged_case = eval_case(
            "merge_trap_merged",
            Category::IncompatibleUnsafe,
            vec![fc(1, 1, 0.02), fc(2, 2, 0.5)],
            expected(expected_decision.clone(), None, None, true, false),
        );
        let correctly_declined_case = eval_case(
            "merge_trap_declined",
            Category::IncompatibleUnsafe,
            vec![fc(1, 1, 0.02), fc(2, 2, 0.5)],
            expected(expected_decision, None, None, true, false),
        );
        let corpus = vec![merged_case, correctly_declined_case];

        let fake = FakeInferencer::simple(vec![
            // Wrongly ACCEPTS a match to the plausible-but-wrong family.
            InferenceDecision::MatchSchema {
                schema_id: wrong_family.clone(),
                relation: Relation::CompatibleDrift,
            },
            // Correctly recognizes resemblance WITHOUT accepting it.
            InferenceDecision::MatchSchema {
                schema_id: wrong_family,
                relation: Relation::IncompatibleSimilarity,
            },
        ]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(metrics.false_merge_trap_count, 2);
        assert_eq!(
            metrics.false_merge_count, 1,
            "only the ACCEPTED wrong-family match counts as a false merge"
        );
        assert_eq!(metrics.false_merge_rate, Some(0.5));
    }

    // -- 3. false-split rate --------------------------------------------------

    #[tokio::test]
    async fn false_split_rate() {
        let id1 = schema_id(1);
        let case = eval_case(
            "split_trap",
            Category::CompatibleDrift,
            vec![fc(1, 1, 0.05)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1,
                    relation: Relation::CompatibleDrift,
                },
                None,
                None,
                false,
                true,
            ),
        );
        let corpus = vec![case];

        // The model incorrectly rejects the match it should have accepted.
        let fake = FakeInferencer::simple(vec![InferenceDecision::NewCandidate {
            novelty: deblob_slm::Novelty::Structural,
        }]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(metrics.false_split_trap_count, 1);
        assert_eq!(metrics.false_split_count, 1);
        assert_eq!(metrics.false_split_rate, Some(1.0));
    }

    // -- 4. abstention precision / recall --------------------------------------

    #[tokio::test]
    async fn abstention_precision_recall() {
        let id1 = schema_id(1);
        let match_expected = expected(
            InferenceDecision::MatchSchema {
                schema_id: id1.clone(),
                relation: Relation::Exact,
            },
            Some(id1.clone()),
            Some(1),
            false,
            false,
        );
        let abstain_expected = expected(
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            None,
            None,
            false,
            false,
        );

        let corpus = vec![
            // 1. should abstain, did abstain -> true positive.
            eval_case(
                "should_abstain_did",
                Category::AmbiguousAdversarial,
                vec![fc(1, 1, 0.3)],
                abstain_expected.clone(),
            ),
            // 2. should abstain, did NOT -> false negative.
            eval_case(
                "should_abstain_didnt",
                Category::AmbiguousAdversarial,
                vec![fc(1, 1, 0.3)],
                abstain_expected,
            ),
            // 3. should NOT abstain, but did -> false positive.
            eval_case(
                "shouldnt_abstain_did",
                Category::KnownExact,
                vec![fc(1, 1, 0.0)],
                match_expected.clone(),
            ),
            // 4. should NOT abstain, and didn't -> true negative.
            eval_case(
                "shouldnt_abstain_didnt",
                Category::KnownExact,
                vec![fc(1, 1, 0.0)],
                match_expected,
            ),
        ];

        let fake = FakeInferencer::simple(vec![
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            InferenceDecision::MatchSchema {
                schema_id: id1.clone(),
                relation: Relation::Exact,
            },
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            InferenceDecision::MatchSchema {
                schema_id: id1,
                relation: Relation::Exact,
            },
        ]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(
            metrics.abstention_precision,
            Some(0.5),
            "2 actual abstains, 1 correct"
        );
        assert_eq!(
            metrics.abstention_recall,
            Some(0.5),
            "2 should-abstain cases, 1 caught"
        );
    }

    // -- 5. recall@k and MRR -----------------------------------------------------

    #[tokio::test]
    async fn recall_at_k_and_mrr() {
        let id1 = schema_id(1);
        let absent_gold = schema_id(99);

        let rank1_case = eval_case(
            "gold_rank1",
            Category::KnownExact,
            vec![fc(1, 1, 0.0)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::Exact,
                },
                Some(id1.clone()),
                Some(1),
                false,
                false,
            ),
        );
        let rank2_case = eval_case(
            "gold_rank2",
            Category::CompatibleDrift,
            vec![fc(9, 1, 0.05), fc(1, 2, 0.2)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::CompatibleDrift,
                },
                Some(id1.clone()),
                Some(2),
                false,
                false,
            ),
        );
        let rank3_case = eval_case(
            "gold_rank3",
            Category::CompatibleDrift,
            vec![fc(9, 1, 0.02), fc(8, 2, 0.1), fc(1, 3, 0.3)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::CompatibleDrift,
                },
                Some(id1),
                Some(3),
                false,
                false,
            ),
        );
        let gold_absent_case = eval_case(
            "gold_absent",
            Category::AmbiguousAdversarial,
            vec![fc(9, 1, 0.4), fc(8, 2, 0.5)],
            expected(
                InferenceDecision::Abstain {
                    cause: AbstainCause::CandidateMissing,
                },
                Some(absent_gold),
                None,
                false,
                false,
            ),
        );
        let corpus = vec![rank1_case, rank2_case, rank3_case, gold_absent_case];

        // Actual decisions echo `expected` — recall@k/MRR depend only on
        // `expected.gold_rank`, not on what the model answered.
        let fake =
            FakeInferencer::simple(corpus.iter().map(|c| c.expected.decision.clone()).collect());

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(metrics.recall_at_1, Some(0.25));
        assert_eq!(metrics.recall_at_3, Some(0.75));
        assert_eq!(metrics.recall_at_5, Some(0.75));
        let mrr = metrics.mrr.expect("gold-bearing cases present");
        let expected_mrr = (1.0 + 0.5 + 1.0 / 3.0 + 0.0) / 4.0;
        assert!(
            (mrr - expected_mrr).abs() < 1e-9,
            "mrr {mrr} != expected {expected_mrr}"
        );
    }

    // -- 6. latency and repair from telemetry --------------------------------------

    #[tokio::test]
    async fn latency_and_repair_from_telemetry() {
        let id1 = schema_id(1);
        let case_template = |name: &str| {
            eval_case(
                name,
                Category::CompatibleDrift,
                vec![fc(1, 1, 0.05)],
                expected(
                    InferenceDecision::MatchSchema {
                        schema_id: id1.clone(),
                        relation: Relation::CompatibleDrift,
                    },
                    Some(id1.clone()),
                    Some(1),
                    false,
                    false,
                ),
            )
        };
        let corpus = vec![
            case_template("no_repair"),
            case_template("repaired_ok"),
            case_template("repaired_failed"),
        ];

        let outcome_no_repair = InferenceOutcome {
            decision: InferenceDecision::MatchSchema {
                schema_id: id1.clone(),
                relation: Relation::CompatibleDrift,
            },
            telemetry: InferenceTelemetry {
                total_latency_ms: Some(100),
                ttft_ms: Some(100),
                repair_count: 0,
                ..base_telemetry()
            },
        };
        let outcome_repaired_ok = InferenceOutcome {
            decision: InferenceDecision::MatchSchema {
                schema_id: id1,
                relation: Relation::CompatibleDrift,
            },
            telemetry: InferenceTelemetry {
                total_latency_ms: Some(200),
                ttft_ms: Some(200),
                repair_count: 1,
                ..base_telemetry()
            },
        };
        let outcome_repaired_failed = InferenceOutcome {
            decision: InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            telemetry: InferenceTelemetry {
                total_latency_ms: Some(300),
                ttft_ms: Some(300),
                repair_count: 1,
                parse_error: true,
                ..base_telemetry()
            },
        };

        let fake = FakeInferencer::new(vec![
            outcome_no_repair,
            outcome_repaired_ok,
            outcome_repaired_failed,
        ]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);

        assert_eq!(metrics.total_latency_p50_ms, Some(200));
        assert_eq!(metrics.total_latency_p95_ms, Some(300));
        assert_eq!(metrics.ttft_p50_ms, Some(200));
        assert_eq!(metrics.ttft_p95_ms, Some(300));

        assert!(
            (metrics.repair_rate - (2.0 / 3.0)).abs() < 1e-9,
            "2 of 3 cases were repaired"
        );
        assert_eq!(
            metrics.repair_success_rate,
            Some(0.5),
            "of 2 repaired cases, 1 ended up schema-valid"
        );
        // accepted matches: no_repair (0 repairs) + repaired_ok (1 repair) = 2
        // accepted; repaired_failed ended in Abstain, not accepted.
        // sum(repair_count) = 0 + 1 + 1 = 2; repairs_per_accepted = 2 / 2.
        assert_eq!(metrics.repairs_per_accepted, Some(1.0));
    }

    // -- 7. report surfaces wrong-valid and false-merge prominently ------------------

    #[tokio::test]
    async fn report_surfaces_wrong_valid_and_false_merge() {
        let id1 = schema_id(1);
        let id2 = schema_id(2);

        let wrong_valid_case = eval_case(
            "wrong_valid",
            Category::CompatibleDrift,
            vec![fc(1, 1, 0.05), fc(2, 2, 0.2)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1,
                    relation: Relation::Exact,
                },
                None,
                None,
                false,
                false,
            ),
        );
        let false_merge_case = eval_case(
            "false_merge",
            Category::IncompatibleUnsafe,
            vec![fc(2, 1, 0.02)],
            expected(
                InferenceDecision::Abstain {
                    cause: AbstainCause::InsufficientEvidence,
                },
                None,
                None,
                true,
                false,
            ),
        );
        let corpus = vec![wrong_valid_case, false_merge_case];

        let fake = FakeInferencer::simple(vec![
            InferenceDecision::MatchSchema {
                schema_id: id2.clone(),
                relation: Relation::Exact,
            },
            InferenceDecision::MatchSchema {
                schema_id: id2,
                relation: Relation::CompatibleDrift,
            },
        ]);

        let run = run_eval(&fake, &corpus).await;
        let metrics = compute_metrics(&run, &corpus);
        // Both cases are schema-valid-but-wrong (the false-merge case is
        // itself a false merge, which is by construction ALSO wrong-valid:
        // an accepted-but-wrong-family match is a subset of wrong-valid).
        assert_eq!(metrics.wrong_valid_rate, 1.0);
        assert_eq!(metrics.false_merge_rate, Some(1.0));

        let (human, json) = crate::report::report(&metrics);

        assert!(
            human.contains("Wrong-valid"),
            "human report must surface wrong-valid prominently:\n{human}"
        );
        assert!(
            human.contains("100.00%"),
            "human report must show the wrong-valid figure:\n{human}"
        );
        assert!(
            human.contains("False-merge"),
            "human report must surface false-merge prominently:\n{human}"
        );

        assert_eq!(json["wrong_valid_rate"], serde_json::json!(1.0));
        assert_eq!(json["false_merge_rate"], serde_json::json!(1.0));
    }

    // -- bonus: multi-run helpers ------------------------------------------------------

    #[tokio::test]
    async fn candidate_order_sensitivity_detects_a_flip() {
        let id1 = schema_id(1);
        let id2 = schema_id(2);
        let case = eval_case(
            "order_sensitive",
            Category::CompatibleDrift,
            vec![fc(1, 1, 0.05), fc(2, 2, 0.4)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::CompatibleDrift,
                },
                Some(id1.clone()),
                Some(1),
                false,
                false,
            ),
        );
        let corpus = vec![case];

        // A fake that always picks whichever candidate is rank 1 — order
        // DOES change its answer.
        struct RankOneFollower;
        #[async_trait]
        impl SemanticInferencer for RankOneFollower {
            async fn classify(
                &self,
                req: InferenceRequest,
            ) -> Result<InferenceOutcome, InferenceError> {
                let top = req.retrieved.iter().min_by_key(|c| c.rank).unwrap();
                Ok(InferenceOutcome {
                    decision: InferenceDecision::MatchSchema {
                        schema_id: top.schema_id.clone(),
                        relation: Relation::CompatibleDrift,
                    },
                    telemetry: base_telemetry(),
                })
            }
        }

        let sensitivity = measure_candidate_order_sensitivity(&RankOneFollower, &corpus).await;
        assert_eq!(sensitivity, Some(1.0));

        let _ = id2; // kept for readability of intent, unused otherwise.
    }

    #[tokio::test]
    async fn repeatability_across_runs_of_a_deterministic_fake() {
        let id1 = schema_id(1);
        let case = eval_case(
            "deterministic",
            Category::KnownExact,
            vec![fc(1, 1, 0.0)],
            expected(
                InferenceDecision::MatchSchema {
                    schema_id: id1.clone(),
                    relation: Relation::Exact,
                },
                Some(id1.clone()),
                Some(1),
                false,
                false,
            ),
        );
        let corpus = vec![case];

        struct AlwaysSame(SchemaId);
        #[async_trait]
        impl SemanticInferencer for AlwaysSame {
            async fn classify(
                &self,
                _req: InferenceRequest,
            ) -> Result<InferenceOutcome, InferenceError> {
                Ok(InferenceOutcome {
                    decision: InferenceDecision::MatchSchema {
                        schema_id: self.0.clone(),
                        relation: Relation::Exact,
                    },
                    telemetry: base_telemetry(),
                })
            }
        }

        let repeatability = measure_repeatability(&AlwaysSame(id1), &corpus, 3).await;
        assert_eq!(repeatability, Some(1.0));
    }
}
