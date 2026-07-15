# Shadow risk–coverage definitions and go-live gate (deblob-p2ab Task 5)

Authoritative source: `docs/superpowers/plans/deblob-p2ab-hermes-review.md`
§ "Task 5 — shadow log + go-live gate". This document restates it in one
place for operators and reviewers; the plan doc remains authoritative if
the two ever disagree.

**This document is DESCRIPTIVE, not automated.** Nothing in P2 enforces the
go-live gate below — no code path in `crates/deblob/src/shadow.rs` (or
anywhere else in this repository) currently checks these numbers or flips
any switch. The gate exists so a *human* reviewing the shadow log later
(P3) has a written, agreed-upon bar to check the data against before any
SLM decision is ever applied to live registry/candidate/schema state. The
shadow classifier's only job in P2 is to accumulate the labeled decisions
this gate will eventually be evaluated against.

## Where the data comes from

Every stable candidate cluster is shadow-classified at most once per
candidate-set digest (see `ShadowClassifier::maybe_classify`'s debounce).
Each classification appends one immutable `ShadowDecision` to
`deblob:shadow:<candidate_id>` (a bounded Redis stream, `MAXLEN ~ 1000`).
`human_label`, `correct_schema_id`, `correct_family_id`, `correct_relation`,
`labeler_id`, and `adjudication_version` are always `null` at write time —
they are populated by a separate, later, OFFLINE labeling process reading
the stream, never by the classifier itself.

## Risk–coverage definitions

- **`coverage`** = (accepted `match_schema` decisions) / (eligible shadow
  decisions). "Eligible" means a shadow classification actually ran
  (candidate was stable, not debounced) — abstentions and unavailable-
  endpoint records both count in the denominator, not the numerator.
- **`semantic_risk`** = (incorrect accepted decisions) / (accepted
  decisions). "Accepted" here means `PolicyOutcome::would_accept == true`
  (the policy grid's counterfactual acceptance, not a live action — P2 never
  takes a live action). "Incorrect" is determined against the offline
  `correct_*` label fields once populated.
- **`false_merge_risk`** = (accepted decisions naming the WRONG family) /
  (accepted decisions). The single most important number in this document
  — see "the hard gate" below.

The operating curve (which combination of rank/distance/margin/observation-
count thresholds to run live with) is built EXCLUSIVELY from the
deterministic gate variables in `PolicyGateInputs` — selected rank,
structural distance, top1/top2 margin, observation count, relation, source
class, and redaction-loss flags. It is never built from, or tuned against,
a model self-reported confidence score, because none exists anywhere in the
Task 1 contract (`InferenceDecision` carries only fixed enums).

## The initial policy grid (already implemented, evaluated in shadow only)

`crates/deblob/src/shadow.rs::evaluate_policy` implements exactly this grid
today — the ONLY thing P2 does with it is log the counterfactual
(`PolicyOutcome`, `LiveDisposition`), never apply it:

| Gate | Threshold |
|---|---|
| Retrieval rank of the selected schema | `== 1` |
| Structural distance (Task 3 weighted distance) | `<= 0.15` |
| Top1/top2 retrieval margin | `>= 0.10` |
| Candidate observation count | `>= 20` |
| Model-selected relation | `∈ {exact, compatible_drift}` |
| Deterministic compatibility check | passed |
| Redaction collision | none |

A decision passes IFF every gate passes; `PolicyOutcome.gate_reasons` lists
every gate that failed (not just the first), so shadow-log analysis can see
the full rejection profile per decision, not just a single reason code.

## Go-live gate (documented; enable ONE source/family slice first)

Before any SLM decision is ever applied live — and even then, only for one
source/family slice at a time, never a global cutover — every one of the
following must hold, computed over the shadow log's labeled decisions:

- **≥ 3000 accepted, labeled shadow decisions** (statistical power floor).
- **ZERO false merges — the hard gate.** A single wrong-family accepted
  decision blocks go-live outright, independent of every other metric
  below. **False merges corrupt identity** (an unrelated schema's data
  becomes indistinguishable from the family it was wrongly merged into,
  and that damage does not self-heal); **false splits only reduce
  coverage** (a legitimately-matching candidate stays a separate
  provisional cluster a little longer) **and are repairable** later. The
  asymmetry is the entire reason this gate exists.
- Accepted precision ≥ 99.5%.
- Compatible-drift precision ≥ 99.0%.
- Wrong-valid rate ≤ 0.5% (schema-valid output that names the wrong
  schema — tracked separately from parse/schema-validity, per the Task 7
  eval-metrics convention this shares its name with).
- Coverage ≥ 25%.
- No slice of ≥ 100 examples (per source or per family) below 99%
  precision — a slice can't hide behind a good aggregate number.
- Injection-induced decision changes: 0 / 500 (an adversarially-named
  field must never flip the decision).
- Temp-0 repeat agreement ≥ 99.9% (the same input, re-run, must produce the
  same decision almost always — instability here means the model/pipeline
  isn't ready, independent of accuracy).
- Endpoint latency/error SLOs pass (operational readiness, separate from
  decision quality).

## What this checkpoint does NOT do

- It does not compute any of the numbers above. That is Task 7's job
  (`crates/deblob-eval`) plus an offline labeling pipeline neither of which
  exist yet in this repository at this commit.
- It does not gate, block, or otherwise affect promotion, publication, or
  any hot/cold-lane behavior. `ShadowClassifier::maybe_classify` is called
  by nothing in `crates/deblob/src/serve.rs` or `main.rs` as of this
  commit — it is available to be wired into a periodic sweep or the
  discovery-consumer pipeline by a follow-up task, once an operator has
  configured a `SemanticInferencer` endpoint to shadow against. See
  `crates/deblob/src/shadow.rs` module docs for the exact zero-mutation
  invariant this checkpoint DOES prove.
