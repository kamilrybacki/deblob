# SLM Continual-Learning / Reinforcement Loop — Design Spec

- **Date:** 2026-07-16
- **Status:** Draft
- **Motivation:** The trusted-apply gate makes a mediocre SLM *safe* (zero false-merge, but low coverage — it defers most cases to humans). Every human decision (promote, accept/reject a proposal, annotate) is GROUND TRUTH. Capturing it as training data and periodically re-fine-tuning the SLM makes it more accurate over time → coverage grows → fewer human hand-offs. Human *rejections* of confident model proposals are the highest-value signal (the reinforcement correction). This builds that loop.
- **Scope:** Deblob owns capture, a durable feedback store, the combined-corpus export, the retrain ORCHESTRATION, and GATED model promotion (governed model versions). The gradient step (actual fine-tune) + model serving are EXTERNAL (a documented hook — e.g. Needle/FunctionGemma). No auto-deploy of an un-gated or worse model. NO product-crate behavioral change (additive).

## 1. Feedback capture — every human decision becomes a labeled example

`TrainingExample { candidate: CandidateProfileView, retrieved: Vec<FamilyCandidate>, gold: InferenceDecision, label_source: LabelSource, weight: f32, partition_key: FamilyId, recorded_at }` — the SAME shape as the eval/corpus `EvalCase` (so feedback + synthetic combine into one training set).

`LabelSource` (drives weight + the reinforcement signal):
- `HumanPromote` — an operator promoted a candidate (→ the gold match/new decision). Positive.
- `TrustedProposalAccepted` — the model proposed a match, the gate proposed-to-human, a human APPROVED. Positive (confirms the model).
- `TrustedProposalRejected` — the model proposed a match, a human REJECTED it. **Hard-negative** (the gold is "this is NOT that family" — new_candidate or a different family). Highest weight — it directly targets a failure mode.
- `SemanticAnnotation` — a P2-D annotation (semantic ground truth).
- `Adjudication` — an offline human label on a shadow-log record.

`FeedbackCapture` hooks the governed decision paths (`TrustedApplier` outcomes, `Promoter::promote`, the annotation API) and emits a `TrainingExample`. It reads only already-redacted/derived data — NEVER raw values (reuse the PII-safe prompt-builder for any rendered form).

## 2. Durable feedback store

`FeedbackStore` trait (`append`, `export_jsonl`, `iter_by_partition`). Redis-stream impl (`deblob:slm-feedback` stream, append-only, trimmed/retention-bound like the evidence store). Export to the fine-tune JSONL format the corpus generator already emits (`{prompt, gold_tool_call}`), **partitioned by family** (Hermes' rule — all examples of a family in one partition; a fine-tune holdout never contains a train family's siblings). Records are immutable.

## 3. Retrain-and-gate orchestrator (Deblob owns everything but the gradient step)

`RetrainPlan`:
1. Collect `FeedbackStore` examples + the synthetic corpus (`deblob-eval generate`) → one combined, family-partitioned training set (train + held-out gate set).
2. Export the training JSONL.
3. **(External hook)** invoke the fine-tuner (Needle `finetune` / HF) → a candidate model artifact + digest. Deblob does NOT train; it calls the configured hook + records the artifact.
4. **Evaluate** the candidate model against the HELD-OUT gate corpus via `deblob-eval` (the model never saw these families) → gate metrics.
5. **Gated promotion** (§4).

## 4. Governed model registry + gated promotion

`ModelVersion { model_id, digest, trained_from (feedback cursor + corpus seed), eval_metrics, recorded_at, state }`; `ModelRegistry` (immutable, audited, in the vault). A candidate is promoted to `active` ONLY IF:
- it PASSES the go-live gate (docs/shadow-golive-gate.md: **zero false-merge (hard)**, wrong-valid ≤ threshold, accepted precision ≥ threshold, no slice regression), AND
- it does NOT regress vs the current `active` model on the held-out gate set.
Otherwise it stays `candidate` / `rejected` (audited, with the failing metrics). Promotion is atomic + audited (actor `retrain:v1`); the previous active is retained for **rollback**. Never auto-deploy a worse or un-gated model — the same evidence discipline as schema promotion, applied to model versions.

## 5. Coverage growth (the payoff)

As the active model's held-out metrics improve on a slice (family/source), the trust policy's thresholds MAY relax on that slice (more auto-applies where the model has proven itself) — always still requiring deterministic corroboration + the zero-false-merge invariant. This is where accuracy-over-time turns into coverage-over-time. (Threshold relaxation is a documented governed operation, not automatic.)

## 6. Non-goals

- No actual gradient training in Deblob (external hook).
- No model serving/inference change (the HttpInferencer just points at whatever endpoint serves the active model).
- No weakening of the trusted-apply no-false-merge invariant — a better model raises COVERAGE, never bypasses the guards.
- No online/streaming training — periodic batch retrain (scheduled), gated each time.

## 7. Acceptance

- `FeedbackCapture` turns a `TrustedProposalRejected` into a hard-negative `TrainingExample` (high weight, gold = not-that-family) and a `HumanPromote` into a positive; a unit test proves the label mapping.
- `FeedbackStore` (real Redis) appends + exports family-partitioned JSONL with NO raw values.
- `ModelRegistry`: a candidate that FAILS the gate (e.g. any false-merge, or worse than active) is NOT promoted; one that passes + improves IS; rollback restores the prior active — all audited, proven vs real Redis.
- The loop is orchestratable end-to-end with the fine-tune step stubbed (a fake hook returning a model artifact), proving the data→eval→gate→promote pipeline without real training.
