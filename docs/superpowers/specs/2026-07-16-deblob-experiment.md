# Deblob Comparative Experiment — Design Spec

- **Date:** 2026-07-16
- **Status:** Draft
- **Motivation:** Prove (or refute) that the SLM intelligence lane buys more *safe automation* than the deterministic lane alone, at the same zero-false-merge bound — and that continual learning moves that number, per model. An earlier risk-coverage result showed `precision=1.000`, which a reviewer flagged as TAUTOLOGICAL: the "gold" was derived from the same predicates the trust gate uses, so the number only proved the gate re-evaluates its own rule consistently. This experiment fixes that with ground truth EXTERNAL to the gate, a four-layer metric decomposition, and the critical `B2` redundancy ablation.
- **Scope:** a new `deblob-experiment` crate (harness + runner + reporter) + a corpus-ingestion path for real public event streams + inference adapters for the model roster. Reuses `deblob-eval` types (`EvalCase`, `Metrics`), the deterministic core (fingerprint/monoid/retrieval), the trust gate (`shadow::evaluate_policy`, `trusted`), and the continual-learning loop (`feedback`, `retrain`, `model_registry`). NO change to product-crate decision logic. Worker-pinned deploy; `lw-main` untouched. Arm C's remote fine-tune path is a pluggable hook (decided separately).
- **Joint design:** brainstormed Claude×Hermes (`jr-slm-bench-162009`). Hermes' review (4-layer eval, `B2` ablation, external labels, GitHub-Archive/Wikimedia corpora, prequential test-then-train + sealed audit, fair deterministic baseline) is authoritative and folded in below.

## 1. The arms (and ablations)

The primary arms answer the user's question; the ablations (Hermes) isolate which component supplies *safety* vs *coverage*.

| Id | Decider | Purpose |
|---|---|---|
| **A0** | retrieval only (top-1 by structural distance) | retrieval-capability floor |
| **A1** | **fair deterministic policy** — tuned thresholds on dev data, calibrated abstain, same redacted value-shape features, same hard trust constraints | the STRONG baseline the SLM must beat |
| **B0** | raw SLM output, NO trust gate (diagnostic only, never deployed) | measures raw model capability |
| **B1** | deterministic retrieval + SLM + full trust gate | the real SLM lane |
| **B2** | deterministic top-1 substituted for the SLM + the SAME trust gate | **the critical redundancy ablation** — if B1≈B2 the SLM is operationally redundant |
| **C1..Cn** | B1's model retrained over prequential feedback rounds | continual-learning trajectory |

Per-predicate gate ablations (no-rank / no-distance / no-margin / no-obs-floor / no-corroboration / no-SLM) are run for ANALYSIS only — never deployed — to attribute safety to each guard.

Arms A/B run per model in the roster (§5). The trust gate is FROZEN across the entire learning comparison (only the model changes).

## 2. Ground truth EXTERNAL to the gate (the anti-tautology core)

Gold labels come from sources independent of any gate predicate:
1. source-native event type / schema identity (GitHub `type`, Wikimedia `$schema`/`meta.stream`);
2. curated family manifests;
3. known schema-evolution lineage (version N → N+1 = same family);
4. deliberately constructed same-structure/different-semantics counterexamples (from the synthetic generator);
5. human adjudication for genuinely ambiguous pairs.

**Stripped from every inference input** (leak guard): event `type`, topic/stream name, `$schema`/`schema_uri`, source fixture path, family identifier, and any label embedded in tool/function names or descriptions. The gate MAY use rank/distance/margin/obs-count/deterministic-corroboration; NONE of those may define the gold. Source-native labels live in an **evaluator-only sidecar** (`GoldSidecar`), never in the record the model or gate sees. A unit test asserts no stripped field reaches the prompt builder.

## 3. Four-layer metrics (Hermes)

Each layer measured + reported separately so the gate cannot hide model errors behind its own filtering.

**Layer 1 — deterministic retrieval capability**
- Recall@1, Recall@k of the true family; MRR; distance/margin distributions for correct vs incorrect top-1; candidate-set miss rate; broken out by family + observation count.
- Separates "gold absent from top-k" (retrieval fault) from model-decision fault. Reported as its own **independent gate**.

**Layer 2 — raw SLM capability, before the gate**
- 3-way macro-F1; exact-family accuracy; argument/value-shape accuracy; abstention precision+recall; JSON/schema parse rate; **wrong-valid rate**; Brier score; **expected calibration error**; externally-labeled risk-coverage curve.

**Layer 3 — trust-gate containment**
- fraction of raw SLM errors BLOCKED; fraction of CORRECT SLM decisions blocked (over-blocking cost); accepted coverage; externally-measured accepted risk; **false-merge count with N + upper confidence bound** (rule-of-three for zero-event); guard-activation reasons; added latency + review cost.

**Layer 4 — incremental system utility** (the primary comparison)
- contingency vs A1 at the same external risk bound: `B correct / A wrong`, `B correct / A abstained`, `A correct / B wrong-or-abstained`, both correct, both abstained.
- human-review-queue reduction; extra CPU latency per uniquely-resolved event.
- significance via **McNemar / paired bootstrap CIs** (A and B see identical events).

## 4. The headline result

**Externally-measured risk vs useful coverage, 95% CIs, + latency + review load.** A persuasive outcome looks like: deterministic-only 60% coverage at the risk bound → det+SLM 72% → retrained SLM 81%, no per-family regression, zero system false-merges over stated N with an upper confidence bound, review volume down correspondingly, added p95 latency within budget. **If the frozen gate makes B1's final actions identical to A1** (B1≈B2), the honest report says the SLM cannot claim safe-coverage lift, and its residual value (earlier veto, review prioritization, feedback mining, disagreement detection) is measured with separate operational metrics — raw model accuracy alone does not justify the complexity.

## 5. Model roster + inference adapters

A `SemanticInferencer` port already exists; add thin backend adapters (record runtime as part of the composite bundle):
- **Ollama adapter** — Granite 3.1-MoE 1B, Qwen2.5 1.5B-Instruct (OpenAI-compat ✓).
- **llama.cpp adapter** — FunctionGemma 270M (GGUF), optionally Qwen.
- **Cactus adapter** — Needle 26M via `cactus serve` (OpenAI-compat).
- **Needle JAX adapter** — diagnostic only.

All share ONE tool/response contract (system/task, candidate family defs, redacted event features, required 3-way output) so model capability is isolated from API/template differences.

## 6. Corpora

**6a. Synthetic** — `deblob-eval generate` (built): construction-truth, family-partitioned, all trap classes. Scalable, deterministic, no external deps.

**6b. Real public streams** (ingestion → `EvalCase` + `GoldSidecar`):
- **GitHub Archive** (gharchive.org) — Push/PullRequest/Issues/IssueComment/Release/Fork/Watch/Create/Delete; real envelopes + family payloads + multi-year drift. Strip `type` + label-revealing URLs; split chronologically + by repo/org (no near-dup leakage).
- **Wikimedia EventStreams** — versioned JSON schemas (page create/delete, revision-create, recentchange); real schema registry + explicit versions = natural compatible-version-vs-new-family. Strip `$schema`/`meta.stream`/topic.
- **Secondary (OOD audit only):** DEBS, GDELT, NYC TLC, Debezium CDC envelopes, USGS GeoJSON / GBFS.

**Three evaluation tiers:** (1) in-domain temporal (old→new same source); (2) cross-source (unseen repos/streams); (3) cross-corpus OOD (train GitHub/Wikimedia, audit DEBS/TLC/GDELT). Difficult-pair categories enumerated (same-family-version-change, same-structure-different-semantics, different-structure-same-family, optional-field-add, rename, type-widen/narrow, value-shape-drift, envelope-same-payload-different, insufficient-obs, gold-absent-from-topk).

## 7. Continual-learning evaluation (arm C) — prequential test-then-train

```
Round r:
  1. Evaluate model-r on the NEXT chronological batch BEFORE revealing labels.
  2. Record predictions permanently.
  3. Reveal feedback for that batch → feedback store.
  4. Train model-(r+1) from the STABLE BASE using cumulative/replay data (not recursively merged weights).
  5. Evaluate retention on FROZEN historical slices.
```
Three datasets: **round stream** (chronological test-then-train), **development set** (hyperparams/replay-ratios/promotion thresholds), **sealed final audit set** (unseen until every model/round/analysis choice frozen). Controls: identical event order + feedback budget per model; no per-model retune on the audit set; identical replay strata; multiple seeds; cumulative-retrain control; random-sampling vs hard-case-mining arm; near-dup clustering before splits; frozen gate. Report **adaptation gain** (future-slice performance) AND **retention loss** (frozen-slice regression) — improving recent rejects while forgetting established families is NOT improvement. Final audit: `C_final` vs `B_v0` with paired CIs; never retrospectively pick the best round from the sealed trajectory.

Arm C's fine-tune step is the external hook (`FineTuneHook`, already stubbed + gated by `model_registry`). The REMOTE fine-tune backend (managed API vs spot GPU vs HF Jobs) is pluggable and decided separately; the harness treats it as "submit job → receive gated quantized adapter."

## 8. Harness shape

New crate `deblob-experiment`:
- **corpus ingest** (`corpus/`) — GitHub-Archive + Wikimedia loaders → `EvalCase` + `GoldSidecar`, with the leak-strip guard.
- **arms** (`arms/`) — A0/A1/B0/B1/B2 + Cn deciders over a shared trait.
- **adapters** (`inference/`) — Ollama/llama.cpp/Cactus behind `SemanticInferencer`.
- **metrics** (`metrics/`) — the 4 layers + McNemar/paired-bootstrap + rule-of-three CI + risk-coverage curve.
- **runner** (`run.rs`) — loads corpus → runs arms → emits per-arm tables + the headline risk-coverage plot data + cross-model comparison + arm-C trajectory. Deterministic seed.
- **reporter** — markdown + machine-readable JSON (for later charting).

Deploy: model endpoints + experiment Job pinned to workers c1/c2 (control-plane `DoesNotExist` affinity), reusing the `deploy/bench` pattern. Ephemeral, seeded, reproducible.

## 9. Non-goals
- No product-crate decision-logic change.
- No new schema relations / contract change (uses the existing 3-way contract).
- No real GPU training inside the cluster (remote hook; workers are CPU-only).
- No weakening of the no-false-merge invariant.
- Not a load test — throughput/latency are reported, but correctness + coverage are the point (the relay benchmark already covered throughput).

## 10. Acceptance
- A leak-guard test: no stripped label field (`type`/`$schema`/topic/family-id/fixture-path) reaches the prompt.
- Arms A0/A1/B0/B1/B2 run over the synthetic corpus and emit all four metric layers; the `B2` ablation is reported alongside `B1`.
- Real-corpus ingestion produces `EvalCase`+`GoldSidecar` from a GitHub-Archive sample + a Wikimedia sample, chronologically split, with source-native labels only in the sidecar.
- The runner emits the headline risk-vs-coverage table (per arm, per model) with rule-of-three false-merge upper bounds and McNemar/paired-bootstrap significance for B1-vs-A1.
- Arm-C prequential loop runs end-to-end with the stubbed fine-tune hook, reporting adaptation-gain + retention-loss per round and a `C_final`-vs-`B_v0` paired-CI comparison; the sealed audit set is untouched until the trajectory is frozen.
- Everything worker-pinned; `lw-main` untouched; deterministic by seed.
