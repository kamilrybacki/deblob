# Deblob — How It Works, What It Does, How the Experiment Went

**Joint report** · Claude Code + Hermes (`jr-deblob-report`) · 2026-07-17
Attribution: `[C]` Claude, `[H]` Hermes, `[C+H]` independently corroborated.

## Executive summary

Deblob is a Rust **schema-tagging service for event streams**: it recognizes which schema an incoming event belongs to, deterministically and at speed, and discovers genuinely new schemas without ever wrongly merging two different ones. It is built as **two lanes** — a deterministic hot path that does the fast, certain work, and a small-language-model (SLM) discovery lane behind a **trust gate** that structurally prevents an unproven model decision from ever being applied. A continual-learning loop lets the SLM improve over time from human decisions, under strict governance (a bad model can't corrupt identity or auto-deploy).

The recent work fine-tuned that SLM lane on real hardware and — critically — proved the **real production training hook end-to-end**: a feedback snapshot went through the deployed Modal trainer, produced a governed adapter, and that exact adapter, loaded back, reproduced the evaluation numbers.

**Honest maturity `[C+H]`:** Deblob is an *advanced deterministic schema-tagging prototype with a credible, safety-oriented semantic research lane* — pre-alpha, single-maintainer, unreleased. Its core contribution is architectural: **model improvement raises coverage without transferring identity authority away from deterministic policy.**

---

## 1. How Deblob works (architecture) `[C+H]`

### The two lanes
- **Deterministic hot path** — every event is parsed (fuzz-hardened, bounded), canonicalized to a structural **fingerprint**, profiled by a **monoid** (property-tested), and matched against known schema families by **structural neighbor retrieval** (weighted-Jaccard). An **exactly-once Kafka transactional relay** (chaos-tested; throughput tuned 14 → 598 rec/s) moves tagged events; a **Redis vault** (atomic Lua publication, write-once immutability, persistence health-gate) holds the schema registry. This lane is fast, deterministic, and where **most correct detections come from** — measured retrieval recall@3 ≈ 92%.
- **SLM cold-discovery lane** — for the ambiguous middle (same-structure-different-meaning, drift, renamed fields), a small model proposes a 3-way decision (`match_schema{exact|compatible_drift|incompatible_similarity}` / `new_candidate` / `abstain`).

### The trust gate — the load-bearing safety property `[H]`
An SLM proposal is applied **only** if a deterministic gate corroborates it: `rank==1`, `distance ≤ 0.15`, `margin ≥ 0.10`, `observations ≥ 20`, deterministic-compat, no redaction collision — **no model confidence in the decision**. The gate *structurally prevents uncorroborated SLM outputs from applying*. The honest safety claim is: *"the gate prevents uncorroborated application and produced zero observed false-merges over a stated externally-labeled sample"* — with the sample size and an upper confidence bound, which the `model_registry` code already enforces (Wilson bounds, per-family floors, non-inferiority).

### The continual-learning control plane `[H]`
Stronger than a conventional "collect feedback + periodically fine-tune" loop: immutable, provenance-bearing feedback; family + near-duplicate partitioning; **reason-coded rejections** (a policy denial is not a model error); **stable-base** retraining (not recursive adapter merge); replay strata for retention; immutable **composite** model bundles; a **statistical offline gate**; **separation of duties** (candidate registration → evidence attachment → promotion are distinct); a **live-shadow hold**; and **full-bundle rollback**. The most valuable property is separation of duties — *the training hook can produce an artifact digest but cannot move the active alias*, containing training bugs, budget failures, and accidental regressions. The main residual risk is **label quality** (operator approval is strong evidence, not automatic ground truth).

### Homelab fit `[H]`
A natural split local/remote system: k3s workers run the deterministic Rust relay + discovery consumer + management API (+ small CPU inference); Redpanda/Kafka for durable transactional transport; Redis (persistent) as the vault + feedback + registry state; NAS/object storage for immutable corpora/manifests/adapters/rollback bundles; **Modal T4** for intermittent, scale-to-zero LoRA training instead of a local GPU; ArgoCD for digest-pinned deployment; Grafana/Prometheus for observability. The **model lane can fail closed without taking down the relay** — critical in a small cluster.

---

## 2. What Deblob does (functionality) `[C]`

| Capability | Status |
|---|---|
| Parse + fingerprint + monoid-profile events; structural neighbor retrieval | ✅ production-grade (tested) |
| Exactly-once Kafka relay (transactional, chaos-proven, 598 rec/s) | ✅ |
| Redis schema vault (atomic, immutable, health-gated, auto-reconnect) | ✅ |
| Management API + maintained schema index | ✅ |
| P2-D semantic fingerprint + weighted-Jaccard similarity | ✅ |
| SLM shadow lane + trusted-apply gate (exhaustively proven no-false-merge in tests) | ✅ (governance machinery) |
| Continual-learning loop: feedback → gated model registry (statistical gate, separation of duties, two-stage canary, rollback) | ✅ (implemented + real-Redis IT) |
| `deblob-experiment` crate: arms A0/A1/B0/B1/B2, four-layer eval, leak-guard external labels, real-corpus ingest (GitHub Archive + Wikimedia) | ✅ (offline harness) |
| Remote fine-tune hook (provider-neutral `TrainingJob`; Modal T4 default) | ✅ **wired + proven end-to-end (this report)** |

---

## 3. How the experiment went `[C]`

### Goal
Prove the SLM lane can be fine-tuned to *correctly infer new schemas with a good score*, and that the whole arm-C flow runs on real hardware.

### The fix cycle (Qwen2.5-0.5B, family-held-out; the intervention is cumulative)
| | new-schema | decision | match | schema-id | note |
|---|---|---|---|---|---|
| v1 baseline (full-seq loss) | **0%** | 46% | ~all | 6% | mode-collapsed to always-match; hallucinated ids |
| v2 + completion-only loss + balance | 50% | 46% | 27% | 19% | collapse fixed |
| v3 + finite hypothesis scoring | 37% | 55% | 54% | 29% | **zero hallucination** (only legal ids) |
| v3b + more data (720) | 33% | **78%** | 100% | 47% | best all-round |
| v4 + deterministic distance veto | **100%** | 61% | 58% | 24% | safe discovery point |
| v5 tuned veto (0.40) | 33% | 56% | 67% | 29% | confirms no clean threshold |

### The fixes (joint Claude×Hermes) `[C+H]`
1. **Completion-only loss** — mask the long prompt, loss only on the tool-call. The #1 lever; the prompt was drowning the decision signal.
2. **Balanced sampling** — `match_schema` was 64% of decisions → the model could emit new/abstain at all.
3. **Finite hypothesis scoring** `[H]` — score every *legal* completion by likelihood, pick argmax, no free generation → +accuracy, +schema-id, and **out-of-catalog identifiers become impossible** (an inference invariant).
4. **More data** → decision 55 → 78%.
5. **Deterministic distance veto** — force `new_candidate` when nothing sits within the gate distance → new-schema recall → 100%.

### The key finding `[C+H]`
**Structural distance cannot cleanly separate a genuinely new family from a drifted/renamed one** — both sit at ~0.29 distance. That is a *semantic* judgment (the SLM's job, improving with data), while the deterministic layer safely handles the far-and-obvious. This negative result is more valuable than a headline number: the deterministic lane is **necessary but insufficient**, and the SLM has a **non-redundant** role in the middle region. Two honest operating points result: **v4** (100% new-recall, conservative/safe) and **v3b** (78% all-round) — neither is universally superior without an explicit error-cost model.

### The real-hook proof (this session) `[C]`
The disclosed gap ("training ran standalone") is now closed for the training half. A feedback snapshot was exported to the base-model Volume with a content-addressed manifest; the **deployed production trainer** (now running the v4 recipe: completion-only loss + balanced sampling) resolved the manifest, trained on real data (balanced 432/432/432; 421 completion-only examples; final_loss 0.026), and returned **digests only** (separation of duties held). The resulting adapter, loaded back from the Volume and run through the finite-scoring + veto decision path, **reproduced the v4 numbers exactly** (new-schema 100%, decision 60.8%, match 58.3%, id 23.6%). The real `feedback → Modal → adapter+manifest → digests → served decisions` path works.

---

## 4. Assessment — solid vs. overclaimed `[H]`

**Established solidly:**
- The v1 diagnosis was correct — full-sequence prompt loss + class imbalance (not parameter count) caused the collapse.
- Finite hypothesis scoring is a genuine system improvement, isolated cleanly (same weights, same inputs).
- Family-held-out evaluation + evaluator-only-sidecar leak-guard is rigorous — it addresses the earlier tautological-evaluation problem.
- The negative distance result is honest and important.

**Currently overclaimed (corrected here):**
- **"External gold"** should read *"evaluator-side gold, independent of the gate, on a family-partitioned **synthetic** (construction-truth) corpus."* Not yet real-world/production-feedback validation.
- **The held-out set is now a development/calibration set** — it was inspected across v1→v5 to choose the recipe, thresholds, and operating points. Its v5 numbers are optimistically selected. **A fresh sealed audit set is required.**
- **Accuracy figures lack uncertainty** — single seed, single trajectory. Need multi-seed, confidence intervals, macro-F1, confusion matrices, recall@k / oracle-retrieval, paired comparisons.
- **A deterministic veto's gain is not an SLM gain** — report raw-SLM / deterministic-override / trust-gated separately, or a conservative rule flatters the headline while hiding false splits.
- **Zero observed false-merges ≠ production safety** — go-live wants thousands of real labeled shadow decisions + per-slice floors + a stated statistical bound.

**Methodological verdict:** *rigorous for an early controlled engineering experiment, not yet a confirmatory benchmark.* Still exploratory.

**Maturity:** deterministic core = advanced pre-alpha (real Kafka/Redis, chaos coverage, measured relay optimization, audited promotion); trust + model governance = strong implementation prototype ("unusually mature design work for the project's age"); SLM lane = validated research prototype.

**Documentation drift (fix before any release) `[H]`:** the README still describes the SLM lane as unsupported; older go-live docs call the gate descriptive-only; newer code implements trust-application + governed registry transitions. Reconcile README/runbooks/state diagrams with shipped behavior.

---

## 5. What's genuinely next `[H]`
1. Freeze v3b + v4; no more tuning against the current held-out set.
2. Build a **sealed audit corpus** (new families, near-dup clustering, independent seed, no threshold inspection).
3. **Multi-seed** runs with mean/spread/paired CIs.
4. Complete **A1/B1/B2** — the decisive question: does the SLM add safe coverage over a strong deterministic policy at the same risk bound? If B1≈B2, the SLM isn't yet operationally justified.
5. Separate **retrieval from semantic accuracy** (recall@k, oracle-candidate SLM accuracy, end-to-end, trust-gated coverage).
6. Add **genuine external data** (source-native GitHub Archive/Wikimedia labels + adjudicated homelab feedback).
7. Calibrate operating points on dev data → **risk-coverage curves**, not one universal threshold.
8. Exercise a **complete real Arm-C trajectory** to promotion + forced rollback (training half now done; gate → shadow-hold → promote → rollback controller steps remain).
9. **Failure drills** (Modal timeout, malformed artifact, digest mismatch, endpoint outage, Redis restart, stale index, failed shadow hold, concurrent promote/rollback).
10. Validate the feedback loop against **poisoning + forgetting** (reason-coded corrections, source caps, frozen replay slices, per-family non-regression).
11. **Reconcile documentation** with shipped behavior.

---

## 6. Confidence & gaps `[H]`

**High confidence:** the deterministic/semantic authority split is correct; SLM off the hot path fits reliability + the homelab envelope; completion-only + balancing fixed a real mechanism; finite legal-completion scoring beats free generation here; candidate-constrained selection eliminates out-of-catalog id hallucination; distance alone can't represent semantic family identity; separation of training/evidence/promotion is a strong governance boundary.

**Medium confidence:** the v1→v5 gains are genuine transferable improvement (family partitioning supports it, but repeated test-set use + no multi-seed intervals weaken the estimate); v4's conservative policy keeps false-merge risk low (directionally safe, production rate not established); the loop will raise safe coverage over time (mechanism plausible + governed, no real multi-round trajectory yet).

**Low confidence / unresolved:** any unconditional zero-false-merge guarantee over real-world semantic identity; generalization to uncontrolled production streams; whether the SLM adds incremental utility over the strongest deterministic baseline; calibration stability after retriever/catalog/prompt/quantization/runtime changes; long-term throughput + recovery at scale; feedback consistency + poison-resistance for autonomous retraining; production readiness (pre-alpha, single-maintainer, unreleased, doc/impl mismatches).

---

## Method note
Claude (this session) covered the code/experiment specifics, ran the real-hook live round + robust test, and synthesized. Hermes independently cloned the repository and reviewed the specs, gate docs, and trainer scripts to produce the architecture assessment, maturity read, overclaim corrections, and next-step plan folded in above. The two halves were complementary; where both reached the same conclusion it is marked `[C+H]`.
