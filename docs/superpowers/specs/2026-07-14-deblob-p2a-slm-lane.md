# Deblob P2-A/B — SLM Lane + Eval Harness Design Specification

- **Date:** 2026-07-14
- **Status:** Draft (extends P1 spec §7; P1 deterministic core merged to `main`)
- **Parent spec:** `docs/superpowers/specs/2026-07-14-deblob-design.md`
- **Scope:** Sub-projects **A** (SLM lane — `HttpInferencer` default + shadow mode) and **B** (eval harness + golden corpus). Sub-projects C (HTTP push proxy) and D (semantic fingerprint) are separate.
- **Research corpus:** Obsidian `research/` — On-the-Fly-Data-Inference-with-SLMs, Prominent-Edge-On-Device-SLM-Landscape-2026, Google-FunctionGemma-270M, Pydantic-V2-in-Deploying-SLMs-Joint-Research-2026.

## 1. Summary

Add the semantic discovery lane the P1 core was built around but deliberately deferred. When the cold lane surfaces a novel structural cluster, a **very small language model proposes** a classification — match an existing family, propose a new one, or abstain — over a *retrieved* top-k candidate set, using only redacted statistics as input. A **policy layer decides**; the model never does. In P2 the entire lane runs in **shadow mode**: it computes and logs what it *would* decide, applies nothing, and is measured against the deterministic baseline by an offline **eval harness** that reports wrong-valid rate as a first-class metric.

Core invariant (unchanged from P1): *deterministic code establishes facts, a task-specialized SLM proposes semantic annotations, constrained decoding guarantees output shape, policy code decides.* P2 adds the SLM step behind a provider-agnostic port, in shadow, with measurement.

## 2. Non-goals (P2-A/B)

- No live application of SLM decisions (shadow only; going live is a later gate contingent on eval results).
- No in-process llama.cpp (that is the optional `local-llama` feature, built after the HTTP path proves out).
- No embeddings retrieval (structural-distance retrieval over the existing bucketed index only).
- No semantic fingerprint, no map-vs-record generalization, no HTTP push proxy (separate sub-projects).
- No model fine-tuning pipeline (the harness *measures* a given model/endpoint; tuning is external).

## 3. Architecture

### 3.1 Where it plugs in

The P1 cold lane (`ColdLane::ingest` → clustering → candidates) is unchanged. P2 adds a **shadow classifier** invoked once per *stable* candidate cluster (debounced, sample/time-stable — never per record), strictly after deterministic retrieval:

```
stable candidate cluster (from P1 cold lane)
  → deterministic retrieval: top-k (3–10) nearest known families
       (structural distance over the bucketed index / monoid stats — no embeddings)
  → build PII-safe prompt (monoid statistics + redacted field metadata only)
  → SemanticInferencer.classify(candidate_profile, retrieved) [HttpInferencer, default]
       → 3-way structured output: match_schema(id, relation) | new_candidate(reason) | abstain(reason)
  → policy evaluation (retrieval margin, producer consistency, sample coverage,
       deterministic compatibility, model agreement — NEVER model self-confidence)
  → SHADOW: append a ShadowDecision to the decision log; APPLY NOTHING
  → (eval harness, offline) scores decisions vs a golden corpus + the deterministic baseline
```

### 3.2 The `SemanticInferencer` port (already in `deblob-core`)

P1 defined the trait as a vendor-free port. P2 implements it. Signature (confirm/adjust against the P1 trait when building):

```rust
#[async_trait]
pub trait SemanticInferencer: Send + Sync {
    async fn classify(&self, req: InferenceRequest) -> Result<InferenceDecision, InferenceError>;
}
```

- `InferenceRequest { candidate: CandidateProfileView, retrieved: Vec<FamilyCandidate>, contract_version, budget: InferenceBudget }` — carries ONLY redacted stats + the top-k family summaries + the allowed id set.
- `InferenceDecision` (the 3-way contract):
  - `MatchSchema { family_id, relation: Relation, evidence }` where `Relation ∈ { Exact, CompatibleDrift, IncompatibleSimilarity }`
  - `NewCandidate { reason: NewReason }` (enum, bounded)
  - `Abstain { reason: AbstainReason }` (enum, bounded — NOT free prose)
  - The `family_id` MUST be one of the `retrieved` ids (constrained; a decision naming an unlisted id is rejected as malformed → one repair → abstain).
- `InferenceError` — transport/timeout/parse; maps to a shadow "unavailable" outcome, never crashes the cold lane.

### 3.3 `HttpInferencer` (default runtime)

OpenAI-compatible HTTP client (`/v1/chat/completions` with structured output / tool-calling; fall back to `/v1/completions` + a supplied grammar if the endpoint supports GBNF). Config (`[slm]` section, secrets env-only):

- `runtime = "http"` (default; `"local"` is the future feature-gated llama.cpp path)
- `base_url`, `model`, `DEBLOB_SLM_API_TOKEN` (env-only), `timeout_ms` (bounded, e.g. 8000), `max_concurrency`, `max_prompt_tokens`.
- Process isolation is free: a crashing/hanging inference server is caught by the timeout and surfaces `InferenceError` — the relay and cold lane are unaffected. A dedicated worker with a bounded channel enforces backpressure and the deadline.

### 3.4 Retrieval (deterministic, before the model)

Reuse the P1 bucketed structural index. For a candidate cluster: compute its `ShapeSummary` / bucket, gather families in the same and neighboring buckets, rank by a structural distance over monoid stats (field-set overlap, type agreement, required-key overlap, depth). Return the top-k (default 5, max 10). This is the *only* information about existing schemas the model sees — never the full registry (prefill economics: never attach the whole catalog per request; retrieve few, cache the prompt prefix + any compiled grammar by candidate-set digest).

### 3.5 Prompt construction (PII-safe)

- Input to the model: monoid **statistics** (field presence/null counts, type unions, numeric ranges as buckets, array emptiness/partial flags) + **redacted field metadata** (field *names*, length-capped and escaped as data, with instruction-like / control-token sequences detected and flagged) + the top-k family summaries + the allowed id list.
- **Never** raw payload values. The deterministic redaction gate (P1) runs before anything reaches the prompt; fail closed.
- Field names are a prompt-injection surface: cap length, escape, never concatenate into instruction text, detect suspicious sequences.

### 3.6 Structured output / constraint discipline (from research)

- The 3-way contract is a small, fixed output schema: ≤2 nesting levels, a handful of fields, small enums, explicit `abstain`. "Reason free, constrain late" — do not over-constrain such that the model produces schema-valid-but-wrong output (the *constraint tax*: hard-constraining a sub-3B model can raise schema validity to 100% while *lowering* answer accuracy). Prefer the endpoint's native tool-calling/structured-output; if using GBNF, keep the grammar the fixed contract, keyed by contract-version + model digest + engine version — **never compile candidate schemas into grammars**.
- Validate the returned decision with a Pydantic-equivalent (Rust: serde + explicit validators): reject extra fields, enforce the enum/id allow-list, one mechanical repair max, else abstain.

### 3.7 Policy (shadow evaluation)

Computes what it *would* decide, from: retrieval margin (gap between top-1 and top-2 structural distance), producer consistency, sample coverage (count + time span + partition/producer spread), deterministic compatibility test result, and **model agreement** across a small ensemble or repeated call — **never the model's self-reported confidence**. In shadow mode the policy result is recorded, not applied. Promotion remains the P1 authenticated/audited manual boundary; the SLM never promotes.

### 3.8 Shadow decision log

Every shadow invocation appends a `ShadowDecision` to a durable log (Redis stream `deblob:shadow:<candidate>` or a dedicated topic — reuse the evidence-store patterns; bounded/trimmed): candidate-set digest, the retrieved ids + distances, the deterministic baseline verdict, the model's raw decision + which contract-version/model-digest produced it, the policy result, and (later, offline) the reviewer/golden verdict. This log is the eval harness's live-data input and the audit trail for the eventual go-live decision.

## 4. Eval harness + golden corpus (sub-project B)

An **offline** harness (a `deblob-eval` binary/crate) that runs a configured `SemanticInferencer` endpoint against a curated corpus and emits a report. It does not touch the running service.

### 4.1 Golden corpus

Curated cases, each = (candidate profile + retrieved top-k + expected decision):
- exact match (a variant of a known family → MatchSchema/Exact)
- compatible drift (known family + safe optional field → MatchSchema/CompatibleDrift)
- false-merge traps (structurally similar but semantically unrelated → must NOT match; expect NewCandidate or IncompatibleSimilarity)
- false-split traps (same family, superficial variation → must match, not split)
- optional-field variants, dynamic-map shapes
- insufficient evidence (→ Abstain)
- prompt-injection payloads (field names / values carrying instruction-like text → must not derail the contract; expect the intended decision or Abstain, never an out-of-contract output)
- schema-bomb / adversarial profiles (deeply nested, huge field sets → bounded handling, no crash)

### 4.2 Metrics (reported SEPARATELY — the research is emphatic)

- JSON parse rate
- schema-valid rate (output conforms to the 3-way contract)
- **exact semantic accuracy** (right family/relation, not just valid)
- tool/decision-choice accuracy
- **wrong-valid rate** (schema-valid but semantically wrong) — the dominant risk; must be tracked apart from schema-valid rate
- abstention precision & recall (did it abstain when it should, and only then)
- id-constraint violations (named an unlisted id)
- quantized-vs-reference disagreement (if two endpoints/models configured)
- prompt-injection resistance (fraction of injection cases handled without out-of-contract output)
- p50/p95 TTFT and total latency; peak prompt tokens
- repairs-per-accepted-decision

### 4.3 Go-live gate (documented, not automated in P2)

The lane goes live (P3) only when the harness + the shadow decision log show a **calibrated risk–coverage curve with a useful low-risk operating region**: acceptably low wrong-valid rate at a coverage level worth having, with abstention catching the uncertain tail. P2 delivers the measurement; the decision to flip from shadow to live is a later, human, evidence-based gate.

## 5. Crates / structure

| Crate | Role |
|---|---|
| `deblob-slm` | `SemanticInferencer` impls: `HttpInferencer` (default); the fixed 3-way contract types, structured-output/grammar handling, decision cache, one-repair policy, redacted prompt builder. `LocalInferencer` (llama.cpp) is a later `local-llama` feature — not in P2-A/B. |
| `deblob` (bin) | wires the shadow classifier into the cold lane (behind `[slm] enabled`, shadow-only); shadow decision log; retrieval over the bucketed index. |
| `deblob-eval` | offline harness binary: loads the golden corpus, drives a configured endpoint, emits the metrics report. |

## 6. Security (P2 deltas)

- SLM endpoint token is env-only (`DEBLOB_SLM_API_TOKEN`), never logged, never in TOML.
- Prompts carry no raw payload values; field names are treated as untrusted data (length-capped, escaped, injection-detected).
- Treat every retrieved schema description and every model output as untrusted input; the id allow-list + contract validation is enforced deterministically outside the model.
- The inference endpoint is an external dependency: its unavailability/timeout is a shadow "unavailable" outcome, not a service failure. No credentials or executor authority ever reach the model or its pod.

## 7. Phasing within P2-A/B

- **A1 — contract + HttpInferencer:** the 3-way contract types, `HttpInferencer` against an OpenAI-compatible endpoint, structured-output handling, id-allow-list + one-repair validation, decision cache. Unit-testable with a mock HTTP server (`wiremock`) — no real model needed for the plumbing.
- **A2 — retrieval + PII-safe prompt builder:** structural-distance top-k over the bucketed index; the redacted prompt builder + injection detection. Unit tests over fixtures.
- **A3 — shadow wiring + decision log:** invoke the classifier per stable cluster from the cold lane, record ShadowDecisions, apply nothing. Integration test (fake inferencer) proving shadow-only (no state mutation) + the log is written.
- **B — eval harness + corpus:** the `deblob-eval` binary, the golden corpus, the metrics report. Run against a mock endpoint in CI; against a real endpoint manually.

## 8. Testing strategy

TDD throughout (80%+). `wiremock` for the HTTP endpoint (deterministic, no real model in CI). Fixture-based tests for retrieval ranking, prompt redaction (assert no raw value leaks; injection sequences flagged), contract validation (id-allow-list, extra-field rejection, one-repair-then-abstain). Shadow wiring integration test asserts **zero** state mutation (no candidate/schema/index writes) — shadow means shadow. The eval harness has a self-test over a tiny corpus + mock endpoint producing a known report. A real-endpoint smoke run is documented but not required in CI.
