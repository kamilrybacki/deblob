# SLM-Proposed, Human-Editable Schema Names — Joint Research Report
run: `jr-schema-naming-211140` · 2026-07-21 · agents: Claude Code + Hermes

## Executive summary
**Yes, feasible — but NOT SLM-only.** The robust design is a **hybrid: deterministic heuristic baseline → Qwen2.5-0.5B refinement → strict grounded validation → fallback**, with an absolute precedence **human > accepted-SLM > heuristic > `fam-<suffix>`**. A human edit must always win and is never clobbered by a later automatic run. Build shape mirrors the consolidation controller (external CronJob), plus one small in-core addition (a name-edit endpoint + a `label_source` field in provenance). Qwen2.5-0.5B is fine as a *wording assistant*, not the naming authority (its IFEval is 27.9 — too weak to trust unguarded).

## Key findings

### Deblob-side mechanism `[C, code-verified]`
- The display name lives in **`SchemaRecord.provenance`** (a free-form `serde_json::Value`); the console reads `provenance.label` and falls back to `fam-<last6>` (`web/console.html:341`). **`FamilyRecord` has NO name field** (`ports.rs:248` = `{family_id, current_version}`), so naming is a schema-provenance concern, not a family-record one.
- The existing **`LabelSource` enum** (`deblob-slm/feedback.rs:25`) is for **ML training-label provenance** (HumanPromote / TrustedProposalAccepted / …) — *not* the display name. So the "name source" governance (`slm|human|heuristic`) is **new** and should live in provenance alongside the name.
- **No name-edit endpoint exists** — the only family route is `GET /families/{id}` (`api/mod.rs:315`). A `PUT` to set the name must be added (in-core → a rebuild, "b22").
- The `OllamaInferencer` (`deblob-slm/adapters/ollama.rs`) implements `SemanticInferencer`, whose shape is the **decision lane** (match/new), not free-text naming — so naming is best a **raw Ollama `/api/chat` call**, not a reuse of that trait. Ollama + `qwen2.5:0.5b` is live and healthy in-cluster.

### SLM-naming approach `[H, verified vs Qwen/Ollama docs]`
- **Hybrid, not SLM-only.** Qwen2.5-0.5B-Instruct strict IFEval = **27.9** (vs 1.5B=42.5, 3B=58.2) → gate everything deterministically. Pipeline: normalize+rank field paths → heuristic name → give heuristic + 15–40 discriminative paths to Qwen → validate → accept only if grounded AND ≥ as specific as the heuristic.
- **Deterministic baseline**: split dots/arrays/camel/snake/kebab; downweight plumbing (`id uuid url timestamp created_at version data items metadata type`); a **versioned signature dictionary** for strong combos (`doi+author+title`→"Scholarly Works", `latitude+longitude+observed_at`→"Location Observations", `claims+labels+sitelinks`→"Knowledge Graph Entities", `repository+actor+payload`→"Repository Events"). Prefer a conservative "Status Events" over hallucinated "Customer Order Updates".
- **Prompt/inference**: Ollama `/api/chat`, `format` = JSON-Schema `{"name": string}`, `temperature: 0`, fixed `seed`, `num_predict: 32`, `stream:false`. System prompt: Title Case, 2–4 words, only concepts supported by fields, banlist (`Data JSON Schema Family Payload Information Misc Unknown`), no prose/prefixes, "heuristic is safe — change only when fields clearly support better". 3–4 few-shot incl. one *preserve-the-baseline* example.
- **Validation gates** (structured output guarantees format, not correctness): exactly one `{name}`, 2–4 words ≤40 chars, regex `^[A-Za-z][A-Za-z0-9]*(?: [A-Za-z][A-Za-z0-9]*){1,3}$`, reject generic/punct/prompt-fragments, **every content token must be licensed** by a field token / abbrev expansion / signature rule, reject less-specific-than-heuristic, 1 retry then fallback. Compute **controller confidence** (grounding ratio, signature strength, coverage, specificity, collision) — never trust self-reported confidence.
- **Uniqueness**: names are display metadata; `family_id` stays identity. **Don't require global uniqueness**; allow duplicate display names; compare via NFKC+casefold+collapsed-space; add a grounded discriminator within a source ("Repository Push Events" vs "Issue Events"); if a machine slug is needed, `repository-events--<family-fragment>` (avoid unstable `(2)` suffixes).

### Human-override + idempotency `[C+H, agreed]`
- **Effective name is a pure precedence function**: `human → accepted_slm → heuristic → fam-<suffix>`. The controller **never writes or clears `human`**; it re-reads the record + uses an **ETag/version precondition** so a concurrent human edit wins; clearing a human override is an explicit audited action.
- Idempotency tuple: `family_id + field_set_hash + prompt_version + normalizer_version + model_digest` — schema drift may create a *new proposal* but must not change the effective name while a human override exists. (Mirrors the annotation controller's idempotent posture — and fixes its "re-run" weakness by keying on this tuple.)

## Recommended design + build plan
**Data model** — add to `provenance` (free-form JSON, no migration):
```
provenance.name = {
  effective,                 # computed by precedence (what the console shows)
  human, slm, heuristic,     # the three candidate strings (any may be null)
  source: human|slm|heuristic|fallback,
  slm_meta: { prompt_version, model_digest, field_set_hash, confidence, accepted }
}
```
**Build (3 parts):**
1. **In-core (rebuild → b22):** `PUT /schemas/{id}/name` (or `/families/{id}/name`) with `{name, source}` + `If-Match`. Server rule: `source=human` always sets `human`+recomputes effective; `source in {slm,heuristic}` sets only if `source != human` (never clobbers a human). Emit an audit entry. Console `GET` already returns provenance → the name+source render for free.
2. **Namer controller (external CronJob, like `49-consolidation-controller`):** list families → for each without a `human` name (or whose `field_set_hash` changed): normalize fields → heuristic → raw Ollama `/api/chat` refine → validate gates → `PUT …/name {source: slm}` (or `{source: heuristic}` on fallback). Idempotency tuple prevents redundant work. Python image (already the pattern).
3. **Console:** show the name + a small **source badge** (`slm`/`human`); inline edit → `PUT …/name {source: human}` with the ETag.

**Rollout (Hermes):** ship in **shadow mode** first; build a 100–200 family gold set (obvious/ambiguous/cryptic/multilingual/drifting/colliding); measure format-validity, human-accept-without-edit rate, hallucination rate, fallback rate, collisions, rename-churn. Auto-accept SLM names only if they beat the heuristic without raising hallucination; if <~80% survive review, evaluate **Qwen2.5-1.5B** (natural Apache-2.0 upgrade) before anything bigger. No frontier model — the deterministic fallback + human edit is already a safe terminal path.

## Conflicts & adjudication
No conflict — the halves compose cleanly (Claude: where/how it's stored + edited in Deblob; Hermes: how to generate + gate the name). Both independently landed on the **human > SLM > heuristic > fallback** precedence `[C+H]`.

## Sources
Qwen2.5 blog (IFEval 27.9/42.5/58.2) https://qwenlm.github.io/blog/qwen2.5-llm/ · Qwen2.5-0.5B-Instruct card https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct · Ollama structured outputs https://docs.ollama.com/capabilities/structured-outputs · Ollama API https://github.com/ollama/ollama/blob/main/docs/api.md · Geng et al. 2025 (structured-output benchmark) https://arxiv.org/abs/2501.10868 · "Constraint Tax" 2026 https://arxiv.org/abs/2605.26128 · SLOT EMNLP-Industry 2025 · Ell et al. WWW 2018 (schema-label generation). Code: `crates/deblob-core/src/ports.rs`, `crates/deblob-slm/{feedback.rs,adapters/ollama.rs}`, `crates/deblob/src/api/mod.rs`, `web/console.html`.

## Method note
Split: Claude = Deblob-side mechanism (label storage, edit endpoint, SLM client, controller-vs-in-core), code-verified; Hermes = SLM-naming approach (Qwen2.5-0.5B prompting, structured output, validation, collisions, idempotency, homelab-fit), report also in its vault `research/Deblob-SLM-Schema-Naming-JR-211140.md`. Dispatched ~11:41 CEST; Hermes COMPLETE 6/6. No timeout.
