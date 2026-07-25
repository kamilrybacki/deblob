# SLMs for Dynamic Heterogeneous-Data Identification & Schema Control — Joint Research Report

run: **jr-slm-schema-241439** · 2026-07-24 · agents: Claude Code + Hermes (live two-agent pass)
attribution: `[C]` Claude · `[H]` Hermes (fresh vault+web pass) · `[C+H]` independently corroborated

---

## Executive summary

Claude's external sweep and Hermes' fresh vault+web pass (run independently, then
cross-examined) reach the **same conclusion**: an SLM belongs on the schema problem only
as a **candidate-generator / semantic-proposer**, never as the deterministic decision and
never on the reproducible, low-latency hot path. Hermes' organizing frame — the cleanest
statement of the boundary — splits the work into **three lanes**, and the safety of the
whole system depends on not blurring them: `[C+H]`

```
DETERMINISTIC FACTS      parse → physical types → canonicalize → fingerprint → profile → detect drift
PROBABILISTIC PROPOSALS  retrieve candidates → rank matches → name/describe → propose mapping → explain
DETERMINISTIC CONTROL    validate → test → classify loss → enforce policy → quarantine/approve → publish
```

The SLM lives **only in the middle lane**. In Deblob's real behavior this session, the
0.5B model **did not catch drift** — canonicalization, fingerprinting, candidate state,
tagged Kafka headers, and source-local schema comparison caught it; the SLM *named and
explained* the change. Likewise, generated code was never the normalizer — the normalizer
was the validated mapping IR plus its deterministic interpreter. `[C+H]`

The SLM's defining failure is **confident hallucination on incomplete context**: at
0.5B-on-CPU it emits a plausible, structurally-valid, *wrong* answer that passes every
check not specifically written to catch it. So constrained output "validates transport,
not truth"; the grounding gate, degrade-to-heuristic fallback, and human-approval of
genuinely ambiguous changes (a unit `÷100`, a type flip, a lossy cast) are first-order
correctness properties, not afterthoughts. `[C+H]` Fine-tuning helps only as a **bounded,
abstention-capable reranker/classifier** — never an autonomous schema author — and the
model's operational envelope (memory admission, model-availability, PII, the
settle-and-sample blind window) is itself a correctness property. `[C+H]`

> **The thesis both agents converged on:** *Use the SLM to compress ambiguity into an
> auditable proposal; use deterministic systems to establish facts and exercise control.*

---

## Division of responsibility (the safety boundary)

Calling all three lanes "dynamic schema inference" hides the boundary that keeps the
system safe. Determinism **owns the decision** for everything reproducible or
irreversible; the SLM only proposes inside a bounded candidate set. `[C+H]`

| Determinism owns (fact / control) | Why |
|---|---|
| Parsing, nested-path extraction | Exact, reproducible |
| Primitive / union-type inference | Cheaper + more reliable than generation |
| Canonical form, fingerprint, schema ID | Identity must not change with model/prompt version |
| Known vs provisional classification | Registry lookup, not interpretation |
| Source-local drift detection | Exact expected-ref vs observed-ref transition |
| Settle-and-sample state | Explicit performance policy |
| Compatibility rules | Must be replayable + auditable |
| Family / version publication | Durable contract mutation |
| PII redaction + egress | Prompts are **not** security boundaries |
| Transform execution | Generated code is untrusted |
| Bidirectional / lossless claims | Require property + round-trip tests |
| Warehouse admission | Must fail closed |

**Invariant** `[H]`: *the model may add evidence or reduce a candidate set; it may not
erase contradictory evidence, weaken a guard, or make an irreversible contract mutation.*

---

## Key findings

### USE — where an SLM correctly fits

1. **Candidate-generation + deterministic validation is the pattern.** The SLM *proposes*
   (a name, a field mapping, a normalization); deterministic rules + empirical evidence
   *validate or filter*. The single most-repeated recommendation across both agents. `[C+H]`
   - Deblob: the naming/matching result is applied only after **deterministic
     corroboration** ("model proposes, deterministic code + policy decides, human override
     wins"); the SLM never risks a false merge.
   - External: "LLM proposes mappings/normalizations; deterministic rules validate or
     filter" (Data Engineering Weekly); "LLMs are most effective as a planning, validation,
     and debugging layer… **augmenting** deterministic systems, not replacing them"
     (PhilArchive). `[C]`

2. **Reranking a finite candidate set is the most *credible* SLM role.** The model is
   strongest choosing among a handful of *retrieved* schemas / glossary terms / canonical
   fields — not inventing a global ontology. Retrieval narrows the space before the model
   makes an expensive semantic judgement. `[C+H]`
   - Magneto, ReMatch, GRAM, Matchmaker (Hermes' web pass) all pair lightweight retrieval
     with a small LM reranker; ArcheType does the same for column-type annotation. `[H]`

3. **Naming / conceptual-attribute inference is the sweet spot.** A fingerprint proves
   `{order_id, amount, currency}` *differs* from a known shape; it cannot decide it means
   `commerce.order` vs `payment.intent`. Given field paths + types + source context + a
   handful (k ≤ 5) of sampled/profiled values, the SLM supplies the human-meaningful name.
   `[C+H]`
   - SI-LLM infers a canonical attribute name per column from header + ≤5 sampled values `[C]`;
     Deblob's Qwen2.5-0.5B names a discovered schema from `system + 4 few-shot +
     Source/Fields/Baseline`, output pinned to a 2–4-word Title-Case name in
     `provenance.label`.

4. **Bridging heterogeneous vocabulary.** Recognizing possible equivalence between
   `cust_no`, `customer_id`, `buyer.identifier` where exact matching and edit-distance
   fail — a good fit for semantic-neighbor retrieval + umbrella *candidate* generation
   (the human still ratifies the umbrella). `[H]`

5. **Enrichment + explanation, not shape-identification.** The structure (fingerprint) is
   deterministic and on the hot path; the SLM adds the semantic layer and can *explain a
   computed diff* ("amount moved under `payment`, changed number→string, may now encode
   minor currency units") — valuable in Drift Sentinel, but the changed paths/types must
   come from deterministic comparison. `[C+H]`
   Deblob: `HotMatcher.classify` resolves shape deterministically (Known → fast tagged);
   the SLM runs only in the cold-discovery lane, off the hot path.

6. **Zero-config discovery works with an LLM + verification loop.** Cluster → LLM-label →
   LLM-infer schema → **verify field frequency across the cluster with grounding quotes** →
   export JSON Schema; "every output traces back to real text"; "CPU is enough" (Lakshana).
   Mirrors Deblob's discover → propose → corroborate flow. `[C]`

7. **Local SLM for bulk + PII-safe; hosted only for edge cases.** Hybrid (local model for
   volume, hosted LLM for hard cases) is the common production shape (Data Engineering
   Weekly). Deblob runs a fully-local 0.5B, sending nothing out. `[C]`

### LIMITATIONS

8. **Confident hallucination on incomplete context — the defining failure.** "When a model
   lacks the metadata it needs, it doesn't stop and ask. It produces a plausible-looking
   answer… the outputs pass every check that doesn't specifically test for the thing the
   model made up" (tianpan.co). A 0.5B is *especially* prone to generic labels, ontology
   invention, missing correspondences, and confident explanations from sparse context.
   Deblob's answer: the trust gate + **holding** an ambiguous change until a human confirms.
   `[C+H]`

9. **Valid JSON ≠ semantic correctness.** Pydantic / JSON Schema / constrained decoding
   guarantee shape, enum membership, bounds, required fields — they **cannot** establish
   that `amount` is major vs minor currency units, a timestamp is event vs ingestion time,
   two IDs share identity, a cast is lossless, or a moved field kept its meaning.
   "Constrained output validates transport, not truth." This is exactly the
   `amount → total_cents` (÷100) case Deblob **refuses to guess**. `[C+H]`

10. **No semantic-equivalence guarantee — structural similarity ≠ semantic equivalence.**
    `customer_segment` may map to `account_tier` or `risk_category` — both valid strings,
    both pass validation, one is wrong (tianpan.co). Hermes' demo framing this session:
    *"safe containment, not magical remapping."* `[C+H]`

11. **Long-tail / rare-field weakness.** One enterprise audit: **6% of LLM entity mappings
    needed human correction, concentrated in long-tail categories** (Data Engineering
    Weekly); a fine-tuned Qwen2-0.5B hit ~70% field accuracy, rare fields "guessed poorly"
    (axon011). Independently, PARSE reports **GPT-4 at an 11.97% invalid-response rate on
    complex extraction** and large gaps on nested entities — the ceiling is well below
    "trust it unsupervised," and it drops further at 0.5B. `[C]`

12. **Generic-type / generic-name fallback.** Small models fall back to "overly generic
    types," missing granularity (SI-LLM). Deblob counters with a **BANNED-word list**
    (Data/JSON/Schema/Payload/Misc/Unknown) and a licensing rule: every name token must be
    licensed by a field token, an abbreviation expansion, a head-noun, or the source. `[C]`

13. **Degrades on sparse/empty/non-English data, and is fooled by popular values.** Quality
    drops for near-empty tables and non-English column names (DBAutoDoc); with multiple
    plausible keys it "chooses the most frequent pattern instead of the semantically
    correct one" (assign.cloud). Deblob's domain-coherence gate + provenance guard this
    cross-domain false-positive class. `[C]`

14. **Latency + memory on CPU are real and can cascade.** 0.5B warm ≈ 2 s; queue contention
    under concurrency; and memory is treacherous. This session's OOM was a genuine outage.
    Hermes' consult isolated the fix as **admission control — not a single-variable
    cause**: bound *all* of loaded-models=1, parallel=1, queue=64, context=4096, container
    limit=3 GiB, **and** disable the llama prompt-cache RAM reservoir. That configuration
    took **421 requests over 12 min with 0 errors, ~1004 MiB peak, no OOM kill.** Prompt
    length, output length, concurrency, model count, queue depth, and memory must each be
    bounded independently. `[C+H]`

### FINE-TUNING

15. **Fine-tune a bounded, abstention-capable reranker/classifier — never an autonomous
    author.** The right target emits: candidate ID, field correspondences, evidence
    categories, structured uncertainty, and **`abstain`**. Start with deterministic
    retrieval + prompt/few-shot baselines; fine-tune only once the ontology is stable and
    accepted/rejected reviewer decisions provide durable labels. `[H]`

16. **Qwen2.5-0.5B + LoRA/SFT is a proven base for schema-constrained tasks** (axon011;
    structured-extraction-ft). **QLoRA is usually overkill at 0.5B** — the model fits in
    memory unquantized, so 4-bit "just hurts quality"; it pays off at ≥7B (Union.ai). For
    Deblob's Arm-C on a Modal T4, prefer **plain LoRA or full FT** at 0.5B. `[C]`

17. **Training data must be adversarial and provenance-tracked.** Real reviewed positives;
    near-match **hard negatives**; renamed + nested fields; sparse-context; explicit
    **no-match / abstention** examples; malformed + adversarial names; prompt-like strings
    inside values; **source-held-out + time-held-out** test sets; balanced sources/classes;
    provenance per example. Distillation lowers labeling cost but a teacher's output is
    weak supervision; synthetic examples must be deduplicated, validated, and **must not
    contaminate the real eval set.** `[H]` Deblob's continual-learning loop + `capture_sources`
    feed this over time — but see the poisoning caveat (#24). `[C]`

18. **Grounding lives in the prompt, and the prompt should be payload-free.** Field name +
    k ≤ 5 profiled values (SI-LLM); Deblob adds a licensing/grounding gate (name tokens
    licensed by field/source tokens) + a **reject-if-weaker-than-heuristic** guard so the
    tuned model can only *improve* on the deterministic baseline, never regress it. The
    preferred prompt carries trusted source/domain context, field paths + physical types,
    and null/distinct/range/length/pattern summaries — **not raw values**. `[C+H]`

19. **Evaluate the complete served artifact, and separate syntax from semantics.** Eval the
    whole stack — base + tokenizer + adapter + quantization + prompt + output grammar +
    Ollama/runtime config — on: top-k recall, macro-F1, **abstention quality**, calibration
    / risk–coverage, **unsafe false-acceptance**, cold/warm latency, peak RSS, timeout+OOM
    rate, heuristic-degradation rate. **Self-reported confidence is not calibration.** The
    field still "lacks standardised criteria for extracted-schema quality" and rarely
    measures runtime/memory (*Nature* SVEF) — so build your own harness. `[C+H]`

20. **Serving hardening is part of the model.** A reference 0.5B extraction server ships
    `MAX_CONCURRENCY=1`, inference-timeout → HTTP 504, readiness probes, Prometheus metrics
    (structured-extraction-ft) — identical to Deblob's ollama admission (`NUM_PARALLEL=1`,
    `OLLAMA_MAX_QUEUE`, caller timeouts) from Hermes' consult. `[C+H]`

### CAVEATS (operational)

21. **Degrade-to-heuristic, never fail — and mark it.** Any SLM timeout/error falls back to
    the deterministic heuristic, not a failed job. Fallback results must be explicitly
    marked `heuristic` / `model_unavailable`, must **not inherit simulated model
    confidence**, must **not create a retry storm**, and must **not auto-promote**. Deblob:
    3 consecutive ollama failures → circuit-breaker → heuristic-only for the rest of the
    run. `[C+H]`

22. **Unloaded ≠ unavailable; readiness needs an inference canary.** A model unloaded after
    `keep_alive` is normal and cold-loads on demand; a missing artifact / non-persistent
    model dir after restart is a *deployment failure*. Readiness must run a **bounded
    inference canary against the exact model + expected output contract** — not merely
    check that the port or model-list endpoint responds. Deblob's fix for the wiped
    `emptyDir` case: an idempotent `postStart` model-pull. `[C+H]`

23. **PII / data leakage — redact before prompting.** Never send raw PII/secrets/free-text
    to the model (values may carry PII, secrets, prompt-injection, rare identifiers).
    Redact **before** the prompt + an allowlisted deterministic output gate; unreviewed
    production values must not enter training corpora, traces, logs, or dead-letter
    prompts (OWASP LLM02; NIST GenAI Profile). Deblob never persists raw payloads and
    PII-gates every collector. `[C+H]`

24. **Do not continuously fine-tune on unlabeled production drift.** It mixes concept
    adaptation with contract governance, enables **poisoning + catastrophic forgetting, and
    destroys reproducibility.** The continual-learning loop must feed a *reviewed, held-out*
    dataset, not raw drift. `[H]`

25. **The settle-and-sample blind window.** A settled source retains its cached tag for
    unsampled changed records; detection delay ≈ `sample_interval / records_per_second`.
    Acceptable only when the bound is documented and consumers tolerate it — **incompatible
    with "the first breaking record was contained"** unless the hero source gets full
    analysis or an independent writer-schema contract. An SLM cannot repair this; sampling
    + containment stay deterministic policy. `[H]`

26. **Declared beats inferred; the model process holds no keys.** "Declared schema takes
    precedence over inferred relationships unless proven wrong and formally updated"
    (assign.cloud) — Deblob's human-approved umbrellas + domain gate. Operationally, the
    model process should have **no registry-promotion credential, no warehouse DDL, no
    direct schema-state writes, no raw k8s/Vault/shell**; a governance service performs
    immutable publication and records schema ID, candidate set, model digest, adapter,
    quantization, prompt/grammar version, validator outcome, policy decision, reviewer
    result. `[C+H]`

27. **Generated transforms need round-trip proof.** Codegen path: SLM proposes mapping IR →
    deterministic type checker → template/compiler emits code → isolated execution →
    fixtures + property + **forward→reverse round-trip** tests → reviewer approval. Prefer a
    **finite transform vocabulary** (`rename, move, cast_checked, enum_map, split, join,
    default`); every reverse mapping declares `lossless` / `conditionally_lossless` /
    `lossy`; **no proof → lossy or blocked.** Unit conversion, timezone interpretation,
    truncation, many-to-one merges, unchecked string→number casts cannot be made safe by
    model confidence (DeepMind Round-Trip Correctness; QueryArtisan verified codegen).
    `[C+H]`

---

## Conflicts & adjudication

- **Little disagreement — strong corroboration.** Claude's external sweep and Hermes' fresh
  vault+web pass independently produced the same "propose / decide / control" boundary from
  disjoint sources (Claude: SI-LLM, PARSE, tianpan, Data-Eng-Weekly, Nature SVEF, the FT
  repos; Hermes: Magneto, ArcheType, ReMatch, GRAM, Matchmaker, DLISC, Confluent, DataHub,
  OpenMetadata, NIST, OWASP). Independent corroboration → the `[C+H]` findings are the
  highest-confidence tier.
- **QLoRA vs plain LoRA at 0.5B.** The extraction repos default to QLoRA (4-bit) and still
  hit ~100% JSON-validity; Union.ai argues 4-bit "just hurts quality" at a scale that fits
  in memory. **Adjudication:** QLoRA *works* but is unnecessary at 0.5B — prefer plain LoRA
  / full FT; reserve QLoRA for ≥7B.
- **Fine-tune target: generation vs reranking.** Claude's external repos fine-tune for
  schema-*validated JSON generation*; Hermes argues the safe Deblob target is a bounded
  *reranker/classifier with abstain*, not a generator. **Adjudication:** no real conflict —
  the repos prove the base can be tuned to emit valid structure; Hermes constrains *what to
  emit* so it can't become an autonomous author. Adopt Hermes' framing (abstain-capable
  reranker), using the repos' serving hardening.

---

## Unverified leads

- Deblob's *own* tuned adapter is unmeasured (the Arm-C LoRA is a planned Modal-T4
  follow-up; only heuristic + base-model naming are live). The ~100% validity / ~70%
  field-accuracy figures are from analogous 0.5B fine-tunes — treat as expectations, not
  Deblob measurements.
- Whether DPO meaningfully helps a *naming/reranking* task (vs. *extraction*) is untested.
- Whether a 0.5B can draft a genuinely useful mapping IR (vs. brittle free-form codegen) is
  medium-confidence and needs a corpus labeled reversible / conditionally-reversible / lossy.

---

## Hermes' perspective

*(Fresh live pass — the mcp-bridge `SHARED_UPSTREAM` fix restored Discord, so this is a
real two-agent run, not a fallback to session memory.)* Hermes recalled **10 vault notes**
from prior Deblob/SLM joint research and ran an independent web sweep, then delivered a
10-part complementary report (saved to its vault at
`research/Deblob-SLM-Dynamic-Identification-Schema-Control-JR-241439.md`). Its core
contributions, now folded into the findings above:

- **The three-lane boundary** (facts / proposals / control) as the organizing safety frame —
  and the sharp observation that *"the 0.5B model did not catch drift; deterministic
  comparison did — the SLM named it."*
- **The OOM correction**: not one variable but *admission control* across six bounded
  dimensions (loaded=1, parallel=1, queue=64, ctx=4096, cache-RAM off, 3 GiB), validated at
  421 req / 12 min / ~1004 MiB / 0 OOM.
- **"Unloaded ≠ unavailable"** → readiness must be a bounded inference canary, not a port
  check; fallback marked `heuristic`/`model_unavailable`, no confidence inheritance, no
  retry storm, no auto-promotion.
- **Fine-tune as an abstaining reranker, never an autonomous author**; never continuously
  fine-tune on unlabeled production drift (poisoning + catastrophic forgetting +
  reproducibility loss).
- **Finite transform vocabulary + `lossless`/`conditionally_lossless`/`lossy` labels + round-trip
  proof** for any generated normalizer; **payload-free prompt**; **model process holds no
  promotion/DDL/write/shell credentials**.
- Confirmed its earlier **settle-and-sample blind-window** and **"safe containment, not
  magical remapping"** design judgements against fresh sources.

Its closing thesis matches Claude's synthesis exactly: *compress ambiguity into an
auditable proposal; let determinism establish facts and exercise control.*

---

## Sources

**Claude — external sweep**
1. Lakshana — zero-config schema discovery (cluster → LLM label/infer → verify + grounding → JSON Schema) — https://github.com/mickyaero/lakshana
2. DBAutoDoc — automated discovery/documentation of undocumented DB schemas (arXiv 2603.23050) — https://arxiv.org/html/2603.23050
3. SVEF — Schema Validation & Evaluation Framework (*Scientific Reports*/Nature 2026) — https://www.nature.com/articles/s41598-026-45554-6
4. SI-LLM — Schema Inference using LLMs (arXiv 2509.04632) — https://www.arxiv.org/pdf/2509.04632
5. PARSE — LLM-driven schema optimization for reliable entity extraction (arXiv 2510.08623) — https://arxiv.org/html/2510.08623v1
6. LLMs as Data Engineers: The Silent Failures in AI-Driven ETL (tianpan.co, 2026) — https://tianpan.co/blog/2026-04-20-llms-as-data-engineers-etl-schema-inference-validation
7. LLMs in Data Engineering: Practical Trade-Offs (Data Engineering Weekly, 2026) — https://data-engineering-weekly.contentwave.net/article/when-to-use-llms-in-data-engineering-practical-tradeoffs-for-2026
8. LLM Inferred Relationships vs Declared Schema: Steward Playbook (assign.cloud, 2026) — https://assign.cloud/llm-inferred-relationships-vs-declared-schema-a-data-steward
9. Safe & Scalable Data Integration Using LLMs for Schema Harmonization (PhilArchive) — https://philarchive.org/rec/SANSAS-13
10. structured-extraction-ft — QLoRA SFT+DPO on Qwen2.5-0.5B, schema-validated JSON — https://github.com/Ashok007-cmd/structured-extraction-ft
11. Fine-tune an LLM with LoRA & QLoRA (Union.ai; "QLoRA overkill at small scale") — https://www.union.ai/blog-post/fine-tune-an-llm-with-lora-qlora-in-a-flyte-pipeline
12. json-extraction-qlora-dpo — schema-constrained JSON, syntax-vs-semantic eval — https://github.com/shreyashreddyk/json-extraction-qlora-dpo
13. axon011/llm-fine-tuning — QLoRA Qwen2-0.5B (100% validity, ~70% field acc) — https://github.com/axon011/llm-fine-tuning

**Hermes — schema matching / structured data**
14. Magneto — combining small + large LMs for schema matching — https://arxiv.org/abs/2412.08194
15. ArcheType — open-source column-type annotation using LLMs — https://www.vldb.org/pvldb/vol17/p2279-freire.pdf
16. ReMatch — retrieval-augmented schema matching — https://arxiv.org/abs/2403.01567
17. GRAM — generative retrieval-augmented matching of data schemas — https://arxiv.org/abs/2406.01876
18. Matchmaker — self-improving schema matching — https://arxiv.org/abs/2410.24105
19. Towards Scalable Schema Mapping Using LLMs — https://arxiv.org/abs/2505.24716
20. DLISC — on-device schema-aware information extraction — https://arxiv.org/abs/2505.14992

**Hermes — verified transform / codegen**
21. DeepMind — Round-Trip Correctness — https://github.com/google-deepmind/icml2024-roundtrip-correctness
22. QueryArtisan — verified generated data-manipulation code — http://www.vldb.org/pvldb/vol18/p108-yao.pdf
23. LLM-assisted explicit + inspectable schema mappings — https://ceur-ws.org/Vol-4192/TGD-paper3.pdf

**Hermes — contracts / catalogs / runtime / safety**
24. Confluent data contracts & compatibility — https://docs.confluent.io/platform/current/schema-registry/fundamentals/data-contracts.html
25. DataHub AI glossary-term suggestions — https://docs.datahub.com/docs/automations/ai-term-suggestion
26. DataHub business glossary — https://docs.datahub.com/docs/glossary/business-glossary
27. DataHub semantic-model metadata — https://docs.datahub.com/docs/generated/metamodel/entities/semanticmodel
28. OpenMetadata column standard — https://openmetadatastandards.org/data-assets/databases/column/
29. Qwen2.5-0.5B-Instruct model card — https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct
30. Ollama FAQ (model loading / keep_alive) — https://docs.ollama.com/faq
31. NIST AI RMF — Generative AI Profile — https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence
32. OWASP LLM02 — Sensitive Information Disclosure — https://genai.owasp.org/llmrisk/llm02-insecure-output-handling/

**Hermes — vault notes recalled** (homelab research vault, not public)
- On-the-Fly-Data-Inference-with-SLMs-and-Edge-Models-2026 · Deblob-SLM-Schema-Naming-JR-211140 · Deblob-SLM-Finetune-Distillation-JR-211549 · Pydantic-V2-in-Deploying-SLMs-2026 · Prominent-Edge-On-Device-SLM-Landscape-2026 · Google-FunctionGemma-270M · Deblob-Stability-Robustness-JR-231518 · Deblob-Drift-Sentinel-Demo-Design-JR-2026-07-23

---

## Recommendations

1. **Keep the SLM in the middle lane only.** Deterministic fingerprinting *identifies* the
   shape and *catches* drift; the SLM *names / reranks / proposes / explains*. Never let a
   probabilistic output be the record-routing, drift, or trust decision.
2. **Gate every SLM output.** Deterministic corroboration + grounding (tokens licensed by
   the data) + reject-if-weaker-than-heuristic + human-approve the genuinely ambiguous
   (unit/type/lossy changes). Validate transport *and* truth — semantics is the layer teams
   skip and regret.
3. **Fine-tune as a bounded, abstaining reranker/classifier** — not an author. Plain LoRA
   (not QLoRA) at 0.5B, on 200+ adversarial, provenance-tracked, source/time-held-out
   examples; evaluate the *served artifact* on abstention quality + unsafe-false-acceptance,
   not just JSON validity. Never continuously fine-tune on unlabeled drift.
4. **Degrade to heuristic, always — and mark it.** Timeout/failure → deterministic baseline,
   circuit-break, no confidence inheritance, no retry storm, no auto-promote. Readiness =
   inference canary, not a port check; self-heal the model load on restart.
5. **Treat the operational envelope as correctness.** Bound loaded-models, parallelism,
   queue, and context independently; disable the RAM cache reservoir; add leading-indicator
   alerts (RSS slope, queue depth, timeout rate).
6. **PII-safe by construction** — payload-free prompts, redact-before-prompt, allowlisted
   output gate; never persist/send raw payloads or feed them to training.
7. **Generated transforms need round-trip proof.** Finite transform vocabulary,
   `lossless`/`conditionally_lossless`/`lossy` labels, no-proof→blocked; a governance service
   (not the model process) performs immutable publication and holds all credentials.

---

## Method note

**Live two-agent parallel pass.** Claude and Hermes each researched a complementary brief
independently, then Claude cross-examined and synthesized. **Claude:** architectural
placement, limitations, fine-tuning mechanics + external web sweep (13 sources incl. 2
arXiv, *Nature*, PARSE, industry practitioners), cross-checked against Deblob's live
implementation. **Hermes:** fresh recall of 10 homelab research-vault notes + an
independent web sweep (schema-matching literature, verified-codegen, data-contract/catalog
docs, NIST/OWASP), plus its co-designer judgement from this session's Deblob work
(OOM consult, drift-sentinel demo, normalizer/codegen). Unlike the earlier attempt, the
Discord backend was healthy: the **mcp-bridge 0.6.0 `SHARED_UPSTREAM`** fix (one persistent
bot session, no per-request re-login churn) restored the channel, so Hermes' input here is
a genuine fresh pass — the independent corroboration underpins every `[C+H]` finding.
Raw Hermes reply preserved at `.jr-slm-schema-241439-hermes-raw.md`.
