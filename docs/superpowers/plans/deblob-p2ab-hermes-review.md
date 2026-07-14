# P2-A/B — Hermes review deltas (`deblob-p2ab-01`), authoritative

Folded from Hermes' design review. These OVERRIDE the corresponding "AMEND" markers in `2026-07-14-deblob-p2ab.md` and refine the spec. Concrete values are binding for the build.

## Task 1 — contract (discriminated union)

Model output is EXACTLY one of (serde discriminated on `decision`, `deny_unknown_fields`):
```
{"decision":"match_schema","schema_id":"sch_…","relation":"exact|compatible_drift|incompatible_similarity"}
{"decision":"new_candidate","novelty":"structural|semantic"}
{"decision":"abstain","cause":"ambiguous|insufficient_evidence|candidate_missing"}
```
- `schema_id` validated against the exact retrieved top-k. `relation`/`novelty`/`cause` are fixed enums — NO free-text rationale, NO confidence field. Do not request either.
- `exact` = physical/canonical equivalence — deterministic code ALREADY knows this; keep it as a calibration/control case and log every model disagreement.
- `compatible_drift` = likely same family AND deterministically compatible.
- `incompatible_similarity` = resemblance WITHOUT permission to tag as that schema. **DANGER:** it lives under `match_schema` but must NEVER be treated as an accepted match. In P2 shadow it's logged; any future live policy treats it as non-match evidence only. Encode this so it can't be mistaken for a match downstream (e.g. a helper `is_accepted_match()` that returns false for `incompatible_similarity`).
- Store OUTSIDE the model output (not model-supplied): `candidate_set_hash`, `retrieval_version`, `selected_candidate_rank`, `deterministic_compatibility_result`.

## Task 2 — structured output (tool-calling default; constraint-tax avoidance)

- DEFAULT: model-native tool calling — one required tool `submit_semantic_decision`, arguments = the discriminated union, `additionalProperties:false`, `temperature:0`, final-call budget ≤ 32 tokens.
- "Reason free, constrain late": (1) present monoid stats + 3–5 candidates; (2) allow a short UNCONSTRAINED comparison ≤ 48 tokens OR the provider's private reasoning channel; (3) require the `submit_semantic_decision` call; (4) strict decoding on the tool ARGS only; (5) discard/never-log private reasoning. If the endpoint can't separate reasoning from args → use direct tool calling as the default (do NOT double prefill by default; a two-pass variant is an experiment only).
- GBNF is for the later llama.cpp path only: flatten schema, no `$ref`/recursion/broad `anyOf`/unbounded strings, precompile + cache by contract hash, grammar-constrain only the final decision object.
- Prevent schema-valid-but-wrong: recompute `exact`/compatibility DETERMINISTICALLY; reject any id outside top-k; score the selected relation against ground truth SEPARATELY from parse validity; ONE repair for syntax/transport defects ONLY — never repair an invalid SEMANTIC decision into a valid one; repeated parse failure / contradictory relation / missing evidence → `abstain`. Log wrong-valid as a first-class outcome (`parsed=true, schema_valid=true, semantic_correct=false`). 100% schema-valid is NOT a success criterion.

## Task 3 — retrieval (weighted structural distance, no embeddings)

Normalized distance weights:
```
35% field-path / type-signature distance
25% normalized field-name token overlap
15% required / presence overlap
10% nesting / depth similarity
10% nullability & type-union similarity
 5% array / map shape similarity
```
- Normalize field names deterministically: case-fold, separator-split, Unicode-normalize, length-cap. Names must never become prompt instructions.
- Retrieve family REPRESENTATIVES, not 5 adjacent versions of one family: the nearest current version + at most one historical drift boundary per family.
- `top_k = 3` default; the eval evaluates k = 1, 3, 5. Log ties and the top-1/top-2 distance margin. Include a gold-ABSENT test arm.
- Embeddings are justified ONLY if known-family `recall@3 < 95%` OR semantic-renaming cases are > 25% of false splits. First fix normalization / family dedup / weighting / bucket boundaries. The SLM cannot recover a schema omitted from top-k.

## Task 5 — shadow log + go-live gate

Immutable record per structural cluster/window (fields, grouped): decision_id, cluster_id, source_id, observation_count, observation_window, canonicalizer_version, monoid_version, redaction_policy_version, structural_evidence_hash · retrieval_algorithm_version, full top-k ids, family/version per candidate, rank+distance per candidate, top1/top2 margin, candidate_set_hash, retrieval_latency · prompt_template_version, rendered-redacted-prompt hash, model_id, model_digest, server/runtime version, quantization, temperature/seed/token-limits, structured-output backend, req/resp token counts · raw model response (access-controlled), parsed decision, selected id+rank, relation, novelty/abstain code, parse_error, schema_validation_error, repair_count, TTFT, total_latency, timeout/provider error · deterministic_compatibility_result, counterfactual_live_disposition, human/reviewer label, correct schema/family/relation, labeler+adjudication version. NEVER log raw values; preserve the exact redacted evidence for reproducibility.

Risk–coverage: `coverage = accepted match_schema / eligible`; `semantic_risk = incorrect accepted / accepted`; `false_merge_risk = wrong-family accepted / accepted`. Operating curve is built on DETERMINISTIC gate variables (selected rank, structural distance, top1/top2 margin, observation count, relation, source class, redaction-loss flags) — NEVER model confidence. Initial policy grid: rank==1, distance ≤ 0.15, margin ≥ 0.10, obs ≥ 20, relation ∈ {exact, compatible_drift}, deterministic compat passed, no redaction collision.

Go-live gate (documented, NOT automated in P2 — enable ONE source/family slice first): ≥ 3000 accepted labeled shadow decisions; **ZERO false merges** (hard gate); accepted precision ≥ 99.5%; compatible-drift precision ≥ 99.0%; wrong-valid ≤ 0.5%; coverage ≥ 25%; no ≥100-example slice < 99% precision; injection-induced changes 0/500; temp-0 repeat agreement ≥ 99.9%; endpoint latency/error SLOs pass. **False merge is the hard gate** — false splits reduce coverage and are repairable; false merges corrupt identity.

## Tasks 6–7 — eval metrics + corpus

Add metrics (beyond parse/schema-valid/exact/wrong-valid/abstention/injection/latency): recall@1/3/5; MRR of gold family; false-merge rate and false-split rate (each separate from generic error); relation confusion matrix; novel-family recall+precision; gold-absent abstention rate; per-source/per-family worst-slice precision; candidate-order sensitivity; repeatability across 3 temp-0 runs; redaction-induced accuracy loss; prompt/model/quant regression delta; mechanical repair rate + repair success rate; timeout/provider-error/malformed/refusal rates; TTFT/prefill/decode/total latency + tokens + cost separately; human-label inter-annotator agreement; counterfactual unsafe-acceptance rate; whole-lane cache-hit/invocation-avoidance rate.

Corpus composition (family/source/time-SEPARATED partitions — never randomly split neighboring versions of one schema): 25% known/exact, 20% compatible drift, 15% incompatible/related-but-unsafe, 20% new family, 20% ambiguous/malformed/adversarial/insufficient.
- False-merge cases: identical structure/different business meaning; same names/different units-currencies-ids-semantics; near-neighbors differing by one discriminator; same envelope/unrelated payload; candidate list with a plausible-but-wrong high-frequency family; two families whose only distinction was removed by redaction; reused generic names (`id/status/type/value/timestamp`).
- False-split cases: renamed fields/unchanged semantics; snake/camel/abbrev/vendor-prefix/Unicode variants; added optionals; nullability widening; new wrapper around unchanged payload; reordered fields; sparse/rare fields; array-cardinality change/same semantics; multiple historical versions.
- Mandatory: gold at ranks 1/2/3; gold absent; all-null + empty-object; map-vs-record ambiguity; heterogeneous arrays; field-name injection; extremely long names + Unicode homoglyphs; redaction collisions; candidate-order permutations; quantized-vs-reference comparison.

## Task 8 — default model targets

- FIRST: **IBM Granite 4.0 Nano 1B** (dense/instruct; ~1.6B actual — eval the exact quantized artifact). Apache 2.0, native structured-JSON/OpenAI-schema tool calling, governed-extraction fit, small enough to self-serve via an OpenAI-compatible endpoint. The deployable zero/few-shot baseline.
- SECOND: **FunctionGemma 270M** (efficiency-specialist lower bound; native function tokens; NOT production-reliable zero-shot — expect eventual fine-tune on Deblob decisions/hard-negatives/abstentions; eval native FunctionGemma formatting, not generic chat JSON; review license).
- Do NOT start with a 3–4B model. Only if BOTH fail the risk–coverage gate, add **SmolLM3 3B** as a capability ceiling to separate "model too weak" from "candidate set is wrong" — before touching retrieval or embeddings.
