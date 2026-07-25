# Deblob similarity precision — IDF + domain-coherence gate — Joint Research Report
run: jr-deblob-similarity-idf-221040 + jr-deblob-domain-gate-221052 · 2026-07-22 · agents: Claude Code + Hermes

## Executive summary

The similarity search has **two independent false-positive classes**, and they need
different fixes:

1. **Generic-overlap false-closes** — two schemas look "close" because they share
   ubiquitous fields (`cfid_timestamp`, `cfid_value`, …). Fixed by the b24 stop-list
   (shipped) and generalized by **IDF** (down-weight common features by document
   frequency). `[C+H]`
2. **Cross-domain false positives** — two *different* domains share the same
   *discriminative* vocabulary (GPU-spot-price vs electricity-spot-price both carry
   `cfid_price`+`cfid_region`+`cfid_timestamp`). Structural math is **blind to
   domain**; IDF cannot fix this (both cfids are rare → high IDF). Requires a
   **domain-coherence gate**. `[C+H]`

Both agents independently converge on the codebase's existing discipline:
**build the mechanism → run in SHADOW → evaluate against a sealed hard-negative set
→ config-gate enforcement with a kill switch.** The exact-rational similarity score
is never mutated by either layer; domain coherence is a *filter*, IDF is an *integer
re-weighting*. Governance invariant throughout: the model proposes a bounded enum,
deterministic code decides, human overrides win.

**Status:** the pure IDF math core is built + tested (`deblob-semantic/signature.rs`,
91 tests, zero live-behaviour change). Everything else is staged below.

## Key findings

### IDF (jr-deblob-similarity-idf-221040)

- **IDF math belongs in pure `signature.rs`; storage only supplies statistics.** A
  handler-side re-implementation would violate the Task-9/Task-10 boundary. `[C+H]`
- **IDF must affect anchor/strength, not only the numerator** — the ranking is
  strength-first, so score-only IDF would leave false-closes at `Medium`. `[H]`
- **Keep `GENERIC_CFIDS` as a hard override**; IDF generalizes it to the long tail
  but never replaces it. Don't remove the stop-list until an ablation shows no
  regression. `[C+H]`
- **`floor(log2(N/df))`, cap `IDF_MAX=16`, anchor threshold `ANCHOR_IDF_MIN=2`** —
  integer quantization preserves the exact rational. Features in ≥½ the corpus →
  IDF 0 (dropped from the union before scoring). `[H]`
- **`N` = count of schemas with a current active semantic revision**, via a
  `deblob:sem-active-schemas` SET maintained atomically with revision activation
  (`SCARD` = O(1)); df = `SCARD(deblob:sem-sig:<hex>)`. Fetch N + all df through one
  atomic snapshot (Lua/`MULTI`) for a coherent read. `[H]`
- **`WEIGHTS_VERSION → v2`; expose `idf_population_n` + `idf_index_epoch`** in the
  response — IDF is corpus-relative, so v2 alone does not pin reproducibility. `[H]`
- **Stop-list scope is the *atomic* generic cfid only** — `cfid_value` is generic,
  but the compound `cfid_value + ISO4217:USD` may be rare/useful; its own df decides
  its weight. `[H]`

### Domain-coherence gate (jr-deblob-domain-gate-221052)

- **Per-schema-revision domain assertion, not pairwise SLM verdicts** — O(schemas)
  not O(pairs), reusable by neighbors + umbrellas + search + lineage, stable across
  changing top-k, independently overridable. `[C+H]`
- **CRITICAL: do not key domain by `sem_id`.** `digest.rs` hashes normalized
  `SemanticMetadata` *without* `sch_id` or source — GPU-price and electricity-price
  can share a `sem_id`, which is exactly when the domain layer must distinguish them.
  Bind assertions to `(schema_id, semantic_revision_id, source_context_digest,
  taxonomy_version, classifier_version)`. `[H]` ← corrected my draft.
- **Closed, hierarchical, versioned taxonomy stored as governed DATA** — not a
  compile-time Rust enum, not model free-text. Nodes (id/slug/parent/aliases/owner)
  + a **sparse, symmetric `DISJOINT`/`RELATED` matrix, default `UNKNOWN`**. Different
  IDs do **not** imply disjoint; the SLM may only pick existing IDs or abstain. `[H]`
- **The 32 ingest sources are bootstrapping evidence, not 32 domains** — sources and
  subject-domains are different concepts; one provider may carry several domains.
  Prefer **source+topic/stream** mappings over provider-wide; mixed sources stay
  `UNKNOWN`. `[H]`
- **No SLM required for the first enforcing cut.** Deterministic high-precision
  evidence (human assignment > governed source+stream map > event/entity concept >
  identifier namespace > unit dimension > high-IDF cfid vocab + name tokens > source
  lineage prior) covers most of the estate cheaply; unknown → `KEEP`. CFID clustering
  *alone* cannot solve the motivating case (same vocabulary); narrowly-scoped source
  lineage can. `[C+H]`
- **Gate = pure function → `KEEP | VETO_PROVEN_DISJOINT`**; veto **only** when *every*
  relevant primary×facet cross-product is explicitly `DISJOINT` and both assertions
  are trusted. Any unknown/related/stale/proposed → `KEEP`. Precedence:
  `FORCE_ALLOW` > `FORCE_DISJOINT` > require-both-trusted. `[H]`
- **Classification is OFF the request path** — a neighbor request never launches O(k)
  model calls; a missing assertion means keep the candidate + async-enqueue
  classification. Neighbor integration = structural retrieve → exact IDF score →
  bounded oversample `max(k×4, k+16)` → domain gate → existing exact ordering →
  top-k. **Exact score never mutated.** `[H]`
- **The "one-sided ⇒ no new false negatives" guarantee is literal only in SHADOW** —
  once enforcing, a wrong accepted label or matrix edge *can* drop a true neighbor.
  Enforcement therefore needs trusted assertions + reviewed disjoint edges + overrides
  + a sealed recall benchmark. `[H]`
- **Same assertions + matrix reused for umbrellas** (child→prototype + complete-link);
  a veto suppresses *provisional* grouping and routes to HITL, never rejects a
  human-approved active umbrella. An umbrella-specific `FORCE_ALLOW` supports
  intentional cross-domain gold products. `[C+H]`

## Conflicts & adjudication

- **My draft: cache domain per `sem_id`.** Hermes refuted it from primary source
  (`digest.rs` excludes source identity). **Resolved in Hermes' favour** — verified
  against `crates/deblob-semantic/src/digest.rs`. Domain is now keyed by the full
  provenance tuple above.
- **My draft: SLM in the first cut.** Hermes: deterministic-first, SLM only for
  unresolved assertions in shadow. **Resolved in Hermes' favour** — matches the user's
  chosen scope ("IDF + deterministic domain gate now, SLM next").

## Recommended layered design

```
1. STRUCTURAL candidate-gen   Jaccard + b24 stop-list + IDF    exact-rational, O(N)→O(k) bounded
2. DOMAIN classify (off-path)  per-(sch,rev,source) assertion   deterministic-first, SLM later, cached
3. DOMAIN gate (pure filter)   KEEP | VETO_PROVEN_DISJOINT      never mutates the score
4. POLICY / HITL               closed taxonomy matrix + overrides   human wins; config-gated enforce
```

## Build order (Hermes' 11 steps, mapped to the user's "now vs next")

**NOW (this effort) — IDF + deterministic domain gate, all SHADOW:**
1. Freeze eval sets: GPU↔electricity price, energy↔aviation, observation↔forecast,
   + structurally-different **same-domain positives** (guard against over-splitting).
2. Taxonomy artifact: stable IDs, hierarchy, aliases, owners, sparse related/disjoint
   matrix, version, override precedence, governed source+stream mappings.
3. Pure domain types + gate (cross-product rule, explicit UNKNOWN, cause codes, tests).
4. Persist revision-bound assertions (`(sch,sem-rev,source-digest)`, immutable
   provenance, human-overwrite protection like schema naming).
5. Deterministic classifier in shadow (governed source maps, event/entity, units,
   namespaces, IDF-cfid evidence; UNKNOWN on conflict).
6. Neighbor **shadow** gate (bounded oversampling, log would-veto, compare
   precision/recall/underfill/latency, preserve exact score).
   — plus wire **IDF** end-to-end (N SET + df snapshot + `similarity_weighted`/
   `strength_weighted`, `WEIGHTS_VERSION` v2), also evaluated in shadow first.

**NEXT — SLM + enforcement:**
7. Umbrella shadow gate (child→prototype + complete-link).
8. Bounded SLM domain proposal lane (`assign_domain`/`assign_multi_domain`/`abstain`,
   PII-safe prompt extension, revision-aware cache, sealed eval, **no SLM-only veto**).
9. Human review + overrides (`FORCE_ALLOW`/`FORCE_DISJOINT`, provenance, stale status).
10. Config-gated enforcement (human + governed-source assertions first; canary neighbor
    filtering; then umbrella; immediate kill switch).
11. Later: optionally accept corroborated SLM labels via explicit policy, never
    confidence alone.

## What is already built

- `deblob-semantic/signature.rs`: pure IDF core — `idf_multiplier(n, df)` =
  `floor(log2(n/df))` clamped `[0, IDF_MAX=16]`; `similarity_weighted(a, b, idf_mult)`
  (integer IDF preserves the exact rational); IDF-aware `has_anchor_weighted` /
  `strength_weighted` / `shared_anchor_count_weighted` with `ANCHOR_IDF_MIN=2`
  generalizing the `GENERIC_CFIDS` stop-list; the non-weighted public fns now delegate
  with a saturating `|_| u64::MAX` multiplier == exact b24 behaviour. +6 tests, 91 pass.
  **Not yet wired** to the handler — changes zero live behaviour.

## Open product decisions (before enforcement)

- How many of the 32 sources are genuinely single-domain? Is provenance
  stream/topic-granular enough for narrow source→domain maps?
- Where does the taxonomy boundary sit between *subject domain* and *event/entity
  concept*?
- Go-live precision gate + wrongful-veto rate on hard same-domain positives.

## Sources

- Deblob @ `49bd63c`: `signature.rs`, `semantic_neighbors.rs`, `deblob-redis/semantic.rs`,
  `deblob-semantic/digest.rs`, `deblob-slm/{contract,prompt,cache}.rs`,
  `deblob-umbrella/{adjudicate,verify}.rs`, `deblob/umbrella_guard.rs`,
  `deblob/umbrella_controller.rs`, `deblob-core/ports.rs`.
- Robertson, *Understanding Inverse Document Frequency*.
- Paulsen/Govind/Doan, *Sparkly: a TF/IDF Blocker for Entity Matching* (VLDB'23).
- Chakrabarti et al., *Weighted Set-Based String Similarity*.
- Rahm & Bernstein, *A Survey of Approaches to Automatic Schema Matching* (VLDBJ'01).
- Doan/Domingos/Halevy, *Learning to Match the Schemas of Data Sources* (VLDBJ'04).
- Gangrade et al., *Selective Classification via One-Sided Prediction* (AISTATS'21).
- DataHub Domains docs; Redis SCARD/pipelining docs.
- Hermes vault: `research/Deblob-Domain-Coherence-Gate-JR-221052.md`,
  `research/Deblob-Umbrella-Schema-Medallion-Joint-Design-2026.md`.

## Method note

Two sequential Hermes consults (IDF, then domain-gate), each ~15 min, both returned
`COMPLETE`. Claude built the pure IDF core in parallel. No infra changes made by either
agent. Hermes' `sem_id`-keying correction was verified against primary source before
adoption.
