# P2-D Task 9/10 — Hermes review `deblob-p2d-02` (semantic similarity), authoritative

Folded from Hermes' review of the semantic-neighbor-search feature. Verdict: **"Confirm with corrections — a good P2-D pull-forward: deterministic, bounded, explainable."** Binding for Tasks 9 (signature core) + 10 (index + API).

## 1. Metric — exact weighted multiset Jaccard (NO cosine, NO MinHash)

For feature counts `cA,f`/`cB,f` and integer weight `w_f`:
```
numerator   = Σ w_f · min(cA,f, cB,f)
denominator = Σ w_f · max(cA,f, cB,f)
```
Retain the score as the rational `(numerator, denominator)`. Rank by integer cross-multiplication `n1·d2 > n2·d1` using **checked `u128`**. A decimal is presentation-only (never used for ranking). Cosine is rejected (over-rewards repeated generic features, floats). MinHash/LSH deferred — add ONLY if telemetry shows p95 candidate union > 20,000 or p95 exact lookup > 50 ms.

## 2. Features + weights (versioned `deblob-semantic-signature-weights-v1`)

Emit BOTH atomic and **compound** tokens (compounds preserve field↔attribute association so `temperature:USD, price:Cel` ≠ `temperature:Cel, price:USD`). Compounds bind to `canonical_field_id` (NOT the path) → path-independent. When `canonical_field_id` is absent, emit only the low-weight standalone feature.

| Feature | Weight |
|---|---|
| `event:<canonical_event_type_id>` | 24 |
| `field:<canonical_field_id>` | 12 |
| `field-idns:<cfid>:<namespace>` | 10 |
| `field-unit:<cfid>:<system>:<code>` | 8 |
| `idns:<identifier_namespace>` (standalone) | 6 |
| `unit:<system>:<code>` (standalone) | 4 |
| `field-enum:<cfid>:<vocab-version>:<meaning_code>` | 4 |
| `enum-meaning:<vocab-version>:<meaning_code>` | 3 |
| `field-temporal:<cfid>:<kind>` | 3 |
| `temporal:<kind>` (standalone) | 1 |

No corpus-derived IDF in v1 (would make scores depend on vault population, hurting reproducibility). Multiplicity: **count-capped multiset**, `effective_count = min(actual_count, 4)`.

## 3. Strength (returned separately from the numeric score)

```
strong:  same canonical_event_type_id  OR  ≥2 shared canonical_field_id with ≥50% canonical-field coverage
medium:  ≥1 shared canonical_field_id  OR  shared identifier_namespace + another semantic feature
weak:    overlap only units / temporal kinds / enum vocab+codes
insufficient: no anchor features
```
If both schemas declare event types and they DIFFER → cap strength at `medium` regardless of raw score.

## 4. Anchors + bounds

Require ≥1 anchor feature (`canonical_event_type_id` / `canonical_field_id` / `identifier_namespace`); with none → `{ neighbors: [], strength: "insufficient", reason: "no_anchor_features" }` (never expand a `temporal.kind=instant`-only query toward the whole vault). `k` default 10, max 50. Max exact candidates 20,000 → if the inverted-index candidate union exceeds it, return `signature_too_broad` (NEVER silently truncate and claim top-k correctness).

## 5. Determinism guards (all 12 mandatory)

1. Domain separation `deblob-semantic-signature-v1\0` (distinct from `sem_`'s `deblob-semantic-v1`).
2. **Typed length-prefixed feature encoding** — `type||length||value||…`, NOT delimiter-joined strings (an embedded `:` in a value would otherwise collide).
3. Reuse Task 3's canonical metadata (post-NFC + vocabulary-resolved); do NOT re-normalize independently in the signature subsystem.
4. NO paths / display names / original field names / position / order in any feature.
5. Defined multiset behavior: dedup rule, `min(count,4)` cap, integer-overflow behavior (checked), feature sort order.
6. Unknown = absent (never synthesize a default unit/temporal/namespace).
7. Every code carries its vocabulary namespace + immutable version.
8. Sort encoded feature bytes lexicographically; NEVER depend on `HashMap` iteration or `DefaultHasher`.
9. Tie-break order: higher strength → higher exact rational score → more shared anchor features → lexicographically smaller `sch_` bytes.
10. **Active-revision indexing:** on re-annotation, atomically remove the old active revision's postings, add the new revision's postings, move the active pointer. Historical revisions stay queryable but must NOT pollute the default neighbor index.
11. **Extractor version:** a future optional axis (CRS, semantic groups) must NOT silently become a v1 feature — it requires a new signature-extractor version even if the `sem_` contract is stable.
12. **Rebuild ≡ incremental:** a full rebuild from active vault assertions must produce byte-identical postings AND identical neighbor ordering to incremental indexing (a test).

(If MinHash ever arrives: additionally freeze hash algorithm, seeds, permutation count, unsigned arithmetic, byte order, band + row counts. Never the randomized std hasher.)

## 6. Authority — strictly diagnostic

The neighbor API must NEVER: merge families, create aliases, promote candidates, mutate `sch_`/`sem_`/active assertions, affect wire tags, affect the hot path, elevate an SLM proposal, or treat score `1.0` as proof of equivalence. Response shape:
```json
{ "query_schema": "sch_…", "signature_version": "deblob-semantic-signature-v1",
  "weights_version": "deblob-semantic-signature-weights-v1",
  "neighbors": [ { "schema_id": "sch_…", "semantic_revision_id": "…",
    "score": { "numerator": 84, "denominator": 96, "decimal": "0.875000" },
    "strength": "strong", "shared_anchor_count": 4,
    "matched_feature_classes": ["canonical_event_type_id","canonical_field_id","field_unit"] } ],
  "authority": "diagnostic_only" }
```
Controls: authenticated management API; exclude the query schema itself; use active semantic revisions by default; `include_historical=true` only for auditors; record signature/weights versions; label each result `semantic_neighbor_candidate`, never `equivalent_schema`.

## Task split

- **Task 9 (pure, `deblob-semantic`):** feature extraction (atomic + compound, count-capped, typed length-prefixed encoding, versioned weights) + `similarity` (exact weighted-multiset Jaccard rational + `u128` cross-mult ranking) + strength classification + anchor detection. Golden-tested (the `temperature:USD/price:Cel` discrimination, rename path-independence, determinism, no-anchor→insufficient, tie-break).
- **Task 10 (`deblob-redis` + `deblob` bin):** the bounded inverted-index postings (atomic active-revision posting swap reusing Task 5's Lua transition, `signature_too_broad` on >20k union, rebuild≡incremental) + `GET /api/v1/schemas/{sch_id}/semantic-neighbors?k=&include_historical=` (authenticated, diagnostic-only, versions + strength + shared_anchor_count + matched_feature_classes, excludes self).
