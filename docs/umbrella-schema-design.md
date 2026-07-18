# Umbrella / consolidation schemas — medallion design (Claude half, jr-umbrella-181605)

Deblob discovers each stream's RAW schema today. This adds a **gold umbrella schema**
that consolidates semantically-similar streams into one canonical shape, plus an
**executable per-child → umbrella transform**. Framed as the **medallion pattern**.

## Medallion → Deblob mapping
| Tier | What it is in Deblob | Status |
|---|---|---|
| **Bronze** | the raw discovered `SchemaRecord` per source (immutable family/version) | exists today |
| **Silver** | bronze + semantic annotation — per-field `canonical_field_id` + `unit` (typed, unit-tagged, cleaned) | **the sem_ lane already produces most of this** |
| **Gold** | a new **UmbrellaSchema**: one canonical schema over N semantically-similar children + a transform per child | new (this design) |

Key reuse: the P2-D semantic lane already assigns each field a `canonical_field_id`
and `unit`. That's the deterministic anchor the umbrella is built on — the SLM
proposes groupings, but a field only joins an umbrella slot when its independently
assigned `canonical_field_id` agrees.

## New governance objects
```
UmbrellaSchema {
  umbrella_id, label, version, state: provisional|active,
  member_schema_ids: [sch_...],          // the bronze/silver children
  canonical_fields: [{
    canonical_field_id, name, type, unit,
    cardinality: required|optional,      // required = present in ALL members
    backed_by: [sch_... -> from_path]    // provenance; every umbrella field is backed by >=1 child field (no invented fields)
  }],
  provenance, audit
}
ChildTransform {                          // one per member; EXECUTABLE + verifiable
  child_schema_id, umbrella_id, version,
  mappings: [{ umbrella_field, op, from_path, params, corroboration }],
  // op ∈ {rename, path_extract, cast, default, unit_convert, nest, flatten}
  unmapped_child_fields: [...],           // parked, not silently dropped
  defaulted_umbrella_fields: [...],       // filled/null on this child
  lossless: bool
}
```
Lifecycle mirrors candidate→schema: umbrella **candidate** (clustering + SLM) →
`provisional` → trust-gated **promote** → `active` (versioned) · reject path + audit.

## Clustering into an umbrella group (deterministic pre-filter)
Candidate umbrella group = a connected component in the **semantic-neighbor graph**
(`sem_` distance ≤ τ) with **≥2 members** AND **≥K shared `canonical_field_id`s**.
Structural retrieval (weighted-Jaccard) is the wrong signal here — two weather APIs
are structurally different but semantically the same domain — so we cluster on the
semantic axis, corroborated by canonical-field overlap. The SLM never sees a group
that the deterministic pre-filter didn't already justify.

## Transform-as-data (not free-form code)
Each mapping is a declarative op in a **restricted DSL** (rename / path_extract /
cast / default / unit_convert{factor,offset} / nest / flatten) — deterministic,
executable, and round-trip-checkable. Applying a ChildTransform to a real child
event MUST yield a record that validates against the UmbrellaSchema, or the
transform is vetoed. No arbitrary code, so it's safe to execute in the hot path.

## Trust gate (the zero-false-merge analog for consolidation)
A proposed (umbrella field ← child field) mapping promotes only if ALL hold —
the SLM proposes, determinism disposes:
1. **Canonical-id agreement** (the anchor): the child field's semantic
   `canonical_field_id` == the umbrella slot's `canonical_field_id`. Two *different*
   child fields may never land in the same umbrella slot for one child unless their
   canonical ids match. This is the direct analog of the deterministic corroboration
   in the merge gate.
2. **Type + unit compatibility**: child type castable to umbrella type; units equal
   or a known conversion exists (the `unit_convert` op carries factor/offset).
3. **Round-trip validation**: apply the transform to ≥M sampled child records →
   every output validates against the umbrella schema. Any failure vetoes.
4. **Evidence + coverage floors**: ≥N members, each observed ≥`min_samples`, and
   ≥X% of each member's fields mapped (low unmapped ratio) — else stays
   provisional/human, never auto-promotes.
5. **No-collision invariant**: within one child, distinct source paths can't collapse
   into one umbrella field (prevents silently merging separate fields).
SLM-only proposals with no canonical-id corroboration → `deferred_human`, exactly
like the existing gate's abstain path.

## End-to-end medallion flow (the payoff)
```
events.raw ──Deblob tag──▶ bronze schema (per source)
                              │ silver = + canonical_field_id/unit annotation
                              ▼
        ChildTransform (governed) applied to each child event
                              ▼
   events.gold.<umbrella> ── ONE canonical stream for ALL weather sources
```
A consumer subscribes to the gold stream and receives every weather source in one
shape — "describe all incoming weather streams + transform each into the umbrella,"
exactly as asked, as **executable, governed artifacts**.

## Convergence with greenwindow
The greenwindow project's hand-written normalizer (`{zone,kind,ts,value,unit,…}`
over grid + compute upstreams) IS an umbrella transform. This feature **generalizes
greenwindow's normalizer** — the grid/compute `signal` record is a gold umbrella over
carbonintensity/ENTSO-E/Azure children. And the console's OpenAPI export should
publish the **gold umbrellas** as the stable API contract (bronze churns; gold is the
promise), which is precisely what a published free API needs.

## New API + console surface
- `GET /umbrellas`, `/umbrellas/{id}`, `/umbrellas/{id}/members`, `/umbrellas/{id}/transforms`
- umbrella candidates via the candidates lifecycle; `POST /umbrellas/{id}/promote|reject`
- console: an "Consolidation" view — umbrella schema + member children + the mapping
  table + a gold-stream sample; OpenAPI export gains a "gold only" mode.

---

# Merged with Hermes (jr-umbrella-181605)

Hermes' half sharpens and extends the above. Attribution: `[C]` Claude, `[H]` Hermes,
`[C+H]` agreed. Full Hermes report: vault `research/Deblob-Umbrella-Schema-Medallion-Joint-Design-2026.md`.

## Clustering — prototype, NOT union-find [H, supersedes my connected-component idea]
Transitive similarity is dangerous: `A≈B` and `B≈C` must **not** imply `A≈C`. Model
each umbrella as a **prototype**; evaluate each child→prototype membership
**independently and many-to-many** (one child may project into several gold products).
Roles: exact `sem_` correspondence = high-precision anchor; semantic-neighbors =
candidate *retrieval only*; SLM = bounded adjudication; **deterministic gate = sole
membership authority**. **Complete-link safety** for multi-child umbrellas: every
child clears the prototype threshold AND a pairwise floor vs every other active child
— no contributor enters via a transitive path.

**Hard eligibility gates before scoring [H]:** same canonical event/entity concept ·
same grain/cardinality · compatible entity-key roles · compatible observation-time
semantics · no `DISJOINT`/cannot-link · no irreconcilable type/unit-dimension conflict.
These block the classic traps: observation-vs-forecast, air-temp-vs-dew-point,
event-time-vs-ingestion-time, latitude-vs-longitude, id-vs-display-label.

## Alignment signal [H]
Max-weight **bipartite** field matching, unmatched allowed, **1:1 only in V1**.
`coverage = harmonic_mean(matched_semantic_mass_i/mass_i, …_j/mass_j)`;
`alignment = coverage × mean(accepted_field_evidence) − hard_conflict_penalties`.
Weight ids / event-times / primary measurements over decorative metadata. The scalar
is for ranking only — **a contradiction is a veto, never averaged away.** Gold fields
from equivalence classes: **core/common** (total source-derived mapping + required in
every child), **shared-optional** (≥2 children), **source-extension** (1 child → stays
silver). A synthetic default never makes a field "common." Types from a
**least-common-lossless type lattice**; units via **UCUM** (equal dimension + quantity kind).

## SLM role — 3 narrow prompts, not one [H, extends my "finite-hypothesis" note]
Authority boundary: the SLM ranks/annotates **finite hypotheses**; it may NOT invent
source paths, target fields, expressions, unit equations, defaults, or promotion
decisions. `UNKNOWN`/abstain is a valid success.
1. **Correspondence adjudication** — deterministic code enumerates candidate field
   pairs; SLM returns `{candidate_id, relation ∈ {EXACT_EQUIVALENT, SAME_QUANTITY_DIFFERENT_UNIT, NARROWER, BROADER, RELATED, DISJOINT, UNKNOWN}, preferred, evidence_ids}`.
   Auto-bindable in V1: only `EXACT_EQUIVALENT` and `SAME_QUANTITY_DIFFERENT_UNIT`
   (registry-confirmed). Batches include **hard negatives** so the top-1/top-2 margin means something.
2. **Umbrella assembly** — SLM picks canonical names/types/units from finite choices;
   every proposed field must cite contributor fields (unsupported fields illegal); ids
   are deterministic, not SLM-authored.
3. **Transform selection** — deterministic code enumerates legal transform plans; SLM
   selects/scores plan IDs, never writes expressions.

**Closed V1 operator set [C+H]:** `bind/rename`, lossless `cast`, synthetic typed
`default` (marked), `nest`, `flatten`, registry-backed `unit_convert`,
cardinality-preserving element-wise array map. **Forbidden V1:** arbitrary code, free
arithmetic, regex, external lookup, inferred timezone, concat/split, joins/aggregation,
n:1 / n:m. (These need a separately governed DSL.)

## Trust gate — atomic bundle promotion [H, extends my gate]
Promote one bundle atomically: `umbrella_rev + accepted memberships + child transform_revs`.
Six stages: (1) **provenance/freshness** — pin+digest every input (sch_/sem_ revs,
silver contracts, samples, vocab, unit registry, model+prompt, enumerated candidate
set); stale → invalidate, never silently rebase. (2) **membership corroboration** —
umbrella ranked #1, calibrated distance+margin, min-obs, complete-link+pairwise floors,
**≥2 evidence classes per material mapping incl. ≥1 deterministic/non-SLM** (the
`canonical_field_id` agreement [C] is one such deterministic class). (3) **static
transform verification** — paths exist, targets exactly once, required outputs total,
casts lossless, unit conversions equal-dimension, defaults marked synthetic. (4)
**held-out execution** — replay sealed stratified samples (nulls/enums/arrays/edge
cases): deterministic+idempotent, every emitted event valid, zero silent drops,
round-trip equality for invertible ops, bounded numeric error. (5) **semantic
invariants + counterfactual negatives** — run lat↔lon, temp↔dewpoint,
event↔ingestion-time, min↔max mappings; they MUST fail. Random easy negatives don't
calibrate a zero-false-merge gate. (6) **shadow lifecycle** — emit shadow gold beside
silver, compare over a window, dead-letter reasons, rollback = flip active pointer
(never rewrite bronze).

**Audit vector [H]:** `(rank, distance, margin, observations, semantic_coverage,
pairwise_floor, transform_validation_rate, reject_rate, roundtrip_error,
hard_negative_margin)`. No universal threshold — calibrate per domain+grain; report the
one-sided upper confidence bound on false-merge risk (zero failures without sample size ≠ zero risk).

## Prior art [H]
Rahm & Bernstein (combine linguistic+structural+constraint+instance matchers), Clio
(separate correspondences from executable mappings), ReMatch (retrieval-narrowed LLM
matching), TaDA LLM-schema-matching study (fixed yes/no/unknown > free-form confidence).

## Synthesis — the design in one line
**Deterministic pre-filter + retrieval narrows the space → the SLM adjudicates finite,
bounded hypotheses → a multi-stage deterministic gate (anchored on canonical-id
agreement + held-out execution + counterfactual negatives) promotes an atomic
umbrella+transform bundle, precision over coverage.** Unresolved children stay silver;
gold appears only after executable mappings survive static validation, replay,
invariants, hard negatives, and shadow.

## Open questions (merged; Hermes' 12 + mine)
1. Membership binds to silver-contract-rev (H rec) vs raw family vs family-version.
2. Who defines event grain (`weather_observation` vs `weather_forecast_point`)?
3. Many-to-many umbrella membership — confirm supported.
4. Single-source fields: stay silver until a 2nd source corroborates?
5. V1 restricted to 1:1 + type/unit/path ops — confirm.
6. Gold must distinguish missing / null / empty-string / sentinel / synthetic-default.
7. Repeated-array flattening without changing grain/identity.
8. Canonical concept IDs local vs imported vs mixed (labels ≠ identity).
9. Which umbrella changes = compatible revision vs new semantic identity.
10. What child drift invalidates an existing transform.
11. Sealed alignment benchmark: how to represent hard negatives + ambiguous concepts.
12. May a gold revision activate with a declared subset of contributors, or all-or-nothing?
13. [C] Do we materialize silver, or keep it an annotation on bronze?

## Convergence with greenwindow [C] — still holds, now sharper
greenwindow's `{zone,kind,ts,value,unit,…}` normalizer over grid+compute upstreams IS
a gold umbrella; the collectors already discovering carbonintensity / Azure /
energy-charts bronze schemas are the first real umbrella-candidate members. This
feature would let the advisor's canonical `signal`/`window` contract be a *governed,
verified* umbrella instead of hand-written glue — and the console's OpenAPI export
publishes the **gold** as the stable API.
