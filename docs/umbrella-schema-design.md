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

## Open questions (for the merge with Hermes)
- Umbrella-field NAMING when children disagree and no canonical vocabulary exists —
  most-common child name, or a controlled vocab the SLM maps into?
- Do we need silver as a materialized tier, or is it just an annotation on bronze?
- Versioning: when a new child joins an existing umbrella and adds a field, is that a
  new umbrella version (additive/optional) or a new umbrella?
- Lossy transforms: allow (with a `lossless:false` flag + parked fields) or forbid?
