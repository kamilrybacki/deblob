# Umbrella consolidation — name + value signals (joint Claude × Hermes design)

run: dc-umbrella-signals-1907 · 2026-07-19 · agents: Claude Code + Hermes

## Problem

Gold-umbrella consolidation groups schemas by the **typed field signature** only —
the set of `(canonical_field_id, scalar_type)` pairs. It ignores two things the
user wants considered:

- **Field names** — retained in the schema canonical but abstracted away by `cfid`.
- **Observed values** — never retained (privacy §9); only coarse, non-reversible
  `NumericBuckets` (neg/zero/small/med/large) + per-type counts exist, and only on
  the **ephemeral candidate profile** (7-day TTL). The promoted schema is
  count-/bucket-blind.

Consequence today: two same-`cfid` fields whose real value distributions differ
wildly (cents-as-int vs ratio) still auto-consolidate; and name evidence never
informs or explains a merge.

## Does it need a separate model or retraining? — No. [C+H]

Name-similarity and value-bucket compatibility are **deterministic**. They
*constrain or explain* the deterministic trust gate; they do not expand the
model's authority. The SLM already sees redacted names + numeric buckets in its
prompt, so the model is not blind to them. No new model, no retraining.

What **is** required: **shadow evaluation + policy calibration** before
enforcement — the change alters which SLM-supported proposals may auto-promote.
Measure added-HITL rate, prevented false merges, false non-merges, behavior by
domain/unit, and legacy (profile-absent) behavior. [H]

## Design (agreed)

### 1. Umbrella identity is unchanged [C+H]
Typed `(canonical_field_id, scalar_type)` stays the grouping key. **Names never
enter identity** (pure renames are a core supported case). Value buckets never
enter identity either.

### 2. Durable value-profile snapshot, captured atomically at promotion [C+H]
The promoter (`policy.rs`) already deserializes the full candidate `Profile` when
it mints the schema — capture there. Best-effort candidate lookup is unfit (TTL,
non-deterministic, unauditable after expiry).

Store it as a **compact, versioned SIDECAR referenced by `SchemaRecord`**, NOT
embedded — so schema lists / retrieval / semantic-neighbor ops don't pay for data
they rarely need. [H — this revises Claude's initial "embed on the record"]

```rust
SchemaRecord { …, value_profile_ref: Option<ValueProfileId>,
                  value_profile_summary: Option<ValueProfileSummary> }

struct ValueProfileSnapshot {          // the sidecar blob (lazy-loaded)
    profile_id, profile_version,       // "value-profile-v1"
    candidate_id, candidate_profile_digest,
    observation_count, captured_at_ms,
    leaves: Vec<LeafValueProfile>,
}
struct LeafValueProfile {
    field_ref: CanonicalPathRef,       // typed canonical path incl. array wildcard
    present_count, explicit_null_count,
    type_counts: TypeCounts,
    numeric_bucket_mask: u8,           // the 5 existing buckets as a bitmask
    int_only, neg_zero_seen,
}
```
Immutable, versioned, atomic with publish, **excluded from `sch_`/`sem_`/umbrella
identity digests**, `None` for legacy schemas (never a synthetic empty profile).
Later drift → append-only later snapshots; never rewrite the promotion snapshot.

### 3. Value-bucket guard = one-sided negative triage, four outcomes [H]
Not binary. Buckets are **OR-merged booleans, not distributions** — overlapping
masks CANNOT prove compatibility; only *disjoint* masks flag suspicion.

```
COMPATIBLE      corroborating (rarely assertable from buckets alone)
CONTRADICTORY   disjoint masks → exclude from auto-merge, require HITL
UNKNOWN         absent/insufficient obs, or below min-support → NO veto
NOT_COMPARABLE  units/scale/temporal differ → defer to transform validation
```
- "Absent data = allow" means **no veto**, not positive compatibility.
- **Compare buckets ONLY after unit/scale/temporal classification.** Different
  units → `NOT_COMPARABLE` (don't compare cents vs dollars masks at all).
- Coarse masks can't be safely transformed (×100 spans boundaries) → abstain,
  never synthesize a normalized mask.
- **Minimum support** before a mismatch may block (guards early-sample/seasonal
  bias); preserve `observation_count` + `captured_at_ms`.

### 4. Name-similarity = capped POSITIVE corroboration only [C+H]
- Corroborates an existing `cfid` correspondence, improves ranking/explanation.
- **Never** establishes correspondence alone; **never** vetoes on name disagreement.
- Generic-token stoplist (`id`,`value`,`type`,`status`,`data`) contributes ~nothing.
- Neutralize injection-flagged / truncated / Unicode-confusable names.
- Never let name similarity rescue a hard type/unit contradiction.

### 5. Don't double-count evidence [C+H]
`cfid` may already derive partly from the name; the SLM already sees names+buckets.
Do **not** treat "SLM agrees" + "name agrees" + "bucket agrees" as independent.
Keep a **feature-level evidence ledger + calibrated policy**, not additive score
bonuses.

### 6. Surface value-PROFILE EVIDENCE + causes, never values [H]
UI may show: `observed types: number (1,204), null (7)`, `numeric regions:
1–10, 11–100`, `sample window: promotion snapshot`, `decision: bucket-compatible`.
Never representative/min/max/sampled/reconstructed values.

Record field-selection provenance as **bounded deterministic cause codes**:
```
CFID_EXACT · TYPE_COMPATIBLE · NAME_CORROBORATED ·
VALUE_PROFILE_{COMPATIBLE,UNKNOWN,NOT_COMPARABLE,CONTRADICTION} ·
UNIT_CONVERSION_VERIFIED · HELD_OUT_EXECUTION_PASSED · HITL_OVERRIDE
```
Store codes + profile refs + quantized deterministic scores — never SLM prose or
raw values.

## Risks [H]
- **False non-merges from representation**: cents/dollars, %-fraction vs points,
  bytes vs MiB, s vs ms, °C/°F, debit sign conventions, power vs energy, rounded/
  clipped sources, source-specific ranges. → the `NOT_COMPARABLE`-before-compare
  rule is the primary mitigation.
- **Attachment correctness**: flatten candidate tree → promoted leaves via canonical
  typed paths (incl. array wildcards); bind the snapshot to canonicalizer version +
  schema canonical digest + candidate profile digest + exact path ref. A
  mis-attached profile is WORSE than none (authoritative false evidence).
- **Privacy of permanence**: "non-reversible" ≠ "non-sensitive" — permanently
  recording a named field once held a negative/huge value can leak about a small
  population. Suppress display for `pii`/`secret` classes; min-support; no per-bucket
  exemplars; keep profiles out of logs, metric labels, lineage headers.
- **Perf coupling**: never embed the full profile in `SchemaRecord`; sidecar +
  reference, lazy-load, one blob per profile (never one Redis key per leaf — bitmask
  the buckets, reference leaf positions, don't repeat field names).

## Vault size [H]
Compact indexed JSON ≈ 33 B/leaf → ~1.66 KB per 50-leaf schema (~1.66 GB at 1M
schemas), vs ~14.7 KB/schema verbose. Bitmask buckets, varint counters, cap stored
leaves to the bounded-parse limit, version the bucket boundaries.

## Rollout [H]
Shadow the new gate (compute + record decisions, don't enforce) → calibrate on the
Greenwindow counterfactuals (price/MWh vs total, 0–1 vs 0–100 %, carbon intensity
vs total emissions, power vs energy, epoch s vs ms, currency major/minor) → enable
enforcement once false-non-merge rate is acceptable.

## Attribution
[C] = Claude, [H] = Hermes, [C+H] = independently agreed. Hermes's key upgrades
over Claude's initial sketch: sidecar-not-embed, four-outcome guard, one-sided
negative semantics, `NOT_COMPARABLE`-before-compare, minimum-support, permanence-
privacy, attachment-digest binding, evidence-ledger over additive scores,
shadow-before-enforce.
