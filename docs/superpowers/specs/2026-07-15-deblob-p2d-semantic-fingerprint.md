# Deblob P2-D — Semantic Fingerprint Design Specification

- **Date:** 2026-07-15
- **Status:** Draft
- **Parent spec:** `docs/superpowers/specs/2026-07-14-deblob-design.md` (P1 §5 defines the three-identity model; the semantic fingerprint is identity #3, deferred there)
- **Scope:** Sub-project **D** — the third identity dimension: a deterministic digest over *controlled metadata only* that distinguishes byte-identical structures with different meaning. Independent of the SLM lane (A/B) and the HTTP proxy (C).

## 1. Summary

Two payloads can share a byte-identical canonical structure — same `sch_` content identity — yet mean different things. A `temperature` number in Celsius vs Fahrenheit; a `price` in USD vs EUR; an `id` in the `user` namespace vs the `order` namespace. The structural fingerprint cannot tell them apart, and it must not: physical structure is exactly what `sch_` is for.

P2-D adds a **second, orthogonal, deterministic identity** — the **semantic fingerprint** — computed over a *controlled-vocabulary* metadata map attached to a schema's fields:

```
sem_<base32(sha256("deblob-semantic-v1\0" || canonical_semantic_bytes))>
```

Same structure + different unit/namespace/privacy-class/enum-meaning → different `sem_`. The metadata is drawn **only** from registered, validated vocabularies (unit codes, identifier namespaces, canonical field IDs, privacy classes, enum-value meanings). **Never free prose** — free text would be an injection/exfiltration channel and would make the digest non-deterministic and non-governable.

### Core invariants (unchanged from P1)

- **Bias false-split over false-merge.** The semantic fingerprint can only ever *add* a distinction (split); it can never cause two families to merge. A missing/unknown semantic annotation never collapses two meanings into one.
- **`sch_` is what rides on messages.** In P2-D the wire tag is unchanged — content identity still tags. `sem_` is vault/governance metadata; it is not a message header in this sub-project.
- **Operator-gated, like promotion.** Semantic metadata is declared through the authenticated, audited management API. Producers never supply it. The SLM (shadow) may *propose* it into the shadow log; only an operator applies it.
- **Off by default / genuine no-op.** An un-annotated schema behaves exactly as it does today: `sem_` is absent, hot path unchanged, every existing test unchanged.

## 2. Non-goals (P2-D)

- No free-prose semantics anywhere in the digest preimage or storage.
- No SLM auto-application — the shadow lane proposes, an operator applies.
- No automatic family split/merge from a semantic-fingerprint change (that is the P3 live gate, same posture as the SLM go-live gate). P2-D **computes and surfaces** the drift signal; it does not act on it.
- No semantic tag on the wire (no `deblob-semantic-id` header). Message tagging stays `sch_`/`cand_`.
- No embeddings, no learned semantics.
- No back-fill inference of meaning for historical schemas. Un-annotated stays un-annotated (`sem_` = none).

## 3. The controlled vocabulary (`deblob-semantic-v1`)

Semantic metadata is a map from **canonical field path** (the same path the structural canonicalizer already produces, so the two identities share a coordinate system) to a `FieldSemantics` record. Every attribute is optional, and every present attribute is drawn from a **registered, validated set** — an unknown value is rejected at the API boundary (`422`), never silently stored.

`FieldSemantics` attributes (all optional; a field may carry any subset):

| Attribute | Domain | Example |
|---|---|---|
| `unit` | registered unit codes (a fixed, versioned table) | `celsius`, `fahrenheit`, `usd_minor`, `eur_minor`, `bytes`, `ms` |
| `identifier_namespace` | registered namespaces | `user`, `order`, `device`, `tenant` |
| `canonical_field_id` | registered canonical field IDs (governance-owned) | `cfid_temperature_ambient` |
| `privacy_class` | enum | `public`, `internal`, `pii`, `secret` |
| `enum_semantics` | map of the field's structural enum values → registered meaning codes | `{"A":"active","I":"inactive"}` |

- The vocabulary tables (`unit`, `identifier_namespace`, `privacy_class`, meaning codes) are **part of the binary** for `deblob-semantic-v1` — deterministic, versioned, reviewable. `canonical_field_id` values are governance-registered (a separate registered set the operator manages) but validated the same way: an id not in the registry is rejected.
- Field paths in the metadata map **must exist** in the schema's structural canonical form. Annotating a path the structure doesn't contain is `422` — the two identities cannot drift out of coordinate.
- The vocabulary version string (`deblob-semantic-v1`) is part of the digest preimage; a future `v2` is a new domain, never a silent reinterpretation.

## 4. The digest

```
canonical_semantic_bytes = deterministic canonical serialization of the metadata map:
  - field paths sorted by code point
  - within each field, attributes emitted in a fixed key order
  - enum_semantics entries sorted by enum value (code point)
  - absent attributes omitted (not emitted as null) so encoding is minimal + stable
sem_id = "sem_" || base32_nopad_lower(sha256("deblob-semantic-v1\0" || canonical_semantic_bytes))
```

- **Empty metadata → no fingerprint.** A schema with zero annotations has `semantic_fingerprint = None` — represented as the sentinel `SemanticFingerprint::None`, **not** a hash of the empty map. An un-annotated schema makes *no semantic-identity claim*; it must never collide with another un-annotated schema as if they were "the same meaning." (Two un-annotated schemas are semantically *unknown*, not semantically *equal*.)
- Determinism is a golden-test invariant: the same metadata map always yields the same `sem_`, and any change to any attribute changes it.
- The digest is domain-separated from `sch_` (`deblob-semantic-v1` vs `deblob-canon-v1`) so a `sem_` can never be confused with or parsed as a `sch_`.

## 5. Identity relationship

| Identity | Digest domain | Coordinate | Rides on wire? | Set by |
|---|---|---|---|---|
| `sch_` content | `deblob-canon-v1` | canonical structure | yes (P1) | deterministic |
| `fam_@v` family | uuidv7 + version | governance | no | promotion (operator) |
| `sem_` semantic | `deblob-semantic-v1` | controlled metadata over the SAME field paths | **no** (P2-D) | annotation (operator) |

- A `(sch_, sem_)` pair is the full physical+semantic identity of a schema record. Two records with the same `sch_` but different `sem_` are *the same structure, different meaning* — the exact case P2-D exists to make visible.
- The semantic fingerprint is stored **on the immutable schema record**. Once a schema is annotated and published with a `sem_`, that binding is write-once and byte-compared on any re-publish, exactly like `canonical` (mismatch is fatal, not dedupe — §6).

## 6. Storage & migration

- `SchemaRecord` gains: `semantic: Option<SemanticMetadata>` and `semantic_fingerprint: Option<SemanticId>`. Both `None` for every schema that predates annotation.
- The annotation write goes through the **same atomic Lua transition** as publication: metadata + recomputed `sem_` + an audit event, all-or-nothing. A partial annotation after a crash is impossible.
- **Non-destructive migration:** existing schema records read back with `semantic = None`, `semantic_fingerprint = None`. No rewrite of historical records. `rebuild_index` and the consistency checker treat the semantic fields as pass-through (they are not part of the structural bucket index).
- **Immutability:** the schema hash's byte-compare (already fatal on `canonical`/`canonicalizer` mismatch) is extended to cover the stored semantic metadata + `sem_`. Re-annotating a schema with *different* semantics is a governed operation that produces a new audit event and is subject to the same "corrections = new intent, never silent overwrite" rule — an annotation change is an explicit, audited `PUT`-with-reason, not an idempotent re-`POST`.
- A new Redis key or a field on the existing schema hash holds the semantic block; it participates in `rebuild_index` restore so a rebuilt vault preserves `sem_` bindings.

## 7. Governance API (`deblob` bin)

On the **management port** (never the ingest path), authenticated + audited — mirrors promotion exactly.

```
GET  /api/v1/schemas/{sch_id}/semantic              → current metadata + sem_ (404 if none)
PUT  /api/v1/schemas/{sch_id}/semantic              → declare/replace annotation (authenticated, audited, reason required)
                                                        201/200 + the computed sem_ ; 422 on unknown vocabulary
                                                        or a field path absent from the structure ; 409 on immutability conflict
GET  /api/v1/semantic/{sem_id}                      → schemas carrying this semantic fingerprint
```

- Request body validated against the `deblob-semantic-v1` vocabulary with `deny_unknown_fields`; unknown unit/namespace/privacy-class/meaning code → `422` with the offending token (a registered token, never echoed free text).
- Reserved-input hygiene: any producer-supplied semantic hint on the ingest path is stripped, exactly like other reserved `deblob-*` input. Semantics enter **only** through this authenticated endpoint.
- Every annotation writes an audit record: actor, reason, prior `sem_`, new `sem_` — the same administrative-boundary treatment as `promote`.

## 8. Semantic-drift signal (computed, not acted on)

- When a family gains a new **structurally-compatible** version (same family, per the P1 drift policy) whose annotated `sem_` differs from the prior version's `sem_`, Deblob records a `semantic_drift` observation: prior `sem_`, new `sem_`, family, versions.
- In P2-D this signal is **surfaced only** — a Prometheus counter, an entry queryable via the API, and a field on the family version record. It does **not** auto-split the family. Acting on it (auto-splitting on semantic drift) is the P3 live gate, documented but not automated, and requires the same evidence discipline as the SLM go-live gate.
- The signal is one-directional: it can flag "these two compatible versions mean different things" (a split candidate). It never flags a merge.

## 9. Crates / structure

| Crate | Change |
|---|---|
| `deblob-core` | `SemanticId` (`sem_` via the existing `digest_id!` macro); `SemanticMetadata` / `FieldSemantics` / `SemanticFingerprint` (`None` vs `Some`) types; `SchemaRecord` gains the two optional fields. |
| `deblob-semantic` (new) | the `deblob-semantic-v1` vocabulary tables + validation, the canonical serialization, and `semantic_fingerprint(metadata) -> SemanticFingerprint`. Pure, no I/O — the deterministic core, golden-tested like `deblob-fingerprint`. |
| `deblob-redis` | store/read the semantic block on the schema hash via the atomic Lua path; byte-compare immutability; `rebuild_index` pass-through; a `sem_` → schemas reverse lookup. |
| `deblob` (bin) | the governance API endpoints; the `semantic_drift` observation + metric; wiring. Un-annotated behavior byte-identical to today. |
| reuse | the structural canonical field paths (shared coordinate system), the audit path, the management-API auth. |

## 10. Error handling

| Condition | Behavior |
|---|---|
| Unknown vocabulary token (unit/namespace/privacy/meaning/cfid) | `422`, offending registered token named, nothing stored |
| Field path not present in the structure | `422`, path named, nothing stored |
| Free-prose where a code is required | rejected by the typed vocabulary (no free-string field exists) |
| Re-annotation with different semantics | audited governed change (reason required); immutability byte-compare distinguishes it from an idempotent replay |
| Empty metadata | `sem_` = `None`; no semantic-identity claim |
| Producer-supplied semantic hint on ingest | stripped (reserved-input hygiene) |
| Redis down | annotation refused (fail closed, like promotion); reads degrade per P1 |

## 11. Testing strategy

TDD (80%+). The digest crate is golden-tested for determinism and sensitivity: same map → same `sem_`; any single-attribute change → different `sem_`; empty map → `None` (not a hash). The false-merge trap is explicit: two schemas with **identical structure**, one annotated `unit=celsius` and one `unit=fahrenheit`, must produce **different** `sem_` (the headline case). Immutability: re-publishing a schema with mismatched stored semantics is fatal. Migration: a pre-P2-D schema record reads back as `None`, and `rebuild_index` preserves `sem_` for annotated ones. API: unknown vocabulary → `422`; a valid annotation → the expected `sem_`; producer-supplied semantic input on ingest is stripped. Drift: a compatible version with a changed `sem_` raises the `semantic_drift` counter and does **not** split the family.
