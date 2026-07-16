# P2-D — Hermes review deltas (`deblob-p2d-01`), authoritative

Folded from Hermes' design review (verdict: **approve with changes**). These OVERRIDE the corresponding text in `2026-07-15-deblob-p2d-semantic-fingerprint.md` and the AMEND markers in `2026-07-15-deblob-p2d.md`. Concrete values are binding for the build.

## Scope: P2-D adopts vs P4 defers

`deblob-semantic-v1` is a **versioned, additive** vocabulary — the digest treats an absent attribute as absent, so a later version adds an axis WITHOUT re-hashing fingerprints that don't use it. That lets P2-D adopt the correctness-critical axes now and defer the heaviest without churn risk.

**Adopted in P2-D:**
- Schema-level `canonical_event_type_id` (the largest omission — `user.created` vs `user.deleted`, same fields).
- Namespaced `unit { system: ucum|iso4217|registered, code }` (UCUM case-sensitive; currency via ISO4217, never proprietary `currency.usd`).
- `numeric.scale` (scale changes meaning: stored `1234` = `12.34`). NOT precision (precision is physical/`sch_`).
- Minimal `temporal { kind, epoch, resolution }` — covers the common epoch-seconds-vs-milliseconds false-merge. (Full timezone machinery deferred.)
- `identifier_namespace`, `canonical_field_id`, `enum_semantics` (kept, with the encoding rules below).
- **`privacy_class` REMOVED from the intrinsic `sem_`.** It is governance metadata (varies by jurisdiction/tenant/policy-version without meaning changing). It becomes a SEPARATE governance annotation on the schema, NOT part of the digest preimage.
- Typed path segments + strict canonical bytes (§2 below).
- Append-only semantic assertion revisions + active pointer (§4 below).
- Reverse `sem_ → sch_` index + `same_semantic_fingerprint_different_structure` diagnostic (§5 below).
- Annotation-coverage tracking (a sparse fingerprint must not read as strong semantic evidence).

**Deferred to P4 (documented, additive later):**
- Full `temporal` timezone machinery (`encoding`, `timezone_policy`, `timezone`, `timezone_field`).
- `coordinate { crs, axis_role }`.
- Cross-field `semantic_groups` (money = amount+currency, coordinate_tuple = lat+lon, interval start/end, value+unit, composite id).
- Path-independent semantic signature for rename retrieval.
- Ingestion-time-aware `sch_` resolution (no consumer in P2-D — `sem_` is not on the wire).

## 1. Axis set (AMENDS spec §3, plan Tasks 1–2)

Final P2-D semantic axes:
```
Schema:  canonical_event_type_id
Field:   canonical_field_id, identifier_namespace,
         unit { system, code },
         numeric_scale (signed integer),
         temporal { kind, epoch, resolution },
         enum_semantics
```
- `privacy_class` is a SEPARATE governance field on the schema record, never hashed into `sem_`.
- All optional, but the schema exposes **annotation coverage** (fraction of leaf fields carrying ≥1 axis; whether `canonical_event_type_id` is present). Coverage is metadata for the strength classifier (§5) — never part of the digest.

## 2. Canonical serialization (AMENDS spec §4, plan Tasks 3–4)

`deblob-semantic-v1` is a **byte-level protocol**, not generic JSON hashing.

- **Paths are typed segments**, not dotted strings: `["a", "b.c", "items", "*", "id"]`. The array wildcard is a distinct typed segment, not the literal string `"*"`. UTF-8 only; Unicode **NFC**; reject invalid surrogates, NUL, control chars; no locale normalization; no case folding; detect duplicate paths AFTER normalization. Path encoding shares the `sch_` canonicalizer version.
- Sort paths by canonical encoded bytes (not language map order).
- Fixed attribute order defined by the protocol. Missing attribute = absent; explicit `null` is INVALID; empty nested map normalizes to absent; a field entry with no attributes is removed; an empty final map → `None` (not bytes). **No defaults in v1** (defaults create absent-vs-explicit ambiguity; if ever needed, the API expands them before hashing and it becomes a new protocol version).
- **Enum:** sort entries by canonical **typed value bytes** (`integer 1` ≠ `string "1"`, `boolean true` ≠ `string "true"`); canonicalize numeric enum values via the P1 numeric protocol so `1`/`1.0`/`1e0` don't vary. Meaning codes carry their immutable namespace+version: `{ vocabulary: "deblob/order-status/v1", code: "pending" }`.
- **Numbers** (scale/epoch/offsets): NEVER via float — signed canonical integers, canonical decimal strings, or reduced rational pairs.
- **Vocabulary stability:** a registered code must never be redefined. Every vocabulary artifact is immutable + version-addressed; its namespace/version (or artifact digest) is part of the canonical bytes. Otherwise identical `sem_` bytes could silently acquire new meaning after a table update.
- **Domain separation:** preimage is exactly `"deblob-semantic-v1\0" || canonical_semantic_bytes`. Do **NOT** include `sch_` in the preimage (that would prevent detecting the same semantic assertion across different physical schemas — the §5 signal). Store the canonical bytes alongside the hash; byte-compare on replay/collision.

## 3. None sentinel (AMENDS spec §4/§5, plan Tasks 1 + 3) — strong agreement

Unannotated must NOT hash to a shared value. Represent it as `Option<SemanticFingerprint>` (where `SemanticFingerprint` is the `sem_` itself — drop the `SemanticFingerprint::None` enum variant built in Task 1; use `Option`). API emits `"semantic_fingerprint": null`. Storage: no fake `sem_none`, no all-zero digest, no hash-of-empty, **no reverse-index entry**, no equality/grouping inference between two unannotated schemas. Only create a `sem_` when ≥1 canonical semantic assertion survives normalization.

**→ Task 1 revision required:** replace `SemanticFingerprint { None, Some }` with a `SemanticFingerprint(SemanticId)` (or keep `SemanticId` directly) returned as `Option<...>`; remove `privacy_class` from `FieldSemantics`; make `unit` the namespaced struct; add `canonical_event_type_id` (schema-level), `numeric_scale`, `temporal`; move `privacy_class` to a separate governance field on the schema record.

## 4. Re-annotation — append-only revisions (AMENDS spec §6/§7, plan Tasks 5–6)

Do NOT write-once; do NOT mint a new `sch_` for a semantic correction (`sch_` is physical identity — correcting meaning must not change it). Also do NOT mutate semantic bytes in place. Instead:

```
immutable schema artifact:   sch_...
append-only revisions:       revision_id, sch_id, sem_id, canonical_semantic_bytes,
                             previous_revision_id, actor, reason_code, reason,
                             recorded_at, effective_from, status
mutable transactional ptr:   active_semantic_revision
```
`PUT /schemas/{sch}/semantic` behavior:
- Same canonical bytes as active revision → idempotent `200`, no new revision.
- Different bytes without `reason` → `400`.
- Different bytes without a matching `If-Match` revision/ETag → `409`.
- Different bytes + reason + correct ETag → append a new immutable revision and atomically move the active pointer.
- Never delete/overwrite a prior revision.

Controlled `reason_code` ∈ `{ correction, ontology_upgrade, policy_review, source_contract_change, operator_override }`.

Store `effective_from`; corrections default **non-retroactive**; a retroactive `effective_from` needs a stronger auth path + explicit audit event. **P2-D stores `effective_from` but does NOT build the ingestion-time-aware `sch_` resolver** (no consumer while `sem_` is off the wire) — that resolver is P4.

The schema artifact stays immutable; revisions are immutable; only the active pointer advances. This replaces the earlier "semantic block byte-compared on the immutable schema record" model.

## 5. Same `sem_`, different `sch_` (AMENDS spec §8, plan Task 7)

Flag in P2-D via the reverse index `sem_ → {sch_a, sch_b, ...}`; emit `same_semantic_fingerprint_different_structure`. Classify deterministically: compatible structures → possible rename/version/false-split; incompatible → representation change / bad annotation / drift; identical paths + changed types → high-value review case. **Diagnostic hint only** — attributes are optional, so identical sparse annotations do NOT prove equivalence.

Strength levels:
```
strong:  same canonical_event_type_id AND ≥80% leaf fields have canonical_field_id AND semantic groups agree
medium:  same event type + partial field coverage
weak:    only units/namespaces/enum-meanings / sparse overlap
```
Only strong/medium create a review candidate; weak is logged for evaluation, never a family link. **No alias, merge, or family mutation in P2-D.** Because `sem_` is path-bound, a pure field rename normally changes `sem_` — the path-independent rename signature is P4; do NOT weaken the authoritative `sem_` to get it.

## Required review deltas (checklist)

1. Add `canonical_event_type_id`. 2. Add controlled `numeric.scale` + minimal `temporal` (coordinate + cross-field groups → P4). 3. Remove `privacy_class` from intrinsic `sem_`. 4. Typed path segments + byte-level canonical protocol (not JSON). 5. Vocabulary artifacts immutable + version-addressed. 6. Unannotated = true `None`/`Option`. 7. Append-only revisions + active pointer (not schema-record mutation). 8. Reverse `sem_ → sch_` diagnostic index now. 9. Same-`sem_` findings proposal-only, never auto-merge. 10. Track annotation coverage so sparse fingerprints can't masquerade as strong identity.
