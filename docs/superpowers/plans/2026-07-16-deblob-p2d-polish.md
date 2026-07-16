# Deblob P2-D Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Close the three documented P2-D follow-ups so the feature is fully finished: drift compares ALL family versions, the `GET /families*` 501 stubs become real reads, and the enum-value key becomes typed so `"true"` (string) ≠ `true` (bool) in the digest.

**Architecture:** Small, mostly-independent fixes on top of merged P2-D. Reuse existing Registry/Redis/API patterns; no new crates.

## Global constraints

- Every existing test stays green; `sem_` stays off-wire; diagnostics stay strictly diagnostic (zero mutation).
- Task 3's identity determinism/injectivity invariants hold (Task P3 changes the enum ENCODING, proven by goldens — no prod `sem_` exists to migrate).
- Management endpoints stay authenticated; `422`/error bodies echo only bounded tokens.
- TDD; 80%+.

---

### Task 1: drift compares all prior family versions

**Files:** `crates/deblob/src/api/semantic.rs` (~line 292), `crates/deblob/src/semantic_drift.rs`.

**Produces:** the drift check on a new annotation compares the new version's active `sem_` against EVERY prior version that has an active `sem_` (not only `version - 1`). Loop versions `1..record.version` via `Registry::family_version_schema`; for each prior with an active `sem_`, run `check_family_version_drift`. A drift finding fires + the counter increments if ANY structurally-compatible prior version has a differing `sem_`. `None`→`Some` (a prior with no `sem_`) still is not a drift. No change to the zero-mutation guarantee.

- [ ] Failing test: family with versions 1,2,3; v1 and v3 annotated with DIFFERENT compatible `sem_`, v2 unannotated; annotating v3 detects drift vs v1 (the non-adjacent case the old code missed) — `deblob_semantic_drift_total` increments; a same-`sem_` case across all versions → no drift.
- [ ] Run → implement → run.
- [ ] Commit `fix(bin): drift compares all prior family versions, not just the adjacent one`.

---

### Task 2: real `GET /families/{fam_id}` and `/versions`

**Files:** `crates/deblob-core/src/ports.rs` (Registry trait), `crates/deblob-redis/src/registry.rs` (impl), `crates/deblob/src/api/schemas.rs` (~lines 63-73), any fake `Registry` impls (grep `impl Registry`).

**Produces:** two Registry trait methods — `get_family(&self, fam_id) -> Result<Option<FamilyRecord>, CoreError>` (the family metadata stored at `deblob:family:<fam_id>` — name, current version, state/compat as stored) and `list_family_versions(&self, fam_id) -> Result<Vec<FamilyVersion>, CoreError>` (the append-only version list). Redis impl reads the existing family key/structures (inspect what `publish`/`family_version_schema` already write — do NOT change the write path; only add reads). Wire `get_family` → `200` + family JSON / `404`; `get_family_versions` → `200` + version list / `404`. Update every fake `Registry` impl minimally. Keep cursor/error conventions consistent with `list_schemas`.

- [ ] Failing tests (real Redis, mirror `registry_it.rs` + `api_it.rs`): after publishing a family with 2 versions, `get_family` returns it and `list_family_versions` returns both; an unknown `fam_id` → `404`; the endpoints require auth (401 without bearer).
- [ ] Run → implement → run.
- [ ] Commit `feat(bin): real GET /families/{fam_id} + /versions (Registry family reads)`.

---

### Task 3: typed enum-value key (`"true"` string ≠ `true` bool in the digest)

**Files:** `crates/deblob-core/src/semantic.rs` (`FieldSemantics.enum_semantics`), `crates/deblob-semantic/src/canon.rs` (digest enum encoding), `crates/deblob-semantic/src/signature.rs` (enum features), `crates/deblob/src/api/semantic.rs` (PUT body deserialize), tests across those crates.

**Produces:** replace `enum_semantics: Option<BTreeMap<String, MeaningCode>>` with a representation that carries the enum value's JSON TYPE explicitly, so a string `"true"` and a boolean `true` are distinct keys. Use a `Vec<EnumMapping>` where `EnumMapping { value: EnumValue, meaning: MeaningCode }` and `EnumValue` is a typed enum `{ Null, Bool(bool), Number(String /* canonical decimal, P1 numeric rule */), String(String) }`, `#[serde(...)]` tagged so it round-trips unambiguously (JSON object keys can't be typed, so a list of `{value, meaning}` — document the API shape change in the runbook). Sort deterministically by (type-tag, canonical value bytes) for the digest + signature. Task 3's enum encoding uses the EXPLICIT `EnumValue` type tag (no more string-parsing to guess type); `1`/`1.0`/`1e0` still canonicalize equal within `Number`. Task 9's `enum-meaning`/`field-enum` features unchanged except sourced from the typed value.

- [ ] Failing tests: a `String("true")` mapping and a `Bool(true)` mapping to the SAME meaning code produce DIFFERENT canonical bytes / `sem_` (the headline limitation, now fixed); `Number("1")` == `Number("1.0")` == `Number("1e0")` (numeric canonicalization preserved); determinism independent of mapping input order; the PUT API accepts the new `{value:{...}, meaning:{...}}` list shape and rejects an untyped/unknown form.
- [ ] Run → implement → run.
- [ ] Commit `fix(semantic): typed enum-value key so string "true" != boolean true in sem_`.

---

## Task order

1 → 2 → 3. Reviewer checkpoint after Task 3 (it changes the identity digest encoding). Each task: fresh implementer + independent reviewer; broad review before merge.
