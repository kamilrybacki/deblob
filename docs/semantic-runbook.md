# Semantic-fingerprint (P2-D) runbook

Accurate to P2-D as built (Tasks 1-10 + the Task 8 capstone). Cross-references:
[the P2-D design spec](superpowers/specs/2026-07-15-deblob-p2-semantic-fingerprint.md)
if present, `docs/superpowers/plans/deblob-p2d-hermes-review.md` and
`deblob-p2d-02-hermes-similarity.md` (authoritative amendments), and
`crates/deblob-semantic/`, `crates/deblob-redis/src/semantic.rs`,
`crates/deblob/src/api/semantic.rs`, `crates/deblob/src/semantic_drift.rs`,
`crates/deblob/src/semantic_neighbors.rs`. Mirrors `docs/runbook.md`'s style
and scope discipline: a documented behavior that is only design intent, not
wired code, is called out explicitly here, never presented as fact.

## What `sem_` is, in one paragraph

`sch_` (P1) is a pure function of a payload's *structure* — field names and
types, never values. Two payloads that mean completely different things
(a `temperature` in Celsius vs Fahrenheit) can be structurally identical and
therefore share one `sch_`. `sem_` is a second, orthogonal identity computed
over *controlled metadata only* — never free prose — that an operator
attaches to a schema through an authenticated governance API. Same `sch_`,
different `sem_` is the entire point of this feature; it never merges or
splits anything on its own.

## The vocabulary

Every attribute in a `FieldSemantics`/`SemanticMetadata` annotation is a
typed code, never a string an operator can just make up. There are two
different kinds of vocabulary:

**Baked, versioned tables** (`crates/deblob-semantic/src/vocab.rs`,
`VOCAB_VERSION = "deblob-semantic-v1"`) — ship inside the binary, immutable
within a protocol version:
- `unit` — UCUM codes (`Cel`, `[degF]`, `K`, `kg`, `mL`, ...) or ISO 4217
  currency codes (`USD`, `EUR`, `PLN`, ...), namespaced as
  `{system: "ucum"|"iso4217"|"registered", code: "..."}`. UCUM codes are
  **case-sensitive** — `Cel` and `cel` are different tokens; never normalize.
- `identifier_namespace` — a curated table of namespace codes
  (`acme.customer_id`, `iso.country_code`, ...).
- `enum_semantics` — each schema-observed enum VALUE maps to a
  `MeaningCode {vocabulary, code}`; only the `vocabulary` NAMESPACE is
  checked against a registered table (`deblob/order-status/v1`, ...) — the
  individual `code` within a registered vocabulary is trusted.

Extending any baked table is a NEW protocol version (`deblob-semantic-v2`),
never an in-place edit — this is what lets a `sem_` computed today stay
valid forever.

**Operator-registered, per-deployment** — `canonical_field_id` and
`canonical_event_type_id`. Unlike the tables above, there is no fixed
external standard for these (they're your own domain vocabulary:
`temperature.ambient`, `order.created`, ...), so they default to **empty**
— every value is rejected (`422`) until explicitly registered.

### Registering a `canonical_field_id` / `event_type`

Add a `[semantic]` section to the TOML config (`deblob.example.toml`'s
commented block, `crates/deblob/src/config.rs::SemanticConfig`):

```toml
[semantic]
canonical_field_ids = ["temperature.ambient", "order.total_amount"]
event_types = ["order.created", "order.cancelled"]
```

- Absent section, or present with both lists empty (the default) — every
  `canonical_field_id`/`event_type` annotation `422`s, exactly as if the
  feature didn't exist. This is a **non-secret**, plain, reviewable
  governance list — never an env var, never a credential.
- There is **no registration API endpoint** — an operator edits the file and
  restarts `deblob` (or redeploys the config). `serve()` builds the
  injectable `deblob_semantic::Registries` from this section once at
  startup (`SemanticConfig::to_registries`) and threads it into
  `ApiState.semantic_registries`; it is never mutated at runtime.
- A `422` from `PUT .../semantic` always names the ONE offending registered
  token (unit code, namespace, field id, event type, or meaning-vocabulary
  name) — never free text, never the whole request echoed back.

## The revision API: ETag / `If-Match` / reason codes

Semantic metadata lives in an **append-only revision log** per `sch_id`,
plus a mutable "active" pointer — never a mutable block on the immutable
schema record. Every write is a single atomic Redis Lua transition.

```
GET  /api/v1/schemas/{sch_id}/semantic              -> active metadata + sem_ + ETag header (404 if never annotated)
GET  /api/v1/schemas/{sch_id}/semantic/revisions     -> full history, oldest first (empty list if never annotated)
PUT  /api/v1/schemas/{sch_id}/semantic               -> declare/replace the active annotation
GET  /api/v1/semantic/{sem_id}                       -> every schema whose ACTIVE sem_ is this one
```

`PUT` body:

```json
{
  "metadata": { "event_type": "...", "fields": [ ... ] },
  "reason_code": "correction",
  "reason": "converted the US feed to Fahrenheit"
}
```

- `reason_code` — one of `correction`, `ontology_upgrade`, `policy_review`,
  `source_contract_change`, `operator_override`. Defaults to `correction`
  when absent.
- `reason` — required text, but ONLY when the write is a genuine change.
- **Idempotent replay**: if `metadata`'s canonical bytes are byte-identical
  to the currently active revision, the call succeeds (`200`, no new
  revision) and skips the reason/`If-Match` checks entirely — a retried
  identical PUT is always safe.
- **Genuine change** requires BOTH a non-empty `reason` (else `400
  MissingReason`) and a correct `If-Match` header carrying the CURRENT
  ETag (absent `If-Match` means "I believe this schema was never
  annotated", i.e. ETag `0`). A stale/wrong `If-Match` is `409
  EtagConflict` — the response names the current ETag so the caller can
  retry with it.
- A successful genuine change is `201` with the new `sem_` and the new
  `ETag` header (`"<n>"`, quoted). The active pointer moves; the PRIOR
  revision is never deleted or edited and stays independently readable via
  `GET .../semantic/revisions`.
- Unknown vocabulary token, or a field path not present in the schema's own
  structural canonical form -> `422`, naming the offending token/path,
  nothing stored.

## `privacy_class` is separate governance, not part of `sem_`

`privacy_class` (`public`/`internal`/`pii`/`secret`) lives on the
`SchemaRecord` itself (`deblob_core::ports::SchemaRecord::privacy_class`),
**not** inside `FieldSemantics`/`SemanticMetadata`, and is therefore **never
part of the `sem_` digest preimage**. Two schemas that assert identical
`unit`/`canonical_field_id`/etc. but different `privacy_class` still share
one `sem_` — this is intentional: privacy classification varies by
jurisdiction/tenant/policy version without the field's *meaning* changing,
and mixing it into the meaning-identity digest would make `sem_` churn on
governance/compliance changes that have nothing to do with what the data
means.

## `None` -> `Some` is never drift; a changed `Some` is

An un-annotated schema's `semantic_fingerprint` is the real sentinel
`SemanticFingerprint::None` — never a hash of an empty map, and never
treated as "equal" to another un-annotated schema (two unknowns are not the
same known thing). This distinction matters for the drift signal
(`deblob_semantic_drift_total`, `crate::semantic_drift::
detect_semantic_drift`):

- A family version gaining its FIRST annotation (`None -> Some`) is **never**
  drift, regardless of what the new `sem_` is.
- A version LOSING its annotation (`Some -> None`) is **never** drift.
- `None -> None` (neither version ever annotated) is **never** drift.
- Drift fires ONLY when TWO ALREADY-ANNOTATED, adjacent, structurally
  COMPATIBLE family versions carry DIFFERENT active `sem_`s. It is
  proposal-only: the counter increments and nothing else happens — no
  auto-split, no alias, no mutation of the family/schema/`sem_` state (see
  [Deferred to P3/P4](#deferred-to-p3p4-gates) below).

The companion same-`sem_`/different-`sch_` diagnostic
(`deblob_semantic_collision_total{strength}`) is the mirror case: two
DIFFERENT schemas whose active metadata hashes to the SAME `sem_`. Strength
(`strong`/`medium`/`weak`) is based on shared `canonical_event_type_id` plus
`canonical_field_id` annotation coverage on the WEAKER of the pair; only
`strong`/`medium` are review candidates, `weak` is logged and discarded.
Also strictly diagnostic — it never merges or aliases the two schemas.

**Both diagnostics are wired into the real annotation path** (P2-D Task 8
follow-up): every genuine (non-idempotent) `PUT .../semantic` write scans
the reverse index for its landed `sem_` (collision) and, if the schema's
family has an adjacent lower version, compares active `sem_`s (drift) —
`crates/deblob/src/api/semantic.rs::put_semantic`. Both calls are read-only
best-effort: a failure to COMPUTE a diagnostic is logged and never fails
the annotation write that already succeeded.

## `semantic-neighbors`: diagnostic-only similarity search

```
GET /api/v1/schemas/{sch_id}/semantic-neighbors?k=&include_historical=
```

Path-independent: two schemas asserting the same `canonical_field_id`/
`unit`/`event_type` combination score identically regardless of the
literal field NAME — this is what makes it useful for spotting a renamed
field carrying the same meaning. Response:

```json
{
  "data": {
    "query_schema": "sch_...",
    "signature_version": "deblob-semantic-signature-v1",
    "weights_version": "deblob-semantic-signature-weights-v1",
    "neighbors": [
      {"schema_id": "sch_...", "semantic_revision_id": "rev_...",
       "score": {"numerator": 7, "denominator": 8, "decimal": "0.875000"},
       "strength": "strong", "shared_anchor_count": 2,
       "matched_feature_classes": ["canonical_event_type_id", "canonical_field_id"]}
    ],
    "authority": "diagnostic_only"
  }
}
```

- `authority` is ALWAYS the literal string `"diagnostic_only"` — a neighbor
  is a candidate, never a claim of equivalence, and nothing in this codebase
  can turn a neighbor result into a merge/alias/split.
- `k` defaults to 10, clamped (never rejected) at 50 — an over-large `k` is
  caller carelessness on a best-effort diagnostic endpoint, not a malformed
  request.
- The query schema's own signature must carry at least one ANCHOR feature
  (`canonical_field_id`/`canonical_event_type_id`/`identifier_namespace`);
  otherwise the response is a `NoAnchor` outcome (empty `neighbors`,
  `strength: "insufficient"`) rather than expanding toward the whole vault.
- If the bounded candidate union exceeds the configured cap, the response is
  `422 signature_too_broad` — never a silently truncated top-`k`.
- **`include_historical` limitation**: the query parameter is accepted (a
  non-breaking future wire contract) but is **always treated as `false`**.
  This codebase has exactly one authentication tier (a single shared bearer
  token, no role/scope system), and the spec gates
  `include_historical=true` to "auditors" — a concept that does not exist
  here. Until an auditor-scope mechanism is added, historical (superseded)
  revisions are **not queryable** through this endpoint at all, regardless
  of what the caller passes.

## Fixed: annotation now supports both canonicalizer grammars

**Originally discovered by the Task 8 capstone** (`crates/deblob/tests/
semantic_capstone_it.rs`'s module doc comment has the full incident
history) and **fixed by the Task 8 follow-up**
(`crates/deblob-semantic/src/path.rs`, `crates/deblob/src/api/
semantic.rs`) — recorded here because the original gap was
operator-visible, not just an implementation footnote.

A `SchemaRecord::canonical` string is written in one of TWO grammars,
selected by `SchemaRecord::canonicalizer`:

- **`"deblob-canon-v1"`** (`deblob_fingerprint::canon::CANONICALIZER`): the
  plain shape tree — `{"t":"obj","f":{...}}` / `{"t":"arr","of":[...]}` /
  `{"t":"null|bool|num|str"}`.
- **`"deblob-monoid-v1"`** (`deblob_monoid::profile::GENERALIZER`): the
  generalized field-statistics tree every `Promoter::promote`d/PROMOTED
  schema actually carries — `{"optional":bool,"types":[...],
  "children":{...},"elem":{...}}`, rooted at the bare field body itself
  (the `{"gen":...,"fields":...}` framing only ever appears in
  `generalized_fingerprint`'s hash preimage, never in the persisted
  `canonical` string).

`deblob_semantic::path::canonical_field_paths_for(canonicalizer,
canonical)` dispatches on the schema record's OWN `canonicalizer` to pick
the matching walker, and `api::semantic::put_semantic` calls it with
`record.canonicalizer` instead of assuming the plain grammar. Path
enumeration is grammar-equivalent either way: an object's fields (`"f"` /
`"children"`) each contribute one `Key(name)` segment; an array's elements
(`"of"` / `"elem"`) contribute one shared `Wildcard` segment;
`types`/`optional` never affect the enumerated path set. An unrecognized
`canonicalizer` string is a named `PathError::UnknownCanonicalizer`,
surfaced as `422` — never a silent accept.

**Practical effect:** a schema published through real candidate promotion
(`POST /candidates/{id}/promote`) can now be annotated exactly like a
hand-published plain-canonicalizer schema — `PUT .../semantic` succeeds for
a path that actually exists in the promoted schema's generalized field
structure, and still `422`s for one that doesn't. Proven end to end against
a real `ColdLane::ingest` -> `Promoter::promote` -> `PUT .../semantic` HTTP
call over real Redis in `crates/deblob/tests/
semantic_monoid_promoted_it.rs`; `crates/deblob/tests/
semantic_capstone_it.rs` still hand-publishes plain-canonicalizer fixture
schemas (an intentional, orthogonal test-isolation choice for that suite,
not a workaround for this gap anymore).

## Deferred to P3/P4 gates

The following are explicitly **NOT** implemented, by design, in P2-D — every
one of them is a computed-and-surfaced SIGNAL at most, never an automated
action:

- **Auto-split/merge on semantic drift or collision.** Both diagnostics
  (`deblob_semantic_drift_total`, `deblob_semantic_collision_total`) are
  proposal-only. Acting on them (splitting a family because its versions'
  `sem_`s diverged, or merging schemas because they share one `sem_`)
  requires the SAME evidence discipline as the SLM go-live gate
  (`docs/shadow-golive-gate.md`) and is the P3 live gate — documented intent,
  not automated code, anywhere in this codebase.
- **Full temporal semantics** — `Temporal` today covers only `kind`
  (instant/local-datetime/date/duration), `epoch`, and `resolution` (the
  epoch-seconds-vs-milliseconds false-merge). Timezone machinery
  (`encoding`, `timezone_policy`, `timezone`, `timezone_field`) is
  explicitly deferred to P4.
- **Coordinate semantics** — no lat/lon/CRS/datum axis exists at all yet.
- **`semantic_groups`** (cross-field semantic relationships, e.g. "these
  three fields together form one address") — not modeled.
- **A rename signature beyond path-independence** — the neighbor search
  (Task 9/10) already scores path-independently via controlled-vocabulary
  features, but there is no dedicated "this is definitely the same field,
  just renamed" classifier beyond that similarity score.
- **An `effective_from` resolver** — `Revision::effective_from` is stored
  and readable per-revision, but nothing in this codebase resolves "which
  revision was active at time T" as a query; only the CURRENT active
  revision is directly queryable.
- **Embeddings / learned semantics** — P2-D is exact, deterministic,
  controlled-vocabulary only. No embedding model, no fuzzy matching beyond
  the weighted-Jaccard signature score, anywhere in this dimension.

## Metrics (semantic-specific additions to `docs/runbook.md`'s table)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `deblob_semantic_drift_total` | counter | none | A structurally-compatible family re-version's active `sem_` changed. Proposal-only. |
| `deblob_semantic_collision_total` | counter | `strength` (`strong`\|`medium`\|`weak`) | Two schemas found sharing one active `sem_`, by annotation-coverage strength. Proposal-only. |

Both are pre-touched at startup (every bounded label value registered at
`0`), matching every other counter in this codebase's metrics surface.
