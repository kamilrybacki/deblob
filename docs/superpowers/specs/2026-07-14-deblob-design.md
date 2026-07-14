# Deblob — Design Specification

- **Date:** 2026-07-14
- **Status:** Approved (user + Hermes review `schema-sentry-bs-01`/`bs-02` + api-design/backend-patterns/security-review skill passes)
- **Visual overview:** https://claude.ai/code/artifact/c6b3bff6-d08b-4173-9f2f-2bb589b821c4
- **Research corpus:** Obsidian vault `research/` — On-the-Fly-Data-Inference-with-SLMs (2026), Automatic-Schema-Detection-Projects-2026, Event-Schema-Architecture-and-Governance, Prominent-Edge-On-Device-SLM-Landscape-2026, Pydantic-V2-in-Deploying-SLMs-Joint-Research-2026

## 1. Summary

Deblob is a **schema identity and discovery control plane for uncontrolled data**. A single Rust binary sits inline on a stream, deterministically fingerprints every message, tags it with a permanent schema identity in transport metadata, and routes unknown shapes into a durable, governed discovery lane where very small language models (SLMs) *propose* schema classifications and a policy layer *decides*.

It is explicitly **not**: a schema registry replacement, a stream processor, an ETL framework, a transport, a catalog UI, or an autonomous data agent. Its moat is the per-message identity decision and the governed discovery lane.

Core invariant (from research): *deterministic code establishes facts, a task-specialized SLM proposes semantic annotations, constrained decoding guarantees output shape, and policy code decides whether to accept, quarantine, or escalate.*

## 2. Locked decisions

| Decision | Value |
|---|---|
| Language | Rust, single binary, cargo workspace |
| v1 sources | Kafka topic relay (P1); HTTP push reverse proxy (P2) |
| Kafka mode | Topic relay: `topic.raw` → tag → `topic.tagged` (no wire-protocol proxy) |
| Tag placement | Kafka record headers / HTTP response headers; payload never mutated |
| Schema vault | Redis (AOF `everysec` minimum, enforced + runtime-monitored) |
| Structural inference | Minimal JSONoid-style monoid merger written in Rust (no JVM) |
| SLM runtime | llama.cpp in-process (`llama-cpp-2`), GGUF artifacts |
| Name | **Deblob** |
| Deployment | Standalone binary → container → inline between producers/consumers |

## 3. Architecture

### 3.1 Two lanes, one transaction

**Hot path** — synchronous, per-message, deterministic only, never waits on model or (on cache hit) Redis:

```
consume(topic.raw, partition p)
  → strip ALL inbound deblob-* headers          (spoofing defense)
  → bounded parse (all limits BEFORE allocation)
  → canonical structural fingerprint
  → LRU exact-match (hit ⇒ no Redis round-trip)
  → Redis bucketed structural index
  → attach exactly one deblob-schema-id header:
      sch_…        known schema
      cand_…       unknown shape (provisional)
      unresolved   registry unavailable (NOT cand_ — prevents outage candidate storm)
  → transactional produce:
      topic.tagged (partition p)
      + deblob.discovery (if unknown)           (same Kafka transaction)
  → send_offsets_to_transaction → commit
```

**Cold path** — asynchronous, consumes durable `deblob.discovery` topic, per structural cluster not per record:

```
consume(deblob.discovery)
  → cluster + debounce (optional-field variants merge under one provisional profile)
  → monoid merge → candidate profile (associative, sample-order independent)
  → retrieve top-k (3–10) nearest known families (bucketed features, no embeddings in v1)
  → SLM propose: match_schema(id, relation) | new_candidate(reason) | abstain(reason)
  → policy gate (ignores model self-confidence)
  → staged → observation window → promote (atomic Lua publication)
```

**Exactly-once scope (documented honestly):** Kafka transactions cover consume→produce→offset within the Kafka path only. HTTP forwarding, Redis writes, and downstream side effects are excluded; Redis evidence is derived data — the discovery topic is the durable source of truth. This closes the most serious pre-review gap: a crash can never emit a permanent provisional tag whose discovery evidence vanished.

### 3.2 Relay correctness rules (Hermes bs-02 §1)

- Derived topic has same partition count; produce source partition `p` → derived partition `p`. Do not rely on key routing. Record original topic/partition/offset in metadata. If layouts differ, ordering is documented as not preserved.
- Cooperative-sticky assignment; on partition revoke: pause, drain/cancel in-flight, abort open transaction before relinquishing. A task must never commit after its partition was revoked.
- Kafka tombstones (null value) are NOT malformed: explicit pass-through policy with reserved tombstone tag; compaction semantics preserved.
- Retries/replays produce **identical** tags: candidate identity, metadata, output partition all deterministic; never mint fresh `cand_`/UUID during replay.
- Kafka allows duplicate headers: strip every inbound reserved header, write exactly one canonical value. Headers stay tiny and bounded (IDs only — never schemas, reasons, or model output); tested against `message.max.bytes`.
- HTTP (P2): accept/generate idempotency key, forward downstream, documented retry contract.

### 3.3 Crate workspace

| Crate | Responsibility |
|---|---|
| `deblob-core` | Domain types + ports (traits): `SourceAdapter`, `PayloadDecoder`, `SchemaMatcher`, `SemanticInferencer`, `Registry`, `EvidenceStore`, `TagSink`, `PolicyEngine`. Zero vendor dependencies. |
| `deblob-fingerprint` | Versioned canonicalizer + hashing. Fuzzed in CI. |
| `deblob-monoid` | Mergeable structural profiles. Algebraic laws proven by proptest. |
| `deblob-kafka` | Transactional relay adapter + header TagSink (rust-rdkafka). |
| `deblob-redis` | Registry + EvidenceStore implementations. |
| `deblob-slm` | llama.cpp worker (P2+): isolated thread, deadline, fixed output grammar, decision cache. |
| `deblob-http` | P2: axum reverse proxy adapter. Trait ships in P1, implementation does not. |
| `deblob` (bin) | Config (TOML + env for secrets), wiring, policy engine, management API. |

## 4. Fingerprint & canonicalization (`deblob-fingerprint`)

- **Reject duplicate JSON object keys** (parsers silently keep last value → identical canonical forms with divergent consumer interpretation).
- **No Unicode NFC normalization of keys.** Unicode-distinct keys may be legitimate. Canonical ordering by code point, documented. Confusable keys flagged as suspicious, not merged.
- **Numbers never round-trip through f64.** Arbitrary-precision decimal text preserved. Equivalence policy: `1` / `1.0` / `1e0` are structurally equivalent (`number`); integer-ness is tracked as a monoid statistic, not a structural type split. `-0` normalizes to `0` with a monoid flag. JSON NaN/±Inf = malformed → quarantine.
- **Canonicalizer version in hash preimage:** `sha256("deblob-canon-v1\0" || canonical_bytes)`. ID: `sch_<base32(digest)>` (full 256-bit stored; prefix display-only).
- Sort only order-insensitive constructs (object keys, `required`, `enum` under defined rule); ordinary array order preserved.
- **Raw-shape hash ≠ candidate identity.** Records with different optional-field subsets cluster under one provisional profile in the cold lane before any `cand_` is minted (kills explosion at the root). Map-vs-record generalization happens only in the cold lane after observing key churn across samples — never from one record.
- Empty arrays are type-unknown; empty/non-empty evidence tracked separately. Large arrays: bounded inspection with explicit `partial` flag — no homogeneity claims from a prefix.
- All limits enforced **before** tree allocation: compressed + uncompressed bytes, depth, fields/object, key/string length, array elements inspected, parse deadline. Streaming parse/hash where possible.
- P1 formats: **strict JSON only.** CSV/XML/Avro/Protobuf deferred (different identity + sampling semantics).

## 5. Identity model

Three linked but independent identities:

1. **Content identity** — `sch_<base32(sha256(canon))>`: immutable; corrections = new IDs. This is what rides on messages.
2. **Family identity** — `fam_<uuidv7>` + monotonically increasing integer version, allocated atomically. The governed semantic contract (e.g. `network.attach_failure`). Human/API-facing as `fam_…@v3`.
3. **Semantic fingerprint** — digest over **controlled metadata only** (units, identifier namespaces, canonical field IDs, privacy class, enum semantics — never free prose). °C→°F bumps semantic version even when JSON is identical. **Deferred to P2/P3:** metadata fields stored from P1, no stable-identity claim until activated.

Candidate lifecycle: `cand_<hash>` (provisional) → staged → observed → promoted (`sch_` + `fam_@v`) → deprecated (never deleted). Promotion writes alias `cand_x → sch_y` exactly once — no reassignment, no chains, no cycles; resolves to a single terminal ID. History never rewritten.

**Drift policy:** compatible change (add optional w/ default, safe widening, controlled deprecation) = new version, same family. Meaning/unit/discriminator change, collapsed required-field overlap, different lifecycle semantics = new family. **Bias false-split over false-merge** — duplicates can be aliased later; a false merge corrupts historical interpretation forever. The SLM never makes this decision alone and can never merge families.

## 6. The vault — Redis

Startup: refuse non-persistent Redis unless `--unsafe-volatile`. Require AOF `everysec`+, `noeviction`. **Runtime monitoring, not startup-only:** AOF write errors, disk exhaustion, `CONFIG SET` drift → freeze promotions + fail readiness. AOF everysec ≈ 1s RPO — documented; backup/export + restore test required (P1).

| Key | Holds | Mutability |
|---|---|---|
| `deblob:schema:<sch_id>` | canonical schema + provenance | write-once via Lua; on collision byte-compare — mismatch is **fatal**, not dedupe |
| `deblob:family:<fam_id>` | name, versions, compat policy, state | versions append-only, atomic allocation; `(family_id, version)` unique |
| `deblob:candidate:<cand_id>` | provisional profile + evidence refs | TTL-expired |
| `deblob:candidate-audit:<cand_id>` | audit stub | permanent (separate key — TTL must not eat provenance) |
| `deblob:alias:<cand_id>` | → terminal `sch_id` | write-once, acyclic |
| `deblob:index:*` | bucketed structural lookup | derived, disposable, rebuildable offline from schema records + consistency checker |
| `deblob:evidence:<id>` | stream: stats + redacted samples | trimmed (`XTRIM ~`), per-candidate max entries/bytes, retention-bound |
| `deblob:audit:*` | promotions: actor, reason, prior state | append-only |

- **Publication is one atomic Lua transition:** schema record + family version + index entries + alias + audit event — or nothing. No partial publication after a crash.
- **Index design:** bounded buckets/inverted features — field-count band, required-key hashes, depth, type signature. Never a global scan/distance pass over all schemas.
- Raw payload bytes live in Kafka (and later an archive) **by reference** — evidence streams hold statistics and redacted samples only.
- Security: TLS/ACLs or private trusted boundary; split credentials for read / evidence-append / promote where feasible. Read access ≠ publication rights.
- Multi-instance (P4): LRU invalidation via Redis pub/sub keyspace events; P1 single process uses an internal channel on promotion.

## 7. SLM lane (`deblob-slm`, P2+)

- **Isolation:** in-process llama.cpp can kill the binary (native crash/OOM — threads don't isolate faults). P2: dedicated worker thread, bounded channel, hard inference deadline, preallocated model memory budget, panic containment. Child-process isolation acknowledged as the eventual endpoint.
- **Economy:** invoke once per stable cluster (debounced, sample/time stability required), dedupe by monoid-profile digest, cancel superseded jobs, cap invocations per source/hour, cache decisions by model+prompt+candidate-set digest. Never per-record autoregression.
- **Grammar:** fixed 3-way output contract, keyed by output-contract version + model/tokenizer/template digest + grammar-engine version. **Never compile attacker-derived candidate schemas into grammars.**
- **Contract:** `match_schema(id, exact | compatible_drift | incompatible_similarity)` / `new_candidate(reason)` / `abstain(reason)`. IDs grammar-constrained to the supplied top-k set — the model cannot invent `sch_`/`fam_`/`cand_` values. Abstain reasons are enums + bounded diagnostics (free prose = exfil channel). `new_candidate` never implies family approval.
- **Prompts:** monoid statistics + redacted field metadata only — never raw payload values (prompt-injection surface). Field names length-capped, escaped as data, instruction-like sequences detected.
- **Repair:** max one, mechanical failures only (malformed scalar, missing field, invalid enum encoding). Uncertainty is never retried into confidence.
- **Policy inputs:** retrieval margin, producer consistency, sample coverage, deterministic compatibility, historical calibration, model agreement — never model self-confidence.
- **Models:** Tier 1 FunctionGemma 270M (GGUF, domain fine-tune planned); Tier 2 Granite 4.0 Nano 1B Q4 (Apache 2.0, field semantics, P4); Tier 3 SmolLM3 3B Q4 verifier / human (not a tie-breaker — persistent disagreement stays provisional). FunctionGemma format compatibility (special tokens, template, stop sequences) verified by golden tests; "model loaded" is not sufficient.
- **Artifact contract:** GGUF digest, tokenizer digest, chat template, grammar version, quantization recorded per decision; evaluation on the actual quantized artifact; wrong-valid rate is a first-class metric distinct from schema-valid rate.

## 8. Management API (`deblob` bin)

Separate listen port from ingest — never reachable from the producer network path. Bearer/API-key auth from env, required.

```
GET  /api/v1/schemas?cursor=&limit=        cursor pagination
GET  /api/v1/schemas/{sch_id}
GET  /api/v1/families | /{fam_id} | /{fam_id}/versions
GET  /api/v1/candidates?state=provisional|staged
POST /api/v1/candidates/{cand_id}/promote  → 201 + Location  (authenticated, audited)
POST /api/v1/candidates/{cand_id}/reject
GET  /api/v1/quarantine?cursor=
GET  /healthz /readyz /metrics             (readyz fails on persistence degradation)
```

Error envelope `{"error":{"code","message","details":[]}}`; `409` on immutability conflicts; `422` semantically invalid promotion. Promotion is an administrative security boundary: actor identity, reason, previous state, immutable audit record — from day one, before any UI.

## 9. Security summary

- **Header namespace reserved:** strip all inbound `deblob-*` (Kafka) / `Deblob-*` (HTTP) on ingress; optional strict mode rejects instead. Producer-provided schema IDs never trusted.
- **Derived-topic ACL:** only Deblob writes `topic.tagged` — otherwise producers inject trusted-looking records past tagging. Tags are ACL-backed, not cryptographic — documented explicitly.
- **PII:** evidence defaults to statistics; deterministic redaction gate before Redis and before any SLM prompt, fail closed; raw payloads by reference in separately controlled archive; Redis AOF/replicas contain every stored sample — retention applies there too.
- **DoS:** byte/compute budgets in addition to candidate-count limits (one payload can carry thousands of nested fields); per-source quotas; producer identity + trust level weighs into promotion (single untrusted producer cannot establish a family).
- **HTTP proxy hardening (P2):** fixed upstream allowlist, body/decompression limits, header limits, slowloris/read/write timeouts, hop-by-hop stripping, request-smuggling defenses, TLS client identity.
- Secrets env-only, validated at startup; `cargo audit` + committed lockfile in CI; rdkafka TLS/SASL supported.

## 10. Error handling

| Condition | Behavior |
|---|---|
| Malformed / over-limit payload | quarantine stream + reason code; never silently dropped; no payload echo in logs |
| Kafka tombstone | pass-through with reserved tag (not malformed) |
| Redis unavailable | tag `unresolved` (never `cand_`); LRU continues; promotions frozen; readiness fails. No WAL in P1 — fail closed (an underspecified WAL is worse than none) |
| SLM crash/timeout | cold lane pauses; hot path unaffected; discovery topic retains backlog durably |
| Rebalance | abort txn before revoke; no post-revoke commits |
| Crash anywhere | Kafka transaction aborts atomically; Lua publication is all-or-nothing |

## 11. Observability contract

`tracing` structured logs (no payload contents by default). Prometheus: match rate, cache-hit rate, candidate-creation rate, unresolved rate, quarantine rate, cold-lane lag, index size, Redis/AOF health, tag latency p50/p99, SLM latency + abstention rate + wrong-valid rate (P2+), promotions count.

## 12. Phasing

- **P1 — deterministic core (no SLM):** Kafka JSON relay w/ transactions; fingerprint + monoid + quarantine; Redis vault + atomic publication + rebuildable indexes; management API + promote CLI/API; crash/rebalance/duplicate-delivery test suite; backup/restore test. *Ships standalone value.*
- **P2 — shadow mode:** SLM lane, decisions logged not applied; wrong-valid calibration vs deterministic baseline; HTTP push adapter + hardening; golden corpus + eval harness (exact match, false merge/split, optional fields, dynamic maps, malformed, prompt injection, schema bombs); canonicalizer v2 migration plan (dual-read/dual-index).
- **P3 — live proposals:** SLM behind policy gate; staged→observed→promote flow; semantic fingerprint activated.
- **P4 — deep semantics (maybe):** Granite Nano field-level tier; embedding retrieval; multi-instance.

## 13. Testing strategy

TDD throughout (house rules; 80%+ coverage).

- `deblob-monoid`: proptest — associativity, commutativity where claimed, idempotence, merge-order independence.
- `deblob-fingerprint`: golden corpus; canonicalization stability; duplicate-key rejection; number-precision cases; cargo-fuzz (schema bombs, deep nesting, huge keys) in CI.
- `deblob-kafka`: testcontainers — transactional produce/consume, crash-consistency between every Kafka/Redis transition, rebalance chaos, duplicate-delivery idempotence, tombstones, header limits.
- `deblob-redis`: Lua publication atomicity under injected crashes; index rebuild + consistency checker; NX collision paths.
- P2+: SLM golden-format tests; eval harness with ≥25% abstain/no-match cases; quantized-vs-reference disagreement; shadow decision log (candidate set, deterministic scores, model choice, policy result, reviewer verdict).

## 14. Retention policy (explicit lifetimes)

Raw records (Kafka retention) ≠ evidence streams (trimmed, bounded) ≠ candidates (TTL) ≠ audit stubs (permanent) ≠ quarantine (bounded) ≠ aliases (permanent) ≠ deprecated schemas (permanent — deletion is destructive; deprecate instead).
