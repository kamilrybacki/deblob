# Operator runbook

Accurate to the P1 deterministic core as built (Tasks 1-19), not the
long-term design. Where a documented behaviour is only a design intent and
not yet wired into code, this runbook says so explicitly rather than
describing aspirational behaviour as fact. Cross-references: the
[design specification](superpowers/specs/2026-07-14-deblob-design.md)
(spec §6/§10 in particular) and `crates/deblob/src/config.rs`,
`crates/deblob-redis/src/health.rs`, `crates/deblob-redis/src/registry.rs`.

## Outage behavior

| Condition | Observed behaviour | Recovery |
|---|---|---|
| **Redis unreachable/down** | The relay keeps running. Every record that would otherwise resolve against the registry is tagged `deblob-schema-id: unresolved` — **never** a minted `cand_…` id (spec §10: an outage must never manufacture a candidate). `RedisRegistry::publish` refuses writes once the runtime `HealthGate` reports degraded, so **promotions freeze**. `GET /readyz` returns `503` (it reads the same `HealthGate`); `GET /healthz` still returns `200` (the process itself is alive). | Automatic. Every long-lived Redis connection in this codebase uses `redis::aio::ConnectionManager` (see `deblob_redis::connection_manager_config()`), which transparently reconnects — no restart needed. `response_timeout` is tuned to 2s specifically so a hung/black-holed connection fails fast into `unresolved` instead of stalling the hot path. The background persistence probe (`HealthGate::spawn_probe`, ~10s interval in production) re-evaluates `INFO persistence` + `CONFIG GET maxmemory-policy` on its own connection and flips the gate back to healthy once Redis is reachable and passes the AOF/`noeviction` checks again — nothing needs to be manually toggled. |
| **Redis reachable but persistence degraded** (`appendonly no`, an AOF/RDB write failed, or `maxmemory-policy` isn't `noeviction`) | Same as "Redis unreachable" from the operator's perspective: `HealthGate` reports degraded, promotions freeze, `/readyz` fails. The specific reason (e.g. `"aof_last_write_status:err (AOF write failing — check disk space)"`) is in the `HealthState::Degraded` value the probe last observed — surface it via logs/tracing, there is no dedicated endpoint exposing it yet. | Fix the underlying Redis persistence problem (free disk, re-enable AOF, correct `maxmemory-policy`) — the next probe tick (≤10s) picks up the change automatically. |
| **Malformed / over-limit payload** (bad JSON, duplicate keys, non-finite numbers, depth/size/field-count/key-length over configured limits, invalid UTF-8) | Quarantined: produced to `kafka.quarantine_topic` with `deblob-schema-id: malformed` and a `deblob-quarantine-reason` header carrying one of 8 bounded reason codes (`duplicate_key`, `non_finite_number`, `depth_exceeded`, `size_exceeded`, `field_count_exceeded`, `key_length_exceeded`, `parse_error`, `utf8_error`). Never silently dropped; the raw payload is never echoed into logs or metric labels. | No action needed unless the quarantine rate is unexpectedly high — check `deblob_quarantine_records_total{reason=...}` and the relevant `limits.*` config values (`deblob.example.toml`'s `[limits]` section). |
| **Kafka tombstone** (null value) | Passed through with `deblob-schema-id: tombstone` — treated as a reserved tag, not as malformed. | N/A — expected behaviour. |
| **Consumer-group rebalance** | The in-flight Kafka transaction is aborted before the partition revoke completes; no commits happen after a revoke. | Automatic — the relay resumes on the next assignment. |
| **Process crash (any point)** | The Kafka transaction aborts atomically (nothing partially committed to `tagged`/`discovery`/`quarantine` topics). On the Redis side, schema publication (`RedisRegistry::publish`) is a single atomic Lua script — schema record, family version, structural-index entries, alias, and audit event commit together or not at all; there is no partially-published schema to clean up. | Automatic — restart the process; Kafka's transactional consumer offsets and Redis's atomic publication together mean there's nothing to manually repair. |
| **SLM lane down/absent** | **N/A in P1** — there is no SLM lane in this build (`deblob-slm`/`SemanticInferencer` is P2 work). The cold lane (sampling → monoid-merged candidate profiles → structural clustering) runs independently of any inference step and is unaffected. |
| **Bad `DEBLOB_API_TOKEN` / no `Authorization` header** | Every `/api/v1/*` route returns `401` with `{"error":{"code":"unauthorized",...}}`. `/healthz`, `/readyz`, `/metrics` are intentionally unauthenticated. | Fix the caller's bearer token. |

Redis's AOF `everysec` fsync policy means an outage or crash can lose **up to
~1 second** of the most recent writes — this is the documented recovery
point objective (RPO), not a defect; see [Backup and restore](#backup-and-restore).

## Index rebuild

`deblob:index:*` is a **derived, disposable** structural index — bucketed
`SET`s that map a `ShapeSummary` (field-count band / depth / required-key
hash) to the schema IDs filed under it. It exists purely to make lookup a
bounded operation on one small bucket instead of a scan over
`deblob:schema:*`, and every bucket membership is reconstructible from the
authoritative schema records themselves (each schema hash carries the
`bucket` field it was filed under at publish time, plus a `variants` field
for observed concrete-shape variants).

**How it's implemented today:** `RedisRegistry::rebuild_index()` (in
`crates/deblob-redis/src/index.rs`) drops every key matching
`deblob:index:*`, then walks all `deblob:schema:*` records via `SCAN` and
re-`SADD`s each one's membership (including variants) into its recorded
bucket. It's safe to run online, at any time — the index is never consulted
for anything but a resolvable, disposable side path. A companion
`RedisRegistry::verify_index()` cross-checks bucket→schema and
schema→bucket consistency and returns a list of any drift found, without
mutating anything.

**Follow-up needed:** neither `rebuild_index` nor `verify_index` is exposed
through the `deblob` binary's CLI or the management API today — they are
library-only operations on `RedisRegistry`. Until a maintenance subcommand
or authenticated API route is added, running a rebuild requires either (a)
a small ad hoc Rust program/test harness that constructs a `RedisRegistry`
against the target Redis URL and calls `.rebuild_index().await`, or (b)
adding a `deblob reindex` CLI subcommand as a small follow-up task. Track
this as a gap, not a documented procedure operators can run today without
writing code.

## Backup and restore

The Redis instance backing `deblob:schema:*`, `deblob:family:*`,
`deblob:candidate:*`, `deblob:alias:*`, `deblob:evidence:*`, and
`deblob:audit:*` **is** the durability floor for the schema vault. AOF
`everysec` is required at startup (`RedisRegistry::connect` refuses to start
against a non-persistent Redis unless `--unsafe-volatile` is explicitly
passed — an accepted risk for ephemeral/dev deployments only, never
production). That gives an RPO of roughly one second.

**Backup:**

1. Trigger a background save: `redis-cli -h <host> BGSAVE` (or rely on the
   AOF file directly — either is a valid point-in-time source since both
   are enabled).
2. Wait for `rdb_bgsave_in_progress:0` in `redis-cli INFO persistence`.
3. Copy the RDB file (`dump.rdb`, path from `redis-cli CONFIG GET dir`) and/or
   the AOF directory (`appendonlydir/`, Redis 7+'s multi-part AOF) to backup
   storage. Copying both is redundant but cheap; either alone is sufficient
   to restore.
4. Record the source Redis's `redis-cli INFO server` `redis_version` and
   the backup timestamp alongside the copied files — useful context if a
   restore ever needs to target a different Redis version.

**Restore (test this before relying on it):**

1. Provision a fresh Redis instance with AOF `everysec` and
   `maxmemory-policy noeviction` (the same two invariants
   `RedisRegistry::connect`'s startup gate enforces) — restoring into a
   misconfigured Redis will just have `deblob` refuse to start against it.
2. Stop that fresh instance, place the copied `appendonlydir/` (preferred —
   AOF is authoritative for the most recent writes) or `dump.rdb` into its
   data directory, and start it.
3. Confirm data is present: `redis-cli DBSIZE`, spot-check a known
   `deblob:schema:<sch_id>` key with `HGETALL`.
4. Run an index rebuild against the restored instance (see
   [Index rebuild](#index-rebuild) above) — `deblob:index:*` is disposable
   and is not guaranteed to be internally consistent immediately after a
   restore from an AOF/RDB snapshot taken mid-write, so always rebuild
   rather than trust the index of a freshly restored dataset.
5. Point `DEBLOB_REDIS_URL` at the restored instance and start `deblob`
   normally; the startup persistence gate will re-verify AOF/`noeviction`
   before accepting writes.

There is no automated restore-test harness in this repo yet — treat the
steps above as a manual runbook and validate them in a staging environment
before depending on them for a real incident.

## Promoting a candidate

Promotion turns a `cand_…` candidate into a published `sch_…` schema. It is
an authenticated, audited administrative action — every promotion records
the actor (from the `x-deblob-actor` header, defaulting to `"api"` if
absent — P1 ships one shared bearer token, not per-caller identity), the
supplied reason, and the candidate's prior state.

```
curl -s -X POST \
  -H "Authorization: Bearer $DEBLOB_API_TOKEN" \
  -H "x-deblob-actor: jane@example.com" \
  -H "Content-Type: application/json" \
  -d '{"family": "new", "name": "orders.created", "reason": "confirmed stable shape after 3 days of samples"}' \
  http://127.0.0.1:9615/api/v1/candidates/cand_XXXX/promote
```

- `family` is either the string `"new"` or `{"existing": "fam_..."}` to
  attach a new version to an existing family.
- `name` is optional (only meaningful when creating a new family).
- `reason` is required and lands in the audit trail.

**Response:** `201 Created` with a `Location: /api/v1/schemas/{sch_id}`
header and `{"data": {...schema record...}}` body on success.

**Guards enforced before a candidate can promote** (`crates/deblob/src/policy.rs`,
config `[promotion]` in `deblob.toml`):
- `min_samples` (default 10) — the candidate must have accumulated at least
  this many observed samples.
- `min_age_ms` (default 300000 / 5 minutes) — the candidate must have
  existed at least this long, so a promotion can't fire on a single burst.

A request against a candidate that hasn't crossed these guards returns
`422 Unprocessable Entity` with `code: "unprocessable_entity"` (spec §8:
`PolicyRejected`, distinct from a `409` identity/state-machine conflict).
A request against a nonexistent or already-terminal candidate returns `404`
or `409` respectively. Promotion writes a `cand_x → sch_y` alias exactly
once — there is no reassignment and no chain; a second promote attempt on
an already-promoted candidate is a `409 Conflict`, not a silent no-op.

To reject a candidate instead: `POST /api/v1/candidates/{cand_id}/reject`
(no body) — `204 No Content` on success, `404` if it doesn't exist.

## Metrics

`GET /metrics` (unauthenticated, Prometheus text exposition format 0.0.4)
exposes exactly this set — every label value is a fixed, bounded enum;
none ever carries a schema id, candidate id, producer/source identifier,
topic name, or error message (`crates/deblob-match/src/metrics.rs`):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `deblob_messages_total` | counter | `fate` (`known`\|`provisional`\|`unresolved`\|`malformed`\|`tombstone`) | Every message classified on the hot path |
| `deblob_schema_matches_total` | counter | `result` (`known`\|`provisional`\|`unresolved`) | Match attempts that reached a real decision (excludes malformed/tombstone) |
| `deblob_cache_hits_total` | counter | none | Exact-match LRU cache hits (zero registry round-trips) |
| `deblob_quarantine_records_total` | counter | `reason` (the 8 bounded quarantine reason codes) | Messages quarantined, by reason |
| `deblob_tag_latency_seconds` | histogram | none | End-to-end `HotMatcher::classify` latency |
| `deblob_registry_operation_duration_seconds` | histogram | `operation` (currently just `resolve_structural`) | Registry backend call duration |
| `deblob_candidates_active` | gauge | none | Distinct candidates currently tracked by the cold lane |
| `deblob_candidate_promotions_total` | counter | `result` | Registered but not yet incremented anywhere in P1 (no live promotion-result callsite wires into it yet) |
| `deblob_relay_records_total` | counter | none | Records read off the raw relay topic |
| `deblob_relay_transactions_total` | counter | `result` (`committed`\|`aborted`) | Relay Kafka transaction outcomes |
| `deblob_cold_lane_lag_records` | gauge | none | Registered but not yet set anywhere in P1 |

Every bounded label combination is pre-touched at startup (registered at
`0`), so `/metrics` shows a stable series set from the very first scrape,
not only after the first matching event of each kind.

`deblob_slm_decisions_total` (P2, shadow-mode SLM) is deliberately **not**
registered — P1 has no SLM lane, and a metric nothing emits would misrepresent
what this binary actually tracks.

## Header contract

Reserved header namespace: every inbound header whose key starts with
`deblob-` (case-insensitive) is stripped before a record is re-produced —
this is a spoofing defense, not a convenience default. A producer can never
inject its own schema tag past the relay, even by sending duplicate
`deblob-schema-id` headers on the wire.

| Header | Values | Written by |
|---|---|---|
| `deblob-schema-id` | `sch_<base32(sha256(canon))>` (known), `cand_<hash>` (new/provisional shape), `unresolved` (registry briefly unavailable), `malformed` (quarantined), `tombstone` (Kafka null value passed through) | The relay, exactly once, on every record it re-produces |
| `deblob-origin` | `<topic>/<partition>/<offset>` — the source record's own coordinates, verbatim | The relay, exactly once — deterministic across replays (same source offset ⇒ identical header, so a replay never mints a fresh id) |
| `deblob-quarantine-reason` | One of `duplicate_key`, `non_finite_number`, `depth_exceeded`, `size_exceeded`, `field_count_exceeded`, `key_length_exceeded`, `parse_error`, `utf8_error` | The relay, only on quarantined records — always a short bounded code, never the full parse-error message or a payload fragment |
