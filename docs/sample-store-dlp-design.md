# Raw-sample store + DLP (joint Claude × Hermes design)

run: dc-samples-dlp-1907 · 2026-07-19 · agents: Claude Code + Hermes

## Goal
Lift the §9 "never store payloads" invariant **for troubleshooting only**: a
bounded, redacted, rolling log of real messages per discovery candidate,
explorable in the console. This relaxes the invariant **for the
Redis/application persistence domain only** — see Kafka caveat below.

## Non-negotiable safety requirements (Hermes review)
1. **Default OFF globally.** Opt in only by **trusted stable `source_id`** —
   never a producer-controlled header or arbitrary topic name (source-spoof).
2. **Two separate controls:** source-level *capture* authorization AND a
   distinct **`samples:read`** *view* capability. Extra auth to view even
   though redacted — DLP is probabilistic and business data can still be
   sensitive. Homelab: an admin-only bearer capability + audit is enough.
3. **No "reveal original" mode, ever.** Only redacted output exists.
4. **Fail-closed for confidentiality, best-effort for availability:** DLP
   error/timeout/panic/over-limit → **store nothing**; store outage → ingest
   continues; backpressure → drop capture, **never spill raw to disk**.
5. **Capture keyed on the RESOLVED candidate id**, after clustering — not
   `DiscoveryMsg.cand_id` (which is the pre-cluster raw id → wrong candidate).
   Requires `ColdLane::ingest` to return the resolved id + is_new.
6. **Idempotent, age+count pruned store** (at-least-once consumer replays
   otherwise eat the budget with dupes).
7. **Volatile persistence domain** — the sample store must NOT share the
   permanent vault's Redis (RDB/AOF/replicas/snapshots outlive TTL).
8. **Re-run DLP on read**, render as escaped text/DOM (never innerHTML),
   `Cache-Control: private, no-store`, audit actor/candidate/source/count only.

## DLP (`deblob-dlp`, redact-before-store)
Parse once into a **bounded JSON tree** (reuse `parse_bounded`); redact the
tree, never operate on raw text. Inspect **keys, scalar values,
strings-containing-encoded-JSON, and container depth/cardinality**.

Action by finding type — NOT uniform "redact in place":
| Finding | Action |
|---|---|
| Sensitive field NAME | replace the **entire value/subtree** with a marker (nested names/lengths leak) — never recurse |
| High-confidence secret in a scalar | replace the whole scalar |
| PII in free text | replace the whole scalar (partial later) |
| Sensitive-looking dynamic KEY | replace key with a **per-sample** placeholder `[REDACTED_KEY_1]` (never a stable hash — linkable/brute-forceable) |
| Unparseable / DLP-failed / too-complex | **store nothing** |
| Excessive findings | **drop the whole sample** |

- Redaction markers are **visible** (`"█REDACTED:sensitive_key█"` / `null` +
  out-of-band `{path, detector, action}`), never type-preserving substitutes
  (`0`/`false`) the console could mistake for real values.
- **Never byte-truncate JSON at 8 KB** (invalid JSON / cuts a secret in half /
  discloses a credential prefix). Redact the full bounded doc, THEN apply
  **structure-aware** limits (drop fields/array tails with markers, or omit).

**Key normalization before name-matching:** Unicode NFKC, lowercase, split
camelCase, strip `_-.`+whitespace, detect confusables/control chars.

**Name detectors (first layer, not a boundary):** password/passwd/pwd/
passphrase, access_token/refresh_token/id_token, client_secret/api_key/
x_api_key, authorization/auth/bearer, cookie/set_cookie/session/session_id,
private_key/signing_key/encryption_key, credential/connection_string/dsn,
webhook_secret/signature, seed/mnemonic/recovery_code, ssn/pesel/passport/
tax_id, card/cvv/iban/bank_account.

**Value detectors:** JWT *structure* (not just `eyJ`), bearer/basic auth,
PEM/SSH private-key blocks, AWS/GitHub/Stripe/Google/Azure/webhook token
formats, connection strings + URLs-with-credentials, emails, payment cards
(length + Luhn), IBAN/gov-ID checksums where feasible, high-entropy hex/base64,
session cookies/OAuth codes, mnemonic/seed patterns.

**Irreducible limit:** secrets can be URL-encoded, split across fields, double-
base64'd, in prose, low-entropy (`summer2026`), or Unicode-obfuscated. The
store is therefore **classified potentially-sensitive even after redaction**.
For high-risk sources (arbitrary logs/webhooks) prefer a source-specific
**safe-path field ALLOWLIST** (permit-listing fields ≫ enumerating secrets).

## Storage (dedicated volatile Redis)
Sorted set per candidate, idempotent, age+count pruned:
```
sample_id = hash(source_id, topic, partition, offset)   # idempotent
ZADD samples:<resolved_cand> NX <captured_at_ms> <record>
ZREMRANGEBYSCORE samples:<cand> -inf <now-7d>            # age prune
ZREMRANGEBYRANK samples:<cand> 0 -21                     # keep newest 20
EXPIRE samples:<cand> 8d                                 # safety-net only
```
Plus a **source-level total memory budget** (candidate storms exhaust Redis
despite per-key bounds: 10k candidates × 20 × 8 KB ≈ 1.5 GiB).

Record shape:
```json
{ "sample_id":"smp_…","source_id":"src_…","captured_at_ms":0,
  "dlp_version":"deblob-dlp-v1",
  "redaction_counts":{"sensitive_key":2,"secret_pattern":1,"pii_pattern":0},
  "truncated":false, "document":{ … redacted … } }
```

## Kafka caveat (stated, not silently ignored)
`DiscoveryMsg` already carries raw bytes on the discovery topic → Kafka
persists them (segments, backups, snapshots, replication, connector logs).
DLP-before-Redis relaxes **only** the Redis/app-persistence invariant, not the
Kafka one. Stronger "raw never touches durable media" would require DLP before
the discovery topic (a hot-path change) or a stats-only discovery message. At
minimum: bounded discovery-topic retention, restricted ACLs, no generic sinks.

## Config `[samples]`
`enabled=false`, `capture_sources=[]` (trusted src ids), `max_per_candidate=20`,
`retention_secs=604800`, `key_ttl_secs=691200`, `max_sample_bytes=8192`,
`max_findings=…`, `source_memory_budget_bytes=…`, `redis_url` (dedicated
volatile instance), `samples_read_token`.

## API
`GET /api/v1/candidates/{id}/samples` — `samples:read` gated, **re-runs DLP on
read**, `Cache-Control: private,no-store` + `X-Content-Type-Options:nosniff`,
audited (actor/candidate/source/count only), rate-limited. No bulk export, no
raw replay, no retention extension, no training use — explicitly prohibited.

## Console
Inspector gains a "Real samples (redacted)" tab. Rendered as **escaped text /
structured DOM nodes only (never innerHTML)**, strict CSP, excluded from any
analytics/error payloads. Prominent "redacted, DLP is probabilistic" banner.

## Logging discipline
DLP failures, parse errors, serialization errors, HTTP error handlers **never**
`Debug`/`%s`/serialize the document. Only sample_id/candidate_id/source_id/
size/detector-version/reason-code.

## Staging
1. `deblob-dlp` crate (pure redaction + detectors) + regression canary corpus.
2. `IngestOutcome{candidate_id,is_new}` + sample store (dedicated volatile Redis,
   sorted-set idempotent prune) + capture wiring (fail-closed).
3. `[samples]` config + `samples:read` API (no-store, audit, re-run DLP, rate).
4. Console tab (escaped DOM).
Default OFF throughout; enable per-source only after the corpus passes.

## Attribution
[C] Claude, [H] Hermes. Hermes's key additions: two-control model + samples:read,
finding-type-specific redaction (subtree replace, dynamic-key redaction, no
byte-truncation), resolved-candidate-id capture, sorted-set idempotent age+count
pruning, dedicated volatile persistence domain, re-run-DLP-on-read, Kafka-topic
caveat, source-memory budget, escaped-DOM rendering, scope-creep prohibitions.
