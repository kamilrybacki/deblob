# Deblob Demo — Drift Sentinel (design)

date: 2026-07-24 · ns: `deblob-demo` · run: jr-deblob-demo-drift

## Purpose

Show Deblob's value in ~60s with one sharp story: **a producer changes its
payload shape without warning → Deblob catches the new schema live → a naive
downstream consumer breaks while a Deblob-aware consumer adapts.** The punchline:
*"your producer broke the contract, and we caught it before the warehouse did."*

## Decisions (locked with user)

- **Hero capability:** drift sentinel.
- **Data:** a scripted "hero" producer with a triggerable v1→v2 drift, running on
  top of the real 43-collector backdrop (authentic scale + reproducible drift).
- **Presentation:** a purpose-built dashboard with a **TRIGGER DRIFT** button.
- **Punchline:** naive-consumer-breaks vs Deblob-aware-consumer-adapts.

## What Deblob already gives us (the output contract)

- Relay consumes `raw_topics` → tags each record → produces the **original payload**
  (unchanged, no re-parse) to the single tagged topic **`events.tagged`** with two
  headers: **`deblob-schema-id`** (the resolved schema/candidate id) and
  **`deblob-origin`** (`<source_topic>/<partition>/<offset>`). Quarantine →
  `deblob.quarantine` with `deblob-quarantine-reason`.
- A new source is ingested only if it is in `raw_topics`; it is auto-promoted to a
  named schema when in `auto_promote.allowed_sources` and it clears `min_samples=50`.
- `/api/v1` (on `deblob-mgmt.deblob.svc:9615`) exposes schemas, families+versions,
  semantic/neighbors, value-profile, quarantine, sources, and an SSE stream. We use
  it to resolve a schema id → human name/family/version for display.
- Deblob **never persists raw payloads** — so the naive consumer must read the
  payload off `events.tagged`, not from Deblob.

## Architecture

New namespace **`deblob-demo`**. Reuses (cross-namespace) Deblob's Redpanda
(`redpanda.deblob.svc:9092` kafka, `:8082` REST proxy) and Deblob API
(`deblob-mgmt.deblob.svc:9615`). One small Deblob-side config change (below).

Four services (Python; `python:3.12-slim`, `pip install confluent-kafka` at start
for the messaging trio; dashboard is pure stdlib; code via configMap):

1. **drift-producer** — emits synthetic e-commerce "order" events to
   `events.demo.orders` (confluent-kafka producer) at ~3/s (clears the 50-sample
   promote gate in <1 min). Holds a `version` (default **v1**). HTTP control:
   `POST /trigger` → flips to **v2**, `POST /reset` → v1, `GET /state` → current
   version + counts, `GET /healthz`.
   - **v1**: `{order_id, customer_name (str), amount (float), currency, item_count,
     placed_at}`.
   - **v2 (breaking drift)**: rename `amount`→`total_cents` (float→**int**, units
     change), `customer_name (str)`→`customer {id,name}` (**nesting**), add
     `shipping {method, eta_days}`. Rename + type-change + new nesting = a clean,
     obviously-breaking drift.

2. **naive-consumer** — consumes `events.tagged`, filters to origin prefix
   `events.demo.orders`, parses the payload **assuming v1** (`customer_name` str,
   `amount` float). v2 records raise KeyError/TypeError → counted as parse errors.
   `GET /status` → `{processed, errors, last_error, last_seen_ok_at}`. Represents
   the warehouse loader that silently breaks.

3. **aware-consumer** — consumes `events.tagged`, filters to the demo origin, reads
   the **`deblob-schema-id`** header. Learns the first stable id as the "blessed" v1
   schema; when a record's id differs from blessed → **DRIFT**: routes it to a
   quarantine tally (does NOT crash), and queries `/api/v1` to resolve the new id →
   name/version for display. `GET /status` →
   `{forwarded, quarantined, blessed_schema_id, drift_detected_at, new_schema_id,
   new_schema_name, new_family_version}`. Represents the Deblob-aware loader.

4. **demo-dashboard** — pure-stdlib HTTP service that (a) serves a single-page
   HTML/JS dashboard, (b) server-side proxies `/api/producer|naive|aware|deblob`
   (avoids CORS), (c) `POST /api/trigger` → producer `/trigger`. The page polls
   every ~1s and shows: live event rate + current producer version; the v1 vs v2
   payload diff; naive error count climbing (RED) vs aware quarantine + "DRIFT
   DETECTED: events.demo.orders shape changed → new schema «name» v«n»" (GREEN);
   and Deblob's discovered schema/version for the demo source. A big **TRIGGER
   DRIFT** button.

## Deblob-side change (ns `deblob`)

In `deploy/console/live/33-deblob-config.yaml` add `events.demo.orders` to
`raw_topics`, `auto_promote.allowed_sources`, and `capture_sources` (so it's
ingested, promotable, and value-profiled). Create the Redpanda topic
`events.demo.orders`. Roll deblob. This is the only change to the running system;
it's additive and PII-safe (synthetic data).

## Edge exposure

NodePort on the dashboard (self-contained, no external deps) for v1; optional Caddy
route `deblob-demo.<domain>` as a follow-up. Everything else is ClusterIP.

## Data flow (the 60s script)

1. Baseline: producer emits v1 → Deblob promotes it to a named schema → both
   consumers steady (naive `errors=0`, aware `forwarded` climbing, `quarantined=0`).
2. Click **TRIGGER DRIFT** → producer flips to v2.
3. Within seconds: naive `errors` climbs (RED) — it's silently dropping/erroring.
   Aware detects the schema-id change, `quarantined` climbs (GREEN), shows the new
   schema Deblob discovered. Dashboard renders the v1→v2 diff and Deblob's new
   family version.
4. Punchline on screen: naive broke; Deblob-aware caught the drift and protected
   downstream.

## Error handling / robustness

- Consumers: own consumer group each; commit after processing; reconnect on broker
  blips; never crash on a bad record (naive *counts* the error, aware *quarantines*).
- Producer: bounded in-memory only; idempotent control endpoints.
- Dashboard: proxy failures render as "service starting…", never a blank page.
- All services `GET /healthz`; k8s readiness probes.
- Resource budget: tiny (each ~64–128Mi / 50–250m CPU).

## Testing / verification

- Unit-ish: producer v1/v2 payload builders; aware drift-classification (blessed vs
  new id).
- E2E on-cluster: produce v1 → confirm promotion + tagged flow → both consumers
  steady → trigger drift → assert naive errors climb and aware quarantines climb and
  Deblob shows a new schema/version for `events.demo.orders`.

## Hermes consult refinements (jr-deblob-demo-drift, folded in)

- **Exclude the hero source from settle-and-sample.** A settled source has a blind
  window: unsampled changed records keep the *cached* schema id until the next
  drift sample, which would hide the drift and break the headline claim. Verified
  settle is disabled (no `[settle]` section → default off); the demo requires it to
  stay off for `events.demo.orders`.
- **b35 exposes no native `drift_exit` event** — the public stream emits generic
  `new_candidate`/`tagged`. So drift is derived consumer-side by maintaining the
  expected (blessed) schema per source and detecting the schema-ref transition —
  which is exactly what the aware consumer does.
- **Honest framing = safe containment, not remapping.** Do NOT claim Deblob infers
  `amount`→`total_cents` (structural similarity ≠ semantic equivalence). The
  credible claim, shown on the scorecard: **zero bad warehouse writes, changed
  records contained, consumer stays healthy, operator gets the exact structural
  change.**
- Added a **Rollback to v1** button and a **scorecard** (drift detected · naive bad
  writes · aware contained · aware crashes=0 · raw stored=never).
- **Detection-latency target < 5s** cue→committed-tag (bless only the promoted
  `sch_` id so the one-time candidate→schema promotion of v1 isn't a false drift).
- Full report: `research/Deblob-Drift-Sentinel-Demo-Design-JR-2026-07-23.md` (Hermes vault).

## Out of scope (YAGNI)

Auth on the dashboard; multi-source drift; contract auto-generation; the read-only
MCP layer (separate effort). One source, one drift, one dashboard.
