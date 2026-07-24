# Deblob demo — Schema Normalization

A live, ~60-second showcase of Deblob's value: a producer changes its payload
shape without warning → Deblob tags every record with a schema id on
`events.tagged` → a **normalizer** reshapes each record into one **stable,
accreting canonical contract** → a **mock downstream ETL** keeps processing with
**zero errors**, in **both drift directions** (v1 → v2 → back to v1).

Backwards compatibility is the whole point: downstream consumers see a fixed
contract no matter how the upstream drifts.

Design: `../../docs/superpowers/specs/2026-07-24-deblob-demo-drift-sentinel-design.md`

## Access

- **Public (Authelia-gated):** https://deblob-demo.kamilandrzejrybacki.dpdns.org —
  TLS at Caddy, Authelia one-factor login in front, routed to the NodePort. Wired
  in the ansible caddy role (`Caddyfile.j2` block + `subdomain_deblob_demo`); no
  DNS/Authelia change needed (wildcard `*.domain` + wildcard `one_factor` cover it).
  Gotcha: Caddy's Caddyfile is a **single-file bind mount** — edits need
  `docker restart caddy` (not `caddy reload`; reload re-reads the stale inode).
- **LAN direct:** http://192.168.0.107:30895 (NodePort 30895 on any node, no auth).

## The demo (what to click)

1. Baseline: producer emits **v1** orders → Deblob promotes+names the schema. The
   normalizer seeds its **canonical contract** from that v1 shape (6 core fields).
   The ETL validates every normalized record against the original v1 contract:
   **0 errors**.
2. Click **⚡ Trigger drift (v1 → v2)** — the producer renames `amount`→`total_cents`
   (float→int, a unit change), nests `customer_name`→`customer{id,name}`, and adds
   `shipping{}`. The normalizer computes a transform for the new shape:
   - `customer_name ← customer.name` — provable rename, applied automatically.
   - `customer_id`, `shipping_method`, `shipping_eta_days` — additive, **accreted**
     into the canonical superset as nullable fields.
   - `amount ← total_cents · ÷100` — a **unit change on a core field**. Deblob won't
     guess: the record is **held** (not emitted) so the ETL never sees a wrong value.
3. The **pending approval** box appears. Click **✔ Approve** — the ÷100 conversion
   activates, every held record is flushed into the pipeline, and the ETL keeps
   going with **still 0 errors**.
4. Click **↺ Reset (v2 → v1)** — the producer drifts back. Old v1 records normalize
   straight through again (additive fields simply come out `null`). The ETL stays
   green in **both** directions.

## Architecture

- `src/producer.py` — hero producer → `events.demo.orders` (v1/v2, HTTP control).
- `src/normalizer.py` — reads the `deblob-schema-id` header on `events.tagged`;
  maintains an **accreting canonical superset** + a per-shape **transform** (via
  the copied `_classify` / `_leaves` / `_tokens` helpers); reshapes every record
  and produces the canonical form to `events.demo.orders.normalized`. Holds any
  record whose core field is a pending unit/type change until an operator approves.
- `src/etl.py` — mock downstream consumer of `events.demo.orders.normalized`;
  validates the original v1 contract (`order_id`, `customer_name`, `amount`,
  `currency`, `item_count`, `placed_at`). Stays green because the normalizer only
  ever emits complete records.
- `src/dashboard.py` — stdlib BFF: serves the UI + proxies producer/normalizer/etl
  (holds the Deblob token server-side; no browser secrets).

### The normalizer state machine

- **CANONICAL** `field → {ty, kind: core|additive}` — seeded from the first blessed
  (`sch_`) shape (all `core`); grows **additive-only** as new shapes bring new
  fields. Core is never renamed or removed.
- **TRANSFORMS** `schema_id → {canonical_field → {src, kind, conversion}}` —
  computed once per distinct shape via `_classify(canonical_leaves, shape_leaves)`:
  identity/rename → read-through; additive → accrete + read-through; unit/type
  change → `held` with a proposed conversion (`*_cents → ÷100`).
- **normalize(payload, schema_id)** rebuilds the canonical record by reading each
  field from its mapped path (applying an approved conversion). If any **core**
  field is held-pending or absent it returns `None` → the record is **HELD**, never
  emitted broken.
- **/approve** activates the first pending conversion, un-holds that field across
  all shapes, and flushes the held backlog.

Reuses (ns `deblob`) the Redpanda broker and Deblob API. Deblob-side change is
`events.demo.orders` in `raw_topics`/`capture_sources`/`auto_promote.allowed_sources`
(`../console/live/33-deblob-config.yaml`); the demo adds the derived topic
`events.demo.orders.normalized`.

## Deploy / redeploy

```sh
./deploy.sh                       # idempotent: topics, config, ns, cms, services
# after editing src/:
./deploy.sh && kubectl -n deblob-demo rollout restart \
  deploy/demo-producer demo-normalizer demo-etl demo-dashboard
```

## Notes

- **First-run setup cost:** the v1 candidate auto-promotes to a named schema only
  after it is 10 min old (`min_age_ms`). The normalizer only seeds its canonical
  from a promoted `sch_` id, so give the topic ~10 min on first creation (or trigger
  the `namer-controller` cronjob) before the first run.
- **Keep settle-and-sample off** for `events.demo.orders` — a settled source has a
  blind window that would hide the drift (currently off by default).
- Producer/normalizer/etl `pip install --user confluent-kafka` at startup (needs
  PyPI egress); the dashboard is pure stdlib.
- Without a `DEBLOB_TOKEN` the normalizer degrades to deriving leaf shapes from the
  payload JSON instead of the Deblob schema API — the demo still runs.
