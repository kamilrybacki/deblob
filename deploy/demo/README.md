# Deblob demo — Drift Sentinel

A live, ~60-second showcase of Deblob's value: a producer changes its payload
shape without warning → Deblob catches the new schema on `events.tagged` → a
**naive** downstream loader breaks while a **Deblob-aware** loader contains the
drift with **zero bad writes**.

Design: `../../docs/superpowers/specs/2026-07-24-deblob-demo-drift-sentinel-design.md`

## Access

Dashboard (NodePort 30895 on any node): **http://192.168.0.107:30895**

## The demo (what to click)

1. Baseline: producer emits **v1** orders → Deblob promotes+names it
   (*"Orders Amount Currency Records"*). Naive loader: 0 errors. Aware loader:
   forwarding, blessed the v1 schema, 0 quarantined.
2. Click **⚡ TRIGGER DRIFT (v1 → v2)** — the producer renames `amount`→`total_cents`
   (float→int), nests `customer_name`→`customer{id,name}`, adds `shipping{}`.
3. Within seconds: **naive errors climb** (`KeyError: 'customer_name'` — silent
   data loss) while the **aware loader quarantines** the drifted records (does not
   crash) and shows the new schema id Deblob discovered. Scorecard: *naive bad
   writes = N, aware contained = N, aware crashes = 0, raw stored = never.*
4. Click **↺ Rollback to v1** to return to baseline.

## Architecture

- `src/producer.py` — hero producer → `events.demo.orders` (v1/v2, HTTP control).
- `src/naive_consumer.py` — parses `events.tagged` assuming v1; breaks on v2.
- `src/aware_consumer.py` — reads the `deblob-schema-id` header; blesses the
  promoted `sch_` id; quarantines on any id transition; resolves id→name via the
  Deblob API.
- `src/dashboard.py` — stdlib BFF: serves the UI + proxies the three services
  (holds the Deblob token server-side; no browser secrets).

Reuses (ns `deblob`) the Redpanda broker and Deblob API. The one Deblob-side
change is `events.demo.orders` added to `raw_topics`/`capture_sources`/
`auto_promote.allowed_sources` in `../console/live/33-deblob-config.yaml`.

## Deploy / redeploy

```sh
./deploy.sh                       # idempotent: topic, config, ns, cms, services
# after editing src/:
./deploy.sh && kubectl -n deblob-demo rollout restart \
  deploy/demo-producer demo-naive demo-aware demo-dashboard
```

## Notes

- **First-run setup cost:** the v1 candidate auto-promotes to a named schema only
  after it is 10 min old (`min_age_ms`). Thereafter it's long-lived; the demo is
  instant. If you ever recreate the topic, wait ~10 min (or trigger the
  `namer-controller` cronjob) before the first run for the pretty named schema.
- **Keep settle-and-sample off** for `events.demo.orders` — a settled source has a
  blind window that would hide the drift (currently off by default).
- Services `pip install --user confluent-kafka` at startup (needs PyPI egress);
  the dashboard is pure stdlib.
