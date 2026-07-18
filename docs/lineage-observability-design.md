# Data observability: live stream + lineage (Claude half, jr-lineage-181921)

Two features: **(1) observe the data stream** flowing through Deblob in real time,
and **(2) end-to-end per-source lineage** (Hermes' half — the model; this half wires
it into the live view + console). Both obey the hard rule: **metadata only, never
payloads/PII** — ids, coordinates, decisions, reason codes, redacted profile
summaries (spec §3.2/§11), enforced at the emit site, not the UI.

## (1) Live data-stream observation

### Mechanism — SSE tap off an in-process broadcast
The relay already processes every event on `events.raw` (fingerprint → tag / new
candidate / quarantine). Add a **`tokio::sync::broadcast` channel** the relay
publishes a redacted `StreamEvent` to for each processed record; the mgmt API
exposes **`GET /api/v1/stream`** (bearer auth) as **Server-Sent Events** that
subscribes to the broadcast and forwards.

- **Non-blocking, never slows the hot path:** the relay uses `try_send`/a bounded
  channel (cap ~1024); if full, drop + bump a `stream_dropped_total` counter. A
  lagged SSE consumer receives a `{"dropped": N}` marker rather than back-pressuring
  the relay.
- **Multi-viewer:** each SSE client is one broadcast subscriber; N viewers are fine.
- Chosen over consuming `events.tagged` directly (misses quarantine + candidate
  lifecycle) and over `/metrics` polling (coarse counts, not per-event).

### `StreamEvent` (payload-free by construction)
```
{ ts, lane: "hot" | "cold",
  origin: { topic, partition, offset },       // deblob-origin coordinates
  outcome: "tagged" | "new_candidate" | "matched_candidate" | "quarantined",
  schema_ref: "sch_…" | "cand_…",
  family_id?: "fam_…",
  reason?: "<bounded quarantine code>",         // never the parse-error text
  fields_count: u32, privacy_class?: "…",
  source?: "<source id>"                         // gains meaning once (2) lands
}
```
No values, no keys beyond counts, no free-text — the same discipline as reserved
headers. A dedicated `redact()` at the emit site is the single enforcement point.

### Console — Live Stream view
A scrolling feed (newest on top, ring-buffered ~200 rows) fed by an `EventSource`
to `/api/v1/stream`:
- each row: `time · outcome badge (tagged=good, new_candidate=accent, matched=muted,
  quarantined=risk) · schema/cand short-id · fam · origin offset · N fields · source`
- controls: pause/resume, filter by outcome, and a small **events/s sparkline** from
  `relay_rec_s`.
- reconnect with `Last-Event-ID` (SSE native) after a drop; show the dropped marker.

### Edge/proxy for SSE
`/api/v1/stream` is long-lived; the same-origin nginx proxy needs, for that location:
`proxy_buffering off; proxy_read_timeout 0; proxy_set_header Connection '';` and
HTTP/1.1 — mirroring the cellarette/ntfy SSE routes at the Caddy edge (which already
`flush_interval -1`). Add a dedicated `location = /api/v1/stream` block.

## (2) Lineage visibility — the console surface (model = Hermes' half)
Once source is propagated + persisted (Hermes), surface it three ways:
1. **In the live stream:** each `StreamEvent.source` shows which collector the event
   came from, in real time.
2. **On the candidate/schema card:** the existing lineage line gains the real
   source(s) (replacing today's uniform `events.raw`), + first/last seen + obs.
3. **A lineage TRACE view:** given a schema/candidate/umbrella, render a small graph:
   - **backward:** contributing source(s) → bronze schema → (silver) → this node
   - **forward:** this schema → the gold umbrella(s) it feeds → gold stream
   A schema/umbrella typically has MANY source contributors (Hermes models the
   many-to-one), so the backward view is a fan-in list with per-source obs counts +
   first/last seen; the forward view is the umbrella membership.

### Lineage API (consumes Hermes' model)
- `GET /api/v1/schemas/{id}/lineage` → `{ sources: [{source, obs, first_seen, last_seen}], families, umbrellas: [...] }`
- `GET /api/v1/candidates/{id}/lineage` → contributing sources + coordinates
- `GET /api/v1/umbrellas/{id}/lineage` → member schemas → their sources (the full fan-in)
- All payload-free; sources are ids, counts, timestamps, coordinates.

## How the two features unify
The live stream is lineage *in motion* (what's arriving, from where, decided how);
the trace view is lineage *at rest* (how a given schema/umbrella came to be). They
share one `source` identity from Hermes' model and one redaction discipline. Wiring
order: (2) source propagation first (so there's a real `source` to show), then (1)
the stream tap emitting it, then the console Live-Stream + Trace views.

## Open questions (for the merge)
- Does the SSE tap live in the relay process or a separate consumer of a
  `deblob.stream` topic (decouples the hot path further, at the cost of a topic)?
- Retain a short rolling history for the stream (last N events) so a newly-opened
  view isn't empty, or live-only?
- Sampling under high volume: emit every event, or 1-in-K with a sampled flag?
- Where does `source` bind on the StreamEvent before (2) lands — omit vs `events.raw`?
