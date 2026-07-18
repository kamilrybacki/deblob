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

---

# Merged with Hermes — the lineage MODEL (jr-lineage-181921)

Full Hermes report: vault `research/Deblob-End-to-End-Lineage-Joint-Design-2026.md`.

## Lineage model [H]
- **Per-source Kafka topics are AUTHORITATIVE.** Producer-supplied source headers are
  spoofable and must NOT determine identity.
- Stable **`src_` ids** with reviewed `topic → source_id` bindings. `Envelope.source`
  becomes the actual consumed topic; a separate logical `source_id` means topic
  renames don't break lineage.
- Relay subscribes to an explicit topic list; adds a canonical `deblob-source-id`
  header ONLY after stripping all inbound reserved headers.
- Every relay transaction emits a payload-free, deterministic **`LineageObservation`**
  alongside tagged / discovery / quarantine output.
- **Separate two kinds:** runtime source *observations* vs immutable, governed lineage
  *assertions* created during promotion.
- Source contribution = **many-to-many edges** with bounded counts, timestamps, and
  first/last Kafka coordinates.
- Full chain preserved: `source → candidate → schema revision → family/version →
  silver contract revision → transform revision → umbrella revision`.
- Gold distinguishes **`eligible_source_ids`** (covered by accepted transforms) vs
  **`observed_source_ids`** (actually produced matching records).
- Field lineage uses stable field ids + `canonical_field_id` + closed operator codes —
  never raw paths or SLM prose.

## Three concrete code gaps Hermes found [H]
1. The relay sets `DiscoveryMsg.source = cfg.raw_topic` instead of the consumed
   record's ACTUAL topic → `source = msg.topic()`.
2. `ColdLane` receives `SampleMeta { source, cursor }` but DISCARDS the cursor and
   persists neither source nor cursor → persist both on the candidate.
3. Structural resolution + generalized candidate clustering are GLOBAL → a false-merge
   risk once several collectors are live. **Source-scope retrieval + cluster aliases:**
   `resolve_structural(source_scope_id, bucket_key, raw_fp)`,
   `cluster_alias(source_scope_id, generalized_fp)`. Cross-source convergence is
   deferred to the governed family/umbrella path — *source co-occurrence is provenance,
   not semantic evidence.*

## Prior art [H]
OpenLineage (runtime vs design lineage), W3C PROV (entity/activity), OpenLineage +
DataHub field-level lineage, Marquez bounded graph traversal — none bypasses Deblob's
deterministic gates.

## Implementation staging (Claude — risk-ordered)
The full model touches Deblob's core (records, relay, coldlane, matcher keys). Staged
so the exactly-once core stays stable:
- **Stage L1 (safe, high value, do now):** per-source topics + relay multi-subscribe +
  `Envelope.source = msg.topic()` (gap 1); persist `source` (+cursor) on
  `CandidateRecord`/`SchemaRecord` (gap 2); the live-stream SSE tap; lineage on cards +
  a read `/lineage` API. Delivers real per-source visibility.
- **Stage L2 (deferred, core surgery):** source-scoped clustering keys (gap 3) — changes
  `resolve_structural`/`cluster_alias` signatures + the matcher/registry; the
  false-merge hardening. Governed lineage *assertions* on umbrella promotion. The `src_`
  id registry + topic→source_id bindings + field-level lineage. Each is its own reviewed
  change; none blocks L1's visibility.

## Open questions (for the merge)
- Does the SSE tap live in the relay process or a separate consumer of a
  `deblob.stream` topic (decouples the hot path further, at the cost of a topic)?
- Retain a short rolling history for the stream (last N events) so a newly-opened
  view isn't empty, or live-only?
- Sampling under high volume: emit every event, or 1-in-K with a sampled flag?
- Where does `source` bind on the StreamEvent before (2) lands — omit vs `events.raw`?
