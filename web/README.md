# Deblob Console

A single-file web UI for browsing Deblob's learned schemas and operating the
discovery queue. No build step — `web/console.html` is self-contained
(inline CSS + vanilla JS, no external requests, CSP-safe).

## Views
- **Dashboard** — learned-schema counts, the false-merge safety trade, core latency.
- **How it decides** — the match / new / abstain decision and the trust-gate outcome per event shape.
- **Schema catalogue** — searchable list + canonical field-shape detail, family/version, semantic fingerprint, privacy class.
- **Families & versions** — grouped identities and their versions.
- **Similarity graph** — schemas linked by structural distance (weighted-Jaccard).
- **Discovery queue** — pending candidates with the model's proposal + gate verdict; **Promote/Reject** (governed).
- **Quarantine** — events that failed bounded parsing.
- **Health & metrics** — `/healthz`, `/readyz`, `/metrics`, measured core numbers.

## Modes
- **Demo (default):** opens with bundled representative data; mutations disabled.
- **Live:** click **Connect**, enter the mgmt API base URL (+ bearer token). The
  console reads `GET /api/v1/schemas` and `/candidates`, and — only in live
  mode, behind a confirm dialog — `POST /candidates/{id}/promote|reject`.
  Falls back to demo data if the API is unreachable.

## Serving
- **Static:** any static host, or behind the Caddy edge (Authelia-gated) — it
  makes no external requests. The mgmt API must send permissive CORS or be
  same-origin.
- **Same-origin (recommended):** serve `console.html` from a `/ui` route on the
  mgmt API listener so the browser's `Authorization` and CORS are trivial;
  base URL then defaults to the current origin.
