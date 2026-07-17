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

## Views (updated)
Nav order leads with the **Decision Explorer** (the fastest way to understand
Deblob), then the learned data, then governance:
Decision explorer · Schema catalogue · Discovery queue · Families · Similarity ·
**Model & learning** (active bundle digest, promotion gate, feedback loop,
rollback target) · **Operations** (health, deps, relay throughput/lag/txn) ·
Quarantine.

## Security
- The bearer **token is held in `sessionStorage` (this tab only), never in
  `localStorage` or on disk**; the API base URL (not secret) is in localStorage.
- No inline event handlers — event delegation only, so a strict CSP
  (`connect-src 'self'`, no `unsafe-inline` for scripts) works.
- Promote/reject are enabled **only** when live AND a token is present; each
  requires a typed reason, promotion additionally requires typing the candidate
  id to confirm. A confirm dialog is UX protection, not authorization — the API
  must still enforce auth, state-transition legality, and audit attribution.
- Three-state banner: **DEMO DATA** / **LIVE · READ-ONLY** / **LIVE · MUTATION**.

## Serving (recommended shape)
Caddy-served static SPA with a same-origin API proxy, all behind Authelia:
```
browser → Cloudflare → Caddy + Authelia
   ├── /            static console.html   (no-store while iterating)
   └── /api/v1/*    reverse_proxy → Deblob management API
```
Same-origin avoids CORS/preflight and keeps the `Authorization` header simple.
Gotchas: preserve `X-Forwarded-Proto: https` through the proxy chain (else
Authelia may reject the target); never embed a token in the HTML/JS/Caddyfile;
protect `/metrics` (it reveals topology); if UI and API are cross-origin,
configure CORS narrowly — never `*` with `Authorization`. Serving from a `/ui`
route on the mgmt API also works for a quick demo (naturally same-origin), but
the Caddy split keeps UI deploys independent of API releases.
