# Deblob P2-C — HTTP Push Reverse Proxy Design Specification

- **Date:** 2026-07-15
- **Status:** Draft
- **Parent spec:** `docs/superpowers/specs/2026-07-14-deblob-design.md` (P1 §2 locked HTTP as P2; §9 lists the hardening checklist)
- **Scope:** Sub-project **C** — the second ingest transport: an HTTP push reverse proxy that tags forwarded payloads with the same deterministic core as the Kafka relay. Independent of the SLM lane (A/B).

## 1. Summary

Add HTTP as a first-class ingest transport alongside the Kafka relay. Producers POST JSON to Deblob instead of the real upstream; Deblob computes the same canonical fingerprint, attaches the schema identity in response/forward headers, forwards the (unmodified) body to a **fixed upstream allowlist**, and — for unknown shapes — feeds the durable discovery lane so the cold lane sees HTTP-ingested schemas too. It reuses the P1 tagging core (`HotMatcher`) and the two-lane architecture; only the transport adapter and its hardening are new.

Core invariant (unchanged): payload bytes are never mutated; tags ride in headers; the hot path is deterministic and never blocks on a model.

## 2. Non-goals (P2-C)

- No transformation/enrichment of the payload (identity forward only).
- No new schema-identity logic (reuse `HotMatcher` + the P1 vault/index verbatim).
- No SLM involvement (that's the async cold lane, A/B).
- No arbitrary/user-selected upstream (fixed allowlist only — SSRF prevention).
- No request buffering beyond the bounded body limit.

## 3. Architecture

### 3.1 Request flow (hot path, deterministic)

```
producer --HTTP POST /ingest--> deblob-http (axum)
  → enforce limits BEFORE reading body (Content-Length / streamed cap, decompression cap)
  → strip ALL inbound deblob-* + hop-by-hop headers (spoofing + smuggling defense)
  → read bounded body
  → HotMatcher.classify(body, limits)  [same core as the Kafka relay]
      Known    → attach deblob-schema-id: sch_…
      Unknown  → attach cand_… AND enqueue a DiscoveryMsg to the durable discovery lane
      Malformed→ 422 + deblob-quarantine-reason (do NOT forward malformed upstream)
      Registry down → attach unresolved (never cand_)
  → forward the UNMODIFIED body to the fixed upstream allowlist (reverse proxy)
      with deblob-schema-id + deblob-origin headers + a preserved/generated idempotency key
  → return the upstream's response to the producer (adding deblob-schema-id so the producer sees the tag)
```

### 3.2 Reuse

- `HotMatcher` (deblob-match) — classification, LRU, the same decision table (Known/Provisional/Unresolved/Malformed).
- The discovery lane: reuse `DiscoveryMsg` (deblob-match) and the Kafka discovery producer (deblob-kafka) so HTTP-ingested unknowns reach the cold lane exactly like Kafka ones. If Kafka isn't configured, HTTP unknowns are tagged `cand_` and forwarded but not fed to the cold lane (documented degraded mode).
- The P1 vault/index/health-gate (deblob-redis) via the same `Registry`.

### 3.3 The `deblob-http` crate (new)

`HttpProxy::run(cfg, matcher, discovery_sink, shutdown)` — an axum server bound to an ingest listen addr, SEPARATE from the management API port (§8 of P1). `HttpProxyCfg { listen_addr, upstream_allowlist: Vec<Url>, route_map, limits, timeouts, idempotency, tls }`.

## 4. Hardening (P1 §9 + Hermes bs-02 §5.8) — each a requirement + test

- **Fixed upstream allowlist** — the forward destination is chosen from a configured allowlist / route map, NEVER from a request header or path the client controls. No SSRF.
- **Body + decompression limits** — reject bodies over `max_body_bytes` (via Content-Length AND a streamed cap so a lying Content-Length can't overflow); cap decompressed size (decompression-bomb guard) if the proxy decompresses at all (prefer NOT decompressing — forward as-is and let the parser's own bounded limits apply to a bounded body).
- **Request-header limits** — cap total header size + count.
- **Slowloris / read / write / idle timeouts** — bounded read, write, and header timeouts so a slow client can't hold a connection open indefinitely.
- **Hop-by-hop header stripping** — strip `Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Proxy-*` per RFC before forwarding.
- **Request-smuggling defenses** — reject requests with BOTH `Content-Length` and `Transfer-Encoding`, or duplicate/conflicting `Content-Length`; rely on a hardened HTTP stack (hyper/axum) and reject ambiguous framing.
- **Reserved-header hygiene** — strip ALL inbound `deblob-*` / `Deblob-*` headers (case-insensitive) before tagging; write exactly one `deblob-schema-id` + one `deblob-origin`. A producer can never spoof its tag.
- **Idempotency-key contract** — accept a client `Idempotency-Key` (or generate one), forward it downstream, and document the retry contract (a successful upstream write + lost response must not silently duplicate — the key lets the upstream dedupe). Deblob itself is stateless per request; it forwards the key.
- **TLS client identity / auth (optional)** — support requiring a client cert or a bearer/API key on the ingest listener (configurable); document that in production the ingest path should be authenticated or network-isolated.
- **Downstream-before-body edge** — explicit behavior when the upstream responds before consuming the body (don't hang; bounded).

## 5. Config + wiring

- `[http_proxy]` config section (non-secret): `enabled: bool` (default false — like `[slm]`, off unless configured, so existing behavior is unchanged), `listen_addr`, `upstream_allowlist`, `route` (path → upstream mapping), `max_body_bytes`, `max_header_bytes`, timeouts, `require_auth: bool`. Any ingest auth secret (bearer) is env-only.
- Wired into `serve()`: when `[http_proxy].enabled`, spawn `HttpProxy::run` alongside the Kafka relay + discovery consumer + management API, sharing the `HotMatcher` + discovery sink + `CancellationToken`, drained in graceful shutdown. Disabled = no proxy spawned, byte-identical prior behavior.

## 6. Error handling

| Condition | Behavior |
|---|---|
| Body over limit | 413 Payload Too Large, not forwarded |
| Malformed JSON | 422 + `deblob-quarantine-reason`, not forwarded (malformed never reaches upstream) |
| Disallowed upstream / bad route | 404/502 (never forward off-allowlist) |
| Registry down | tag `unresolved`, still forward (degrade, don't block) |
| Upstream timeout/error | 502/504 with a bounded error, no hang |
| Smuggling/ambiguous framing | 400, rejected |

## 7. Crates / structure

| Crate | Change |
|---|---|
| `deblob-http` (new) | axum reverse proxy + `HttpProxy::run` + hardening; header hygiene module. |
| `deblob` (bin) | `[http_proxy]` config; wire `HttpProxy::run` into `serve()`. |
| reuse | `deblob-match` (HotMatcher, DiscoveryMsg), `deblob-kafka` (discovery producer), `deblob-redis` (Registry/health). |

## 8. Testing strategy

TDD (80%+). Unit tests for header hygiene (strip reserved + hop-by-hop), allowlist enforcement, limit checks. Integration tests with a **test upstream** (a wiremock/axum stub as the allowlisted destination): a known-shape POST → tagged + forwarded + upstream received the body with the header; an unknown-shape POST → `cand_` + a DiscoveryMsg enqueued; a malformed body → 422, upstream NOT called; an over-size body → 413; a spoofed `deblob-schema-id` inbound → stripped, replaced; a disallowed upstream/route → rejected; a both-CL-and-TE request → 400; an idempotency key → forwarded. A serve-level test that `[http_proxy].enabled=false` spawns nothing.
