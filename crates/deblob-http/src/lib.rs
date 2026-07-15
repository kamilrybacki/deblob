//! `deblob-http` — the HTTP push reverse-proxy ingest transport (spec
//! `docs/superpowers/specs/2026-07-15-deblob-p2c-http-proxy.md`).
//!
//! Task 1 (this crate's initial scope): the crate scaffold + the
//! tag-and-forward core + header hygiene. Producers POST a JSON body to
//! [`proxy::HttpProxy::run`]'s listener instead of the real upstream;
//! the body is classified via the SAME [`deblob_match::matcher::HotMatcher`]
//! the Kafka relay uses (spec §3.2 reuse — no new schema-identity logic
//! lives here), tagged with exactly one `deblob-schema-id` +
//! `deblob-origin` header pair, and forwarded byte-for-byte to a fixed,
//! config-supplied upstream (never a client-controlled destination —
//! SSRF prevention, spec §4).
//!
//! Hardening (body/header limits, allowlist enforcement, malformed 422,
//! request-smuggling defenses, timeouts) is Task 2; the discovery-lane
//! feed for unknown shapes + idempotency-key contract is Task 3; the
//! `[http_proxy]` config section + `serve()` wiring is Task 4. This crate
//! stays additive-only through all four tasks — nothing here changes
//! behavior for a deployment that never enables `[http_proxy]`.

pub mod headers;
pub mod limits;
pub mod proxy;

pub use proxy::{DiscoveryError, DiscoverySink, HttpProxy, HttpProxyCfg, HttpProxyError};
