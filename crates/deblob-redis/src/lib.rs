//! `deblob-redis`: the Redis-backed permanent schema vault (spec §6).
//!
//! Publication is a single atomic Lua transition — schema record, family
//! version, structural index entry, alias, and audit event all commit
//! together or not at all. See [`lua::PUBLISH_SCRIPT`] for the invariants
//! it enforces (write-once schema bytes, write-once alias, atomic family
//! version allocation).

pub mod evidence;
pub mod feedback_store;
pub mod health;
pub mod index;
pub mod lua;
pub mod registry;
pub mod semantic;
pub mod umbrella;

pub use evidence::{RedisEvidence, RedisEvidenceOpts};
pub use feedback_store::{
    assign_split, ExportCaps, ExportManifest, FeedbackStore, ManifestEntry, RedisFeedbackStore,
    SplitName, DEFAULT_FEEDBACK_STREAM_MAXLEN, FEEDBACK_STREAM_KEY, QUARANTINED_ACTORS_KEY,
    SAFETY_SUITE_DEDUP_PREFIX,
};
pub use health::{HealthGate, HealthState, PersistenceHealth};
pub use umbrella::RedisUmbrella;
pub use index::{bucket_key, bucket_member};
pub use registry::{RedisOpts, RedisRegistry};
pub use semantic::SEM_INDEX_KEY_PATTERN;

/// Shared [`redis::aio::ConnectionManagerConfig`] for every long-lived Redis
/// connection this crate hands out — `RedisRegistry`, `RedisEvidence`, and
/// (via `serve.rs`, which builds its own probe connection with this same
/// config) the runtime `HealthGate` probe.
///
/// `ConnectionManager` transparently reconnects a broken connection instead
/// of staying dead forever like the plain `MultiplexedConnection` it
/// replaced (Task 19 fix — see spec §10's outage-recovery requirement). Two
/// things must both hold, and the tuning here is what makes them hold:
///
///   - **Fast-fail during an outage**: `response_timeout` bounds how long
///     any single command can block waiting for a reply. Without this, a
///     command issued while Redis is unreachable but the socket hasn't
///     visibly closed (e.g. a network black hole rather than an immediate
///     TCP reset) would hang indefinitely — stalling the hot-path relay
///     instead of promptly tagging `unresolved` (spec §10). Two seconds is
///     comfortably inside every caller's own timeout budget (the e2e test's
///     shortest is 15s) while still being "prompt" on a human timescale.
///   - **Recovery once Redis is back**: `connection_timeout` bounds how
///     long each individual reconnect attempt's TCP handshake can take.
///     The retry *count*/backoff (`number_of_retries`/`factor`/
///     `exponent_base`) is deliberately left at the crate's defaults (6
///     retries, exponentially backing off from ~100ms): `ConnectionManager`
///     restarts a fresh retry sequence on every subsequent failing command,
///     not just once at the moment the connection first dropped, so a
///     single sequence exhausting its retries before Redis comes back is
///     not fatal — the next command that touches the connection simply
///     tries again from scratch.
pub fn connection_manager_config() -> redis::aio::ConnectionManagerConfig {
    redis::aio::ConnectionManagerConfig::new()
        .set_response_timeout(std::time::Duration::from_secs(2))
        .set_connection_timeout(std::time::Duration::from_secs(2))
}
