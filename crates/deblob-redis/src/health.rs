//! Runtime persistence health monitoring (Task 10, spec §6).
//!
//! The startup gate in `RedisRegistry::connect` (Task 9) only checks
//! persistence state once, at connect time. It cannot see disk exhaustion,
//! an AOF write failure, or an operator running `CONFIG SET appendonly no`
//! (or flipping `maxmemory-policy` to an eviction policy) hours into a live
//! process. This module adds a background probe that polls Redis on an
//! interval and flips a cheap, shared [`HealthGate`] flag; `RedisRegistry::
//! publish` reads that flag (an atomic load, never a Redis round trip) and
//! refuses to write while it is degraded — freezing promotions rather than
//! risking a publish Redis can't durably keep. `/readyz` (Task 12) is meant
//! to read the same gate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

/// Health of the Redis persistence layer, as last observed by the
/// background probe (or, at startup, assumed healthy — `RedisRegistry::
/// connect`'s own gate already checked before a [`HealthGate`] is wired
/// in).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthState {
    Ok,
    /// Carries a human-readable reason: which check failed and the raw
    /// value observed, so an operator staring at a frozen-promotions alert
    /// doesn't have to go re-run `INFO persistence` themselves.
    Degraded(String),
}

impl HealthState {
    pub fn is_ok(&self) -> bool {
        matches!(self, HealthState::Ok)
    }
}

/// Parses `INFO persistence`'s `key:value\r\n`-per-line reply into a
/// lookup table. Ignores section headers (`# Persistence`) and blank
/// lines; tolerant of both `\n` and `\r\n` line endings since `str::lines`
/// already strips a trailing `\r`.
fn parse_info(info: &str) -> std::collections::HashMap<&str, &str> {
    info.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            line.split_once(':')
        })
        .collect()
}

/// Pure decision function, deliberately kept Redis-free so it can be
/// unit-tested against captured/sample `INFO persistence` strings without a
/// live connection. Checks (spec §6):
///   - `aof_enabled:1` — AOF persistence must be turned on;
///   - `aof_last_write_status:ok` — the last AOF write (fsync) must have
///     succeeded (a stuck disk or ENOSPC surfaces here as `err`);
///   - `rdb_last_bgsave_status:ok` — the last RDB background save must have
///     succeeded;
///   - `maxmemory-policy == noeviction` — any eviction policy risks Redis
///     silently dropping schema data under memory pressure instead of
///     erroring, which this vault can never tolerate.
///
/// Any single failure is fatal: this returns `Degraded` with a reason
/// naming the specific field and the value actually observed.
pub fn evaluate_persistence(info_persistence: &str, maxmemory_policy: &str) -> HealthState {
    let fields = parse_info(info_persistence);

    match fields.get("aof_enabled").copied() {
        Some("1") => {}
        Some(other) => {
            return HealthState::Degraded(format!(
                "aof_enabled:{other} (AOF persistence is disabled)"
            ))
        }
        None => {
            return HealthState::Degraded("aof_enabled missing from INFO persistence".to_string())
        }
    }

    match fields.get("aof_last_write_status").copied() {
        Some("ok") => {}
        Some(other) => {
            return HealthState::Degraded(format!(
                "aof_last_write_status:{other} (AOF write failing — check disk space)"
            ))
        }
        None => {
            return HealthState::Degraded(
                "aof_last_write_status missing from INFO persistence".to_string(),
            )
        }
    }

    match fields.get("rdb_last_bgsave_status").copied() {
        Some("ok") => {}
        Some(other) => {
            return HealthState::Degraded(format!(
                "rdb_last_bgsave_status:{other} (RDB background save failing)"
            ))
        }
        None => {
            return HealthState::Degraded(
                "rdb_last_bgsave_status missing from INFO persistence".to_string(),
            )
        }
    }

    if maxmemory_policy != "noeviction" {
        return HealthState::Degraded(format!(
            "maxmemory-policy:{maxmemory_policy} (must be noeviction — an eviction policy can silently drop schema data)"
        ));
    }

    HealthState::Ok
}

/// Runs the live Redis checks `evaluate_persistence` decides over.
pub struct PersistenceHealth;

impl PersistenceHealth {
    /// Runs `INFO persistence` and `CONFIG GET maxmemory-policy` against
    /// `conn` and evaluates the result via `evaluate_persistence`. Any
    /// command failure (connection dropped, command error) is itself a
    /// `Degraded` result — a probe that can't reach Redis is exactly the
    /// kind of failure this gate exists to catch.
    pub async fn probe(mut conn: redis::aio::MultiplexedConnection) -> HealthState {
        let info_persistence: String = match redis::cmd("INFO")
            .arg("persistence")
            .query_async(&mut conn)
            .await
        {
            Ok(info) => info,
            Err(e) => return HealthState::Degraded(format!("INFO persistence failed: {e}")),
        };

        let maxmemory_policy_reply: Vec<String> = match redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory-policy")
            .query_async(&mut conn)
            .await
        {
            Ok(reply) => reply,
            Err(e) => {
                return HealthState::Degraded(format!("CONFIG GET maxmemory-policy failed: {e}"))
            }
        };
        let maxmemory_policy = maxmemory_policy_reply.get(1).cloned().unwrap_or_default();

        evaluate_persistence(&info_persistence, &maxmemory_policy)
    }
}

/// A cheap, `Clone`-able shared flag: `RedisRegistry::publish` reads it via
/// an atomic load on every call (never a Redis round trip), while a
/// background tokio task flips it by running `PersistenceHealth::probe`
/// every `probe_interval`. `/readyz` (Task 12) is meant to share the same
/// `HealthGate` instance.
#[derive(Clone)]
pub struct HealthGate {
    healthy: Arc<AtomicBool>,
}

impl HealthGate {
    /// A fresh gate starts healthy. This is safe because a `HealthGate` is
    /// only ever wired in after `RedisRegistry::connect`'s own startup gate
    /// has already verified persistence (Task 9) — the background probe
    /// spawned via `spawn_probe` will correct this within one interval if
    /// that assumption doesn't hold.
    pub fn new() -> Self {
        Self {
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Cheap atomic load — safe to call on every `publish`.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn set(&self, state: &HealthState) {
        self.healthy.store(state.is_ok(), Ordering::Relaxed);
    }

    /// Test-only escape hatch: forces the gate degraded without running a
    /// probe (or needing a live Redis connection at all), so `RedisRegistry
    /// ::publish`'s freeze behaviour can be exercised in isolation.
    pub fn force_degraded_for_test(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }

    /// Spawns the background probe loop: every `interval`, runs
    /// `PersistenceHealth::probe(conn)` and stores the result. Returns the
    /// `JoinHandle` so a caller can hold onto it (and `.abort()` it on
    /// shutdown) — dropping the handle does NOT stop the task, since tokio
    /// tasks run detached by default.
    ///
    /// `interval` is deliberately a parameter rather than a hardcoded
    /// constant: production wants ~10s (spec §6), tests want milliseconds
    /// so they don't have to sleep for real seconds.
    pub fn spawn_probe(
        &self,
        conn: redis::aio::MultiplexedConnection,
        interval: Duration,
    ) -> JoinHandle<()> {
        let gate = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let state = PersistenceHealth::probe(conn.clone()).await;
                gate.set(&state);
            }
        })
    }
}

impl Default for HealthGate {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HealthGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthGate")
            .field("healthy", &self.is_healthy())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEALTHY: &str = "# Persistence\r\n\
loading:0\r\n\
async_loading:0\r\n\
rdb_changes_since_last_save:0\r\n\
rdb_bgsave_in_progress:0\r\n\
rdb_last_save_time:1700000000\r\n\
rdb_last_bgsave_status:ok\r\n\
rdb_last_bgsave_time_sec:0\r\n\
rdb_current_bgsave_time_sec:-1\r\n\
aof_enabled:1\r\n\
aof_rewrite_in_progress:0\r\n\
aof_rewrite_scheduled:0\r\n\
aof_last_rewrite_time_sec:-1\r\n\
aof_current_rewrite_time_sec:-1\r\n\
aof_last_bgrewrite_status:ok\r\n\
aof_last_write_status:ok\r\n\
aof_last_cow_size:0\r\n";

    #[test]
    fn healthy_info_and_noeviction_policy_is_ok() {
        assert_eq!(evaluate_persistence(HEALTHY, "noeviction"), HealthState::Ok);
    }

    #[test]
    fn aof_disabled_is_degraded() {
        let info = HEALTHY.replace("aof_enabled:1", "aof_enabled:0");
        let state = evaluate_persistence(&info, "noeviction");
        match state {
            HealthState::Degraded(reason) => {
                assert!(reason.contains("aof_enabled"), "reason: {reason}");
            }
            HealthState::Ok => panic!("expected Degraded, got Ok"),
        }
    }

    #[test]
    fn aof_last_write_status_err_is_degraded() {
        let info = HEALTHY.replace("aof_last_write_status:ok", "aof_last_write_status:err");
        let state = evaluate_persistence(&info, "noeviction");
        match state {
            HealthState::Degraded(reason) => {
                assert!(reason.contains("aof_last_write_status"), "reason: {reason}");
            }
            HealthState::Ok => panic!("expected Degraded, got Ok"),
        }
    }

    #[test]
    fn rdb_last_bgsave_status_err_is_degraded() {
        let info = HEALTHY.replace("rdb_last_bgsave_status:ok", "rdb_last_bgsave_status:err");
        let state = evaluate_persistence(&info, "noeviction");
        match state {
            HealthState::Degraded(reason) => {
                assert!(
                    reason.contains("rdb_last_bgsave_status"),
                    "reason: {reason}"
                );
            }
            HealthState::Ok => panic!("expected Degraded, got Ok"),
        }
    }

    #[test]
    fn wrong_eviction_policy_is_degraded() {
        let state = evaluate_persistence(HEALTHY, "allkeys-lru");
        match state {
            HealthState::Degraded(reason) => {
                assert!(reason.contains("maxmemory-policy"), "reason: {reason}");
            }
            HealthState::Ok => panic!("expected Degraded, got Ok"),
        }
    }

    #[test]
    fn missing_aof_enabled_field_is_degraded() {
        let info = "# Persistence\r\nrdb_last_bgsave_status:ok\r\naof_last_write_status:ok\r\n";
        let state = evaluate_persistence(info, "noeviction");
        assert!(!state.is_ok());
    }

    #[test]
    fn gate_starts_healthy_and_force_degraded_flips_it() {
        let gate = HealthGate::new();
        assert!(gate.is_healthy());
        gate.force_degraded_for_test();
        assert!(!gate.is_healthy());
    }
}
