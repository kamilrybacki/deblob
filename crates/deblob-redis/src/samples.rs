//! Redis backend for the redacted troubleshooting [`SampleStore`] (joint
//! design `dc-samples-dlp-1907`, Stage 2).
//!
//! MUST target a DEDICATED, VOLATILE Redis instance (persistence disabled, no
//! backups/replicas) — the permanent vault's RDB/AOF/snapshots would outlive
//! the retention TTL and defeat the privacy promise. `connect` is deliberately
//! separate from every other store's connection for that reason.
//!
//! Layout, per candidate:
//!   * `deblob:samples:z:<cand>`  ZSET  score=captured_at_ms  member=sample_id
//!   * `deblob:samples:h:<cand>`  HASH  field=sample_id       value=record json
//!
//! Storage is a single atomic Lua script: idempotent insert (`ZADD NX` on the
//! `sample_id`, so an at-least-once consumer replay is a no-op), then age-prune
//! (server-side `TIME`, no client clock skew) AND count-prune (keep newest N),
//! then refresh both keys' TTL as a cleanup safety-net. `LPUSH+LTRIM+EXPIRE`
//! was rejected: refreshing a list TTL on every append keeps old members alive
//! far past the per-item retention (Hermes review §7).

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{SampleRecord, SampleStore};
use redis::Script;

use crate::registry::redis_err;

/// `KEYS[1]`=zset `KEYS[2]`=hash · `ARGV`: sample_id, record_json,
/// retention_ms, max_count, ttl_secs. Returns 1 if newly inserted, 0 if replay.
///
/// The ZSET score is the SERVER's `TIME` (not a client-supplied `captured_at`),
/// so age-pruning compares scores and the retention cutoff against the SAME
/// clock — immune to client/server skew (Hermes review §7). `ZADD NX` keeps the
/// original score on a replay, so retention is measured from FIRST capture.
const PUT_SAMPLE_LUA: &str = r#"
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local added = redis.call('ZADD', KEYS[1], 'NX', now_ms, ARGV[1])
if added == 1 then
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
end
local cutoff = now_ms - tonumber(ARGV[3])
local old = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', '(' .. cutoff)
if #old > 0 then
  redis.call('ZREM', KEYS[1], unpack(old))
  redis.call('HDEL', KEYS[2], unpack(old))
end
local card = redis.call('ZCARD', KEYS[1])
local maxc = tonumber(ARGV[4])
if card > maxc then
  local rm = redis.call('ZRANGE', KEYS[1], 0, card - maxc - 1)
  if #rm > 0 then
    redis.call('ZREM', KEYS[1], unpack(rm))
    redis.call('HDEL', KEYS[2], unpack(rm))
  end
end
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return added
"#;

/// Retention/bounding options for [`RedisSampleStore`].
#[derive(Debug, Clone, Copy)]
pub struct SampleStoreOpts {
    pub max_per_candidate: usize,
    pub retention_secs: u64,
    /// Key TTL (cleanup safety-net; should exceed `retention_secs`).
    pub key_ttl_secs: u64,
}

pub struct RedisSampleStore {
    conn: redis::aio::ConnectionManager,
    put_script: Script,
    opts: SampleStoreOpts,
}

impl std::fmt::Debug for RedisSampleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSampleStore").field("opts", &self.opts).finish_non_exhaustive()
    }
}

fn zkey(c: &CandidateId) -> String {
    format!("deblob:samples:z:{}", c.as_str())
}
fn hkey(c: &CandidateId) -> String {
    format!("deblob:samples:h:{}", c.as_str())
}

impl RedisSampleStore {
    /// Connect to the DEDICATED volatile sample Redis. A distinct instance
    /// from the vault — see module docs on why a separate DB number is not
    /// enough (RDB/AOF are instance-wide).
    pub async fn connect(url: &str, opts: SampleStoreOpts) -> Result<Self, CoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid sample redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("sample redis connect: {e}")))?;
        Ok(Self { conn, put_script: Script::new(PUT_SAMPLE_LUA), opts })
    }
}

#[async_trait]
impl SampleStore for RedisSampleStore {
    async fn put_sample(&self, sample: &SampleRecord) -> Result<bool, CoreError> {
        let record = serde_json::to_string(sample)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize sample: {e}")))?;
        let mut conn = self.conn.clone();
        let added: i64 = self
            .put_script
            .key(zkey(&sample.candidate_id))
            .key(hkey(&sample.candidate_id))
            .arg(&sample.sample_id)
            .arg(record)
            .arg((self.opts.retention_secs as i64) * 1000)
            .arg(self.opts.max_per_candidate as i64)
            .arg(self.opts.key_ttl_secs as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(added == 1)
    }

    async fn list_samples(
        &self,
        candidate_id: &CandidateId,
        limit: usize,
    ) -> Result<Vec<SampleRecord>, CoreError> {
        let mut conn = self.conn.clone();
        // Newest first.
        let ids: Vec<String> = redis::cmd("ZREVRANGE")
            .arg(zkey(candidate_id))
            .arg(0)
            .arg(limit.saturating_sub(1) as i64)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let mut hmget = redis::cmd("HMGET");
        hmget.arg(hkey(candidate_id));
        for id in &ids {
            hmget.arg(id);
        }
        let records: Vec<Option<String>> = hmget.query_async(&mut conn).await.map_err(redis_err)?;
        Ok(records
            .into_iter()
            .flatten()
            .filter_map(|j| serde_json::from_str(&j).ok())
            .collect())
    }
}
