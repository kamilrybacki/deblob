//! Redis backend for the [`SourceRegistry`] (spec §9 lineage): a durable,
//! content-addressed registry of every data source the service has observed.
//!
//! Layout:
//!   * `deblob:source:<id>`  HASH {name, first_seen_ms, last_seen_ms}
//!   * `deblob:sources`      SET of every registered `src_` id (a maintained
//!     index so `list_sources` is one `SMEMBERS`, never a keyspace scan)
//!
//! Registration is idempotent: [`SourceId::from_source`] makes the id a pure
//! function of the name, so re-registering the same source only advances
//! `last_seen_ms` (and lowers `first_seen_ms` if an earlier sighting is
//! reported). NEVER on the hot path — see [`SourceRegistry`]'s docs.

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::SourceId;
use deblob_core::ports::{SourceRecord, SourceRegistry};

use crate::registry::redis_err;

const SOURCES_INDEX_KEY: &str = "deblob:sources";

fn source_key(id: &SourceId) -> String {
    format!("deblob:source:{}", id.as_str())
}

#[derive(Clone)]
pub struct RedisSourceRegistry {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisSourceRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisSourceRegistry")
    }
}

impl RedisSourceRegistry {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }

    /// Opens its own `ConnectionManager` (transparent reconnect after a
    /// Redis restart, like every other store here). The `src_` registry has
    /// no persistence gate of its own — it is observability/provenance, not
    /// the permanent schema vault — so this is just connect-and-go.
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self::new(conn))
    }

    async fn read(&self, id: &SourceId) -> Result<Option<SourceRecord>, CoreError> {
        let mut conn = self.conn.clone();
        let fields: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(source_key(id))
            .arg("name")
            .arg("first_seen_ms")
            .arg("last_seen_ms")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let name = match fields.first().cloned().flatten() {
            Some(n) => n,
            None => return Ok(None), // no HASH -> unregistered
        };
        let first_seen_ms = fields
            .get(1)
            .cloned()
            .flatten()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let last_seen_ms = fields
            .get(2)
            .cloned()
            .flatten()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Ok(Some(SourceRecord {
            source_id: id.clone(),
            name,
            first_seen_ms,
            last_seen_ms,
        }))
    }
}

#[async_trait]
impl SourceRegistry for RedisSourceRegistry {
    async fn register_source(
        &self,
        name: &str,
        observed_at_ms: i64,
    ) -> Result<SourceRecord, CoreError> {
        let id = SourceId::from_source(name);
        // Read-then-write: registration is off the hot path and low-
        // concurrency, so a plain read/merge/write (rather than a Lua CAS)
        // is sufficient. `first_seen_ms` only ever moves earlier, and
        // `last_seen_ms` only ever moves later, so concurrent racing writers
        // converge monotonically regardless of order.
        let existing = self.read(&id).await?;
        let (first_seen_ms, last_seen_ms) = match &existing {
            Some(r) => (r.first_seen_ms.min(observed_at_ms), r.last_seen_ms.max(observed_at_ms)),
            None => (observed_at_ms, observed_at_ms),
        };
        let mut conn = self.conn.clone();
        redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(source_key(&id))
            .arg("name")
            .arg(name)
            .arg("first_seen_ms")
            .arg(first_seen_ms)
            .arg("last_seen_ms")
            .arg(last_seen_ms)
            .ignore()
            .cmd("SADD")
            .arg(SOURCES_INDEX_KEY)
            .arg(id.as_str())
            .ignore()
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(SourceRecord {
            source_id: id,
            name: name.to_string(),
            first_seen_ms,
            last_seen_ms,
        })
    }

    async fn get_source(&self, id: &SourceId) -> Result<Option<SourceRecord>, CoreError> {
        self.read(id).await
    }

    async fn list_sources(&self) -> Result<Vec<SourceRecord>, CoreError> {
        let mut conn = self.conn.clone();
        let ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(SOURCES_INDEX_KEY)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let mut out = Vec::with_capacity(ids.len());
        for raw in ids {
            if let Ok(id) = SourceId::parse(&raw) {
                if let Some(rec) = self.read(&id).await? {
                    out.push(rec);
                }
            }
        }
        Ok(out)
    }
}
