//! Redis backend for the [`ValueProfileStore`] (joint design
//! `dc-umbrella-signals-1907`, Stage 1): one compact, immutable blob per
//! value-profile snapshot — NEVER one key per leaf (per-key overhead would
//! dominate the tiny payload).
//!
//! Layout: `deblob:value-profile:<id>` STRING = the snapshot JSON. Snapshots
//! are content-addressed and immutable, so a write is idempotent; no index
//! set is maintained (profiles are looked up by the id a `SchemaRecord`
//! already references, never enumerated).

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::ValueProfileId;
use deblob_core::ports::{ValueProfileSnapshot, ValueProfileStore};

use crate::registry::redis_err;

fn profile_key(id: &ValueProfileId) -> String {
    format!("deblob:value-profile:{}", id.as_str())
}

#[derive(Clone)]
pub struct RedisValueProfile {
    conn: redis::aio::ConnectionManager,
}

impl std::fmt::Debug for RedisValueProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedisValueProfile")
    }
}

impl RedisValueProfile {
    pub fn new(conn: redis::aio::ConnectionManager) -> Self {
        Self { conn }
    }

    /// Opens its own `ConnectionManager` (transparent reconnect after a Redis
    /// restart). No persistence gate — value profiles are governance evidence
    /// referenced by durable schemas, not the vault-of-record themselves.
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        let client = redis::Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;
        Ok(Self::new(conn))
    }
}

#[async_trait]
impl ValueProfileStore for RedisValueProfile {
    async fn put_value_profile(&self, snapshot: &ValueProfileSnapshot) -> Result<(), CoreError> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize value profile: {e}")))?;
        let mut conn = self.conn.clone();
        redis::cmd("SET")
            .arg(profile_key(&snapshot.profile_id))
            .arg(json)
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_value_profile(
        &self,
        id: &ValueProfileId,
    ) -> Result<Option<ValueProfileSnapshot>, CoreError> {
        let mut conn = self.conn.clone();
        let json: Option<String> = redis::cmd("GET")
            .arg(profile_key(id))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match json {
            Some(j) => serde_json::from_str(&j)
                .map(Some)
                .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt value profile: {e}"))),
            None => Ok(None),
        }
    }
}
