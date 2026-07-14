//! Redis-backed `Registry`: the permanent schema vault (spec §6).

use crate::lua::PUBLISH_SCRIPT;
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use redis::{AsyncCommands, Client, Script};
use std::time::{SystemTime, UNIX_EPOCH};

/// Startup options for [`RedisRegistry::connect`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RedisOpts {
    /// Allow connecting to a Redis instance that has AOF persistence
    /// disabled. Spec §6: "refuse non-persistent Redis unless
    /// `--unsafe-volatile`." Off by default; flipping it on is an explicit,
    /// documented risk acceptance (e.g. ephemeral test containers).
    pub allow_volatile: bool,
}

/// The permanent schema vault. All mutation goes through one atomic Lua
/// publication script (spec §6) — no partial publication is ever visible,
/// even under a crash mid-script or concurrent publishers.
pub struct RedisRegistry {
    /// Cheaply `Clone`-able handle over one multiplexed connection; cloning
    /// it (once per call, see `conn()`) shares the same underlying socket
    /// rather than opening a new TCP connection per operation, and safely
    /// supports the concurrent publishers this vault must serialize inside
    /// Redis via the Lua script's atomicity, not via client-side locking.
    conn: redis::aio::MultiplexedConnection,
    publish_script: Script,
}

impl std::fmt::Debug for RedisRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisRegistry").finish_non_exhaustive()
    }
}

fn schema_key(id: &SchemaId) -> String {
    format!("deblob:schema:{}", id.as_str())
}

fn family_key(id: &FamilyId) -> String {
    format!("deblob:family:{}", id.as_str())
}

fn alias_key(id: &CandidateId) -> String {
    format!("deblob:alias:{}", id.as_str())
}

fn published_key(id: &SchemaId) -> String {
    format!("deblob:published:{}", id.as_str())
}

const INDEX_KEY: &str = "deblob:index:structural";
const AUDIT_KEY: &str = "deblob:audit:log";

fn redis_err(e: redis::RedisError) -> CoreError {
    CoreError::RegistryUnavailable(e.to_string())
}

/// Maps the Lua script's `redis.error_reply` sentinels back onto the
/// `CoreError` taxonomy the rest of the system expects.
fn map_script_error(e: redis::RedisError) -> CoreError {
    let msg = e.to_string();
    if msg.contains("IMMUTABILITY") {
        CoreError::ImmutabilityViolation(format!(
            "canonical identity differs from stored record: {msg}"
        ))
    } else if msg.contains("ALIAS_CONFLICT") {
        CoreError::Conflict(format!("alias already points at a different schema: {msg}"))
    } else {
        CoreError::RegistryUnavailable(msg)
    }
}

/// Reconstructs a `SchemaRecord` from the schema hash's `record` blob,
/// overwriting its `version` field with the AUTHORITATIVE version stored
/// separately by the publication script (§6 Fix B) — the version baked
/// into the stored `record` JSON is whatever the original caller guessed
/// and must never be trusted.
fn record_from_hash(record_json: &str, version: Option<String>) -> Result<SchemaRecord, CoreError> {
    let mut value: serde_json::Value = serde_json::from_str(record_json)
        .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt schema record: {e}")))?;
    if let Some(v) = version {
        let v: u32 = v
            .parse()
            .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt schema version: {e}")))?;
        value["version"] = serde_json::Value::from(v);
    }
    serde_json::from_value(value)
        .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt schema record: {e}")))
}

impl RedisRegistry {
    /// Connect and run the startup persistence gate. Runtime drift
    /// monitoring (AOF write errors, `CONFIG SET` drift) is Task 10 — this
    /// only checks the state at connect time.
    pub async fn connect(url: &str, opts: RedisOpts) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;

        let appendonly_reply: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("appendonly")
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                CoreError::RegistryUnavailable(format!("CONFIG GET appendonly failed: {e}"))
            })?;
        let appendonly = appendonly_reply
            .get(1)
            .cloned()
            .unwrap_or_else(|| "no".to_string());

        if appendonly == "no" && !opts.allow_volatile {
            return Err(CoreError::RegistryUnavailable(
                "redis persistence disabled; pass allow_volatile to override".to_string(),
            ));
        }

        Ok(Self {
            conn,
            publish_script: Script::new(PUBLISH_SCRIPT),
        })
    }

    /// A cheap clone of the shared multiplexed connection. `redis::aio::
    /// MultiplexedConnection` is designed to be cloned per concurrent
    /// caller — it pipelines requests over one socket rather than opening a
    /// new connection each time.
    fn conn(&self) -> redis::aio::MultiplexedConnection {
        self.conn.clone()
    }
}

#[async_trait::async_trait]
impl Registry for RedisRegistry {
    async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        let mut conn = self.conn();
        let (record_json, version): (Option<String>, Option<String>) = redis::cmd("HMGET")
            .arg(schema_key(id))
            .arg("record")
            .arg("version")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match record_json {
            None => Ok(None),
            Some(json) => record_from_hash(&json, version).map(Some),
        }
    }

    async fn resolve_structural(
        &self,
        bucket_key: &str,
        fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        // Task 8 owns the real bucketed structural index; Task 7 only wires
        // the SADD side of publication into `deblob:index:structural` using
        // the same "<bucket_key>:<schema_id>" member shape checked here.
        let mut conn = self.conn();
        let member = format!("{bucket_key}:{}", fingerprint.as_str());
        let is_member: bool = conn.sismember(INDEX_KEY, member).await.map_err(redis_err)?;
        Ok(is_member.then(|| fingerprint.clone()))
    }

    /// Atomic publication: schema + family version + index + alias + audit
    /// (§6), performed entirely inside one server-side Lua script.
    async fn publish(
        &self,
        record: SchemaRecord,
        alias_from: &CandidateId,
        bucket_key: &str,
        actor: &str,
        reason: &str,
    ) -> Result<FamilyVersion, CoreError> {
        let mut conn = self.conn();
        let schema_json = serde_json::to_string(&record)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize schema record: {e}")))?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();
        let bucket_member = format!("{bucket_key}:{}", record.schema_id.as_str());

        // The Lua script is the sole authority for the version (HINCRBY on
        // fresh publish, or the previously-allocated version on an
        // idempotent republish) — `record.version` is never trusted for
        // storage; only `canonical`/`canonicalizer` gate the immutability
        // check (Fix A).
        let result: redis::RedisResult<i64> = self
            .publish_script
            .key(schema_key(&record.schema_id))
            .key(family_key(&record.family_id))
            .key(alias_key(alias_from))
            .key(INDEX_KEY)
            .key(AUDIT_KEY)
            .key(published_key(&record.schema_id))
            .arg(&schema_json)
            .arg(&record.canonical)
            .arg(&record.canonicalizer)
            .arg(record.family_id.as_str())
            .arg(record.schema_id.as_str())
            .arg(&bucket_member)
            .arg(actor)
            .arg(reason)
            .arg(&now_ms)
            .invoke_async(&mut conn)
            .await;

        result
            .map(|version| FamilyVersion(version as u32))
            .map_err(map_script_error)
    }

    async fn get_alias(&self, id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
        let mut conn = self.conn();
        let raw: Option<String> = conn.get(alias_key(id)).await.map_err(redis_err)?;
        raw.map(|s| {
            SchemaId::parse(&s)
                .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt alias target: {e:?}")))
        })
        .transpose()
    }

    async fn list_schemas(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        let mut conn = self.conn();
        let start_cursor = cursor.unwrap_or_else(|| "0".to_string());
        let count = limit.max(1);
        let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
            .arg(&start_cursor)
            .arg("MATCH")
            .arg("deblob:schema:*")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            let (record_json, version): (Option<String>, Option<String>) = redis::cmd("HMGET")
                .arg(&key)
                .arg("record")
                .arg("version")
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
            if let Some(json) = record_json {
                records.push(record_from_hash(&json, version)?);
            }
        }

        let next = if next_cursor == "0" {
            None
        } else {
            Some(next_cursor)
        };
        Ok((records, next))
    }
}
