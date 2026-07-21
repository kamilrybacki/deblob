//! Redis-backed `Registry`: the permanent schema vault (spec §6).

use crate::health::HealthGate;
use crate::lua::{PUBLISH_SCRIPT, SEM_APPEND_SCRIPT, SET_NAME_SCRIPT};
use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{FamilyRecord, FamilyRef, NameWriteOutcome, Registry, SchemaRecord};
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
    /// Cheaply `Clone`-able handle over a `ConnectionManager`-wrapped
    /// multiplexed connection; cloning it (once per call, see `conn()`)
    /// shares the same underlying socket rather than opening a new TCP
    /// connection per operation, and safely supports the concurrent
    /// publishers this vault must serialize inside Redis via the Lua
    /// script's atomicity, not via client-side locking. Unlike a bare
    /// `MultiplexedConnection`, `ConnectionManager` transparently
    /// reconnects when the underlying TCP connection breaks (Redis
    /// restart/network blip) instead of staying dead for the lifetime of
    /// the process (Task 19 fix, spec §10).
    conn: redis::aio::ConnectionManager,
    publish_script: Script,
    /// The atomic semantic-revision-append transition (Task 5, Hermes
    /// review §4) — see `crate::lua::SEM_APPEND_SCRIPT` and
    /// `crate::semantic` for the storage methods that invoke it.
    pub(crate) sem_append_script: Script,
    /// The atomic, governed display-name write (`jr-schema-naming-211140`) —
    /// see `crate::lua::SET_NAME_SCRIPT` and `set_schema_name`. Enforces the
    /// human-override-wins guard inside one Lua transition.
    set_name_script: Script,
    /// Runtime persistence health gate (Task 10, spec §6). `None` for
    /// registries built without one (the default, and every existing
    /// caller/test) — publishing then behaves exactly as before this task,
    /// gated only by the startup check in `connect`. `Some` freezes
    /// `publish` the moment the background probe observes a degraded
    /// state, without a Redis round trip on the hot path.
    health_gate: Option<HealthGate>,
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

/// Shared audit stream key. Reused by `crate::semantic`'s
/// `append_revision` (Task 5) — semantic-revision audit events land on the
/// SAME stream `publish` already writes to, rather than a second stream, so
/// an operator reviewing "everything that changed" doesn't have to know to
/// check two places.
pub(crate) const AUDIT_KEY: &str = "deblob:audit:log";

/// The maintained schemas-listing index (fix1): a Redis `SET` of every
/// published schema's `sch_id`. `PUBLISH_SCRIPT` `SADD`s it atomically
/// alongside the schema's own record on every publish (fresh or idempotent
/// republish), so `list_schemas` can page over this one small, dense SET —
/// O(schemas) — instead of `SCAN`ning the entire `deblob:*` keyspace for
/// sparse `deblob:schema:*` keys, which previously produced empty pages
/// with a non-zero cursor even when schemas existed (a `SCAN COUNT` batch
/// over the whole keyspace can easily land on zero schema keys among the
/// thousands of candidate/evidence/index/semantic keys sharing the same
/// prefix space). `crate::index::rebuild_index` reconstructs this SET from
/// the authoritative `deblob:schema:*` hashes, so a vault written before
/// this index existed is repairable by running it once.
pub(crate) const SCHEMA_INDEX_KEY: &str = "deblob:schemas";

pub(crate) fn redis_err(e: redis::RedisError) -> CoreError {
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

/// Overlay the governed display name (stored in SEPARATE small hash fields by
/// `SET_NAME_SCRIPT`, never re-serialized into `record`) onto the record's
/// provenance, so `provenance.label` — the field the console renders — carries
/// the human/SLM name. A no-op when the schema was never named
/// (`jr-schema-naming-211140`).
fn overlay_name(
    rec: &mut SchemaRecord,
    label: Option<String>,
    source: Option<String>,
    meta: Option<String>,
) {
    if label.is_none() && source.is_none() && meta.is_none() {
        return;
    }
    if !rec.provenance.is_object() {
        rec.provenance = serde_json::Value::Object(serde_json::Map::new());
    }
    let obj = rec
        .provenance
        .as_object_mut()
        .expect("provenance coerced to a JSON object above");
    if let Some(l) = label {
        obj.insert("label".to_string(), serde_json::Value::String(l));
    }
    if let Some(s) = source {
        obj.insert("name_source".to_string(), serde_json::Value::String(s));
    }
    if let Some(m) = meta {
        // Stored as a JSON string; re-parse to a value, or keep as a string
        // if it isn't valid JSON (never fail a read over metadata).
        let v =
            serde_json::from_str::<serde_json::Value>(&m).unwrap_or(serde_json::Value::String(m));
        obj.insert("name_meta".to_string(), v);
    }
}

impl RedisRegistry {
    /// Connect and run the startup persistence gate. This only checks the
    /// state at connect time; runtime drift monitoring (AOF write errors,
    /// disk exhaustion, `CONFIG SET` drift) is a separate concern — see
    /// `with_health_gate` and `crate::health`.
    pub async fn connect(url: &str, opts: RedisOpts) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let mut conn = client
            .get_connection_manager_with_config(crate::connection_manager_config())
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
            sem_append_script: Script::new(SEM_APPEND_SCRIPT),
            set_name_script: Script::new(SET_NAME_SCRIPT),
            health_gate: None,
        })
    }

    /// Attaches a runtime persistence [`HealthGate`] (Task 10, spec §6).
    /// Builder-style so existing callers (and every pre-Task-10 test) that
    /// never call this keep `health_gate: None` — `publish` then skips the
    /// gate check entirely, preserving prior behaviour exactly. Callers
    /// that DO want runtime monitoring construct a `HealthGate`, start its
    /// background probe via `HealthGate::spawn_probe`, and pass the same
    /// (cheaply `Clone`-able) gate here — `/readyz` (Task 12) is meant to
    /// share that same instance.
    pub fn with_health_gate(mut self, gate: HealthGate) -> Self {
        self.health_gate = Some(gate);
        self
    }

    /// A cheap clone of the shared connection manager. `redis::aio::
    /// ConnectionManager` is designed to be cloned per concurrent caller —
    /// it pipelines requests over one socket rather than opening a new
    /// connection each time, and every clone shares the same reconnect
    /// state.
    pub(crate) fn conn(&self) -> redis::aio::ConnectionManager {
        self.conn.clone()
    }

    /// Cheap accessor for `crate::semantic`'s write path, which must gate
    /// `append_revision` behind the same runtime persistence `HealthGate`
    /// `publish` already checks (spec §6, Task 10) — see `publish`'s own
    /// use of this same field for the rationale.
    pub(crate) fn health_gate(&self) -> Option<&HealthGate> {
        self.health_gate.as_ref()
    }
}

#[async_trait::async_trait]
impl Registry for RedisRegistry {
    async fn get_schema(&self, id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        let mut conn = self.conn();
        // Also pull the governed name fields (jr-schema-naming-211140) and
        // overlay them onto provenance.label — see `overlay_name`.
        let (record_json, version, name_label, name_source, name_meta): (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = redis::cmd("HMGET")
            .arg(schema_key(id))
            .arg("record")
            .arg("version")
            .arg("name_label")
            .arg("name_source")
            .arg("name_meta")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match record_json {
            None => Ok(None),
            Some(json) => {
                let mut rec = record_from_hash(&json, version)?;
                overlay_name(&mut rec, name_label, name_source, name_meta);
                Ok(Some(rec))
            }
        }
    }

    async fn set_schema_name(
        &self,
        id: &SchemaId,
        label: &str,
        source: &str,
        meta: Option<serde_json::Value>,
    ) -> Result<NameWriteOutcome, CoreError> {
        // Same persistence gate as `publish`: refuse writes while the
        // background probe reports Redis degraded.
        if let Some(gate) = &self.health_gate {
            if !gate.is_healthy() {
                return Err(CoreError::RegistryUnavailable(
                    "persistence degraded".to_string(),
                ));
            }
        }
        let mut conn = self.conn();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();
        let meta_str = match &meta {
            Some(v) => serde_json::to_string(v)
                .map_err(|e| CoreError::RegistryUnavailable(format!("serialize name meta: {e}")))?,
            None => String::new(),
        };
        let outcome: String = self
            .set_name_script
            .key(schema_key(id))
            .arg(label)
            .arg(source)
            .arg(meta_str)
            .arg(now_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(match outcome.as_str() {
            "applied" => NameWriteOutcome::Applied,
            "skipped_human" => NameWriteOutcome::SkippedHumanProtected,
            "not_found" => NameWriteOutcome::NotFound,
            other => {
                return Err(CoreError::RegistryUnavailable(format!(
                    "unexpected set_name reply: {other}"
                )))
            }
        })
    }

    async fn resolve_structural(
        &self,
        bucket_key: &str,
        fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        // The real bucketed structural index (Task 8): `bucket_key` is
        // itself the Redis key of a small per-bucket SET, so this is a
        // bounded SSCAN over that one bucket — never a scan over
        // `deblob:schema:*`. See `crate::index`.
        self.resolve_structural_bucketed(bucket_key, fingerprint)
            .await
    }

    /// Atomic publication: schema + family version + index + alias + audit
    /// (§6), performed entirely inside one server-side Lua script.
    ///
    /// `variant_members` (Task 14 fix): threaded straight into the Lua
    /// script as extra `KEYS`/`ARGV` pairs — see `crate::lua::PUBLISH_SCRIPT`
    /// docs — so every observed concrete shape is indexed into ITS OWN
    /// bucket atomically alongside the schema record itself, and the
    /// `variants` field persisted onto the schema hash lets
    /// `rebuild_index` restore them later purely from the authoritative
    /// record.
    async fn publish(
        &self,
        record: SchemaRecord,
        alias_from: &CandidateId,
        bucket_key: &str,
        variant_members: &[(String, String)],
        actor: &str,
        reason: &str,
    ) -> Result<FamilyVersion, CoreError> {
        // Task 10: runtime persistence health. This is a cheap atomic load
        // (never a Redis round trip) — the background probe spawned via
        // `HealthGate::spawn_probe` is what actually talks to Redis, on its
        // own ~10s interval. A degraded gate freezes promotions before any
        // write is attempted.
        if let Some(gate) = &self.health_gate {
            if !gate.is_healthy() {
                return Err(CoreError::RegistryUnavailable(
                    "persistence degraded".to_string(),
                ));
            }
        }

        let mut conn = self.conn();
        let schema_json = serde_json::to_string(&record)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize schema record: {e}")))?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .to_string();
        // Task 8: `bucket_key` is the real Redis key of the schema's
        // structural-index bucket set (not a member prefix within one
        // global set), and the member records the fingerprint alongside the
        // schema id so a bucket can be SSCAN-matched — see `crate::index`.
        let bucket_member = crate::index::bucket_member(&record.schema_id);

        // Task 14 fix: every observed CONCRETE shape recorded against the
        // promoted candidate gets its own SADD, into ITS OWN bucket (which
        // may differ from `bucket_key` above), plus a `variants` field on
        // the schema hash recording the full set so `rebuild_index` can
        // restore them later from the authoritative record alone.
        let variant_members_str: Vec<String> = variant_members
            .iter()
            .map(|(_, fp_b32)| crate::index::variant_member(fp_b32, &record.schema_id))
            .collect();
        let variants_json = crate::index::encode_variants_field(variant_members);

        // The Lua script is the sole authority for the version (HINCRBY on
        // fresh publish, or the previously-allocated version on an
        // idempotent republish) — `record.version` is never trusted for
        // storage; only `canonical`/`canonicalizer` gate the immutability
        // check (Fix A). KEYS[4] doubles as the bucket's Redis key (for the
        // SADD) and the value persisted to the schema hash's `bucket` field
        // (for `rebuild_index` to read back directly), since it's the same
        // string either way.
        let mut invocation = self.publish_script.prepare_invoke();
        invocation
            .key(schema_key(&record.schema_id))
            .key(family_key(&record.family_id))
            .key(alias_key(alias_from))
            .key(bucket_key)
            .key(AUDIT_KEY)
            .key(published_key(&record.schema_id))
            .key(SCHEMA_INDEX_KEY);
        for (variant_bucket, _) in variant_members {
            invocation.key(variant_bucket);
        }
        invocation
            .arg(&schema_json)
            .arg(&record.canonical)
            .arg(&record.canonicalizer)
            .arg(record.family_id.as_str())
            .arg(record.schema_id.as_str())
            .arg(&bucket_member)
            .arg(actor)
            .arg(reason)
            .arg(&now_ms)
            .arg(&variants_json);
        for member in &variant_members_str {
            invocation.arg(member);
        }

        let result: redis::RedisResult<i64> = invocation.invoke_async(&mut conn).await;

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

    /// fix1: pages over the maintained [`SCHEMA_INDEX_KEY`] SET via `SSCAN`
    /// — never `SCAN`s the whole `deblob:*` keyspace. Every member of that
    /// SET is a real `sch_id` (nothing else is ever `SADD`ed into it), so
    /// every batch `SSCAN` returns is, by construction, real schema ids —
    /// O(schemas), not O(keyspace) — which is what makes a returned page
    /// empty ONLY when there are genuinely no more schemas to return
    /// (`next_cursor` is then `None` too). A member whose `deblob:schema:*`
    /// hash is somehow missing (never expected — schemas are immutable and
    /// never deleted, spec §6) is defensively skipped rather than surfaced
    /// as an error, matching this function's pre-fix1 behaviour.
    async fn list_schemas(
        &self,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        let mut conn = self.conn();
        let start_cursor = cursor.unwrap_or_else(|| "0".to_string());
        let count = limit.max(1);
        let (next_cursor, ids): (String, Vec<String>) = redis::cmd("SSCAN")
            .arg(SCHEMA_INDEX_KEY)
            .arg(&start_cursor)
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut records = Vec::with_capacity(ids.len());
        for id in ids {
            let key = format!("deblob:schema:{id}");
            let (record_json, version, name_label, name_source, name_meta): (
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            ) = redis::cmd("HMGET")
                .arg(&key)
                .arg("record")
                .arg("version")
                .arg("name_label")
                .arg("name_source")
                .arg("name_meta")
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
            if let Some(json) = record_json {
                let mut rec = record_from_hash(&json, version)?;
                overlay_name(&mut rec, name_label, name_source, name_meta);
                records.push(rec);
            }
        }

        let next = if next_cursor == "0" {
            None
        } else {
            Some(next_cursor)
        };
        Ok((records, next))
    }

    /// deblob-p2ab Task 3 retrieval: delegates to the bucketed lookup in
    /// `crate::index` (same module that owns every other structural-index
    /// operation), which is the single source of truth for bucket-member
    /// scanning.
    async fn list_families_in_buckets(
        &self,
        bucket_keys: &[String],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        self.list_families_in_buckets_bucketed(bucket_keys).await
    }

    /// deblob-p2ab Task 3 recall fix: delegates to the prefix-scanning
    /// lookup in `crate::index` (same module that owns every other
    /// structural-index operation).
    async fn list_families_by_band_depth(
        &self,
        bands: &[u32],
        depths: &[u32],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        self.list_families_by_band_depth_bucketed(bands, depths)
            .await
    }

    /// P2-D Task 8 follow-up: a single `HGET` on the family hash's
    /// `v:<version>` field — the SAME field `crate::lua::PUBLISH_SCRIPT`
    /// writes on a fresh publish (`HSET family_key 'v:' .. version,
    /// schema_id`). `None` for a missing field OR a family hash that
    /// doesn't exist at all — Redis's `HGET` on a missing key/field both
    /// return `nil`, which `redis::AsyncCommands::hget` surfaces as `None`,
    /// never an error.
    async fn family_version_schema(
        &self,
        family_id: &FamilyId,
        version: FamilyVersion,
    ) -> Result<Option<SchemaId>, CoreError> {
        let mut conn = self.conn();
        let raw: Option<String> = conn
            .hget(family_key(family_id), format!("v:{}", version.0))
            .await
            .map_err(redis_err)?;
        raw.map(|s| {
            SchemaId::parse(&s).map_err(|e| {
                CoreError::RegistryUnavailable(format!("corrupt family version entry: {e:?}"))
            })
        })
        .transpose()
    }

    /// P2-D polish Task 2: a single `HGET` on the family hash's
    /// `next_version` field — the SAME counter `crate::lua::PUBLISH_SCRIPT`'s
    /// `HINCRBY` maintains (fresh publish -> increment; idempotent
    /// republish -> untouched). `None` for a family hash that doesn't
    /// exist at all (nothing ever published to it) — a READ-only addition,
    /// no new write, no change to `publish`'s own behaviour.
    async fn get_family(&self, family_id: &FamilyId) -> Result<Option<FamilyRecord>, CoreError> {
        let mut conn = self.conn();
        let raw: Option<String> = conn
            .hget(family_key(family_id), "next_version")
            .await
            .map_err(redis_err)?;
        raw.map(|v| {
            v.parse::<u32>()
                .map(|v| FamilyRecord {
                    family_id: family_id.clone(),
                    current_version: FamilyVersion(v),
                })
                .map_err(|e| {
                    CoreError::RegistryUnavailable(format!("corrupt family version counter: {e}"))
                })
        })
        .transpose()
    }

    /// P2-D polish Task 2: `1..=current_version`, derived from
    /// [`RedisRegistry::get_family`] rather than a second Redis round trip
    /// enumerating `v:1..v:N` — family versions are allocated via `HINCRBY`
    /// and are contiguous, never sparse (spec §6, and see
    /// `family_versions_allocate_atomically` in `registry_it.rs`), so this
    /// derivation is exact. `Ok(vec![])` for an unknown family.
    async fn list_family_versions(
        &self,
        family_id: &FamilyId,
    ) -> Result<Vec<FamilyVersion>, CoreError> {
        match self.get_family(family_id).await? {
            None => Ok(vec![]),
            Some(fam) => Ok((1..=fam.current_version.0).map(FamilyVersion).collect()),
        }
    }
}
