//! Redis-backed `EvidenceStore`: the candidate lifecycle (spec §6, Task 9).
//!
//! Candidates are cheap and short-lived — unlike the permanent schema vault
//! (`crate::registry`), every `deblob:candidate:<id>` key carries an
//! `EXPIRE` so an abandoned candidate simply falls out of Redis on its own.
//! The one thing that must NEVER expire alongside it is provenance: the
//! very first sighting of a candidate is recorded once, permanently, under
//! `deblob:candidate-audit:<id>` — so even after the candidate itself has
//! long since expired, "did we ever see this shape, and when" remains
//! answerable. Evidence samples accumulate on a bounded Redis STREAM
//! (`deblob:evidence:<id>`, `XTRIM MAXLEN ~ 1000`) so a chatty candidate can
//! never grow Redis memory without bound.

use crate::lua::SET_STATE_SCRIPT;
use crate::registry::{redis_err, RedisOpts};
use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
use redis::{Client, Script};

/// Default candidate TTL: 7 days, in seconds (spec §6).
pub const DEFAULT_CANDIDATE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Evidence stream entries are (approximately) trimmed to this many most
/// recent samples per candidate — bounded growth regardless of how chatty a
/// candidate is (spec §6).
const EVIDENCE_STREAM_MAXLEN: u64 = 1000;

/// Startup options for [`RedisEvidence::connect`].
#[derive(Debug, Clone, Copy)]
pub struct RedisEvidenceOpts {
    /// How long a `deblob:candidate:<id>` key survives without being
    /// refreshed by another `upsert_candidate` call, in seconds. Defaults
    /// to 7 days. The permanent audit stub at `deblob:candidate-audit:<id>`
    /// NEVER carries this (or any) TTL — see module docs.
    pub candidate_ttl_secs: u64,
}

impl Default for RedisEvidenceOpts {
    fn default() -> Self {
        Self {
            candidate_ttl_secs: DEFAULT_CANDIDATE_TTL_SECS,
        }
    }
}

/// The evidence store: ephemeral candidate records + their bounded evidence
/// streams, backed by a permanent (never-expiring) audit trail of first
/// sightings.
pub struct RedisEvidence {
    /// Cheaply `Clone`-able handle over one multiplexed connection — see
    /// `RedisRegistry::conn` for why sharing it this way is safe.
    conn: redis::aio::MultiplexedConnection,
    candidate_ttl_secs: u64,
    set_state_script: Script,
}

impl std::fmt::Debug for RedisEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEvidence").finish_non_exhaustive()
    }
}

fn candidate_key(id: &CandidateId) -> String {
    format!("deblob:candidate:{}", id.as_str())
}

fn candidate_audit_key(id: &CandidateId) -> String {
    format!("deblob:candidate-audit:{}", id.as_str())
}

fn evidence_key(id: &CandidateId) -> String {
    format!("deblob:evidence:{}", id.as_str())
}

/// Cold-lane cluster-alias key (Task 14, spec §4): maps a hex-encoded
/// generalized fingerprint onto the one candidate its variants converge on.
fn cluster_key(gen_fp: &str) -> String {
    format!("deblob:cluster:{gen_fp}")
}

/// Task 14 fix: the set of every distinct CONCRETE shape observed for a
/// candidate, recorded by `ColdLane::ingest` so `Promoter::promote` can
/// replay them into the structural index at publish time. Members are
/// `"<bucket_key>=<fp_b32>"` — see `add_variant`/`get_variants`.
fn variant_key(id: &CandidateId) -> String {
    format!("deblob:candidate-variants:{}", id.as_str())
}

fn state_str(state: CandidateState) -> &'static str {
    match state {
        CandidateState::Provisional => "provisional",
        CandidateState::Staged => "staged",
        CandidateState::Rejected => "rejected",
    }
}

/// Reconstructs a `CandidateRecord` from the candidate hash's `record`
/// blob, overwriting its `state` field with the AUTHORITATIVE value stored
/// separately in the hash's own `state` field — mirrors
/// `registry::record_from_hash`'s treatment of `version`. `set_state` only
/// ever touches the hash's `state` field (never re-serializes `record`), so
/// this override is what makes an updated state visible to `get_candidate`.
fn candidate_from_hash(
    record_json: &str,
    state: Option<String>,
) -> Result<CandidateRecord, CoreError> {
    let mut value: serde_json::Value = serde_json::from_str(record_json)
        .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt candidate record: {e}")))?;
    if let Some(s) = state {
        value["state"] = serde_json::Value::String(s);
    }
    serde_json::from_value(value)
        .map_err(|e| CoreError::RegistryUnavailable(format!("corrupt candidate record: {e}")))
}

/// The permanent provenance stub written once per candidate, under
/// `deblob:candidate-audit:<id>`, and never given a TTL. Deliberately
/// minimal — just enough to answer "did we ever see this, when, from
/// where" long after the ephemeral candidate record has expired.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AuditStub {
    first_seen_ms: i64,
    generalized_fp: String,
    source: String,
}

/// Maps the `set_state` Lua script's `redis.error_reply` sentinels onto the
/// `CoreError` taxonomy the rest of the system expects.
fn map_set_state_error(e: redis::RedisError) -> CoreError {
    let msg = e.to_string();
    if msg.contains("NOT_FOUND") {
        CoreError::NotFound
    } else if msg.contains("TERMINAL_STATE") {
        CoreError::Conflict(format!(
            "candidate is in the terminal Rejected state and cannot transition: {msg}"
        ))
    } else {
        CoreError::RegistryUnavailable(msg)
    }
}

impl RedisEvidence {
    /// Connect and run the startup persistence gate. Although ephemeral
    /// candidate records (TTL'd) are by design, the permanent audit stub
    /// (`deblob:candidate-audit:<id>`, never-expiring) must be durable —
    /// persistence is therefore required by default and enforced via the
    /// same `CONFIG GET appendonly` gate as `RedisRegistry::connect` (spec
    /// §6). Pass `allow_volatile: true` only for test containers.
    pub async fn connect(
        url: &str,
        candidate_opts: RedisEvidenceOpts,
        persist_opts: RedisOpts,
    ) -> Result<Self, CoreError> {
        let client = Client::open(url)
            .map_err(|e| CoreError::RegistryUnavailable(format!("invalid redis url: {e}")))?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CoreError::RegistryUnavailable(format!("connect failed: {e}")))?;

        // Enforce persistence gate: permanent audit stubs must be durable.
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

        if appendonly == "no" && !persist_opts.allow_volatile {
            return Err(CoreError::RegistryUnavailable(
                "redis persistence disabled; pass allow_volatile to override".to_string(),
            ));
        }

        Ok(Self {
            conn,
            candidate_ttl_secs: candidate_opts.candidate_ttl_secs,
            set_state_script: Script::new(SET_STATE_SCRIPT),
        })
    }

    fn conn(&self) -> redis::aio::MultiplexedConnection {
        self.conn.clone()
    }
}

#[async_trait::async_trait]
impl EvidenceStore for RedisEvidence {
    async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let key = candidate_key(&rec.candidate_id);
        let audit_key = candidate_audit_key(&rec.candidate_id);

        let record_json = serde_json::to_string(&rec)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize candidate: {e}")))?;
        let state = state_str(rec.state);

        // Best-effort provenance extraction from the caller-supplied
        // profile blob. `generalized_fp` reuses the candidate's own
        // digest-derived identity (a `CandidateId` IS `"cand_" +
        // base32(sha256(fingerprint))`, per `deblob-core::id`), and
        // `source` falls back to "unknown" for a profile that doesn't
        // carry one — the profile's shape is owned by `deblob-monoid`, not
        // this crate, so this stays tolerant of either.
        let generalized_fp = rec
            .candidate_id
            .as_str()
            .strip_prefix("cand_")
            .unwrap_or_else(|| rec.candidate_id.as_str())
            .to_string();
        let source = rec
            .profile
            .get("source")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| "unknown".to_string());
        let stub_json = serde_json::to_string(&AuditStub {
            first_seen_ms: rec.first_seen_ms,
            generalized_fp,
            source,
        })
        .map_err(|e| CoreError::RegistryUnavailable(format!("serialize audit stub: {e}")))?;

        // One atomic round trip: refresh the ephemeral candidate hash + its
        // TTL, and write the permanent audit stub IFF it doesn't already
        // exist (`SET ... NX`, deliberately with no `EX`/`PX` — it must
        // never be given an expiry).
        let _: () = redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(&key)
            .arg("record")
            .arg(&record_json)
            .arg("state")
            .arg(state)
            .ignore()
            .cmd("EXPIRE")
            .arg(&key)
            .arg(self.candidate_ttl_secs)
            .ignore()
            .cmd("SET")
            .arg(&audit_key)
            .arg(&stub_json)
            .arg("NX")
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn get_candidate(&self, id: &CandidateId) -> Result<Option<CandidateRecord>, CoreError> {
        let mut conn = self.conn();
        let key = candidate_key(id);
        let (record_json, state): (Option<String>, Option<String>) = redis::cmd("HMGET")
            .arg(&key)
            .arg("record")
            .arg("state")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        match record_json {
            None => Ok(None),
            Some(json) => candidate_from_hash(&json, state).map(Some),
        }
    }

    async fn list_candidates(
        &self,
        state: CandidateState,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
        let mut conn = self.conn();
        let start_cursor = cursor.unwrap_or_else(|| "0".to_string());
        let count = limit.max(1);
        // `deblob:candidate:*` never matches `deblob:candidate-audit:*` —
        // the latter lacks the literal colon right after "candidate" that
        // the pattern requires, so this scan can never accidentally sweep
        // up (or filter against) the permanent audit stubs.
        let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
            .arg(&start_cursor)
            .arg("MATCH")
            .arg("deblob:candidate:*")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            let (record_json, rec_state): (Option<String>, Option<String>) = redis::cmd("HMGET")
                .arg(&key)
                .arg("record")
                .arg("state")
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
            if let Some(json) = record_json {
                let rec = candidate_from_hash(&json, rec_state)?;
                if rec.state == state {
                    records.push(rec);
                }
            }
        }

        let next = if next_cursor == "0" {
            None
        } else {
            Some(next_cursor)
        };
        Ok((records, next))
    }

    async fn append_evidence(
        &self,
        id: &CandidateId,
        stats: serde_json::Value,
    ) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let key = evidence_key(id);
        let payload = serde_json::to_string(&stats)
            .map_err(|e| CoreError::RegistryUnavailable(format!("serialize evidence: {e}")))?;

        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg("MAXLEN")
            .arg("~")
            .arg(EVIDENCE_STREAM_MAXLEN)
            .arg("*")
            .arg("data")
            .arg(&payload)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(())
    }

    async fn set_state(&self, id: &CandidateId, state: CandidateState) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let key = candidate_key(id);
        let new_state = state_str(state);

        let result: redis::RedisResult<i64> = self
            .set_state_script
            .key(&key)
            .arg(new_state)
            .invoke_async(&mut conn)
            .await;

        result.map(|_| ()).map_err(map_set_state_error)
    }

    async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
        let mut conn = self.conn();
        let raw: Option<String> = redis::cmd("GET")
            .arg(cluster_key(gen_fp))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        raw.map(|s| {
            CandidateId::parse(&s).map_err(|e| {
                CoreError::RegistryUnavailable(format!("corrupt cluster target: {e:?}"))
            })
        })
        .transpose()
    }

    async fn set_cluster(&self, gen_fp: &str, cand_id: &CandidateId) -> Result<(), CoreError> {
        let mut conn = self.conn();
        // Same TTL as the candidate itself: a cluster alias outliving its
        // target candidate would resolve to a dangling id.
        let _: () = redis::cmd("SET")
            .arg(cluster_key(gen_fp))
            .arg(cand_id.as_str())
            .arg("EX")
            .arg(self.candidate_ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn add_variant(
        &self,
        cand_id: &CandidateId,
        bucket_key: &str,
        fp_b32: &str,
    ) -> Result<(), CoreError> {
        let mut conn = self.conn();
        let key = variant_key(cand_id);
        let member = format!("{bucket_key}={fp_b32}");
        // One atomic round trip: SADD the member (Redis SETs are naturally
        // de-duplicated) and refresh the key's TTL to match the candidate's
        // own — same pattern as `set_cluster` above, for the same reason: a
        // variant set outliving its candidate would just be dead weight.
        let _: () = redis::pipe()
            .atomic()
            .cmd("SADD")
            .arg(&key)
            .arg(&member)
            .ignore()
            .cmd("EXPIRE")
            .arg(&key)
            .arg(self.candidate_ttl_secs)
            .ignore()
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_variants(
        &self,
        cand_id: &CandidateId,
    ) -> Result<Vec<(String, String)>, CoreError> {
        let mut conn = self.conn();
        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg(variant_key(cand_id))
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(members
            .into_iter()
            .filter_map(|m| {
                m.split_once('=')
                    .map(|(bucket, fp_b32)| (bucket.to_string(), fp_b32.to_string()))
            })
            .collect())
    }
}
