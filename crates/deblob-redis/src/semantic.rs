//! Append-only semantic-assertion revisions + mutable active pointer +
//! reverse `sem_` index (P2-D Task 5, `deblob-p2d-hermes-review.md` §4).
//!
//! Mirrors `crate::index`'s split from `crate::registry`: the base
//! `RedisRegistry` connection/health-gate machinery lives in `registry.rs`;
//! this module adds a second family of inherent methods on the same
//! `RedisRegistry` handle for the semantic-revision store, the same way
//! `index.rs` adds the structural-index methods. It is intentionally NOT a
//! new `deblob_core::ports` trait — `rebuild_index`/`verify_index` aren't
//! either, and Task 6 (the HTTP surface, out of scope here) is free to wrap
//! these in a trait later if it needs to mock them.
//!
//! Key layout:
//!   - `deblob:sem-rev:<sch_id>:<revision_id>` (HASH, immutable): one
//!     semantic-assertion revision. Never deleted or overwritten.
//!   - `deblob:sem-active:<sch_id>` (HASH, mutable): the schema's current
//!     `revision_id` + `sem_id` + a monotonically-increasing `etag`.
//!   - `deblob:sem-index:<sem_id>` (SET of `sch_id`s): the reverse index,
//!     populated ONLY for schemas that carry a real `sem_` — an
//!     un-annotated schema (which never gets a `sem_` at all, per
//!     `deblob_semantic::digest::semantic_fingerprint`'s `Option<..>`
//!     contract) never appears in any `deblob:sem-index:*` set.
//!
//! Every mutation goes through `crate::lua::SEM_APPEND_SCRIPT` — one atomic
//! transition, gated behind the same `HealthGate` `RedisRegistry::publish`
//! checks. The `deblob:schema:*` hash `crate::registry` owns is never
//! touched by anything in this module — annotating a schema's meaning
//! writes ONLY the three key families above.

use std::collections::HashMap;

use data_encoding::{HEXLOWER, HEXLOWER_PERMISSIVE};
use redis::AsyncCommands;

use deblob_core::error::CoreError;
use deblob_core::id::{RevisionId, SchemaId, SemanticId};
use deblob_core::revision::{
    AppendOutcome, Etag, ReasonCode, Revision, RevisionStatus, SemError, SignatureCandidates,
    MAX_SIGNATURE_CANDIDATES,
};
use deblob_core::semantic::SemanticMetadata;
use deblob_semantic::signature::semantic_signature;

use crate::index::delete_matching;
use crate::registry::{redis_err, RedisRegistry, AUDIT_KEY};

/// Redis pattern matching every reverse semantic-index key. Analogous to
/// `crate::index::INDEX_KEY_PATTERN`; walked by `rebuild_semantic_index`.
pub const SEM_INDEX_KEY_PATTERN: &str = "deblob:sem-index:*";

/// Redis pattern matching every bounded-neighbor-search posting key (Task
/// 10, brief §4/§5.10): `deblob:sem-sig:<feature_hex>` (SET of `sch_id`s).
/// Walked by `rebuild_semantic_index`'s postings-rebuild pass, exactly like
/// [`SEM_INDEX_KEY_PATTERN`] is for the reverse `sem_` index.
pub const SEM_SIG_KEY_PATTERN: &str = "deblob:sem-sig:*";

fn sem_rev_key(sch_id: &SchemaId, revision_id: &RevisionId) -> String {
    format!(
        "deblob:sem-rev:{}:{}",
        sch_id.as_str(),
        revision_id.as_str()
    )
}

fn sem_rev_scan_pattern(sch_id: &SchemaId) -> String {
    format!("deblob:sem-rev:{}:*", sch_id.as_str())
}

fn sem_active_key(sch_id: &SchemaId) -> String {
    format!("deblob:sem-active:{}", sch_id.as_str())
}

fn sem_index_key(sem_id: &SemanticId) -> String {
    format!("deblob:sem-index:{}", sem_id.as_str())
}

/// Task 10: the bounded-neighbor-search posting key for one already-encoded
/// (lowercase-hex) signature feature.
fn sem_sig_key(feature_hex: &str) -> String {
    format!("deblob:sem-sig:{feature_hex}")
}

fn reason_code_str(code: ReasonCode) -> &'static str {
    match code {
        ReasonCode::Correction => "correction",
        ReasonCode::OntologyUpgrade => "ontology_upgrade",
        ReasonCode::PolicyReview => "policy_review",
        ReasonCode::SourceContractChange => "source_contract_change",
        ReasonCode::OperatorOverride => "operator_override",
    }
}

fn parse_reason_code(s: &str) -> Result<ReasonCode, SemError> {
    match s {
        "correction" => Ok(ReasonCode::Correction),
        "ontology_upgrade" => Ok(ReasonCode::OntologyUpgrade),
        "policy_review" => Ok(ReasonCode::PolicyReview),
        "source_contract_change" => Ok(ReasonCode::SourceContractChange),
        "operator_override" => Ok(ReasonCode::OperatorOverride),
        other => Err(SemError::Corrupt(format!("unknown reason_code {other:?}"))),
    }
}

fn sem_redis_err(e: redis::RedisError) -> SemError {
    SemError::StoreUnavailable(e.to_string())
}

/// Maps `SEM_APPEND_SCRIPT`'s `redis.error_reply` sentinels onto the
/// `SemError` taxonomy. `expected` is the CALLER's own `expected_etag`
/// argument (already known in Rust — never parsed back out of the error
/// text); only the ACTUAL current etag needs extracting from the script's
/// `ETAG_CONFLICT:<current>` payload.
fn map_sem_script_error(e: redis::RedisError, expected: Option<Etag>) -> SemError {
    let msg = e.to_string();
    if msg.contains("MISSING_REASON") {
        return SemError::MissingReason;
    }
    if let Some(idx) = msg.find("ETAG_CONFLICT:") {
        let tail = &msg[idx + "ETAG_CONFLICT:".len()..];
        let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
        let current = digits.parse().unwrap_or(0);
        return SemError::EtagConflict {
            expected,
            current: Etag(current),
        };
    }
    SemError::StoreUnavailable(msg)
}

/// Reconstructs a full [`Revision`] from a `deblob:sem-rev:*` hash's
/// fields. Every field is validated — a hash missing a field, or carrying
/// an unparseable one, is a data-integrity problem (`SemError::Corrupt`),
/// never silently defaulted.
fn revision_from_hash(fields: &HashMap<String, String>) -> Result<Revision, SemError> {
    let get = |k: &'static str| -> Result<&String, SemError> {
        fields
            .get(k)
            .ok_or_else(|| SemError::Corrupt(format!("revision hash missing field {k:?}")))
    };

    let revision_id = RevisionId::parse(get("revision_id")?)
        .map_err(|e| SemError::Corrupt(format!("bad revision_id: {e:?}")))?;
    let sch_id = SchemaId::parse(get("sch_id")?)
        .map_err(|e| SemError::Corrupt(format!("bad sch_id: {e:?}")))?;
    let sem_id = SemanticId::parse(get("sem_id")?)
        .map_err(|e| SemError::Corrupt(format!("bad sem_id: {e:?}")))?;
    let canonical_semantic_bytes = HEXLOWER_PERMISSIVE
        .decode(get("canonical_semantic_bytes")?.as_bytes())
        .map_err(|e| SemError::Corrupt(format!("bad canonical_semantic_bytes hex: {e}")))?;
    let metadata: SemanticMetadata = serde_json::from_str(get("metadata_json")?)
        .map_err(|e| SemError::Corrupt(format!("bad metadata_json: {e}")))?;

    let previous_raw = get("previous_revision_id")?;
    let previous_revision_id = if previous_raw.is_empty() {
        None
    } else {
        Some(
            RevisionId::parse(previous_raw)
                .map_err(|e| SemError::Corrupt(format!("bad previous_revision_id: {e:?}")))?,
        )
    };

    let actor = get("actor")?.clone();
    let reason_code = parse_reason_code(get("reason_code")?)?;
    let reason = get("reason")?.clone();
    let recorded_at: i64 = get("recorded_at")?
        .parse()
        .map_err(|e| SemError::Corrupt(format!("bad recorded_at: {e}")))?;
    let effective_from: i64 = get("effective_from")?
        .parse()
        .map_err(|e| SemError::Corrupt(format!("bad effective_from: {e}")))?;
    let status = match get("status")?.as_str() {
        "active" => RevisionStatus::Active,
        other => {
            return Err(SemError::Corrupt(format!(
                "unknown revision status {other:?}"
            )))
        }
    };

    Ok(Revision {
        revision_id,
        sch_id,
        sem_id,
        metadata,
        canonical_semantic_bytes,
        previous_revision_id,
        actor,
        reason_code,
        reason,
        recorded_at,
        effective_from,
        status,
    })
}

impl RedisRegistry {
    /// Reads one immutable revision hash directly. `None` if it doesn't
    /// exist (Redis's `HGETALL` on a missing key returns an empty map,
    /// which `redis::AsyncCommands::hgetall` surfaces as an empty
    /// `HashMap`, not an error).
    async fn read_revision(
        &self,
        sch_id: &SchemaId,
        revision_id: &RevisionId,
    ) -> Result<Option<Revision>, SemError> {
        let mut conn = self.conn();
        let fields: HashMap<String, String> = conn
            .hgetall(sem_rev_key(sch_id, revision_id))
            .await
            .map_err(sem_redis_err)?;
        if fields.is_empty() {
            return Ok(None);
        }
        revision_from_hash(&fields).map(Some)
    }

    /// Appends a new semantic-assertion revision for `sch_id`, or performs
    /// an idempotent no-op if `canonical_bytes` matches the currently
    /// active revision — all inside ONE atomic Lua transition
    /// (`crate::lua::SEM_APPEND_SCRIPT`): the new immutable revision hash,
    /// the active-pointer advance, the reverse-index update (unlink old
    /// `sem_`, link new `sem_`), and one audit event, or nothing at all.
    ///
    /// `metadata`/`canonical_bytes`/`sem_id` are the ALREADY-canonicalized
    /// output of `deblob_semantic::{canonical_semantic_bytes,
    /// semantic_fingerprint}` (Task 3/4) — this module deliberately does
    /// NOT depend on `deblob-semantic` in its production dependency graph
    /// (only as a dev-dependency for realistic integration-test fixtures,
    /// mirroring how `index_it.rs` depends on `deblob-fingerprint`): Task 5
    /// is storage only, never canonicalization.
    ///
    /// `recorded_at`/`effective_from` are caller-supplied epoch-ms — never
    /// computed via `SystemTime::now()` in this function — see
    /// `Revision::recorded_at`'s docs for why semantic-revision timestamps
    /// are treated differently from `RedisRegistry::publish`'s own
    /// operational audit timestamp.
    ///
    /// `expected_etag`: `None` means "I believe this schema has never been
    /// annotated" (equivalent to expecting etag `0`); `Some(etag)` means "I
    /// believe the active pointer is currently at exactly this etag" — the
    /// compare-and-swap token guarding every REAL (non-idempotent) change.
    /// Never inspected at all when `canonical_bytes` matches the active
    /// revision exactly (idempotent replay always succeeds).
    #[allow(clippy::too_many_arguments)]
    pub async fn append_revision(
        &self,
        sch_id: &SchemaId,
        metadata: &SemanticMetadata,
        canonical_bytes: &[u8],
        sem_id: &SemanticId,
        actor: &str,
        reason_code: ReasonCode,
        reason: &str,
        recorded_at: i64,
        effective_from: i64,
        expected_etag: Option<Etag>,
    ) -> Result<AppendOutcome, SemError> {
        // Task 10 parity: `publish` freezes on a degraded persistence gate
        // before ever touching Redis; semantic-revision writes must not be
        // any less careful about durability than structural publishes are.
        if let Some(gate) = self.health_gate() {
            if !gate.is_healthy() {
                return Err(SemError::StoreUnavailable(
                    "persistence degraded".to_string(),
                ));
            }
        }

        let mut conn = self.conn();
        let new_revision_id = RevisionId::new_v7();
        let metadata_json = serde_json::to_string(metadata)
            .map_err(|e| SemError::StoreUnavailable(format!("serialize metadata: {e}")))?;
        let canonical_bytes_hex = HEXLOWER.encode(canonical_bytes);
        let expected_arg = expected_etag.map(|e| e.0.to_string()).unwrap_or_default();

        // Task 10: the new active revision's bounded-neighbor-search
        // posting keys, computed HERE in Rust from `metadata` (never
        // recomputed inside the Lua script — see `SEM_APPEND_SCRIPT`'s doc
        // comment) and threaded through as one more ARGV. On an idempotent
        // replay this is computed but never used by the script (the
        // idempotency check returns before ever reaching the postings-swap
        // code) — a harmless, cheap, pure computation either way.
        let new_feature_keys_hex = semantic_signature(metadata).feature_keys_hex();
        let new_feature_keys_json = serde_json::to_string(&new_feature_keys_hex)
            .map_err(|e| SemError::StoreUnavailable(format!("serialize feature keys: {e}")))?;

        let mut invocation = self.sem_append_script.prepare_invoke();
        invocation
            .key(sem_active_key(sch_id))
            .key(sem_rev_key(sch_id, &new_revision_id))
            .key(sem_index_key(sem_id))
            .key(AUDIT_KEY)
            .arg(sch_id.as_str())
            .arg(sem_id.as_str())
            .arg(canonical_bytes_hex.as_str())
            .arg(metadata_json.as_str())
            .arg(actor)
            .arg(reason_code_str(reason_code))
            .arg(reason)
            .arg(recorded_at)
            .arg(effective_from)
            .arg(new_revision_id.as_str())
            .arg(expected_arg.as_str())
            .arg(new_feature_keys_json.as_str());

        let result: redis::RedisResult<(String, String, String, String)> =
            invocation.invoke_async(&mut conn).await;

        let (revision_id_str, _sem_id_str, etag_str, outcome) =
            result.map_err(|e| map_sem_script_error(e, expected_etag))?;

        let revision_id = RevisionId::parse(&revision_id_str)
            .map_err(|e| SemError::Corrupt(format!("script returned bad revision_id: {e:?}")))?;
        // The script's 3rd reply element is the AUTHORITATIVE current etag
        // — computed and returned inside the same atomic transition that
        // decided `outcome`, for BOTH branches (`already_active`'s reply
        // carries the pre-existing pointer's etag; `appended`'s reply
        // carries the just-advanced one). Threading it through here is what
        // lets `api::semantic::put_semantic` build its `ETag` header
        // straight from this call's result, with no extra Redis round trip
        // that could race a concurrent writer.
        let etag: u64 = etag_str.parse().map_err(|e| {
            SemError::Corrupt(format!("script returned bad etag {etag_str:?}: {e}"))
        })?;
        let revision = self
            .read_revision(sch_id, &revision_id)
            .await?
            .ok_or_else(|| {
                SemError::Corrupt(
                    "revision vanished immediately after the atomic write that created it"
                        .to_string(),
                )
            })?;

        Ok(match outcome.as_str() {
            "already_active" => AppendOutcome::AlreadyActive {
                revision,
                etag: Etag(etag),
            },
            _ => AppendOutcome::Appended {
                revision,
                etag: Etag(etag),
            },
        })
    }

    /// The schema's current FULL active [`Revision`] (including its
    /// `revision_id`) plus its current [`Etag`], or `None` if it has never
    /// been annotated (no `deblob:sem-active:<sch_id>` key at all — a real
    /// absence, never a sentinel value; migration case: a schema published
    /// before this feature existed reads back exactly the same way). Task
    /// 10 needs the `revision_id` (for the neighbors API's
    /// `semantic_revision_id` field) that [`Self::active_semantic`] used to
    /// discard — this is the single read both now share.
    pub async fn active_revision(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(Revision, Etag)>, SemError> {
        let mut conn = self.conn();
        let fields: HashMap<String, String> = conn
            .hgetall(sem_active_key(sch_id))
            .await
            .map_err(sem_redis_err)?;
        if fields.is_empty() {
            return Ok(None);
        }

        let revision_id_str = fields
            .get("revision_id")
            .ok_or_else(|| SemError::Corrupt("active pointer missing revision_id".to_string()))?;
        let revision_id = RevisionId::parse(revision_id_str)
            .map_err(|e| SemError::Corrupt(format!("bad revision_id: {e:?}")))?;
        let etag: u64 = fields
            .get("etag")
            .ok_or_else(|| SemError::Corrupt("active pointer missing etag".to_string()))?
            .parse()
            .map_err(|e| SemError::Corrupt(format!("bad etag: {e}")))?;

        let revision = self
            .read_revision(sch_id, &revision_id)
            .await?
            .ok_or_else(|| {
                SemError::Corrupt(format!(
                    "active pointer references missing revision {revision_id:?}"
                ))
            })?;

        Ok(Some((revision, Etag(etag))))
    }

    /// The schema's current semantic assertion, or `None` if it has never
    /// been annotated. A thin projection of [`Self::active_revision`] —
    /// kept as its own method (rather than inlining `.map(...)` at every
    /// call site) since it's the pre-Task-10 public contract every existing
    /// caller (`api::semantic::get_semantic`/`put_semantic`) already
    /// depends on.
    pub async fn active_semantic(
        &self,
        sch_id: &SchemaId,
    ) -> Result<Option<(SemanticMetadata, SemanticId, Etag)>, SemError> {
        Ok(self
            .active_revision(sch_id)
            .await?
            .map(|(revision, etag)| (revision.metadata, revision.sem_id, etag)))
    }

    /// The schema's full revision history, oldest first. `RevisionId` is a
    /// UUIDv7 (`deblob_core::id::RevisionId::new_v7`), so sorting by its
    /// string form sorts chronologically without needing a separate
    /// ordered Redis structure — see that type's docs.
    pub async fn revisions(&self, sch_id: &SchemaId) -> Result<Vec<Revision>, SemError> {
        let mut conn = self.conn();
        let pattern = sem_rev_scan_pattern(sch_id);
        let mut cursor = "0".to_string();
        let mut out = Vec::new();

        loop {
            let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(sem_redis_err)?;

            for key in &keys {
                let fields: HashMap<String, String> =
                    conn.hgetall(key).await.map_err(sem_redis_err)?;
                if fields.is_empty() {
                    continue;
                }
                out.push(revision_from_hash(&fields)?);
            }

            if next_cursor == "0" {
                break;
            }
            cursor = next_cursor;
        }

        out.sort_by(|a, b| a.revision_id.as_str().cmp(b.revision_id.as_str()));
        Ok(out)
    }

    /// Every schema currently carrying `sem_id` as its ACTIVE semantic
    /// assertion (the reverse index, spec §5's same-`sem_`-different-`sch_`
    /// diagnostic signal — the diagnostic logic itself is a later task,
    /// this is just the lookup it will need).
    pub async fn schemas_by_semantic(
        &self,
        sem_id: &SemanticId,
    ) -> Result<Vec<SchemaId>, SemError> {
        let mut conn = self.conn();
        let members: Vec<String> = conn
            .smembers(sem_index_key(sem_id))
            .await
            .map_err(sem_redis_err)?;
        members
            .into_iter()
            .map(|s| {
                SchemaId::parse(&s).map_err(|e| {
                    SemError::Corrupt(format!("bad schema id in reverse index: {e:?}"))
                })
            })
            .collect()
    }

    /// Rebuilds BOTH `deblob:sem-index:*` (the reverse `sem_` index) AND
    /// `deblob:sem-sig:*` (Task 10's bounded neighbor-search postings) from
    /// scratch, purely from the authoritative `deblob:sem-active:*`
    /// pointers (revisions are the deeper source of truth, but the ACTIVE
    /// pointer is what both indexes track — a superseded revision's `sem_`
    /// or signature must NOT appear in either). Mirrors
    /// `crate::index::RedisRegistry::rebuild_index`'s drop-then-rebuild
    /// strategy exactly, including reusing its `delete_matching` helper.
    /// Always safe to run online.
    ///
    /// Rebuild ≡ incremental (spec §5.12): the postings this reconstructs
    /// use the EXACT SAME `deblob_semantic::signature::semantic_signature`
    /// → `feature_keys_hex()` pipeline `append_revision` already used to
    /// compute what it wrote incrementally — recomputed here from each
    /// active revision's own stored `metadata_json` (not re-derived from
    /// `canonical_semantic_bytes`, which is a one-way hash preimage). Two
    /// deterministic computations over the SAME stored metadata always
    /// agree, so a full rebuild and the incremental writes that got there
    /// produce byte-identical `deblob:sem-sig:*` sets and
    /// `feature_keys_json` fields — see `deblob-redis/tests/semantic_it.rs`
    /// for the pinning test.
    ///
    /// Returns the number of annotated schemas reindexed.
    pub async fn rebuild_semantic_index(&self) -> Result<u64, CoreError> {
        let mut conn = self.conn();

        delete_matching(conn.clone(), SEM_INDEX_KEY_PATTERN).await?;
        delete_matching(conn.clone(), SEM_SIG_KEY_PATTERN).await?;
        // Task 10 IDF (jr-deblob-similarity-idf-221040): the active-annotated
        // population set is rebuilt from the same authoritative `sem-active:*`
        // pointers as the postings, so `N = SCARD` stays consistent with the
        // `deblob:sem-sig:*` df values after any rebuild.
        let _: () = conn
            .del("deblob:sem-active-schemas")
            .await
            .map_err(redis_err)?;

        let mut count: u64 = 0;
        let mut cursor = "0".to_string();
        loop {
            let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("deblob:sem-active:*")
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            for key in &keys {
                let sch_id_str = key.strip_prefix("deblob:sem-active:").unwrap_or(key);
                let (sem_id, revision_id): (Option<String>, Option<String>) = redis::cmd("HMGET")
                    .arg(key)
                    .arg("sem_id")
                    .arg("revision_id")
                    .query_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
                let (Some(sem_id), Some(revision_id)) = (sem_id, revision_id) else {
                    // Defensive: a pointer hash missing `sem_id`/
                    // `revision_id` can't be reindexed. Skip rather than
                    // fail the whole rebuild — matches `rebuild_index`'s
                    // posture for schemas published before the `bucket`
                    // field existed.
                    continue;
                };
                let schema_id = SchemaId::parse(sch_id_str).map_err(|e| {
                    CoreError::RegistryUnavailable(format!("corrupt sem-active key {key}: {e:?}"))
                })?;
                let sem_id_parsed = SemanticId::parse(&sem_id).map_err(|e| {
                    CoreError::RegistryUnavailable(format!("corrupt sem_id in {key}: {e:?}"))
                })?;
                let _: () = conn
                    .sadd(sem_index_key(&sem_id_parsed), schema_id.as_str())
                    .await
                    .map_err(redis_err)?;
                // IDF population set (jr-deblob-similarity-idf-221040).
                let _: () = conn
                    .sadd("deblob:sem-active-schemas", schema_id.as_str())
                    .await
                    .map_err(redis_err)?;

                // Task 10 postings: re-derive the signature from the active
                // revision's own stored `metadata_json` — the same
                // authoritative source `append_revision` reads/writes.
                let rev_key = format!("deblob:sem-rev:{sch_id_str}:{revision_id}");
                let metadata_json: Option<String> = conn
                    .hget(&rev_key, "metadata_json")
                    .await
                    .map_err(redis_err)?;
                if let Some(metadata_json) = metadata_json {
                    let metadata: SemanticMetadata =
                        serde_json::from_str(&metadata_json).map_err(|e| {
                            CoreError::RegistryUnavailable(format!(
                                "corrupt metadata_json in {rev_key}: {e}"
                            ))
                        })?;
                    let feature_keys_hex = semantic_signature(&metadata).feature_keys_hex();
                    for hex in &feature_keys_hex {
                        let _: () = conn
                            .sadd(sem_sig_key(hex), schema_id.as_str())
                            .await
                            .map_err(redis_err)?;
                    }
                    let feature_keys_json = serde_json::to_string(&feature_keys_hex)
                        .unwrap_or_else(|_| "[]".to_string());
                    let _: () = conn
                        .hset(key, "feature_keys_json", feature_keys_json)
                        .await
                        .map_err(redis_err)?;
                }

                count += 1;
            }

            if next_cursor == "0" {
                break;
            }
            cursor = next_cursor;
        }

        Ok(count)
    }

    /// Task 10: the bounded feature-postings union for `feature_keys_hex`
    /// (spec §4) — `SUNION` across every `deblob:sem-sig:<hex>` set. Never
    /// truncates: if the union's size exceeds
    /// [`deblob_core::revision::MAX_SIGNATURE_CANDIDATES`], the FULL
    /// (over-bound) member count is what triggers `TooBroad`, never a
    /// partial read masquerading as complete. May include the query
    /// schema's own id (its own postings ARE part of the union over its own
    /// feature keys) — callers exclude it themselves, same posture as
    /// `schemas_by_semantic`'s reverse-index reads.
    pub async fn signature_candidates(
        &self,
        feature_keys_hex: &[String],
    ) -> Result<SignatureCandidates, SemError> {
        if feature_keys_hex.is_empty() {
            return Ok(SignatureCandidates::Bounded(Vec::new()));
        }
        let mut conn = self.conn();
        let keys: Vec<String> = feature_keys_hex.iter().map(|h| sem_sig_key(h)).collect();
        let members: Vec<String> = conn.sunion(&keys).await.map_err(sem_redis_err)?;
        if members.len() > MAX_SIGNATURE_CANDIDATES {
            return Ok(SignatureCandidates::TooBroad);
        }
        let ids = members
            .into_iter()
            .map(|s| {
                SchemaId::parse(&s).map_err(|e| {
                    SemError::Corrupt(format!("bad schema id in postings index: {e:?}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SignatureCandidates::Bounded(ids))
    }

    /// Task 10 IDF snapshot (`jr-deblob-similarity-idf-221040`): the
    /// active-annotated population `N` plus the document frequency `df` of every
    /// `feature_keys_hex` posting, read atomically via
    /// [`crate::lua::SEM_IDF_STATS_SCRIPT`] so the neighbor handler never scores
    /// over a torn view of a concurrent index transition. The returned
    /// `Vec<u64>` is aligned to `feature_keys_hex` order; a posting key that
    /// does not exist (never posted) `SCARD`s to `0`. An empty `feature_keys_hex`
    /// still returns `N` (with an empty `df` vec) — the caller may want the
    /// population for the response envelope even for a featureless query.
    pub async fn idf_stats(
        &self,
        feature_keys_hex: &[String],
    ) -> Result<(u64, Vec<u64>), SemError> {
        let mut conn = self.conn();
        let mut invocation = self.sem_idf_stats_script.prepare_invoke();
        for hex in feature_keys_hex {
            invocation.arg(hex.as_str());
        }
        let reply: Vec<u64> = invocation
            .invoke_async(&mut conn)
            .await
            .map_err(sem_redis_err)?;
        let mut iter = reply.into_iter();
        let n = iter.next().unwrap_or(0);
        let dfs: Vec<u64> = iter.collect();
        Ok((n, dfs))
    }
}
