//! The atomic publication script. Spec §6: "Publication is one atomic Lua
//! transition: schema record + family version + index entries + alias +
//! audit event — or nothing."
//!
//! KEYS:
//!   1. schema key            deblob:schema:<sch_id>  (HASH)
//!   2. family key            deblob:family:<fam_id>
//!   3. alias key             deblob:alias:<cand_id>
//!   4. bucket key            deblob:index:<fieldband>:<depth>:<reqhash8>
//!   5. audit stream key      deblob:audit:log
//!   6. published-marker key  deblob:published:<sch_id>
//!   7. schema-index key      deblob:schemas  (SET, fix1) — the maintained
//!      listing index `GET /api/v1/schemas` pages over. Every publish call
//!      (fresh or idempotent republish) `SADD`s the schema id here, so
//!      `RedisRegistry::list_schemas` can `SSCAN` this one small SET
//!      (O(schemas)) instead of `SCAN`ning the entire `deblob:*` keyspace
//!      (O(keyspace)) looking for sparse `deblob:schema:*` keys — the bug
//!      that produced empty pages with a non-zero cursor even when schemas
//!      existed, because a keyspace `SCAN COUNT` batch could easily contain
//!      zero schema keys among the thousands of candidate/evidence/index/
//!      semantic keys sharing the same `deblob:` prefix space.
//!   - variant bucket keys (KEYS[8..7+N], Task 14 fix): one structural-index
//!     SET key per observed CONCRETE shape recorded against the promoted
//!     candidate (`EvidenceStore::get_variants`), possibly a DIFFERENT
//!     bucket than KEYS[4]: an observed variant with more/fewer top-level
//!     fields than the candidate's generalized profile can band into a
//!     different `ShapeSummary` bucket. `N` is derived from `#KEYS - 7`, so
//!     `N == 0` (no extra KEYS beyond the fixed seven) is a valid, no-op
//!     call — a candidate promoted with no recorded variants must not fail.
//!
//! `KEYS[4]` (Task 8) is the real per-bucket structural-index SET this
//! schema belongs to, computed by the caller from its `ShapeSummary`. It is
//! also persisted onto the schema hash's `bucket` field below, so
//! `rebuild_index` can reconstruct the index without re-deriving a
//! `ShapeSummary` from `canonical`.
//!
//! ARGV:
//!   1. schema_json    full serialized `SchemaRecord` JSON, stored verbatim
//!      under the schema hash's `record` field (its `version` is the
//!      caller's best guess and is NOT authoritative — see below)
//!   2. canonical      the record's canonical shape JSON
//!   3. canonicalizer  the record's canonicalizer tag (e.g. "deblob-canon-v1")
//!   4. family_id      recorded for parity / future audit use
//!   5. schema_id      the terminal schema id the alias resolves to
//!   6. bucket_member  bucket-set member to add: "<fp_b32>=<sch_id>"
//!   7. actor
//!   8. reason
//!   9. now_ms
//!   10. variants_json (Task 14 fix) JSON array of `"<bucket>=<fp_b32>"`
//!       strings — one per KEYS[7..], in the SAME order — persisted onto
//!       the schema hash's `variants` field so `rebuild_index` can restore
//!       every variant's bucket membership from the authoritative schema
//!       record alone (spec §6), without depending on the (ephemeral,
//!       TTL'd) `EvidenceStore` candidate-variant set still existing.
//!       `"[]"` when there are no variants.
//!   - variant_member strings (ARGV[11..10+N], Task 14 fix): one per
//!     KEYS[7..], in the SAME order: `"<fp_b32>=<sch_id>"` (same shape as
//!     ARGV[6], just for a concrete observation's own digest instead of
//!     the schema's).
//!
//! Semantics (all decided BEFORE any write, so a rejected call leaves no
//! partial state):
//!   - Immutability guards CANONICAL IDENTITY ONLY — the `canonical` shape
//!     json plus the `canonicalizer` tag — never the full record. Since
//!     `sch_id = base32(sha256("deblob-canon-v1\0" || canonical))`, two
//!     publishes of the SAME schema legitimately carry DIFFERENT
//!     `provenance` (fresh timestamps/offsets on a retry) or a stale/guessed
//!     `version`; neither may trigger IMMUTABILITY. Only a genuinely
//!     different `canonical` (or `canonicalizer`) under the same `sch_id`
//!     is fatal.
//!   - Alias write-once: if the alias key already points at a different
//!     schema id than ARGV[5], fail with ALIAS_CONFLICT.
//!   - Idempotent republish: if the stored identity agrees (or the key
//!     doesn't exist yet) and the alias agrees (or doesn't exist yet), the
//!     call proceeds. Family-version allocation is guarded by the
//!     published-marker key so a retry of the SAME publish never
//!     double-increments the family counter — it returns the version
//!     recorded on the first successful publish.
//!   - Family version allocation (first-time publications only) is done via
//!     HINCRBY on the family hash, which is atomic on the Redis server, so
//!     concurrent first-time publishes of distinct schemas to the same
//!     family always get distinct, consecutive versions. This
//!     HINCRBY-allocated version is the SOLE authority for
//!     `SchemaRecord.version` — the caller-supplied `record.version` is
//!     never trusted for storage; it is only ever used to seed the stored
//!     `record` blob, and `get_schema` overwrites that field with the
//!     authoritative version on every read. The script returns the
//!     authoritative version as its result so `Registry::publish` can
//!     report it back to the caller.
pub const PUBLISH_SCRIPT: &str = r#"
local schema_key = KEYS[1]
local family_key = KEYS[2]
local alias_key = KEYS[3]
local index_key = KEYS[4]
local audit_key = KEYS[5]
local published_key = KEYS[6]
local schema_index_key = KEYS[7]

local schema_json = ARGV[1]
local canonical = ARGV[2]
local canonicalizer = ARGV[3]
local family_id = ARGV[4]
local schema_id = ARGV[5]
local bucket_member = ARGV[6]
local actor = ARGV[7]
local reason = ARGV[8]
local now_ms = ARGV[9]
local variants_json = ARGV[10]

-- Immutability compares CANONICAL IDENTITY ONLY. Differing provenance or
-- version must never raise IMMUTABILITY.
local existing_canonical = redis.call('HGET', schema_key, 'canonical')
if existing_canonical then
  local existing_canonicalizer = redis.call('HGET', schema_key, 'canonicalizer')
  if existing_canonical ~= canonical or existing_canonicalizer ~= canonicalizer then
    return redis.error_reply('IMMUTABILITY')
  end
end

local existing_alias = redis.call('GET', alias_key)
if existing_alias and existing_alias ~= schema_id then
  return redis.error_reply('ALIAS_CONFLICT')
end

-- The family key is the sole authority for the version: fresh publish ->
-- HINCRBY-allocated version; idempotent republish (published marker
-- present) -> the SAME previously-allocated version.
local published = redis.call('GET', published_key)
local version
if published then
  version = tonumber(published)
else
  version = redis.call('HINCRBY', family_key, 'next_version', 1)
  redis.call('HSET', family_key, 'v:' .. version, schema_id)
  redis.call('HSET', family_key, 'family_id', family_id)
  redis.call('SET', published_key, tostring(version))
end

if not existing_canonical then
  redis.call('HSET', schema_key,
    'record', schema_json,
    'canonical', canonical,
    'canonicalizer', canonicalizer,
    'family', family_id,
    'version', tostring(version),
    'bucket', index_key)
end
if not existing_alias then
  redis.call('SET', alias_key, schema_id)
end

redis.call('SADD', index_key, bucket_member)

-- fix1: maintain the schemas-listing index unconditionally (fresh publish
-- AND idempotent republish alike — SADD is a no-op on a member that's
-- already present), atomically alongside every other write this script
-- makes, so `list_schemas` never has to derive it separately and can never
-- observe a schema record without a corresponding listing-index entry.
redis.call('SADD', schema_index_key, schema_id)

-- Task 14 fix: index every observed CONCRETE shape recorded against this
-- candidate, into ITS OWN bucket (KEYS[8..], one per variant, possibly
-- distinct from index_key), so a hot-path lookup of any previously
-- observed shape resolves to schema_id — not just the schema's own
-- generalized digest. Unconditional (not gated behind `not
-- existing_canonical`/`not existing_alias`) and idempotent (SADD), so a
-- republish that now knows about MORE variants than the original publish
-- still gets them indexed. The `variants` field is likewise always
-- refreshed to the fullest set of variants any publish call has supplied,
-- so `rebuild_index` can restore all of them later purely from this hash.
redis.call('HSET', schema_key, 'variants', variants_json)
local variant_count = #KEYS - 7
for i = 1, variant_count do
  local variant_bucket_key = KEYS[7 + i]
  local variant_member = ARGV[10 + i]
  redis.call('SADD', variant_bucket_key, variant_member)
end

redis.call('XADD', audit_key, '*', 'actor', actor, 'reason', reason, 'schema', schema_id, 'ts', now_ms)

return version
"#;

/// Guards `EvidenceStore::set_state` against leaving Redis's `Rejected`
/// terminal state, atomically, in a single round trip — no client-side
/// `WATCH`/`MULTI` dance (fragile to reason about over a shared
/// multiplexed connection where unrelated commands from other callers may
/// interleave between `WATCH` and `EXEC`).
///
/// KEYS:
///   1. candidate key   deblob:candidate:<cand_id>  (HASH)
///
/// ARGV:
///   1. new_state target state, e.g. "staged" (snake_case, matching
///      `CandidateState`'s serde representation)
///
/// Semantics:
///   - Missing candidate -> `NOT_FOUND`.
///   - Candidate whose current `state` field is `"rejected"` -> refuses
///     ANY transition out of it (Rejected is terminal, spec §6) with
///     `TERMINAL_STATE`.
///   - Otherwise `HSET`s just the `state` field to the new value. Only
///     that one field is touched — the candidate hash's `record` blob and
///     its `EXPIRE`-set TTL are left completely alone, since `HSET` never
///     resets a key's expiry.
pub const SET_STATE_SCRIPT: &str = r#"
local candidate_key = KEYS[1]
local new_state = ARGV[1]

local exists = redis.call('EXISTS', candidate_key)
if exists == 0 then
  return redis.error_reply('NOT_FOUND')
end

local current_state = redis.call('HGET', candidate_key, 'state')
if current_state == 'rejected' then
  return redis.error_reply('TERMINAL_STATE')
end

redis.call('HSET', candidate_key, 'state', new_state)
return 1
"#;

/// Atomic, governed display-NAME write (`jr-schema-naming-211140`).
///
/// The SLM-proposed / human-edited schema name lives in SEPARATE small hash
/// fields on the schema key (`name_label`/`name_source`/`name_meta`/
/// `name_updated_ms`) — deliberately NOT re-serialized into the big `record`
/// JSON, so the schema's shape/identity is never round-tripped (and never at
/// risk of an empty-array to `{}` cjson corruption). The read path overlays
/// these fields onto `provenance.label` so the console renders the name.
///
/// GOVERNANCE — a human name always wins: when the incoming `source` is not
/// `human` and the record already carries `name_source == 'human'`, the write
/// is refused (`skipped_human`) inside this single atomic transition, so a
/// human edit landing between an automatic namer's read and write can never be
/// clobbered.
///
/// KEYS:
///   1. schema key   deblob:schema:<sch_id>   (HASH)
///
/// ARGV:
///   1. label   2. source   3. meta_json (`''` = none)   4. now_ms
///
/// Returns a status string: `applied`, `skipped_human`, or `not_found`.
pub const SET_NAME_SCRIPT: &str = r#"
local key = KEYS[1]
if redis.call('HEXISTS', key, 'record') == 0 then
  return 'not_found'
end
local cur = redis.call('HGET', key, 'name_source')
if ARGV[2] ~= 'human' and cur == 'human' then
  return 'skipped_human'
end
redis.call('HSET', key, 'name_label', ARGV[1], 'name_source', ARGV[2], 'name_updated_ms', ARGV[4])
if ARGV[3] == '' then
  redis.call('HDEL', key, 'name_meta')
else
  redis.call('HSET', key, 'name_meta', ARGV[3])
end
return 'applied'
"#;

/// The atomic semantic-assertion append transition (P2-D Task 5, Hermes
/// review §4): "append + pointer-advance + reverse-index update + audit is
/// ONE Lua transition (crash → all-or-nothing)." See `crate::semantic` for
/// the Rust-side `append_revision` that invokes this and reconstructs a
/// full `Revision` from its result.
///
/// KEYS:
///   1. active key       deblob:sem-active:<sch_id>       (HASH, mutable)
///   2. revision key     deblob:sem-rev:<sch_id>:<new_revision_id>
///      (HASH, immutable — freshly minted per call, so this key never
///      already exists; computed client-side, same as `PUBLISH_SCRIPT`'s
///      keys, so the Rust layer stays the single source of truth for key
///      naming)
///   3. new index key    deblob:sem-index:<new_sem_id>     (SET)
///   4. audit stream key deblob:audit:log (`crate::registry::AUDIT_KEY` —
///      the SAME stream `PUBLISH_SCRIPT` writes to)
///
/// The OLD `deblob:sem-index:<old_sem_id>` key is deliberately NOT passed
/// via `KEYS`: which `sem_` was active is only known once this script reads
/// `KEYS[1]`'s `sem_id` field, atomically, inside this same transition.
/// Computing that key client-side ahead of the call (the way every other
/// key in this crate's scripts is computed) would race a concurrent
/// transition the etag check below is specifically there to prevent: if
/// another writer moved the pointer between a client-side pre-read and this
/// call, the client's precomputed "old" key would already be wrong, and the
/// reverse index would silently corrupt (unlinking the WRONG `sem_`'s set,
/// or failing to unlink the right one). Constructing it here — after the
/// read that the CAS check below has already validated — is a correctness
/// requirement, not a style choice; this crate targets a single Redis
/// instance (not Cluster), so a script referencing a key outside `KEYS` is
/// safe (no slot-hashing to satisfy).
///
/// ARGV:
///   1. sch_id
///   2. new_sem_id            the `sem_` this call is asserting
///   3. canonical_bytes_hex   hex(`deblob_semantic::canonical_semantic_bytes`)
///   4. metadata_json         `serde_json`-serialized `SemanticMetadata`,
///      stored so `active_semantic`/`revisions` can reconstruct the typed
///      value directly — `canonical_bytes_hex` is a one-way HASH PREIMAGE
///      (Task 3/4's byte protocol), not designed to be decoded back
///   5. actor
///   6. reason_code           controlled enum, snake_case (may be `''` on
///      an idempotent replay attempt — never inspected in that branch)
///   7. reason                free-form prose (`''` means "no reason
///      supplied"; required for any REAL change, never for an idempotent
///      replay)
///   8. recorded_at           caller-supplied epoch-ms (never computed
///      here — see `Revision::recorded_at`'s docs for why)
///   9. effective_from        caller-supplied epoch-ms
///   10. new_revision_id      freshly minted client-side (`RevisionId::new_v7`)
///   11. expected_etag        decimal string, or `''` for "expect no active
///       revision yet" (equivalent to expecting etag `0`)
///   12. new_feature_keys_json (Task 10) JSON array of lowercase-hex
///       [`deblob_semantic::signature::SemanticSignature::feature_keys_hex`]
///       strings for `metadata_json`'s signature — computed in Rust
///       (`crate::semantic::append_revision`), NEVER recomputed here: this
///       script only ever does `SADD`/`SREM` against already-encoded
///       feature keys, exactly like `PUBLISH_SCRIPT`'s `variants_json`
///       never re-derives a `ShapeSummary` server-side. `"[]"` for a
///       signature with zero features (never expected in practice, since
///       `append_revision` is only ever called with a metadata that
///       produced a real `sem_`, but always a syntactically valid JSON
///       array either way).
///
/// Postings swap (Task 10, spec §5.10/§5.12 — "on re-annotation, atomically
/// remove the old active revision's postings, add the new revision's
/// postings, move the active pointer"): the OLD feature-key list is never
/// passed in from the client (that would race a concurrent writer the exact
/// same way a client-side-computed old `sem-index` key would — see this
/// const's doc comment above on `cur_sem_id`). Instead it is round-tripped
/// through a `feature_keys_json` field stored on `KEYS[1]` (the active
/// pointer hash) by the PREVIOUS call to this script, and read back here —
/// atomically, inside this same transition, before it is overwritten with
/// the new list. A schema annotated before Task 10 existed simply has no
/// `feature_keys_json` field yet: `old_features` is then treated as empty
/// (nothing to `SREM`), and this call's `SADD`s plus the freshly-written
/// `feature_keys_json` field self-heal it going forward — the same
/// "defensive, skip rather than fail" posture `rebuild_index`/
/// `rebuild_semantic_index` already use for schemas published before a
/// field they now depend on existed. `deblob:sem-sig:<hex>` keys are
/// computed inline (string-concatenated), the same way `old_index_key`
/// below already is, for the identical reason: this crate targets a single
/// Redis instance, never Cluster, so keys outside `KEYS` are safe here.
///
/// Semantics (spec order: idempotency check first, then the guarded write):
///   - If an active revision already exists AND its OWN stored
///     `canonical_semantic_bytes` (read back from ITS immutable hash, never
///     recomputed) equals ARGV[3] byte-for-byes: idempotent no-op. Nothing
///     is written; the reply's 4th element is `'already_active'` and the
///     first three elements describe the EXISTING active revision/pointer,
///     unchanged. `reason`/`expected_etag` are not even inspected on this
///     path — a byte-identical replay is always fine, no matter how it's
///     annotated.
///   - Otherwise the write is REAL: `reason == ''` -> `MISSING_REASON`.
///   - Then the CAS check: let `current_etag` be `KEYS[1]`'s `etag` field,
///     or `0` if `KEYS[1]` doesn't exist yet (never annotated). Let
///     `expected` be `0` if ARGV[11] is `''`, else `tonumber(ARGV[11])`. If
///     `expected ~= current_etag` -> `ETAG_CONFLICT:<current_etag>` (the
///     actual current value is embedded so the Rust layer can report it
///     without a second round trip).
///   - Otherwise: write the new immutable revision hash (KEYS[2]) with
///     `previous_revision_id` set to the prior active revision's id (or
///     `''` if this is the schema's first ever revision) and
///     `status = 'active'` (see `RevisionStatus`'s docs for why this is a
///     creation-time-only marker, never later mutated); advance the active
///     pointer (KEYS[1]) to the new revision/sem_/etag (`current_etag + 1`);
///     unlink `sch_id` from the OLD sem_'s reverse-index set if one existed
///     and differs from the new one; link `sch_id` into KEYS[3] (idempotent
///     `SADD` — safe even if the new `sem_` happens to equal the old one);
///     append one audit event to KEYS[4]. Reply's 4th element is
///     `'appended'`.
pub const SEM_APPEND_SCRIPT: &str = r#"
local active_key = KEYS[1]
local revision_key = KEYS[2]
local new_index_key = KEYS[3]
local audit_key = KEYS[4]

local sch_id = ARGV[1]
local new_sem_id = ARGV[2]
local canonical_bytes_hex = ARGV[3]
local metadata_json = ARGV[4]
local actor = ARGV[5]
local reason_code = ARGV[6]
local reason = ARGV[7]
local recorded_at = ARGV[8]
local effective_from = ARGV[9]
local new_revision_id = ARGV[10]
local expected_etag_arg = ARGV[11]
local new_feature_keys_json = ARGV[12]

local cur_revision_id = redis.call('HGET', active_key, 'revision_id')
local cur_sem_id = redis.call('HGET', active_key, 'sem_id')
local cur_etag_str = redis.call('HGET', active_key, 'etag')
local cur_etag = tonumber(cur_etag_str) or 0
local old_feature_keys_json = redis.call('HGET', active_key, 'feature_keys_json')

-- Idempotent replay: compare against the ACTIVE revision's own stored
-- bytes (never recomputed), and bypass reason/etag checks entirely.
if cur_revision_id then
  local cur_revision_key = 'deblob:sem-rev:' .. sch_id .. ':' .. cur_revision_id
  local cur_bytes_hex = redis.call('HGET', cur_revision_key, 'canonical_semantic_bytes')
  if cur_bytes_hex == canonical_bytes_hex then
    return {cur_revision_id, cur_sem_id, tostring(cur_etag), 'already_active'}
  end
end

-- Real change from here on: a reason is mandatory.
if reason == '' then
  return redis.error_reply('MISSING_REASON')
end

-- Optimistic-concurrency CAS: '' means "expect no active revision" (etag 0).
local expected_etag
if expected_etag_arg == '' then
  expected_etag = 0
else
  expected_etag = tonumber(expected_etag_arg)
end
if expected_etag ~= cur_etag then
  return redis.error_reply('ETAG_CONFLICT:' .. tostring(cur_etag))
end

local new_etag = cur_etag + 1
local previous_revision_id = cur_revision_id or ''

-- Task 10 fix (atomicity hardening): decode BOTH feature-key lists here,
-- before the first write below. Redis Lua has no rollback on a runtime
-- error, so a malformed JSON caught only after HSETs/SADDs had already run
-- would leave an "advance without swap" partial state (pointer/revision
-- moved, postings not swapped). `new_feature_keys_json` is always
-- `serde_json`-serialized in Rust immediately before this call, and
-- `old_feature_keys_json` is only ever written by this same script, so a
-- decode failure here is not reachable in normal operation -- but parsing
-- everything that can fail up front, before any write, is what makes the
-- all-or-nothing guarantee actually hold rather than merely appear to.
local old_features = {}
if old_feature_keys_json then
  old_features = cjson.decode(old_feature_keys_json)
end
local new_features = cjson.decode(new_feature_keys_json)

redis.call('HSET', revision_key,
  'revision_id', new_revision_id,
  'sch_id', sch_id,
  'sem_id', new_sem_id,
  'canonical_semantic_bytes', canonical_bytes_hex,
  'metadata_json', metadata_json,
  'previous_revision_id', previous_revision_id,
  'actor', actor,
  'reason_code', reason_code,
  'reason', reason,
  'recorded_at', recorded_at,
  'effective_from', effective_from,
  'status', 'active')

redis.call('HSET', active_key,
  'revision_id', new_revision_id,
  'sem_id', new_sem_id,
  'etag', tostring(new_etag))

-- Reverse index: unlink from the OLD sem_'s set (only if one existed and
-- differs from the new one -- see this const's doc comment for why the key
-- is computed here rather than passed via KEYS), then link into the new
-- one.
if cur_sem_id and cur_sem_id ~= new_sem_id then
  local old_index_key = 'deblob:sem-index:' .. cur_sem_id
  redis.call('SREM', old_index_key, sch_id)
end
redis.call('SADD', new_index_key, sch_id)

-- Task 10 (IDF, jr-deblob-similarity-idf-221040): the active-annotated-schema
-- population set. Maintained atomically here so `N = SCARD` is O(1) and always
-- matches the `deblob:sem-sig:*` posting semantics (both track ACTIVE
-- signatures). A schema's FIRST annotation always reaches this append path (an
-- idempotent replay returns far above, before any write), and SADD of an
-- existing member is a harmless no-op on every later real change — so the set
-- is exactly {schemas with a current active semantic revision}. Rebuilt from
-- `deblob:sem-active:*` by `rebuild_semantic_index`, same as the postings.
redis.call('SADD', 'deblob:sem-active-schemas', sch_id)

-- Task 10: bounded inverted-index postings swap, atomically alongside the
-- pointer move above. `old_features` comes from what THIS SAME active hash
-- said its own feature list was, immediately before this call overwrote its
-- revision_id/sem_id/etag fields (read at the very top of the script, well
-- before any write) -- never recomputed, never client-supplied. Both lists
-- were already decoded above, before the first write.
for _, hex in ipairs(old_features) do
  redis.call('SREM', 'deblob:sem-sig:' .. hex, sch_id)
end
for _, hex in ipairs(new_features) do
  redis.call('SADD', 'deblob:sem-sig:' .. hex, sch_id)
end
redis.call('HSET', active_key, 'feature_keys_json', new_feature_keys_json)

redis.call('XADD', audit_key, '*',
  'actor', actor, 'reason', reason, 'schema', sch_id, 'sem', new_sem_id, 'ts', recorded_at)

return {new_revision_id, new_sem_id, tostring(new_etag), 'appended'}
"#;

/// Task 10 IDF read snapshot (`jr-deblob-similarity-idf-221040`): returns the
/// active-annotated population `N` and the document frequency `df` of every
/// requested feature posting, in ONE atomic script so the caller never observes
/// a torn view across a concurrent index transition. Reply is an integer array
/// `{N, df(ARGV[1]), df(ARGV[2]), ...}` aligned to the ARGV feature-hex order.
/// `N = SCARD deblob:sem-active-schemas`; `df = SCARD deblob:sem-sig:<hex>`
/// (a missing posting key SCARDs to 0). Pure reads — no key mutation, safe to
/// run against a replica.
pub const SEM_IDF_STATS_SCRIPT: &str = r#"
local out = {}
out[1] = redis.call('SCARD', 'deblob:sem-active-schemas')
for i = 1, #ARGV do
  out[i + 1] = redis.call('SCARD', 'deblob:sem-sig:' .. ARGV[i])
end
return out
"#;
