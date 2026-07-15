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
//!   - variant bucket keys (KEYS[7..6+N], Task 14 fix): one structural-index
//!     SET key per observed CONCRETE shape recorded against the promoted
//!     candidate (`EvidenceStore::get_variants`), possibly a DIFFERENT
//!     bucket than KEYS[4]: an observed variant with more/fewer top-level
//!     fields than the candidate's generalized profile can band into a
//!     different `ShapeSummary` bucket. `N` is derived from `#KEYS - 6`, so
//!     `N == 0` (no extra KEYS beyond the fixed six) is a valid, no-op call
//!     — a candidate promoted with no recorded variants must not fail.
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

-- Task 14 fix: index every observed CONCRETE shape recorded against this
-- candidate, into ITS OWN bucket (KEYS[7..], one per variant, possibly
-- distinct from index_key), so a hot-path lookup of any previously
-- observed shape resolves to schema_id — not just the schema's own
-- generalized digest. Unconditional (not gated behind `not
-- existing_canonical`/`not existing_alias`) and idempotent (SADD), so a
-- republish that now knows about MORE variants than the original publish
-- still gets them indexed. The `variants` field is likewise always
-- refreshed to the fullest set of variants any publish call has supplied,
-- so `rebuild_index` can restore all of them later purely from this hash.
redis.call('HSET', schema_key, 'variants', variants_json)
local variant_count = #KEYS - 6
for i = 1, variant_count do
  local variant_bucket_key = KEYS[6 + i]
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

local cur_revision_id = redis.call('HGET', active_key, 'revision_id')
local cur_sem_id = redis.call('HGET', active_key, 'sem_id')
local cur_etag_str = redis.call('HGET', active_key, 'etag')
local cur_etag = tonumber(cur_etag_str) or 0

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

redis.call('XADD', audit_key, '*',
  'actor', actor, 'reason', reason, 'schema', sch_id, 'sem', new_sem_id, 'ts', recorded_at)

return {new_revision_id, new_sem_id, tostring(new_etag), 'appended'}
"#;
