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
redis.call('XADD', audit_key, '*', 'actor', actor, 'reason', reason, 'schema', schema_id, 'ts', now_ms)

return version
"#;
