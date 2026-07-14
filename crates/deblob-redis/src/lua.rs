//! The atomic publication script. Spec §6: "Publication is one atomic Lua
//! transition: schema record + family version + index entries + alias +
//! audit event — or nothing."
//!
//! KEYS:
//!   1. schema key            deblob:schema:<sch_id>
//!   2. family key            deblob:family:<fam_id>
//!   3. alias key             deblob:alias:<cand_id>
//!   4. structural index key  deblob:index:structural
//!   5. audit stream key      deblob:audit:log
//!   6. published-marker key  deblob:published:<sch_id>
//!
//! ARGV:
//!   1. schema_json    canonical JSON blob to store under the schema key
//!   2. family_id      recorded for parity / future audit use
//!   3. schema_id      the terminal schema id the alias resolves to
//!   4. bucket_member  structural index member to add
//!   5. actor
//!   6. reason
//!   7. now_ms
//!
//! Semantics (all decided BEFORE any write, so a rejected call leaves no
//! partial state):
//!   - Immutability: if the schema key already holds bytes that differ from
//!     ARGV[1], fail fatally with IMMUTABILITY. Never treated as dedupe.
//!   - Alias write-once: if the alias key already points at a different
//!     schema id than ARGV[3], fail with ALIAS_CONFLICT.
//!   - Idempotent republish: if the schema bytes are identical (or the key
//!     doesn't exist yet) and the alias agrees (or doesn't exist yet), the
//!     call proceeds. Family-version allocation is guarded by the
//!     published-marker key so a retry of the SAME publish never
//!     double-increments the family counter — it returns the version
//!     recorded on the first successful publish.
//!   - Family version allocation (first-time publications only) is done via
//!     HINCRBY on the family hash, which is atomic on the Redis server, so
//!     concurrent first-time publishes of distinct schemas to the same
//!     family always get distinct, consecutive versions.
pub const PUBLISH_SCRIPT: &str = r#"
local schema_key = KEYS[1]
local family_key = KEYS[2]
local alias_key = KEYS[3]
local index_key = KEYS[4]
local audit_key = KEYS[5]
local published_key = KEYS[6]

local schema_json = ARGV[1]
local family_id = ARGV[2]
local schema_id = ARGV[3]
local bucket_member = ARGV[4]
local actor = ARGV[5]
local reason = ARGV[6]
local now_ms = ARGV[7]

local existing_schema = redis.call('GET', schema_key)
if existing_schema and existing_schema ~= schema_json then
  return redis.error_reply('IMMUTABILITY')
end

local existing_alias = redis.call('GET', alias_key)
if existing_alias and existing_alias ~= schema_id then
  return redis.error_reply('ALIAS_CONFLICT')
end

if not existing_schema then
  redis.call('SET', schema_key, schema_json)
end
if not existing_alias then
  redis.call('SET', alias_key, schema_id)
end

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

redis.call('SADD', index_key, bucket_member)
redis.call('XADD', audit_key, '*', 'actor', actor, 'reason', reason, 'schema', schema_id, 'ts', now_ms)

return version
"#;
