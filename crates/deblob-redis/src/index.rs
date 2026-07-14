//! The bucketed structural index (spec §6, Task 8).
//!
//! Every published schema lands in a small Redis `SET`, keyed by a
//! deterministic bucket derived from a lossy [`deblob_fingerprint::ShapeSummary`] of its shape
//! (`deblob-fingerprint`): field-count band, nesting depth, and a short
//! hash of the required top-level keys. Buckets are small *by
//! construction* — looking a schema up is always a bounded operation on
//! one bucket's members, never a scan over `deblob:schema:*`.
//!
//! The index is entirely **derived** from the authoritative
//! `deblob:schema:*` hashes (Task 7): every schema's hash also carries a
//! `bucket` field recording which bucket key it was filed under at publish
//! time, so [`RedisRegistry::rebuild_index`] can reconstruct every bucket
//! set from scratch without re-deriving a `ShapeSummary` from the stored
//! canonical bytes. [`RedisRegistry::verify_index`] cross-checks the two
//! directions (bucket → schema, schema → bucket) and reports drift.

use deblob_core::error::CoreError;
use deblob_core::id::SchemaId;
use redis::AsyncCommands;

use crate::registry::{redis_err, RedisRegistry};

/// Redis pattern matching every derived structural-index key. `rebuild_index`
/// drops everything matching this pattern before reconstructing it, and
/// `verify_index` walks it to check bucket → schema consistency.
pub const INDEX_KEY_PATTERN: &str = "deblob:index:*";

/// Deterministic Redis key for the bucket a schema with this `ShapeSummary`
/// belongs to: `"deblob:index:{fieldband}:{depth}:{reqhash8}"`.
///
/// Re-exported from `deblob-fingerprint`, which is the single source of
/// truth for the bucket-key algorithm (see [`deblob_fingerprint::bucket_key`]
/// for the full algorithm docs and its pinned golden test). Both this crate
/// (indexing a schema at publish time) and the hot-path matcher in
/// `deblob` (looking a fingerprint up) depend on `deblob-fingerprint` and
/// call the same function, so the two can never compute different keys for
/// the same shape.
pub use deblob_fingerprint::bucket_key;

/// The bucket-set member recorded for `schema_id`: `"<fp_b32>=<sch_id>"`.
///
/// `fp_b32` is the schema's fingerprint in the same lowercase-base32
/// encoding embedded in its own id (a `SchemaId` IS `"sch_" + base32(sha256
/// fingerprint of its canonical shape)`, per `deblob-core::id`'s
/// immutable-identity design) — so `fp_b32` is simply that id with its
/// `"sch_"` prefix stripped. Keeping it as an explicit prefix (rather than
/// relying on callers to strip the prefix themselves) lets
/// `resolve_structural` do a bounded `SSCAN MATCH "<fp_b32>=*"` against a
/// bucket instead of enumerating every member.
pub fn bucket_member(schema_id: &SchemaId) -> String {
    let full = schema_id.as_str();
    let fp_b32 = full.strip_prefix("sch_").unwrap_or(full);
    format!("{fp_b32}={full}")
}

/// A bucket-set member for an arbitrary observed `fp_b32` pointing at
/// `schema_id`: `"<fp_b32>=<sch_id>"`. Generalizes `bucket_member` (which is
/// just `variant_member` called with `schema_id`'s own digest) to Task 14's
/// variant-indexing fix: any CONCRETE shape's raw-fingerprint base32 body
/// can be indexed here, not only the schema's own (generalized) digest.
pub fn variant_member(fp_b32: &str, schema_id: &SchemaId) -> String {
    format!("{fp_b32}={}", schema_id.as_str())
}

/// Serializes a schema's observed concrete-shape variants — `(bucket_key,
/// fp_b32)` pairs — into the JSON array stored on the schema hash's
/// `variants` field: `["<bucket>=<fp_b32>", ...]`. This is what
/// [`RedisRegistry::rebuild_index`] reads back to restore variant
/// membership from the authoritative schema record alone (spec §6:
/// rebuildable from schema records), without needing the (ephemeral,
/// TTL'd) `EvidenceStore` candidate-variant sets to still exist.
pub fn encode_variants_field(variants: &[(String, String)]) -> String {
    let entries: Vec<String> = variants
        .iter()
        .map(|(bucket, fp_b32)| format!("{bucket}={fp_b32}"))
        .collect();
    serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Inverse of [`encode_variants_field`]: parses the schema hash's `variants`
/// field JSON back into `(bucket_key, fp_b32)` pairs. Malformed entries
/// (no `=`) are silently skipped rather than failing the whole rebuild —
/// mirrors `rebuild_index`'s existing "skip what can't be reconstructed"
/// posture for pre-Task-14 schema records that have no `variants` field at
/// all (handled by the caller via `Option`, not here).
fn decode_variants_field(json: &str) -> Vec<(String, String)> {
    let entries: Vec<String> = serde_json::from_str(json).unwrap_or_default();
    entries
        .into_iter()
        .filter_map(|entry| {
            entry
                .split_once('=')
                .map(|(bucket, fp_b32)| (bucket.to_string(), fp_b32.to_string()))
        })
        .collect()
}

/// The `fp_b32=*` match pattern used to find `schema_id`'s membership
/// within a bucket via a bounded `SSCAN`.
fn fp_match_pattern(schema_id: &SchemaId) -> String {
    let full = schema_id.as_str();
    let fp_b32 = full.strip_prefix("sch_").unwrap_or(full);
    format!("{fp_b32}=*")
}

/// Splits a `"<fp_b32>=<sch_id>"` bucket member back into its two halves.
/// Returns `None` for a malformed member (no `=`), which `verify_index`
/// reports as an inconsistency rather than panicking on.
fn split_member(member: &str) -> Option<(&str, &str)> {
    member.split_once('=')
}

impl RedisRegistry {
    /// Look up `bucket_key`'s member set for a member whose `fp_b32` half
    /// matches `fingerprint`'s base32 body, via a bounded `SSCAN MATCH` —
    /// never a scan over `deblob:schema:*` or over every bucket. Buckets
    /// are small by construction, so this terminates after at most a
    /// handful of `SSCAN` round-trips even on a very large vault.
    pub(crate) async fn resolve_structural_bucketed(
        &self,
        bucket_key: &str,
        fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        let mut conn = self.conn();
        let pattern = fp_match_pattern(fingerprint);
        let mut cursor = "0".to_string();
        loop {
            let (next_cursor, members): (String, Vec<String>) = redis::cmd("SSCAN")
                .arg(bucket_key)
                .arg(&cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            for member in &members {
                if let Some((_, sch)) = split_member(member) {
                    let id = SchemaId::parse(sch).map_err(|e| {
                        CoreError::RegistryUnavailable(format!(
                            "corrupt index member {member:?} in bucket {bucket_key}: {e:?}"
                        ))
                    })?;
                    return Ok(Some(id));
                }
            }

            if next_cursor == "0" {
                return Ok(None);
            }
            cursor = next_cursor;
        }
    }

    /// Rebuild the entire structural index from scratch, purely from the
    /// authoritative `deblob:schema:*` records: drops every key matching
    /// [`INDEX_KEY_PATTERN`], then re-`SADD`s each schema's membership into
    /// the bucket recorded on its own hash (the `bucket` field written by
    /// `publish`). The index is disposable — this is always safe to run,
    /// online, at any time. Returns the number of schemas reindexed.
    pub async fn rebuild_index(&self) -> Result<u64, CoreError> {
        let mut conn = self.conn();

        delete_matching(conn.clone(), INDEX_KEY_PATTERN).await?;

        let mut count: u64 = 0;
        let mut cursor = "0".to_string();
        loop {
            let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("deblob:schema:*")
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            for key in &keys {
                let sch_id_str = key.strip_prefix("deblob:schema:").unwrap_or(key);
                let bucket: Option<String> = conn.hget(key, "bucket").await.map_err(redis_err)?;
                let Some(bucket) = bucket else {
                    // Defensive: a schema published before the `bucket`
                    // field existed has nothing to rebuild from. Skip
                    // rather than fail the whole rebuild.
                    continue;
                };
                let schema_id = SchemaId::parse(sch_id_str).map_err(|e| {
                    CoreError::RegistryUnavailable(format!("corrupt schema key {key}: {e:?}"))
                })?;
                let member = bucket_member(&schema_id);
                let _: () = conn.sadd(&bucket, member).await.map_err(redis_err)?;
                count += 1;

                // Task 14 fix: also restore every CONCRETE-shape variant
                // member recorded on this schema's `variants` field at
                // publish time, into ITS OWN bucket (which may differ from
                // the schema's own self-referential `bucket` above — an
                // observed variant with more/fewer top-level fields can
                // band into a different structural bucket). Absent for
                // schemas published before this field existed (or with no
                // recorded variants) — `decode_variants_field` returns an
                // empty vec for both, so this loop is simply a no-op then.
                let variants_json: Option<String> =
                    conn.hget(key, "variants").await.map_err(redis_err)?;
                if let Some(variants_json) = variants_json {
                    for (variant_bucket, fp_b32) in decode_variants_field(&variants_json) {
                        let vmember = variant_member(&fp_b32, &schema_id);
                        let _: () = conn
                            .sadd(&variant_bucket, vmember)
                            .await
                            .map_err(redis_err)?;
                    }
                }
            }

            if next_cursor == "0" {
                break;
            }
            cursor = next_cursor;
        }

        Ok(count)
    }

    /// Cross-checks the structural index for drift against the
    /// authoritative `deblob:schema:*` records and returns a human-readable
    /// description of every inconsistency found (empty if none): a bucket
    /// member that points at a schema that doesn't exist, or a schema whose
    /// recorded bucket doesn't actually contain its membership. Unlike
    /// `resolve_structural`, this is a full audit sweep — intended for
    /// offline / maintenance use, not the hot path.
    pub async fn verify_index(&self) -> Result<Vec<String>, CoreError> {
        let mut conn = self.conn();
        let mut problems = Vec::new();

        // Direction 1: bucket -> schema. Every member of every
        // deblob:index:* set must point at a schema that actually exists.
        let mut cursor = "0".to_string();
        loop {
            let (next_cursor, buckets): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg(INDEX_KEY_PATTERN)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            for bucket in &buckets {
                let members: Vec<String> = conn.smembers(bucket).await.map_err(redis_err)?;
                for member in members {
                    match split_member(&member) {
                        None => problems.push(format!(
                            "bucket {bucket} has malformed member {member:?} (no '=')"
                        )),
                        Some((_, sch)) => {
                            let exists: bool = conn
                                .exists(format!("deblob:schema:{sch}"))
                                .await
                                .map_err(redis_err)?;
                            if !exists {
                                problems.push(format!(
                                    "bucket {bucket} member {member:?} points at missing schema {sch}"
                                ));
                            }
                        }
                    }
                }
            }

            if next_cursor == "0" {
                break;
            }
            cursor = next_cursor;
        }

        // Direction 2: schema -> bucket. Every schema's recorded bucket
        // must actually contain its own membership.
        let mut cursor = "0".to_string();
        loop {
            let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
                .arg(&cursor)
                .arg("MATCH")
                .arg("deblob:schema:*")
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;

            for key in &keys {
                let sch_id_str = key.strip_prefix("deblob:schema:").unwrap_or(key);
                let bucket: Option<String> = conn.hget(key, "bucket").await.map_err(redis_err)?;
                match bucket {
                    None => problems.push(format!("schema {sch_id_str} has no recorded bucket")),
                    Some(bucket) => match SchemaId::parse(sch_id_str) {
                        Err(e) => {
                            problems.push(format!("schema key {key} has invalid schema id: {e:?}"))
                        }
                        Ok(schema_id) => {
                            let member = bucket_member(&schema_id);
                            let is_member: bool =
                                conn.sismember(&bucket, &member).await.map_err(redis_err)?;
                            if !is_member {
                                problems.push(format!(
                                    "schema {sch_id_str} missing from its recorded bucket {bucket}"
                                ));
                            }
                        }
                    },
                }
            }

            if next_cursor == "0" {
                break;
            }
            cursor = next_cursor;
        }

        Ok(problems)
    }
}

/// Deletes every key matching `pattern`, via bounded `SCAN` + `DEL` batches
/// (never `KEYS`, which blocks the whole server on a large vault).
async fn delete_matching(
    mut conn: redis::aio::MultiplexedConnection,
    pattern: &str,
) -> Result<(), CoreError> {
    let mut cursor = "0".to_string();
    loop {
        let (next_cursor, keys): (String, Vec<String>) = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(200)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        if !keys.is_empty() {
            let _: () = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
        }

        if next_cursor == "0" {
            return Ok(());
        }
        cursor = next_cursor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_fingerprint::ShapeSummary;

    #[test]
    fn bucket_key_is_stable() {
        // Golden string: pins the exact bucket_key format. Any change to
        // field-band math, depth math, key-hash input, or key layout must
        // break this test.
        let summary = ShapeSummary {
            top_level_fields: 3,
            depth: 2,
            top_keys_sorted: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        assert_eq!(
            bucket_key(&summary),
            "deblob:index:4:2:8badde10",
            "bucket_key golden string changed — verify the change is intentional"
        );
    }

    #[test]
    fn bucket_key_zero_fields_bands_to_zero_not_one() {
        let summary = ShapeSummary {
            top_level_fields: 0,
            depth: 1,
            top_keys_sorted: vec![],
        };
        assert!(bucket_key(&summary).starts_with("deblob:index:0:1:"));
    }

    #[test]
    fn bucket_key_is_deterministic_across_calls() {
        let summary = ShapeSummary {
            top_level_fields: 5,
            depth: 3,
            top_keys_sorted: vec!["x".to_string(), "y".to_string()],
        };
        assert_eq!(bucket_key(&summary), bucket_key(&summary));
    }

    #[test]
    fn bucket_member_round_trips_via_split() {
        let id = SchemaId::from_digest(&[7u8; 32]);
        let member = bucket_member(&id);
        let (fp, sch) = split_member(&member).unwrap();
        assert_eq!(sch, id.as_str());
        assert_eq!(format!("sch_{fp}"), id.as_str());
    }
}
