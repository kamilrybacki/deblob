//! Deterministic structural-index bucket keying, derived from a
//! [`crate::ShapeSummary`]. This is the single source of truth for the
//! bucket-key algorithm: both the permanent vault (`deblob-redis`, which
//! indexes schemas under this key at publish time) and the hot-path matcher
//! (`deblob`, which must compute the *same* key to look a fingerprint up)
//! depend on this crate, so hoisting the algorithm here — rather than
//! duplicating it in both consumers — makes drift between "where a schema
//! was indexed" and "where the matcher looks for it" structurally
//! impossible.

use data_encoding::HEXLOWER;
use sha2::{Digest, Sha256};

use crate::shape::ShapeSummary;

/// The field-count "band" component of a [`bucket_key`]: `0` for zero
/// top-level fields (`usize::next_power_of_two` would otherwise round `0`
/// up to `1`, colliding the "no top-level fields" case with "exactly one
/// top-level field"), otherwise `top_level_fields.next_power_of_two()`.
///
/// Exposed separately from `bucket_key` (rather than only inline there) so
/// callers that need to discover buckets by `(band, depth)` PREFIX —
/// ignoring the `reqhash8` component entirely, deblob-p2ab Task 3's
/// renamed-top-level-field recall fix — can compute the exact same band
/// `bucket_key` would have produced for a given field count, without
/// duplicating the zero-collision special case.
pub fn fieldband(top_level_fields: usize) -> u32 {
    let band = if top_level_fields == 0 {
        0
    } else {
        top_level_fields.next_power_of_two()
    };
    band as u32
}

/// Deterministic Redis key for the bucket a schema with this [`ShapeSummary`]
/// belongs to: `"deblob:index:{fieldband}:{depth}:{reqhash8}"`.
///
/// - `fieldband` = [`fieldband`] of `top_level_fields`.
/// - `reqhash8` = first 8 lowercase hex characters of
///   `sha256(top_keys_sorted.join("\0"))`.
///
/// Pure and deterministic: the same `ShapeSummary` always produces the same
/// key, on any run, on any machine (pinned by a golden-string test).
pub fn bucket_key(summary: &ShapeSummary) -> String {
    let band = fieldband(summary.top_level_fields);
    let joined = summary.top_keys_sorted.join("\0");
    let mut hasher = Sha256::new();
    hasher.update(joined.as_bytes());
    let digest = hasher.finalize();
    let hex = HEXLOWER.encode(&digest);
    let reqhash8 = &hex[..8];
    format!("deblob:index:{band}:{}:{reqhash8}", summary.depth)
}

/// The `SCAN MATCH` prefix pattern that matches every bucket for a given
/// `(band, depth)` pair, regardless of `reqhash8` —
/// `"deblob:index:{band}:{depth}:*"`. This is what widened bucket
/// DISCOVERY (deblob-p2ab Task 3 fix) scans for: a family whose top-level
/// field NAMES were merely renamed (case/separator variant, same
/// structure) hashes to a DIFFERENT `reqhash8` at the SAME `(band, depth)`,
/// so an exact [`bucket_key`] lookup computed from a candidate's own
/// top-level key names can never find it — matching by prefix can.
pub fn band_depth_prefix(band: u32, depth: u32) -> String {
    format!("deblob:index:{band}:{depth}:*")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_key_is_stable() {
        // Golden string: pins the exact bucket_key format. Any change to
        // field-band math, depth math, key-hash input, or key layout must
        // break this test. This is the single source-of-truth golden for
        // the algorithm; `deblob-redis`'s own golden test exercises the
        // same value through its public re-export.
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
    fn fieldband_matches_bucket_key_component() {
        // fieldband() must agree with the band bucket_key() embeds, for
        // every top_level_fields value bucket_key's own golden tests use.
        assert_eq!(fieldband(0), 0);
        assert_eq!(fieldband(1), 1);
        assert_eq!(fieldband(2), 2);
        assert_eq!(fieldband(3), 4);
        assert_eq!(fieldband(4), 4);
        assert_eq!(fieldband(5), 8);
    }

    #[test]
    fn band_depth_prefix_matches_bucket_key_layout() {
        let summary = ShapeSummary {
            top_level_fields: 3,
            depth: 2,
            top_keys_sorted: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let exact = bucket_key(&summary);
        let prefix = band_depth_prefix(fieldband(summary.top_level_fields), summary.depth);
        assert_eq!(prefix, "deblob:index:4:2:*");
        assert!(
            exact.starts_with(prefix.trim_end_matches('*')),
            "exact bucket_key must fall under its own band_depth_prefix"
        );
    }

    #[test]
    fn band_depth_prefix_ignores_reqhash_by_construction() {
        // Two ShapeSummary values with the SAME (band, depth) but
        // DIFFERENT top_keys_sorted (i.e. a renamed-field family) produce
        // different exact bucket_key values but the SAME prefix — this is
        // the whole point of the prefix: it's blind to reqhash8.
        let a = ShapeSummary {
            top_level_fields: 2,
            depth: 2,
            top_keys_sorted: vec!["widgetCount".to_string(), "itemName".to_string()],
        };
        let b = ShapeSummary {
            top_level_fields: 2,
            depth: 2,
            top_keys_sorted: vec!["widget_count".to_string(), "item_name".to_string()],
        };
        assert_ne!(bucket_key(&a), bucket_key(&b));
        assert_eq!(
            band_depth_prefix(fieldband(a.top_level_fields), a.depth),
            band_depth_prefix(fieldband(b.top_level_fields), b.depth)
        );
    }
}
