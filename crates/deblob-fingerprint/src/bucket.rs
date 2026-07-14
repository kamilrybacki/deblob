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

/// Deterministic Redis key for the bucket a schema with this [`ShapeSummary`]
/// belongs to: `"deblob:index:{fieldband}:{depth}:{reqhash8}"`.
///
/// - `fieldband` = `top_level_fields.next_power_of_two()`, with `0` mapped
///   to `0` (Rust's `next_power_of_two` would otherwise round `0` up to
///   `1`, which would collide the "no top-level fields" case with "exactly
///   one top-level field").
/// - `reqhash8` = first 8 lowercase hex characters of
///   `sha256(top_keys_sorted.join("\0"))`.
///
/// Pure and deterministic: the same `ShapeSummary` always produces the same
/// key, on any run, on any machine (pinned by a golden-string test).
pub fn bucket_key(summary: &ShapeSummary) -> String {
    let fieldband = if summary.top_level_fields == 0 {
        0
    } else {
        summary.top_level_fields.next_power_of_two()
    };
    let joined = summary.top_keys_sorted.join("\0");
    let mut hasher = Sha256::new();
    hasher.update(joined.as_bytes());
    let digest = hasher.finalize();
    let hex = HEXLOWER.encode(&digest);
    let reqhash8 = &hex[..8];
    format!("deblob:index:{fieldband}:{}:{reqhash8}", summary.depth)
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
}
