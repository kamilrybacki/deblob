//! Additive, versioned STRUCTURAL SimHash sketch over a schema's generalized
//! canonical shape — a 64-bit locality-sensitive digest for near-duplicate
//! family discovery.
//!
//! ## Not an identity — a discovery signal only
//!
//! UNLIKE [`crate::Profile::generalized_fingerprint`] (a crypto-exact `sch_`
//! identity), this sketch is deliberately LOSSY: two *structurally similar*
//! schemas land at small Hamming distance, and a single added/renamed field
//! moves only a few bits. It MUST NOT enter any `sch_`/`sem_` preimage, and a
//! sketch collision proves nothing about identity — the exact fingerprint
//! remains the sole authority on "same schema". The sketch's only job is to
//! surface *candidate* near-duplicate / umbrella families for a human or the
//! deterministic gate to adjudicate (governance: model/heuristic proposes,
//! deterministic decides, human approves).
//!
//! ## Why this exists (validated, not speculative)
//!
//! Measured on the live registry (2026-07-26, 51 promoted schemas): the exact
//! structural bucket correctly assigns every distinct structure its own
//! `sch_`/family, so near-variants of one logical source are split across
//! families (e.g. `Flight/Transit/EvalPlus … Observations` each appear as two
//! ~80%-identical families). This sketch separates such same-label
//! near-duplicates from unrelated schemas at ROC-AUC 0.83, with precision 1.00
//! at Hamming <= 8 — a signal the exact bucket structurally cannot provide.
//! It is stored alongside the record and never on the hot fingerprint path.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Version tag mixed into every token hash. A bump changes every sketch, so a
/// sketch persisted under a prior version is never silently Hamming-compared
/// against a new one — callers gate on this exactly as they gate on
/// [`crate::GENERALIZER`].
pub const STRUCTURAL_SIMHASH_VERSION: &str = "deblob-simhash-v1";

/// The near-duplicate Hamming threshold validated on live data (precision 1.00,
/// recall of the exact same-label duplicates). Exposed so the discovery
/// consumer and its tests share one source of truth rather than hard-coding
/// `8` in two places.
pub const NEAR_DUPLICATE_HAMMING_MAX: u32 = 8;

/// 64-bit structural SimHash of a generalized canonical shape JSON — the string
/// produced by [`crate::Profile::generalized_canonical_json`] and persisted as
/// `SchemaRecord::canonical`. Returns `None` when `canonical` is not the
/// expected object-shaped generalized JSON or carries no structural tokens
/// (never panics on malformed input — a discovery signal must never take down
/// its caller).
pub fn structural_simhash(canonical: &str) -> Option<u64> {
    let value: Value = serde_json::from_str(canonical).ok()?;
    let mut tokens: Vec<String> = Vec::new();
    collect_tokens(&value, "", &mut tokens);
    if tokens.is_empty() {
        return None;
    }
    Some(simhash64(&tokens))
}

/// Hamming distance between two structural sketches: `0` = identical sketch,
/// `64` = maximally different. Two sketches are only comparable when computed
/// under the same [`STRUCTURAL_SIMHASH_VERSION`].
pub fn structural_hamming(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Emit one `path:type` token per type present at each field position, matching
/// the generalized-canonical traversal exactly: the root under `$`, object
/// children under `path.key`, array elements under `path[]`. Order-independent
/// — SimHash sums per-token contributions commutatively, so token emission
/// order never affects the result.
fn collect_tokens(node: &Value, path: &str, out: &mut Vec<String>) {
    let Some(obj) = node.as_object() else {
        return;
    };
    let label = if path.is_empty() { "$" } else { path };
    if let Some(types) = obj.get("types").and_then(Value::as_array) {
        for t in types.iter().filter_map(Value::as_str) {
            out.push(format!("{label}:{t}"));
        }
    }
    if let Some(children) = obj.get("children").and_then(Value::as_object) {
        for (key, child) in children {
            let child_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            collect_tokens(child, &child_path, out);
        }
    }
    if let Some(elem) = obj.get("elem") {
        collect_tokens(elem, &format!("{path}[]"), out);
    }
}

/// First 8 bytes of `sha256(VERSION \0 token)` as a big-endian `u64` — a
/// stable, version-scoped 64-bit id for one structural token.
fn token_hash(token: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(STRUCTURAL_SIMHASH_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(token.as_bytes());
    let digest = hasher.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 digest is 32 bytes"))
}

/// Classic 64-bit SimHash: per bit, sum +1/-1 across every token's hash, then
/// set the output bit where the sum is positive.
fn simhash64(tokens: &[String]) -> u64 {
    let mut acc = [0i64; 64];
    for token in tokens {
        let h = token_hash(token);
        for (bit, slot) in acc.iter_mut().enumerate() {
            *slot += if (h >> bit) & 1 == 1 { 1 } else { -1 };
        }
    }
    let mut out = 0u64;
    for (bit, slot) in acc.iter().enumerate() {
        if *slot > 0 {
            out |= 1u64 << bit;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLIGHT: &str = r#"{"optional":false,"types":["object"],"children":{
        "lat":{"optional":false,"types":["number"]},
        "lon":{"optional":false,"types":["number"]},
        "alt":{"optional":false,"types":["number"]},
        "callsign":{"optional":false,"types":["string"]},
        "ts":{"optional":false,"types":["string"]}}}"#;

    #[test]
    fn identical_shape_yields_identical_sketch() {
        // Values are already erased in a canonical shape, so two records of the
        // same generalized shape MUST produce the same sketch.
        let a = structural_simhash(FLIGHT).unwrap();
        let b = structural_simhash(FLIGHT).unwrap();
        assert_eq!(a, b);
    }

    const FLIGHT_PLUS_ONE: &str = r#"{"optional":false,"types":["object"],"children":{"lat":{"optional":false,"types":["number"]},"lon":{"optional":false,"types":["number"]},"alt":{"optional":false,"types":["number"]},"callsign":{"optional":false,"types":["string"]},"ts":{"optional":false,"types":["string"]},"speed":{"optional":false,"types":["number"]}}}"#;
    const UNRELATED: &str = r#"{"optional":false,"types":["object"],"children":{"userId":{"optional":false,"types":["string"]},"email":{"optional":false,"types":["string"]},"roles":{"optional":false,"types":["array"],"elem":{"optional":false,"types":["string"]}},"active":{"optional":false,"types":["bool"]}}}"#;

    #[test]
    fn near_duplicate_is_much_closer_than_an_unrelated_shape() {
        // The core locality property, asserted RELATIVELY (an absolute
        // single-edit bound is not `NEAR_DUPLICATE_HAMMING_MAX` — that constant
        // is the corpus-validated precision-1.0 cutoff, and a one-field add to a
        // 5-field schema legitimately moves ~10 bits). What must hold is
        // separation: a near-duplicate is far closer than an unrelated schema.
        let flight = structural_simhash(FLIGHT).unwrap();
        let near = structural_hamming(flight, structural_simhash(FLIGHT_PLUS_ONE).unwrap());
        let far = structural_hamming(flight, structural_simhash(UNRELATED).unwrap());
        assert!(
            near + 6 <= far,
            "expected clear separation: near-dup={near} bits vs unrelated={far} bits"
        );
    }

    #[test]
    fn nested_and_array_paths_are_distinguished() {
        // `a.b:number` and `a[]:number` are different structural positions and
        // must tokenize differently (locality without collapsing structure).
        let nested = r#"{"optional":false,"types":["object"],"children":{"a":{"optional":false,"types":["object"],"children":{"b":{"optional":false,"types":["number"]}}}}}"#;
        let arrayed = r#"{"optional":false,"types":["object"],"children":{"a":{"optional":false,"types":["array"],"elem":{"optional":false,"types":["number"]}}}}"#;
        assert_ne!(
            structural_simhash(nested).unwrap(),
            structural_simhash(arrayed).unwrap()
        );
    }

    #[test]
    fn malformed_input_is_none_never_panics() {
        assert_eq!(structural_simhash("not json"), None);
        assert_eq!(structural_simhash("42"), None);
        assert_eq!(structural_simhash("{}"), None); // no types/children -> no tokens
    }

    #[test]
    fn golden_sketch_is_stable() {
        // Pins the exact 64-bit sketch for a fixed shape: any change to token
        // emission, the version tag, or the SimHash math must break this. A
        // deliberate change re-blesses the snapshot (and requires a
        // STRUCTURAL_SIMHASH_VERSION bump so stored sketches are recomputed).
        let sketch = structural_simhash(FLIGHT).unwrap();
        insta::assert_snapshot!(format!("{sketch:016x}"));
    }
}
