//! Value-profile extraction (Stage 1 of the joint `dc-umbrella-signals-1907`
//! design): flatten a candidate's monoid [`Profile`] into a durable, immutable
//! [`ValueProfileSnapshot`] captured atomically at promotion.
//!
//! The snapshot is a compact SIDECAR (referenced by `SchemaRecord`, never
//! embedded, never part of any identity digest). It carries ONLY coarse,
//! non-reversible evidence — per-type counts + the OR-merged numeric-bucket
//! bitmask — never a raw observed value, preserving the privacy invariant
//! (spec §9). Leaf paths mirror the schema-canonical walk exactly (the same
//! semantics `crate::api::umbrellas::child_fields_from_schema` uses) so the
//! Stage-2 guard can join a leaf's evidence to its `canonical_field_id` on
//! `path`. A mis-attached profile is worse than none, so the snapshot binds
//! itself to the exact inputs that produced it (canonicalizer + schema
//! canonical digest + candidate id + candidate profile digest).

use deblob_core::id::{CandidateId, SchemaId, ValueProfileId};
use deblob_core::ports::{value_bucket, LeafTypeCounts, LeafValueProfile, ValueProfileSnapshot};
use deblob_monoid::{FieldNode, NumericBuckets, Profile, TypeCounts, GENERALIZER};
use sha2::{Digest, Sha256};

pub const VALUE_PROFILE_VERSION: u32 = 1;
pub const BUCKET_BOUNDARIES_VERSION: u32 = 1;

fn mask_of(b: &NumericBuckets) -> u8 {
    let mut m = 0u8;
    if b.negative {
        m |= value_bucket::NEGATIVE;
    }
    if b.zero {
        m |= value_bucket::ZERO;
    }
    if b.small_positive {
        m |= value_bucket::SMALL_POSITIVE;
    }
    if b.medium_positive {
        m |= value_bucket::MEDIUM_POSITIVE;
    }
    if b.large_positive {
        m |= value_bucket::LARGE_POSITIVE;
    }
    m
}

fn counts_of(t: &TypeCounts) -> LeafTypeCounts {
    LeafTypeCounts {
        null: t.null,
        bool: t.bool,
        number: t.number,
        string: t.string,
        array: t.array,
        object: t.object,
    }
}

/// Emit one [`LeafValueProfile`] per leaf of the field tree. A leaf is any
/// node that is NOT an object-with-children — mirroring
/// `child_fields_from_schema`'s walk (which descends only into object nodes
/// with children and emits every other node, arrays included, as a leaf at
/// its own dotted path). The document root itself is never a leaf.
fn walk(node: &FieldNode, path: &str, out: &mut Vec<LeafValueProfile>) {
    if !node.children.is_empty() {
        // BTreeMap iterates in sorted key order -> deterministic output.
        for (k, child) in &node.children {
            let child_path = if path.is_empty() {
                k.clone()
            } else {
                format!("{path}.{k}")
            };
            walk(child, &child_path, out);
        }
        return;
    }
    if path.is_empty() {
        return; // root with no children: no leaves
    }
    out.push(LeafValueProfile {
        path: path.to_string(),
        present_count: node.present,
        explicit_null_count: node.explicit_null,
        type_counts: counts_of(&node.types),
        numeric_bucket_mask: mask_of(&node.numeric_buckets),
        int_only: node.int_only,
        neg_zero_seen: node.neg_zero_seen,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let d: [u8; 32] = Sha256::digest(bytes).into();
    hex_lower(&d)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

/// Builds the immutable value-profile snapshot for a schema being promoted
/// from `candidate`'s `profile`. `captured_at_ms` is threaded in (never read
/// from a clock here) so the caller controls capture time and the function
/// stays a pure function of its inputs.
///
/// `profile_id` is content-addressed over everything EXCEPT `captured_at_ms`,
/// so re-promoting the identical (schema, candidate-profile) pair yields the
/// identical id (replay-safe), while any change to the shape, the candidate,
/// or the observed evidence mints a new one.
pub fn build_snapshot(
    schema_id: &SchemaId,
    schema_canonical: &str,
    candidate_id: &CandidateId,
    profile: &Profile,
    captured_at_ms: i64,
) -> ValueProfileSnapshot {
    let mut leaves = Vec::new();
    walk(&profile.root, "", &mut leaves);

    let schema_canonical_digest = sha256_hex(schema_canonical.as_bytes());
    let candidate_profile_digest = serde_json::to_vec(profile)
        .map(|v| sha256_hex(&v))
        .unwrap_or_default();

    // Content-addressed id preimage: bind identity to the shape, the source
    // candidate, and the observed evidence — NOT the capture clock.
    let mut hasher = Sha256::new();
    hasher.update(b"deblob-value-profile-v1\0");
    hasher.update(schema_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(schema_canonical_digest.as_bytes());
    hasher.update([0]);
    hasher.update(candidate_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(candidate_profile_digest.as_bytes());
    let id_digest: [u8; 32] = hasher.finalize().into();
    let profile_id = ValueProfileId::from_digest(&id_digest);

    ValueProfileSnapshot {
        profile_id,
        profile_version: VALUE_PROFILE_VERSION,
        bucket_boundaries_version: BUCKET_BOUNDARIES_VERSION,
        canonicalizer: GENERALIZER.to_string(),
        schema_canonical_digest,
        candidate_id: candidate_id.clone(),
        candidate_profile_digest,
        observation_count: profile.count,
        captured_at_ms,
        leaves,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf_number(present: u64, buckets: NumericBuckets, int_only: bool) -> FieldNode {
        FieldNode {
            present,
            explicit_null: 0,
            types: TypeCounts {
                number: present,
                ..Default::default()
            },
            children: Default::default(),
            array_elem: None,
            array_empty_seen: false,
            array_partial_seen: false,
            int_only,
            neg_zero_seen: false,
            numeric_buckets: buckets,
        }
    }

    fn object(children: Vec<(&str, FieldNode)>) -> FieldNode {
        let mut map = std::collections::BTreeMap::new();
        let total: u64 = children.iter().map(|(_, c)| c.present).max().unwrap_or(0);
        for (k, v) in children {
            map.insert(k.to_string(), v);
        }
        FieldNode {
            present: total,
            explicit_null: 0,
            types: TypeCounts {
                object: total,
                ..Default::default()
            },
            children: map,
            array_elem: None,
            array_empty_seen: false,
            array_partial_seen: false,
            int_only: true,
            neg_zero_seen: false,
            numeric_buckets: NumericBuckets::default(),
        }
    }

    #[test]
    fn flattens_leaves_with_masks_and_counts() {
        let profile = Profile {
            count: 100,
            root: object(vec![
                (
                    "amount",
                    leaf_number(
                        100,
                        NumericBuckets {
                            large_positive: true,
                            ..Default::default()
                        },
                        true,
                    ),
                ),
                (
                    "ratio",
                    leaf_number(
                        90,
                        NumericBuckets {
                            small_positive: true,
                            zero: true,
                            ..Default::default()
                        },
                        false,
                    ),
                ),
            ]),
        };
        let sid = SchemaId::from_digest(&[1u8; 32]);
        let cid = CandidateId::from_digest(&[2u8; 32]);
        let snap = build_snapshot(&sid, "{\"canonical\":true}", &cid, &profile, 1234);

        assert_eq!(snap.observation_count, 100);
        assert_eq!(snap.captured_at_ms, 1234);
        assert_eq!(snap.leaves.len(), 2);
        assert!(snap.profile_id.as_str().starts_with("vp_"));

        // BTreeMap order: "amount" before "ratio".
        let amount = &snap.leaves[0];
        assert_eq!(amount.path, "amount");
        assert_eq!(amount.numeric_bucket_mask, value_bucket::LARGE_POSITIVE);
        assert!(amount.int_only);
        assert_eq!(amount.type_counts.number, 100);

        let ratio = &snap.leaves[1];
        assert_eq!(ratio.path, "ratio");
        assert_eq!(
            ratio.numeric_bucket_mask,
            value_bucket::SMALL_POSITIVE | value_bucket::ZERO
        );
        assert!(!ratio.int_only);
    }

    #[test]
    fn nested_paths_are_dotted() {
        let profile = Profile {
            count: 5,
            root: object(vec![(
                "main",
                object(vec![(
                    "temp",
                    leaf_number(5, NumericBuckets::default(), true),
                )]),
            )]),
        };
        let sid = SchemaId::from_digest(&[3u8; 32]);
        let cid = CandidateId::from_digest(&[4u8; 32]);
        let snap = build_snapshot(&sid, "{}", &cid, &profile, 0);
        assert_eq!(snap.leaves.len(), 1);
        assert_eq!(snap.leaves[0].path, "main.temp");
    }

    #[test]
    fn id_is_deterministic_across_capture_time_but_varies_by_evidence() {
        let profile = Profile {
            count: 10,
            root: object(vec![(
                "x",
                leaf_number(10, NumericBuckets::default(), true),
            )]),
        };
        let sid = SchemaId::from_digest(&[5u8; 32]);
        let cid = CandidateId::from_digest(&[6u8; 32]);
        let a = build_snapshot(&sid, "{}", &cid, &profile, 1);
        let b = build_snapshot(&sid, "{}", &cid, &profile, 999);
        assert_eq!(
            a.profile_id, b.profile_id,
            "capture time must not affect id"
        );

        let profile2 = Profile {
            count: 10,
            root: object(vec![(
                "x",
                leaf_number(
                    10,
                    NumericBuckets {
                        negative: true,
                        ..Default::default()
                    },
                    true,
                ),
            )]),
        };
        let c = build_snapshot(&sid, "{}", &cid, &profile2, 1);
        assert_ne!(
            a.profile_id, c.profile_id,
            "different evidence -> different id"
        );
    }
}
