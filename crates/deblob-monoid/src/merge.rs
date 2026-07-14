//! `Profile::merge`/`Profile::identity`: the monoid operation and neutral
//! element proven associative/commutative by the proptest laws below.

use std::collections::BTreeMap;

use crate::profile::{FieldNode, Profile, TypeCounts};

impl Profile {
    /// The neutral element: zero observed documents, empty root.
    /// `Profile::merge(a, &Profile::identity()) == *a` for every `a`.
    pub fn identity() -> Self {
        Self {
            count: 0,
            root: FieldNode::identity(),
        }
    }

    /// Combine two profiles into a new one summarizing every observation
    /// behind both. Immutable — never mutates `a` or `b`, always returns a
    /// fresh `Profile`. Associative and commutative, with
    /// [`Profile::identity`] as neutral element (proven by proptest
    /// below).
    pub fn merge(a: &Profile, b: &Profile) -> Profile {
        Profile {
            count: a.count + b.count,
            root: FieldNode::merge(&a.root, &b.root),
        }
    }
}

impl FieldNode {
    /// Combine two field observations into a new one. Immutable — never
    /// mutates `a` or `b`. Element-wise `u64` addition, recursive
    /// `BTreeMap` union merge of children, `array_elem` merge, and bool
    /// flags combined with OR (each records whether the property was ever
    /// observed in either operand), except `int_only` which merges with AND
    /// (it is a universal claim: "all numbers seen here were integer-text").
    pub(crate) fn merge(a: &FieldNode, b: &FieldNode) -> FieldNode {
        FieldNode {
            present: a.present + b.present,
            explicit_null: a.explicit_null + b.explicit_null,
            types: TypeCounts::merge(&a.types, &b.types),
            children: merge_children(&a.children, &b.children),
            array_elem: merge_array_elem(&a.array_elem, &b.array_elem),
            array_empty_seen: a.array_empty_seen || b.array_empty_seen,
            array_partial_seen: a.array_partial_seen || b.array_partial_seen,
            int_only: a.int_only && b.int_only,
            neg_zero_seen: a.neg_zero_seen || b.neg_zero_seen,
        }
    }
}

impl TypeCounts {
    fn merge(a: &TypeCounts, b: &TypeCounts) -> TypeCounts {
        TypeCounts {
            null: a.null + b.null,
            bool: a.bool + b.bool,
            number: a.number + b.number,
            string: a.string + b.string,
            array: a.array + b.array,
            object: a.object + b.object,
        }
    }
}

/// `BTreeMap` union of two children maps: keys present in only one side
/// are cloned unchanged; keys present in both are recursively merged.
/// Deterministic (`BTreeMap` iteration order), associative, and
/// commutative given `FieldNode::merge` is.
fn merge_children(
    a: &BTreeMap<String, FieldNode>,
    b: &BTreeMap<String, FieldNode>,
) -> BTreeMap<String, FieldNode> {
    let mut out = a.clone();
    for (k, v) in b {
        out.entry(k.clone())
            .and_modify(|existing| *existing = FieldNode::merge(existing, v))
            .or_insert_with(|| v.clone());
    }
    out
}

/// `Option<Box<FieldNode>>` union: `None` is the neutral element, `Some`
/// merges recursively.
fn merge_array_elem(
    a: &Option<Box<FieldNode>>,
    b: &Option<Box<FieldNode>>,
) -> Option<Box<FieldNode>> {
    match (a, b) {
        (None, None) => None,
        (Some(x), None) => Some(x.clone()),
        (None, Some(y)) => Some(y.clone()),
        (Some(x), Some(y)) => Some(Box::new(FieldNode::merge(x, y))),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::Profile;

    /// Fixed key pool `synth_json` draws object field names from — kept
    /// small and shared across recursion so proptest exercises repeated
    /// key overlap (and thus `BTreeMap` merge) across generated profiles.
    const KEY_POOL: [&str; 6] = ["a", "b", "c", "opt", "d", "e"];

    /// Deterministic tiny-JSON generator: maps `seed` bytes onto a fixed
    /// grammar (root object with <=3 keys from `KEY_POOL`; values are
    /// scalar, array, or nested object, depth-capped so generation always
    /// terminates). Always emits syntactically valid JSON that
    /// `parse_bounded` accepts under `Limits::default()`.
    fn synth_json(seed: &[u8]) -> String {
        let mut idx = 0usize;
        synth_object(seed, &mut idx, 0)
    }

    fn next_byte(seed: &[u8], idx: &mut usize) -> u8 {
        if seed.is_empty() {
            return 0;
        }
        let b = seed[*idx % seed.len()];
        *idx = idx.wrapping_add(1);
        b
    }

    fn synth_value(seed: &[u8], idx: &mut usize, depth: u32) -> String {
        if depth >= 3 {
            return synth_scalar(seed, idx);
        }
        match next_byte(seed, idx) % 6 {
            0 => "null".to_string(),
            1 => synth_bool(seed, idx),
            2 => synth_number(seed, idx),
            3 => synth_string(seed, idx),
            4 => synth_array(seed, idx, depth),
            _ => synth_object(seed, idx, depth),
        }
    }

    fn synth_scalar(seed: &[u8], idx: &mut usize) -> String {
        match next_byte(seed, idx) % 4 {
            0 => "null".to_string(),
            1 => synth_number(seed, idx),
            2 => synth_string(seed, idx),
            _ => synth_bool(seed, idx),
        }
    }

    fn synth_bool(seed: &[u8], idx: &mut usize) -> String {
        if next_byte(seed, idx) % 2 == 0 {
            "true".to_string()
        } else {
            "false".to_string()
        }
    }

    fn synth_number(seed: &[u8], idx: &mut usize) -> String {
        let n = next_byte(seed, idx) as i32 - 128;
        if next_byte(seed, idx) % 3 == 0 {
            format!("{n}.{}", next_byte(seed, idx) % 10)
        } else {
            n.to_string()
        }
    }

    fn synth_string(seed: &[u8], idx: &mut usize) -> String {
        let len = (next_byte(seed, idx) % 4) as usize;
        let mut s = String::from("s");
        for _ in 0..len {
            s.push((b'a' + (next_byte(seed, idx) % 26)) as char);
        }
        format!("\"{s}\"")
    }

    fn synth_array(seed: &[u8], idx: &mut usize, depth: u32) -> String {
        let n = (next_byte(seed, idx) % 3) as usize;
        let items: Vec<String> = (0..n).map(|_| synth_value(seed, idx, depth + 1)).collect();
        format!("[{}]", items.join(","))
    }

    fn synth_object(seed: &[u8], idx: &mut usize, depth: u32) -> String {
        let n = (next_byte(seed, idx) % 4) as usize; // 0..=3 keys
        let mut pool: Vec<&str> = KEY_POOL.to_vec();
        let mut fields: Vec<String> = Vec::new();
        for _ in 0..n {
            if pool.is_empty() {
                break;
            }
            let i = (next_byte(seed, idx) as usize) % pool.len();
            let key = pool.remove(i);
            let value = synth_value(seed, idx, depth + 1);
            fields.push(format!("\"{key}\":{value}"));
        }
        format!("{{{}}}", fields.join(","))
    }

    fn arb_profile() -> impl Strategy<Value = Profile> {
        proptest::collection::vec(any::<u8>(), 0..64).prop_filter_map(
            "valid json profiles",
            |seed| {
                let payload = synth_json(&seed);
                let node =
                    deblob_fingerprint::parse_bounded(payload.as_bytes(), &Default::default())
                        .ok()?;
                Some(Profile::from_shape(&deblob_fingerprint::shape_of(&node)))
            },
        )
    }

    fn profile_of(bytes: &[u8]) -> Profile {
        let node = deblob_fingerprint::parse_bounded(bytes, &Default::default()).unwrap();
        Profile::from_node(&node)
    }

    fn raw_fp(bytes: &[u8]) -> [u8; 32] {
        let node = deblob_fingerprint::parse_bounded(bytes, &Default::default()).unwrap();
        deblob_fingerprint::fingerprint(&deblob_fingerprint::shape_of(&node))
    }

    proptest! {
        #[test] fn merge_is_associative(a in arb_profile(), b in arb_profile(), c in arb_profile()) {
            prop_assert_eq!(Profile::merge(&Profile::merge(&a, &b), &c),
                            Profile::merge(&a, &Profile::merge(&b, &c)));
        }
        #[test] fn merge_is_commutative(a in arb_profile(), b in arb_profile()) {
            prop_assert_eq!(Profile::merge(&a, &b), Profile::merge(&b, &a));
        }
        #[test] fn identity_is_neutral(a in arb_profile()) {
            prop_assert_eq!(Profile::merge(&a, &Profile::identity()), a.clone());
            prop_assert_eq!(Profile::merge(&Profile::identity(), &a), a);
        }
    }

    #[test]
    fn optional_field_variants_share_generalized_fingerprint() {
        let p1 = profile_of(br#"{"a":1}"#); // helper: parse->shape->profile
        let p2 = profile_of(br#"{"a":1,"opt":"x"}"#);
        let merged12 = Profile::merge(&p1, &p2);
        let merged21 = Profile::merge(&p2, &p1);
        assert_eq!(
            merged12.generalized_fingerprint(),
            merged21.generalized_fingerprint()
        );
        // and the raw shapes differ:
        assert_ne!(raw_fp(br#"{"a":1}"#), raw_fp(br#"{"a":1,"opt":"x"}"#));
    }

    #[test]
    fn generalized_fp_differs_from_raw_shape_fp() {
        let p = profile_of(br#"{"a":1}"#);
        let generalized = p.generalized_fingerprint();
        let raw = raw_fp(br#"{"a":1}"#);
        assert_ne!(generalized, raw);
    }
}
