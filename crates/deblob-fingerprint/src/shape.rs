//! Shape extraction: folds a parsed [`crate::Node`] tree into a `Shape` that
//! captures structure (types, object keys, array element shapes) while
//! discarding values. Two documents with the same shape must fingerprint
//! identically regardless of the concrete values they carry. Spec §4.

use std::collections::{BTreeMap, BTreeSet};

use crate::parse::Node;

/// The set of distinct element shapes observed inside an array. Kept sorted
/// (via `BTreeSet`'s `Ord` on `Shape`) so serialization is deterministic
/// without an explicit sort step.
pub type ShapeSet = BTreeSet<Shape>;

/// Whether an array was empty, had at least one inspected element, or was
/// truncated by `Limits::max_array_inspect` before every element could be
/// inspected. An empty array carries no element-type information, and a
/// truncated array cannot claim homogeneity from its inspected prefix, so
/// both are tracked distinctly from a fully-inspected non-empty array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Emptiness {
    Empty,
    NonEmpty,
    Partial,
}

/// Structural shape of a JSON value: type plus, for containers, the shape of
/// their contents. Values themselves (numbers, strings, booleans) are erased
/// — only their type contributes to the shape.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shape {
    Null,
    Bool,
    Number,
    String,
    Array(Box<ShapeSet>, Emptiness),
    Object(BTreeMap<String, Shape>),
}

/// Fold a parsed [`Node`] into its [`Shape`], erasing concrete values.
pub fn shape_of(node: &Node) -> Shape {
    match node {
        Node::Null => Shape::Null,
        Node::Bool(_) => Shape::Bool,
        Node::Number(_) => Shape::Number,
        Node::String(_) => Shape::String,
        Node::Array(items, truncated) => {
            let emptiness = if *truncated {
                Emptiness::Partial
            } else if items.is_empty() {
                Emptiness::Empty
            } else {
                Emptiness::NonEmpty
            };
            let set: ShapeSet = items.iter().map(shape_of).collect();
            Shape::Array(Box::new(set), emptiness)
        }
        Node::Object(fields) => {
            let map: BTreeMap<String, Shape> = fields
                .iter()
                .map(|(k, v)| (k.clone(), shape_of(v)))
                .collect();
            Shape::Object(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{canonical_bytes, fingerprint, parse_bounded, Limits};

    #[test]
    fn values_do_not_change_shape() {
        let a = shape_of(&parse_bounded(br#"{"a":1,"b":"x"}"#, &Limits::default()).unwrap());
        let b = shape_of(&parse_bounded(br#"{"a":99,"b":"y"}"#, &Limits::default()).unwrap());
        assert_eq!(a, b);
        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn key_order_does_not_change_fingerprint() {
        let a = shape_of(&parse_bounded(br#"{"a":1,"b":2}"#, &Limits::default()).unwrap());
        let b = shape_of(&parse_bounded(br#"{"b":2,"a":1}"#, &Limits::default()).unwrap());
        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn int_float_exp_are_one_number_shape() {
        for p in [br#"{"x":1}"#.as_slice(), br#"{"x":1.0}"#, br#"{"x":1e0}"#] {
            assert_eq!(
                shape_of(&parse_bounded(p, &Limits::default()).unwrap()),
                shape_of(&parse_bounded(br#"{"x":7.5}"#, &Limits::default()).unwrap())
            );
        }
    }

    #[test]
    fn empty_array_is_type_unknown_not_equal_to_typed() {
        let empty = shape_of(&parse_bounded(br#"{"a":[]}"#, &Limits::default()).unwrap());
        let typed = shape_of(&parse_bounded(br#"{"a":[1]}"#, &Limits::default()).unwrap());
        assert_ne!(fingerprint(&empty), fingerprint(&typed)); // §4 empty arrays type-unknown
    }

    #[test]
    fn truncated_array_marks_partial() {
        let l = Limits {
            max_array_inspect: 1,
            ..Default::default()
        };
        let s = shape_of(&parse_bounded(br#"{"a":[1,2,3]}"#, &l).unwrap());
        let canon = String::from_utf8(canonical_bytes(&s)).unwrap();
        assert!(canon.contains("partial")); // no homogeneity claim from prefix (§4)
    }

    #[test]
    fn preimage_includes_canonicalizer_version() {
        // changing version string must change digest: guard test pinned to a golden value
        let s = shape_of(&parse_bounded(br#"{"a":1}"#, &Limits::default()).unwrap());
        let hex = data_encoding::HEXLOWER.encode(&fingerprint(&s));
        insta::assert_snapshot!(hex); // golden: canonicalizer version bump must break this test
    }

    #[test]
    fn unicode_keys_ordered_by_code_point_not_normalized() {
        // U+00E9 (é) vs U+0065 U+0301 (é decomposed) are DISTINCT keys (§4: no NFC)
        let a = parse_bounded(
            "{\"\u{00E9}\":1,\"\u{0065}\u{0301}\":2}".as_bytes(),
            &Limits::default(),
        );
        assert!(a.is_ok()); // distinct keys, not duplicates
    }
}
