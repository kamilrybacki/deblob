//! Mergeable structural `Profile`: per-field presence, nullability, and
//! type-union statistics accumulated across many observed JSON documents.
//! Building block for schema inference (cold lane / promotion, later
//! tasks). Spec §4/§6.

use std::collections::BTreeMap;

use deblob_fingerprint::{Emptiness, Node, Shape};
use sha2::{Digest, Sha256};

/// Identifies the generalized-fingerprint scheme embedded in every
/// `generalized_fingerprint` preimage. Deliberately distinct from
/// `deblob-fingerprint`'s `CANONICALIZER` — a candidate identity is never
/// the same digest as a raw shape fingerprint (§4).
const GENERALIZER: &str = "deblob-monoid-v1";

/// Per-type observation counts at a single field position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TypeCounts {
    pub null: u64,
    pub bool: u64,
    pub number: u64,
    pub string: u64,
    pub array: u64,
    pub object: u64,
}

/// Accumulated statistics for one field position (or the document root),
/// mergeable across observations. Never mutated in place — every producer
/// in this crate returns a new value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldNode {
    /// Number of observations where this field was present (including
    /// explicit `null`).
    pub present: u64,
    /// Number of observations where this field was present and explicitly
    /// `null`.
    pub explicit_null: u64,
    pub types: TypeCounts,
    /// Nested field statistics, keyed by object field name.
    pub children: BTreeMap<String, FieldNode>,
    /// Merged statistics for this field's array elements, if any
    /// observation held an array here.
    pub array_elem: Option<Box<FieldNode>>,
    /// Whether an empty array was ever observed here.
    pub array_empty_seen: bool,
    /// Whether a truncated (bound-limited) array was ever observed here.
    pub array_partial_seen: bool,
    /// Whether at least one integer-text number (no `.`/`e`/`E`) was ever
    /// observed here.
    pub int_only: bool,
    /// Whether a negative-zero number text was ever observed here.
    pub neg_zero_seen: bool,
}

impl FieldNode {
    /// The neutral element: zero observations, no type/child/array
    /// information. `FieldNode::merge(a, &FieldNode::identity()) == a`.
    pub(crate) fn identity() -> Self {
        Self {
            present: 0,
            explicit_null: 0,
            types: TypeCounts::default(),
            children: BTreeMap::new(),
            array_elem: None,
            array_empty_seen: false,
            array_partial_seen: false,
            int_only: false,
            neg_zero_seen: false,
        }
    }

    fn from_node(node: &Node) -> Self {
        let mut out = Self::identity();
        out.present = 1;
        match node {
            Node::Null => {
                out.explicit_null = 1;
                out.types.null = 1;
            }
            Node::Bool(_) => out.types.bool = 1,
            Node::Number(text) => {
                out.types.number = 1;
                out.int_only = is_int_text(text);
                out.neg_zero_seen = is_neg_zero(text);
            }
            Node::String(_) => out.types.string = 1,
            Node::Array(items, truncated) => {
                out.types.array = 1;
                if *truncated {
                    out.array_partial_seen = true;
                } else if items.is_empty() {
                    out.array_empty_seen = true;
                }
                out.array_elem = merge_all(items.iter().map(FieldNode::from_node)).map(Box::new);
            }
            Node::Object(fields) => {
                out.types.object = 1;
                out.children = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), FieldNode::from_node(v)))
                    .collect();
            }
        }
        out
    }

    fn from_shape(shape: &Shape) -> Self {
        let mut out = Self::identity();
        out.present = 1;
        match shape {
            Shape::Null => {
                out.explicit_null = 1;
                out.types.null = 1;
            }
            Shape::Bool => out.types.bool = 1,
            // `Shape` erases the number's source text, so `int_only` and
            // `neg_zero_seen` can't be recovered here — they default to
            // `false` (the merge-identity value), same as any other field
            // never observed to hold that property. `Profile::from_node`
            // is the primary constructor and does not lose this info.
            Shape::Number => out.types.number = 1,
            Shape::String => out.types.string = 1,
            Shape::Array(set, emptiness) => {
                out.types.array = 1;
                match emptiness {
                    Emptiness::Empty => out.array_empty_seen = true,
                    Emptiness::Partial => out.array_partial_seen = true,
                    Emptiness::NonEmpty => {}
                }
                out.array_elem = merge_all(set.iter().map(FieldNode::from_shape)).map(Box::new);
            }
            Shape::Object(fields) => {
                out.types.object = 1;
                out.children = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), FieldNode::from_shape(v)))
                    .collect();
            }
        }
        out
    }
}

/// A mergeable structural profile of `count` observed documents, rooted at
/// `root`. Forms a commutative monoid under [`Profile::merge`] with
/// [`Profile::identity`] as neutral element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub count: u64,
    pub root: FieldNode,
}

impl Profile {
    /// Build a profile from a single parsed [`Node`] observation
    /// (`count == 1`). Primary constructor: `Node` retains number source
    /// text, so `int_only`/`neg_zero_seen` are populated exactly.
    pub fn from_node(node: &Node) -> Self {
        Self {
            count: 1,
            root: FieldNode::from_node(node),
        }
    }

    /// Build a profile from a single [`Shape`] observation (`count == 1`).
    /// `Shape` has already erased number source text, so `int_only` and
    /// `neg_zero_seen` default to `false` on every field touched by this
    /// constructor; all other statistics (presence, type unions, children,
    /// array emptiness) are exact.
    pub fn from_shape(shape: &Shape) -> Self {
        Self {
            count: 1,
            root: FieldNode::from_shape(shape),
        }
    }

    /// Candidate identity over the *generalized* profile: the field set
    /// with optionality (`present < count` at each level) and type
    /// unions, serialized deterministically and hashed. Deliberately NOT
    /// the raw shape fingerprint — two profiles built from different
    /// concrete shapes (e.g. with vs. without an optional field) converge
    /// to the same generalized fingerprint once merged (§4).
    pub fn generalized_fingerprint(&self) -> [u8; 32] {
        let mut body = Vec::new();
        body.extend_from_slice(br#"{"gen":""#);
        body.extend_from_slice(GENERALIZER.as_bytes());
        body.extend_from_slice(br#"","fields":"#);
        write_generalized_field(&self.root, self.count, &mut body);
        body.push(b'}');

        let mut hasher = Sha256::new();
        hasher.update(GENERALIZER.as_bytes());
        hasher.update([0u8]);
        hasher.update(&body);
        hasher.finalize().into()
    }
}

/// Folds a sequence of `FieldNode`s (e.g. every observed shape of one
/// array's elements) into a single merged `FieldNode` via the same
/// associative/commutative merge used across profiles. `None` if the
/// sequence is empty (no element information to merge).
fn merge_all(nodes: impl Iterator<Item = FieldNode>) -> Option<FieldNode> {
    nodes.fold(None, |acc, next| match acc {
        None => Some(next),
        Some(acc) => Some(FieldNode::merge(&acc, &next)),
    })
}

/// `true` iff `text` (a JSON number's exact source text) has no
/// fractional or exponent part — i.e. it is integer-only.
fn is_int_text(text: &str) -> bool {
    !text.contains(['.', 'e', 'E'])
}

/// `true` iff `text` is a negative-zero number text (`-0`, `-0.0`,
/// `-0e3`, ...): starts with `-` and its numeric value is exactly zero.
fn is_neg_zero(text: &str) -> bool {
    text.starts_with('-') && text.parse::<f64>().map(|v| v == 0.0).unwrap_or(false)
}

/// Writes one field's generalized (type-union + optionality) view into
/// `out`. `denom` is the presence count this field's own `present` is
/// compared against to decide optionality: `self.count` at the root, and
/// the parent's matching type count (`types.object`/`types.array`) for
/// children/array elements, since those can only appear in the subset of
/// observations where the parent actually held that type.
fn write_generalized_field(field: &FieldNode, denom: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(br#"{"optional":"#);
    out.extend_from_slice(if field.present < denom {
        b"true"
    } else {
        b"false"
    });
    out.extend_from_slice(br#","types":["#);
    let mut first = true;
    for (name, count) in [
        ("array", field.types.array),
        ("bool", field.types.bool),
        ("null", field.types.null),
        ("number", field.types.number),
        ("object", field.types.object),
        ("string", field.types.string),
    ] {
        if count > 0 {
            if !first {
                out.push(b',');
            }
            first = false;
            out.push(b'"');
            out.extend_from_slice(name.as_bytes());
            out.push(b'"');
        }
    }
    out.push(b']');
    if !field.children.is_empty() {
        out.extend_from_slice(br#","children":{"#);
        let mut cfirst = true;
        for (k, v) in &field.children {
            if !cfirst {
                out.push(b',');
            }
            cfirst = false;
            write_json_key(k, out);
            out.push(b':');
            write_generalized_field(v, field.types.object, out);
        }
        out.push(b'}');
    }
    if let Some(elem) = &field.array_elem {
        out.extend_from_slice(br#","elem":"#);
        write_generalized_field(elem, field.types.array, out);
    }
    out.push(b'}');
}

/// Minimal JSON string escaping for object keys, matching
/// `deblob-fingerprint`'s canonicalizer: escapes `"`, `\`, and control
/// characters as `\uXXXX`; every other Unicode scalar is emitted as raw
/// UTF-8 bytes, unmodified and unnormalized.
fn write_json_key(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}
