//! Versioned canonical serialization and hashing of a [`crate::Shape`].
//! `canonical_bytes` is a deterministic, hand-rolled compact JSON encoding
//! (never delegated to `serde_json`, to keep full control over key
//! ordering and the exact preimage bytes); `fingerprint` hashes that
//! encoding chained after a canonicalizer-version tag so that a future
//! bump to the encoding is guaranteed to change every digest. Spec §4.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::shape::{Emptiness, Shape};

/// Identifies the canonicalization scheme embedded in every fingerprint
/// preimage. Bumping this string is a breaking change to all fingerprints.
pub const CANONICALIZER: &str = "deblob-canon-v1";

/// Serialize `shape` into a deterministic compact-JSON byte encoding. Object
/// fields are emitted in `BTreeMap` (code-point) order; array element
/// shapes are emitted in `BTreeSet` order; emptiness is spelled out as one
/// of `"empty"`, `"nonempty"`, `"partial"`.
pub fn canonical_bytes(shape: &Shape) -> Vec<u8> {
    let mut out = Vec::new();
    write_shape(shape, &mut out);
    out
}

/// `Sha256` digest over the canonicalizer version tag chained with
/// `canonical_bytes(shape)`.
pub fn fingerprint(shape: &Shape) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(CANONICALIZER.as_bytes());
    hasher.update([0u8]);
    hasher.update(canonical_bytes(shape));
    hasher.finalize().into()
}

fn write_shape(shape: &Shape, out: &mut Vec<u8>) {
    match shape {
        Shape::Null => out.extend_from_slice(br#"{"t":"null"}"#),
        Shape::Bool => out.extend_from_slice(br#"{"t":"bool"}"#),
        Shape::Number => out.extend_from_slice(br#"{"t":"num"}"#),
        Shape::String => out.extend_from_slice(br#"{"t":"str"}"#),
        Shape::Array(set, emptiness) => {
            out.extend_from_slice(br#"{"t":"arr","e":""#);
            out.extend_from_slice(emptiness_str(*emptiness).as_bytes());
            out.extend_from_slice(br#"","of":["#);
            write_shape_set(set, out);
            out.extend_from_slice(b"]}");
        }
        Shape::Object(fields) => {
            out.extend_from_slice(br#"{"t":"obj","f":{"#);
            let mut first = true;
            for (k, v) in fields {
                if !first {
                    out.push(b',');
                }
                first = false;
                write_json_string(k, out);
                out.push(b':');
                write_shape(v, out);
            }
            out.extend_from_slice(b"}}");
        }
    }
}

fn write_shape_set(set: &BTreeSet<Shape>, out: &mut Vec<u8>) {
    let mut first = true;
    for s in set {
        if !first {
            out.push(b',');
        }
        first = false;
        write_shape(s, out);
    }
}

fn emptiness_str(e: Emptiness) -> &'static str {
    match e {
        Emptiness::Empty => "empty",
        Emptiness::NonEmpty => "nonempty",
        Emptiness::Partial => "partial",
    }
}

/// Minimal JSON string escaping for object keys: escapes `"`, `\\`, and
/// control characters as `\uXXXX`; every other Unicode scalar (including
/// non-ASCII) is emitted as its raw UTF-8 bytes, unmodified and
/// unnormalized, so distinct code points always produce distinct output.
fn write_json_string(s: &str, out: &mut Vec<u8>) {
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
