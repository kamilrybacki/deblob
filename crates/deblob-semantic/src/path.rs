//! Typed-segment field-path enumeration and validation against a schema's
//! structural canonical form (P2-D Task 4; monoid-grammar dispatch added by
//! the Task 8 follow-up).
//!
//! Keeps the semantic metadata and the structural `sch_`/generalized
//! identity in one coordinate system: an annotation (`FieldEntry::path`)
//! may only reference a field path that actually exists in the schema's
//! structure, as decided by walking the SAME canonical JSON that identity
//! is derived from — never a re-derivation of canonicalization rules.
//!
//! A `SchemaRecord::canonical` string is written in one of TWO grammars,
//! selected by `SchemaRecord::canonicalizer`:
//!
//! - `"deblob-canon-v1"` (`deblob_fingerprint::canon::CANONICALIZER`,
//!   written by `deblob_fingerprint::canon::canonical_bytes`): a raw shape
//!   tree, `{"t":"obj","f":{<key>:<node>}}` / `{"t":"arr","of":[<node>...]}`
//!   / `{"t":"null|bool|num|str"}`. Walked by [`walk_shape`].
//! - `"deblob-monoid-v1"` (`deblob_monoid::profile::GENERALIZER`, written by
//!   `deblob_monoid::profile::Profile::generalized_canonical_json`, the
//!   form every `Promoter::promote`d/PROMOTED `SchemaRecord` actually
//!   carries): a generalized field-statistics tree rooted at the bare field
//!   body itself (NOT wrapped in the `{"gen":...,"fields":...}` framing —
//!   that framing exists only in `generalized_fingerprint`'s hash preimage,
//!   never in the persisted `canonical` string), each node shaped
//!   `{"optional":bool,"types":[...],"children":{<key>:<node>...},"elem":<node>}`
//!   with `"children"`/`"elem"` present only when actually populated.
//!   Walked by [`walk_monoid_field`].
//!
//! [`canonical_field_paths`] (grammar hard-coded to `deblob-canon-v1`, kept
//! for existing callers/tests) and [`canonical_field_paths_for`] (dispatches
//! on an explicit `canonicalizer` string — use this for any canonical form
//! that might have come from a promoted schema) both enumerate the SAME
//! typed [`PathSegment`] path set, module grammar: object/children keys
//! become one `Key(name)` segment each; an array/`elem` contributes one
//! shared `Wildcard` segment. `types`/`optional` never affect the path set
//! — only presence of `children`/`elem` does.
//!
//! Paths are always typed segments (`PathSegment::Key`/`Wildcard`), never
//! dotted strings: a structural object key literally containing a `.`
//! (e.g. `"a.b"`) enumerates as exactly one `Key("a.b")` segment, matching
//! the same anti-ambiguity invariant `deblob_semantic::canon` already
//! upholds for the `sem_` digest preimage (see
//! `canon::encode_path`'s doc comment).
//!
//! Scope: enumeration + validation ONLY. No storage, API, signature, or
//! digest concerns here.

use std::collections::BTreeSet;

use deblob_core::semantic::{PathSegment, SemanticMetadata};

/// Mirrors `deblob_fingerprint::canon::CANONICALIZER`. Duplicated as a
/// plain string literal (rather than a runtime dependency on
/// `deblob-fingerprint`) to keep this crate's dependency graph unchanged —
/// see the crate's `Cargo.toml`, which only pulls `deblob-fingerprint` in as
/// a dev-dependency for tests.
const CANON_V1: &str = "deblob-canon-v1";

/// Mirrors `deblob_monoid::profile::GENERALIZER`. Duplicated as a plain
/// string literal for the same reason as [`CANON_V1`] — this crate takes no
/// runtime dependency on `deblob-monoid`.
const MONOID_V1: &str = "deblob-monoid-v1";

/// Errors from enumerating a structural canonical form's field paths, or
/// from validating [`SemanticMetadata`] against an already-enumerated set.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// `structural_canonical` was not even well-formed JSON.
    #[error("structural canonical form is not valid JSON: {0}")]
    InvalidCanonicalJson(String),
    /// `structural_canonical` parsed as JSON but did not match the
    /// `deblob-canon-v1` shape grammar (missing/unrecognized `"t"`
    /// discriminator, or a `"t"` variant missing its expected payload).
    #[error("structural canonical form does not match the deblob-canon-v1 shape grammar")]
    MalformedShape,
    /// `structural_canonical` parsed as JSON but did not match the
    /// `deblob-monoid-v1` generalized-field grammar (a node that isn't a
    /// JSON object, or a `"children"`/`"elem"` value of the wrong type).
    #[error("structural canonical form does not match the deblob-monoid-v1 field grammar")]
    MalformedMonoidField,
    /// [`canonical_field_paths_for`] was called with a `canonicalizer`
    /// string that isn't one of the two grammars this crate understands.
    /// Named and reported explicitly — never silently treated as either
    /// known grammar.
    #[error("unknown canonicalizer, cannot enumerate field paths: {0:?}")]
    UnknownCanonicalizer(String),
    /// A `FieldEntry.path` in the metadata is not a member of the
    /// enumerated canonical field-path set.
    #[error("annotated path not present in the structural canonical form: {0:?}")]
    UnknownPath(Vec<PathSegment>),
}

/// Enumerate every field path present in `structural_canonical` (a
/// `deblob-canon-v1` shape JSON string, e.g. `SchemaRecord::canonical`) as
/// typed [`PathSegment`] sequences.
///
/// An object contributes one `Key(name)` segment per field (its own path,
/// plus everything nested under it); an array contributes one `Wildcard`
/// segment shared by every element shape it was observed to hold (a set,
/// since `Shape::Array` may carry more than one distinct element shape —
/// they all extend the SAME `Wildcard` path, not one each). The document
/// root itself is never a path (only sub-paths reached through at least one
/// key/wildcard are field paths), so a top-level scalar or empty object
/// yields an empty set.
///
/// Returned as a `BTreeSet` for deterministic ordering; enumeration only
/// depends on the parsed structure, never on the input JSON's own key
/// order (object fields are walked from a parsed `serde_json::Map`, whose
/// default backing store is a `BTreeMap`).
pub fn canonical_field_paths(
    structural_canonical: &str,
) -> Result<BTreeSet<Vec<PathSegment>>, PathError> {
    let value: serde_json::Value = serde_json::from_str(structural_canonical)
        .map_err(|e| PathError::InvalidCanonicalJson(e.to_string()))?;
    let mut paths = BTreeSet::new();
    let mut current = Vec::new();
    walk_shape(&value, &mut current, &mut paths)?;
    Ok(paths)
}

/// Enumerate every field path present in `structural_canonical`, dispatching
/// on `canonicalizer` (a `SchemaRecord::canonicalizer` value) to pick the
/// matching grammar:
///
/// - `"deblob-canon-v1"` — the plain shape grammar, identical to
///   [`canonical_field_paths`].
/// - `"deblob-monoid-v1"` — the generalized field-statistics grammar every
///   `Promoter::promote`d `SchemaRecord` actually carries (see this module's
///   doc comment). Enumerates the same typed [`PathSegment`] path set:
///   `children` keys become `Key` segments, `elem` becomes one shared
///   `Wildcard` segment; `types`/`optional` never affect the result.
/// - anything else — [`PathError::UnknownCanonicalizer`], never a silent
///   accept of an unrecognized grammar.
pub fn canonical_field_paths_for(
    canonicalizer: &str,
    structural_canonical: &str,
) -> Result<BTreeSet<Vec<PathSegment>>, PathError> {
    match canonicalizer {
        CANON_V1 => canonical_field_paths(structural_canonical),
        MONOID_V1 => {
            let value: serde_json::Value = serde_json::from_str(structural_canonical)
                .map_err(|e| PathError::InvalidCanonicalJson(e.to_string()))?;
            let mut paths = BTreeSet::new();
            let mut current = Vec::new();
            walk_monoid_field(&value, &mut current, &mut paths)?;
            Ok(paths)
        }
        other => Err(PathError::UnknownCanonicalizer(other.to_string())),
    }
}

/// Walks one `deblob-canon-v1` shape JSON node, appending every sub-path
/// reached through at least one key/wildcard segment to `out`. `current` is
/// the path accumulated so far (mutated in place and restored before
/// returning, so callers can keep reusing the same buffer across siblings).
fn walk_shape(
    value: &serde_json::Value,
    current: &mut Vec<PathSegment>,
    out: &mut BTreeSet<Vec<PathSegment>>,
) -> Result<(), PathError> {
    let t = value
        .get("t")
        .and_then(serde_json::Value::as_str)
        .ok_or(PathError::MalformedShape)?;
    match t {
        "null" | "bool" | "num" | "str" => Ok(()), // leaf: nothing further to recurse into
        "obj" => {
            let fields = value
                .get("f")
                .and_then(serde_json::Value::as_object)
                .ok_or(PathError::MalformedShape)?;
            for (k, v) in fields {
                current.push(PathSegment::Key(k.clone()));
                out.insert(current.clone());
                walk_shape(v, current, out)?;
                current.pop();
            }
            Ok(())
        }
        "arr" => {
            let elements = value
                .get("of")
                .and_then(serde_json::Value::as_array)
                .ok_or(PathError::MalformedShape)?;
            // One Wildcard segment shared by every observed element shape
            // (a set, since a heterogeneous array carries more than one
            // distinct element shape) — not one Wildcard per shape.
            current.push(PathSegment::Wildcard);
            out.insert(current.clone());
            for element in elements {
                walk_shape(element, current, out)?;
            }
            current.pop();
            Ok(())
        }
        _ => Err(PathError::MalformedShape),
    }
}

/// Walks one `deblob-monoid-v1` generalized field-body JSON node (see this
/// module's doc comment for the exact grammar), appending every sub-path
/// reached through at least one key/wildcard segment to `out`. `current` is
/// the path accumulated so far (mutated in place and restored before
/// returning), matching [`walk_shape`]'s calling convention.
///
/// A field node with BOTH `"children"` and `"elem"` (a field observed as an
/// object in some samples and an array in others, since `types` is a set,
/// not a single tag) contributes paths from both — a `Key` per child AND a
/// shared `Wildcard` into `elem` — since path enumeration only cares which
/// shapes were ever reachable, never which single type a field "is".
fn walk_monoid_field(
    value: &serde_json::Value,
    current: &mut Vec<PathSegment>,
    out: &mut BTreeSet<Vec<PathSegment>>,
) -> Result<(), PathError> {
    let obj = value.as_object().ok_or(PathError::MalformedMonoidField)?;

    if let Some(children) = obj.get("children") {
        let children = children
            .as_object()
            .ok_or(PathError::MalformedMonoidField)?;
        for (k, v) in children {
            current.push(PathSegment::Key(k.clone()));
            out.insert(current.clone());
            walk_monoid_field(v, current, out)?;
            current.pop();
        }
    }

    if let Some(elem) = obj.get("elem") {
        current.push(PathSegment::Wildcard);
        out.insert(current.clone());
        walk_monoid_field(elem, current, out)?;
        current.pop();
    }

    Ok(())
}

/// Validate that every `FieldEntry.path` in `metadata` is a member of
/// `valid_paths` (as produced by [`canonical_field_paths`]). `event_type`
/// is schema-level, not a field path, and is therefore never checked here.
///
/// Fails fast on the first path not found, reporting it via
/// [`PathError::UnknownPath`].
pub fn validate_paths(
    metadata: &SemanticMetadata,
    valid_paths: &BTreeSet<Vec<PathSegment>>,
) -> Result<(), PathError> {
    for field in &metadata.fields {
        if !valid_paths.contains(&field.path) {
            return Err(PathError::UnknownPath(field.path.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::semantic::{FieldEntry, FieldSemantics, PathSegment};
    use deblob_fingerprint::{canonical_bytes, parse_bounded, shape_of, Limits};

    fn key(s: &str) -> PathSegment {
        PathSegment::Key(s.to_string())
    }

    fn empty_semantics() -> FieldSemantics {
        FieldSemantics {
            canonical_field_id: None,
            identifier_namespace: None,
            unit: None,
            numeric_scale: None,
            temporal: None,
            enum_semantics: None,
        }
    }

    fn field(path: Vec<PathSegment>) -> FieldEntry {
        FieldEntry {
            path,
            semantics: empty_semantics(),
        }
    }

    fn metadata(fields: Vec<FieldEntry>) -> SemanticMetadata {
        SemanticMetadata {
            event_type: None,
            fields,
        }
    }

    /// Real `deblob-canon-v1` shape JSON produced by
    /// `deblob-fingerprint`'s actual parse -> shape -> canonicalize
    /// pipeline (never hand-typed), so the enumerator is proven against
    /// the real wire format, not an assumption about it.
    fn real_canonical(json_doc: &[u8]) -> String {
        let node = parse_bounded(json_doc, &Limits::default()).unwrap();
        let shape = shape_of(&node);
        String::from_utf8(canonical_bytes(&shape)).unwrap()
    }

    // -- enumeration ---------------------------------------------------

    #[test]
    fn nested_object_and_array_paths_enumerate_correctly() {
        // {"a":{"b":1},"c":[1,2]}
        let canonical = real_canonical(br#"{"a":{"b":1},"c":[1,2]}"#);
        let paths = canonical_field_paths(&canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![key("a")],
            vec![key("a"), key("b")],
            vec![key("c")],
            vec![key("c"), PathSegment::Wildcard],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn dotted_key_enumerates_as_one_key_segment_not_two() {
        // A structural field literally named "a.b" must enumerate as ONE
        // Key("a.b") segment, never split into [Key("a"), Key("b")].
        let canonical = real_canonical(br#"{"a.b":1}"#);
        let paths = canonical_field_paths(&canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [vec![key("a.b")]].into_iter().collect();
        assert_eq!(paths, expected);
        assert!(!paths.contains(&vec![key("a"), key("b")]));
    }

    #[test]
    fn top_level_scalar_has_no_field_paths() {
        let canonical = real_canonical(b"42");
        let paths = canonical_field_paths(&canonical).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn empty_object_has_no_field_paths() {
        let canonical = real_canonical(b"{}");
        let paths = canonical_field_paths(&canonical).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn top_level_array_enumerates_a_bare_wildcard_path() {
        let canonical = real_canonical(br#"[{"x":1}]"#);
        let paths = canonical_field_paths(&canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![PathSegment::Wildcard],
            vec![PathSegment::Wildcard, key("x")],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn enumeration_is_independent_of_object_key_input_order() {
        let forward = real_canonical(br#"{"a":1,"b":2,"c":3}"#);
        let reverse = real_canonical(br#"{"c":3,"b":2,"a":1}"#);
        assert_eq!(
            canonical_field_paths(&forward).unwrap(),
            canonical_field_paths(&reverse).unwrap()
        );
    }

    #[test]
    fn array_with_multiple_distinct_element_shapes_shares_one_wildcard_path() {
        // Heterogeneous array: element shapes are a *set* under one
        // Wildcard segment, not one Wildcard per distinct shape.
        let canonical = real_canonical(br#"{"a":[1,"x",{"y":true}]}"#);
        let paths = canonical_field_paths(&canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![key("a")],
            vec![key("a"), PathSegment::Wildcard],
            vec![key("a"), PathSegment::Wildcard, key("y")],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn invalid_json_reports_invalid_canonical_json_error() {
        let err = canonical_field_paths("not json at all").unwrap_err();
        assert!(matches!(err, PathError::InvalidCanonicalJson(_)));
    }

    #[test]
    fn json_missing_shape_grammar_reports_malformed_shape_error() {
        let err = canonical_field_paths(r#"{"foo":"bar"}"#).unwrap_err();
        assert_eq!(err, PathError::MalformedShape);
    }

    // -- validation ------------------------------------------------------

    #[test]
    fn metadata_annotating_an_existing_path_validates() {
        let canonical = real_canonical(br#"{"a":{"b":1}}"#);
        let valid = canonical_field_paths(&canonical).unwrap();
        let meta = metadata(vec![field(vec![key("a"), key("b")])]);
        assert!(validate_paths(&meta, &valid).is_ok());
    }

    #[test]
    fn metadata_annotating_a_missing_path_returns_unknown_path_error() {
        let canonical = real_canonical(br#"{"a":{"b":1}}"#);
        let valid = canonical_field_paths(&canonical).unwrap();
        let missing_path = vec![key("does"), key("not"), key("exist")];
        let meta = metadata(vec![field(missing_path.clone())]);

        let err = validate_paths(&meta, &valid).unwrap_err();
        assert_eq!(err, PathError::UnknownPath(missing_path));
    }

    #[test]
    fn metadata_annotating_a_wildcard_path_validates_against_array_structure() {
        let canonical = real_canonical(br#"{"items":[{"x":1}]}"#);
        let valid = canonical_field_paths(&canonical).unwrap();
        let meta = metadata(vec![field(vec![
            key("items"),
            PathSegment::Wildcard,
            key("x"),
        ])]);
        assert!(validate_paths(&meta, &valid).is_ok());
    }

    #[test]
    fn empty_metadata_fields_always_validates() {
        let canonical = real_canonical(br#"{"a":1}"#);
        let valid = canonical_field_paths(&canonical).unwrap();
        let meta = metadata(vec![]);
        assert!(validate_paths(&meta, &valid).is_ok());
    }

    #[test]
    fn event_type_is_not_path_checked() {
        // A metadata with only an event_type and no field entries validates
        // regardless of the structural canonical form's contents.
        let canonical = real_canonical(b"{}");
        let valid = canonical_field_paths(&canonical).unwrap();
        let meta = SemanticMetadata {
            event_type: Some(deblob_core::semantic::CanonicalEventTypeId::new(
                "user.created",
            )),
            fields: vec![],
        };
        assert!(validate_paths(&meta, &valid).is_ok());
    }

    // -- deblob-monoid-v1 dispatch / enumeration -------------------------

    /// Real `deblob-monoid-v1` generalized-field JSON, built via
    /// `deblob-monoid`'s ACTUAL `Node -> Profile -> generalized_canonical_json`
    /// pipeline (never hand-typed) from one or more parsed JSON documents
    /// merged into a single profile — exactly the path `Promoter::promote`
    /// takes (`crates/deblob/src/policy.rs`), so the enumerator here is
    /// proven against the real wire format, not an assumption about it.
    fn real_monoid_canonical(json_docs: &[&[u8]]) -> String {
        use deblob_fingerprint::{parse_bounded, Limits};
        use deblob_monoid::Profile;

        let profile = json_docs
            .iter()
            .map(|doc| {
                let node = parse_bounded(doc, &Limits::default()).unwrap();
                Profile::from_node(&node)
            })
            .reduce(|a, b| Profile::merge(&a, &b))
            .expect("at least one document");
        profile.generalized_canonical_json()
    }

    #[test]
    fn monoid_nested_object_and_array_paths_enumerate_correctly() {
        // {"a":{"b":1},"c":[1,2]}
        let canonical = real_monoid_canonical(&[br#"{"a":{"b":1},"c":[1,2]}"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![key("a")],
            vec![key("a"), key("b")],
            vec![key("c")],
            vec![key("c"), PathSegment::Wildcard],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn monoid_dotted_key_enumerates_as_one_key_segment_not_two() {
        let canonical = real_monoid_canonical(&[br#"{"a.b":1}"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [vec![key("a.b")]].into_iter().collect();
        assert_eq!(paths, expected);
        assert!(!paths.contains(&vec![key("a"), key("b")]));
    }

    #[test]
    fn monoid_top_level_scalar_has_no_field_paths() {
        let canonical = real_monoid_canonical(&[b"42"]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn monoid_empty_object_has_no_field_paths() {
        let canonical = real_monoid_canonical(&[b"{}"]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn monoid_top_level_array_enumerates_a_bare_wildcard_path() {
        let canonical = real_monoid_canonical(&[br#"[{"x":1}]"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![PathSegment::Wildcard],
            vec![PathSegment::Wildcard, key("x")],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn monoid_optional_field_from_two_merged_observations_still_enumerates() {
        // First observation has "b" absent, second has it present — merged
        // profile marks "b" optional, but it must still enumerate: path
        // enumeration only cares about `children`/`elem` presence, never
        // `optional`/`types`.
        let canonical = real_monoid_canonical(&[br#"{"a":1}"#, br#"{"a":1,"b":2}"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> =
            [vec![key("a")], vec![key("b")]].into_iter().collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn monoid_field_with_both_children_and_elem_enumerates_both() {
        // Field "x" observed as an object in one sample and an array of
        // objects in another — a real heterogeneous type union
        // (`types: ["array","object"]`). Both the object's `children` keys
        // and the array's `elem` (as a Wildcard) must enumerate.
        let canonical =
            real_monoid_canonical(&[br#"{"x":{"obj_field":1}}"#, br#"{"x":[{"arr_field":2}]}"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![key("x")],
            vec![key("x"), key("obj_field")],
            vec![key("x"), PathSegment::Wildcard],
            vec![key("x"), PathSegment::Wildcard, key("arr_field")],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn monoid_heterogeneous_array_elements_share_one_wildcard_path() {
        let canonical = real_monoid_canonical(&[br#"{"a":[1,"x",{"y":true}]}"#]);
        let paths = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();

        let expected: BTreeSet<Vec<PathSegment>> = [
            vec![key("a")],
            vec![key("a"), PathSegment::Wildcard],
            vec![key("a"), PathSegment::Wildcard, key("y")],
        ]
        .into_iter()
        .collect();
        assert_eq!(paths, expected);
    }

    #[test]
    fn monoid_invalid_json_reports_invalid_canonical_json_error() {
        let err = canonical_field_paths_for(MONOID_V1, "not json at all").unwrap_err();
        assert!(matches!(err, PathError::InvalidCanonicalJson(_)));
    }

    #[test]
    fn monoid_non_object_node_reports_malformed_monoid_field_error() {
        // A "children" map whose value isn't an object at all.
        let err = canonical_field_paths_for(MONOID_V1, r#"{"children":{"a":42}}"#).unwrap_err();
        assert_eq!(err, PathError::MalformedMonoidField);
    }

    #[test]
    fn dispatch_canon_v1_matches_canonical_field_paths_directly() {
        let canonical = real_canonical(br#"{"a":{"b":1},"c":[1,2]}"#);
        assert_eq!(
            canonical_field_paths_for(CANON_V1, &canonical).unwrap(),
            canonical_field_paths(&canonical).unwrap()
        );
    }

    #[test]
    fn dispatch_unknown_canonicalizer_reports_named_error() {
        let err = canonical_field_paths_for("some-future-v2", "{}").unwrap_err();
        assert_eq!(
            err,
            PathError::UnknownCanonicalizer("some-future-v2".to_string())
        );
    }

    #[test]
    fn monoid_validation_wires_through_validate_paths() {
        let canonical = real_monoid_canonical(&[br#"{"items":[{"x":1}]}"#]);
        let valid = canonical_field_paths_for(MONOID_V1, &canonical).unwrap();
        let meta = metadata(vec![field(vec![
            key("items"),
            PathSegment::Wildcard,
            key("x"),
        ])]);
        assert!(validate_paths(&meta, &valid).is_ok());

        let missing = metadata(vec![field(vec![key("does_not_exist")])]);
        let err = validate_paths(&missing, &valid).unwrap_err();
        assert_eq!(err, PathError::UnknownPath(vec![key("does_not_exist")]));
    }
}
