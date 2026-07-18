//! Get/set a `serde_json::Value` by a restricted [`JsonPath`] (object keys only).

use crate::types::JsonPath;
use serde_json::Value;

/// Resolve a path to a value, or `None` if any segment is missing or a non-object
/// is encountered mid-path. A JSON `null` at the leaf is returned as `Some(Null)` —
/// present-but-null is distinct from absent (design §missing vs null).
pub fn get<'a>(root: &'a Value, path: &JsonPath) -> Option<&'a Value> {
    let mut cur = root;
    for seg in &path.0 {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

/// Whether the path resolves to a present value (including an explicit null).
pub fn exists(root: &Value, path: &JsonPath) -> bool {
    get(root, path).is_some()
}

/// Set a value at a path, creating intermediate objects as needed. Errors if an
/// intermediate segment already holds a non-object (would clobber structure).
pub fn set(root: &mut Value, path: &JsonPath, v: Value) -> Result<(), PathSetError> {
    if root.is_null() {
        *root = Value::Object(serde_json::Map::new());
    }
    let mut cur = root;
    let n = path.0.len();
    for (i, seg) in path.0.iter().enumerate() {
        let obj = cur.as_object_mut().ok_or_else(|| PathSetError(seg.clone()))?;
        if i == n - 1 {
            obj.insert(seg.clone(), v);
            return Ok(());
        }
        cur = obj
            .entry(seg.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        if !cur.is_object() {
            return Err(PathSetError(seg.clone()));
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("cannot set path: segment {0:?} is not an object")]
pub struct PathSetError(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn get_nested_and_missing() {
        let v = json!({"main": {"temp": 21.5}, "id": "x"});
        assert_eq!(get(&v, &JsonPath::parse("$.main.temp").unwrap()), Some(&json!(21.5)));
        assert_eq!(get(&v, &JsonPath::parse("$.id").unwrap()), Some(&json!("x")));
        assert_eq!(get(&v, &JsonPath::parse("$.main.humidity").unwrap()), None);
        assert_eq!(get(&v, &JsonPath::parse("$.nope.deep").unwrap()), None);
    }

    #[test]
    fn present_null_is_some() {
        let v = json!({"x": null});
        assert_eq!(get(&v, &JsonPath::parse("$.x").unwrap()), Some(&Value::Null));
        assert!(exists(&v, &JsonPath::parse("$.x").unwrap()));
        assert!(!exists(&v, &JsonPath::parse("$.y").unwrap()));
    }

    #[test]
    fn set_creates_nested_objects() {
        let mut out = Value::Null;
        set(&mut out, &JsonPath::parse("$.a.b.c").unwrap(), json!(7)).unwrap();
        assert_eq!(out, json!({"a": {"b": {"c": 7}}}));
        set(&mut out, &JsonPath::parse("$.a.b.d").unwrap(), json!(8)).unwrap();
        assert_eq!(out, json!({"a": {"b": {"c": 7, "d": 8}}}));
    }

    #[test]
    fn set_rejects_clobbering_scalar() {
        let mut out = json!({"a": 1});
        let err = set(&mut out, &JsonPath::parse("$.a.b").unwrap(), json!(2));
        assert!(err.is_err());
    }
}
