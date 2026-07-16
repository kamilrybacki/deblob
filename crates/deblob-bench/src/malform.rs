//! Turns a well-formed record into an invalid one, matching the three
//! malformed shapes the spec calls out (duplicate JSON key, `NaN`, and a
//! truncated body) — each engineered to trip a specific rejection path in
//! `deblob_fingerprint::parse::parse_bounded` (`DuplicateKey`,
//! `ParseError`, `ParseError` respectively).

use serde_json::{Map, Value};

use crate::fields::{FieldKind, FieldSpec};

/// Duplicate the record's alphabetically-first key at the front of the
/// object, so the parser's duplicate-key check trips on the *second*
/// occurrence. `json` must be `obj` serialized with `serde_json::to_string`
/// (a `{...}` object literal with no leading/trailing whitespace).
pub fn duplicate_key(json: &str, obj: &Map<String, Value>) -> Vec<u8> {
    let (k, v) = obj
        .iter()
        .next()
        .expect("well-formed records always have at least the core fields");
    let dup = format!(
        "\"{k}\":{}",
        serde_json::to_string(v).expect("value serializes")
    );
    let inner = &json[1..json.len() - 1]; // strip the outer '{' / '}'
    format!("{{{dup},{inner}}}").into_bytes()
}

/// Replace one numeric field's value text with the bare token `NaN`, which
/// the number grammar in `parse_bounded` does not accept (it only accepts
/// `-`/digit-led tokens).
pub fn nan_value(json: &str, obj: &Map<String, Value>, fields: &[FieldSpec]) -> Vec<u8> {
    let num_field = fields
        .iter()
        .find(|f| f.kind == FieldKind::Num && obj.contains_key(f.name))
        .expect("every schema family includes a numeric core field (created_at)");
    let value = obj
        .get(num_field.name)
        .expect("field name was just matched against this object");
    let needle = format!(
        "\"{}\":{}",
        num_field.name,
        serde_json::to_string(value).expect("value serializes")
    );
    let replacement = format!("\"{}\":NaN", num_field.name);
    json.replacen(&needle, &replacement, 1).into_bytes()
}

/// Cut the serialized body off partway through, leaving an unterminated
/// object/array/string that the parser hits end-of-input on.
pub fn truncated(json: &str) -> Vec<u8> {
    let target = (json.len() * 2 / 3).max(1);
    let mut cut = target.min(json.len().saturating_sub(1));
    while cut > 0 && !json.is_char_boundary(cut) {
        cut -= 1;
    }
    json.as_bytes()[..cut].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::CORE_FIELDS;
    use deblob_core::error::QuarantineReason;
    use deblob_fingerprint::{parse_bounded, Limits};

    fn sample_obj() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("id".to_string(), Value::String("x".to_string()));
        m.insert("kind".to_string(), Value::String("y".to_string()));
        m.insert("created_at".to_string(), Value::Number(1.into()));
        m
    }

    #[test]
    fn duplicate_key_rejected_with_duplicate_key_reason() {
        let obj = sample_obj();
        let json = serde_json::to_string(&Value::Object(obj.clone())).unwrap();
        let bytes = duplicate_key(&json, &obj);
        let err = parse_bounded(&bytes, &Limits::default()).unwrap_err();
        assert_eq!(err, QuarantineReason::DuplicateKey);
    }

    #[test]
    fn nan_value_rejected_as_parse_error() {
        let obj = sample_obj();
        let json = serde_json::to_string(&Value::Object(obj.clone())).unwrap();
        let bytes = nan_value(&json, &obj, CORE_FIELDS);
        let err = parse_bounded(&bytes, &Limits::default()).unwrap_err();
        assert_eq!(err, QuarantineReason::ParseError);
    }

    #[test]
    fn truncated_rejected() {
        let obj = sample_obj();
        let json = serde_json::to_string(&Value::Object(obj)).unwrap();
        let bytes = truncated(&json);
        assert!(parse_bounded(&bytes, &Limits::default()).is_err());
    }
}
