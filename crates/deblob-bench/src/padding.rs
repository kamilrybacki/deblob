//! Pads a record's base object up to (roughly) a target serialized size by
//! adding a `meta_padding` sub-object of small realistic-looking fields —
//! deliberately many small fields rather than one giant string, so the
//! padded record still resembles a real payload's field layout.

use serde_json::{Map, Value};

/// Safety ceiling on the number of filler fields, so a pathological target
/// can't spin forever.
const MAX_FILLER_FIELDS: usize = 8_192;

/// One rotating filler value: alternates string/number/bool so padding
/// looks like a handful of realistic fields rather than one giant string.
fn filler_value(i: usize) -> Value {
    match i % 3 {
        0 => Value::String("x".repeat(20)),
        1 => Value::Number((i as u64).into()),
        _ => Value::Bool(i % 2 == 0),
    }
}

/// Serialized-byte cost of one `"pad_N":value` pair standing alone
/// (key + colon + value, no surrounding braces/comma — those are counted
/// separately). Used to turn a target byte count into a field count
/// without re-serializing the whole (possibly large) object on every
/// single field addition.
fn isolated_field_cost(key: &str, value: &Value) -> usize {
    serde_json::to_string(key).expect("key serializes").len()
        + 1 // ':'
        + serde_json::to_string(value).expect("value serializes").len()
        + 1 // ',' joiner (over-counts by one for the very first field, negligible)
}

fn serialized_len(obj: &Map<String, Value>) -> usize {
    serde_json::to_string(&Value::Object(obj.clone()))
        .expect("object serializes")
        .len()
}

/// Pad `obj` in place with a `meta_padding` object until its serialized
/// size is at or above `target_bytes`, or [`MAX_FILLER_FIELDS`] is hit.
/// Adds nothing if `obj` already meets the target. Deterministic: the
/// padding added depends only on `obj`'s current serialized length and
/// `target_bytes`, never on randomness, so identical base objects always
/// get identical padding.
///
/// Estimates the required field count from the average per-field cost
/// (measured once, not by re-serializing the growing object on every
/// addition — that would be quadratic in the field count) and builds the
/// filler object in one pass, then runs a small bounded correction loop to
/// close any gap left by the estimate.
pub fn pad_to_target(obj: &mut Map<String, Value>, target_bytes: usize) {
    obj.remove("meta_padding");
    let baseline = serialized_len(obj);
    if baseline >= target_bytes {
        return;
    }
    let needed = target_bytes - baseline;

    let avg_cost: f64 = (0..3)
        .map(|i| isolated_field_cost(&format!("pad_{i}"), &filler_value(i)) as f64)
        .sum::<f64>()
        / 3.0;
    let estimated_count =
        ((needed as f64 / avg_cost.max(1.0)).ceil() as usize).clamp(1, MAX_FILLER_FIELDS);

    let mut filler = Map::new();
    for i in 0..estimated_count {
        filler.insert(format!("pad_{i}"), filler_value(i));
    }
    obj.insert("meta_padding".to_string(), Value::Object(filler));

    // Bounded correction: the closed-form estimate can undershoot slightly
    // (growing key-digit widths, the wrapping-object overhead). Top up a
    // few fields at a time until the target is met or the ceiling is hit.
    while serialized_len(obj) < target_bytes {
        let Some(Value::Object(filler)) = obj.get_mut("meta_padding") else {
            unreachable!("meta_padding was just inserted as an object")
        };
        if filler.len() >= MAX_FILLER_FIELDS {
            break;
        }
        let i = filler.len();
        filler.insert(format!("pad_{i}"), filler_value(i));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pads_up_toward_target() {
        let mut obj = Map::new();
        obj.insert("id".to_string(), Value::String("a".to_string()));
        let before = serde_json::to_string(&Value::Object(obj.clone()))
            .unwrap()
            .len();
        pad_to_target(&mut obj, 2_000);
        let after = serde_json::to_string(&Value::Object(obj.clone()))
            .unwrap()
            .len();
        assert!(before < 2_000);
        assert!(
            after >= 2_000,
            "expected padded size >= target, got {after}"
        );
    }

    #[test]
    fn no_op_when_already_at_target() {
        let mut obj = Map::new();
        obj.insert("blob".to_string(), Value::String("x".repeat(500)));
        let before = serde_json::to_string(&Value::Object(obj.clone()))
            .unwrap()
            .len();
        pad_to_target(&mut obj, 100);
        let after = serde_json::to_string(&Value::Object(obj.clone()))
            .unwrap()
            .len();
        assert_eq!(before, after);
        assert!(!obj.contains_key("meta_padding"));
    }
}
