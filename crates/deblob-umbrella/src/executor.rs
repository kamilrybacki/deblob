//! Deterministic executor: apply a [`ChildTransform`] to one child event and
//! produce a gold event. Pure and idempotent — no clock, no randomness, no I/O —
//! so the same input always yields byte-identical output (design §held-out
//! execution). Any op failure rejects the whole event; nothing is silently
//! dropped or coerced.

use crate::types::{Binding, ChildTransform, Op, OnMissing, ScalarType};
use crate::{path, units};
use serde_json::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ExecError {
    #[error("required source path {0} missing and on_missing=reject")]
    MissingSource(String),
    #[error("on_missing=use_default but binding for {0} has no default op")]
    NoDefault(String),
    #[error("cast to {to:?} failed: value is not a compatible {to:?}")]
    CastFailed { to: ScalarType },
    #[error("unit_convert expects a number at {0}")]
    UnitConvertNonNumber(String),
    #[error("array_map expects an array at {0}")]
    ArrayMapNonArray(String),
    #[error(transparent)]
    Unit(#[from] units::UnitError),
    #[error(transparent)]
    Set(#[from] path::PathSetError),
    #[error("number {0} is not representable as f64 for unit conversion")]
    NonFiniteNumber(String),
}

/// The scalar type of a JSON value, or `None` for null/array/object.
pub fn value_scalar_type(v: &Value) -> Option<ScalarType> {
    match v {
        Value::Bool(_) => Some(ScalarType::Bool),
        Value::String(_) => Some(ScalarType::String),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Some(ScalarType::Integer)
            } else {
                Some(ScalarType::Decimal)
            }
        }
        _ => None,
    }
}

/// Apply a transform to a child event, producing a gold event.
pub fn apply(transform: &ChildTransform, child_event: &Value) -> Result<Value, ExecError> {
    let mut out = Value::Object(serde_json::Map::new());
    for b in &transform.bindings {
        let src_path_str = String::from(b.source.clone());
        match path::get(child_event, &b.source) {
            Some(v) => {
                let mapped = apply_ops(v.clone(), &b.ops, &src_path_str)?;
                path::set(&mut out, &b.target, mapped)?;
            }
            None => match b.on_missing {
                OnMissing::Omit => {} // legal only for optional targets (checked in verify)
                OnMissing::Reject => return Err(ExecError::MissingSource(src_path_str)),
                OnMissing::UseDefault => {
                    let d = default_value(b).ok_or(ExecError::NoDefault(src_path_str))?;
                    path::set(&mut out, &b.target, d)?;
                }
            },
        }
    }
    Ok(out)
}

fn default_value(b: &Binding) -> Option<Value> {
    b.ops.iter().find_map(|op| match op {
        Op::Default { value, .. } => Some(value.clone()),
        _ => None,
    })
}

fn apply_ops(mut v: Value, ops: &[Op], src_path: &str) -> Result<Value, ExecError> {
    for op in ops {
        v = match op {
            Op::Cast { to, .. } => {
                let vt = value_scalar_type(&v).ok_or(ExecError::CastFailed { to: *to })?;
                if vt.widens_losslessly_to(*to) {
                    v
                } else {
                    return Err(ExecError::CastFailed { to: *to });
                }
            }
            Op::UnitConvert { rule_id, .. } => {
                let n = v
                    .as_f64()
                    .ok_or_else(|| ExecError::UnitConvertNonNumber(src_path.to_string()))?;
                if !n.is_finite() {
                    return Err(ExecError::NonFiniteNumber(src_path.to_string()));
                }
                let converted = units::convert(n, rule_id)?;
                Value::from(converted)
            }
            // A Default op only materialises via OnMissing::UseDefault; on a
            // present value it is a no-op (verify forbids it as the sole op).
            Op::Default { .. } => v,
            Op::ArrayMap { element_ops } => {
                let arr = v
                    .as_array()
                    .ok_or_else(|| ExecError::ArrayMapNonArray(src_path.to_string()))?;
                let mapped: Result<Vec<Value>, ExecError> = arr
                    .iter()
                    .map(|el| apply_ops(el.clone(), element_ops, src_path))
                    .collect();
                Value::Array(mapped?)
            }
        };
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CastMode, JsonPath, OnError};
    use deblob_core::semantic::{Unit, UnitSystem};
    use serde_json::json;

    fn ucum(c: &str) -> Unit { Unit { system: UnitSystem::Ucum, code: c.into() } }
    fn bind(src: &str, tgt: &str, ops: Vec<Op>, on_missing: OnMissing) -> Binding {
        Binding {
            source: JsonPath::parse(src).unwrap(),
            target: JsonPath::parse(tgt).unwrap(),
            ops,
            on_missing,
            on_error: OnError::Reject,
        }
    }
    fn transform(bindings: Vec<Binding>) -> ChildTransform {
        ChildTransform {
            child_schema_id: "sch_a".into(),
            umbrella_id: "umb_weather".into(),
            child_revision: "sem_a@1".into(),
            umbrella_revision: "umb_weather@1".into(),
            bindings,
            unmapped_source_paths: vec![],
        }
    }

    #[test]
    fn rename_cast_and_unit_convert() {
        // OpenWeather-ish {main:{temp:25.0 Cel}, dt: 1} -> {air_temperature: 298.15 K, event_time: 1}
        let t = transform(vec![
            bind("$.main.temp", "$.air_temperature",
                 vec![Op::Cast { to: ScalarType::Decimal, mode: CastMode::Lossless },
                      Op::UnitConvert { from: ucum("Cel"), to: ucum("K"), rule_id: "ucum:Cel->K".into() }],
                 OnMissing::Reject),
            bind("$.dt", "$.event_time", vec![], OnMissing::Reject),
        ]);
        let child = json!({"main": {"temp": 25.0}, "dt": 1});
        let gold = apply(&t, &child).unwrap();
        assert!((gold["air_temperature"].as_f64().unwrap() - 298.15).abs() < 1e-9);
        assert_eq!(gold["event_time"], json!(1));
    }

    #[test]
    fn deterministic_and_idempotent() {
        let t = transform(vec![bind("$.a", "$.x", vec![], OnMissing::Reject)]);
        let child = json!({"a": 5});
        assert_eq!(apply(&t, &child).unwrap(), apply(&t, &child).unwrap());
    }

    #[test]
    fn missing_source_rejects_or_defaults() {
        let reject = transform(vec![bind("$.a", "$.x", vec![], OnMissing::Reject)]);
        assert!(matches!(apply(&reject, &json!({})), Err(ExecError::MissingSource(_))));

        let dflt = transform(vec![bind(
            "$.a", "$.x",
            vec![Op::Default { value: json!("n/a"), synthetic: true }],
            OnMissing::UseDefault,
        )]);
        assert_eq!(apply(&dflt, &json!({})).unwrap(), json!({"x": "n/a"}));
    }

    #[test]
    fn omit_drops_optional_target() {
        let t = transform(vec![bind("$.a", "$.x", vec![], OnMissing::Omit)]);
        assert_eq!(apply(&t, &json!({})).unwrap(), json!({}));
    }

    #[test]
    fn lossy_cast_is_rejected() {
        // Decimal value, cast to Integer -> lossy -> reject
        let t = transform(vec![bind(
            "$.a", "$.x",
            vec![Op::Cast { to: ScalarType::Integer, mode: CastMode::Lossless }],
            OnMissing::Reject,
        )]);
        assert!(matches!(apply(&t, &json!({"a": 1.5})), Err(ExecError::CastFailed { .. })));
    }

    #[test]
    fn array_map_converts_elementwise() {
        // generation_mw: [1.0, 2.0] MW -> [1_000_000, 2_000_000] W
        let t = transform(vec![bind(
            "$.gen", "$.generation_w",
            vec![Op::ArrayMap { element_ops: vec![Op::UnitConvert {
                from: ucum("MW"), to: ucum("W"), rule_id: "ucum:MW->W".into() }] }],
            OnMissing::Reject,
        )]);
        let gold = apply(&t, &json!({"gen": [1.0, 2.0]})).unwrap();
        assert_eq!(gold, json!({"generation_w": [1_000_000.0, 2_000_000.0]}));
    }
}
