//! Deterministic verification of a transform bundle (design §static transform
//! verification + §held-out execution). Nothing here calls an SLM — this is the
//! authority a proposal (SLM- or human-authored) must satisfy before it can be
//! trusted. Two layers:
//!   * [`verify_static`] — structural + type + unit soundness against the child's
//!     field set and the umbrella schema, with no data needed.
//!   * [`replay`] — apply the transform to held-out sample events and confirm every
//!     output validates against the umbrella and is deterministic.

use crate::executor::{self, value_scalar_type};
use crate::types::{
    Cardinality, ChildTransform, FieldType, JsonPath, Op, OnMissing, ScalarType, UmbrellaField,
    UmbrellaSchema,
};
use crate::units;
use deblob_core::semantic::Unit;
use serde_json::Value;
use std::collections::BTreeSet;

/// A child schema's leaf field, as the caller extracts it from the bronze/silver
/// schema. Kept decoupled from the fingerprint crate: `deblob-umbrella` only needs
/// path + scalar type + declared unit to verify a transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildField {
    pub path: JsonPath,
    pub ty: ScalarType,
    pub unit: Option<Unit>,
    pub is_array: bool,
}

/// A single reason a transform bundle is not verifiably sound.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyIssue {
    #[error("binding source {0} is not a field of the child schema")]
    SourceMissing(String),
    #[error("binding target {0} is not a field of the umbrella")]
    UnknownTarget(String),
    #[error("two bindings target the same umbrella field {0}")]
    DuplicateTarget(String),
    #[error("required umbrella field {0} has no total source-derived binding")]
    RequiredNotTotal(String),
    #[error("on_missing=omit is illegal for required umbrella field {0}")]
    OmitOnRequired(String),
    #[error("cast to {to:?} at target {target} is lossy from {from:?}")]
    LossyCast { target: String, from: ScalarType, to: ScalarType },
    #[error("final type {got:?} at target {target} does not match umbrella type {want:?}")]
    TypeMismatch { target: String, got: ScalarType, want: ScalarType },
    #[error("unit op at target {target} is invalid: {reason}")]
    BadUnitOp { target: String, reason: String },
    #[error("synthetic default at target {target} may not satisfy a required field")]
    SyntheticSatisfiesRequired { target: String },
}

/// Static soundness of a bundle. An empty result means it passed every structural,
/// type, and unit check — a necessary (not yet sufficient; see [`replay`]) gate.
pub fn verify_static(
    transform: &ChildTransform,
    umbrella: &UmbrellaSchema,
    child_fields: &[ChildField],
) -> Vec<VerifyIssue> {
    let mut issues = Vec::new();
    let mut targeted: BTreeSet<Vec<String>> = BTreeSet::new();

    for b in &transform.bindings {
        let tgt_str = String::from(b.target.clone());
        // 1. source exists on the child
        let child = child_fields.iter().find(|f| f.path == b.source);
        if child.is_none() {
            issues.push(VerifyIssue::SourceMissing(String::from(b.source.clone())));
        }
        // 2. target is an umbrella field
        let uf = umbrella.field(&b.target);
        let Some(uf) = uf else {
            issues.push(VerifyIssue::UnknownTarget(tgt_str));
            continue;
        };
        // 3. no duplicate target
        if !targeted.insert(b.target.0.clone()) {
            issues.push(VerifyIssue::DuplicateTarget(tgt_str.clone()));
        }
        // 4. omit only for optional
        if matches!(b.on_missing, OnMissing::Omit) && uf.cardinality == Cardinality::Required {
            issues.push(VerifyIssue::OmitOnRequired(tgt_str.clone()));
        }
        // 5. op-chain type + unit legality (only when source resolved)
        if let Some(child) = child {
            check_op_chain(child, uf, b, &tgt_str, &mut issues);
        }
    }

    // 6. every required umbrella field is totally, source-derived-ly satisfied
    for uf in &umbrella.fields {
        if uf.cardinality != Cardinality::Required {
            continue;
        }
        let total = transform.bindings.iter().any(|b| {
            b.target == uf.path
                && !matches!(b.on_missing, OnMissing::Omit)
                && !is_synthetic_only(b)
        });
        if !total {
            issues.push(VerifyIssue::RequiredNotTotal(String::from(uf.path.clone())));
        }
    }
    issues
}

/// A binding whose only value source is a synthetic default (no real source
/// projection) may not make a required field "total".
fn is_synthetic_only(b: &crate::types::Binding) -> bool {
    matches!(b.on_missing, OnMissing::UseDefault)
        && b.ops.iter().any(|o| matches!(o, Op::Default { synthetic: true, .. }))
        && b.ops.iter().all(|o| matches!(o, Op::Default { .. }))
}

fn check_op_chain(
    child: &ChildField,
    uf: &UmbrellaField,
    b: &crate::types::Binding,
    tgt: &str,
    issues: &mut Vec<VerifyIssue>,
) {
    if is_synthetic_only(b) {
        if uf.cardinality == Cardinality::Required {
            issues.push(VerifyIssue::SyntheticSatisfiesRequired { target: tgt.into() });
        }
        return;
    }
    // Array fields: require both sides array; deep element typing is deferred (V1).
    if child.is_array || matches!(uf.ty, FieldType::Array(_)) {
        if !(child.is_array && matches!(uf.ty, FieldType::Array(_))) {
            issues.push(VerifyIssue::TypeMismatch {
                target: tgt.into(), got: child.ty, want: umbrella_scalar(uf).unwrap_or(child.ty),
            });
        }
        return;
    }
    // Scalar path: fold the op chain over the child's scalar type.
    let mut cur = child.ty;
    let mut cur_unit = child.unit.clone();
    for op in &b.ops {
        match op {
            Op::Cast { to, .. } => {
                if !cur.widens_losslessly_to(*to) {
                    issues.push(VerifyIssue::LossyCast { target: tgt.into(), from: cur, to: *to });
                }
                cur = *to;
            }
            Op::UnitConvert { from, to, rule_id } => {
                if cur_unit.as_ref() != Some(from) {
                    issues.push(VerifyIssue::BadUnitOp {
                        target: tgt.into(),
                        reason: format!("child unit {:?} != op.from {:?}", cur_unit, from),
                    });
                }
                if let Err(e) = units::verify_conversion(from, to, rule_id) {
                    issues.push(VerifyIssue::BadUnitOp { target: tgt.into(), reason: e.to_string() });
                }
                cur = ScalarType::Decimal;
                cur_unit = Some(to.clone());
            }
            Op::Default { .. } | Op::ArrayMap { .. } => {}
        }
    }
    // final scalar type must match the umbrella field
    if let Some(want) = umbrella_scalar(uf) {
        if !cur.widens_losslessly_to(want) {
            issues.push(VerifyIssue::TypeMismatch { target: tgt.into(), got: cur, want });
        }
    }
    // final unit must match the umbrella field's declared unit (if any)
    if uf.unit.is_some() && cur_unit != uf.unit {
        issues.push(VerifyIssue::BadUnitOp {
            target: tgt.into(),
            reason: format!("final unit {:?} != umbrella unit {:?}", cur_unit, uf.unit),
        });
    }
}

fn umbrella_scalar(uf: &UmbrellaField) -> Option<ScalarType> {
    match &uf.ty {
        FieldType::Scalar(s) => Some(*s),
        FieldType::Array(_) => None,
    }
}

/// Does a JSON value satisfy an umbrella field type?
fn value_matches(ty: &FieldType, v: &Value) -> bool {
    match ty {
        FieldType::Scalar(want) => match value_scalar_type(v) {
            Some(got) => got.widens_losslessly_to(*want),
            None => false,
        },
        FieldType::Array(el) => v.as_array().is_some_and(|a| a.iter().all(|x| value_matches(el, x))),
    }
}

/// Validate one emitted gold event against the umbrella: required fields present
/// with matching type; present optional fields matching type. Returns error strings.
pub fn validate_gold(umbrella: &UmbrellaSchema, event: &Value) -> Vec<String> {
    let mut errs = Vec::new();
    for uf in &umbrella.fields {
        match crate::path::get(event, &uf.path) {
            None => {
                if uf.cardinality == Cardinality::Required {
                    errs.push(format!("required field {} missing", String::from(uf.path.clone())));
                }
            }
            Some(v) => {
                if !value_matches(&uf.ty, v) {
                    errs.push(format!("field {} has wrong type", String::from(uf.path.clone())));
                }
            }
        }
    }
    errs
}

/// Held-out replay: apply the transform to each sample and confirm the output
/// validates against the umbrella and is deterministic (apply twice ≡). One entry
/// per failing sample.
#[derive(Debug, Default, PartialEq)]
pub struct ReplayReport {
    pub samples: usize,
    pub failures: Vec<ReplayFailure>,
}
#[derive(Debug, PartialEq)]
pub enum ReplayFailure {
    Exec { index: usize, error: String },
    Invalid { index: usize, errors: Vec<String> },
    NonDeterministic { index: usize },
}
impl ReplayReport {
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

pub fn replay(transform: &ChildTransform, umbrella: &UmbrellaSchema, samples: &[Value]) -> ReplayReport {
    let mut report = ReplayReport { samples: samples.len(), failures: Vec::new() };
    for (i, sample) in samples.iter().enumerate() {
        match executor::apply(transform, sample) {
            Err(e) => report.failures.push(ReplayFailure::Exec { index: i, error: e.to_string() }),
            Ok(gold) => {
                let errs = validate_gold(umbrella, &gold);
                if !errs.is_empty() {
                    report.failures.push(ReplayFailure::Invalid { index: i, errors: errs });
                }
                // determinism: a second application must be byte-identical
                if executor::apply(transform, sample).ok().as_ref() != Some(&gold) {
                    report.failures.push(ReplayFailure::NonDeterministic { index: i });
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Binding, OnError};
    use deblob_core::semantic::{CanonicalFieldId, UnitSystem};
    use serde_json::json;

    fn ucum(c: &str) -> Unit { Unit { system: UnitSystem::Ucum, code: c.into() } }
    fn scalar(s: ScalarType) -> FieldType { FieldType::Scalar(s) }
    fn uf(cfid: &str, path: &str, ty: FieldType, unit: Option<Unit>, card: Cardinality) -> UmbrellaField {
        UmbrellaField {
            canonical_field_id: CanonicalFieldId::new(cfid),
            path: JsonPath::parse(path).unwrap(),
            name: cfid.into(),
            ty, unit, cardinality: card,
        }
    }
    fn cf(path: &str, ty: ScalarType, unit: Option<Unit>) -> ChildField {
        ChildField { path: JsonPath::parse(path).unwrap(), ty, unit, is_array: false }
    }
    fn bind(src: &str, tgt: &str, ops: Vec<Op>, on_missing: OnMissing) -> Binding {
        Binding { source: JsonPath::parse(src).unwrap(), target: JsonPath::parse(tgt).unwrap(),
            ops, on_missing, on_error: OnError::Reject }
    }
    fn xform(bindings: Vec<Binding>) -> ChildTransform {
        ChildTransform { child_schema_id: "sch_a".into(), umbrella_id: "u".into(),
            child_revision: "a@1".into(), umbrella_revision: "u@1".into(), bindings, unmapped_source_paths: vec![] }
    }

    fn weather_umbrella() -> UmbrellaSchema {
        UmbrellaSchema {
            umbrella_id: "umb_weather".into(), label: "weather_observation".into(), version: 1,
            fields: vec![
                uf("air_temperature", "$.air_temperature", scalar(ScalarType::Decimal), Some(ucum("K")), Cardinality::Required),
                uf("event_time", "$.event_time", scalar(ScalarType::Integer), None, Cardinality::Required),
            ],
        }
    }

    #[test]
    fn sound_transform_passes_static_and_replay() {
        let umb = weather_umbrella();
        let child = vec![cf("$.main.temp", ScalarType::Decimal, Some(ucum("Cel"))), cf("$.dt", ScalarType::Integer, None)];
        let t = xform(vec![
            bind("$.main.temp", "$.air_temperature",
                 vec![Op::UnitConvert { from: ucum("Cel"), to: ucum("K"), rule_id: "ucum:Cel->K".into() }],
                 OnMissing::Reject),
            bind("$.dt", "$.event_time", vec![], OnMissing::Reject),
        ]);
        assert_eq!(verify_static(&t, &umb, &child), vec![]);
        let rep = replay(&t, &umb, &[json!({"main": {"temp": 25.0}, "dt": 1})]);
        assert!(rep.passed(), "{:?}", rep);
    }

    #[test]
    fn required_field_without_total_binding_is_flagged() {
        let umb = weather_umbrella();
        let child = vec![cf("$.dt", ScalarType::Integer, None)];
        let t = xform(vec![bind("$.dt", "$.event_time", vec![], OnMissing::Reject)]); // air_temperature unmapped
        let issues = verify_static(&t, &umb, &child);
        assert!(issues.iter().any(|i| matches!(i, VerifyIssue::RequiredNotTotal(_))));
    }

    #[test]
    fn wrong_unit_op_is_flagged() {
        let umb = weather_umbrella();
        let child = vec![cf("$.main.temp", ScalarType::Decimal, Some(ucum("Cel"))), cf("$.dt", ScalarType::Integer, None)];
        // op.from says K but child unit is Cel
        let t = xform(vec![
            bind("$.main.temp", "$.air_temperature",
                 vec![Op::UnitConvert { from: ucum("K"), to: ucum("K"), rule_id: "ucum:Cel->K".into() }], OnMissing::Reject),
            bind("$.dt", "$.event_time", vec![], OnMissing::Reject),
        ]);
        let issues = verify_static(&t, &umb, &child);
        assert!(issues.iter().any(|i| matches!(i, VerifyIssue::BadUnitOp { .. })));
    }

    #[test]
    fn synthetic_default_cannot_satisfy_required() {
        let umb = weather_umbrella();
        let child = vec![cf("$.dt", ScalarType::Integer, None)];
        let t = xform(vec![
            bind("$.dt", "$.event_time", vec![], OnMissing::Reject),
            bind("$.missing", "$.air_temperature",
                 vec![Op::Default { value: json!(0.0), synthetic: true }], OnMissing::UseDefault),
        ]);
        let issues = verify_static(&t, &umb, &child);
        assert!(issues.iter().any(|i| matches!(i,
            VerifyIssue::RequiredNotTotal(_) | VerifyIssue::SyntheticSatisfiesRequired { .. })));
    }

    #[test]
    fn replay_catches_invalid_gold() {
        // umbrella wants event_time Integer, but the child value is a string -> invalid gold
        let umb = weather_umbrella();
        let t = xform(vec![
            bind("$.main.temp", "$.air_temperature",
                 vec![Op::UnitConvert { from: ucum("Cel"), to: ucum("K"), rule_id: "ucum:Cel->K".into() }], OnMissing::Reject),
            bind("$.dt", "$.event_time", vec![], OnMissing::Reject),
        ]);
        let rep = replay(&t, &umb, &[json!({"main": {"temp": 25.0}, "dt": "not-an-int"})]);
        assert!(!rep.passed());
        assert!(rep.failures.iter().any(|f| matches!(f, ReplayFailure::Invalid { .. })));
    }
}
