//! Correspondence adjudication + transform assembly (design §clustering signal,
//! §Prompt 1, §Prompt 3).
//!
//! The division of labour is the design's whole safety story: **deterministic code
//! enumerates finite candidate correspondences** from hard evidence, an
//! [`Adjudicator`] returns a *bounded* [`Relation`] per candidate (the SLM is one
//! pluggable adjudicator; the built-in [`DeterministicAnchor`] is the non-SLM
//! evidence class the gate requires), and [`assemble_transform`] binds only the
//! **auto-bindable** relations. The result is not trusted until it passes
//! [`crate::verify`].

use crate::types::{
    Binding, Cardinality, CastMode, ChildTransform, FieldType, JsonPath, OnError, OnMissing, Op,
    Relation, ScalarType, UmbrellaField, UmbrellaSchema,
};
use crate::units;
use crate::verify::ChildField;
use std::collections::BTreeSet;

/// How a child field's unit relates to an umbrella field's unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitRelation {
    /// Neither side declares a unit.
    NoUnits,
    /// Identical units.
    Same,
    /// A registry rule maps child → umbrella.
    Convertible { rule_id: String },
    /// Different dimensions, no rule, or unit-vs-unitless — never auto-bindable.
    Incompatible,
}

/// Deterministic evidence for one (child, umbrella-field) candidate pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairEvidence {
    /// The child's silver canonical_field_id equals the umbrella field's.
    pub canonical_id_match: bool,
    /// The child scalar type widens losslessly to the umbrella field's scalar type.
    pub type_lossless: bool,
    pub unit: UnitRelation,
}

/// One enumerated candidate correspondence awaiting adjudication.
pub struct CandidatePair<'a> {
    pub child: &'a ChildField,
    pub umbrella_field: &'a UmbrellaField,
    pub evidence: PairEvidence,
}

fn umbrella_scalar(uf: &UmbrellaField) -> Option<ScalarType> {
    match &uf.ty {
        FieldType::Scalar(s) => Some(*s),
        FieldType::Array(_) => None,
    }
}

fn unit_relation(
    child: Option<&deblob_core::semantic::Unit>,
    umb: Option<&deblob_core::semantic::Unit>,
) -> UnitRelation {
    match (child, umb) {
        (None, None) => UnitRelation::NoUnits,
        (Some(c), Some(u)) if c == u => UnitRelation::Same,
        (Some(c), Some(u)) => match units::find_rule(c, u) {
            Some(r) => UnitRelation::Convertible {
                rule_id: r.rule_id.to_string(),
            },
            None => UnitRelation::Incompatible,
        },
        _ => UnitRelation::Incompatible, // unit-vs-unitless
    }
}

/// Enumerate candidate correspondences. V1 keys on the deterministic anchor: a
/// pair is only a candidate when the child's `canonical_field_id` equals the
/// umbrella field's. (A future SLM slice would widen enumeration to name/structure
/// neighbours for children lacking an annotation — but those still get adjudicated,
/// never auto-bound without corroboration.)
pub fn enumerate<'a>(
    child_fields: &'a [ChildField],
    umbrella: &'a UmbrellaSchema,
) -> Vec<CandidatePair<'a>> {
    let mut pairs = Vec::new();
    for cf in child_fields {
        let Some(cfid) = &cf.canonical_field_id else {
            continue;
        };
        for uf in &umbrella.fields {
            if &uf.canonical_field_id != cfid {
                continue;
            }
            let type_lossless = umbrella_scalar(uf).is_some_and(|s| cf.ty.widens_losslessly_to(s));
            let unit = unit_relation(cf.unit.as_ref(), uf.unit.as_ref());
            pairs.push(CandidatePair {
                child: cf,
                umbrella_field: uf,
                evidence: PairEvidence {
                    canonical_id_match: true,
                    type_lossless,
                    unit,
                },
            });
        }
    }
    pairs
}

/// Adjudicates a candidate pair to a bounded [`Relation`]. The SLM (deblob-slm)
/// would implement this by ranking finite hypotheses; the built-in
/// [`DeterministicAnchor`] implements it from hard evidence alone.
pub trait Adjudicator {
    fn adjudicate(&self, pair: &CandidatePair) -> Relation;
}

/// The non-SLM, deterministic evidence class the trust gate requires: decides
/// ONLY from canonical-id agreement + the unit relation, and returns
/// [`Relation::Unknown`] whenever it cannot decide — it never guesses.
pub struct DeterministicAnchor;

impl Adjudicator for DeterministicAnchor {
    fn adjudicate(&self, pair: &CandidatePair) -> Relation {
        let e = &pair.evidence;
        if !e.canonical_id_match {
            return Relation::Unknown;
        }
        match &e.unit {
            UnitRelation::NoUnits | UnitRelation::Same => {
                if e.type_lossless {
                    Relation::ExactEquivalent
                } else {
                    Relation::Unknown
                }
            }
            UnitRelation::Convertible { .. } => Relation::SameQuantityDifferentUnit,
            // same concept, irreconcilable unit (e.g. Cel vs W) — defer, never merge.
            UnitRelation::Incompatible => Relation::Unknown,
        }
    }
}

/// Assemble a [`ChildTransform`] by binding only the **auto-bindable** adjudicated
/// correspondences (exact / same-quantity), first-accepted-wins per child and per
/// target. Unbound child fields are parked in `unmapped_source_paths`, never
/// silently dropped. The result must still pass [`crate::verify::verify_static`]
/// and [`crate::verify::replay`] before it is trusted.
pub fn assemble_transform(
    child_schema_id: &str,
    child_revision: &str,
    umbrella: &UmbrellaSchema,
    umbrella_revision: &str,
    child_fields: &[ChildField],
    adjudicator: &dyn Adjudicator,
) -> ChildTransform {
    let pairs = enumerate(child_fields, umbrella);
    let mut bindings: Vec<Binding> = Vec::new();
    let mut bound_children: BTreeSet<Vec<String>> = BTreeSet::new();

    for pair in &pairs {
        let rel = adjudicator.adjudicate(pair);
        if !rel.auto_bindable() {
            continue;
        }
        if bound_children.contains(&pair.child.path.0) {
            continue;
        }
        if bindings
            .iter()
            .any(|b| b.target == pair.umbrella_field.path)
        {
            continue;
        }
        let mut ops = Vec::new();
        if let Some(want) = umbrella_scalar(pair.umbrella_field) {
            if pair.child.ty != want && pair.child.ty.widens_losslessly_to(want) {
                ops.push(Op::Cast {
                    to: want,
                    mode: CastMode::Lossless,
                });
            }
        }
        if let UnitRelation::Convertible { rule_id } = &pair.evidence.unit {
            // both units are Some here (Convertible only arises for Some/Some)
            if let (Some(from), Some(to)) =
                (pair.child.unit.clone(), pair.umbrella_field.unit.clone())
            {
                ops.push(Op::UnitConvert {
                    from,
                    to,
                    rule_id: rule_id.clone(),
                });
            }
        }
        let on_missing = if pair.umbrella_field.cardinality == Cardinality::Required {
            OnMissing::Reject
        } else {
            OnMissing::Omit
        };
        bindings.push(Binding {
            source: pair.child.path.clone(),
            target: pair.umbrella_field.path.clone(),
            ops,
            on_missing,
            on_error: OnError::Reject,
        });
        bound_children.insert(pair.child.path.0.clone());
    }

    let unmapped: Vec<JsonPath> = child_fields
        .iter()
        .filter(|c| !bound_children.contains(&c.path.0))
        .map(|c| c.path.clone())
        .collect();

    ChildTransform {
        child_schema_id: child_schema_id.into(),
        umbrella_id: umbrella.umbrella_id.clone(),
        child_revision: child_revision.into(),
        umbrella_revision: umbrella_revision.into(),
        bindings,
        unmapped_source_paths: unmapped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verify::{replay, verify_static};
    use deblob_core::semantic::{CanonicalFieldId, Unit, UnitSystem};
    use serde_json::json;

    fn ucum(c: &str) -> Unit {
        Unit {
            system: UnitSystem::Ucum,
            code: c.into(),
        }
    }
    fn cfid(s: &str) -> CanonicalFieldId {
        CanonicalFieldId::new(s)
    }
    fn scalar(s: ScalarType) -> FieldType {
        FieldType::Scalar(s)
    }
    fn uf(
        cf: &str,
        path: &str,
        ty: FieldType,
        unit: Option<Unit>,
        card: Cardinality,
    ) -> UmbrellaField {
        UmbrellaField {
            canonical_field_id: cfid(cf),
            path: JsonPath::parse(path).unwrap(),
            name: cf.into(),
            ty,
            unit,
            cardinality: card,
        }
    }
    fn cf(path: &str, ty: ScalarType, unit: Option<Unit>, canon: Option<&str>) -> ChildField {
        ChildField {
            path: JsonPath::parse(path).unwrap(),
            ty,
            unit,
            is_array: false,
            canonical_field_id: canon.map(cfid),
        }
    }
    fn weather() -> UmbrellaSchema {
        UmbrellaSchema {
            umbrella_id: "umb_weather".into(),
            label: "weather_observation".into(),
            version: 1,
            fields: vec![
                uf(
                    "air_temperature",
                    "$.air_temperature",
                    scalar(ScalarType::Decimal),
                    Some(ucum("K")),
                    Cardinality::Required,
                ),
                uf(
                    "event_time",
                    "$.event_time",
                    scalar(ScalarType::Integer),
                    None,
                    Cardinality::Required,
                ),
            ],
        }
    }

    #[test]
    fn anchor_assembles_verified_transform() {
        let umb = weather();
        // child: main.temp is Celsius (same canonical id, convertible unit); dt is the event time.
        let child = vec![
            cf(
                "$.main.temp",
                ScalarType::Decimal,
                Some(ucum("Cel")),
                Some("air_temperature"),
            ),
            cf("$.dt", ScalarType::Integer, None, Some("event_time")),
        ];
        let t = assemble_transform(
            "sch_ow",
            "sem_ow@1",
            &umb,
            "umb_weather@1",
            &child,
            &DeterministicAnchor,
        );
        // both fields bound, none parked
        assert_eq!(t.bindings.len(), 2);
        assert!(t.unmapped_source_paths.is_empty());
        // the temperature binding carries a unit_convert
        let temp = t
            .bindings
            .iter()
            .find(|b| String::from(b.target.clone()) == "$.air_temperature")
            .unwrap();
        assert!(temp.ops.iter().any(|o| matches!(o, Op::UnitConvert { .. })));
        // it passes the gate + real replay
        assert_eq!(verify_static(&t, &umb, &child), vec![]);
        let rep = replay(&t, &umb, &[json!({"main": {"temp": 25.0}, "dt": 1})]);
        assert!(rep.passed(), "{:?}", rep);
    }

    #[test]
    fn anchor_defers_on_incompatible_unit() {
        // same canonical id, but child unit is Cel and umbrella wants W (different
        // dimension) — a data error the anchor must NOT auto-bind.
        let umb = UmbrellaSchema {
            umbrella_id: "u".into(),
            label: "x".into(),
            version: 1,
            fields: vec![uf(
                "power",
                "$.power",
                scalar(ScalarType::Decimal),
                Some(ucum("W")),
                Cardinality::Optional,
            )],
        };
        let child = vec![cf(
            "$.p",
            ScalarType::Decimal,
            Some(ucum("Cel")),
            Some("power"),
        )];
        let pairs = enumerate(&child, &umb);
        assert_eq!(pairs.len(), 1);
        assert_eq!(DeterministicAnchor.adjudicate(&pairs[0]), Relation::Unknown);
        let t = assemble_transform("sch", "a@1", &umb, "u@1", &child, &DeterministicAnchor);
        assert!(t.bindings.is_empty());
        assert_eq!(t.unmapped_source_paths.len(), 1); // parked
    }

    #[test]
    fn no_canonical_id_yields_no_candidates() {
        let umb = weather();
        let child = vec![cf(
            "$.main.temp",
            ScalarType::Decimal,
            Some(ucum("Cel")),
            None,
        )];
        assert!(enumerate(&child, &umb).is_empty());
    }

    #[test]
    fn slm_adjudicator_can_withhold_binding() {
        // A pluggable adjudicator that treats everything as merely RELATED (not
        // auto-bindable) — assembly must produce no binding, proving determinism
        // disposes: even an anchor-eligible pair isn't bound without an
        // auto-bindable relation.
        struct RelatedOnly;
        impl Adjudicator for RelatedOnly {
            fn adjudicate(&self, _: &CandidatePair) -> Relation {
                Relation::Related
            }
        }
        let umb = weather();
        let child = vec![cf("$.dt", ScalarType::Integer, None, Some("event_time"))];
        let t = assemble_transform("sch", "a@1", &umb, "u@1", &child, &RelatedOnly);
        assert!(t.bindings.is_empty());
    }
}
