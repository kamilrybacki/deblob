//! The closed umbrella / consolidation data model (V1).
//!
//! Gold is a **versioned mediated schema with verified projections** (design
//! §Core model): an [`UmbrellaSchema`] plus one [`ChildTransform`] per contributing
//! source. Every type here is intentionally closed — the SLM may only rank/select
//! finite instances of these, never invent new operators, paths, or fields.

use deblob_core::semantic::{CanonicalFieldId, Unit};
use serde::{Deserialize, Serialize};

/// The least-common-lossless scalar lattice for canonical (gold) field types.
/// `Integer` widens losslessly to `Decimal`; nothing else is an implicit widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarType {
    Bool,
    Integer,
    Decimal,
    String,
}

impl ScalarType {
    /// A value-preserving widening: identity, or `Integer -> Decimal`. Everything
    /// else (e.g. `Decimal -> Integer`, `String -> Integer`) is lossy and must be
    /// rejected by the trust gate rather than silently coerced.
    pub fn widens_losslessly_to(self, to: ScalarType) -> bool {
        self == to || (self == ScalarType::Integer && to == ScalarType::Decimal)
    }
}

/// A canonical (gold) field's type. Arrays are element-typed and
/// cardinality-preserving; objects nest further umbrella fields by path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "of")]
pub enum FieldType {
    Scalar(ScalarType),
    Array(Box<FieldType>),
}

/// Whether a gold field must be present in every emitted event. `Required` is
/// only ever set by deterministic totality analysis (every active child has a
/// total, source-derived mapping) — never by an SLM opinion or a synthetic default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    Required,
    Optional,
}

/// One field of a gold umbrella schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UmbrellaField {
    /// The deterministic identity anchor. Two child fields may only converge here
    /// when the semantic lane independently assigned them this same id.
    pub canonical_field_id: CanonicalFieldId,
    /// The path this field occupies in an emitted gold event (e.g. `$.air_temperature`).
    pub path: JsonPath,
    /// Human display name (labels are never identity — [`Self::canonical_field_id`] is).
    pub name: String,
    pub ty: FieldType,
    pub unit: Option<Unit>,
    pub cardinality: Cardinality,
}

/// A gold canonical event contract over N semantically-similar children.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UmbrellaSchema {
    pub umbrella_id: String,
    pub label: String,
    pub version: u32,
    pub fields: Vec<UmbrellaField>,
}

impl UmbrellaSchema {
    pub fn field(&self, path: &JsonPath) -> Option<&UmbrellaField> {
        self.fields.iter().find(|f| &f.path == path)
    }
}

/// A restricted JSON path: object keys only (`$.a.b.c`). Array traversal is never
/// a path segment — it is expressed by [`Op::ArrayMap`], which keeps cardinality
/// explicit. This deliberately excludes wildcards, filters, and indexing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct JsonPath(pub Vec<String>);

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("invalid json path {0:?}: must be $.key.key… of non-empty object keys")]
pub struct PathParseError(pub String);

impl std::convert::TryFrom<String> for JsonPath {
    type Error = PathParseError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        let rest = s.strip_prefix("$.").ok_or_else(|| PathParseError(s.clone()))?;
        if rest.is_empty() {
            return Err(PathParseError(s));
        }
        let segs: Vec<String> = rest.split('.').map(str::to_string).collect();
        if segs.iter().any(|k| k.is_empty()) {
            return Err(PathParseError(s));
        }
        Ok(JsonPath(segs))
    }
}

impl From<JsonPath> for String {
    fn from(p: JsonPath) -> String {
        let mut s = String::from("$");
        for seg in &p.0 {
            s.push('.');
            s.push_str(seg);
        }
        s
    }
}

impl JsonPath {
    /// Parse from a `$.a.b` string (convenience for tests/authoring).
    pub fn parse(s: &str) -> Result<Self, PathParseError> {
        Self::try_from(s.to_string())
    }
}

/// A lossless numeric/type cast mode. Only lossless casts are legal in V1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CastMode {
    Lossless,
}

/// The **closed** V1 transform operator set (design §Closed V1 operator set).
/// Rename/nest/flatten are expressed implicitly by the binding's source→target
/// paths, so they need no operator. Explicitly forbidden (absent here): arbitrary
/// code, free arithmetic, regex, external lookup, concat/split, joins, n:m.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Op {
    /// Lossless type widening (identity or Integer→Decimal).
    Cast { to: ScalarType, mode: CastMode },
    /// Registry-backed unit conversion; both units + the rule id are declared and
    /// verified against [`crate::units`].
    UnitConvert { from: Unit, to: Unit, rule_id: String },
    /// A governance-supplied constant, always flagged synthetic. A synthetic
    /// default can never make a gold field `Required`.
    Default { value: serde_json::Value, synthetic: bool },
    /// Cardinality-preserving element-wise mapping over an array value.
    ArrayMap { element_ops: Vec<Op> },
}

/// What to do when a binding's source path is absent from a child event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Omit the target field (only legal for `Optional` gold fields).
    Omit,
    /// Fail the whole transform (the safe default for required fields).
    Reject,
    /// Fill from a `Default` op in this binding.
    UseDefault,
}

/// What to do when an op fails at runtime (bad cast, unknown rule, …). V1 always
/// rejects — a failed transform never silently drops or coerces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    Reject,
}

/// One source→target field projection with an ordered op chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    pub source: JsonPath,
    pub target: JsonPath,
    #[serde(default)]
    pub ops: Vec<Op>,
    pub on_missing: OnMissing,
    pub on_error: OnError,
}

/// A pinned, executable projection from one child schema into a gold umbrella.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildTransform {
    pub child_schema_id: String,
    pub umbrella_id: String,
    /// Digest-addressed revisions the transform was authored against (design
    /// §provenance/freshness — stale revisions invalidate rather than rebase).
    pub child_revision: String,
    pub umbrella_revision: String,
    pub bindings: Vec<Binding>,
    /// Child paths intentionally not carried into gold (parked, never silently dropped).
    #[serde(default)]
    pub unmapped_source_paths: Vec<JsonPath>,
}

/// The bounded correspondence relation an SLM adjudication may return (design
/// §Prompt 1). Only [`Relation::ExactEquivalent`] and
/// [`Relation::SameQuantityDifferentUnit`] are auto-bindable in V1; the rest are
/// distinct concepts that must not collapse. Defined here so the later SLM slice
/// and the trust gate share one closed vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Relation {
    ExactEquivalent,
    SameQuantityDifferentUnit,
    Narrower,
    Broader,
    Related,
    Disjoint,
    Unknown,
}

impl Relation {
    /// Whether this relation may be automatically bound into a transform in V1.
    pub fn auto_bindable(self) -> bool {
        matches!(self, Relation::ExactEquivalent | Relation::SameQuantityDifferentUnit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_parse_roundtrips() {
        let p = JsonPath::parse("$.main.temp").unwrap();
        assert_eq!(p.0, vec!["main".to_string(), "temp".to_string()]);
        assert_eq!(String::from(p), "$.main.temp");
    }

    #[test]
    fn path_rejects_malformed() {
        assert!(JsonPath::parse("main.temp").is_err()); // no $.
        assert!(JsonPath::parse("$.").is_err()); // empty
        assert!(JsonPath::parse("$.a..b").is_err()); // empty segment
    }

    #[test]
    fn lossless_lattice() {
        assert!(ScalarType::Integer.widens_losslessly_to(ScalarType::Decimal));
        assert!(ScalarType::Decimal.widens_losslessly_to(ScalarType::Decimal));
        assert!(!ScalarType::Decimal.widens_losslessly_to(ScalarType::Integer));
        assert!(!ScalarType::String.widens_losslessly_to(ScalarType::Integer));
    }

    #[test]
    fn only_two_relations_auto_bind() {
        assert!(Relation::ExactEquivalent.auto_bindable());
        assert!(Relation::SameQuantityDifferentUnit.auto_bindable());
        for r in [Relation::Narrower, Relation::Broader, Relation::Related, Relation::Disjoint, Relation::Unknown] {
            assert!(!r.auto_bindable());
        }
    }

    #[test]
    fn op_json_is_tagged() {
        let op = Op::Cast { to: ScalarType::Decimal, mode: CastMode::Lossless };
        let j = serde_json::to_string(&op).unwrap();
        assert!(j.contains("\"op\":\"cast\""));
    }
}
