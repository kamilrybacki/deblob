//! Field pools the schema builder draws from. Every field carries a fixed
//! name and [`FieldKind`] so that two schema families that include the same
//! field always agree on its shape — only *which* fields a family includes
//! varies (see `schema.rs`).

use serde_json::{Map, Value};

/// The structural type of a generated field. Kept small and shape-relevant
/// only (deblob-fingerprint erases concrete values, so field *values*
/// inside `sample_value` are representative filler, not meaningful data).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Str,
    Num,
    Bool,
    /// Array of strings.
    StrArray,
    /// A small nested object with two fixed sub-fields.
    NestedObject,
}

/// A named, typed field a schema family may include.
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub name: &'static str,
    pub kind: FieldKind,
}

/// Folds an arbitrary `seed_hint` into a fixed range so every rendered
/// value has a *constant* text width regardless of how large `seed_hint`
/// grows across a long stream. This matters: the payload-padding pass
/// (`padding.rs`) measures a record's serialized byte length to decide how
/// much filler to add, and a value whose digit count silently grows with
/// the record index would make that length — and therefore the padding
/// field count, and therefore the record's `Shape` — vary record-to-record
/// even within one otherwise-identical schema family. Clamping to a fixed
/// 3-digit range keeps content varied but length constant.
fn fixed_width_hint(seed_hint: usize) -> u32 {
    100 + (seed_hint % 900) as u32
}

impl FieldSpec {
    /// A representative value for this field. Content is filler — only the
    /// structural type matters to the fingerprint — but see
    /// [`fixed_width_hint`] for why its *length* is still fixed.
    pub fn sample_value(&self, seed_hint: usize) -> Value {
        let hint = fixed_width_hint(seed_hint);
        match self.kind {
            FieldKind::Str => Value::String(format!("{}-{hint}", self.name)),
            FieldKind::Num => Value::Number(hint.into()),
            // Fixed, not `hint`-derived: `true`/`false` serialize to
            // different byte lengths (4 vs 5), which would make the
            // padding pass's target-byte-count computation (and thus the
            // record's shape, via filler field count) vary record-to-
            // record within one schema family. See `fixed_width_hint`.
            FieldKind::Bool => Value::Bool(true),
            FieldKind::StrArray => Value::Array(vec![
                Value::String(format!("{}-a", self.name)),
                Value::String(format!("{}-b", self.name)),
            ]),
            FieldKind::NestedObject => {
                let mut m = Map::new();
                m.insert(
                    "id".to_string(),
                    Value::String(format!("{}-id-{hint}", self.name)),
                );
                m.insert("kind".to_string(), Value::String(self.name.to_string()));
                Value::Object(m)
            }
        }
    }

    /// A structurally *widened* value for the same field name, used by
    /// drift generation: a scalar becomes nullable (i.e. its concrete
    /// value is `null`, which deblob-fingerprint still types as `Shape::
    /// Null` — a compatible, if distinct, shape from the base type), and
    /// containers gain one more element/sub-field without changing kind.
    pub fn widened_value(&self, seed_hint: usize) -> Value {
        let hint = fixed_width_hint(seed_hint);
        match self.kind {
            FieldKind::Str | FieldKind::Num | FieldKind::Bool => Value::Null,
            FieldKind::StrArray => Value::Array(vec![
                Value::String(format!("{}-a", self.name)),
                Value::String(format!("{}-b", self.name)),
                Value::String(format!("{}-c-{hint}", self.name)),
            ]),
            FieldKind::NestedObject => {
                let mut m = Map::new();
                m.insert(
                    "id".to_string(),
                    Value::String(format!("{}-id-{hint}", self.name)),
                );
                m.insert("kind".to_string(), Value::String(self.name.to_string()));
                m.insert("note".to_string(), Value::String("drift".to_string()));
                Value::Object(m)
            }
        }
    }
}

/// Fields present in every schema family, giving every record a realistic
/// common backbone regardless of which optional/signature fields it draws.
pub const CORE_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "id",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "kind",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "created_at",
        kind: FieldKind::Num,
    },
];

/// The 20-field signature pool. A schema family's *required* fields are
/// selected from this pool via a bitmask equal to the family's index
/// (`schema.rs`), which guarantees every family index in `0..2^20` maps to
/// a structurally distinct field set. Field kinds alternate across
/// scalars/containers so the resulting shapes are structurally varied, not
/// just differently named.
pub const SIGNATURE_POOL: &[FieldSpec] = &[
    FieldSpec {
        name: "name",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "status",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "count",
        kind: FieldKind::Num,
    },
    FieldSpec {
        name: "amount",
        kind: FieldKind::Num,
    },
    FieldSpec {
        name: "active",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "verified",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "tags",
        kind: FieldKind::StrArray,
    },
    FieldSpec {
        name: "labels",
        kind: FieldKind::StrArray,
    },
    FieldSpec {
        name: "owner",
        kind: FieldKind::NestedObject,
    },
    FieldSpec {
        name: "region",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "priority",
        kind: FieldKind::Num,
    },
    FieldSpec {
        name: "category",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "score",
        kind: FieldKind::Num,
    },
    FieldSpec {
        name: "enabled",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "aliases",
        kind: FieldKind::StrArray,
    },
    FieldSpec {
        name: "metadata",
        kind: FieldKind::NestedObject,
    },
    FieldSpec {
        name: "version",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "url",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "retries",
        kind: FieldKind::Num,
    },
    FieldSpec {
        name: "archived",
        kind: FieldKind::Bool,
    },
];

/// Small pool of fields subject to `optional_field_churn`: each is
/// independently included in a well-formed record with probability
/// `optional_field_churn`, regardless of the record's schema family. These
/// are deliberately disjoint from [`SIGNATURE_POOL`] so churn never
/// changes which family a record belongs to (P1's "same family,
/// optional-field subset" clustering).
pub const CHURN_POOL: &[FieldSpec] = &[
    FieldSpec {
        name: "notes",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "trace_id",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "debug_flag",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "extra_score",
        kind: FieldKind::Num,
    },
];

/// Fields drift may introduce that are never part of `CHURN_POOL` or a
/// family's signature — a genuinely novel optional field, simulating
/// schema evolution.
pub const DRIFT_NOVEL_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "correlation_id",
        kind: FieldKind::Str,
    },
    FieldSpec {
        name: "shadow_metrics",
        kind: FieldKind::NestedObject,
    },
];
