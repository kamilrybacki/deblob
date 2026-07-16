//! The synthetic field pool + deterministic document/value generation used
//! by [`super::families`] and [`super::variants`]. Every random choice here
//! is drawn from a caller-supplied `ChaCha8Rng` — nothing in this module
//! ever touches wall-clock time or thread-local randomness, so the same
//! RNG state always produces the same document (spec §6, determinism).

use rand::Rng;
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

/// One field's static template: a name plus its [`FieldKind`]. `'static`
/// throughout — the whole pool (including nested objects) is plain `const`
/// data, never allocated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub name: &'static str,
    pub kind: FieldKind,
}

/// The shape a [`FieldSpec`] takes. Deliberately small and JSON-primitive —
/// enough variety (scalar types, an array, one level of nesting) to give
/// base families visibly distinct canonical shapes without needing a full
/// schema DSL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// An opaque identifier-shaped string (content is irrelevant to shape).
    StringId,
    /// A string restricted (in the generator, not in real JSON) to one of
    /// a small closed set of values — realistic for a `status`/`type`/
    /// `currency`-style field.
    StringEnum(&'static [&'static str]),
    NumberInt,
    NumberFloat,
    Bool,
    ArrayOfStrings,
    /// One level of nested object. Deliberately not recursive beyond this
    /// — the generator only needs "varied nesting" (spec §2), not
    /// arbitrary depth.
    NestedObject(&'static [FieldSpec]),
}

/// This field's coarse *type* label, ignoring its NAME — the basis of the
/// generator's structural-distance heuristic ([`super::families::jaccard_distance`]),
/// which deliberately mirrors how `deblob-fingerprint`'s canonical `Shape`
/// only ever carries type information, never a value.
pub fn type_label(kind: FieldKind) -> &'static str {
    match kind {
        FieldKind::StringId | FieldKind::StringEnum(_) => "string",
        FieldKind::NumberInt | FieldKind::NumberFloat => "number",
        FieldKind::Bool => "bool",
        FieldKind::ArrayOfStrings => "array",
        FieldKind::NestedObject(_) => "object",
    }
}

/// The sorted multiset of [`type_label`]s across `fields`, recursing one
/// level into any [`FieldKind::NestedObject`]. Field NAMES never
/// contribute — this is a "same shape, different surface" signature, used
/// only to rank distractor closeness for the generated corpus's
/// `retrieved` top-k (see `super::families::jaccard_distance`), not the
/// product's real retrieval algorithm.
pub fn type_signature(fields: &[FieldSpec]) -> Vec<&'static str> {
    let mut out = Vec::new();
    collect_type_signature(fields, &mut out);
    out.sort_unstable();
    out
}

fn collect_type_signature(fields: &[FieldSpec], out: &mut Vec<&'static str>) {
    for f in fields {
        out.push(type_label(f.kind));
        if let FieldKind::NestedObject(nested) = f.kind {
            collect_type_signature(nested, out);
        }
    }
}

const ADDRESS_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "street",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "city",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "zip",
        kind: FieldKind::StringId,
    },
];

const METADATA_FIELDS: &[FieldSpec] = &[
    FieldSpec {
        name: "source",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "revision",
        kind: FieldKind::NumberInt,
    },
];

/// The base pool base families sample their fields from (spec §2: "varied
/// field sets/types/nesting"). 20 entries — plenty of combinatorial room
/// for `--families` in the tens to produce distinct canonical shapes
/// (distinctness is enforced by [`super::families::build_families`], not
/// here).
pub const FIELD_POOL: &[FieldSpec] = &[
    FieldSpec {
        name: "order_id",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "user_id",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "amount",
        kind: FieldKind::NumberFloat,
    },
    FieldSpec {
        name: "status",
        kind: FieldKind::StringEnum(&["pending", "active", "done", "cancelled"]),
    },
    FieldSpec {
        name: "email",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "created_at",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "tags",
        kind: FieldKind::ArrayOfStrings,
    },
    FieldSpec {
        name: "active",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "count",
        kind: FieldKind::NumberInt,
    },
    FieldSpec {
        name: "kind",
        kind: FieldKind::StringEnum(&["alpha", "beta", "gamma"]),
    },
    FieldSpec {
        name: "address",
        kind: FieldKind::NestedObject(ADDRESS_FIELDS),
    },
    FieldSpec {
        name: "priority",
        kind: FieldKind::NumberInt,
    },
    FieldSpec {
        name: "currency",
        kind: FieldKind::StringEnum(&["USD", "EUR", "GBP"]),
    },
    FieldSpec {
        name: "region",
        kind: FieldKind::StringEnum(&["us", "eu", "apac"]),
    },
    FieldSpec {
        name: "quantity",
        kind: FieldKind::NumberInt,
    },
    FieldSpec {
        name: "description",
        kind: FieldKind::StringId,
    },
    FieldSpec {
        name: "category",
        kind: FieldKind::StringEnum(&["hardware", "software", "service"]),
    },
    FieldSpec {
        name: "score",
        kind: FieldKind::NumberFloat,
    },
    FieldSpec {
        name: "verified",
        kind: FieldKind::Bool,
    },
    FieldSpec {
        name: "metadata",
        kind: FieldKind::NestedObject(METADATA_FIELDS),
    },
];

/// Fixed, hand-authored field templates used ONLY by the `new_family`
/// variant (spec §2 "a genuinely different structure"). None of these
/// names appear in [`FIELD_POOL`], so a base family can never collide with
/// one, and their type composition (multiple nested objects / arrays) is
/// deliberately unlike the flatter base pool combinations.
pub const NOVEL_TEMPLATES: &[&[FieldSpec]] = &[
    &[
        FieldSpec {
            name: "shipment_tracking_code",
            kind: FieldKind::StringId,
        },
        FieldSpec {
            name: "carrier_code",
            kind: FieldKind::StringId,
        },
        FieldSpec {
            name: "eta_bucket",
            kind: FieldKind::StringEnum(&["fast", "standard", "slow"]),
        },
    ],
    &[
        FieldSpec {
            name: "sensor_reading_id",
            kind: FieldKind::StringId,
        },
        FieldSpec {
            name: "readings",
            kind: FieldKind::ArrayOfStrings,
        },
        FieldSpec {
            name: "calibration",
            kind: FieldKind::NestedObject(&[
                FieldSpec {
                    name: "offset",
                    kind: FieldKind::NumberFloat,
                },
                FieldSpec {
                    name: "unit",
                    kind: FieldKind::StringEnum(&["c", "f"]),
                },
            ]),
        },
    ],
    &[
        FieldSpec {
            name: "ticket_ref",
            kind: FieldKind::StringId,
        },
        FieldSpec {
            name: "escalation_level",
            kind: FieldKind::NumberInt,
        },
        FieldSpec {
            name: "tags_v2",
            kind: FieldKind::ArrayOfStrings,
        },
        FieldSpec {
            name: "resolved",
            kind: FieldKind::Bool,
        },
    ],
];

/// Coarse numeric magnitude a generated number should land in — feeds
/// [`deblob_monoid::NumericBuckets`] indirectly (the actual bucket is
/// derived from the generated number's text by `deblob-monoid`, never set
/// directly). `Shifted` is used by the `incompatible_similarity` unit-swap
/// variant to make one field's observed magnitude systematically differ
/// from the same family's typical (`Medium`) observations — a visible,
/// legitimate "discriminator" per spec §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MagnitudeBias {
    Medium,
    Shifted,
}

fn gen_int_magnitude(rng: &mut ChaCha8Rng, bias: MagnitudeBias) -> i64 {
    match bias {
        MagnitudeBias::Medium => rng.gen_range(11..=100),
        MagnitudeBias::Shifted => rng.gen_range(1_000..=10_000),
    }
}

fn gen_float_magnitude(rng: &mut ChaCha8Rng, bias: MagnitudeBias) -> f64 {
    let base = gen_int_magnitude(rng, bias) as f64;
    base + 0.5
}

/// Generates one value of `kind`, honoring `bias` for numeric kinds and
/// `allow_null`/`present`. Returns `None` iff `present` is `false` (the
/// field is omitted from its parent object entirely, distinct from an
/// explicit JSON `null`).
pub fn gen_value(
    rng: &mut ChaCha8Rng,
    kind: FieldKind,
    bias: MagnitudeBias,
    allow_null: bool,
    present: bool,
) -> Option<Value> {
    if !present {
        return None;
    }
    if allow_null && rng.gen_bool(0.15) {
        return Some(Value::Null);
    }
    Some(match kind {
        FieldKind::StringId => Value::String(format!("v{:08x}", rng.gen::<u32>())),
        FieldKind::StringEnum(values) => {
            Value::String(values[rng.gen_range(0..values.len())].to_string())
        }
        FieldKind::NumberInt => Value::from(gen_int_magnitude(rng, bias)),
        FieldKind::NumberFloat => Value::from(gen_float_magnitude(rng, bias)),
        FieldKind::Bool => Value::Bool(rng.gen_bool(0.5)),
        FieldKind::ArrayOfStrings => {
            let n = rng.gen_range(0..4);
            Value::Array(
                (0..n)
                    .map(|_| Value::String(format!("t{:04x}", rng.gen::<u16>())))
                    .collect(),
            )
        }
        FieldKind::NestedObject(nested) => {
            let mut map = serde_json::Map::new();
            for f in nested {
                if let Some(v) = gen_value(rng, f.kind, MagnitudeBias::Medium, false, true) {
                    map.insert(f.name.to_string(), v);
                }
            }
            Value::Object(map)
        }
    })
}

/// Builds one JSON document from `fields`. `optional_field`, if set, is
/// omitted from roughly 40% of documents (models a `compatible_drift`
/// "added optional field"). `nullable_field`, if set, is sometimes an
/// explicit JSON `null` instead of its normal value (models "widened
/// nullability"). `biased_field`, if set, uses `biased_bias` instead of
/// [`MagnitudeBias::Medium`] for that one field only (models a
/// unit-swapped/incompatible-similarity discriminator).
#[allow(clippy::too_many_arguments)]
pub fn gen_document(
    rng: &mut ChaCha8Rng,
    fields: &[FieldSpec],
    optional_field: Option<&str>,
    nullable_field: Option<&str>,
    biased_field: Option<&str>,
    biased_bias: MagnitudeBias,
    rename: Option<&dyn Fn(&str) -> String>,
) -> Value {
    let mut map = serde_json::Map::new();
    for f in fields {
        let present = match optional_field {
            Some(name) if name == f.name => rng.gen_bool(0.6),
            _ => true,
        };
        if !present {
            continue;
        }
        let allow_null = matches!(nullable_field, Some(name) if name == f.name);
        let bias = match biased_field {
            Some(name) if name == f.name => biased_bias,
            _ => MagnitudeBias::Medium,
        };
        if let Some(v) = gen_value(rng, f.kind, bias, allow_null, true) {
            let key = match rename {
                Some(f_rename) => f_rename(f.name),
                None => f.name.to_string(),
            };
            map.insert(key, v);
        }
    }
    Value::Object(map)
}

/// A fixed placeholder document built from `fields` with NO randomness —
/// values don't matter to a canonical fingerprint (only types/names do),
/// so a family's identity ([`super::families::compute_family_schema_id`])
/// never needs an RNG.
pub fn placeholder_document(fields: &[FieldSpec]) -> Value {
    let mut map = serde_json::Map::new();
    for f in fields {
        map.insert(f.name.to_string(), placeholder_value(f.kind));
    }
    Value::Object(map)
}

fn placeholder_value(kind: FieldKind) -> Value {
    match kind {
        FieldKind::StringId => Value::String("x".to_string()),
        FieldKind::StringEnum(values) => Value::String(values[0].to_string()),
        FieldKind::NumberInt => Value::from(1),
        FieldKind::NumberFloat => Value::from(1.5),
        FieldKind::Bool => Value::Bool(true),
        FieldKind::ArrayOfStrings => Value::Array(vec![Value::String("x".to_string())]),
        FieldKind::NestedObject(nested) => Value::Object(
            nested
                .iter()
                .map(|f| (f.name.to_string(), placeholder_value(f.kind)))
                .collect(),
        ),
    }
}

/// A deterministic renaming of `name` from `snake_case` to `camelCase`.
pub fn rename_snake_to_camel(name: &str) -> String {
    let mut out = String::new();
    let mut upper_next = false;
    for c in name.chars() {
        if c == '_' {
            upper_next = true;
            continue;
        }
        if upper_next {
            out.extend(c.to_uppercase());
            upper_next = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// A deterministic vendor-prefixed renaming of `name`.
pub fn rename_vendor_prefix(name: &str) -> String {
    format!("vnd_{name}")
}

/// A deterministic abbreviation of `name`: drops internal vowels (keeps the
/// first character and any leading run so the name stays recognizable-ish,
/// which is exactly the "plausible drift" the false-split trap targets).
pub fn rename_abbrev(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if i == 0 || !"aeiou".contains(c) {
            out.push(c);
        }
    }
    if out.is_empty() {
        name.to_string()
    } else {
        out
    }
}
