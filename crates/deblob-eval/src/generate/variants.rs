//! The six ground-truth-by-construction variant transformations (spec §2's
//! table) — each produces a candidate's observed documents PLUS the
//! `expected` label the transformation itself determines. No LLM, no
//! matcher invocation: the label is set directly by which branch of
//! [`build_blueprint`] ran.

use std::collections::BTreeSet;

use deblob_core::id::SchemaId;
use deblob_slm::{AbstainCause, FamilyCandidate, InferenceDecision, Novelty, Relation};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::corpus::Category;
use crate::generate::families::{
    jaccard_distance, nearest_same_partition, same_partition_peer, Family,
};
use crate::generate::fields::{
    gen_document, rename_abbrev, rename_snake_to_camel, rename_vendor_prefix, type_label,
    type_signature, FieldKind, FieldSpec, MagnitudeBias, FIELD_POOL, NOVEL_TEMPLATES,
};

/// Which of the spec §2 table's six transformations (further split into
/// named sub-flavors for corpus variety) produced a case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantKind {
    Exact,
    CompatibleDriftAddOptional,
    CompatibleDriftWidenNullability,
    CompatibleDriftWrapper,
    CompatibleDriftEnumValue,
    FalseSplitSnakeCamel,
    FalseSplitVendorPrefix,
    FalseSplitAbbrev,
    IncompatibleUnitSwap,
    IncompatibleGenericNames,
    NewFamily,
    AbstainInsufficientEvidence,
    AbstainAmbiguous,
    AbstainCandidateMissing,
}

/// Everything [`crate::generate::finalize_case`] needs to assemble a full
/// `EvalCase` once the candidate's `Profile` has been built from `docs`.
pub struct CaseBlueprint {
    pub docs: Vec<Value>,
    pub decision: InferenceDecision,
    pub gold_schema_id: Option<SchemaId>,
    pub false_merge_trap: bool,
    pub false_split_trap: bool,
    pub category: Category,
    pub retrieved: Vec<FamilyCandidate>,
    pub name_suffix: &'static str,
}

fn gen_obs_count(rng: &mut ChaCha8Rng, lo: u32, hi: u32) -> usize {
    rng.gen_range(lo..=hi) as usize
}

/// Builds `retrieved` = the gold family (at whatever rank its real
/// [`jaccard_distance`] to `candidate_signature` naturally sorts to) plus
/// up to `k - 1` same-partition distractors, nearest first. `k` is
/// deliberately small (matches the product's real top-k, `<= 3` in every
/// golden seed case).
fn build_retrieved_with_gold(
    family: &Family,
    all: &[Family],
    candidate_signature: &[&'static str],
    k: usize,
) -> Vec<FamilyCandidate> {
    let gold_distance = jaccard_distance(candidate_signature, &family.signature);
    let distractors = nearest_same_partition(
        all,
        family.index,
        family.partition,
        candidate_signature,
        k.saturating_sub(1),
    );

    let mut entries: Vec<(deblob_core::id::FamilyId, SchemaId, f32)> = vec![(
        family.family_id.clone(),
        family.schema_id.clone(),
        gold_distance,
    )];
    for (f, d) in distractors {
        entries.push((f.family_id.clone(), f.schema_id.clone(), d));
    }
    entries.sort_by(|a, b| {
        a.2.partial_cmp(&b.2)
            .unwrap()
            .then_with(|| a.1.as_str().cmp(b.1.as_str()))
    });
    entries
        .into_iter()
        .enumerate()
        .map(|(i, (family_id, schema_id, distance))| FamilyCandidate {
            family_id,
            schema_id,
            version: 1,
            distance,
            rank: (i + 1) as u32,
        })
        .collect()
}

/// Builds a `retrieved` list of up to `k` same-partition distractors ONLY
/// — no gold entry — for `new_family` / `abstain(candidate_missing)`
/// cases where there is no correct existing family (or it's deliberately
/// withheld).
fn build_retrieved_distractors_only(
    all: &[Family],
    exclude_index: Option<usize>,
    partition: crate::corpus::Partition,
    target_signature: &[&'static str],
    k: usize,
) -> Vec<FamilyCandidate> {
    let scored = nearest_same_partition(
        all,
        exclude_index.unwrap_or(usize::MAX),
        partition,
        target_signature,
        k,
    );
    scored
        .into_iter()
        .enumerate()
        .map(|(i, (f, d))| FamilyCandidate {
            family_id: f.family_id.clone(),
            schema_id: f.schema_id.clone(),
            version: 1,
            distance: d,
            rank: (i + 1) as u32,
        })
        .collect()
}

/// Picks a field name from [`FIELD_POOL`] not already used by `base`,
/// deterministically offset by `offset` (so different families pick
/// different "new" fields for their `add_optional` drift case).
fn pick_unused_field(base: &[FieldSpec], offset: usize) -> FieldSpec {
    let used: BTreeSet<&str> = base.iter().map(|f| f.name).collect();
    let candidates: Vec<FieldSpec> = FIELD_POOL
        .iter()
        .copied()
        .filter(|f| !used.contains(f.name))
        .collect();
    if candidates.is_empty() {
        // Every pool field is already in use (possible only with a huge
        // field_count) — fall back to re-adding an existing field, which
        // is still a harmless (if less interesting) "drift" candidate.
        base[offset % base.len()]
    } else {
        candidates[offset % candidates.len()]
    }
}

/// Weighted, largest-remainder allocation of `total` slots across
/// Hermes' 5-bucket composition target (25/20/15/20/20 — spec §5,
/// matching `crate::corpus::Category`'s doc comment): `[exact, drift,
/// incompatible, new_family, abstain]`.
fn bucket_counts(total: usize) -> [usize; 5] {
    let weights = [0.25, 0.20, 0.15, 0.20, 0.20];
    let mut counts = [0usize; 5];
    let mut allocated = 0usize;
    let mut remainders = Vec::with_capacity(5);
    for (i, w) in weights.iter().enumerate() {
        let exact = (total as f64) * w;
        let base = exact.floor() as usize;
        counts[i] = base;
        allocated += base;
        remainders.push((exact - base as f64, i));
    }
    let mut remaining = total.saturating_sub(allocated);
    remainders.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap().then(a.1.cmp(&b.1)));
    for (_, i) in remainders {
        if remaining == 0 {
            break;
        }
        counts[i] += 1;
        remaining -= 1;
    }
    counts
}

fn expand_drift(n: usize, family_index: usize) -> Vec<VariantKind> {
    let compat = [
        VariantKind::CompatibleDriftAddOptional,
        VariantKind::CompatibleDriftWidenNullability,
        VariantKind::CompatibleDriftWrapper,
        VariantKind::CompatibleDriftEnumValue,
    ];
    let split = [
        VariantKind::FalseSplitSnakeCamel,
        VariantKind::FalseSplitVendorPrefix,
        VariantKind::FalseSplitAbbrev,
    ];
    (0..n)
        .map(|k| {
            if k % 2 == 0 {
                compat[(family_index + k) % compat.len()]
            } else {
                split[(family_index + k) % split.len()]
            }
        })
        .collect()
}

fn expand_incompatible(n: usize, family_index: usize) -> Vec<VariantKind> {
    let flavors = [
        VariantKind::IncompatibleUnitSwap,
        VariantKind::IncompatibleGenericNames,
    ];
    (0..n)
        .map(|k| flavors[(family_index + k) % flavors.len()])
        .collect()
}

fn expand_abstain(n: usize, family_index: usize) -> Vec<VariantKind> {
    let flavors = [
        VariantKind::AbstainInsufficientEvidence,
        VariantKind::AbstainAmbiguous,
        VariantKind::AbstainCandidateMissing,
    ];
    (0..n)
        .map(|k| flavors[(family_index + k) % flavors.len()])
        .collect()
}

/// The fixed, per-family sequence of [`VariantKind`]s for `m` variants —
/// deterministic in both composition (matches [`bucket_counts`]) and order
/// (block order is always exact/drift/incompatible/new_family/abstain;
/// only sub-flavor cycling varies with `family_index`).
pub fn variant_schedule(m: usize, family_index: usize) -> Vec<VariantKind> {
    let counts = bucket_counts(m);
    let mut out = Vec::with_capacity(m);
    out.extend(std::iter::repeat(VariantKind::Exact).take(counts[0]));
    out.extend(expand_drift(counts[1], family_index));
    out.extend(expand_incompatible(counts[2], family_index));
    out.extend(std::iter::repeat(VariantKind::NewFamily).take(counts[3]));
    out.extend(expand_abstain(counts[4], family_index));
    out
}

/// Builds the full [`CaseBlueprint`] for one `(family, kind)` pair. This
/// is the ONLY place a case's `expected` label is decided — always by
/// which `kind` branch ran, never by invoking any matcher/LLM (spec §7:
/// "No LLM in the loop").
pub fn build_blueprint(
    kind: VariantKind,
    family: &Family,
    all: &[Family],
    rng: &mut ChaCha8Rng,
) -> CaseBlueprint {
    match kind {
        VariantKind::Exact => {
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = family.signature.clone();
            let retrieved = build_retrieved_with_gold(family, all, &signature, 3);
            CaseBlueprint {
                docs,
                decision: InferenceDecision::MatchSchema {
                    schema_id: family.schema_id.clone(),
                    relation: Relation::Exact,
                },
                gold_schema_id: Some(family.schema_id.clone()),
                false_merge_trap: false,
                false_split_trap: false,
                category: Category::KnownExact,
                retrieved,
                name_suffix: "exact",
            }
        }

        VariantKind::CompatibleDriftAddOptional => {
            let extra = pick_unused_field(&family.fields, family.index);
            let mut ext_fields = family.fields.clone();
            ext_fields.push(extra);
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &ext_fields,
                        Some(extra.name),
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = type_signature(&ext_fields);
            drift_match(family, all, docs, signature, "drift_add_optional")
        }

        VariantKind::CompatibleDriftWidenNullability => {
            let target = family.fields[0];
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        Some(target.name),
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = family.signature.clone();
            drift_match(family, all, docs, signature, "drift_widen_nullability")
        }

        VariantKind::CompatibleDriftWrapper => {
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    let inner = gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    );
                    let mut meta = serde_json::Map::new();
                    meta.insert("source".to_string(), Value::String("gen".to_string()));
                    let mut outer = serde_json::Map::new();
                    outer.insert("data".to_string(), inner);
                    outer.insert("meta".to_string(), Value::Object(meta));
                    Value::Object(outer)
                })
                .collect();
            let mut signature = family.signature.clone();
            signature.push("object");
            signature.push("object");
            signature.push("string");
            signature.sort_unstable();
            drift_match(family, all, docs, signature, "drift_wrapper")
        }

        VariantKind::CompatibleDriftEnumValue => {
            let target_enum = family
                .fields
                .iter()
                .find(|f| matches!(f.kind, FieldKind::StringEnum(_)))
                .copied();
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    let mut doc = gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    );
                    if let (Some(target), Value::Object(map)) = (target_enum, &mut doc) {
                        if rng.gen_bool(0.2) {
                            map.insert(
                                target.name.to_string(),
                                Value::String("new_enum_value".to_string()),
                            );
                        }
                    }
                    doc
                })
                .collect();
            let signature = family.signature.clone();
            drift_match(family, all, docs, signature, "drift_enum_value")
        }

        VariantKind::FalseSplitSnakeCamel => false_split(
            family,
            all,
            rng,
            &rename_snake_to_camel,
            "false_split_snake_camel",
        ),
        VariantKind::FalseSplitVendorPrefix => false_split(
            family,
            all,
            rng,
            &rename_vendor_prefix,
            "false_split_vendor_prefix",
        ),
        VariantKind::FalseSplitAbbrev => {
            false_split(family, all, rng, &rename_abbrev, "false_split_abbrev")
        }

        VariantKind::IncompatibleUnitSwap => {
            let numeric_field = family
                .fields
                .iter()
                .find(|f| type_label(f.kind) == "number")
                .copied()
                .expect("every family has >=1 numeric field (families::sample_fields)");
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        Some(numeric_field.name),
                        MagnitudeBias::Shifted,
                        None,
                    )
                })
                .collect();
            let lookalike_family_id = crate::generate::families::random_family_id(rng);
            let retrieved = vec![FamilyCandidate {
                family_id: lookalike_family_id,
                schema_id: family.schema_id.clone(),
                version: 1,
                distance: 0.01,
                rank: 1,
            }];
            CaseBlueprint {
                docs,
                decision: InferenceDecision::MatchSchema {
                    schema_id: family.schema_id.clone(),
                    relation: Relation::IncompatibleSimilarity,
                },
                gold_schema_id: None,
                false_merge_trap: true,
                false_split_trap: false,
                category: Category::IncompatibleUnsafe,
                retrieved,
                name_suffix: "incompatible_unit_swap",
            }
        }

        VariantKind::IncompatibleGenericNames => {
            let obs = gen_obs_count(rng, 20, 90);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = family.signature.clone();
            let neighbor =
                nearest_same_partition(all, family.index, family.partition, &signature, 1)
                    .into_iter()
                    .next();
            let (lookalike_family_id, lookalike_schema_id, distance) = match neighbor {
                Some((f, d)) => (f.family_id.clone(), f.schema_id.clone(), d.max(0.02)),
                None => (
                    crate::generate::families::random_family_id(rng),
                    family.schema_id.clone(),
                    0.02,
                ),
            };
            let retrieved = vec![FamilyCandidate {
                family_id: lookalike_family_id,
                schema_id: lookalike_schema_id.clone(),
                version: 1,
                distance,
                rank: 1,
            }];
            CaseBlueprint {
                docs,
                decision: InferenceDecision::MatchSchema {
                    schema_id: lookalike_schema_id,
                    relation: Relation::IncompatibleSimilarity,
                },
                gold_schema_id: None,
                false_merge_trap: true,
                false_split_trap: false,
                category: Category::IncompatibleUnsafe,
                retrieved,
                name_suffix: "incompatible_generic_names",
            }
        }

        VariantKind::NewFamily => {
            let template = NOVEL_TEMPLATES[family.index % NOVEL_TEMPLATES.len()];
            let obs = gen_obs_count(rng, 15, 60);
            let docs = (0..obs)
                .map(|_| gen_document(rng, template, None, None, None, MagnitudeBias::Medium, None))
                .collect();
            let signature = type_signature(template);
            let retrieved =
                build_retrieved_distractors_only(all, None, family.partition, &signature, 2);
            let novelty = if family.index % 2 == 0 {
                Novelty::Structural
            } else {
                Novelty::Semantic
            };
            CaseBlueprint {
                docs,
                decision: InferenceDecision::NewCandidate { novelty },
                gold_schema_id: None,
                false_merge_trap: false,
                false_split_trap: false,
                category: Category::NewFamily,
                retrieved,
                name_suffix: "new_family",
            }
        }

        VariantKind::AbstainInsufficientEvidence => {
            let obs = gen_obs_count(rng, 1, 3);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = family.signature.clone();
            let retrieved = build_retrieved_with_gold(family, all, &signature, 1);
            CaseBlueprint {
                docs,
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::InsufficientEvidence,
                },
                gold_schema_id: None,
                false_merge_trap: false,
                false_split_trap: false,
                category: Category::AmbiguousAdversarial,
                retrieved,
                name_suffix: "abstain_insufficient_evidence",
            }
        }

        VariantKind::AbstainAmbiguous => {
            let other = same_partition_peer(all, family, family.index + 1);
            let mut mixed_fields: Vec<FieldSpec> = family
                .fields
                .iter()
                .take(family.fields.len().div_ceil(2))
                .copied()
                .collect();
            for f in other.fields.iter().take(other.fields.len().div_ceil(2)) {
                if !mixed_fields.iter().any(|mf| mf.name == f.name) {
                    mixed_fields.push(*f);
                }
            }
            if mixed_fields.is_empty() {
                mixed_fields = family.fields.clone();
            }
            let obs = gen_obs_count(rng, 20, 60);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &mixed_fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = type_signature(&mixed_fields);
            let da = jaccard_distance(&signature, &family.signature);
            let db = jaccard_distance(&signature, &other.signature);
            let tie = (da + db) / 2.0;
            let (first, second) = if family.schema_id.as_str() <= other.schema_id.as_str() {
                (family, other)
            } else {
                (other, family)
            };
            let retrieved = vec![
                FamilyCandidate {
                    family_id: first.family_id.clone(),
                    schema_id: first.schema_id.clone(),
                    version: 1,
                    distance: tie,
                    rank: 1,
                },
                FamilyCandidate {
                    family_id: second.family_id.clone(),
                    schema_id: second.schema_id.clone(),
                    version: 1,
                    distance: tie,
                    rank: 2,
                },
            ];
            CaseBlueprint {
                docs,
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::Ambiguous,
                },
                gold_schema_id: None,
                false_merge_trap: false,
                false_split_trap: false,
                category: Category::AmbiguousAdversarial,
                retrieved,
                name_suffix: "abstain_ambiguous",
            }
        }

        VariantKind::AbstainCandidateMissing => {
            let obs = gen_obs_count(rng, 20, 60);
            let docs = (0..obs)
                .map(|_| {
                    gen_document(
                        rng,
                        &family.fields,
                        None,
                        None,
                        None,
                        MagnitudeBias::Medium,
                        None,
                    )
                })
                .collect();
            let signature = family.signature.clone();
            let retrieved = build_retrieved_distractors_only(
                all,
                Some(family.index),
                family.partition,
                &signature,
                2,
            );
            CaseBlueprint {
                docs,
                decision: InferenceDecision::Abstain {
                    cause: AbstainCause::CandidateMissing,
                },
                gold_schema_id: Some(family.schema_id.clone()),
                false_merge_trap: false,
                false_split_trap: false,
                category: Category::AmbiguousAdversarial,
                retrieved,
                name_suffix: "abstain_candidate_missing",
            }
        }
    }
}

/// Shared tail for the four `compatible_drift` sub-flavors: same
/// accepted-match decision shape, only `docs`/`signature`/`name_suffix`
/// differ.
fn drift_match(
    family: &Family,
    all: &[Family],
    docs: Vec<Value>,
    signature: Vec<&'static str>,
    name_suffix: &'static str,
) -> CaseBlueprint {
    let retrieved = build_retrieved_with_gold(family, all, &signature, 3);
    CaseBlueprint {
        docs,
        decision: InferenceDecision::MatchSchema {
            schema_id: family.schema_id.clone(),
            relation: Relation::CompatibleDrift,
        },
        gold_schema_id: Some(family.schema_id.clone()),
        false_merge_trap: false,
        false_split_trap: false,
        category: Category::CompatibleDrift,
        retrieved,
        name_suffix,
    }
}

/// Shared body for the three `false_split` (rename) sub-flavors: same
/// field TYPES as `family`, every NAME run through `rename` — the
/// candidate's structural signature is therefore identical to `family`'s
/// own (renaming never changes a type), so it registers as maximally
/// close in [`build_retrieved_with_gold`] despite the surface difference
/// — exactly the "the model MUST recognize it as same-family" case spec
/// §2 describes.
fn false_split(
    family: &Family,
    all: &[Family],
    rng: &mut ChaCha8Rng,
    rename: &dyn Fn(&str) -> String,
    name_suffix: &'static str,
) -> CaseBlueprint {
    let obs = gen_obs_count(rng, 20, 90);
    let docs = (0..obs)
        .map(|_| {
            gen_document(
                rng,
                &family.fields,
                None,
                None,
                None,
                MagnitudeBias::Medium,
                Some(rename),
            )
        })
        .collect();
    let signature = family.signature.clone();
    let retrieved = build_retrieved_with_gold(family, all, &signature, 3);
    CaseBlueprint {
        docs,
        decision: InferenceDecision::MatchSchema {
            schema_id: family.schema_id.clone(),
            relation: Relation::CompatibleDrift,
        },
        gold_schema_id: Some(family.schema_id.clone()),
        false_merge_trap: false,
        false_split_trap: true,
        category: Category::CompatibleDrift,
        retrieved,
        name_suffix,
    }
}
