//! Synthetic training-corpus generator (spec:
//! `docs/superpowers/specs/2026-07-16-slm-corpus-generator.md`).
//!
//! `deblob-eval generate` produces ground-truth-labeled [`crate::corpus::EvalCase`]s
//! at scale, entirely deterministically and WITHOUT an LLM in the loop
//! (spec §7): for a seed set of base "family" schemas (each with a
//! distinct canonical fingerprint, computed via the SAME
//! `deblob-fingerprint`/`deblob-monoid` tools the product uses), it
//! generates variants under six known transformations
//! ([`variants::VariantKind`]) whose `expected` label is set directly by
//! the transformation that produced them (spec §2's table) — never by
//! invoking a matcher or model. Output is byte-compatible with the
//! hand-authored golden corpus (`corpus::load_corpus` loads it with zero
//! errors) and additionally partitions cases by FAMILY into train/holdout
//! (spec §5) and can render a PII-safe fine-tune JSONL export (spec §4)
//! through the exact same `deblob_slm::prompt` builder the shadow lane
//! uses.
//!
//! Determinism (spec §6): every random choice — field sampling, document
//! values, family/schema ids, partition assignment — is drawn from a
//! single `ChaCha8Rng` seeded by `GenerateConfig::seed`, in a fixed call
//! order. No wall-clock time, no `Uuid::now_v7()`, no `HashMap` iteration
//! anywhere in the generation path.

mod families;
mod fields;
mod variants;

use std::collections::HashMap;
use std::path::Path;

use deblob_core::id::SchemaId;
use deblob_fingerprint::{parse_bounded, Limits};
use deblob_monoid::Profile;
use deblob_slm::CandidateProfileView;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde_json::Value;

use crate::corpus::{Category, EvalCase, Expected, Partition};
use families::{build_families, Family};
use variants::{build_blueprint, variant_schedule, CaseBlueprint};

/// Generator knobs — mirrors the `deblob-eval generate` CLI flags 1:1.
#[derive(Debug, Clone, Copy)]
pub struct GenerateConfig {
    /// Number of distinct base family schemas to generate.
    pub families: usize,
    /// Number of variant cases to generate per family (spread across the
    /// six transformation kinds per Hermes' composition target).
    pub variants_per_family: usize,
    /// RNG seed. Same `GenerateConfig` (same `seed` in particular) always
    /// produces byte-identical output.
    pub seed: u64,
}

/// The full output of one [`generate_corpus`] run.
pub struct GeneratedCorpus {
    pub cases: Vec<EvalCase>,
    pub summary: GenerationSummary,
}

/// Case-mix + partition counts, printed by the CLI (spec §6: "Prints the
/// generated case-mix + partition summary").
#[derive(Debug, Clone)]
pub struct GenerationSummary {
    pub total_cases: usize,
    pub families: usize,
    pub by_category: Vec<(Category, usize)>,
    pub by_partition: Vec<(Partition, usize)>,
    pub false_merge_traps: usize,
    pub false_split_traps: usize,
}

/// Generates the full corpus for `cfg`. See the module docs for the
/// determinism contract.
pub fn generate_corpus(cfg: &GenerateConfig) -> GeneratedCorpus {
    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);
    let families = build_families(cfg, &mut rng);

    let mut cases = Vec::new();
    for family in &families {
        let schedule = variant_schedule(cfg.variants_per_family, family.index);
        for (slot, kind) in schedule.into_iter().enumerate() {
            let blueprint = build_blueprint(kind, family, &families, &mut rng);
            let name = format!(
                "gen_{:03}_{:02}_{}",
                family.index, slot, blueprint.name_suffix
            );
            cases.push(finalize_case(name, family, blueprint));
        }
    }

    let summary = summarize(&cases, families.len());
    GeneratedCorpus { cases, summary }
}

/// Merges `docs` into one [`Profile`] via the SAME deterministic
/// `deblob-fingerprint`/`deblob-monoid` pipeline a real endpoint's
/// candidate-cluster ingestion uses — the generated candidate is
/// therefore byte-for-byte what a real `Profile` built from those
/// documents would look like, not a hand-rolled approximation.
fn profile_from_docs(docs: &[Value]) -> Profile {
    docs.iter().fold(Profile::identity(), |acc, doc| {
        let bytes = serde_json::to_vec(doc).expect("generated document always serializes");
        let node = parse_bounded(&bytes, &Limits::default())
            .expect("generated document is always well-formed JSON within limits");
        Profile::merge(&acc, &Profile::from_node(&node))
    })
}

fn finalize_case(name: String, family: &Family, blueprint: CaseBlueprint) -> EvalCase {
    let profile = profile_from_docs(&blueprint.docs);
    let candidate = CandidateProfileView::from_profile(&profile);
    let gold_rank = blueprint.gold_schema_id.as_ref().and_then(|gold_id| {
        blueprint
            .retrieved
            .iter()
            .find(|c| &c.schema_id == gold_id)
            .map(|c| c.rank)
    });

    EvalCase {
        name,
        category: blueprint.category,
        candidate,
        retrieved: blueprint.retrieved,
        expected: Expected {
            decision: blueprint.decision,
            gold_schema_id: blueprint.gold_schema_id,
            gold_rank,
            false_merge_trap: blueprint.false_merge_trap,
            false_split_trap: blueprint.false_split_trap,
        },
        partition: family.partition,
    }
}

fn summarize(cases: &[EvalCase], family_count: usize) -> GenerationSummary {
    let mut cat_counts: HashMap<Category, usize> = HashMap::new();
    let mut part_counts: HashMap<Partition, usize> = HashMap::new();
    let mut false_merge_traps = 0usize;
    let mut false_split_traps = 0usize;
    for c in cases {
        *cat_counts.entry(c.category).or_default() += 1;
        *part_counts.entry(c.partition).or_default() += 1;
        if c.expected.false_merge_trap {
            false_merge_traps += 1;
        }
        if c.expected.false_split_trap {
            false_split_traps += 1;
        }
    }
    let cat_order = [
        Category::KnownExact,
        Category::CompatibleDrift,
        Category::IncompatibleUnsafe,
        Category::NewFamily,
        Category::AmbiguousAdversarial,
    ];
    let by_category = cat_order
        .iter()
        .map(|c| (*c, cat_counts.get(c).copied().unwrap_or(0)))
        .collect();
    let part_order = [Partition::Train, Partition::Test];
    let by_partition = part_order
        .iter()
        .map(|p| (*p, part_counts.get(p).copied().unwrap_or(0)))
        .collect();

    GenerationSummary {
        total_cases: cases.len(),
        families: family_count,
        by_category,
        by_partition,
        false_merge_traps,
        false_split_traps,
    }
}

fn category_label(c: Category) -> &'static str {
    match c {
        Category::KnownExact => "known_exact",
        Category::CompatibleDrift => "compatible_drift",
        Category::IncompatibleUnsafe => "incompatible_unsafe",
        Category::NewFamily => "new_family",
        Category::AmbiguousAdversarial => "ambiguous_adversarial",
    }
}

fn partition_label(p: Partition) -> &'static str {
    match p {
        Partition::Train => "train",
        Partition::Test => "test",
    }
}

/// Renders `summary` as the human-readable case-mix + partition report the
/// CLI prints (spec §6).
pub fn format_summary(summary: &GenerationSummary) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "generated {} case(s) across {} families\n",
        summary.total_cases, summary.families
    ));
    out.push_str("case mix by category:\n");
    for (cat, count) in &summary.by_category {
        let pct = if summary.total_cases == 0 {
            0.0
        } else {
            100.0 * (*count as f64) / (summary.total_cases as f64)
        };
        out.push_str(&format!(
            "  {:<24} {:>5}  ({pct:.1}%)\n",
            category_label(*cat),
            count
        ));
    }
    out.push_str("partition split:\n");
    for (part, count) in &summary.by_partition {
        out.push_str(&format!("  {:<24} {:>5}\n", partition_label(*part), count));
    }
    out.push_str(&format!(
        "traps: false_merge={} false_split={}\n",
        summary.false_merge_traps, summary.false_split_traps
    ));
    out
}

/// Writes each case as a pretty-printed `<seq>_<name>.json` file under
/// `out_dir` (creating it if needed). The zero-padded numeric prefix
/// preserves generation order under `corpus::load_corpus`'s
/// sort-by-filename load order.
pub fn write_corpus(out_dir: &Path, cases: &[EvalCase]) -> std::io::Result<()> {
    std::fs::create_dir_all(out_dir)?;
    for (i, case) in cases.iter().enumerate() {
        let file_name = format!("{i:04}_{}.json", case.name);
        let path = out_dir.join(file_name);
        let json = serde_json::to_string_pretty(case).expect("EvalCase always serializes");
        std::fs::write(path, json)?;
    }
    Ok(())
}

/// Renders the fine-tune export (spec §4): one JSON line per case, each
/// `{case_name, partition, prompt, gold_tool_call}` — `prompt` is the
/// EXACT text `deblob_slm::build_prompt` (the same PII-safe builder the
/// shadow lane uses) renders from the case's already-redacted `candidate`
/// and `retrieved`, and `gold_tool_call` is the case's `expected.decision`
/// serialized in the exact `submit_semantic_decision` tool-call shape
/// (spec §1). Because `prompt` is built from `CandidateProfileView`
/// (stats-only by construction, see `deblob_slm::prompt`'s module docs)
/// and `retrieved` (ids/distances only), no raw payload value can ever
/// appear in a line here — see this module's tests.
pub fn render_finetune_jsonl(cases: &[EvalCase]) -> String {
    let mut out = String::new();
    for case in cases {
        let allowed_ids: Vec<SchemaId> =
            case.retrieved.iter().map(|c| c.schema_id.clone()).collect();
        let prompt = deblob_slm::build_prompt(&case.candidate, &case.retrieved, &allowed_ids);
        let record = serde_json::json!({
            "case_name": case.name,
            "partition": case.partition,
            "prompt": prompt.text,
            "gold_tool_call": serde_json::to_value(&case.expected.decision)
                .expect("InferenceDecision always serializes"),
        });
        out.push_str(&serde_json::to_string(&record).expect("finetune record always serializes"));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::load_corpus;
    use deblob_slm::{InferenceDecision, Relation};
    use std::collections::{HashMap as StdHashMap, HashSet};

    fn small_cfg(seed: u64) -> GenerateConfig {
        GenerateConfig {
            families: 6,
            variants_per_family: 8,
            seed,
        }
    }

    fn family_index_of(case_name: &str) -> &str {
        // "gen_003_02_exact" -> "003"
        case_name
            .split('_')
            .nth(1)
            .expect("case names always have a family-index segment")
    }

    #[test]
    fn generated_cases_load_back_through_the_corpus_loader() {
        let generated = generate_corpus(&small_cfg(1));
        assert_eq!(generated.cases.len(), 6 * 8);

        let dir = std::env::temp_dir().join(format!(
            "deblob-eval-gen-loadtest-{}-{}",
            std::process::id(),
            1
        ));
        write_corpus(&dir, &generated.cases).expect("write_corpus should succeed");
        let loaded = load_corpus(&dir).expect("generated corpus must load back with zero errors");
        assert_eq!(loaded.len(), generated.cases.len());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn every_generated_case_is_self_valid() {
        let generated = generate_corpus(&small_cfg(2));
        for case in &generated.cases {
            case.validate()
                .unwrap_or_else(|e| panic!("generated case {} failed validate(): {e}", case.name));
        }
    }

    #[test]
    fn exact_variant_labels_exact_relation() {
        let generated = generate_corpus(&small_cfg(3));
        let exact_cases: Vec<_> = generated
            .cases
            .iter()
            .filter(|c| c.name.ends_with("_exact"))
            .collect();
        assert!(
            !exact_cases.is_empty(),
            "expected at least one exact-variant case"
        );
        for case in exact_cases {
            match &case.expected.decision {
                InferenceDecision::MatchSchema { relation, .. } => {
                    assert_eq!(*relation, Relation::Exact, "case {}", case.name);
                }
                other => panic!(
                    "case {} expected MatchSchema(Exact), got {other:?}",
                    case.name
                ),
            }
            assert_eq!(case.category, Category::KnownExact);
            assert!(!case.expected.false_merge_trap);
            assert!(!case.expected.false_split_trap);
        }
    }

    #[test]
    fn unit_swap_variant_is_incompatible_similarity_with_false_merge_trap() {
        let generated = generate_corpus(&small_cfg(4));
        let cases: Vec<_> = generated
            .cases
            .iter()
            .filter(|c| c.name.ends_with("_incompatible_unit_swap"))
            .collect();
        assert!(!cases.is_empty(), "expected at least one unit-swap case");
        for case in cases {
            match &case.expected.decision {
                InferenceDecision::MatchSchema { relation, .. } => {
                    assert_eq!(
                        *relation,
                        Relation::IncompatibleSimilarity,
                        "case {}",
                        case.name
                    );
                }
                other => panic!(
                    "case {} expected MatchSchema(IncompatibleSimilarity), got {other:?}",
                    case.name
                ),
            }
            assert!(case.expected.false_merge_trap, "case {}", case.name);
            assert!(!case.expected.false_split_trap, "case {}", case.name);
            assert_eq!(case.category, Category::IncompatibleUnsafe);
            assert!(case.expected.gold_schema_id.is_none());
        }
    }

    #[test]
    fn rename_variant_is_compatible_drift_with_false_split_trap() {
        let generated = generate_corpus(&small_cfg(5));
        let cases: Vec<_> = generated
            .cases
            .iter()
            .filter(|c| c.name.contains("_false_split_"))
            .collect();
        assert!(
            !cases.is_empty(),
            "expected at least one false-split (rename) case"
        );
        for case in cases {
            match &case.expected.decision {
                InferenceDecision::MatchSchema { relation, .. } => {
                    assert_eq!(*relation, Relation::CompatibleDrift, "case {}", case.name);
                }
                other => panic!(
                    "case {} expected MatchSchema(CompatibleDrift), got {other:?}",
                    case.name
                ),
            }
            assert!(case.expected.false_split_trap, "case {}", case.name);
            assert!(!case.expected.false_merge_trap, "case {}", case.name);
            assert_eq!(case.category, Category::CompatibleDrift);
        }
    }

    #[test]
    fn partition_by_family_holds() {
        let generated = generate_corpus(&small_cfg(6));

        // Every case sharing a family index (the "gen_XXX_" prefix) must
        // share the same partition — Hermes' rule (spec §5): never split
        // sibling variants of one family across train/holdout.
        let mut partition_by_family: StdHashMap<&str, Partition> = StdHashMap::new();
        for case in &generated.cases {
            let fam = family_index_of(&case.name);
            match partition_by_family.get(fam) {
                Some(existing) => assert_eq!(
                    *existing, case.partition,
                    "family {fam} has cases split across partitions (case {})",
                    case.name
                ),
                None => {
                    partition_by_family.insert(fam, case.partition);
                }
            }
        }

        // No schema id referenced by a Train case (via `retrieved` or
        // `gold_schema_id`) may also be referenced by a Test case.
        let mut train_ids: HashSet<String> = HashSet::new();
        let mut test_ids: HashSet<String> = HashSet::new();
        for case in &generated.cases {
            let target = match case.partition {
                Partition::Train => &mut train_ids,
                Partition::Test => &mut test_ids,
            };
            for r in &case.retrieved {
                target.insert(r.schema_id.as_str().to_string());
            }
            if let Some(gold) = &case.expected.gold_schema_id {
                target.insert(gold.as_str().to_string());
            }
        }
        let overlap: Vec<_> = train_ids.intersection(&test_ids).collect();
        assert!(
            overlap.is_empty(),
            "schema ids leaked across the generated train/test partition: {overlap:?}"
        );

        assert!(generated
            .cases
            .iter()
            .any(|c| c.partition == Partition::Train));
        assert!(generated
            .cases
            .iter()
            .any(|c| c.partition == Partition::Test));
    }

    #[test]
    fn finetune_jsonl_never_contains_raw_field_values() {
        let generated = generate_corpus(&small_cfg(7));
        let jsonl = render_finetune_jsonl(&generated.cases);
        assert_eq!(jsonl.lines().count(), generated.cases.len());

        // Every string-enum literal in the field pool is a real candidate
        // VALUE, distinct from the escaped field NAMES the prompt builder
        // is allowed to render — it must never appear as a bare quoted
        // token, since `CandidateProfileView` carries stats only (never a
        // raw value) by construction (see `deblob_slm::prompt`).
        for spec in fields::FIELD_POOL {
            if let fields::FieldKind::StringEnum(values) = spec.kind {
                for v in values {
                    if v.len() < 3 {
                        continue; // avoid coincidental short-token false positives
                    }
                    assert!(
                        !jsonl.contains(&format!("\"{v}\"")),
                        "raw enum value {v:?} leaked into the fine-tune JSONL"
                    );
                }
            }
        }
    }

    #[test]
    fn same_seed_produces_byte_identical_output() {
        let a = generate_corpus(&small_cfg(42));
        let b = generate_corpus(&small_cfg(42));
        let a_json: Vec<String> = a
            .cases
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect();
        let b_json: Vec<String> = b
            .cases
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect();
        assert_eq!(a_json, b_json);

        let a_jsonl = render_finetune_jsonl(&a.cases);
        let b_jsonl = render_finetune_jsonl(&b.cases);
        assert_eq!(a_jsonl, b_jsonl);
    }

    #[test]
    fn different_seed_produces_different_output() {
        let a = generate_corpus(&small_cfg(1));
        let b = generate_corpus(&small_cfg(2));
        let a_json = serde_json::to_string(&a.cases).unwrap();
        let b_json = serde_json::to_string(&b.cases).unwrap();
        assert_ne!(a_json, b_json);
    }

    #[test]
    fn case_names_are_unique() {
        let generated = generate_corpus(&small_cfg(9));
        let mut seen = HashSet::new();
        for case in &generated.cases {
            assert!(
                seen.insert(case.name.clone()),
                "duplicate case name: {}",
                case.name
            );
        }
    }

    #[test]
    fn format_summary_reports_every_category_and_partition() {
        let generated = generate_corpus(&small_cfg(10));
        let text = format_summary(&generated.summary);
        for cat in [
            "known_exact",
            "compatible_drift",
            "incompatible_unsafe",
            "new_family",
            "ambiguous_adversarial",
        ] {
            assert!(text.contains(cat), "summary missing category {cat}: {text}");
        }
        assert!(text.contains("train"));
        assert!(text.contains("test"));
    }
}
