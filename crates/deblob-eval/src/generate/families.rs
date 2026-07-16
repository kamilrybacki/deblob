//! Base "family" schema construction: sampling field sets from the pool,
//! computing each family's canonical [`SchemaId`] via the SAME
//! deterministic tools the product uses (`deblob-fingerprint`), assigning
//! a deterministic (never `Uuid::now_v7`-derived) [`FamilyId`], and
//! splitting families into train/holdout partitions BY FAMILY (spec §5 —
//! Hermes' review: never split sibling variants of one family across
//! partitions).

use std::collections::{BTreeMap, BTreeSet};

use deblob_core::id::{FamilyId, SchemaId};
use deblob_fingerprint::{fingerprint as shape_fingerprint, parse_bounded, shape_of, Limits};
use rand::Rng;
use rand_chacha::ChaCha8Rng;

use crate::corpus::Partition;
use crate::generate::fields::{placeholder_document, type_signature, FieldSpec, FIELD_POOL};
use crate::generate::GenerateConfig;

/// One base family: a sampled field template, its canonical identity, and
/// which partition (train/holdout) it — and every variant derived from it
/// — belongs to.
pub struct Family {
    pub index: usize,
    pub family_id: FamilyId,
    pub schema_id: SchemaId,
    pub fields: Vec<FieldSpec>,
    pub signature: Vec<&'static str>,
    pub partition: Partition,
}

/// Builds `cfg.families` distinct base families (distinct canonical
/// `schema_id`s — enforced by resampling on collision) and assigns each a
/// train/holdout partition. Every random choice is drawn from `rng` in a
/// fixed order, so equal `cfg`/`rng` state always yields the identical
/// family set.
pub fn build_families(cfg: &GenerateConfig, rng: &mut ChaCha8Rng) -> Vec<Family> {
    let mut families = Vec::with_capacity(cfg.families);
    let mut used_schema_ids: BTreeSet<String> = BTreeSet::new();

    for i in 0..cfg.families {
        let (fields, schema_id) = loop {
            let fields = sample_fields(rng, i);
            let schema_id = compute_family_schema_id(&fields);
            if used_schema_ids.insert(schema_id.as_str().to_string()) {
                break (fields, schema_id);
            }
            // Collision (rare, only possible with a tiny FIELD_POOL /
            // large --families): loop resamples using the same `rng`
            // stream, so the outcome is still fully determined by seed.
        };
        let signature = type_signature(&fields);
        let family_id = random_family_id(rng);
        families.push(Family {
            index: i,
            family_id,
            schema_id,
            fields,
            signature,
            partition: Partition::Train, // placeholder, set below
        });
    }

    assign_partitions(&mut families, rng);
    families
}

/// Deterministically samples 3-7 fields from [`FIELD_POOL`] for family
/// `family_index`, guaranteeing at least one numeric field (needed by the
/// `incompatible_similarity` unit-swap variant's discriminator).
fn sample_fields(rng: &mut ChaCha8Rng, family_index: usize) -> Vec<FieldSpec> {
    let field_count = 3 + (family_index % 5); // 3..=7
    let mut indices: Vec<usize> = (0..FIELD_POOL.len()).collect();
    for i in (1..indices.len()).rev() {
        let j = rng.gen_range(0..=i);
        indices.swap(i, j);
    }
    let mut selected: Vec<FieldSpec> = indices
        .into_iter()
        .take(field_count)
        .map(|idx| FIELD_POOL[idx])
        .collect();

    if !selected
        .iter()
        .any(|f| crate::generate::fields::type_label(f.kind) == "number")
    {
        let numeric = FIELD_POOL
            .iter()
            .find(|f| crate::generate::fields::type_label(f.kind) == "number")
            .copied()
            .expect("FIELD_POOL always contains at least one numeric field");
        let last = selected.len() - 1;
        selected[last] = numeric;
    }

    selected.sort_by_key(|f| f.name);
    selected
}

/// A family's canonical identity: the [`SchemaId`] of a fixed placeholder
/// document built from `fields`, run through the SAME deterministic
/// canonicalizer (`deblob-fingerprint`) a real endpoint uses. Values never
/// affect a canonical fingerprint (only types/names do), so this needs no
/// RNG.
pub fn compute_family_schema_id(fields: &[FieldSpec]) -> SchemaId {
    let doc = placeholder_document(fields);
    let bytes = serde_json::to_vec(&doc).expect("generated placeholder document always serializes");
    let node = parse_bounded(&bytes, &Limits::default())
        .expect("generated placeholder document is always well-formed JSON within limits");
    let shape = shape_of(&node);
    SchemaId::from_digest(&shape_fingerprint(&shape))
}

/// A deterministic, RNG-derived [`FamilyId`]. Deliberately NOT
/// `FamilyId::new_v7()` (which mints from wall-clock `Uuid::now_v7()` and
/// would break "same seed -> byte-identical corpus", spec §6) — 16 random
/// bytes from `rng` formatted into standard UUID hyphenated hex, which
/// `FamilyId::parse` accepts (it only validates hex/hyphen shape, not
/// version/variant bits).
pub fn random_family_id(rng: &mut ChaCha8Rng) -> FamilyId {
    let bytes: [u8; 16] = rng.gen();
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let s = format!(
        "fam_{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    );
    FamilyId::parse(&s).expect("16 random bytes always format into a syntactically valid UUID")
}

/// Assigns each family a train/holdout [`Partition`] — a deterministic
/// shuffle of family indices, holding out ~20%. Every variant later
/// derived from a family inherits ITS family's partition (spec §5: never
/// split siblings).
fn assign_partitions(families: &mut [Family], rng: &mut ChaCha8Rng) {
    let mut order: Vec<usize> = (0..families.len()).collect();
    for i in (1..order.len()).rev() {
        let j = rng.gen_range(0..=i);
        order.swap(i, j);
    }
    let holdout_count = ((families.len() as f64) * 0.2).round() as usize;
    let holdout: BTreeSet<usize> = order.into_iter().take(holdout_count).collect();
    for f in families.iter_mut() {
        f.partition = if holdout.contains(&f.index) {
            Partition::Test
        } else {
            Partition::Train
        };
    }
}

/// A coarse, symmetric structural-distance heuristic between two type
/// signatures (see [`crate::generate::fields::type_signature`]): 1 minus
/// their multiset Jaccard similarity. `0.0` for identical type
/// composition (e.g. a rename that preserves every field's type), `1.0`
/// for completely disjoint composition. Deliberately field-NAME-blind —
/// this is what makes a renamed (`false_split`) or semantically-swapped
/// (`incompatible_similarity`) variant register as "structurally close"
/// to its true family while a `new_family` candidate registers as far,
/// mirroring spec §3's "ranked by real structural distance".
pub fn jaccard_distance(a: &[&str], b: &[&str]) -> f32 {
    let mut counts_a: BTreeMap<&str, u32> = BTreeMap::new();
    for x in a {
        *counts_a.entry(x).or_insert(0) += 1;
    }
    let mut counts_b: BTreeMap<&str, u32> = BTreeMap::new();
    for x in b {
        *counts_b.entry(x).or_insert(0) += 1;
    }
    let keys: BTreeSet<&str> = counts_a.keys().chain(counts_b.keys()).copied().collect();
    let mut intersection = 0u32;
    let mut union = 0u32;
    for k in keys {
        let ca = *counts_a.get(k).unwrap_or(&0);
        let cb = *counts_b.get(k).unwrap_or(&0);
        intersection += ca.min(cb);
        union += ca.max(cb);
    }
    if union == 0 {
        0.0
    } else {
        1.0 - (intersection as f32 / union as f32)
    }
}

/// The `take` families (excluding `exclude_index`, restricted to
/// `partition`) nearest to `target_signature` by [`jaccard_distance`],
/// ascending, ties broken by `schema_id` string order for determinism.
pub fn nearest_same_partition<'a>(
    all: &'a [Family],
    exclude_index: usize,
    partition: Partition,
    target_signature: &[&'static str],
    take: usize,
) -> Vec<(&'a Family, f32)> {
    let mut scored: Vec<(&Family, f32)> = all
        .iter()
        .filter(|f| f.index != exclude_index && f.partition == partition)
        .map(|f| (f, jaccard_distance(target_signature, &f.signature)))
        .collect();
    scored.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap()
            .then_with(|| a.0.schema_id.as_str().cmp(b.0.schema_id.as_str()))
    });
    scored.into_iter().take(take).collect()
}

/// A deterministic "another family in the same partition" pick, used by
/// the `abstain(ambiguous)` variant to blend two families' fields. Falls
/// back to `family` itself if it has no same-partition peers (only
/// possible with a very small `--families`).
pub fn same_partition_peer<'a>(all: &'a [Family], family: &'a Family, offset: usize) -> &'a Family {
    let peers: Vec<&Family> = all
        .iter()
        .filter(|f| f.index != family.index && f.partition == family.partition)
        .collect();
    if peers.is_empty() {
        family
    } else {
        peers[offset % peers.len()]
    }
}
