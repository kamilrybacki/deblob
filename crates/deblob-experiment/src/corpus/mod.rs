//! Real-corpus ingestion (spec §6b): GitHub Archive + Wikimedia EventStreams
//! samples -> `deblob_eval::EvalCase`, the SAME shape the synthetic
//! generator (`deblob_eval::generate_corpus`) produces and
//! [`crate::labels::split_case`]/`split_corpus` already know how to
//! leak-strip. Loaders in this module therefore reuse Task 1's leak guard
//! VERBATIM rather than reimplementing it: they never construct an
//! [`crate::labels::InferenceInput`] or a [`crate::labels::GoldSidecar`]
//! directly — they build `EvalCase`s (`name`/`category`/`candidate`
//! /`retrieved`/`expected`/`partition`), and the SAME `split_case` Task 1
//! tested for the synthetic corpus does the stripping.
//!
//! ## Building a structural profile from a real event payload
//!
//! [`profile_from_json`] pipes a `serde_json::Value` through the SAME
//! parser + monoid the product's ingest path uses
//! (`deblob_fingerprint::parse_bounded` -> `deblob_monoid::Profile::from_node`)
//! — never a hand-rolled reimplementation. Only ever called on a record's
//! `payload`-equivalent sub-value (never the full envelope), so
//! type-revealing envelope fields (`type`, `$schema`, `meta.stream`, repo/org
//! identifiers, ...) never enter the profile, the redacted
//! `CandidateProfileView`, or the rendered prompt in the first place — the
//! leak guard in `labels.rs` is a second, independent line of defense on
//! top of this structural exclusion, not the only one.
//!
//! ## Real structural-distance retrieval over ingested families
//!
//! [`retrieve_over_pool`] calls `deblob::retrieval::retrieve_topk` — the
//! EXACT six-component weighted structural-distance ranker Task 1's A0/A1
//! arms and the product's shadow lane use — never a reimplementation.
//! `retrieve_topk` takes a `&dyn deblob_core::ports::Registry`; the product
//! implementation is Redis-backed, which this offline, fixture-driven crate
//! has no business depending on. [`FixturePoolRegistry`] is a minimal,
//! in-memory `Registry` that answers `list_families_by_band_depth` (the
//! only method `retrieve_topk` calls) with the WHOLE ingested pool,
//! ignoring the `bands`/`depths` arguments. This is a disclosed
//! simplification: band/depth bucketing is a discovery-scale optimization
//! (finding candidates worth scoring among millions of registered
//! schemas) — irrelevant for the handful of families a hand-authored
//! fixture set ever contains. The actual per-pair distance scoring, the
//! family-representative dedup, and the top-k ranking (all private to
//! `deblob::retrieval`, reached only through the public `retrieve_topk`
//! entry point) run completely unmodified.

use std::collections::BTreeMap;

use async_trait::async_trait;
use deblob_core::error::CoreError;
use deblob_core::id::{FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{FamilyRecord, FamilyRef, Registry, SchemaRecord};
use deblob_eval::{Category, EvalCase};
use deblob_fingerprint::{parse_bounded, Limits};
use deblob_monoid::Profile;
use deblob_slm::{AbstainCause, FamilyCandidate, InferenceDecision, Relation};
use sha2::{Digest, Sha256};

pub mod github_archive;
pub mod pairs;
pub mod tiers;
pub mod wikimedia;

/// Failures while turning a fixture record into ingestible structural data.
/// Never a leak vector: variant payloads carry only byte offsets / field
/// names, never the source-native label fields this crate strips.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("failed to parse fixture JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "fixture record's payload was quarantined while building its structural profile: {0:?}"
    )]
    Quarantined(deblob_core::error::QuarantineReason),
    #[error("fixture record missing required field `{0}`")]
    MissingField(&'static str),
    #[error("fixture record's `{0}` field had an unexpected shape")]
    MalformedField(&'static str),
}

/// Builds a [`Profile`] from one JSON value via the product's real parser +
/// monoid — see this module's doc comment. Callers MUST pass only the
/// non-label-bearing sub-value (a GitHub event's `payload`, a Wikimedia
/// event stripped of `$schema`/`meta`) — this function has no opinion about
/// which fields are safe; that's the caller's job (`github_archive`/
/// `wikimedia`).
pub fn profile_from_json(value: &serde_json::Value) -> Result<Profile, IngestError> {
    let bytes = serde_json::to_vec(value)?;
    let node = parse_bounded(&bytes, &Limits::default()).map_err(IngestError::Quarantined)?;
    Ok(Profile::from_node(&node))
}

/// Deterministically mints a [`FamilyId`] from a stable seed string (e.g.
/// `"github:PushEvent"`, `"wikimedia:mediawiki.page-create"`) — unlike
/// [`FamilyId::new_v7`] (time-ordered, non-reproducible), ingestion MUST
/// produce byte-identical `EvalCase`s across runs over the same fixtures
/// (spec §6: "deterministic: fixture ingestion ... is seed/order-stable").
/// `sha256(seed)`'s first 16 bytes become a UUID body; `FamilyId::parse`
/// only checks the string is a well-formed UUID (any version), so this
/// never fails for a non-empty digest.
pub fn deterministic_family_id(seed: &str) -> FamilyId {
    let digest = Sha256::digest(seed.as_bytes());
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&digest[..16]);
    let uuid = uuid::Uuid::from_bytes(uuid_bytes);
    FamilyId::parse(&format!("fam_{uuid}")).expect("sha256-derived UUID body always parses")
}

/// A schema id derived deterministically from a [`Profile`]'s generalized
/// structural fingerprint — the same identity domain the product's
/// promotion path (`Profile::generalized_fingerprint`) uses.
pub fn schema_id_of(profile: &Profile) -> SchemaId {
    SchemaId::from_digest(&profile.generalized_fingerprint())
}

/// One ingested record, reduced to exactly what family-pool construction
/// needs: which source-native family it belongs to (evaluator-only —
/// never leaked further than this pooling step), a deterministic ordering
/// key (so pooling/tiering is never wall-clock- or HashMap-order-
/// dependent), and its own structural profile.
#[derive(Debug, Clone)]
pub struct FamilyMember {
    pub family_name: String,
    pub order_key: String,
    pub profile: Profile,
}

/// The result of pooling a set of [`FamilyMember`]s into distinct
/// `(family, version)` schema entries: one [`FamilyRef`] per distinct
/// structural fingerprint observed for a given `family_name`, in
/// first-chronological-appearance order (so a genuine schema-evolution
/// step — e.g. Wikimedia's `$schema` version bump — becomes `version: 2`
/// of the SAME `family_id`, never a new unrelated family).
#[derive(Debug, Clone)]
pub struct FamilyPool {
    /// Every distinct `(family_name, fingerprint)` discovered, in stable
    /// order — the retrieval pool every ingested record is scored against.
    pub families: Vec<FamilyRef>,
    /// `(family_name, fingerprint) -> schema_id`, the gold lookup each
    /// loader uses to find ITS OWN record's true schema id in `families`.
    pub gold_schema_of: BTreeMap<(String, [u8; 32]), SchemaId>,
}

/// Pools `members` into a [`FamilyPool`]. Deterministic and NEVER random:
/// members are sorted by `(family_name, order_key)` before de-duplication,
/// so the same input always yields the same version assignment regardless
/// of caller-side iteration order (spec §6: "chronological ... split").
pub fn build_family_pool(members: &[FamilyMember]) -> FamilyPool {
    let mut sorted: Vec<&FamilyMember> = members.iter().collect();
    sorted.sort_by(|a, b| {
        a.family_name
            .cmp(&b.family_name)
            .then_with(|| a.order_key.cmp(&b.order_key))
    });

    let mut families = Vec::new();
    let mut gold_schema_of = BTreeMap::new();
    let mut next_version: BTreeMap<String, u32> = BTreeMap::new();

    for member in sorted {
        let fingerprint = member.profile.generalized_fingerprint();
        let key = (member.family_name.clone(), fingerprint);
        if gold_schema_of.contains_key(&key) {
            continue;
        }
        let family_id = deterministic_family_id(&member.family_name);
        let version = next_version.entry(member.family_name.clone()).or_insert(0);
        *version += 1;
        let schema_id = SchemaId::from_digest(&fingerprint);

        families.push(FamilyRef {
            family_id,
            schema_id: schema_id.clone(),
            version: FamilyVersion(*version),
            canonical: member.profile.generalized_canonical_json(),
        });
        gold_schema_of.insert(key, schema_id);
    }

    FamilyPool {
        families,
        gold_schema_of,
    }
}

impl FamilyPool {
    /// The [`FamilyId`] a given `schema_id` belongs to, if it was minted by
    /// this pool. Lets a downstream analysis (`pairs::tag_pairs`) recover
    /// "these two cases are different VERSIONS of the same family" from
    /// nothing but each case's `expected.gold_schema_id` — the one piece of
    /// family lineage every [`EvalCase`] is allowed to carry (it is, after
    /// all, the gold answer, not a leak into the model-facing input).
    pub fn family_id_of(&self, schema_id: &SchemaId) -> Option<FamilyId> {
        self.families
            .iter()
            .find(|f| &f.schema_id == schema_id)
            .map(|f| f.family_id.clone())
    }
}

/// A loader's full output: the ingested [`EvalCase`]s plus the
/// [`FamilyPool`] they were scored against. The pool is NOT leaked into any
/// case — it is evaluator/analysis-only plumbing (`pairs`/`tiers` consume
/// it directly; no `Arm` ever sees it) that lets downstream difficult-pair
/// tagging and tiering recover family lineage without re-deriving it from
/// the (never persisted) source records.
#[derive(Debug, Clone)]
pub struct IngestedCorpus {
    pub cases: Vec<EvalCase>,
    pub pool: FamilyPool,
}

/// The shared match/category heuristic both loaders (`github_archive`,
/// `wikimedia`) use to turn "where (if anywhere) did the gold schema land
/// in the real retrieved top-k" into a 3-way [`InferenceDecision`] +
/// [`Category`] — kept in one place so both loaders' gold-labeling logic is
/// visibly identical, not two independently-drifting copies.
///
/// - Gold retrieved at rank 1, near-zero distance → `Exact` / `KnownExact`.
/// - Gold retrieved at rank 1, nonzero distance → `CompatibleDrift` /
///   `CompatibleDrift` (the record structurally matches its OWN pooled
///   version most closely, but not perfectly — e.g. an optional field
///   present in one observation and not another).
/// - Gold retrieved but NOT at rank 1 → `Abstain{Ambiguous}` /
///   `AmbiguousAdversarial` (retrieval didn't clearly resolve it — spec §6's
///   "value-shape-drift"/"envelope-same-payload-different" difficult-pair
///   territory).
/// - Gold not retrieved at all → `Abstain{CandidateMissing}` /
///   `AmbiguousAdversarial` — the spec §6 "gold-absent-from-topk" difficult
///   pair class.
pub fn decide_from_gold_candidate(
    gold_schema_id: &SchemaId,
    gold_candidate: Option<&FamilyCandidate>,
) -> (InferenceDecision, Category) {
    match gold_candidate {
        Some(c) if c.rank == 1 && c.distance <= 1e-6 => (
            InferenceDecision::MatchSchema {
                schema_id: gold_schema_id.clone(),
                relation: Relation::Exact,
            },
            Category::KnownExact,
        ),
        Some(c) if c.rank == 1 => (
            InferenceDecision::MatchSchema {
                schema_id: gold_schema_id.clone(),
                relation: Relation::CompatibleDrift,
            },
            Category::CompatibleDrift,
        ),
        Some(_) => (
            InferenceDecision::Abstain {
                cause: AbstainCause::Ambiguous,
            },
            Category::AmbiguousAdversarial,
        ),
        None => (
            InferenceDecision::Abstain {
                cause: AbstainCause::CandidateMissing,
            },
            Category::AmbiguousAdversarial,
        ),
    }
}

/// A minimal, in-memory `Registry` over a fixed [`FamilyPool`] — see this
/// module's doc comment for why `list_families_by_band_depth` returning the
/// whole pool (ignoring `bands`/`depths`) is a sound, disclosed
/// simplification for fixture-scale ingestion. Every other `Registry`
/// method is unreachable from `retrieve_topk`'s call path and panics if
/// called, mirroring the product's own test fakes
/// (`deblob::retrieval`'s `cfg(test)` fakes, which this type cannot import
/// since they are private to that crate).
struct FixturePoolRegistry<'a> {
    families: &'a [FamilyRef],
}

#[async_trait]
impl Registry for FixturePoolRegistry<'_> {
    async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
        unimplemented!("ingestion retrieval never reads a schema by id directly")
    }
    async fn resolve_structural(
        &self,
        _bucket_key: &str,
        _fingerprint: &SchemaId,
    ) -> Result<Option<SchemaId>, CoreError> {
        unimplemented!("ingestion retrieval never resolves the hot-path exact index")
    }
    async fn publish(
        &self,
        _record: SchemaRecord,
        _alias_from: &deblob_core::id::CandidateId,
        _bucket_key: &str,
        _variant_members: &[(String, String)],
        _actor: &str,
        _reason: &str,
    ) -> Result<FamilyVersion, CoreError> {
        unimplemented!("ingestion retrieval never publishes")
    }
    async fn get_alias(
        &self,
        _id: &deblob_core::id::CandidateId,
    ) -> Result<Option<SchemaId>, CoreError> {
        unimplemented!("ingestion retrieval never resolves aliases")
    }
    async fn list_schemas(
        &self,
        _cursor: Option<String>,
        _limit: usize,
    ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
        unimplemented!("ingestion retrieval never lists all schemas")
    }
    async fn list_families_in_buckets(
        &self,
        _bucket_keys: &[String],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        unimplemented!("ingestion retrieval only ever calls list_families_by_band_depth")
    }
    async fn list_families_by_band_depth(
        &self,
        _bands: &[u32],
        _depths: &[u32],
    ) -> Result<Vec<FamilyRef>, CoreError> {
        Ok(self.families.to_vec())
    }
    async fn family_version_schema(
        &self,
        _family_id: &FamilyId,
        _version: FamilyVersion,
    ) -> Result<Option<SchemaId>, CoreError> {
        unimplemented!("ingestion retrieval never resolves a specific family version")
    }
    async fn get_family(&self, _family_id: &FamilyId) -> Result<Option<FamilyRecord>, CoreError> {
        unimplemented!("ingestion retrieval never reads family records")
    }
    async fn list_family_versions(
        &self,
        _family_id: &FamilyId,
    ) -> Result<Vec<FamilyVersion>, CoreError> {
        unimplemented!("ingestion retrieval never lists family versions")
    }
}

/// Retrieves the real deterministic top-`k` structural-distance candidates
/// for `profile` against `pool` — see this module's doc comment.
/// `futures_executor::block_on` bridges `retrieve_topk`'s `async fn` (the
/// SAME sync bridge Task 1's `SemanticArm` already uses for
/// `SemanticInferencer::classify`); the in-memory registry never actually
/// awaits, so this is a single, immediate poll, not a blocking wait.
pub fn retrieve_over_pool(
    profile: &Profile,
    pool: &[FamilyRef],
    k: usize,
) -> deblob::retrieval::RetrievalResult {
    let registry = FixturePoolRegistry { families: pool };
    futures_executor::block_on(deblob::retrieval::retrieve_topk(profile, &registry, k))
        .expect("the in-memory FixturePoolRegistry never returns an error")
}

/// Fails deterministically (never panics on malformed but well-typed
/// input) if `value` is not an object, so a loader can produce a clean
/// [`IngestError`] instead of a `serde_json` type-mismatch panic.
pub(crate) fn require_object<'a>(
    value: &'a serde_json::Value,
    field: &'static str,
) -> Result<&'a serde_json::Map<String, serde_json::Value>, IngestError> {
    value.as_object().ok_or(IngestError::MalformedField(field))
}

pub(crate) fn require_str<'a>(
    value: &'a serde_json::Value,
    field: &'static str,
) -> Result<&'a str, IngestError> {
    value.as_str().ok_or(IngestError::MalformedField(field))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_of(json: &str) -> Profile {
        profile_from_json(&serde_json::from_str(json).unwrap()).unwrap()
    }

    #[test]
    fn deterministic_family_id_is_stable_across_calls() {
        let a = deterministic_family_id("github:PushEvent");
        let b = deterministic_family_id("github:PushEvent");
        assert_eq!(a, b);
        assert!(a.as_str().starts_with("fam_"));
    }

    #[test]
    fn deterministic_family_id_differs_by_seed() {
        let a = deterministic_family_id("github:PushEvent");
        let b = deterministic_family_id("github:PullRequestEvent");
        assert_ne!(a, b);
    }

    #[test]
    fn build_family_pool_assigns_versions_in_chronological_order() {
        let v1 = profile_of(r#"{"a": 1}"#);
        let v2 = profile_of(r#"{"a": 1, "b": "new"}"#);
        let members = vec![
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "2020-01-02".to_string(),
                profile: v2.clone(),
            },
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "2020-01-01".to_string(),
                profile: v1.clone(),
            },
        ];
        let pool = build_family_pool(&members);
        assert_eq!(pool.families.len(), 2);
        // v1 (earlier order_key) must be version 1, v2 version 2 —
        // regardless of the members Vec's own order above.
        let v1_ref = pool
            .families
            .iter()
            .find(|f| f.schema_id == schema_id_of(&v1))
            .unwrap();
        let v2_ref = pool
            .families
            .iter()
            .find(|f| f.schema_id == schema_id_of(&v2))
            .unwrap();
        assert_eq!(v1_ref.version, FamilyVersion(1));
        assert_eq!(v2_ref.version, FamilyVersion(2));
        assert_eq!(v1_ref.family_id, v2_ref.family_id);
    }

    #[test]
    fn build_family_pool_deduplicates_identical_structural_fingerprints() {
        let members = vec![
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "1".to_string(),
                profile: profile_of(r#"{"a": 1}"#),
            },
            FamilyMember {
                family_name: "stream.x".to_string(),
                order_key: "2".to_string(),
                profile: profile_of(r#"{"a": 2}"#), // same structural shape
            },
        ];
        let pool = build_family_pool(&members);
        assert_eq!(pool.families.len(), 1);
    }

    #[test]
    fn retrieve_over_pool_finds_the_nearer_of_two_distinct_families() {
        let near = profile_of(r#"{"a": 1, "b": "x"}"#);
        let far = profile_of(r#"{"totally": {"different": {"shape": [1, 2, 3]}}}"#);
        let members = vec![
            FamilyMember {
                family_name: "near".to_string(),
                order_key: "1".to_string(),
                profile: near.clone(),
            },
            FamilyMember {
                family_name: "far".to_string(),
                order_key: "1".to_string(),
                profile: far,
            },
        ];
        let pool = build_family_pool(&members);
        let query = profile_of(r#"{"a": 2, "b": "y"}"#); // structurally == near
        let result = retrieve_over_pool(&query, &pool.families, 2);
        assert_eq!(
            result.candidates.first().unwrap().schema_id,
            schema_id_of(&near)
        );
    }
}
