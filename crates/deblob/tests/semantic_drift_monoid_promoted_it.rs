//! Whole-branch review fix: `deblob::semantic_drift`'s two orchestrators
//! (`check_family_version_drift`, `scan_semantic_collisions`) against
//! schemas published through REAL candidate promotion
//! (`deblob::coldlane::ColdLane::ingest` -> `deblob::policy::Promoter::promote`,
//! the exact production path `POST /candidates/{id}/promote` runs), never a
//! hand-built `deblob-canon-v1` stand-in — against a REAL Redis (Docker via
//! testcontainers).
//!
//! Before this fix: `crate::semantic_drift`'s shape walker (`typed_paths`,
//! consumed by `structural_relation`/`leaf_field_count`/
//! `canonical_field_id_coverage`) only understood the plain
//! `"deblob-canon-v1"` shape grammar. `Promoter::promote` ALWAYS stores a
//! promoted `SchemaRecord` with `canonicalizer: "deblob-monoid-v1"` and a
//! `canonical` string in the generalized-field grammar
//! (`deblob_monoid::Profile::generalized_canonical_json`'s
//! `{"optional":...,"types":[...],"children":{...},"elem":...}` shape) — so
//! `put_semantic`'s best-effort calls into `scan_semantic_collisions`/
//! `check_family_version_drift` always hit `ShapeWalkError::MalformedShape`,
//! silently swallowed as a `tracing::warn!`. Neither diagnostic EVER fired
//! for a genuinely promoted schema — the exact scenario they exist to
//! cover. This suite proves the fix directly against the two orchestrators
//! (mirrors `semantic_drift_it.rs`'s style) using real promoted schemas
//! (mirrors `semantic_monoid_promoted_it.rs`'s promotion helpers), and — like
//! `semantic_drift_it.rs` — captures the full relevant Redis key set before
//! and after each diagnostic call and asserts byte-identical state.

use std::collections::HashMap;
use std::sync::Arc;

use deblob::coldlane::{ColdLane, SampleMeta};
use deblob::metrics::Metrics;
use deblob::policy::{Promoter, PromotionPolicy};
use deblob::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};
use deblob::semantic_drift::{
    check_family_version_drift, scan_semantic_collisions, CollisionStrength, StructuralRelation,
};
use deblob::semantic_store::SemanticStore;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_core::revision::ReasonCode;
use deblob_core::semantic::{
    CanonicalEventTypeId, CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment,
    SemanticMetadata,
};
use deblob_fingerprint::{fingerprint, parse_bounded, shape_of, Limits, Node};
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

// ---------------------------------------------------------------------
// Setup: real Redis, real Registry + Evidence — mirrors
// `semantic_monoid_promoted_it.rs::setup`.
// ---------------------------------------------------------------------

async fn setup() -> (
    Arc<RedisRegistry>,
    Arc<RedisEvidence>,
    String,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let opts = RedisOpts {
        allow_volatile: true,
    };
    let registry = Arc::new(RedisRegistry::connect(&url, opts).await.unwrap());
    let evidence = Arc::new(
        RedisEvidence::connect(&url, RedisEvidenceOpts::default(), opts)
            .await
            .unwrap(),
    );
    (registry, evidence, url, node)
}

fn node_of(json: &[u8]) -> Node {
    parse_bounded(json, &Limits::default()).unwrap()
}

fn cand_id_of(json: &[u8]) -> CandidateId {
    let node = node_of(json);
    CandidateId::from_digest(&fingerprint(&shape_of(&node)))
}

fn meta(source: &str) -> SampleMeta {
    SampleMeta {
        source: source.to_string(),
        cursor: None,
    }
}

/// Both promotion guards disabled — this suite cares about the
/// diagnostics-vs-grammar wiring, not the guard thresholds (already covered
/// by `crates/deblob/src/policy.rs`'s own unit tests).
fn no_guard_policy() -> PromotionPolicy {
    PromotionPolicy {
        min_samples: 1,
        min_age_ms: 0,
    }
}

/// Ingests one concrete `payload` as a fresh candidate and promotes it
/// through the REAL `deblob::policy::Promoter` (no HTTP involved) — the
/// exact `ColdLane::ingest` -> `Promoter::promote` pipeline
/// `POST /candidates/{id}/promote` runs in production. `family` selects a
/// brand-new family or an EXISTING one (so two promotions can land as two
/// versions of the same family, mirroring `Promoter::promote`'s own
/// `FamilyChoice::Existing` path). Returns the resulting `SchemaRecord`,
/// whose `canonicalizer` is always `"deblob-monoid-v1"`
/// (`deblob_monoid::GENERALIZER`) — never `"deblob-canon-v1"` — by
/// construction of `Promoter::promote`.
async fn promote_real_schema(
    registry: &Arc<RedisRegistry>,
    evidence: &Arc<RedisEvidence>,
    payload: &[u8],
    name: &str,
    family: FamilyChoice,
) -> SchemaRecord {
    let lane = ColdLane::new(evidence.clone());
    let cand_id = cand_id_of(payload);
    lane.ingest(cand_id.clone(), &node_of(payload), meta("drift-monoid-it"))
        .await
        .unwrap();

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    let req = PromoteRequest {
        family,
        name: Some(name.to_string()),
        reason: "semantic_drift_monoid_promoted_it fixture".to_string(),
    };
    promoter.promote(&cand_id, req, "tester").await.unwrap()
}

fn metadata_with_unit_on(field: &str, code: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key(field.to_string())],
            semantics: FieldSemantics {
                canonical_field_id: None,
                identifier_namespace: None,
                unit: Some(deblob_core::semantic::Unit {
                    system: deblob_core::semantic::UnitSystem::Ucum,
                    code: code.to_string(),
                }),
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

fn metadata_event_type_with_cfid_on(event_type: &str, field: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: Some(CanonicalEventTypeId::new(event_type)),
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key(field.to_string())],
            semantics: FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("device.reading")),
                identifier_namespace: None,
                unit: None,
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

fn canon(metadata: &SemanticMetadata) -> (Vec<u8>, deblob_core::id::SemanticId) {
    let bytes = deblob_semantic::canonical_semantic_bytes(metadata).unwrap();
    let fp = deblob_semantic::semantic_fingerprint(metadata)
        .unwrap()
        .unwrap();
    (bytes, fp.0)
}

/// Snapshot of every key this module's diagnostics could conceivably touch,
/// across the relevant key families. Used to prove "no mutation" by direct
/// byte-for-byte comparison, not by inference — copied verbatim from
/// `semantic_drift_it.rs::snapshot_all`.
async fn snapshot_all(url: &str) -> HashMap<String, HashMap<String, String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut out = HashMap::new();
    for pattern in [
        "deblob:schema:*",
        "deblob:family:*",
        "deblob:sem-active:*",
        "deblob:sem-index:*",
        "deblob:sem-rev:*",
        "deblob:alias:*",
    ] {
        let keys: Vec<String> = conn.keys(pattern).await.unwrap();
        for key in keys {
            let fields: HashMap<String, String> = conn.hgetall(&key).await.unwrap_or_default();
            if !fields.is_empty() {
                out.insert(key, fields);
                continue;
            }
            let key_type: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut conn)
                .await
                .unwrap();
            let rendered = match key_type.as_str() {
                "set" => {
                    let mut members: Vec<String> = conn.smembers(&key).await.unwrap();
                    members.sort();
                    members.join(",")
                }
                "string" => conn.get(&key).await.unwrap_or_default(),
                other => format!("<unhandled type {other}>"),
            };
            out.insert(key, HashMap::from([("__raw__".to_string(), rendered)]));
        }
    }
    out
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// The headline collision-side regression: two schemas published through
/// REAL candidate promotion (`canonicalizer == "deblob-monoid-v1"` on both)
/// sharing one `sem_` must produce a `strong`/`medium` review-candidate
/// finding — before the fix, `scan_semantic_collisions` always hit
/// `ShapeWalkError::MalformedShape` on a promoted schema's canonical shape,
/// silently swallowed by `put_semantic`, so this diagnostic never actually
/// fired for anything `Promoter::promote` ever produced.
#[tokio::test]
async fn two_promoted_monoid_schemas_sharing_one_sem_fire_a_review_candidate_collision_never_mutating_state(
) {
    let (registry, evidence, url, _node) = setup().await;

    let sch_a = promote_real_schema(
        &registry,
        &evidence,
        br#"{"temperature":1}"#,
        "device.reading.a",
        FamilyChoice::New,
    )
    .await;
    // TWO new fields relative to schema A, deliberately — NOT one:
    // `ColdLane::ingest` clusters a sample onto an EXISTING candidate
    // whenever dropping exactly one top-level field from it reproduces a
    // previously-registered candidate's own full generalized fingerprint
    // (`reduced_generalized_fps`'s "optional-field convergence" — a real,
    // intentional production behavior). Adding only "humidity" here would
    // make schema B's "drop humidity" projection collide with schema A's
    // own fingerprint and silently MERGE B into A's candidate instead of
    // producing two distinct schemas — exactly the trap this comment is
    // now here to warn the next editor away from.
    let sch_b = promote_real_schema(
        &registry,
        &evidence,
        br#"{"temperature":1,"humidity":2,"pressure":3}"#,
        "device.reading.b",
        FamilyChoice::New,
    )
    .await;
    assert_eq!(sch_a.canonicalizer, "deblob-monoid-v1");
    assert_eq!(sch_b.canonicalizer, "deblob-monoid-v1");
    assert_ne!(
        sch_a.schema_id, sch_b.schema_id,
        "sanity: the two promoted schemas must be structurally distinct"
    );

    // Shared metadata: same event_type + canonical_field_id on the field
    // BOTH schemas actually have ("temperature") — schema A has 1 leaf
    // field (100% coverage), schema B has 3 (33%): min coverage well under
    // 80% with an event_type set lands Medium, not Strong — still a review
    // candidate.
    let metadata = metadata_event_type_with_cfid_on("device.reading", "temperature");
    let (bytes, sem_id) = canon(&metadata);

    for sch in [&sch_a.schema_id, &sch_b.schema_id] {
        registry
            .append_revision(
                sch,
                &metadata,
                &bytes,
                &sem_id,
                "kamil",
                ReasonCode::Correction,
                "shared annotation on a real promoted schema",
                1,
                1,
                None,
            )
            .await
            .unwrap();
    }

    let metrics = Metrics::new();
    let before = snapshot_all(&url).await;

    let findings = scan_semantic_collisions(
        registry.as_ref() as &dyn Registry,
        registry.as_ref() as &dyn SemanticStore,
        &metrics,
        &sem_id,
    )
    .await
    .unwrap();

    assert_eq!(
        findings.len(),
        1,
        "exactly one pair for two promoted schemas sharing one sem_ — got: {findings:?}"
    );
    let finding = &findings[0];
    assert_eq!(finding.sem_id, sem_id);
    assert_eq!(
        finding.relation,
        StructuralRelation::Compatible,
        "temperature+humidity is a compatible superset of temperature-only"
    );
    assert!(
        matches!(
            finding.strength,
            CollisionStrength::Strong | CollisionStrength::Medium
        ),
        "must be a real strong/medium finding, never Weak (and never a swallowed \
         MalformedShape — this is the exact regression the fix closes): {:?}",
        finding.strength
    );
    assert!(
        finding.is_review_candidate,
        "strong/medium findings must always be review candidates"
    );
    let mut pair = [finding.sch_a.clone(), finding.sch_b.clone()];
    pair.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut expected = [sch_a.schema_id.clone(), sch_b.schema_id.clone()];
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(pair, expected);

    let families = metrics.registry().gather();
    let collision_total: f64 = families
        .iter()
        .find(|f| f.get_name() == "deblob_semantic_collision_total")
        .unwrap()
        .get_metric()
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.get_name() == "strength" && l.get_value() == finding.strength.as_str())
        })
        .unwrap()
        .get_counter()
        .get_value();
    assert_eq!(
        collision_total,
        1.0,
        "deblob_semantic_collision_total{{strength=\"{}\"}} must have incremented exactly once",
        finding.strength.as_str()
    );

    let after = snapshot_all(&url).await;
    assert_eq!(
        before, after,
        "classifying a same-sem_ collision on promoted schemas must not mutate any deblob: key"
    );
}

/// The headline drift-side regression: a family PROMOTED through the real
/// `deblob-monoid-v1` pipeline reaching version 2 with a structurally
/// compatible re-version whose active `sem_` changed must fire drift and
/// increment `deblob_semantic_drift_total` — before the fix,
/// `check_family_version_drift` always hit `ShapeWalkError::MalformedShape`
/// on either promoted version's canonical shape.
#[tokio::test]
async fn promoted_monoid_family_reaching_version_2_with_changed_active_sem_fires_drift_never_mutating_state(
) {
    let (registry, evidence, url, _node) = setup().await;

    let v1 = promote_real_schema(
        &registry,
        &evidence,
        br#"{"amount":5}"#,
        "payments.charged.v1",
        FamilyChoice::New,
    )
    .await;
    let family_id: FamilyId = v1.family_id.clone();
    assert_eq!(v1.version, FamilyVersion(1));

    // A structurally-COMPATIBLE re-version (superset of v1's fields) in the
    // SAME family — TWO new fields relative to v1, deliberately not one:
    // see `two_promoted_monoid_schemas_sharing_one_sem_...`'s comment on
    // `ColdLane::ingest`'s one-field-drop clustering convergence, which
    // would otherwise silently merge v2's ingest onto v1's own candidate
    // instead of producing a genuinely distinct second schema/version.
    let v2 = promote_real_schema(
        &registry,
        &evidence,
        br#"{"amount":5,"currency":"USD","note":"paid"}"#,
        "payments.charged.v2",
        FamilyChoice::Existing(family_id.clone()),
    )
    .await;
    assert_eq!(v2.family_id, family_id, "v2 must land in the SAME family");
    assert_eq!(v2.version, FamilyVersion(2));
    assert_eq!(v1.canonicalizer, "deblob-monoid-v1");
    assert_eq!(v2.canonicalizer, "deblob-monoid-v1");

    let prior_meta = metadata_with_unit_on("amount", "[degF]");
    let (prior_bytes, prior_sem) = canon(&prior_meta);
    registry
        .append_revision(
            &v1.schema_id,
            &prior_meta,
            &prior_bytes,
            &prior_sem,
            "kamil",
            ReasonCode::Correction,
            "v1 initial",
            1,
            1,
            None,
        )
        .await
        .unwrap();

    let new_meta = metadata_with_unit_on("amount", "USD"); // different unit -> different sem_
    let (new_bytes, new_sem) = canon(&new_meta);
    registry
        .append_revision(
            &v2.schema_id,
            &new_meta,
            &new_bytes,
            &new_sem,
            "kamil",
            ReasonCode::Correction,
            "v2 initial",
            1,
            1,
            None,
        )
        .await
        .unwrap();
    assert_ne!(prior_sem, new_sem);

    let metrics = Metrics::new();
    let before = snapshot_all(&url).await;

    let drift = check_family_version_drift(
        registry.as_ref() as &dyn Registry,
        registry.as_ref() as &dyn SemanticStore,
        &metrics,
        family_id.clone(),
        &v1.schema_id,
        FamilyVersion(1),
        &v2.schema_id,
        FamilyVersion(2),
    )
    .await
    .unwrap()
    .expect(
        "a promoted family's compatible re-version with a changed active sem_ must fire drift \
         (this used to fail to compute at all: MalformedShape swallowed as a warning)",
    );

    assert_eq!(drift.family_id, family_id);
    assert_eq!(drift.prior_version, FamilyVersion(1));
    assert_eq!(drift.new_version, FamilyVersion(2));
    assert_eq!(drift.prior_sem, prior_sem);
    assert_eq!(drift.new_sem, new_sem);

    let families = metrics.registry().gather();
    let drift_total = families
        .iter()
        .find(|f| f.get_name() == "deblob_semantic_drift_total")
        .unwrap()
        .get_metric()[0]
        .get_counter()
        .get_value();
    assert_eq!(
        drift_total, 1.0,
        "deblob_semantic_drift_total must have incremented exactly once"
    );

    // "Without splitting the family": the family hash still resolves
    // EXACTLY v:1 -> v1 and v:2 -> v2.
    let v1_schema = registry
        .family_version_schema(&family_id, FamilyVersion(1))
        .await
        .expect("read family v:1")
        .expect("family v:1 must resolve");
    let v2_schema = registry
        .family_version_schema(&family_id, FamilyVersion(2))
        .await
        .expect("read family v:2")
        .expect("family v:2 must resolve");
    assert_eq!(v1_schema, v1.schema_id);
    assert_eq!(v2_schema, v2.schema_id);

    let after = snapshot_all(&url).await;
    assert_eq!(
        before, after,
        "the drift diagnostic must not have mutated any deblob: key at all"
    );
}
