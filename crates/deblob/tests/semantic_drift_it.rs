//! P2-D Task 7: `deblob::semantic_drift`'s two orchestrators
//! (`check_family_version_drift`, `scan_semantic_collisions`) against a
//! REAL (AOF-enabled) Redis via testcontainers — same harness style as
//! `deblob-redis`'s own `semantic_it.rs` and this crate's
//! `promote_resolve_it.rs`.
//!
//! Both tests capture the FULL relevant Redis key set (`deblob:schema:*`,
//! `deblob:family:*`, `deblob:sem-active:*`, `deblob:sem-index:*`) before
//! and after running the diagnostic, and assert byte-identical state:
//! proof (not just claim) that a diagnostic firing never aliases, merges,
//! promotes, or mutates a family/schema/`sem_`.

use std::collections::HashMap;

use deblob::semantic_drift::{
    check_family_version_drift, scan_semantic_collisions, CollisionStrength, StructuralRelation,
};
use deblob::semantic_store::SemanticStore;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_core::revision::ReasonCode;
use deblob_core::semantic::{
    CanonicalEventTypeId, CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment,
    SemanticMetadata,
};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_match::metrics::Metrics;
use deblob_redis::{RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

/// Publishes a real, structurally-canonicalized schema into `family_id`
/// (mirrors `deblob-redis`'s `semantic_it.rs::publish_schema`, extended to
/// take an explicit `family_id` so two versions can share one family).
async fn publish_schema(
    reg: &RedisRegistry,
    family_id: FamilyId,
    json: &[u8],
    cand_seed: u8,
) -> SchemaId {
    let node = parse_bounded(json, &Limits::default()).unwrap();
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).unwrap();
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id,
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "semantic_drift_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
    };
    let bucket = format!("bucket:semdrift-it:{cand_seed}");
    let cand = CandidateId::from_digest(&[cand_seed; 32]);
    reg.publish(record, &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();
    schema_id
}

fn metadata_with_unit(code: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
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

fn metadata_with_event_type_and_cfid(event_type: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: Some(CanonicalEventTypeId::new(event_type)),
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
            semantics: FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new("device.temperature")),
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

async fn connect() -> (ContainerAsync<Redis>, RedisRegistry, String) {
    let node = Redis::default()
        .with_cmd(["--appendonly", "yes"])
        .start()
        .await
        .unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: false,
        },
    )
    .await
    .unwrap();
    (node, reg, url)
}

/// Snapshot of every key this module's diagnostics could conceivably
/// touch, across the four relevant key families. Used to prove "no
/// mutation" by direct byte-for-byte comparison, not by inference.
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
            // Non-hash key (e.g. deblob:sem-index:* is a SET, deblob:alias:*
            // is a STRING) — capture via a type-appropriate read so the
            // snapshot still catches a mutation of it.
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

#[tokio::test]
async fn family_version_drift_fires_and_never_mutates_registry_state() {
    let (_node, reg, url) = connect().await;
    let family_id = FamilyId::new_v7();

    let prior_sch = publish_schema(&reg, family_id.clone(), br#"{"x":1}"#, 1).await;
    let new_sch = publish_schema(&reg, family_id.clone(), br#"{"x":1,"y":2}"#, 2).await;

    let prior_meta = metadata_with_unit("Cel");
    let (prior_bytes, prior_sem) = canon(&prior_meta);
    reg.append_revision(
        &prior_sch,
        &prior_meta,
        &prior_bytes,
        &prior_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let new_meta = metadata_with_unit("K"); // different unit -> different sem_
    let (new_bytes, new_sem) = canon(&new_meta);
    reg.append_revision(
        &new_sch,
        &new_meta,
        &new_bytes,
        &new_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
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
        &reg as &dyn Registry,
        &reg as &dyn SemanticStore,
        &metrics,
        family_id.clone(),
        &prior_sch,
        FamilyVersion(1),
        &new_sch,
        FamilyVersion(2),
    )
    .await
    .unwrap()
    .expect("compatible new version with a changed active sem_ must fire drift");

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
        "the counter must have incremented exactly once"
    );

    let after = snapshot_all(&url).await;
    assert_eq!(
        before, after,
        "the diagnostic must not have mutated any deblob: key at all"
    );
}

#[tokio::test]
async fn same_sem_two_schemas_high_coverage_is_strong_and_never_mutates_registry_state() {
    let (_node, reg, url) = connect().await;

    // Two UNRELATED families/schemas that happen to carry byte-identical
    // semantic metadata — the brief's core same-sem_/different-sch_ case.
    // The two canonical shapes must genuinely differ (distinct top-level
    // field sets => distinct sch_ digests — shape ignores VALUES, so
    // `{"temperature":1}` and `{"temperature":2}` would fingerprint
    // IDENTICALLY and this wouldn't be two schemas at all) while both
    // still exposing the single "temperature" leaf path the shared
    // metadata annotates, so per-schema leaf-field coverage stays 100% on
    // both sides (an empty nested "meta" object contributes a container
    // path, never a leaf, so it doesn't dilute coverage).
    let family_a = FamilyId::new_v7();
    let family_b = FamilyId::new_v7();
    let sch_a = publish_schema(&reg, family_a, br#"{"temperature":1}"#, 3).await;
    let sch_b = publish_schema(&reg, family_b, br#"{"temperature":1,"meta":{}}"#, 4).await;
    assert_ne!(sch_a, sch_b, "sanity: the two schemas must be distinct");

    let metadata = metadata_with_event_type_and_cfid("device.reading");
    let (bytes, sem_id) = canon(&metadata);

    for sch in [&sch_a, &sch_b] {
        reg.append_revision(
            sch,
            &metadata,
            &bytes,
            &sem_id,
            "kamil",
            ReasonCode::Correction,
            "shared annotation",
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
        &reg as &dyn Registry,
        &reg as &dyn SemanticStore,
        &metrics,
        &sem_id,
    )
    .await
    .unwrap();

    assert_eq!(findings.len(), 1, "exactly one pair for two schemas");
    let finding = &findings[0];
    assert_eq!(finding.sem_id, sem_id);
    assert_eq!(finding.strength, CollisionStrength::Strong);
    assert!(finding.is_review_candidate);
    assert_eq!(finding.relation, StructuralRelation::Compatible);
    let mut pair = [finding.sch_a.clone(), finding.sch_b.clone()];
    pair.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut expected = [sch_a.clone(), sch_b.clone()];
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(pair, expected);

    let families = metrics.registry().gather();
    let collision_total = families
        .iter()
        .find(|f| f.get_name() == "deblob_semantic_collision_total")
        .unwrap()
        .get_metric()
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.get_name() == "strength" && l.get_value() == "strong")
        })
        .unwrap()
        .get_counter()
        .get_value();
    assert_eq!(collision_total, 1.0);

    let after = snapshot_all(&url).await;
    assert_eq!(
        before, after,
        "classifying a same-sem_ collision must not mutate any deblob: key"
    );
}

#[tokio::test]
async fn sem_with_a_single_schema_is_not_a_collision() {
    let (_node, reg, _url) = connect().await;
    let family = FamilyId::new_v7();
    let sch = publish_schema(&reg, family, br#"{"temperature":1}"#, 5).await;

    let metadata = metadata_with_event_type_and_cfid("device.reading");
    let (bytes, sem_id) = canon(&metadata);
    reg.append_revision(
        &sch,
        &metadata,
        &bytes,
        &sem_id,
        "kamil",
        ReasonCode::Correction,
        "solo annotation",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let metrics = Metrics::new();
    let findings = scan_semantic_collisions(
        &reg as &dyn Registry,
        &reg as &dyn SemanticStore,
        &metrics,
        &sem_id,
    )
    .await
    .unwrap();
    assert!(findings.is_empty());

    let families = metrics.registry().gather();
    let collision_family = families
        .iter()
        .find(|f| f.get_name() == "deblob_semantic_collision_total")
        .unwrap();
    for m in collision_family.get_metric() {
        assert_eq!(
            m.get_counter().get_value(),
            0.0,
            "a lone schema under one sem_ must never touch the collision counter"
        );
    }
}
