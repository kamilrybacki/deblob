//! P2-D Task 5: append-only semantic-assertion revisions + active pointer +
//! reverse `sem_` index, run against a REAL (AOF-enabled) Redis via
//! testcontainers — same harness style as `registry_it.rs`/`index_it.rs`.
//!
//! Fixtures go through the real `deblob-semantic` canonicalizer
//! (`canonical_semantic_bytes`/`semantic_fingerprint`), not hand-rolled
//! bytes, so these tests stay honest about what a real caller would
//! actually pass to `append_revision` — mirrors `index_it.rs`'s
//! `record_and_bucket` helper depending on `deblob-fingerprint` for the
//! same reason.

use std::collections::BTreeMap;

use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId, SemanticId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_core::revision::{
    Etag, ReasonCode, SemError, SignatureCandidates, MAX_SIGNATURE_CANDIDATES,
};
use deblob_core::semantic::{
    CanonicalFieldId, FieldEntry, FieldSemantics, PathSegment, SemanticMetadata, Unit, UnitSystem,
};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_redis::{RedisOpts, RedisRegistry};
use deblob_semantic::signature::semantic_signature;
use redis::AsyncCommands;
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

/// Publishes a real, structurally-canonicalized schema (mirrors
/// `index_it.rs::record_and_bucket`) so `append_revision`'s target
/// `sch_id` genuinely exists in the vault, letting the "schema record
/// untouched by annotation" test compare a real `deblob:schema:*` hash.
async fn publish_schema(reg: &RedisRegistry, json: &[u8], cand_seed: u8) -> SchemaId {
    let node = parse_bounded(json, &Limits::default()).unwrap();
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).unwrap();
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let record = SchemaRecord {
        schema_id: schema_id.clone(),
        family_id: FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "semantic_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    };
    let bucket = format!("bucket:sem-it:{cand_seed}");
    let cand = CandidateId::from_digest(&[cand_seed; 32]);
    reg.publish(record, &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();
    schema_id
}

/// A minimal, real `SemanticMetadata` fixture: one field carrying a UCUM
/// unit, distinguished by `code` so two calls produce genuinely different
/// canonical bytes / `sem_` identities.
fn metadata_with_unit(code: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
            semantics: FieldSemantics {
                canonical_field_id: None,
                identifier_namespace: None,
                unit: Some(Unit {
                    system: UnitSystem::Ucum,
                    code: code.to_string(),
                }),
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

/// A minimal, real `SemanticMetadata` fixture carrying a `canonical_field_id`
/// (an anchor feature, unlike `metadata_with_unit`'s standalone `unit:`) —
/// Task 10's postings/candidate-union tests need anchor-bearing fixtures so
/// `signature_candidates`'s union is non-trivial.
fn metadata_with_cfid(cfid: &str) -> SemanticMetadata {
    SemanticMetadata {
        event_type: None,
        fields: vec![FieldEntry {
            path: vec![PathSegment::Key("temperature".to_string())],
            semantics: FieldSemantics {
                canonical_field_id: Some(CanonicalFieldId::new(cfid)),
                identifier_namespace: None,
                unit: None,
                numeric_scale: None,
                temporal: None,
                enum_semantics: None,
            },
        }],
    }
}

/// Runs `metadata` through the real Task 3/4 canonicalizer, the same way a
/// real caller (Task 6's API layer) would before calling
/// `RedisRegistry::append_revision`.
fn canon(metadata: &SemanticMetadata) -> (Vec<u8>, SemanticId) {
    let bytes = deblob_semantic::canonical_semantic_bytes(metadata).unwrap();
    let fp = deblob_semantic::semantic_fingerprint(metadata)
        .unwrap()
        .unwrap();
    (bytes, fp.0)
}

/// Starts an AOF-enabled Redis container (brief: "fixed port, AOF" — the
/// harness's "fixed port" is testcontainers' own published-port binding,
/// re-derived per test via `get_host_port_ipv4`, same as every other `*_it.rs`
/// file in this crate) and connects `RedisRegistry` to it with
/// `allow_volatile: false` — proving persistence is genuinely on, not just
/// permitted. Returns the container guard alongside the registry/URL: the
/// container stops the moment its `ContainerAsync` is dropped, so callers
/// MUST hold the first element for the test's entire duration.
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

#[tokio::test]
async fn first_annotation_is_readable_via_active_semantic_and_reverse_index() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 1).await;

    let metadata = metadata_with_unit("Cel");
    let (bytes, sem_id) = canon(&metadata);

    let outcome = reg
        .append_revision(
            &sch_id,
            &metadata,
            &bytes,
            &sem_id,
            "kamil",
            ReasonCode::Correction,
            "initial annotation",
            1_700_000_000_000,
            1_700_000_000_000,
            None,
        )
        .await
        .unwrap();
    assert!(outcome.was_appended());
    let outcome_etag = outcome.etag();
    let revision = outcome.into_revision();
    assert_eq!(revision.sem_id, sem_id);
    assert_eq!(revision.metadata, metadata);
    assert_eq!(revision.previous_revision_id, None);

    let (active_meta, active_sem, etag) = reg
        .active_semantic(&sch_id)
        .await
        .unwrap()
        .expect("must be annotated after append_revision");
    assert_eq!(active_meta, metadata);
    assert_eq!(active_sem, sem_id);
    assert_eq!(etag, Etag(1));
    assert_eq!(
        outcome_etag, etag,
        "the atomic append's own etag must match a subsequent active_semantic read"
    );

    let found = reg.schemas_by_semantic(&sem_id).await.unwrap();
    assert_eq!(found, vec![sch_id.clone()]);

    let history = reg.revisions(&sch_id).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].revision_id, revision.revision_id);
}

#[tokio::test]
async fn schema_never_annotated_returns_none() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"a":1}"#, 2).await;

    assert_eq!(reg.active_semantic(&sch_id).await.unwrap(), None);
    assert_eq!(reg.revisions(&sch_id).await.unwrap(), vec![]);
}

#[tokio::test]
async fn identical_bytes_replay_is_idempotent_no_op() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 3).await;

    let metadata = metadata_with_unit("Cel");
    let (bytes, sem_id) = canon(&metadata);

    let first = reg
        .append_revision(
            &sch_id,
            &metadata,
            &bytes,
            &sem_id,
            "kamil",
            ReasonCode::Correction,
            "initial",
            1,
            1,
            None,
        )
        .await
        .unwrap()
        .into_revision();

    // Replay with the SAME canonical bytes/sem_id but a DIFFERENT actor,
    // empty reason, and a deliberately wrong etag — none of that should
    // matter, since the idempotency check happens before reason/etag are
    // even consulted.
    let second = reg
        .append_revision(
            &sch_id,
            &metadata,
            &bytes,
            &sem_id,
            "someone-else",
            ReasonCode::OperatorOverride,
            "",
            999,
            999,
            Some(Etag(9999)),
        )
        .await
        .unwrap();

    assert!(!second.was_appended(), "identical bytes must be a no-op");
    let second_outcome_etag = second.etag();
    let second_rev = second.into_revision();
    assert_eq!(second_rev.revision_id, first.revision_id);
    assert_eq!(
        second_rev.actor, first.actor,
        "must be the ORIGINAL revision, not a new one"
    );

    let history = reg.revisions(&sch_id).await.unwrap();
    assert_eq!(
        history.len(),
        1,
        "idempotent replay must not create a new revision"
    );

    let (_, _, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(etag, Etag(1), "idempotent replay must not advance the etag");
    assert_eq!(
        second_outcome_etag, etag,
        "AlreadyActive's own etag must match a subsequent active_semantic read"
    );
}

#[tokio::test]
async fn different_bytes_without_reason_is_rejected() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 4).await;

    let initial = metadata_with_unit("Cel");
    let (initial_bytes, initial_sem) = canon(&initial);
    reg.append_revision(
        &sch_id,
        &initial,
        &initial_bytes,
        &initial_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let changed = metadata_with_unit("K");
    let (changed_bytes, changed_sem) = canon(&changed);
    assert_ne!(changed_bytes, initial_bytes);

    let err = reg
        .append_revision(
            &sch_id,
            &changed,
            &changed_bytes,
            &changed_sem,
            "kamil",
            ReasonCode::Correction,
            "", // no reason
            2,
            2,
            Some(Etag(1)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, SemError::MissingReason));

    // Nothing must have been written.
    let (active_meta, _, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, initial);
    assert_eq!(etag, Etag(1));
    assert_eq!(reg.revisions(&sch_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn different_bytes_with_stale_etag_is_rejected() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 5).await;

    let initial = metadata_with_unit("Cel");
    let (initial_bytes, initial_sem) = canon(&initial);
    reg.append_revision(
        &sch_id,
        &initial,
        &initial_bytes,
        &initial_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let changed = metadata_with_unit("K");
    let (changed_bytes, changed_sem) = canon(&changed);

    let err = reg
        .append_revision(
            &sch_id,
            &changed,
            &changed_bytes,
            &changed_sem,
            "kamil",
            ReasonCode::Correction,
            "fix the unit",
            2,
            2,
            Some(Etag(999)), // stale — current is 1
        )
        .await
        .unwrap_err();
    match err {
        SemError::EtagConflict { expected, current } => {
            assert_eq!(expected, Some(Etag(999)));
            assert_eq!(current, Etag(1));
        }
        other => panic!("expected EtagConflict, got {other:?}"),
    }

    let (active_meta, _, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, initial);
    assert_eq!(etag, Etag(1));
    assert_eq!(reg.revisions(&sch_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn different_bytes_with_missing_etag_on_annotated_schema_is_rejected() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 9).await;

    let initial = metadata_with_unit("Cel");
    let (initial_bytes, initial_sem) = canon(&initial);
    reg.append_revision(
        &sch_id,
        &initial,
        &initial_bytes,
        &initial_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let changed = metadata_with_unit("K");
    let (changed_bytes, changed_sem) = canon(&changed);

    let err = reg
        .append_revision(
            &sch_id,
            &changed,
            &changed_bytes,
            &changed_sem,
            "kamil",
            ReasonCode::Correction,
            "fix the unit",
            2,
            2,
            None, // missing etag — should be rejected as EtagConflict
        )
        .await
        .unwrap_err();
    match err {
        SemError::EtagConflict { expected, current } => {
            assert_eq!(expected, None);
            assert_eq!(current, Etag(1));
        }
        other => panic!("expected EtagConflict, got {other:?}"),
    }

    let (active_meta, _, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, initial);
    assert_eq!(etag, Etag(1));
    assert_eq!(reg.revisions(&sch_id).await.unwrap().len(), 1);
}

#[tokio::test]
async fn real_change_appends_advances_pointer_and_relinks_reverse_index() {
    let (_node, reg, _url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 6).await;

    let initial = metadata_with_unit("Cel");
    let (initial_bytes, initial_sem) = canon(&initial);
    let first = reg
        .append_revision(
            &sch_id,
            &initial,
            &initial_bytes,
            &initial_sem,
            "kamil",
            ReasonCode::Correction,
            "initial",
            1,
            1,
            None,
        )
        .await
        .unwrap()
        .into_revision();

    let changed = metadata_with_unit("K");
    let (changed_bytes, changed_sem) = canon(&changed);
    assert_ne!(changed_sem, initial_sem);

    let outcome = reg
        .append_revision(
            &sch_id,
            &changed,
            &changed_bytes,
            &changed_sem,
            "kamil",
            ReasonCode::Correction,
            "unit was wrong",
            2,
            2,
            Some(Etag(1)),
        )
        .await
        .unwrap();
    assert!(outcome.was_appended());
    let outcome_etag = outcome.etag();
    let second = outcome.into_revision();
    assert_eq!(second.previous_revision_id, Some(first.revision_id.clone()));
    assert_ne!(second.revision_id, first.revision_id);

    // Pointer advanced.
    let (active_meta, active_sem, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, changed);
    assert_eq!(active_sem, changed_sem);
    assert_eq!(etag, Etag(2));
    assert_eq!(
        outcome_etag, etag,
        "the atomic append's own etag must match a subsequent active_semantic read"
    );

    // Prior revision still readable, unmodified, via full history.
    let history = reg.revisions(&sch_id).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].revision_id, first.revision_id);
    assert_eq!(history[0].metadata, initial);
    assert_eq!(history[1].revision_id, second.revision_id);
    assert_eq!(history[1].metadata, changed);

    // Reverse index: old sem_ de-linked, new sem_ linked.
    assert_eq!(reg.schemas_by_semantic(&initial_sem).await.unwrap(), vec![]);
    assert_eq!(
        reg.schemas_by_semantic(&changed_sem).await.unwrap(),
        vec![sch_id.clone()]
    );
}

#[tokio::test]
async fn rebuild_semantic_index_restores_reverse_index_from_active_pointers() {
    let (_node, reg, url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 7).await;

    let metadata = metadata_with_unit("Cel");
    let (bytes, sem_id) = canon(&metadata);
    reg.append_revision(
        &sch_id,
        &metadata,
        &bytes,
        &sem_id,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        reg.schemas_by_semantic(&sem_id).await.unwrap(),
        vec![sch_id.clone()]
    );

    // Wipe ONLY the derived reverse-index keys via raw KEYS/DEL — the
    // sem-rev/sem-active keys (the source of truth) are untouched.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let index_keys: Vec<String> = conn.keys("deblob:sem-index:*").await.unwrap();
    assert!(!index_keys.is_empty());
    for key in &index_keys {
        let _: () = conn.del(key).await.unwrap();
    }

    assert_eq!(reg.schemas_by_semantic(&sem_id).await.unwrap(), vec![]);

    let rebuilt = reg.rebuild_semantic_index().await.unwrap();
    assert!(rebuilt >= 1);

    assert_eq!(
        reg.schemas_by_semantic(&sem_id).await.unwrap(),
        vec![sch_id]
    );
}

#[tokio::test]
async fn schema_record_bytes_are_unchanged_by_annotation() {
    let (_node, reg, url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 8).await;

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let before: std::collections::HashMap<String, String> = conn
        .hgetall(format!("deblob:schema:{}", sch_id.as_str()))
        .await
        .unwrap();
    assert!(!before.is_empty(), "sanity: schema record must exist");

    let metadata = metadata_with_unit("Cel");
    let (bytes, sem_id) = canon(&metadata);
    reg.append_revision(
        &sch_id,
        &metadata,
        &bytes,
        &sem_id,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    // And a real change on top, for good measure.
    let changed = metadata_with_unit("K");
    let (changed_bytes, changed_sem) = canon(&changed);
    reg.append_revision(
        &sch_id,
        &changed,
        &changed_bytes,
        &changed_sem,
        "kamil",
        ReasonCode::Correction,
        "fix",
        2,
        2,
        Some(Etag(1)),
    )
    .await
    .unwrap();

    let after: std::collections::HashMap<String, String> = conn
        .hgetall(format!("deblob:schema:{}", sch_id.as_str()))
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "the sch_ schema record hash must be byte-identical before/after annotation"
    );
}

// ---------------------------------------------------------------------
// P2-D Task 10: the bounded semantic-neighbor inverted index
// (`deblob:sem-sig:*`), maintained atomically alongside the Task 5
// active-pointer transition. CHECKPOINT: index atomicity + rebuild
// equivalence.
// ---------------------------------------------------------------------

/// Full snapshot of every `deblob:sem-sig:*` posting SET, member-sorted, so
/// two snapshots compare byte-for-byte regardless of `SMEMBERS` ordering.
async fn snapshot_sem_sig(url: &str) -> BTreeMap<String, Vec<String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = conn.keys("deblob:sem-sig:*").await.unwrap();
    let mut out = BTreeMap::new();
    for key in keys {
        let mut members: Vec<String> = conn.smembers(&key).await.unwrap();
        members.sort();
        out.insert(key, members);
    }
    out
}

/// Snapshot of the `feature_keys_json` field on every `deblob:sem-active:*`
/// hash listed in `sch_ids` — the round-tripped "what were my postings"
/// state the atomic swap reads back on the NEXT re-annotation.
async fn snapshot_feature_keys_json(
    url: &str,
    sch_ids: &[SchemaId],
) -> BTreeMap<String, Option<String>> {
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let mut out = BTreeMap::new();
    for id in sch_ids {
        let key = format!("deblob:sem-active:{}", id.as_str());
        let v: Option<String> = conn.hget(&key, "feature_keys_json").await.unwrap();
        out.insert(key, v);
    }
    out
}

#[tokio::test]
async fn postings_are_populated_from_first_annotation() {
    let (_node, reg, url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 10).await;

    let metadata = metadata_with_cfid("device.temperature");
    let (bytes, sem_id) = canon(&metadata);
    reg.append_revision(
        &sch_id,
        &metadata,
        &bytes,
        &sem_id,
        "kamil",
        ReasonCode::Correction,
        "initial annotation",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let expected_hex = semantic_signature(&metadata).feature_keys_hex();
    assert!(
        !expected_hex.is_empty(),
        "sanity: fixture must carry features"
    );

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for hex in &expected_hex {
        let is_member: bool = conn
            .sismember(format!("deblob:sem-sig:{hex}"), sch_id.as_str())
            .await
            .unwrap();
        assert!(is_member, "posting for feature {hex} must list the schema");
    }

    // The active pointer round-trips the same feature-key list it wrote,
    // for the NEXT call to atomically read back as "old".
    let stored: Option<String> = conn
        .hget(
            format!("deblob:sem-active:{}", sch_id.as_str()),
            "feature_keys_json",
        )
        .await
        .unwrap();
    let stored: Vec<String> = serde_json::from_str(&stored.unwrap()).unwrap();
    assert_eq!(stored, expected_hex);
}

#[tokio::test]
async fn reannotation_atomically_swaps_postings_together_with_the_pointer() {
    let (_node, reg, url) = connect().await;
    let sch_id = publish_schema(&reg, br#"{"temperature":1}"#, 11).await;

    let initial = metadata_with_cfid("device.temperature");
    let (initial_bytes, initial_sem) = canon(&initial);
    reg.append_revision(
        &sch_id,
        &initial,
        &initial_bytes,
        &initial_sem,
        "kamil",
        ReasonCode::Correction,
        "initial",
        1,
        1,
        None,
    )
    .await
    .unwrap();
    let initial_hex = semantic_signature(&initial).feature_keys_hex();

    let changed = metadata_with_cfid("device.humidity");
    let (changed_bytes, changed_sem) = canon(&changed);
    let outcome = reg
        .append_revision(
            &sch_id,
            &changed,
            &changed_bytes,
            &changed_sem,
            "kamil",
            ReasonCode::Correction,
            "corrected field",
            2,
            2,
            Some(Etag(1)),
        )
        .await
        .unwrap();
    assert!(outcome.was_appended());
    let changed_hex = semantic_signature(&changed).feature_keys_hex();
    assert_ne!(
        initial_hex, changed_hex,
        "sanity: distinct cfid must yield distinct feature sets"
    );

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Old postings fully de-listed — never left dangling alongside the new
    // ones (the "atomic swap", not an additive union).
    for hex in &initial_hex {
        let is_member: bool = conn
            .sismember(format!("deblob:sem-sig:{hex}"), sch_id.as_str())
            .await
            .unwrap();
        assert!(
            !is_member,
            "old posting for {hex} must be removed after re-annotation"
        );
    }
    // New postings fully listed.
    for hex in &changed_hex {
        let is_member: bool = conn
            .sismember(format!("deblob:sem-sig:{hex}"), sch_id.as_str())
            .await
            .unwrap();
        assert!(
            is_member,
            "new posting for {hex} must be present after re-annotation"
        );
    }

    // The active pointer moved together with the postings swap — both were
    // decided inside the very same atomic Lua transition.
    let (active_meta, active_sem, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, changed);
    assert_eq!(active_sem, changed_sem);
    assert_eq!(etag, Etag(2));

    let stored: Option<String> = conn
        .hget(
            format!("deblob:sem-active:{}", sch_id.as_str()),
            "feature_keys_json",
        )
        .await
        .unwrap();
    let stored: Vec<String> = serde_json::from_str(&stored.unwrap()).unwrap();
    assert_eq!(stored, changed_hex);
}

#[tokio::test]
async fn candidate_union_is_index_derived_from_shared_features_only() {
    let (_node, reg, _url) = connect().await;
    let sch_a = publish_schema(&reg, br#"{"temperature":1}"#, 12).await;
    let sch_b = publish_schema(&reg, br#"{"temperature":1,"meta":{}}"#, 13).await;
    let sch_c = publish_schema(&reg, br#"{"other":1}"#, 14).await;

    // A and B are annotated with the SAME canonical_field_id (a
    // structurally-different, rename-similar pair); C is annotated with a
    // completely disjoint canonical_field_id (unrelated).
    let shared = metadata_with_cfid("device.temperature");
    let (shared_bytes, shared_sem) = canon(&shared);
    for sch in [&sch_a, &sch_b] {
        reg.append_revision(
            sch,
            &shared,
            &shared_bytes,
            &shared_sem,
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
    let unrelated = metadata_with_cfid("device.humidity");
    let (unrelated_bytes, unrelated_sem) = canon(&unrelated);
    reg.append_revision(
        &sch_c,
        &unrelated,
        &unrelated_bytes,
        &unrelated_sem,
        "kamil",
        ReasonCode::Correction,
        "unrelated annotation",
        1,
        1,
        None,
    )
    .await
    .unwrap();
    assert_ne!(shared_sem, unrelated_sem);

    let feature_keys = semantic_signature(&shared).feature_keys_hex();
    let result = reg.signature_candidates(&feature_keys).await.unwrap();
    let mut candidates = match result {
        SignatureCandidates::Bounded(ids) => ids,
        SignatureCandidates::TooBroad => panic!("must not be too broad for 2 schemas"),
    };
    candidates.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut expected = vec![sch_a.clone(), sch_b.clone()];
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(
        candidates, expected,
        "the union must contain exactly the feature-sharing schemas (never C), \
         proving it's index-derived and not a scan over the whole vault"
    );
}

#[tokio::test]
async fn signature_candidates_returns_too_broad_when_union_exceeds_bound() {
    let (_node, reg, url) = connect().await;
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Seeds the raw posting SET directly with more than the
    // 20,000-candidate bound worth of synthetic members — a legitimate
    // storage-layer boundary test that doesn't require 20,001 real
    // annotated schemas. `signature_candidates` checks the bound BEFORE
    // ever attempting to parse a member as a `SchemaId`, so the synthetic
    // (non-`sch_`-shaped) members below are fine for this path.
    let feature_hex = "aabbcc";
    let key = format!("deblob:sem-sig:{feature_hex}");
    let members: Vec<String> = (0..=MAX_SIGNATURE_CANDIDATES)
        .map(|i| format!("sch_synthetic{i}"))
        .collect();
    for chunk in members.chunks(5000) {
        let _: () = conn.sadd(&key, chunk).await.unwrap();
    }

    let result = reg
        .signature_candidates(&[feature_hex.to_string()])
        .await
        .unwrap();
    assert_eq!(result, SignatureCandidates::TooBroad);
}

#[tokio::test]
async fn signature_candidates_accepts_union_of_exactly_the_bound() {
    let (_node, reg, url) = connect().await;
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // Pins the `>` (not `>=`) boundary in `signature_candidates`
    // (`crates/deblob-redis/src/semantic.rs`): a union of EXACTLY
    // `MAX_SIGNATURE_CANDIDATES` members must be ACCEPTED (real candidates
    // returned), complementing the test above (`MAX_SIGNATURE_CANDIDATES +
    // 1` -> `TooBroad`). Unlike that test, this one hits the `Bounded`
    // return path, which parses every union member as a `SchemaId` before
    // returning, so the synthetic members here must be real, distinct,
    // valid `sch_` ids -- not arbitrary strings.
    let feature_hex = "ddeeff";
    let key = format!("deblob:sem-sig:{feature_hex}");
    let members: Vec<String> = (0..MAX_SIGNATURE_CANDIDATES as u32)
        .map(|i| {
            let mut digest = [0u8; 32];
            digest[0..4].copy_from_slice(&i.to_be_bytes());
            SchemaId::from_digest(&digest).as_str().to_string()
        })
        .collect();
    assert_eq!(members.len(), MAX_SIGNATURE_CANDIDATES);
    for chunk in members.chunks(5000) {
        let _: () = conn.sadd(&key, chunk).await.unwrap();
    }

    let result = reg
        .signature_candidates(&[feature_hex.to_string()])
        .await
        .unwrap();
    match result {
        SignatureCandidates::Bounded(ids) => {
            assert_eq!(
                ids.len(),
                MAX_SIGNATURE_CANDIDATES,
                "a union of exactly the bound must return every candidate, not truncate"
            );
        }
        SignatureCandidates::TooBroad => {
            panic!("a union of exactly MAX_SIGNATURE_CANDIDATES members must not be TooBroad")
        }
    }
}

#[tokio::test]
async fn unannotated_schema_contributes_no_postings() {
    let (_node, reg, url) = connect().await;
    let _sch_id = publish_schema(&reg, br#"{"never_annotated":1}"#, 17).await;

    let postings = snapshot_sem_sig(&url).await;
    assert!(
        postings.is_empty(),
        "an un-annotated schema must never appear in any deblob:sem-sig:* posting"
    );
}

/// CHECKPOINT (spec §5.12): a full rebuild from active pointers must
/// produce postings BYTE-IDENTICAL to what incremental `append_revision`
/// calls already wrote — including through a re-annotation that exercised
/// the SREM/SADD swap.
#[tokio::test]
async fn rebuild_semantic_index_produces_byte_identical_postings_to_incremental() {
    let (_node, reg, url) = connect().await;
    let sch_a = publish_schema(&reg, br#"{"temperature":1}"#, 15).await;
    let sch_b = publish_schema(&reg, br#"{"temperature":1,"meta":{}}"#, 16).await;

    let meta_a1 = metadata_with_cfid("device.temperature");
    let (a1_bytes, a1_sem) = canon(&meta_a1);
    reg.append_revision(
        &sch_a,
        &meta_a1,
        &a1_bytes,
        &a1_sem,
        "kamil",
        ReasonCode::Correction,
        "a1",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    // Re-annotate A (exercises the SREM/SADD swap) and annotate B once.
    let meta_a2 = metadata_with_cfid("device.humidity");
    let (a2_bytes, a2_sem) = canon(&meta_a2);
    reg.append_revision(
        &sch_a,
        &meta_a2,
        &a2_bytes,
        &a2_sem,
        "kamil",
        ReasonCode::Correction,
        "a2",
        2,
        2,
        Some(Etag(1)),
    )
    .await
    .unwrap();

    let meta_b = metadata_with_cfid("device.temperature");
    let (b_bytes, b_sem) = canon(&meta_b);
    reg.append_revision(
        &sch_b,
        &meta_b,
        &b_bytes,
        &b_sem,
        "kamil",
        ReasonCode::Correction,
        "b",
        1,
        1,
        None,
    )
    .await
    .unwrap();

    let before_postings = snapshot_sem_sig(&url).await;
    let before_feature_keys =
        snapshot_feature_keys_json(&url, &[sch_a.clone(), sch_b.clone()]).await;
    assert!(!before_postings.is_empty());

    let rebuilt = reg.rebuild_semantic_index().await.unwrap();
    assert!(rebuilt >= 2);

    let after_postings = snapshot_sem_sig(&url).await;
    let after_feature_keys =
        snapshot_feature_keys_json(&url, &[sch_a.clone(), sch_b.clone()]).await;

    assert_eq!(
        before_postings, after_postings,
        "rebuild must produce byte-identical postings to incremental writes"
    );
    assert_eq!(
        before_feature_keys, after_feature_keys,
        "rebuild must produce byte-identical feature_keys_json fields to incremental writes"
    );
}

/// Task 10 IDF (jr-deblob-similarity-idf-221040): `idf_stats` reports the
/// active-annotated population `N` (`SCARD deblob:sem-active-schemas`) and each
/// requested feature's document frequency (`SCARD deblob:sem-sig:<hex>`) in one
/// atomic snapshot; and `rebuild_semantic_index` reconstructs the population set
/// from the active pointers exactly like the postings.
#[tokio::test]
async fn idf_stats_reports_population_and_document_frequencies() {
    let (_node, reg, url) = connect().await;

    let a = publish_schema(&reg, br#"{"a":1}"#, 40).await;
    let b = publish_schema(&reg, br#"{"b":1}"#, 41).await;
    let c = publish_schema(&reg, br#"{"c":1}"#, 42).await;

    // cfid_alpha annotated on two schemas, cfid_beta on one -> df 2 and 1, N=3.
    for (sch, cfid) in [(&a, "cfid_alpha"), (&b, "cfid_alpha"), (&c, "cfid_beta")] {
        let metadata = metadata_with_cfid(cfid);
        let (bytes, sem_id) = canon(&metadata);
        reg.append_revision(
            sch,
            &metadata,
            &bytes,
            &sem_id,
            "kamil",
            ReasonCode::Correction,
            "initial",
            1,
            1,
            None,
        )
        .await
        .unwrap();
    }

    let alpha_hex = semantic_signature(&metadata_with_cfid("cfid_alpha")).feature_keys_hex();
    let beta_hex = semantic_signature(&metadata_with_cfid("cfid_beta")).feature_keys_hex();
    let query: Vec<String> = alpha_hex.iter().chain(beta_hex.iter()).cloned().collect();

    let (n, dfs) = reg.idf_stats(&query).await.unwrap();
    assert_eq!(n, 3, "three schemas carry an active semantic revision");
    assert_eq!(dfs, vec![2, 1], "cfid_alpha in 2 schemas, cfid_beta in 1");

    // A never-posted feature SCARDs to 0 rather than erroring.
    let (_n, missing) = reg.idf_stats(&["deadbeef".to_string()]).await.unwrap();
    assert_eq!(missing, vec![0]);

    // Wiping the population set drops N to 0; a rebuild restores it (and the
    // postings) from the authoritative active pointers.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = conn.del("deblob:sem-active-schemas").await.unwrap();
    let (n_wiped, _) = reg.idf_stats(&query).await.unwrap();
    assert_eq!(n_wiped, 0, "population set wiped");

    reg.rebuild_semantic_index().await.unwrap();
    let (n_rebuilt, dfs_rebuilt) = reg.idf_stats(&query).await.unwrap();
    assert_eq!(n_rebuilt, 3, "rebuild restores the population set");
    assert_eq!(dfs_rebuilt, vec![2, 1]);
}
