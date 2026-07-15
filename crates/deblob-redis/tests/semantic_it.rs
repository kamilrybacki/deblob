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

use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId, SemanticId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_core::revision::{Etag, ReasonCode, SemError};
use deblob_core::semantic::{
    FieldEntry, FieldSemantics, PathSegment, SemanticMetadata, Unit, UnitSystem,
};
use deblob_fingerprint::{canonical_bytes, fingerprint, parse_bounded, shape_of, Limits};
use deblob_redis::{RedisOpts, RedisRegistry};
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
    let second = outcome.into_revision();
    assert_eq!(second.previous_revision_id, Some(first.revision_id.clone()));
    assert_ne!(second.revision_id, first.revision_id);

    // Pointer advanced.
    let (active_meta, active_sem, etag) = reg.active_semantic(&sch_id).await.unwrap().unwrap();
    assert_eq!(active_meta, changed);
    assert_eq!(active_sem, changed_sem);
    assert_eq!(etag, Etag(2));

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
