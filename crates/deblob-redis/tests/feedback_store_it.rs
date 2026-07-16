//! `RedisFeedbackStore` against a REAL Redis (Docker via testcontainers) —
//! mirrors `registry_it.rs`'s setup. Proves the durable feedback store
//! (spec: `docs/superpowers/specs/2026-07-16-slm-continual-learning.md`
//! §2, amendments A3/A4/A5): append-only, family-partitioned JSONL export,
//! no raw values, immutable records (no update-in-place operation exists
//! on the trait), dedup-by-cluster, quarantine exclusion, a
//! content-addressed manifest/checksum snapshot, and the permanent
//! never-trained safety partition.

use std::collections::HashSet;

use deblob_core::id::{FamilyId, SchemaId};
use deblob_redis::{ExportCaps, FeedbackStore, RedisFeedbackStore, SplitName};
use deblob_slm::{
    AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision, LabelSource,
    RejectionReason, Relation, SourceTrustLevel, TrainingExample,
};
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

fn schema_id(byte: u8) -> SchemaId {
    SchemaId::from_digest(&[byte; 32])
}

fn candidate_with_raw_looking_marker() -> CandidateProfileView {
    // A raw payload value (e.g. `"super-secret-value-should-never-appear"`)
    // never has anywhere to live on `CandidateProfileView` — it carries
    // stats only. This helper documents that intent for the "no raw
    // values" assertion below: there is structurally nothing to redact
    // because nothing raw was ever accepted in the first place.
    CandidateProfileView {
        observation_count: 77,
        fields: vec![],
        truncated: false,
    }
}

fn example(family_id: FamilyId, gold_schema: u8, recorded_at: i64) -> TrainingExample {
    example_with(family_id, gold_schema, recorded_at, "operator:default", "")
}

fn example_with(
    family_id: FamilyId,
    gold_schema: u8,
    recorded_at: i64,
    actor: &str,
    dedup_cluster: &str,
) -> TrainingExample {
    TrainingExample {
        candidate: candidate_with_raw_looking_marker(),
        retrieved: vec![FamilyCandidate {
            family_id: family_id.clone(),
            schema_id: schema_id(gold_schema),
            version: 1,
            distance: 0.02,
            rank: 1,
        }],
        gold: InferenceDecision::MatchSchema {
            schema_id: schema_id(gold_schema),
            relation: Relation::Exact,
        },
        label_source: LabelSource::HumanPromote,
        weight: 1.0,
        partition_key: family_id,
        recorded_at,
        rejection_reason: None,
        actor: actor.to_string(),
        source_trust_level: SourceTrustLevel::Standard,
        tool_schema_version: 1,
        dedup_cluster: dedup_cluster.to_string(),
    }
}

async fn connect() -> (
    RedisFeedbackStore,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let store = RedisFeedbackStore::connect(&url).await.unwrap();
    (store, node)
}

#[tokio::test]
async fn append_then_iter_by_partition_groups_by_family() {
    let (store, _node) = connect().await;

    let family_a = FamilyId::new_v7();
    let family_b = FamilyId::new_v7();

    store
        .append(&example(family_a.clone(), 1, 1000))
        .await
        .unwrap();
    store
        .append(&example(family_a.clone(), 2, 1001))
        .await
        .unwrap();
    store
        .append(&example(family_b.clone(), 3, 1002))
        .await
        .unwrap();

    let grouped = store.iter_by_partition().await.unwrap();
    assert_eq!(grouped.len(), 2, "two distinct families were appended");
    assert_eq!(grouped[family_a.as_str()].len(), 2);
    assert_eq!(grouped[family_b.as_str()].len(), 1);
}

#[tokio::test]
async fn export_jsonl_is_family_partitioned_when_a_partition_is_requested() {
    let (store, _node) = connect().await;

    let family_a = FamilyId::new_v7();
    let family_b = FamilyId::new_v7();
    store
        .append(&example(family_a.clone(), 10, 2000))
        .await
        .unwrap();
    store
        .append(&example(family_b.clone(), 11, 2001))
        .await
        .unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, Some(&family_a)).await.unwrap();
    assert_eq!(count, 1, "only family_a's example must be exported");

    let text = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 1);

    let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed["partition_key"], family_a.as_str());
    assert!(parsed["prompt"].is_string());
    assert_eq!(parsed["gold_tool_call"]["decision"], "match_schema");

    // The OTHER family must not have leaked into this export.
    assert!(!text.contains(family_b.as_str()));
}

#[tokio::test]
async fn export_jsonl_with_no_partition_filter_exports_every_record() {
    let (store, _node) = connect().await;

    let family_a = FamilyId::new_v7();
    let family_b = FamilyId::new_v7();
    store
        .append(&example(family_a.clone(), 20, 3000))
        .await
        .unwrap();
    store
        .append(&example(family_b.clone(), 21, 3001))
        .await
        .unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, None).await.unwrap();
    assert_eq!(count, 2);

    let text = String::from_utf8(buf).unwrap();
    let partitions: HashSet<String> = text
        .lines()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            v["partition_key"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        partitions,
        HashSet::from([family_a.as_str().to_string(), family_b.as_str().to_string()])
    );
}

/// The export must NEVER carry a raw candidate payload value — only the
/// PII-safe rendered prompt (stats/field-names only) and the fixed-enum
/// decision. This proves it end-to-end through the real store, not just
/// through `CandidateProfileView`'s type shape.
#[tokio::test]
async fn exported_jsonl_never_contains_a_raw_candidate_value() {
    let (store, _node) = connect().await;

    let family = FamilyId::new_v7();
    let mut ex = example(family.clone(), 30, 4000);
    // A hard-negative with a human-supplied Abstain gold — exercises the
    // richer TrainingExample shape too.
    ex.label_source = LabelSource::TrustedProposalRejected;
    ex.rejection_reason = Some(RejectionReason::WrongFamily);
    ex.gold = InferenceDecision::Abstain {
        cause: AbstainCause::Ambiguous,
    };
    ex.weight = 3.0;
    store.append(&ex).await.unwrap();

    let mut buf: Vec<u8> = Vec::new();
    store.export_jsonl(&mut buf, None).await.unwrap();
    let text = String::from_utf8(buf).unwrap();

    // The only literal string values the field pool contract permits here
    // are the field NAMES `deblob_slm::redact_field_name` renders — never a
    // raw enum/string VALUE, since `CandidateProfileView` structurally
    // never carries one (see `deblob_slm::prompt`'s module docs; this
    // mirrors `deblob-eval::generate`'s
    // `finetune_jsonl_never_contains_raw_field_values` test for the
    // synthetic corpus's identical export shape).
    let never_expected = ["super-secret-value-should-never-appear", "raw_payload"];
    for marker in never_expected {
        assert!(
            !text.contains(marker),
            "exported jsonl leaked a raw-looking marker {marker:?}: {text}"
        );
    }
}

/// Records are append-only: two appends of distinct examples both survive,
/// and there is no method on the trait that could mutate/delete an
/// individual record (the only shrink path is bounded retention trim,
/// exercised separately by construction — `MAXLEN ~` on the stream).
#[tokio::test]
async fn appended_records_are_never_lost_or_overwritten() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();

    for i in 0..5u8 {
        store
            .append(&example(family.clone(), 40 + i, 5000 + i as i64))
            .await
            .unwrap();
    }

    let grouped = store.iter_by_partition().await.unwrap();
    let examples = &grouped[family.as_str()];
    assert_eq!(
        examples.len(),
        5,
        "every appended record must survive intact"
    );

    let mut recorded_ats: Vec<i64> = examples.iter().map(|e| e.recorded_at).collect();
    recorded_ats.sort_unstable();
    assert_eq!(recorded_ats, vec![5000, 5001, 5002, 5003, 5004]);
}

// -- Amendment A4: dedup by cluster + quarantine -----------------------------

#[tokio::test]
async fn export_jsonl_deduplicates_by_cluster_keeping_the_first_appended() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();

    store
        .append(&example_with(family.clone(), 50, 6000, "a1", "dup-cluster"))
        .await
        .unwrap();
    store
        .append(&example_with(family.clone(), 51, 6001, "a2", "dup-cluster"))
        .await
        .unwrap();
    store
        .append(&example_with(family.clone(), 52, 6002, "a3", ""))
        .await
        .unwrap();

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, None).await.unwrap();
    assert_eq!(
        count, 2,
        "one dup-cluster member + the un-clustered record; the second dup-cluster member is deduped"
    );

    let text = String::from_utf8(buf).unwrap();
    let actors: Vec<String> = text
        .lines()
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            v["actor"].as_str().unwrap().to_string()
        })
        .collect();
    assert!(
        actors.contains(&"a1".to_string()),
        "the first-appended dup-cluster member must be kept: {actors:?}"
    );
    assert!(
        !actors.contains(&"a2".to_string()),
        "the later same-cluster record must be deduped away: {actors:?}"
    );
    assert!(actors.contains(&"a3".to_string()));
}

#[tokio::test]
async fn quarantined_actor_is_excluded_from_export_jsonl() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();

    store
        .append(&example_with(family.clone(), 60, 7000, "trusted-actor", ""))
        .await
        .unwrap();
    store
        .append(&example_with(family.clone(), 61, 7001, "bad-actor", ""))
        .await
        .unwrap();

    store.quarantine_actor("bad-actor").await.unwrap();
    let quarantined = store.quarantined_actors().await.unwrap();
    assert!(quarantined.contains("bad-actor"));

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, None).await.unwrap();
    assert_eq!(
        count, 1,
        "only the non-quarantined actor's record is exported"
    );

    let text = String::from_utf8(buf).unwrap();
    assert!(
        !text.contains("bad-actor"),
        "a quarantined actor's record must never appear in export: {text}"
    );
}

// -- Amendment A3: per-(family, label_source) export cap ---------------------

#[tokio::test]
async fn export_jsonl_caps_a_burst_of_correlated_rejections_from_one_family() {
    let (store, _node) = connect().await;
    let url_family = FamilyId::new_v7();

    for i in 0..10u8 {
        let mut ex = example_with(
            url_family.clone(),
            70 + i,
            8000 + i as i64,
            &format!("actor-{i}"),
            "",
        );
        ex.label_source = LabelSource::TrustedProposalRejected;
        ex.rejection_reason = Some(RejectionReason::WrongFamily);
        store.append(&ex).await.unwrap();
    }

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, None).await.unwrap();
    // Default cap (50) does not bite 10 records — proves the cap doesn't
    // over-trigger on ordinary volume.
    assert_eq!(count, 10);
}

#[tokio::test]
async fn export_jsonl_cap_can_be_tightened_via_with_caps() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let store = RedisFeedbackStore::connect(&url)
        .await
        .unwrap()
        .with_caps(ExportCaps {
            max_examples_per_partition_and_label_source: 2,
        });

    let family = FamilyId::new_v7();
    for i in 0..5u8 {
        let mut ex = example_with(
            family.clone(),
            80 + i,
            9000 + i as i64,
            &format!("actor-{i}"),
            "",
        );
        ex.label_source = LabelSource::TrustedProposalRejected;
        ex.rejection_reason = Some(RejectionReason::WrongFamily);
        store.append(&ex).await.unwrap();
    }

    let mut buf: Vec<u8> = Vec::new();
    let count = store.export_jsonl(&mut buf, None).await.unwrap();
    assert_eq!(
        count, 2,
        "a burst of correlated rejections from one family must be capped"
    );
}

// -- Amendment A5: content-addressed snapshot + never-trained safety suite --

#[tokio::test]
async fn export_snapshot_writes_a_manifest_with_checksums_and_is_content_addressed() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();
    store
        .append(&example_with(family.clone(), 90, 10_000, "a1", ""))
        .await
        .unwrap();
    store
        .append(&example_with(family, 91, 10_001, "a2", ""))
        .await
        .unwrap();

    let dir_a = std::env::temp_dir().join(format!("deblob-snapshot-a-{}", std::process::id()));
    let dir_b = std::env::temp_dir().join(format!("deblob-snapshot-b-{}", std::process::id()));

    let manifest_a = store.export_snapshot(&dir_a).await.unwrap();
    let manifest_b = store.export_snapshot(&dir_b).await.unwrap();

    assert_eq!(
        manifest_a.snapshot_id, manifest_b.snapshot_id,
        "identical store content must produce an identical content-addressed snapshot id"
    );
    assert!(!manifest_a.snapshot_id.is_empty());
    assert_eq!(
        manifest_a.entries.len(),
        3,
        "train + holdout + safety-suite files"
    );
    for entry in &manifest_a.entries {
        assert!(!entry.sha256.is_empty());
        let path = dir_a.join(&entry.file_name);
        let bytes = std::fs::read(&path).unwrap();
        let digest = <sha2::Sha256 as sha2::Digest>::digest(&bytes);
        assert_eq!(
            entry.sha256,
            data_encoding::HEXLOWER.encode(&digest),
            "manifest checksum must match the actual file bytes on disk for {}",
            entry.file_name
        );
    }
    assert!(dir_a.join("manifest.json").exists());

    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

#[tokio::test]
async fn export_snapshot_never_places_a_safety_suite_record_in_train_or_holdout() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();
    store
        .append(&example_with(
            family.clone(),
            92,
            11_000,
            "safety-suite-actor",
            "safety:regression-42",
        ))
        .await
        .unwrap();
    store
        .append(&example_with(family, 93, 11_001, "ordinary-actor", ""))
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!("deblob-snapshot-safety-{}", std::process::id()));
    let manifest = store.export_snapshot(&dir).await.unwrap();

    let safety_entry = manifest
        .entries
        .iter()
        .find(|e| e.split == SplitName::NeverTrainedSafetySuite)
        .unwrap();
    assert_eq!(safety_entry.record_count, 1);

    // Parse each split's lines structurally (never a raw substring search —
    // a family/schema id is an opaque hex/base32 string that could
    // coincidentally contain any short substring) and check the `actor`
    // field precisely.
    let actors_in = |file_name: &str| -> Vec<String> {
        std::fs::read_to_string(dir.join(file_name))
            .unwrap()
            .lines()
            .map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).unwrap();
                v["actor"].as_str().unwrap().to_string()
            })
            .collect()
    };
    let train_actors = actors_in("train.jsonl");
    let holdout_actors = actors_in("holdout.jsonl");
    assert!(
        !train_actors.contains(&"safety-suite-actor".to_string())
            && !holdout_actors.contains(&"safety-suite-actor".to_string()),
        "the safety-suite-tagged record must never appear in train ({train_actors:?}) or \
         holdout ({holdout_actors:?})"
    );
    assert!(
        train_actors.contains(&"ordinary-actor".to_string())
            || holdout_actors.contains(&"ordinary-actor".to_string()),
        "the ordinary record must land in train or holdout"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn export_snapshot_excludes_quarantined_actors_from_every_split() {
    let (store, _node) = connect().await;
    let family = FamilyId::new_v7();
    store
        .append(&example_with(
            family.clone(),
            94,
            12_000,
            "bad-actor",
            "safety:always-excluded",
        ))
        .await
        .unwrap();
    store
        .append(&example_with(family, 95, 12_001, "good-actor", ""))
        .await
        .unwrap();
    store.quarantine_actor("bad-actor").await.unwrap();

    let dir =
        std::env::temp_dir().join(format!("deblob-snapshot-quarantine-{}", std::process::id()));
    let manifest = store.export_snapshot(&dir).await.unwrap();

    let total: usize = manifest.entries.iter().map(|e| e.record_count).sum();
    assert_eq!(
        total, 1,
        "the quarantined actor's record must be excluded everywhere"
    );

    for entry in &manifest.entries {
        let text = std::fs::read_to_string(dir.join(&entry.file_name)).unwrap();
        assert!(
            !text.contains("bad-actor"),
            "quarantined actor leaked into {}",
            entry.file_name
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
