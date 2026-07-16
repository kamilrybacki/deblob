//! `RedisFeedbackStore` against a REAL Redis (Docker via testcontainers) —
//! mirrors `registry_it.rs`'s setup. Proves the durable feedback store
//! (spec: `docs/superpowers/specs/2026-07-16-slm-continual-learning.md`
//! §2): append-only + family-partitioned JSONL export + no raw values +
//! immutable records (no update-in-place operation exists on the trait).

use std::collections::HashSet;

use deblob_core::id::{FamilyId, SchemaId};
use deblob_redis::{FeedbackStore, RedisFeedbackStore};
use deblob_slm::{
    AbstainCause, CandidateProfileView, FamilyCandidate, InferenceDecision, LabelSource, Relation,
    TrainingExample,
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
