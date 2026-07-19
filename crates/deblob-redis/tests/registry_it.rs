use std::collections::HashMap;
use std::sync::Arc;

use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_redis::{RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// Builds a valid `SchemaRecord`. Matches the brief's `sample_record()`
/// helper contract: schema_id from a digest, a family_id, version,
/// canonical string, canonicalizer "deblob-canon-v1", provenance json.
fn sample_record() -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[1u8; 32]),
        family_id: FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: r#"{"t":"obj","f":{"id":{"t":"str"}}}"#.to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "test"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

/// Variant of `sample_record()` with a caller-chosen digest and family, for
/// tests that need multiple distinct schemas.
fn record_with(digest: [u8; 32], family_id: FamilyId) -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&digest),
        family_id,
        version: FamilyVersion(1),
        canonical: r#"{"t":"obj","f":{"id":{"t":"str"}}}"#.to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "test"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
        value_profile_ref: None,
        value_profile_summary: None,
    }
}

#[tokio::test]
async fn publish_is_atomic_and_write_once() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let rec = sample_record(); // helper building a SchemaRecord
    let cand = CandidateId::from_digest(&[9u8; 32]);
    let v1 = reg
        .publish(rec.clone(), &cand, "bucket:3:abc", &[], "kamil", "initial")
        .await
        .unwrap();
    assert_eq!(v1, FamilyVersion(1), "first publish allocates version 1");

    // schema readable, alias resolves, republish identical = idempotent OK
    // and returns the SAME authoritative version, never a new one.
    let stored = reg.get_schema(&rec.schema_id).await.unwrap().unwrap();
    assert_eq!(stored.version, v1);
    assert_eq!(reg.get_alias(&cand).await.unwrap().unwrap(), rec.schema_id);
    let v2 = reg
        .publish(rec.clone(), &cand, "bucket:3:abc", &[], "kamil", "retry")
        .await
        .unwrap();
    assert_eq!(
        v2, v1,
        "idempotent republish must return the SAME authoritative version"
    );

    // same schema_id with a genuinely DIFFERENT canonical identity =
    // fatal ImmutabilityViolation (§6) — this is a real identity change,
    // not merely different provenance/version.
    let mut tampered = rec.clone();
    tampered.canonical = "{\"t\":\"obj\",\"f\":{}}".into();
    let err = reg
        .publish(tampered, &cand, "bucket:3:abc", &[], "kamil", "tamper")
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::ImmutabilityViolation(_)));
}

#[tokio::test]
async fn republish_with_different_provenance_is_idempotent() {
    // Fix A: the immutability check must compare CANONICAL IDENTITY only
    // (canonical + canonicalizer), not the whole record. Republishing the
    // SAME schema_id with the SAME canonical but DIFFERENT provenance (a
    // fresh timestamp, as a real retry would produce) must succeed and
    // must NOT raise ImmutabilityViolation. Fix B: it must also return the
    // SAME authoritative version — a caller-guessed `version` on the retry
    // must be ignored, never trusted for storage.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let cand = CandidateId::from_digest(&[40u8; 32]);
    let rec = sample_record();
    let v1 = reg
        .publish(rec.clone(), &cand, "bucket:4:abc", &[], "kamil", "initial")
        .await
        .unwrap();
    assert_eq!(v1, FamilyVersion(1));

    let mut retried = rec.clone();
    retried.provenance = serde_json::json!({"source": "test", "first_seen_ms": 999});
    retried.version = FamilyVersion(999); // caller's stale/guessed version — must be ignored

    let v2 = reg
        .publish(
            retried,
            &cand,
            "bucket:4:abc",
            &[],
            "kamil",
            "retry-with-new-provenance",
        )
        .await
        .unwrap();
    assert_eq!(
        v2, v1,
        "republish with different provenance must return the SAME authoritative version"
    );

    let stored = reg.get_schema(&rec.schema_id).await.unwrap().unwrap();
    assert_eq!(
        stored.version, v1,
        "stored record must carry the authoritative version, not the caller's guess"
    );
}

#[tokio::test]
async fn alias_reassignment_rejected() {
    // publish cand→sch_A, then attempt cand→sch_B → Conflict
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let cand = CandidateId::from_digest(&[7u8; 32]);

    let rec_a = record_with([10u8; 32], FamilyId::new_v7());
    reg.publish(
        rec_a.clone(),
        &cand,
        "bucket:1:aaa",
        &[],
        "kamil",
        "publish-a",
    )
    .await
    .unwrap();

    // different schema_id, same alias_from (cand) → write-once alias rejects
    let rec_b = record_with([11u8; 32], FamilyId::new_v7());
    let err = reg
        .publish(
            rec_b.clone(),
            &cand,
            "bucket:1:bbb",
            &[],
            "kamil",
            "publish-b",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::Conflict(_)));
    assert_ne!(rec_a.schema_id, rec_b.schema_id);

    // alias still resolves to the original, unreassigned target
    assert_eq!(
        reg.get_alias(&cand).await.unwrap().unwrap(),
        rec_a.schema_id
    );
}

#[tokio::test]
async fn family_versions_allocate_atomically() {
    // two concurrent publishes to same family → versions 1 and 2, never duplicate
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = Arc::new(
        RedisRegistry::connect(
            &url,
            RedisOpts {
                allow_volatile: true,
            },
        )
        .await
        .unwrap(),
    );

    let family = FamilyId::new_v7();
    let rec1 = record_with([20u8; 32], family.clone());
    let rec2 = record_with([21u8; 32], family.clone());
    let cand1 = CandidateId::from_digest(&[30u8; 32]);
    let cand2 = CandidateId::from_digest(&[31u8; 32]);

    let reg_a = reg.clone();
    let rec1_c = rec1.clone();
    let cand1_c = cand1.clone();
    let task_a = tokio::spawn(async move {
        reg_a
            .publish(
                rec1_c,
                &cand1_c,
                "bucket:2:aaa",
                &[],
                "kamil",
                "concurrent-a",
            )
            .await
    });

    let reg_b = reg.clone();
    let rec2_c = rec2.clone();
    let cand2_c = cand2.clone();
    let task_b = tokio::spawn(async move {
        reg_b
            .publish(
                rec2_c,
                &cand2_c,
                "bucket:2:bbb",
                &[],
                "kamil",
                "concurrent-b",
            )
            .await
    });

    // join two tasks together
    let (res_a, res_b) = tokio::join!(task_a, task_b);
    let ver_a = res_a.unwrap().unwrap();
    let ver_b = res_b.unwrap().unwrap();

    // the two RETURNED versions must be exactly {1, 2} — never a duplicate,
    // never a caller-guessed value.
    let mut returned = vec![ver_a.0, ver_b.0];
    returned.sort();
    assert_eq!(
        returned,
        vec![1, 2],
        "the two returned versions must be exactly {{1,2}}"
    );

    // inspect the family hash directly: HINCRBY must have produced two
    // distinct, consecutive versions — never a duplicate.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let hash: HashMap<String, String> = conn
        .hgetall(format!("deblob:family:{}", family.as_str()))
        .await
        .unwrap();

    assert_eq!(hash.get("next_version").map(String::as_str), Some("2"));

    let mut versions: Vec<String> = vec![
        hash.get("v:1").cloned().expect("version 1 must exist"),
        hash.get("v:2").cloned().expect("version 2 must exist"),
    ];
    versions.sort();

    let mut expected = vec![
        rec1.schema_id.as_str().to_string(),
        rec2.schema_id.as_str().to_string(),
    ];
    expected.sort();

    assert_eq!(
        versions, expected,
        "{{1,2}} must map to the two distinct schema ids"
    );
}

#[tokio::test]
async fn get_family_and_list_family_versions_reflect_published_versions() {
    // publish two schemas to the SAME family → get_family reports the
    // current (highest) version, list_family_versions reports both 1 and 2.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let family = FamilyId::new_v7();
    let cand1 = CandidateId::from_digest(&[50u8; 32]);
    let cand2 = CandidateId::from_digest(&[51u8; 32]);
    let rec1 = record_with([60u8; 32], family.clone());
    let rec2 = record_with([61u8; 32], family.clone());

    reg.publish(rec1.clone(), &cand1, "bucket:5:aaa", &[], "kamil", "v1")
        .await
        .unwrap();
    reg.publish(rec2.clone(), &cand2, "bucket:5:bbb", &[], "kamil", "v2")
        .await
        .unwrap();

    let fam = reg
        .get_family(&family)
        .await
        .unwrap()
        .expect("family must exist after two publishes");
    assert_eq!(fam.family_id, family);
    assert_eq!(fam.current_version, FamilyVersion(2));

    let versions = reg.list_family_versions(&family).await.unwrap();
    assert_eq!(versions, vec![FamilyVersion(1), FamilyVersion(2)]);
}

#[tokio::test]
async fn get_family_unknown_returns_none_not_error() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let unknown = FamilyId::new_v7();
    assert_eq!(reg.get_family(&unknown).await.unwrap(), None);
    assert_eq!(
        reg.list_family_versions(&unknown).await.unwrap(),
        Vec::<FamilyVersion>::new()
    );
}

#[tokio::test]
async fn list_schemas_returns_all_published_schemas() {
    // fix1 baseline: publish 3 schemas, list_schemas must return exactly
    // those 3 (in however many pages), never fewer, never an empty page
    // with data still outstanding.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let mut published = Vec::new();
    for i in 0..3u8 {
        let rec = record_with([100 + i; 32], FamilyId::new_v7());
        let cand = CandidateId::from_digest(&[110 + i; 32]);
        reg.publish(
            rec.clone(),
            &cand,
            &format!("bucket:list:{i}"),
            &[],
            "kamil",
            "publish",
        )
        .await
        .unwrap();
        published.push(rec.schema_id);
    }

    let (data, next) = reg.list_schemas(None, 50).await.unwrap();
    assert_eq!(
        data.len(),
        3,
        "a single page with limit=50 must return all 3 published schemas, got: {data:?}"
    );
    assert!(
        next.is_none(),
        "a page that returns every schema must not carry a next_cursor"
    );

    let mut got: Vec<String> = data
        .iter()
        .map(|r| r.schema_id.as_str().to_string())
        .collect();
    got.sort();
    let mut expected: Vec<String> = published.iter().map(|id| id.as_str().to_string()).collect();
    expected.sort();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn list_schemas_never_returns_empty_page_amid_sparse_keyspace() {
    // fix1 regression test, pinned against the exact live-reproduced
    // defect: `list_schemas` used to `SCAN MATCH "deblob:schema:*"` over
    // the WHOLE keyspace, so a `SCAN COUNT` batch could land entirely on
    // unrelated `deblob:*` keys (candidates/evidence/index/semantic, all
    // sharing the same prefix space) and return `{"data":[],"next_cursor":
    // "<nonzero>"}` even though schemas existed. Publish 3 schemas, then
    // sprinkle hundreds of decoy `deblob:*` keys of OTHER kinds around
    // them, and page through with a small (limit=1) COUNT hint: every
    // non-final page must be non-empty.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let mut published = Vec::new();
    for i in 0..3u8 {
        let rec = record_with([120 + i; 32], FamilyId::new_v7());
        let cand = CandidateId::from_digest(&[130 + i; 32]);
        reg.publish(
            rec.clone(),
            &cand,
            &format!("bucket:sparse:{i}"),
            &[],
            "kamil",
            "publish",
        )
        .await
        .unwrap();
        published.push(rec.schema_id);
    }

    // Sprinkle decoys of other `deblob:*` key kinds — the same shape the
    // live defect report described (semantic-neighbor postings, evidence,
    // etc.) — so schema keys are a tiny minority of the whole keyspace.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for i in 0..500u32 {
        let _: () = conn
            .set(format!("deblob:sem-sig:{i:04}"), "decoy")
            .await
            .unwrap();
    }

    let mut collected = Vec::new();
    let mut cursor = None;
    loop {
        let (data, next) = reg.list_schemas(cursor.clone(), 1).await.unwrap();
        assert!(
            !data.is_empty() || next.is_none(),
            "an empty page must only ever occur once nothing is left to page (next={next:?})"
        );
        collected.extend(data.into_iter().map(|r| r.schema_id));
        if next.is_none() {
            break;
        }
        cursor = next;
    }

    let mut got: Vec<String> = collected.iter().map(|id| id.as_str().to_string()).collect();
    got.sort();
    let mut expected: Vec<String> = published.iter().map(|id| id.as_str().to_string()).collect();
    expected.sort();
    assert_eq!(
        got, expected,
        "list_schemas must return exactly the published schemas across pages, decoys never counted"
    );
}

#[tokio::test]
async fn list_schemas_includes_promoted_monoid_schema() {
    // A promoted schema is published with canonicalizer "deblob-monoid-v1"
    // (`deblob_monoid::GENERALIZER`, exercised end-to-end via the real
    // Promoter in `crates/deblob/tests/promote_resolve_it.rs`) rather than
    // the raw "deblob-canon-v1" tag — `publish`/`list_schemas` must not
    // care which; fix1's schemas-listing index is populated
    // unconditionally by the same Lua script regardless of canonicalizer.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();

    let mut promoted = sample_record();
    promoted.canonicalizer = "deblob-monoid-v1".to_string();
    let cand = CandidateId::from_digest(&[80u8; 32]);
    reg.publish(
        promoted.clone(),
        &cand,
        "bucket:promoted:1",
        &[],
        "kamil",
        "promote",
    )
    .await
    .unwrap();

    let (data, _) = reg.list_schemas(None, 50).await.unwrap();
    assert!(
        data.iter()
            .any(|r| r.schema_id == promoted.schema_id && r.canonicalizer == "deblob-monoid-v1"),
        "the promoted (monoid-canonicalizer) schema must appear in the listing, got: {data:?}"
    );
}

#[tokio::test]
async fn refuses_volatile_redis_without_flag() {
    // container has no AOF → connect with allow_volatile: false must error
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );

    let err = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: false,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CoreError::RegistryUnavailable(_)));

    // with the flag, the same volatile instance is accepted
    RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();
}
