//! Task 8: bucketed structural index, offline rebuild, and consistency
//! checker. Builds on the `RedisRegistry` published in Task 7's
//! `registry_it.rs` — see that file for the base publish/alias/version
//! invariants; this file is scoped to the structural index alone.

use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_fingerprint::{
    canonical_bytes, fieldband, fingerprint, parse_bounded, shape_of, summarize, Limits,
};
use deblob_redis::{bucket_key, RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// Shapes a real JSON document into a publishable `(SchemaRecord, bucket)`
/// pair the same way a real caller would: parse -> shape -> canonicalize ->
/// fingerprint -> summarize -> bucket_key. Keeps the integration tests
/// honest about the actual publish path instead of hand-waving a fake
/// schema_id/bucket pair that a real caller could never have produced.
fn record_and_bucket(json: &[u8], family_id: FamilyId) -> (SchemaRecord, String) {
    let node = parse_bounded(json, &Limits::default()).unwrap();
    let shape = shape_of(&node);
    let canonical = String::from_utf8(canonical_bytes(&shape)).unwrap();
    let digest = fingerprint(&shape);
    let schema_id = SchemaId::from_digest(&digest);
    let bucket = bucket_key(&summarize(&shape));
    let record = SchemaRecord {
        schema_id,
        family_id,
        version: FamilyVersion(1),
        canonical,
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "index_it"}),
        semantic: None,
        semantic_fingerprint: None,
        privacy_class: None,
    };
    (record, bucket)
}

#[test]
fn bucket_key_is_stable() {
    // Golden string, exercised again here through the crate's public
    // re-export (see also the unit test pinned in `index.rs`).
    let summary = deblob_fingerprint::ShapeSummary {
        top_level_fields: 3,
        depth: 2,
        top_keys_sorted: vec!["a".to_string(), "b".to_string(), "c".to_string()],
    };
    assert_eq!(bucket_key(&summary), "deblob:index:4:2:8badde10");
}

#[tokio::test]
async fn resolve_after_publish() {
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

    let (record, bucket) = record_and_bucket(br#"{"id":"x","name":"y"}"#, FamilyId::new_v7());
    let cand = CandidateId::from_digest(&[55u8; 32]);

    reg.publish(record.clone(), &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();

    let found = reg
        .resolve_structural(&bucket, &record.schema_id)
        .await
        .unwrap();
    assert_eq!(found, Some(record.schema_id));
}

#[tokio::test]
async fn rebuild_restores_resolution() {
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

    let (record, bucket) = record_and_bucket(br#"{"id":"x","count":1}"#, FamilyId::new_v7());
    let cand = CandidateId::from_digest(&[56u8; 32]);

    reg.publish(record.clone(), &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();

    // Sanity: resolvable right after publish.
    assert_eq!(
        reg.resolve_structural(&bucket, &record.schema_id)
            .await
            .unwrap(),
        Some(record.schema_id.clone())
    );

    // Wipe ONLY the derived index keys, via raw KEYS/DEL — the schema
    // record itself (deblob:schema:*) is untouched, so this simulates
    // losing/corrupting the index without losing the authoritative vault.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let index_keys: Vec<String> = conn.keys("deblob:index:*").await.unwrap();
    assert!(
        !index_keys.is_empty(),
        "publish must have created at least one deblob:index:* key"
    );
    for key in &index_keys {
        let _: () = conn.del(key).await.unwrap();
    }

    // Index gone -> resolution now misses.
    assert_eq!(
        reg.resolve_structural(&bucket, &record.schema_id)
            .await
            .unwrap(),
        None,
        "resolve_structural must miss once the index keys are gone"
    );

    // Rebuild purely from deblob:schema:* -> resolution restored.
    let reindexed = reg.rebuild_index().await.unwrap();
    assert!(
        reindexed >= 1,
        "rebuild_index must report at least the one schema published above"
    );

    assert_eq!(
        reg.resolve_structural(&bucket, &record.schema_id)
            .await
            .unwrap(),
        Some(record.schema_id),
        "resolve_structural must find the schema again after rebuild_index"
    );
}

#[tokio::test]
async fn verify_reports_poisoned_index() {
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

    let (record, bucket) = record_and_bucket(br#"{"id":"x","flag":true}"#, FamilyId::new_v7());
    let cand = CandidateId::from_digest(&[57u8; 32]);

    reg.publish(record.clone(), &cand, &bucket, &[], "kamil", "publish")
        .await
        .unwrap();

    // A consistent vault must report no problems before poisoning it.
    let clean = reg.verify_index().await.unwrap();
    assert!(
        clean.is_empty(),
        "freshly published vault must be consistent, got: {clean:?}"
    );

    // Manually SADD a bogus member into a real deblob:index:* bucket,
    // pointing at a schema id that was never published.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let poison_member = "deadbeef=sch_doesnotexist";
    let _: () = conn.sadd(&bucket, poison_member).await.unwrap();

    let problems = reg.verify_index().await.unwrap();
    assert!(
        problems.iter().any(|p| p.contains("sch_doesnotexist")),
        "verify_index must report the poisoned member, got: {problems:?}"
    );
}

/// deblob-p2ab Task 3 recall fix, exercised against a REAL Redis: two
/// schemas with the SAME field-count band + depth but DIFFERENT top-level
/// key names land in DIFFERENT exact `bucket_key`s (different `reqhash8`).
/// `list_families_in_buckets` (exact-key lookup, the pre-fix retrieval
/// path) can only ever find the one whose exact bucket it's given —
/// `list_families_by_band_depth` (widened, name-blind `SCAN MATCH
/// "deblob:index:{band}:{depth}:*"` discovery) must find BOTH.
#[tokio::test]
async fn list_families_by_band_depth_finds_renamed_bucket() {
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

    let (record_a, bucket_a) =
        record_and_bucket(br#"{"widgetCount":1,"itemName":"x"}"#, FamilyId::new_v7());
    let (record_b, bucket_b) =
        record_and_bucket(br#"{"widget_count":1,"item_name":"x"}"#, FamilyId::new_v7());
    assert_ne!(
        bucket_a, bucket_b,
        "fixture must land in different reqhash8 buckets to exercise the fix"
    );

    let cand_a = CandidateId::from_digest(&[71u8; 32]);
    let cand_b = CandidateId::from_digest(&[72u8; 32]);
    reg.publish(
        record_a.clone(),
        &cand_a,
        &bucket_a,
        &[],
        "kamil",
        "publish",
    )
    .await
    .unwrap();
    reg.publish(
        record_b.clone(),
        &cand_b,
        &bucket_b,
        &[],
        "kamil",
        "publish",
    )
    .await
    .unwrap();

    // OLD path: an exact-key lookup restricted to bucket_a's own key only
    // ever finds record_a -- the defect, pinned here as a direct contrast.
    // `SchemaId` has no `Ord`/`Hash` impl, so membership is checked by its
    // string form rather than via a `BTreeSet`/`HashSet`.
    let exact = reg
        .list_families_in_buckets(std::slice::from_ref(&bucket_a))
        .await
        .unwrap();
    let exact_ids: Vec<String> = exact
        .iter()
        .map(|f| f.schema_id.as_str().to_string())
        .collect();
    assert!(exact_ids.contains(&record_a.schema_id.as_str().to_string()));
    assert!(
        !exact_ids.contains(&record_b.schema_id.as_str().to_string()),
        "exact bucket_key lookup must miss the renamed-field schema"
    );

    // NEW path: (band, depth) parsed straight out of bucket_a's own key
    // (both fixtures share the same band/depth by construction -- only
    // reqhash8 differs) must find BOTH via prefix SCAN.
    let parts: Vec<&str> = bucket_a.split(':').collect();
    let band: u32 = parts[2].parse().unwrap();
    let depth: u32 = parts[3].parse().unwrap();
    assert_eq!(
        band,
        fieldband(2),
        "sanity: both fixtures are 2-field objects"
    );

    let widened = reg
        .list_families_by_band_depth(&[band], &[depth])
        .await
        .unwrap();
    let widened_ids: Vec<String> = widened
        .iter()
        .map(|f| f.schema_id.as_str().to_string())
        .collect();
    assert!(
        widened_ids.contains(&record_a.schema_id.as_str().to_string()),
        "widened discovery must still find record_a"
    );
    assert!(
        widened_ids.contains(&record_b.schema_id.as_str().to_string()),
        "widened discovery must find record_b via (band, depth) prefix, ignoring reqhash8"
    );
}
