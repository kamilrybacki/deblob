//! Task 14 fix: end-to-end proof that the promote→resolve round trip
//! actually works, against a REAL `RedisRegistry` + `RedisEvidence`
//! (Docker via testcontainers) — never fakes. Before this fix,
//! `Promoter::promote` indexed only a single SELF-REFERENTIAL bucket
//! member derived from the promoted schema's own GENERALIZED
//! (`deblob-monoid-v1`-domain) digest, which can never equal any concrete
//! message's raw (`deblob-canon-v1`-domain) fingerprint — so
//! `HotMatcher`-style `resolve_structural` lookups always missed and every
//! promoted schema stayed unreachable on the hot path forever.
//!
//! These tests drive the REAL production pipeline end to end:
//! `ColdLane::ingest` (records concrete-shape variants) → `policy::Promoter`
//! (replays them into the structural index at publish time) →
//! `RedisRegistry::resolve_structural` (the exact call the hot-path
//! `HotMatcher` makes for an incoming message).

use std::sync::Arc;

use deblob::coldlane::{ColdLane, SampleMeta};
use deblob::policy::{Promoter, PromotionPolicy};
use deblob::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};
use deblob_core::id::SchemaId;
use deblob_core::ports::{EvidenceStore, Registry};
use deblob_fingerprint::{
    bucket_key, fingerprint, parse_bounded, shape_of, summarize, Limits, Node,
};
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use redis::AsyncCommands;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// Spins up a fresh Redis container and connects both a `RedisRegistry` and
/// a `RedisEvidence` against it (`allow_volatile: true` — the container has
/// no AOF, matching every other Docker-backed test in this workspace).
/// Returns the connection URL too, so tests can reach in with a raw client
/// for direct key manipulation (mirrors `deblob-redis`'s own `index_it.rs`).
async fn setup() -> (
    String,
    Arc<RedisRegistry>,
    Arc<RedisEvidence>,
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let registry = Arc::new(
        RedisRegistry::connect(
            &url,
            RedisOpts {
                allow_volatile: true,
            },
        )
        .await
        .unwrap(),
    );
    let evidence = Arc::new(
        RedisEvidence::connect(
            &url,
            RedisEvidenceOpts::default(),
            RedisOpts {
                allow_volatile: true,
            },
        )
        .await
        .unwrap(),
    );
    // `node` must stay alive for the container to keep running — returned
    // to the caller rather than dropped here.
    (url, registry, evidence, node)
}

fn node_of(json: &[u8]) -> Node {
    parse_bounded(json, &Limits::default()).unwrap()
}

fn cand_id_of(json: &[u8]) -> deblob_core::id::CandidateId {
    let node = node_of(json);
    deblob_core::id::CandidateId::from_digest(&fingerprint(&shape_of(&node)))
}

fn meta(source: &str) -> SampleMeta {
    SampleMeta {
        source: source.to_string(),
        cursor: None,
    }
}

/// Exactly what the hot path (`crate::matcher::HotMatcher::classify`)
/// computes for an incoming message: its structural-index bucket key, and a
/// `SchemaId`-wrapped raw fingerprint to look up (the SAME wrapping
/// `HotMatcher` performs — `SchemaId::from_digest(&raw_fp)` — even though
/// this isn't a real published schema id; it's just reusing the type's
/// digest encoding, see `matcher.rs`).
fn hot_path_lookup_args(json: &[u8]) -> (String, SchemaId) {
    let shape = shape_of(&node_of(json));
    let bucket = bucket_key(&summarize(&shape));
    let fp_id = SchemaId::from_digest(&fingerprint(&shape));
    (bucket, fp_id)
}

/// A `PromotionPolicy` with both guards disabled, so a candidate ingested
/// just once/moments ago is immediately eligible — these tests care about
/// the promote→resolve WIRING, not the guard thresholds (already covered
/// by `policy.rs`'s own unit tests).
fn no_guard_policy() -> PromotionPolicy {
    PromotionPolicy {
        min_samples: 1,
        min_age_ms: 0,
    }
}

fn promote_request() -> PromoteRequest {
    PromoteRequest {
        family: FamilyChoice::New,
        name: Some("orders.created".to_string()),
        reason: "task-14-fix integration test".to_string(),
    }
}

/// Test A: a concrete message, ingested then promoted, must resolve on the
/// hot path for THAT SAME message. Before the fix this was always `None`.
#[tokio::test]
async fn promoted_schema_resolves_for_concrete_message() {
    let (_url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());

    let payload: &[u8] = br#"{"a":1,"b":"x"}"#;
    let cand_id = cand_id_of(payload);
    let outcome = lane
        .ingest(cand_id.clone(), &node_of(payload), meta("hot-path-sim"))
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        deblob::coldlane::IngestOutcome::Ingested { .. }
    ));

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    let schema = promoter
        .promote(&cand_id, promote_request(), "tester")
        .await
        .unwrap();

    let (bucket, fp_id) = hot_path_lookup_args(payload);
    let resolved = registry.resolve_structural(&bucket, &fp_id).await.unwrap();
    assert_eq!(
        resolved,
        Some(schema.schema_id.clone()),
        "the exact concrete message that seeded promotion must resolve to the promoted schema"
    );
}

/// Test B: two concrete variants of ONE emerging schema — with and without
/// an optional field — ingested into the same candidate (via `ColdLane`'s
/// generalized-fingerprint clustering) and promoted together. BOTH
/// concrete shapes must resolve to the same promoted schema afterward, not
/// just whichever one happened to seed the candidate.
#[tokio::test]
async fn promoted_schema_resolves_all_observed_variants() {
    let (_url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());

    let base: &[u8] = br#"{"a":1}"#;
    let variant: &[u8] = br#"{"a":1,"opt":"x"}"#;
    let base_id = cand_id_of(base);
    let variant_id = cand_id_of(variant);
    assert_ne!(base_id, variant_id, "raw shapes must differ");

    lane.ingest(base_id.clone(), &node_of(base), meta("hot-path-sim"))
        .await
        .unwrap();
    // Clusters onto base_id (spec §4 / ColdLane's reduced-generalized-fp
    // convergence): the variant differs from base by exactly one top-level
    // field, so both observations end up on ONE candidate.
    let second = lane
        .ingest(variant_id, &node_of(variant), meta("hot-path-sim"))
        .await
        .unwrap();
    assert!(matches!(
        second,
        deblob::coldlane::IngestOutcome::Ingested { .. }
    ));

    // Sanity: both variants were actually recorded against base_id before
    // promotion even runs.
    let recorded = evidence.get_variants(&base_id).await.unwrap();
    assert_eq!(
        recorded.len(),
        2,
        "both concrete variants must be recorded against the clustered candidate: {recorded:?}"
    );

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    let schema = promoter
        .promote(&base_id, promote_request(), "tester")
        .await
        .unwrap();

    for payload in [base, variant] {
        let (bucket, fp_id) = hot_path_lookup_args(payload);
        let resolved = registry.resolve_structural(&bucket, &fp_id).await.unwrap();
        assert_eq!(
            resolved,
            Some(schema.schema_id.clone()),
            "payload {:?} must resolve to the promoted schema",
            std::str::from_utf8(payload).unwrap()
        );
    }
}

/// Test C: the structural index is DERIVED, disposable state (spec §6) —
/// `rebuild_index` must be able to restore variant resolution purely from
/// the authoritative `deblob:schema:*` records, even after every
/// `deblob:index:*` key is wiped out from under it.
#[tokio::test]
async fn rebuild_index_restores_variant_resolution() {
    let (url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());

    let payload: &[u8] = br#"{"id":"x","count":1}"#;
    let cand_id = cand_id_of(payload);
    lane.ingest(cand_id.clone(), &node_of(payload), meta("hot-path-sim"))
        .await
        .unwrap();

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    let schema = promoter
        .promote(&cand_id, promote_request(), "tester")
        .await
        .unwrap();

    let (bucket, fp_id) = hot_path_lookup_args(payload);

    // Sanity: resolvable right after promotion (this is Test A's
    // assertion, replayed here as a pre-condition).
    assert_eq!(
        registry.resolve_structural(&bucket, &fp_id).await.unwrap(),
        Some(schema.schema_id.clone())
    );

    // Wipe ONLY the derived index keys via raw SCAN/DEL — the authoritative
    // deblob:schema:* record (including its `variants` field) is untouched.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let index_keys: Vec<String> = conn.keys("deblob:index:*").await.unwrap();
    assert!(
        !index_keys.is_empty(),
        "promotion must have created at least one deblob:index:* key"
    );
    for key in &index_keys {
        let _: () = conn.del(key).await.unwrap();
    }

    // Index gone -> resolution now misses.
    assert_eq!(
        registry.resolve_structural(&bucket, &fp_id).await.unwrap(),
        None,
        "resolve_structural must miss once the index keys are gone"
    );

    // Rebuild purely from deblob:schema:* -> variant resolution restored.
    let reindexed = registry.rebuild_index().await.unwrap();
    assert!(
        reindexed >= 1,
        "rebuild_index must report at least the one schema published above"
    );

    assert_eq!(
        registry.resolve_structural(&bucket, &fp_id).await.unwrap(),
        Some(schema.schema_id),
        "resolve_structural must find the concrete-shape variant again after rebuild_index"
    );
}

/// Test D (fix1): a real promoted schema — published by `Promoter::promote`
/// through the actual production `deblob-monoid` generalization path, so
/// its `canonicalizer` is `deblob_monoid::GENERALIZER` ("deblob-monoid-v1"),
/// never the raw "deblob-canon-v1" tag a directly-ingested concrete message
/// gets — must show up in `Registry::list_schemas`, the same call
/// `GET /api/v1/schemas` makes. Before fix1, `list_schemas` scanned the
/// whole `deblob:*` keyspace for `deblob:schema:*` keys and could return an
/// empty page with a non-zero cursor even though this schema existed.
#[tokio::test]
async fn promoted_monoid_schema_appears_in_list_schemas() {
    let (_url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());

    let payload: &[u8] = br#"{"a":1,"b":"x"}"#;
    let cand_id = cand_id_of(payload);
    lane.ingest(cand_id.clone(), &node_of(payload), meta("hot-path-sim"))
        .await
        .unwrap();

    let promoter = Promoter::with_policy(registry.clone(), evidence.clone(), no_guard_policy());
    let schema = promoter
        .promote(&cand_id, promote_request(), "tester")
        .await
        .unwrap();
    assert_eq!(
        schema.canonicalizer,
        deblob_monoid::GENERALIZER,
        "sanity: a promoted schema is generalized via the monoid canonicalizer"
    );

    let (data, next) = registry.list_schemas(None, 50).await.unwrap();
    assert!(
        data.iter()
            .any(|r| r.schema_id == schema.schema_id
                && r.canonicalizer == deblob_monoid::GENERALIZER),
        "the promoted (monoid-canonicalizer) schema must appear in list_schemas, got: {data:?}"
    );
    assert!(
        next.is_none(),
        "a single-schema vault must not carry a next_cursor past the only page"
    );
}
