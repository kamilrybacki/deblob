use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts};
use redis::AsyncCommands;
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// Builds a valid `CandidateRecord` in the `Provisional` state.
fn sample_candidate() -> CandidateRecord {
    CandidateRecord {
        candidate_id: CandidateId::from_digest(&[5u8; 32]),
        profile: serde_json::json!({"source": "sensor-x", "fields": ["a", "b"]}),
        sample_count: 1,
        first_seen_ms: 1_700_000_000_000,
        last_seen_ms: 1_700_000_000_000,
        state: CandidateState::Provisional,
        source: None,
    }
}

/// Variant of `sample_candidate()` with a caller-chosen digest, for tests
/// that need multiple distinct candidates.
fn candidate_with(digest: [u8; 32]) -> CandidateRecord {
    CandidateRecord {
        candidate_id: CandidateId::from_digest(&digest),
        ..sample_candidate()
    }
}

async fn connect_evidence(url: &str) -> RedisEvidence {
    RedisEvidence::connect(
        url,
        RedisEvidenceOpts::default(),
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn upsert_get_roundtrip() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    let rec = sample_candidate();
    ev.upsert_candidate(rec.clone()).await.unwrap();

    let fetched = ev.get_candidate(&rec.candidate_id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&fetched).unwrap(),
        serde_json::to_value(&rec).unwrap(),
        "roundtripped candidate must equal the original"
    );

    // Unknown candidate -> None, not an error.
    let missing = CandidateId::from_digest(&[99u8; 32]);
    assert!(ev.get_candidate(&missing).await.unwrap().is_none());
}

#[tokio::test]
async fn candidate_has_ttl_audit_stub_permanent() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    let rec = sample_candidate();
    ev.upsert_candidate(rec.clone()).await.unwrap();

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let candidate_ttl: i64 = redis::cmd("TTL")
        .arg(format!("deblob:candidate:{}", rec.candidate_id.as_str()))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        candidate_ttl > 0,
        "candidate key must have a positive TTL, got {candidate_ttl}"
    );

    let audit_ttl: i64 = redis::cmd("TTL")
        .arg(format!(
            "deblob:candidate-audit:{}",
            rec.candidate_id.as_str()
        ))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        audit_ttl, -1,
        "audit stub must be persistent (TTL == -1), got {audit_ttl}"
    );

    // The audit stub must survive even after the candidate itself expires
    // (simulated here by deleting it directly, since waiting out a 7-day
    // TTL isn't practical in a test).
    let _: () = conn
        .del(format!("deblob:candidate:{}", rec.candidate_id.as_str()))
        .await
        .unwrap();
    let audit_ttl_after: i64 = redis::cmd("TTL")
        .arg(format!(
            "deblob:candidate-audit:{}",
            rec.candidate_id.as_str()
        ))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        audit_ttl_after, -1,
        "audit stub must still exist and be permanent after candidate expiry"
    );
}

#[tokio::test]
async fn evidence_stream_trimmed() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    let rec = sample_candidate();
    ev.upsert_candidate(rec.clone()).await.unwrap();

    for i in 0..1500u32 {
        ev.append_evidence(&rec.candidate_id, serde_json::json!({"n": i}))
            .await
            .unwrap();
    }

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let len: u64 = redis::cmd("XLEN")
        .arg(format!("deblob:evidence:{}", rec.candidate_id.as_str()))
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        len < 1500,
        "stream must be trimmed well below the 1500 entries appended, got {len}"
    );
    assert!(
        len >= 1000,
        "approximate MAXLEN trim should not drop below the 1000 cap, got {len}"
    );
}

#[tokio::test]
async fn state_transition_guarded() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    // Provisional -> Staged: allowed.
    let a = candidate_with([1u8; 32]);
    ev.upsert_candidate(a.clone()).await.unwrap();
    ev.set_state(&a.candidate_id, CandidateState::Staged)
        .await
        .unwrap();
    let a_after = ev.get_candidate(&a.candidate_id).await.unwrap().unwrap();
    assert_eq!(a_after.state, CandidateState::Staged);

    // Provisional -> Rejected: allowed.
    let b = candidate_with([2u8; 32]);
    ev.upsert_candidate(b.clone()).await.unwrap();
    ev.set_state(&b.candidate_id, CandidateState::Rejected)
        .await
        .unwrap();
    let b_after = ev.get_candidate(&b.candidate_id).await.unwrap().unwrap();
    assert_eq!(b_after.state, CandidateState::Rejected);

    // Rejected -> Staged: rejected (Rejected is terminal).
    let err = ev
        .set_state(&b.candidate_id, CandidateState::Staged)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CoreError::Conflict(_)),
        "expected Conflict for a transition out of the terminal Rejected state, got {err:?}"
    );

    // The illegal transition must not have taken effect.
    let b_still = ev.get_candidate(&b.candidate_id).await.unwrap().unwrap();
    assert_eq!(
        b_still.state,
        CandidateState::Rejected,
        "state must remain Rejected after the guarded transition was refused"
    );

    // set_state on an unknown candidate -> NotFound.
    let missing = CandidateId::from_digest(&[123u8; 32]);
    let err = ev
        .set_state(&missing, CandidateState::Staged)
        .await
        .unwrap_err();
    assert!(matches!(err, CoreError::NotFound));
}

/// fix2: `list_candidates` must return EVERY candidate of the requested
/// state, paging fully via its cursor, even when other-state candidates and
/// unrelated keys (evidence stream, audit stub, cluster alias) share the
/// keyspace. Proves the index-backed `SSCAN` path (not the old whole-
/// keyspace `SCAN`) is what's driving the listing — this is the exact
/// under-reporting bug fix1 already fixed for schemas.
#[tokio::test]
async fn list_candidates_returns_all_of_state_across_pages() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    // 25 distinct provisional candidates + a handful of staged/rejected
    // decoys that must never show up in the provisional page.
    let mut provisional_ids = std::collections::HashSet::new();
    for i in 0..25u8 {
        let mut digest = [0u8; 32];
        digest[0] = i;
        let rec = candidate_with(digest);
        provisional_ids.insert(rec.candidate_id.as_str().to_string());
        ev.upsert_candidate(rec).await.unwrap();
    }
    for i in 100..105u8 {
        let mut digest = [0u8; 32];
        digest[0] = i;
        let rec = candidate_with(digest);
        ev.upsert_candidate(rec.clone()).await.unwrap();
        ev.set_state(&rec.candidate_id, CandidateState::Staged)
            .await
            .unwrap();
    }

    // Page through with a small limit, exactly like the API handler does,
    // and accumulate every returned id.
    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = ev
            .list_candidates(CandidateState::Provisional, cursor.clone(), 5)
            .await
            .unwrap();
        for rec in &page {
            assert_eq!(rec.state, CandidateState::Provisional);
            seen.insert(rec.candidate_id.as_str().to_string());
        }
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    assert_eq!(
        seen, provisional_ids,
        "list_candidates must return every provisional candidate across all pages, no under-reporting"
    );
}

/// fix2: `set_state` must move index membership, not just the hash's own
/// `state` field — a candidate staged after having been provisional must
/// disappear from the provisional listing and appear in the staged one.
#[tokio::test]
async fn set_state_moves_index_membership() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    let rec = candidate_with([7u8; 32]);
    ev.upsert_candidate(rec.clone()).await.unwrap();

    let (provisional_before, _) = ev
        .list_candidates(CandidateState::Provisional, None, 100)
        .await
        .unwrap();
    assert!(provisional_before
        .iter()
        .any(|r| r.candidate_id == rec.candidate_id));

    ev.set_state(&rec.candidate_id, CandidateState::Staged)
        .await
        .unwrap();

    let (provisional_after, _) = ev
        .list_candidates(CandidateState::Provisional, None, 100)
        .await
        .unwrap();
    assert!(
        !provisional_after
            .iter()
            .any(|r| r.candidate_id == rec.candidate_id),
        "candidate must no longer be listed under its stale, pre-transition state"
    );

    let (staged, _) = ev
        .list_candidates(CandidateState::Staged, None, 100)
        .await
        .unwrap();
    assert!(
        staged.iter().any(|r| r.candidate_id == rec.candidate_id),
        "candidate must be listed under its new state"
    );
}

/// fix2 backfill: candidates written before the per-state index existed
/// (simulated here by writing the hash directly, bypassing
/// `upsert_candidate`'s `SADD`) are invisible to `list_candidates` until
/// `rebuild_candidate_index` runs once, after which they appear.
#[tokio::test]
async fn rebuild_candidate_index_backfills_pre_existing_candidates() {
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let ev = connect_evidence(&url).await;

    let rec = candidate_with([42u8; 32]);
    let record_json = serde_json::to_string(&rec).unwrap();

    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("HSET")
        .arg(format!("deblob:candidate:{}", rec.candidate_id.as_str()))
        .arg("record")
        .arg(&record_json)
        .arg("state")
        .arg("provisional")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Not indexed yet: invisible to list_candidates.
    let (before, _) = ev
        .list_candidates(CandidateState::Provisional, None, 100)
        .await
        .unwrap();
    assert!(!before.iter().any(|r| r.candidate_id == rec.candidate_id));

    let reindexed = ev.rebuild_candidate_index().await.unwrap();
    assert!(reindexed >= 1);

    let (after, _) = ev
        .list_candidates(CandidateState::Provisional, None, 100)
        .await
        .unwrap();
    assert!(
        after.iter().any(|r| r.candidate_id == rec.candidate_id),
        "rebuild_candidate_index must backfill pre-existing candidates into the state index"
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

    let err = RedisEvidence::connect(
        &url,
        RedisEvidenceOpts::default(),
        RedisOpts {
            allow_volatile: false,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, CoreError::RegistryUnavailable(_)));

    // with the flag, the same volatile instance is accepted
    RedisEvidence::connect(
        &url,
        RedisEvidenceOpts::default(),
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap();
}
