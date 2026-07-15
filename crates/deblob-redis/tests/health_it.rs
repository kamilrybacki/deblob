//! Task 10: runtime persistence health monitoring. `evaluate_persistence`'s
//! parsing/decision logic is unit-tested (Redis-free) in
//! `crates/deblob-redis/src/health.rs`; this file covers the two things
//! that genuinely need a live Redis: `PersistenceHealth::probe` against a
//! real (AOF-enabled) instance, and `RedisRegistry::publish` refusing to
//! write when its `HealthGate` is degraded.

use deblob_core::error::CoreError;
use deblob_core::id::{CandidateId, FamilyId, FamilyVersion, SchemaId};
use deblob_core::ports::{Registry, SchemaRecord};
use deblob_redis::health::{HealthGate, HealthState, PersistenceHealth};
use deblob_redis::{RedisOpts, RedisRegistry};
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{runners::AsyncRunner, ImageExt},
};

fn sample_record() -> SchemaRecord {
    SchemaRecord {
        schema_id: SchemaId::from_digest(&[1u8; 32]),
        family_id: FamilyId::new_v7(),
        version: FamilyVersion(1),
        canonical: r#"{"t":"obj","f":{"id":{"t":"str"}}}"#.to_string(),
        canonicalizer: "deblob-canon-v1".to_string(),
        provenance: serde_json::json!({"source": "health_it"}),
        semantic: None,
        semantic_fingerprint: None,
    }
}

#[tokio::test]
async fn probe_reports_ok_on_persistent_redis() {
    // `--appendonly yes` turns AOF on inside the container; Redis's
    // defaults otherwise already satisfy the rest of the gate
    // (rdb_last_bgsave_status/aof_last_write_status start at "ok",
    // maxmemory-policy defaults to "noeviction").
    let node = Redis::default()
        .with_cmd(["--appendonly", "yes"])
        .start()
        .await
        .unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let client = redis::Client::open(url.as_str()).unwrap();
    let conn = client
        .get_connection_manager_with_config(deblob_redis::connection_manager_config())
        .await
        .unwrap();

    let state = PersistenceHealth::probe(conn).await;
    assert_eq!(
        state,
        HealthState::Ok,
        "AOF-enabled container with default config should probe healthy, got {state:?}"
    );
}

#[tokio::test]
async fn probe_reports_degraded_when_aof_disabled() {
    // Sibling case to the above, run against the plain (no-AOF) default
    // image: the probe must observe and report `Degraded`, not silently
    // pass.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let client = redis::Client::open(url.as_str()).unwrap();
    let conn = client
        .get_connection_manager_with_config(deblob_redis::connection_manager_config())
        .await
        .unwrap();

    let state = PersistenceHealth::probe(conn).await;
    assert!(
        !state.is_ok(),
        "AOF-disabled container must probe Degraded, got {state:?}"
    );
}

#[tokio::test]
async fn publish_frozen_when_gate_degraded() {
    // The gate is forced degraded WITHOUT running a real probe — this
    // isolates "does publish honor the gate" from "does the probe work",
    // which is covered separately above.
    let node = Redis::default().start().await.unwrap();
    let url = format!(
        "redis://127.0.0.1:{}",
        node.get_host_port_ipv4(6379).await.unwrap()
    );
    let gate = HealthGate::new();
    let reg = RedisRegistry::connect(
        &url,
        RedisOpts {
            allow_volatile: true,
        },
    )
    .await
    .unwrap()
    .with_health_gate(gate.clone());

    gate.force_degraded_for_test();

    let rec = sample_record();
    let cand = CandidateId::from_digest(&[99u8; 32]);
    let err = reg
        .publish(
            rec.clone(),
            &cand,
            "bucket:health:abc",
            &[],
            "kamil",
            "frozen",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CoreError::RegistryUnavailable(ref msg) if msg.contains("persistence degraded")),
        "expected RegistryUnavailable(\"persistence degraded\"), got {err:?}"
    );

    // Confirm nothing was actually written — the gate check happens
    // before any Redis command, not after a failed one.
    assert!(
        reg.get_schema(&rec.schema_id).await.unwrap().is_none(),
        "publish must not have written anything while frozen"
    );
}
