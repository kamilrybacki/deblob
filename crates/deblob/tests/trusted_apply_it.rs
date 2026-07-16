//! trusted-slm-apply: `TrustedApplier` against a REAL Redis (Docker via
//! testcontainers) — mirrors `promote_resolve_it.rs`'s setup, driving the
//! actual production `RedisRegistry`/`RedisEvidence`/`policy::Promoter`
//! stack end to end rather than fakes. Proves the governed-apply path for
//! real:
//!
//!   - `TrustVerdict::Apply` publishes through `Promoter::promote` ->
//!     `Registry::publish`: the candidate's family gets a new version, the
//!     write is atomic, and the audit stream (`deblob:audit:log`) records
//!     `actor = "policy:slm-v1"`.
//!   - `TrustVerdict::ProposeToHuman` leaves the registry byte-for-byte
//!     unchanged — only an in-memory `Proposal` is recorded.
//!   - An `IncompatibleSimilarity` decision resolves to `ShadowOnly` and
//!     also leaves the registry unchanged, even when every deterministic
//!     gate would otherwise pass — the false-merge relation is structurally
//!     unreachable regardless of how "good" the rest of the evidence looks.

use std::sync::Arc;

use deblob::coldlane::{ColdLane, SampleMeta};
use deblob::policy::{Promoter, PromotionPolicy};
use deblob::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};
use deblob::shadow::PolicyGateInputs;
use deblob::trusted::{
    AppliedOutcome, ApplyContext, InMemoryProposalSink, TrustMode, TrustedApplier,
};
use deblob_core::ports::Registry;
use deblob_fingerprint::{fingerprint, parse_bounded, shape_of, Limits, Node};
use deblob_redis::{RedisEvidence, RedisEvidenceOpts, RedisOpts, RedisRegistry};
use deblob_slm::{InferenceDecision, Relation};
use testcontainers_modules::{redis::Redis, testcontainers::runners::AsyncRunner};

/// Mirrors `promote_resolve_it.rs::setup` exactly: a fresh Redis container
/// wired to a real `RedisRegistry` + `RedisEvidence`.
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

/// Both guards disabled (mirrors `promote_resolve_it.rs::no_guard_policy`)
/// — these tests exercise the trust-gate/apply WIRING, not the
/// sample-count/age promotion thresholds (already covered elsewhere).
fn no_guard_policy() -> PromotionPolicy {
    PromotionPolicy {
        min_samples: 1,
        min_age_ms: 0,
    }
}

/// A `PolicyGateInputs` with every deterministic gate passing.
fn all_pass_gate(relation: Relation) -> PolicyGateInputs {
    PolicyGateInputs {
        is_match_schema: true,
        selected_rank: Some(1),
        selected_distance: Some(0.0),
        top1_top2_margin: 1.0,
        observation_count: 1_000,
        relation: Some(relation),
        deterministic_compat_passed: true,
        redaction_collision: false,
    }
}

/// Ingests `payload` as a candidate and returns its `CandidateId`, so a test
/// can hand `TrustedApplier` a candidate that actually exists in the
/// (real) evidence store — exactly what `Promoter::promote` requires.
async fn ingest(lane: &ColdLane, payload: &[u8], source: &str) -> deblob_core::id::CandidateId {
    let cand_id = cand_id_of(payload);
    lane.ingest(cand_id.clone(), &node_of(payload), meta(source))
        .await
        .unwrap();
    cand_id
}

/// Test 1: an `Apply` verdict publishes through the governed `Promoter`
/// path — the candidate ends up as a new version of the SAME family the
/// SLM-corroborated schema already belongs to, and the write lands on the
/// real, immutable, audited registry (audit actor = "policy:slm-v1").
#[tokio::test]
async fn apply_verdict_publishes_through_governed_path_and_audits_as_policy_slm() {
    let (url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());
    let promoter: Arc<dyn PromoterTrait> = Arc::new(Promoter::with_policy(
        registry.clone(),
        evidence.clone(),
        no_guard_policy(),
    ));
    let proposals = Arc::new(InMemoryProposalSink::new());
    let applier = TrustedApplier::new(
        registry.clone() as Arc<dyn Registry>,
        promoter.clone(),
        proposals.clone(),
    );

    // Seed an "existing" schema the SLM will claim a match against.
    let existing_payload: &[u8] = br#"{"a":1,"b":"x"}"#;
    let existing_cand = ingest(&lane, existing_payload, "seed").await;
    let existing_schema = promoter
        .promote(
            &existing_cand,
            PromoteRequest {
                family: FamilyChoice::New,
                name: Some("orders.created".to_string()),
                reason: "seed schema".to_string(),
            },
            "tester",
        )
        .await
        .unwrap();

    let before_count = registry.list_schemas(None, 50).await.unwrap().0.len();
    assert_eq!(before_count, 1);

    // A DIFFERENT candidate the SLM proposes as an Exact match to the
    // seeded schema.
    let dup_payload: &[u8] = br#"{"x":1,"y":true,"z":"unrelated"}"#;
    let dup_cand = ingest(&lane, dup_payload, "incoming").await;

    let decision = InferenceDecision::MatchSchema {
        schema_id: existing_schema.schema_id.clone(),
        relation: Relation::Exact,
    };
    let gate = all_pass_gate(Relation::Exact);
    let ctx = ApplyContext {
        candidate_id: dup_cand,
    };

    let outcome = applier
        .apply_if_trusted(&decision, &gate, TrustMode::AutoApply, &ctx)
        .await
        .unwrap();

    let published = match outcome {
        AppliedOutcome::Applied(schema) => schema,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(
        published.family_id, existing_schema.family_id,
        "the trusted apply must join the SAME family as the corroborated schema, \
         via the governed FamilyChoice::Existing path"
    );

    let (schemas_after, _) = registry.list_schemas(None, 50).await.unwrap();
    assert_eq!(
        schemas_after.len(),
        2,
        "the applied decision must publish a real, listable schema record: {schemas_after:?}"
    );

    // No proposal must have been recorded for an Apply verdict.
    assert!(proposals.proposals().await.is_empty());

    // Audit: the real `deblob:audit:log` stream must carry an entry
    // attributing this write to "policy:slm-v1" — not a human actor.
    let client = redis::Client::open(url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
        .arg("deblob:audit:log")
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(
        !entries.is_empty(),
        "audit stream must have entries after publish"
    );
    let found_policy_actor = entries.iter().any(|(_, fields)| {
        fields
            .windows(2)
            .any(|pair| pair[0] == "actor" && pair[1] == "policy:slm-v1")
    });
    assert!(
        found_policy_actor,
        "audit stream must attribute at least one entry to actor=policy:slm-v1, got: {entries:?}"
    );
}

/// Test 2: a `ProposeToHuman` verdict (deterministically corroborated, but
/// `TrustMode::ProposeOnly`) must NOT touch the registry at all — only the
/// `ProposalSink` gets an entry.
#[tokio::test]
async fn propose_to_human_verdict_leaves_registry_unchanged() {
    let (_url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());
    let promoter: Arc<dyn PromoterTrait> = Arc::new(Promoter::with_policy(
        registry.clone(),
        evidence.clone(),
        no_guard_policy(),
    ));
    let proposals = Arc::new(InMemoryProposalSink::new());
    let applier = TrustedApplier::new(
        registry.clone() as Arc<dyn Registry>,
        promoter.clone(),
        proposals.clone(),
    );

    let existing_payload: &[u8] = br#"{"a":1,"b":"x"}"#;
    let existing_cand = ingest(&lane, existing_payload, "seed").await;
    let existing_schema = promoter
        .promote(
            &existing_cand,
            PromoteRequest {
                family: FamilyChoice::New,
                name: Some("orders.created".to_string()),
                reason: "seed schema".to_string(),
            },
            "tester",
        )
        .await
        .unwrap();

    let (schemas_before, _) = registry.list_schemas(None, 50).await.unwrap();
    assert_eq!(schemas_before.len(), 1);

    let dup_payload: &[u8] = br#"{"x":1,"y":true,"z":"unrelated"}"#;
    let dup_cand = ingest(&lane, dup_payload, "incoming").await;

    let decision = InferenceDecision::MatchSchema {
        schema_id: existing_schema.schema_id.clone(),
        relation: Relation::CompatibleDrift,
    };
    let gate = all_pass_gate(Relation::CompatibleDrift);
    let ctx = ApplyContext {
        candidate_id: dup_cand.clone(),
    };

    let outcome = applier
        .apply_if_trusted(&decision, &gate, TrustMode::ProposeOnly, &ctx)
        .await
        .unwrap();

    match outcome {
        AppliedOutcome::Proposed(proposal) => {
            assert_eq!(proposal.candidate_id, dup_cand);
            assert_eq!(proposal.schema_id, existing_schema.schema_id);
            assert_eq!(proposal.relation, Relation::CompatibleDrift);
        }
        other => panic!("expected Proposed, got {other:?}"),
    }

    let recorded = proposals.proposals().await;
    assert_eq!(recorded.len(), 1, "exactly one proposal must be recorded");

    let (schemas_after, _) = registry.list_schemas(None, 50).await.unwrap();
    assert_eq!(
        schemas_after.len(),
        1,
        "ProposeToHuman must NEVER change registry state: {schemas_after:?}"
    );
    // Alias must not have been created for the un-applied candidate either.
    assert_eq!(registry.get_alias(&dup_cand).await.unwrap(), None);
}

/// Test 3: an `IncompatibleSimilarity` decision must resolve to
/// `ShadowOnly` and leave the registry unchanged — even when every OTHER
/// deterministic gate would pass. The false-merge relation is unreachable
/// by construction, not merely unlikely.
#[tokio::test]
async fn incompatible_similarity_never_applies_leaves_registry_unchanged() {
    let (_url, registry, evidence, _node) = setup().await;
    let lane = ColdLane::new(evidence.clone());
    let promoter: Arc<dyn PromoterTrait> = Arc::new(Promoter::with_policy(
        registry.clone(),
        evidence.clone(),
        no_guard_policy(),
    ));
    let proposals = Arc::new(InMemoryProposalSink::new());
    let applier = TrustedApplier::new(
        registry.clone() as Arc<dyn Registry>,
        promoter.clone(),
        proposals.clone(),
    );

    let existing_payload: &[u8] = br#"{"a":1,"b":"x"}"#;
    let existing_cand = ingest(&lane, existing_payload, "seed").await;
    let existing_schema = promoter
        .promote(
            &existing_cand,
            PromoteRequest {
                family: FamilyChoice::New,
                name: Some("orders.created".to_string()),
                reason: "seed schema".to_string(),
            },
            "tester",
        )
        .await
        .unwrap();

    let (schemas_before, _) = registry.list_schemas(None, 50).await.unwrap();
    assert_eq!(schemas_before.len(), 1);

    let dup_payload: &[u8] = br#"{"x":1,"y":true,"z":"unrelated"}"#;
    let dup_cand = ingest(&lane, dup_payload, "incoming").await;

    let decision = InferenceDecision::MatchSchema {
        schema_id: existing_schema.schema_id.clone(),
        relation: Relation::IncompatibleSimilarity,
    };
    // Deliberately an "all other gates pass" gate to prove it's the
    // RELATION alone, not a coincidentally-failing gate, that blocks this.
    let gate = all_pass_gate(Relation::IncompatibleSimilarity);
    let ctx = ApplyContext {
        candidate_id: dup_cand.clone(),
    };

    for mode in [TrustMode::AutoApply, TrustMode::ProposeOnly] {
        let outcome = applier
            .apply_if_trusted(&decision, &gate, mode, &ctx)
            .await
            .unwrap();
        assert!(
            matches!(outcome, AppliedOutcome::ShadowOnly),
            "expected ShadowOnly for IncompatibleSimilarity under mode {mode:?}, got {outcome:?}"
        );
    }

    let (schemas_after, _) = registry.list_schemas(None, 50).await.unwrap();
    assert_eq!(
        schemas_after.len(),
        1,
        "an IncompatibleSimilarity decision must NEVER change registry state: {schemas_after:?}"
    );
    assert!(proposals.proposals().await.is_empty());
    assert_eq!(registry.get_alias(&dup_cand).await.unwrap(), None);
}
