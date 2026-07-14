//! Cold lane: clustering + evidence accumulation (spec §4, §6, §9).
//!
//! `ColdLane::ingest` is the read-merge-write counterpart to the hot path's
//! deterministic classification (`crate::matcher`): for every observed
//! message it builds a [`deblob_monoid::Profile`] from the parsed node,
//! resolves which candidate this observation belongs to via the
//! *generalized* fingerprint cluster map (so optional-field variants of one
//! emerging schema converge onto ONE candidate even though the hot path's
//! raw shape digest mints a different `cand_` id per variant, spec §4),
//! merges the new observation into the candidate's stored profile, and
//! appends STATS-ONLY evidence (field presence/type counts — never raw
//! values, spec §9). Newly-minted candidates are rate-limited per source
//! (`governor`) so a misbehaving/compromised producer can't create an
//! unbounded number of candidates.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use data_encoding::HEXLOWER;
use deblob_core::envelope::SourceCursor;
use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
use deblob_fingerprint::Node;
use deblob_monoid::{FieldNode, Profile};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use serde::{Deserialize, Serialize};

/// Default per-source rate limit on newly minted candidates: 10/minute
/// (spec §9 abuse-resistance — a compromised/misbehaving producer must not
/// be able to mint unbounded candidates).
pub const DEFAULT_NEW_CANDIDATES_PER_MIN: u32 = 10;

/// Provenance carried alongside a single cold-lane observation: which
/// source produced it, and (when available) where in that source's stream
/// it was read from. Deliberately holds no payload bytes itself — the
/// payload is threaded through separately as a `Node`/`DiscoveryMsg`.
#[derive(Debug, Clone)]
pub struct SampleMeta {
    pub source: String,
    pub cursor: Option<SourceCursor>,
}

/// One message forwarded to the discovery topic for cold-lane processing
/// (Task 16's Kafka wiring consumes this). Carries the RAW payload bytes —
/// unlike the stats-only evidence `ColdLane::ingest` appends to the
/// `EvidenceStore`, this is the transport envelope between the hot path and
/// the cold-lane consumer, not a permanent record (spec §9 governs what
/// gets *persisted*, not what's in flight on the discovery topic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMsg {
    pub cand_id: String,
    pub payload: Bytes,
    pub source: String,
    pub cursor: SourceCursor,
}

/// Outcome of one `ColdLane::ingest` call. Rate-limited ingestion is
/// dropped and counted, never panicked (spec brief) — the caller sees this
/// variant instead of an error so a burst from one source degrades
/// observably rather than propagating as a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestOutcome {
    Ingested,
    RateLimited,
}

/// The cold lane: read-merge-write candidate accumulation backed by an
/// [`EvidenceStore`], with per-source rate limiting on brand-new
/// candidates.
pub struct ColdLane {
    evidence: Arc<dyn EvidenceStore>,
    limiter: DefaultKeyedRateLimiter<String>,
}

impl ColdLane {
    /// Build a `ColdLane` with the default rate limit
    /// ([`DEFAULT_NEW_CANDIDATES_PER_MIN`] new candidates/min/source).
    pub fn new(evidence: Arc<dyn EvidenceStore>) -> Self {
        Self::with_rate_limit(evidence, DEFAULT_NEW_CANDIDATES_PER_MIN)
    }

    /// Build a `ColdLane` with a caller-supplied new-candidates-per-minute
    /// limit (per source). `per_minute == 0` is treated as `1` so
    /// `Quota::per_minute` never panics on a degenerate config value.
    pub fn with_rate_limit(evidence: Arc<dyn EvidenceStore>, per_minute: u32) -> Self {
        let quota = Quota::per_minute(NonZeroU32::new(per_minute).unwrap_or(NonZeroU32::MIN));
        Self {
            evidence,
            limiter: RateLimiter::keyed(quota),
        }
    }

    /// Merge one observed `node` into the candidate `cand_id` resolves to
    /// (after generalized-fingerprint clustering), append stats-only
    /// evidence, and record the observation's cluster alias.
    ///
    /// `cand_id` is the hot path's raw-shape-derived candidate id (spec
    /// §3.1/§3.2) — the *first* observation of a brand-new generalized
    /// schema always ingests under this id; every later observation of an
    /// optional-field variant of the *same* generalized schema converges
    /// onto whichever candidate the cluster map already points at, even
    /// though its own raw `cand_id` differs.
    pub async fn ingest(
        &self,
        cand_id: CandidateId,
        node: &Node,
        meta: SampleMeta,
    ) -> Result<IngestOutcome, CoreError> {
        let observation = Profile::from_node(node);
        // The FULL generalized fingerprint, plus (bounded, top-level-only)
        // "drop one field" projections — see `reduced_generalized_fps` docs
        // for why a single observation's own full fingerprint can never
        // equal another single-sample variant's on its own, and why this
        // projection set is what actually makes clustering converge.
        let candidate_fps = reduced_generalized_fps(&observation);

        let mut clustered = None;
        for fp in &candidate_fps {
            let hex = HEXLOWER.encode(fp);
            if let Some(existing) = self.evidence.get_cluster(&hex).await? {
                clustered = Some(existing);
                break;
            }
        }
        let target_id = clustered.unwrap_or(cand_id);

        let existing = self.evidence.get_candidate(&target_id).await?;

        // Only a genuinely brand-new candidate (no cluster hit AND no
        // existing record under its raw id) counts against the per-source
        // rate limit — repeat observations of an already-known candidate
        // are not new candidates.
        if existing.is_none() && self.limiter.check_key(&meta.source).is_err() {
            return Ok(IngestOutcome::RateLimited);
        }

        let now = now_ms();
        let merged_profile = match &existing {
            Some(rec) => {
                let stored: Profile = serde_json::from_value(rec.profile.clone()).map_err(|e| {
                    CoreError::RegistryUnavailable(format!("corrupt stored profile: {e}"))
                })?;
                Profile::merge(&stored, &observation)
            }
            None => observation.clone(),
        };

        let record = CandidateRecord {
            candidate_id: target_id.clone(),
            profile: serde_json::to_value(&merged_profile)
                .map_err(|e| CoreError::RegistryUnavailable(format!("serialize profile: {e}")))?,
            sample_count: existing.as_ref().map(|r| r.sample_count).unwrap_or(0) + 1,
            first_seen_ms: existing.as_ref().map(|r| r.first_seen_ms).unwrap_or(now),
            last_seen_ms: now,
            state: existing
                .as_ref()
                .map(|r| r.state)
                .unwrap_or(CandidateState::Provisional),
        };

        self.evidence.upsert_candidate(record).await?;
        // Register every projection (full fingerprint + each top-level
        // single-field-dropped variant) as an alias for `target_id`, so a
        // FUTURE sample that's this one plus/minus exactly one top-level
        // field converges here regardless of which variant is observed
        // first.
        for fp in &candidate_fps {
            let hex = HEXLOWER.encode(fp);
            self.evidence.set_cluster(&hex, &target_id).await?;
        }

        // §9: append ONLY this observation's presence/type-count stats —
        // `Profile` never retains a raw observed value, so serializing it
        // cannot leak payload contents.
        let stats = serde_json::to_value(&observation).map_err(|e| {
            CoreError::RegistryUnavailable(format!("serialize evidence stats: {e}"))
        })?;
        self.evidence.append_evidence(&target_id, stats).await?;

        // `meta.cursor` is carried for provenance/replay bookkeeping the
        // discovery-topic wiring (Task 16) is responsible for persisting
        // alongside `DiscoveryMsg`; the evidence store itself only tracks
        // aggregate profile statistics, not per-message cursor state.
        let _ = &meta.cursor;

        Ok(IngestOutcome::Ingested)
    }
}

/// The clustering key set for one profile: its own full generalized
/// fingerprint, plus — ONLY when it has at least two top-level object
/// fields — one fingerprint per "drop exactly one top-level field"
/// projection.
///
/// Why this exists: `Profile::generalized_fingerprint` is only
/// order-independent once fields have actually been observed as optional
/// (`present < count`) — a *single* observation's own fingerprint is, in
/// effect, identical to a raw shape fingerprint (every field looks
/// "required" with `count == 1`). Two single-sample profiles of otherwise
/// related schemas that merely differ by one optional field (e.g.
/// `{"a":1}` vs `{"a":1,"opt":"x"}`) therefore NEVER share a full
/// fingerprint on their own — convergence can only happen by recognizing
/// that removing one field from the larger sample yields the smaller
/// sample's shape (or vice versa). Registering AND looking up every
/// single-field-dropped projection (in both directions, on every ingest)
/// makes that convergence hold regardless of which variant is observed
/// first.
///
/// Deliberately bounded to ONE field removed at a time (not the full power
/// set) and deliberately skips reducing all the way down to zero
/// top-level fields: an empty-object projection carries no discriminating
/// information at all, so registering it as a cluster alias would
/// spuriously merge every unrelated single-required-field schema (e.g.
/// `{"x":1}` and `{"y":2}` would otherwise collide on "drop the only
/// field"). This intentionally does not generalize to nested/array-element
/// optional fields, or to two-or-more simultaneously-optional top-level
/// fields — a documented P1 scope limitation, not a claim of complete
/// schema unification.
fn reduced_generalized_fps(profile: &Profile) -> Vec<[u8; 32]> {
    let mut out = vec![profile.generalized_fingerprint()];
    let children = &profile.root.children;
    if children.len() >= 2 {
        for key in children.keys() {
            let mut reduced_children = children.clone();
            reduced_children.remove(key);
            let reduced_root = FieldNode {
                children: reduced_children,
                ..profile.root.clone()
            };
            let reduced = Profile {
                count: profile.count,
                root: reduced_root,
            };
            out.push(reduced.generalized_fingerprint());
        }
    }
    out
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_fingerprint::{parse_bounded, Limits};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// In-memory `EvidenceStore` fake: candidates keyed by id, plus the
    /// cluster alias map Task 14 adds to the trait.
    #[derive(Default)]
    struct FakeEvidence {
        candidates: StdMutex<HashMap<CandidateId, CandidateRecord>>,
        clusters: StdMutex<HashMap<String, CandidateId>>,
    }

    #[async_trait::async_trait]
    impl EvidenceStore for FakeEvidence {
        async fn upsert_candidate(&self, rec: CandidateRecord) -> Result<(), CoreError> {
            self.candidates
                .lock()
                .unwrap()
                .insert(rec.candidate_id.clone(), rec);
            Ok(())
        }

        async fn get_candidate(
            &self,
            id: &CandidateId,
        ) -> Result<Option<CandidateRecord>, CoreError> {
            Ok(self.candidates.lock().unwrap().get(id).cloned())
        }

        async fn list_candidates(
            &self,
            state: CandidateState,
            _cursor: Option<String>,
            limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            let items: Vec<_> = self
                .candidates
                .lock()
                .unwrap()
                .values()
                .filter(|c| c.state == state)
                .take(limit)
                .cloned()
                .collect();
            Ok((items, None))
        }

        async fn append_evidence(
            &self,
            _id: &CandidateId,
            _stats: serde_json::Value,
        ) -> Result<(), CoreError> {
            Ok(())
        }

        async fn set_state(
            &self,
            id: &CandidateId,
            state: CandidateState,
        ) -> Result<(), CoreError> {
            if let Some(rec) = self.candidates.lock().unwrap().get_mut(id) {
                rec.state = state;
            }
            Ok(())
        }

        async fn get_cluster(&self, gen_fp: &str) -> Result<Option<CandidateId>, CoreError> {
            Ok(self.clusters.lock().unwrap().get(gen_fp).cloned())
        }

        async fn set_cluster(&self, gen_fp: &str, cand_id: &CandidateId) -> Result<(), CoreError> {
            self.clusters
                .lock()
                .unwrap()
                .insert(gen_fp.to_string(), cand_id.clone());
            Ok(())
        }
    }

    fn node_of(json: &str) -> Node {
        parse_bounded(json.as_bytes(), &Limits::default()).unwrap()
    }

    fn cand_id_of(json: &str) -> CandidateId {
        let node = node_of(json);
        let shape = deblob_fingerprint::shape_of(&node);
        CandidateId::from_digest(&deblob_fingerprint::fingerprint(&shape))
    }

    fn meta(source: &str) -> SampleMeta {
        SampleMeta {
            source: source.to_string(),
            cursor: None,
        }
    }

    #[tokio::test]
    async fn optional_variants_cluster_to_one_candidate() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        let base = r#"{"a":1}"#;
        let variant = r#"{"a":1,"opt":"x"}"#;
        let base_id = cand_id_of(base);
        let variant_id = cand_id_of(variant);
        assert_ne!(base_id, variant_id, "raw shapes must differ");

        let out1 = lane
            .ingest(base_id.clone(), &node_of(base), meta("src-a"))
            .await
            .unwrap();
        assert_eq!(out1, IngestOutcome::Ingested);

        let out2 = lane
            .ingest(variant_id.clone(), &node_of(variant), meta("src-a"))
            .await
            .unwrap();
        assert_eq!(out2, IngestOutcome::Ingested);

        // Exactly one candidate stored, at the FIRST raw id observed
        // (base_id) — the variant clustered onto it.
        let candidates = evidence.candidates.lock().unwrap();
        assert_eq!(candidates.len(), 1);
        let stored = candidates.get(&base_id).expect("base_id candidate stored");
        assert_eq!(stored.sample_count, 2);
        assert!(!candidates.contains_key(&variant_id));
    }

    #[tokio::test]
    async fn distinct_schemas_do_not_cluster() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        let a = r#"{"a":1}"#;
        let b = r#"{"totally":"different","shape":true}"#;

        lane.ingest(cand_id_of(a), &node_of(a), meta("src-a"))
            .await
            .unwrap();
        lane.ingest(cand_id_of(b), &node_of(b), meta("src-a"))
            .await
            .unwrap();

        assert_eq!(evidence.candidates.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn rate_limiter_blocks_11th_new_candidate_per_source() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        for i in 0..10u8 {
            let json = format!(r#"{{"distinct_key_{i}":true}}"#);
            let out = lane
                .ingest(cand_id_of(&json), &node_of(&json), meta("src-a"))
                .await
                .unwrap();
            assert_eq!(out, IngestOutcome::Ingested, "sample {i} should ingest");
        }

        // 11th brand-new candidate from the SAME source in the same window
        // is blocked.
        let eleventh = r#"{"distinct_key_10":true}"#;
        let out = lane
            .ingest(cand_id_of(eleventh), &node_of(eleventh), meta("src-a"))
            .await
            .unwrap();
        assert_eq!(out, IngestOutcome::RateLimited);
        assert_eq!(evidence.candidates.lock().unwrap().len(), 10);

        // A brand-new candidate from a DIFFERENT source in the same window
        // is NOT blocked.
        let from_b = r#"{"distinct_key_b":true}"#;
        let out_b = lane
            .ingest(cand_id_of(from_b), &node_of(from_b), meta("src-b"))
            .await
            .unwrap();
        assert_eq!(out_b, IngestOutcome::Ingested);
        assert_eq!(evidence.candidates.lock().unwrap().len(), 11);
    }

    #[tokio::test]
    async fn repeat_observation_of_existing_candidate_is_never_rate_limited() {
        // Exhaust the source's new-candidate quota, then keep re-observing
        // an already-known candidate: it must never be dropped, since it's
        // not a *new* candidate.
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        let known = r#"{"known":true}"#;
        lane.ingest(cand_id_of(known), &node_of(known), meta("src-a"))
            .await
            .unwrap();

        for i in 0..15u8 {
            let json = format!(r#"{{"filler_{i}":true}}"#);
            let _ = lane
                .ingest(cand_id_of(&json), &node_of(&json), meta("src-a"))
                .await
                .unwrap();
        }

        // Quota for src-a is now exhausted for this window; re-observing
        // the already-known candidate must still succeed.
        let out = lane
            .ingest(cand_id_of(known), &node_of(known), meta("src-a"))
            .await
            .unwrap();
        assert_eq!(out, IngestOutcome::Ingested);
    }

    #[test]
    fn evidence_stats_carry_no_raw_values() {
        // §9: appended evidence is `Profile`'s serde form — presence/type
        // counts only. Assert the serialized stats JSON never contains the
        // literal observed string value, only structural counters.
        let node = node_of(r#"{"secret":"super-sensitive-value-12345"}"#);
        let profile = Profile::from_node(&node);
        let stats = serde_json::to_value(&profile).unwrap();
        let rendered = stats.to_string();
        assert!(!rendered.contains("super-sensitive-value-12345"));
    }
}
