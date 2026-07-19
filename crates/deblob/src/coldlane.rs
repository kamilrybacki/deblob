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
//!
//! CANDIDATE clustering is SOURCE-SCOPED (Hermes lineage gap 3): the
//! generalized-fingerprint cluster map convergence described above only
//! ever merges observations from the SAME `meta.source` — see
//! [`scoped_gen_fp`]. Two different sources (Kafka topics, or an HTTP
//! proxy route's `origin_prefix`) that happen to observe the exact same
//! shape mint and cluster onto DISTINCT candidates: "source co-occurrence
//! is provenance, not semantic evidence," never grounds for merging. This
//! is deliberately narrower than [`deblob_core::ports::Registry::
//! resolve_structural`]'s KNOWN-schema structural retrieval, which stays
//! GLOBAL/source-blind for now — widening retrieval the same way is a
//! documented follow-up, out of scope here.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use data_encoding::{BASE32_NOPAD, HEXLOWER};
use deblob_core::envelope::SourceCursor;
use deblob_core::error::CoreError;
use deblob_core::id::CandidateId;
use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
use deblob_fingerprint::{bucket_key, fingerprint, shape_of, summarize, Node};
use deblob_monoid::{FieldNode, Profile};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};

use crate::metrics::Metrics;

/// Re-exported so `deblob::coldlane::DiscoveryMsg` keeps resolving after
/// Task 18 moved the type's definition to `deblob-match` (so `deblob-kafka`
/// can depend on it without depending on the `deblob` package — see
/// `deblob_match`'s crate docs).
pub use deblob_match::discovery::DiscoveryMsg;

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

/// Outcome of one `ColdLane::ingest` call. Rate-limited ingestion is
/// dropped and counted, never panicked (spec brief) — the caller sees this
/// variant instead of an error so a burst from one source degrades
/// observably rather than propagating as a failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestOutcome {
    /// Successfully ingested. `candidate_id` is the RESOLVED id AFTER
    /// generalized clustering (a raw pre-cluster `cand_id` may redirect onto an
    /// existing candidate) — the sample store MUST key on this, not on
    /// `DiscoveryMsg.cand_id`, or samples attach to the wrong candidate (joint
    /// design `dc-samples-dlp-1907`). `is_new` is `true` iff no candidate
    /// record existed under `candidate_id` before this observation.
    Ingested { candidate_id: CandidateId, is_new: bool },
    RateLimited,
}

/// The cold lane: read-merge-write candidate accumulation backed by an
/// [`EvidenceStore`], with per-source rate limiting on brand-new
/// candidates.
pub struct ColdLane {
    evidence: Arc<dyn EvidenceStore>,
    limiter: DefaultKeyedRateLimiter<String>,
    /// `None` for the plain constructors (existing call sites, unit/
    /// integration tests that don't care about observability) — `ingest`
    /// simply skips the `deblob_candidates_active` increment when this is
    /// unset rather than requiring every caller to thread a `Metrics`
    /// handle through.
    metrics: Option<Arc<Metrics>>,
}

impl ColdLane {
    /// Build a `ColdLane` with the default rate limit
    /// ([`DEFAULT_NEW_CANDIDATES_PER_MIN`] new candidates/min/source) and no
    /// metrics wired up.
    pub fn new(evidence: Arc<dyn EvidenceStore>) -> Self {
        Self::with_rate_limit(evidence, DEFAULT_NEW_CANDIDATES_PER_MIN)
    }

    /// Build a `ColdLane` with a caller-supplied new-candidates-per-minute
    /// limit (per source) and no metrics wired up. `per_minute == 0` is
    /// treated as `1` so `Quota::per_minute` never panics on a degenerate
    /// config value.
    pub fn with_rate_limit(evidence: Arc<dyn EvidenceStore>, per_minute: u32) -> Self {
        let quota = Quota::per_minute(NonZeroU32::new(per_minute).unwrap_or(NonZeroU32::MIN));
        Self {
            evidence,
            limiter: RateLimiter::keyed(quota),
            metrics: None,
        }
    }

    /// Build a `ColdLane` with the default rate limit, reporting into
    /// `metrics` (spec §11): every genuinely new candidate increments
    /// `deblob_candidates_active`.
    pub fn with_metrics(evidence: Arc<dyn EvidenceStore>, metrics: Arc<Metrics>) -> Self {
        Self::with_rate_limit_and_metrics(evidence, DEFAULT_NEW_CANDIDATES_PER_MIN, metrics)
    }

    /// Build a `ColdLane` with a caller-supplied rate limit AND metrics
    /// wired up.
    pub fn with_rate_limit_and_metrics(
        evidence: Arc<dyn EvidenceStore>,
        per_minute: u32,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut lane = Self::with_rate_limit(evidence, per_minute);
        lane.metrics = Some(metrics);
        lane
    }

    /// Merge one observed `node` into the candidate `cand_id` resolves to
    /// (after generalized-fingerprint clustering), append stats-only
    /// evidence, and record the observation's cluster alias.
    ///
    /// `cand_id` is the hot path's raw-shape-derived candidate id (spec
    /// §3.1/§3.2) — the *first* observation of a brand-new generalized
    /// schema always ingests under this id; every later observation of an
    /// optional-field variant of the *same* generalized schema, FROM THE
    /// SAME `meta.source` (Hermes lineage gap 3), converges onto whichever
    /// candidate the cluster map already points at, even though its own raw
    /// `cand_id` differs. An observation from a DIFFERENT source never
    /// clusters onto another source's candidate, even sharing the exact
    /// same generalized fingerprint — see [`scoped_gen_fp`].
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
            let scoped = scoped_gen_fp(&meta.source, &hex);
            if let Some(existing) = self.evidence.get_cluster(&scoped).await? {
                clustered = Some(existing);
                break;
            }
        }
        let target_id = clustered.unwrap_or(cand_id);

        let existing = self.evidence.get_candidate(&target_id).await?;
        // Same "genuinely brand-new" test the rate limiter uses below,
        // captured once so the `deblob_candidates_active` increment after a
        // successful ingest and the rate-limit check can't drift apart.
        let is_new_candidate = existing.is_none();

        // Only a genuinely brand-new candidate (no cluster hit AND no
        // existing record under its raw id) counts against the per-source
        // rate limit — repeat observations of an already-known candidate
        // are not new candidates.
        if is_new_candidate && self.limiter.check_key(&meta.source).is_err() {
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
            // Hermes review gap 2: the REAL per-record source (now the
            // actual consumed record's topic — see
            // `deblob-kafka::relay`'s `DiscoveryMsg.source` fix — rather
            // than a static config value), persisted onto the candidate so
            // `GET /api/v1/candidates` can surface it. Always set from this
            // observation's `meta.source` (last-observation-wins, same
            // "latest write" posture as `last_seen_ms` above). This FIELD
            // itself is provenance/observability only, never read back as a
            // key — CANDIDATE clustering (Hermes lineage gap 3) is instead
            // source-scoped via this same `meta.source` value folded into
            // the candidate id mint (`HotMatcher::classify`) and the
            // cluster-map key (`scoped_gen_fp`, below); KNOWN-schema
            // structural retrieval (`Registry::resolve_structural`) stays
            // GLOBAL/source-blind, a documented follow-up.
            source: Some(meta.source.clone()),
        };

        self.evidence.upsert_candidate(record).await?;
        // §11: report a brand-new candidate exactly once, on the ingest
        // call that actually created its `CandidateRecord` — never on
        // repeat observations of an already-tracked candidate.
        if is_new_candidate {
            if let Some(metrics) = &self.metrics {
                metrics.inc_candidates_active();
            }
        }
        // Register every projection (full fingerprint + each top-level
        // single-field-dropped variant) as an alias for `target_id`, so a
        // FUTURE sample that's this one plus/minus exactly one top-level
        // field converges here regardless of which variant is observed
        // first.
        for fp in &candidate_fps {
            let hex = HEXLOWER.encode(fp);
            let scoped = scoped_gen_fp(&meta.source, &hex);
            self.evidence.set_cluster(&scoped, &target_id).await?;
        }

        // Task 14 fix (promote→resolve round trip): record this
        // observation's own CONCRETE-shape (bucket_key, raw fp base32 body)
        // against `target_id`, de-duplicated via the underlying Redis SET.
        // `Promoter::promote` later replays every variant recorded here
        // into the structural index, which is what lets a hot-path lookup
        // for THIS EXACT raw shape resolve to the schema once the
        // candidate is promoted — the schema's own `schema_id` is derived
        // from the GENERALIZED profile (a different fingerprint domain,
        // spec §5) and can never equal a concrete observation's raw digest
        // on its own.
        let (variant_bucket, variant_fp_b32) = concrete_variant(node);
        self.evidence
            .add_variant(&target_id, &variant_bucket, &variant_fp_b32)
            .await?;

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

        Ok(IngestOutcome::Ingested {
            candidate_id: target_id,
            is_new: is_new_candidate,
        })
    }
}

/// Source-scopes a generalized-fingerprint cluster key (Hermes lineage gap
/// 3): "source co-occurrence is provenance, not semantic evidence" — two
/// DIFFERENT sources (Kafka topics, or an HTTP proxy route's
/// `origin_prefix`) that happen to share the exact same generalized shape
/// must never converge onto the same candidate cluster, even though
/// [`EvidenceStore::get_cluster`]/[`EvidenceStore::set_cluster`]'s own
/// trait signature stays a plain `&str` key (deliberately unchanged, lowest
/// blast radius — every `EvidenceStore` implementation, real or fake,
/// keeps working unmodified; only `ColdLane::ingest`, the sole production
/// caller, needs to change).
///
/// `:` is a safe, unambiguous delimiter: Kafka topic names are restricted
/// to `[a-zA-Z0-9._-]` (`:` never appears in one), and `gen_fp_hex` is
/// always a fixed-width lowercase-hex string (`HEXLOWER` of a 32-byte
/// SHA-256 digest) — so no `(source, gen_fp_hex)` pair can collide with a
/// different pair's concatenation.
fn scoped_gen_fp(source: &str, gen_fp_hex: &str) -> String {
    format!("{source}:{gen_fp_hex}")
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

/// The `(bucket_key, fp_b32)` pair identifying `node`'s own CONCRETE shape —
/// exactly what the hot path (`crate::matcher::HotMatcher::classify`)
/// computes for an incoming message: `deblob_fingerprint::bucket_key` of its
/// summarized shape, and the base32 body of its raw
/// `deblob_fingerprint::fingerprint` digest (the same encoding
/// `SchemaId`/`CandidateId` use, computed directly rather than round-
/// tripping through either id type). Recorded per observation via
/// [`EvidenceStore::add_variant`] so promotion can index every concrete
/// shape actually seen, not just the candidate's generalized identity.
fn concrete_variant(node: &Node) -> (String, String) {
    let shape = shape_of(node);
    let raw_fp = fingerprint(&shape);
    let bucket = bucket_key(&summarize(&shape));
    let fp_b32 = BASE32_NOPAD.encode(&raw_fp).to_ascii_lowercase();
    (bucket, fp_b32)
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
        variants: StdMutex<HashMap<CandidateId, Vec<(String, String)>>>,
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

        async fn add_variant(
            &self,
            cand_id: &CandidateId,
            bucket_key: &str,
            fp_b32: &str,
        ) -> Result<(), CoreError> {
            let mut variants = self.variants.lock().unwrap();
            let entry = variants.entry(cand_id.clone()).or_default();
            let pair = (bucket_key.to_string(), fp_b32.to_string());
            if !entry.contains(&pair) {
                entry.push(pair);
            }
            Ok(())
        }

        async fn get_variants(
            &self,
            cand_id: &CandidateId,
        ) -> Result<Vec<(String, String)>, CoreError> {
            Ok(self
                .variants
                .lock()
                .unwrap()
                .get(cand_id)
                .cloned()
                .unwrap_or_default())
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

    /// The counter/gauge value of the family `name`, optionally filtered to
    /// the metric instance carrying label `(key, value)`. Duplicated (not
    /// reused) from `deblob_match::metrics`'s own `#[cfg(test)]`-only
    /// `test_support` module: that module is crate-private and only
    /// compiled under `deblob-match`'s OWN test config, so it isn't visible
    /// here across the crate boundary Task 18 introduced. Panics if the
    /// family doesn't exist at all (a real bug in the test).
    fn value_of(families: &[prometheus::proto::MetricFamily], name: &str) -> f64 {
        let family = families
            .iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("metric family {name:?} not found in gathered output"));
        family
            .get_metric()
            .iter()
            .find(|m| m.get_label().is_empty())
            .map(|m| {
                if m.has_counter() {
                    m.get_counter().get_value()
                } else if m.has_gauge() {
                    m.get_gauge().get_value()
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0)
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
        assert!(matches!(out1, IngestOutcome::Ingested { .. }));

        let out2 = lane
            .ingest(variant_id.clone(), &node_of(variant), meta("src-a"))
            .await
            .unwrap();
        assert!(matches!(out2, IngestOutcome::Ingested { .. }));

        // Exactly one candidate stored, at the FIRST raw id observed
        // (base_id) — the variant clustered onto it. Scoped in a block (not
        // just an explicit `drop`) so the `MutexGuard` is provably released
        // before the `.await` below — `clippy::await_holding_lock` doesn't
        // always credit a bare `drop()` call.
        {
            let candidates = evidence.candidates.lock().unwrap();
            assert_eq!(candidates.len(), 1);
            let stored = candidates.get(&base_id).expect("base_id candidate stored");
            assert_eq!(stored.sample_count, 2);
            assert!(!candidates.contains_key(&variant_id));
        }

        // Task 14 fix: BOTH concrete variants' (bucket, fp_b32) pairs must
        // be recorded against base_id (where they clustered), even though
        // only base_id ever entered a `CandidateRecord` — this is what
        // lets `Promoter::promote` later index both raw shapes, not just
        // whichever one seeded the candidate.
        let recorded = evidence.get_variants(&base_id).await.unwrap();
        assert_eq!(
            recorded.len(),
            2,
            "both variants must be recorded: {recorded:?}"
        );
        let expected_base = concrete_variant(&node_of(base));
        let expected_variant = concrete_variant(&node_of(variant));
        assert!(recorded.contains(&expected_base));
        assert!(recorded.contains(&expected_variant));
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

    // Hermes lineage gap 3 (the core fix): two DIFFERENT sources observing
    // the EXACT SAME shape must NOT converge onto one candidate — "source
    // co-occurrence is provenance, not semantic evidence." Candidate ids
    // here are minted exactly like the real hot path does
    // (`CandidateId::from_source_and_digest`), not the source-blind
    // `cand_id_of` fixture helper used elsewhere in this file, so this
    // test exercises the actual `(source, raw_fp) -> cand_id` contract
    // end to end, not just the cluster-map half of it.
    #[tokio::test]
    async fn different_sources_same_shape_do_not_cluster() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        let shape = r#"{"shared_shape":1}"#;
        let node = node_of(shape);
        let raw_fp = deblob_fingerprint::fingerprint(&deblob_fingerprint::shape_of(&node));
        let cand_a = CandidateId::from_source_and_digest("src-a", &raw_fp);
        let cand_b = CandidateId::from_source_and_digest("src-b", &raw_fp);
        assert_ne!(
            cand_a, cand_b,
            "sanity: source-scoped mint must differ across sources"
        );

        lane.ingest(cand_a.clone(), &node, meta("src-a"))
            .await
            .unwrap();
        lane.ingest(cand_b.clone(), &node, meta("src-b"))
            .await
            .unwrap();

        let candidates = evidence.candidates.lock().unwrap();
        assert_eq!(
            candidates.len(),
            2,
            "different sources must never cluster onto one candidate: {candidates:?}"
        );
        assert!(candidates.contains_key(&cand_a));
        assert!(candidates.contains_key(&cand_b));
    }

    // The SAME source re-observing the SAME shape must still reuse the one
    // candidate it already created (sample_count accumulates rather than a
    // second `CandidateRecord` being minted) — the positive counterpart to
    // `different_sources_same_shape_do_not_cluster` above, proving
    // source-scoping didn't accidentally make EVERY ingest a fresh
    // candidate.
    #[tokio::test]
    async fn same_source_same_shape_reuses_one_candidate() {
        let evidence = Arc::new(FakeEvidence::default());
        let lane = ColdLane::new(evidence.clone());

        let shape = r#"{"repeated_shape":1}"#;
        let node = node_of(shape);
        let raw_fp = deblob_fingerprint::fingerprint(&deblob_fingerprint::shape_of(&node));
        let cand_id = CandidateId::from_source_and_digest("src-a", &raw_fp);

        lane.ingest(cand_id.clone(), &node, meta("src-a"))
            .await
            .unwrap();
        lane.ingest(cand_id.clone(), &node, meta("src-a"))
            .await
            .unwrap();

        let candidates = evidence.candidates.lock().unwrap();
        assert_eq!(candidates.len(), 1, "same source must reuse one candidate");
        let stored = candidates.get(&cand_id).expect("candidate stored");
        assert_eq!(stored.sample_count, 2);
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
            assert!(matches!(out, IngestOutcome::Ingested { .. }), "sample {i} should ingest");
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
        assert!(matches!(out_b, IngestOutcome::Ingested { .. }));
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
        assert!(matches!(out, IngestOutcome::Ingested { .. }));
    }

    // Task 15 (spec §11): a brand-new candidate increments
    // `deblob_candidates_active`; a repeat observation of the SAME
    // candidate must not increment it again.
    #[tokio::test]
    async fn candidate_creation_increments_active_gauge() {
        let evidence = Arc::new(FakeEvidence::default());
        let metrics = Metrics::new();
        let lane = ColdLane::with_metrics(evidence.clone(), metrics.clone());

        let payload = r#"{"brand_new":true}"#;
        lane.ingest(cand_id_of(payload), &node_of(payload), meta("src-a"))
            .await
            .unwrap();

        let families = metrics.registry().gather();
        assert_eq!(value_of(&families, "deblob_candidates_active"), 1.0);

        // Re-observing the same candidate must not double-count it.
        lane.ingest(cand_id_of(payload), &node_of(payload), meta("src-a"))
            .await
            .unwrap();
        let families = metrics.registry().gather();
        assert_eq!(value_of(&families, "deblob_candidates_active"), 1.0);
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
