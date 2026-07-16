//! Hot-path matcher: the per-message deterministic decision table (spec
//! §3.1, §10).
//!
//! `HotMatcher::classify` is the synchronous, per-message hot path:
//! bounded parse → structural fingerprint → LRU exact-match (zero Redis
//! round-trip on hit) → Redis bucketed structural index → exactly one
//! [`deblob_core::id::SchemaRef`] outcome. A registry outage tags
//! `Unresolved`, **never** `Provisional` — minting a `cand_` id during an
//! outage would create a candidate storm once the registry recovers (spec
//! §10).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use deblob_core::error::QuarantineReason;
use deblob_core::id::{CandidateId, SchemaId, SchemaRef};
use deblob_core::ports::Registry;
use deblob_fingerprint::{bucket_key, fingerprint, parse_bounded, shape_of, summarize, Limits};
use lru::LruCache;
use parking_lot::Mutex;

use crate::metrics::Metrics;

/// Outcome of classifying one message on the hot path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    /// The tag decided for this message; the single canonical
    /// `deblob-schema-id` header value (spec §3.1).
    pub schema_ref: SchemaRef,
    /// Set only when `schema_ref` is [`SchemaRef::Malformed`]: why the
    /// bounded parse rejected the payload.
    pub quarantine: Option<QuarantineReason>,
    /// The raw structural fingerprint, when the payload parsed
    /// successfully (`None` for `Malformed`).
    pub raw_fp: Option<[u8; 32]>,
    /// The structural-index bucket key the fingerprint was looked up
    /// under, when the payload parsed successfully (`None` for
    /// `Malformed`).
    pub bucket: Option<String>,
}

/// The per-message deterministic decision table (spec §3.1, §10):
///
/// | Condition | Outcome |
/// |---|---|
/// | bounded parse fails | `Malformed` + reason, no registry call |
/// | LRU exact-match hit | `Known`, zero registry calls |
/// | LRU miss, index hit | `Known`, LRU filled for next time |
/// | LRU miss, index miss | `Provisional(cand_<raw shape digest>)` |
/// | registry error | `Unresolved` (never `cand_`) |
pub struct HotMatcher {
    registry: Arc<dyn Registry>,
    lru: Mutex<LruCache<[u8; 32], SchemaId>>,
    metrics: Arc<Metrics>,
}

impl HotMatcher {
    /// Build a matcher backed by `registry`, with an exact-match LRU cache
    /// holding up to `lru_capacity` fingerprint → schema entries, reporting
    /// into `metrics` (spec §11). A `lru_capacity` of `0` is treated as `1`
    /// so `LruCache::new` never panics on a degenerate config value.
    pub fn new(registry: Arc<dyn Registry>, lru_capacity: usize, metrics: Arc<Metrics>) -> Self {
        let capacity = NonZeroUsize::new(lru_capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            registry,
            lru: Mutex::new(LruCache::new(capacity)),
            metrics,
        }
    }

    /// Classify one message against the decision table on [`HotMatcher`]'s
    /// docs. Never panics on malformed input — every failure mode of
    /// `parse_bounded` becomes `Malformed` with a reason, and a registry
    /// error becomes `Unresolved` rather than propagating.
    ///
    /// Every outcome increments `deblob_messages_total{fate}` and observes
    /// `deblob_tag_latency_seconds` exactly once (spec §11); the payload
    /// itself is never touched by a metric label or a log field — only
    /// bounded, derived values (fate, quarantine reason, latency) ever
    /// leave this function via `self.metrics`.
    pub async fn classify(&self, payload: &[u8], limits: &Limits) -> Classification {
        let started = Instant::now();

        let node = match parse_bounded(payload, limits) {
            Ok(node) => node,
            Err(reason) => {
                self.metrics.record_quarantine(reason);
                let classification = Classification {
                    schema_ref: SchemaRef::Malformed,
                    quarantine: Some(reason),
                    raw_fp: None,
                    bucket: None,
                };
                self.metrics
                    .record_classification(&classification.schema_ref);
                self.metrics.observe_tag_latency(started.elapsed());
                tracing::debug!(
                    reason = crate::metrics::quarantine_reason_label(reason),
                    "quarantined malformed message"
                );
                return classification;
            }
        };

        let shape = shape_of(&node);
        let raw_fp = fingerprint(&shape);
        let bucket = bucket_key(&summarize(&shape));

        if let Some(known) = self.lru.lock().get(&raw_fp).cloned() {
            self.metrics.record_cache_hit();
            let classification = Classification {
                schema_ref: SchemaRef::Known(known),
                quarantine: None,
                raw_fp: Some(raw_fp),
                bucket: Some(bucket),
            };
            self.metrics
                .record_classification(&classification.schema_ref);
            self.metrics.observe_tag_latency(started.elapsed());
            return classification;
        }

        let fp_id = SchemaId::from_digest(&raw_fp);
        let registry_started = Instant::now();
        let resolved = self.registry.resolve_structural(&bucket, &fp_id).await;
        self.metrics
            .observe_registry_op("resolve_structural", registry_started.elapsed());

        let schema_ref = match resolved {
            Ok(Some(known)) => {
                self.lru.lock().put(raw_fp, known.clone());
                SchemaRef::Known(known)
            }
            Ok(None) => SchemaRef::Provisional(CandidateId::from_digest(&raw_fp)),
            // A registry outage must never mint a candidate: that would
            // create a candidate storm once the registry recovers (§10).
            Err(_) => SchemaRef::Unresolved,
        };
        self.metrics.record_classification(&schema_ref);

        let classification = Classification {
            schema_ref,
            quarantine: None,
            raw_fp: Some(raw_fp),
            bucket: Some(bucket),
        };
        self.metrics.observe_tag_latency(started.elapsed());
        classification
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::error::CoreError;
    use deblob_core::id::FamilyVersion;
    use deblob_core::ports::{CandidateRecord, CandidateState, SchemaRecord};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// What `FakeRegistry::resolve_structural` should return, programmable
    /// per test: an index hit, an index miss, or a simulated outage.
    #[derive(Clone)]
    enum ResolveResponse {
        Hit(SchemaId),
        Miss,
        Err,
    }

    /// In-memory fake implementing the *full* current `Registry` trait
    /// (Task 7). Only `resolve_structural` is exercised by the hot-path
    /// matcher; every other method panics if called, since a call to any
    /// of them would mean the hot path stopped being "deterministic-only,
    /// never waits on the model" (spec §3.1) — it never publishes, never
    /// reads a schema by id, never lists.
    struct FakeRegistry {
        response: StdMutex<ResolveResponse>,
        resolve_calls: AtomicUsize,
    }

    impl FakeRegistry {
        fn new(response: ResolveResponse) -> Self {
            Self {
                response: StdMutex::new(response),
                resolve_calls: AtomicUsize::new(0),
            }
        }

        fn resolve_call_count(&self) -> usize {
            self.resolve_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Registry for FakeRegistry {
        async fn get_schema(&self, _id: &SchemaId) -> Result<Option<SchemaRecord>, CoreError> {
            unimplemented!("hot-path matcher must never read a schema by id directly")
        }

        async fn resolve_structural(
            &self,
            _bucket_key: &str,
            _fingerprint: &SchemaId,
        ) -> Result<Option<SchemaId>, CoreError> {
            self.resolve_calls.fetch_add(1, Ordering::SeqCst);
            match &*self.response.lock().unwrap() {
                ResolveResponse::Hit(id) => Ok(Some(id.clone())),
                ResolveResponse::Miss => Ok(None),
                ResolveResponse::Err => {
                    Err(CoreError::RegistryUnavailable("simulated outage".into()))
                }
            }
        }

        async fn publish(
            &self,
            _record: SchemaRecord,
            _alias_from: &CandidateId,
            _bucket_key: &str,
            _variant_members: &[(String, String)],
            _actor: &str,
            _reason: &str,
        ) -> Result<FamilyVersion, CoreError> {
            unimplemented!("hot-path matcher must never publish")
        }

        async fn get_alias(&self, _id: &CandidateId) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("hot-path matcher must never resolve aliases")
        }

        async fn list_schemas(
            &self,
            _cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<SchemaRecord>, Option<String>), CoreError> {
            unimplemented!("hot-path matcher must never list schemas")
        }

        async fn list_families_in_buckets(
            &self,
            _bucket_keys: &[String],
        ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
            unimplemented!("hot-path matcher must never run retrieval")
        }
        async fn list_families_by_band_depth(
            &self,
            _bands: &[u32],
            _depths: &[u32],
        ) -> Result<Vec<deblob_core::ports::FamilyRef>, CoreError> {
            unimplemented!("hot-path matcher must never run retrieval")
        }
        async fn family_version_schema(
            &self,
            _family_id: &deblob_core::id::FamilyId,
            _version: deblob_core::id::FamilyVersion,
        ) -> Result<Option<SchemaId>, CoreError> {
            unimplemented!("hot-path matcher must never run retrieval")
        }

        async fn get_family(
            &self,
            _family_id: &deblob_core::id::FamilyId,
        ) -> Result<Option<deblob_core::ports::FamilyRecord>, CoreError> {
            unimplemented!("hot-path matcher must never run retrieval")
        }

        async fn list_family_versions(
            &self,
            _family_id: &deblob_core::id::FamilyId,
        ) -> Result<Vec<deblob_core::id::FamilyVersion>, CoreError> {
            unimplemented!("hot-path matcher must never run retrieval")
        }
    }

    // Silence "unused" on evidence-store-shaped items pulled in only so the
    // fake's imports compile identically to a real store-backed registry.
    #[allow(dead_code)]
    fn _unused_candidate_record_shape(_r: CandidateRecord) -> CandidateState {
        CandidateState::Provisional
    }

    fn matcher(fake: Arc<FakeRegistry>) -> HotMatcher {
        HotMatcher::new(fake, 16, Metrics::new())
    }

    /// Like [`matcher`], but also hands back the `Metrics` instance so a
    /// test can gather its registry and assert on recorded values.
    fn matcher_with_metrics(fake: Arc<FakeRegistry>) -> (HotMatcher, Arc<Metrics>) {
        let metrics = Metrics::new();
        (HotMatcher::new(fake, 16, metrics.clone()), metrics)
    }

    // Row 1: parse fails → Malformed + reason, no registry call attempted.
    #[tokio::test]
    async fn parse_error_tags_malformed_without_registry_call() {
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Miss));
        let m = matcher(fake.clone());

        let out = m.classify(b"{not json", &Limits::default()).await;

        assert_eq!(out.schema_ref, SchemaRef::Malformed);
        assert!(out.quarantine.is_some());
        assert_eq!(out.raw_fp, None);
        assert_eq!(out.bucket, None);
        assert_eq!(fake.resolve_call_count(), 0);
    }

    // Row 2: LRU hit → Known, zero further registry calls.
    #[tokio::test]
    async fn lru_hit_returns_known_with_zero_registry_calls() {
        let known = SchemaId::from_digest(&[7u8; 32]);
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Hit(known.clone())));
        let m = matcher(fake.clone());
        let payload = br#"{"a":1,"b":"x"}"#;

        let first = m.classify(payload, &Limits::default()).await;
        assert_eq!(first.schema_ref, SchemaRef::Known(known.clone()));
        assert_eq!(fake.resolve_call_count(), 1);

        // Second classify of the identical payload must be an LRU hit:
        // same answer, no growth in registry calls.
        let second = m.classify(payload, &Limits::default()).await;
        assert_eq!(second.schema_ref, SchemaRef::Known(known));
        assert_eq!(fake.resolve_call_count(), 1);
    }

    // Row 3: index hit (LRU miss) → Known, and fills the LRU.
    #[tokio::test]
    async fn index_hit_returns_known_and_fills_lru() {
        let known = SchemaId::from_digest(&[9u8; 32]);
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Hit(known.clone())));
        let m = matcher(fake.clone());
        let payload = br#"{"x":1}"#;

        let out = m.classify(payload, &Limits::default()).await;
        assert_eq!(out.schema_ref, SchemaRef::Known(known));
        assert_eq!(fake.resolve_call_count(), 1);

        // LRU is now filled for this payload's fingerprint: a second
        // classify must not touch the registry again.
        let _ = m.classify(payload, &Limits::default()).await;
        assert_eq!(fake.resolve_call_count(), 1);
    }

    // Row 4: index miss → Provisional(cand_<raw digest>), deterministic.
    #[tokio::test]
    async fn index_miss_returns_deterministic_provisional() {
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Miss));
        let m = matcher(fake.clone());
        let payload = br#"{"unseen":true}"#;

        let out = m.classify(payload, &Limits::default()).await;

        let node = parse_bounded(payload, &Limits::default()).unwrap();
        let raw_fp = fingerprint(&shape_of(&node));
        let expected = CandidateId::from_digest(&raw_fp);

        assert_eq!(out.schema_ref, SchemaRef::Provisional(expected));
        assert_eq!(out.raw_fp, Some(raw_fp));
        assert!(out.bucket.is_some());
        assert_eq!(fake.resolve_call_count(), 1);
    }

    // Row 5: registry error → Unresolved, NEVER Provisional/cand_ (an
    // outage must not create a candidate storm, spec §10).
    #[tokio::test]
    async fn registry_error_returns_unresolved_never_candidate() {
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Err));
        let m = matcher(fake.clone());

        let out = m.classify(br#"{"x":1}"#, &Limits::default()).await;

        assert_eq!(out.schema_ref, SchemaRef::Unresolved);
        assert_ne!(
            std::mem::discriminant(&out.schema_ref),
            std::mem::discriminant(&SchemaRef::Provisional(CandidateId::from_digest(&[0; 32])))
        );
        assert_eq!(fake.resolve_call_count(), 1);
    }

    // Explicit LRU-skips-Redis test: repeated classifies of the same
    // payload only ever hit the registry once.
    #[tokio::test]
    async fn lru_hit_skips_registry() {
        let known = SchemaId::from_digest(&[3u8; 32]);
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Hit(known)));
        let m = matcher(fake.clone());
        let payload = br#"{"repeat":1}"#;

        for _ in 0..3 {
            let _ = m.classify(payload, &Limits::default()).await;
        }

        assert_eq!(fake.resolve_call_count(), 1);
    }

    // Explicit determinism test: classifying the same unknown payload
    // twice must mint the identical cand_ id both times (replay-stable,
    // spec §3.2 — never mint a fresh id on replay).
    #[tokio::test]
    async fn cand_id_is_deterministic() {
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Miss));
        let m = matcher(fake.clone());
        let payload = br#"{"never":"seen","before":1}"#;

        let first = m.classify(payload, &Limits::default()).await;
        let second = m.classify(payload, &Limits::default()).await;

        assert_eq!(first.schema_ref, second.schema_ref);
        match first.schema_ref {
            SchemaRef::Provisional(_) => {}
            other => panic!("expected Provisional, got {other:?}"),
        }
    }

    // Task 15 (spec §11): a malformed, duplicate-key payload must tag
    // `deblob_messages_total{fate="malformed"}` AND
    // `deblob_quarantine_records_total{reason="duplicate_key"}`, each
    // exactly once, in the SAME registry gather.
    #[tokio::test]
    async fn quarantine_metric_increments() {
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Miss));
        let (m, metrics) = matcher_with_metrics(fake);

        let out = m.classify(br#"{"a":1,"a":2}"#, &Limits::default()).await;
        assert_eq!(out.schema_ref, SchemaRef::Malformed);
        assert_eq!(out.quarantine, Some(QuarantineReason::DuplicateKey));

        let families = metrics.registry().gather();
        assert_eq!(
            crate::metrics::test_support::value_of(
                &families,
                "deblob_messages_total",
                Some(("fate", "malformed"))
            ),
            1.0
        );
        assert_eq!(
            crate::metrics::test_support::value_of(
                &families,
                "deblob_quarantine_records_total",
                Some(("reason", "duplicate_key"))
            ),
            1.0
        );
    }

    // Task 15 (spec §11): a known-schema classify increments
    // `deblob_messages_total{fate="known"}`; a SECOND identical classify
    // (now an LRU exact-match hit) increments `deblob_cache_hits_total`
    // without a second registry call.
    #[tokio::test]
    async fn known_and_cache_metrics() {
        let known = SchemaId::from_digest(&[11u8; 32]);
        let fake = Arc::new(FakeRegistry::new(ResolveResponse::Hit(known)));
        let (m, metrics) = matcher_with_metrics(fake.clone());
        let payload = br#"{"cached":true}"#;

        let first = m.classify(payload, &Limits::default()).await;
        assert!(matches!(first.schema_ref, SchemaRef::Known(_)));

        let families = metrics.registry().gather();
        assert_eq!(
            crate::metrics::test_support::value_of(
                &families,
                "deblob_messages_total",
                Some(("fate", "known"))
            ),
            1.0
        );
        assert_eq!(
            crate::metrics::test_support::value_of(&families, "deblob_cache_hits_total", None),
            0.0,
            "first classify is a registry hit, not an LRU cache hit"
        );

        let second = m.classify(payload, &Limits::default()).await;
        assert!(matches!(second.schema_ref, SchemaRef::Known(_)));
        assert_eq!(
            fake.resolve_call_count(),
            1,
            "second classify must be an LRU hit"
        );

        let families = metrics.registry().gather();
        assert_eq!(
            crate::metrics::test_support::value_of(
                &families,
                "deblob_messages_total",
                Some(("fate", "known"))
            ),
            2.0
        );
        assert_eq!(
            crate::metrics::test_support::value_of(&families, "deblob_cache_hits_total", None),
            1.0
        );
    }
}
