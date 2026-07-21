//! Automatic candidate promotion sweep — OFF unless `[auto_promote].enabled`.
//!
//! Governance note: deblob's authority model is "the model proposes,
//! deterministic code + policy decides." This sweep does not change that. The
//! cold lane (model-assisted) is what PROPOSES a candidate; this worker only
//! promotes a candidate once it has crossed a purely deterministic bar on
//! deterministic evidence ([`crate::policy::AutoPromotePolicy`]: sample count,
//! observation age, and a settled REQUIRED-field backbone). Gold-umbrella
//! consolidation remains human-in-the-loop and is untouched. When the section
//! is absent or `enabled=false` (the default), no task is spawned at all and
//! promotion stays entirely operator-driven.
//!
//! Idempotency: promoting a candidate moves it out of the `Provisional` set
//! (`Registry::publish` aliases it), so the next sweep no longer sees it. A
//! candidate that fails its promote (e.g. transient registry error) simply
//! stays provisional and is retried on the next tick.

use std::sync::Arc;
use std::time::Duration;

use deblob_core::ports::{CandidateRecord, CandidateState, EvidenceStore};
use tokio_util::sync::CancellationToken;

use crate::policy::AutoPromotePolicy;
use crate::promote::{FamilyChoice, PromoteRequest, Promoter as PromoterTrait};

/// Recorded in the immutable audit trail as the promoting actor for every
/// auto-promotion, distinguishing it from an operator's token identity.
pub const AUTO_PROMOTE_ACTOR: &str = "deblob-auto-promote";

/// Page size for the provisional-candidate scan. Smaller than the shadow
/// sweep's 500: each eligible candidate here triggers a full `Registry::publish`
/// (Redis Lua write + audit), not shadow's read-only classify, so a smaller
/// page bounds worst-case tick latency and lets the per-tick promotion cap and
/// shutdown check take effect between pages promptly.
const SWEEP_PAGE_SIZE: usize = 128;

/// `true` iff `source` is one of the operator-trusted `allowed`. Default-deny:
/// an empty allowlist (or a candidate with no recorded source) never matches.
fn source_allowed(source: Option<&str>, allowed: &[String]) -> bool {
    match source {
        Some(s) => allowed.iter().any(|a| a == s),
        None => false,
    }
}

/// The promote request for an eligible candidate: always a NEW family (a
/// novel candidate has, by definition, no existing family to join), with a
/// reason string that records the evidence that cleared the bar. Pure — unit
/// tested without any I/O.
pub(crate) fn auto_promote_request(cand: &CandidateRecord, now_ms: i64) -> PromoteRequest {
    // Wall-clock age (now - first_seen) — matches the gate in `PromotionPolicy::
    // check`. Must NOT be the observation span (last_seen - first_seen): a burst
    // source has a ~0 span but is genuinely old, so the audit reason would read a
    // misleading "over 3ms" while the gate actually cleared it on wall-clock age.
    let age_ms = now_ms - cand.first_seen_ms;
    PromoteRequest {
        family: FamilyChoice::New,
        name: None,
        reason: format!(
            "auto-promoted: {} samples over {age_ms}ms, shape settled",
            cand.sample_count
        ),
    }
}

/// Periodically scans every PROVISIONAL candidate and promotes each one that
/// is from a trusted source and clears `policy`, until `shutdown` is cancelled.
/// Wired by [`crate::serve::serve`] only when `[auto_promote].enabled` is
/// `true` (and its config validated). `allowed_sources` is the operator's
/// default-deny source allowlist; `max_per_tick` caps promotions per pass.
pub async fn run_auto_promote_sweep(
    promoter: Arc<dyn PromoterTrait>,
    evidence: Arc<dyn EvidenceStore>,
    policy: AutoPromotePolicy,
    allowed_sources: Arc<[String]>,
    max_per_tick: usize,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick so the sweep doesn't run twice
    // back-to-back at startup before the configured interval elapses.
    ticker.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("auto-promote sweep shutting down");
                return;
            }
            _ = ticker.tick() => {
                sweep_once(
                    promoter.as_ref(),
                    evidence.as_ref(),
                    &policy,
                    &allowed_sources,
                    max_per_tick,
                    &shutdown,
                )
                .await;
            }
        }
    }
}

/// One pass over PROVISIONAL candidates, paginating until `list_candidates`
/// reports no further cursor, at most `max_per_tick` promotions. Checks
/// `shutdown` between pages so a large backlog can't block graceful shutdown.
async fn sweep_once(
    promoter: &dyn PromoterTrait,
    evidence: &dyn EvidenceStore,
    policy: &AutoPromotePolicy,
    allowed_sources: &[String],
    max_per_tick: usize,
    shutdown: &CancellationToken,
) {
    let mut cursor: Option<String> = None;
    let mut promoted = 0usize;
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let page = evidence
            .list_candidates(CandidateState::Provisional, cursor.clone(), SWEEP_PAGE_SIZE)
            .await;
        let (records, next_cursor) = match page {
            Ok(page) => page,
            Err(err) => {
                tracing::warn!(error = %err, "auto-promote sweep: list_candidates failed, will retry next tick");
                return;
            }
        };

        let now_ms = crate::policy::now_epoch_ms();
        for record in &records {
            // Default-deny source gate first — cheapest, and the strongest
            // abuse control: only operator-named sources are auto-promotable.
            if !source_allowed(record.source.as_deref(), allowed_sources) {
                continue;
            }
            match policy.eligible(record, now_ms) {
                // Not yet eligible is the common, expected case — debug only.
                Err(reason) => tracing::debug!(
                    candidate_id = %record.candidate_id.as_str(),
                    reason = %reason,
                    "auto-promote: candidate not yet eligible"
                ),
                Ok(()) => {
                    let req = auto_promote_request(record, now_ms);
                    // `promote` re-fetches, re-verifies state==Provisional and
                    // the manual policy, publishes, and transitions state — so
                    // a candidate rejected or already published between this
                    // scan and here is handled there, not double-published.
                    match promoter
                        .promote(&record.candidate_id, req, AUTO_PROMOTE_ACTOR)
                        .await
                    {
                        Ok(schema) => {
                            tracing::info!(
                                candidate_id = %record.candidate_id.as_str(),
                                schema_id = %schema.schema_id.as_str(),
                                family_id = %schema.family_id.as_str(),
                                samples = record.sample_count,
                                "auto-promoted candidate to a new family"
                            );
                            promoted += 1;
                            if promoted >= max_per_tick {
                                tracing::info!(
                                    max_per_tick,
                                    "auto-promote: reached per-tick cap, remainder deferred to next tick"
                                );
                                return;
                            }
                        }
                        Err(err) => tracing::warn!(
                            candidate_id = %record.candidate_id.as_str(),
                            error = %err,
                            "auto-promote: promote failed, will retry next tick"
                        ),
                    }
                }
            }
        }

        match next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use deblob_core::error::CoreError;
    use deblob_core::id::{CandidateId as CandId, FamilyId, FamilyVersion, SchemaId};
    use deblob_core::ports::SchemaRecord;
    use deblob_fingerprint::{parse_bounded, Limits};
    use deblob_monoid::Profile as MonoidProfile;
    use std::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    fn settled_profile(json: &str) -> serde_json::Value {
        let node = parse_bounded(json.as_bytes(), &Limits::default()).unwrap();
        serde_json::to_value(MonoidProfile::from_node(&node)).unwrap()
    }

    fn cand(seed: u8, samples: u64, age_ms: i64, source: Option<&str>) -> CandidateRecord {
        CandidateRecord {
            candidate_id: CandId::from_digest(&[seed; 32]),
            state: CandidateState::Provisional,
            // Two always-present leaf fields => clears the shape guard.
            profile: settled_profile(r#"{"a":1,"b":2}"#),
            sample_count: samples,
            first_seen_ms: 0,
            last_seen_ms: age_ms,
            source: source.map(str::to_string),
        }
    }

    #[test]
    fn request_targets_new_family_and_records_evidence() {
        let c = cand(7, 60, 700_100, Some("events.ok"));
        let req = auto_promote_request(&c, c.last_seen_ms);
        assert!(matches!(req.family, FamilyChoice::New));
        assert!(req.name.is_none());
        assert!(req.reason.contains("60 samples"));
    }

    #[test]
    fn source_allowlist_is_default_deny() {
        let allowed = vec!["events.ok".to_string()];
        assert!(source_allowed(Some("events.ok"), &allowed));
        assert!(!source_allowed(Some("events.bad"), &allowed));
        assert!(!source_allowed(None, &allowed));
        // Empty allowlist denies everything (default-deny).
        assert!(!source_allowed(Some("events.ok"), &[]));
    }

    /// Records the candidate ids a sweep promoted.
    #[derive(Default)]
    struct RecordingPromoter {
        promoted: Mutex<Vec<String>>,
    }
    #[async_trait]
    impl PromoterTrait for RecordingPromoter {
        async fn promote(
            &self,
            cand: &CandId,
            _req: PromoteRequest,
            actor: &str,
        ) -> Result<SchemaRecord, CoreError> {
            assert_eq!(actor, AUTO_PROMOTE_ACTOR);
            self.promoted
                .lock()
                .unwrap()
                .push(cand.as_str().to_string());
            Ok(SchemaRecord {
                schema_id: SchemaId::from_digest(&[1u8; 32]),
                family_id: FamilyId::new_v7(),
                version: FamilyVersion(1),
                canonical: "{}".to_string(),
                canonicalizer: "test".to_string(),
                provenance: serde_json::json!({}),
                semantic: None,
                semantic_fingerprint: None,
                privacy_class: None,
                value_profile_ref: None,
                value_profile_summary: None,
            })
        }
    }

    /// Returns a fixed set of provisional candidates on the first page.
    struct PageEvidence(Vec<CandidateRecord>);
    #[async_trait]
    impl EvidenceStore for PageEvidence {
        async fn list_candidates(
            &self,
            _state: CandidateState,
            cursor: Option<String>,
            _limit: usize,
        ) -> Result<(Vec<CandidateRecord>, Option<String>), CoreError> {
            if cursor.is_some() {
                return Ok((vec![], None));
            }
            Ok((self.0.clone(), None))
        }
        async fn upsert_candidate(&self, _r: CandidateRecord) -> Result<(), CoreError> {
            unimplemented!()
        }
        async fn get_candidate(&self, _id: &CandId) -> Result<Option<CandidateRecord>, CoreError> {
            unimplemented!()
        }
        async fn append_evidence(
            &self,
            _id: &CandId,
            _s: serde_json::Value,
        ) -> Result<(), CoreError> {
            unimplemented!()
        }
        async fn set_state(&self, _id: &CandId, _s: CandidateState) -> Result<(), CoreError> {
            unimplemented!()
        }
        async fn get_cluster(&self, _g: &str) -> Result<Option<CandId>, CoreError> {
            unimplemented!()
        }
        async fn set_cluster(&self, _g: &str, _c: &CandId) -> Result<(), CoreError> {
            unimplemented!()
        }
        async fn add_variant(&self, _c: &CandId, _b: &str, _f: &str) -> Result<(), CoreError> {
            unimplemented!()
        }
        async fn get_variants(&self, _c: &CandId) -> Result<Vec<(String, String)>, CoreError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn sweep_promotes_only_eligible_allowed_source_candidates() {
        let allowed = vec!["events.ok".to_string()];
        let eligible = cand(1, 60, 700_000, Some("events.ok")); // promote
        let too_few = cand(2, 3, 700_000, Some("events.ok")); // ineligible
        let bad_source = cand(3, 60, 700_000, Some("events.bad")); // denied
        let no_source = cand(4, 60, 700_000, None); // denied
        let evidence = PageEvidence(vec![eligible, too_few, bad_source, no_source]);
        let promoter = RecordingPromoter::default();

        sweep_once(
            &promoter,
            &evidence,
            &AutoPromotePolicy::default(),
            &allowed,
            10,
            &CancellationToken::new(),
        )
        .await;

        let promoted = promoter.promoted.lock().unwrap().clone();
        assert_eq!(
            promoted,
            vec![CandId::from_digest(&[1u8; 32]).as_str().to_string()]
        );
    }

    #[tokio::test]
    async fn sweep_respects_per_tick_cap() {
        let allowed = vec!["events.ok".to_string()];
        let cands: Vec<_> = (10u8..15)
            .map(|s| cand(s, 60, 700_000, Some("events.ok")))
            .collect();
        let evidence = PageEvidence(cands);
        let promoter = RecordingPromoter::default();

        sweep_once(
            &promoter,
            &evidence,
            &AutoPromotePolicy::default(),
            &allowed,
            2, // cap
            &CancellationToken::new(),
        )
        .await;

        assert_eq!(promoter.promoted.lock().unwrap().len(), 2);
    }
}
