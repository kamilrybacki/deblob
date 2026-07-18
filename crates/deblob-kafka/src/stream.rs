//! The live-stream tap (Stage L1): a payload-free, best-effort broadcast of
//! per-record hot-path outcomes, fed by [`crate::relay::Relay::run`] and
//! consumed by the management API's `GET /api/v1/stream` SSE endpoint (spec
//! `deblob` crate `api::stream`).
//!
//! Deliberately carries NO payload bytes and no raw field names/values —
//! only bounded, derived identifiers and counts (spec §3.2/§11's "no
//! payload/message text in observability surfaces" posture extends here).
//! Delivery is best-effort: [`crate::relay::RelayCfg::stream_tx`] is
//! `Option`-al (`None` in every existing call site/test, so the tap is
//! opt-in and every pre-existing behavior is unchanged), and the relay only
//! ever `try_send`s — a full broadcast channel or zero subscribers simply
//! drops the event, exactly like a lossy multicast tap. The exactly-once
//! transactional relay's correctness NEVER depends on this succeeding.

use deblob_core::envelope::SourceCursor;
use serde::Serialize;

/// One hot-path outcome, emitted after `deblob-kafka::relay` has decided a
/// record's classification (and, for a tombstone, after the tombstone
/// pass-through itself) — never before, so `outcome`/`schema_ref` always
/// reflect the SAME decision the record was actually tagged/routed with.
#[derive(Debug, Clone, Serialize)]
pub struct StreamEvent {
    /// Wall-clock milliseconds since the Unix epoch, captured when the
    /// event was built (not the source record's own event time, which the
    /// relay never parses out of the payload).
    pub ts_ms: i64,
    /// Always `"hot"` (Stage L1) — reserved so a future cold-lane tap can
    /// share this same envelope shape, distinguished by this field.
    pub lane: &'static str,
    /// The consumed record's own coordinates — the SAME `(topic, partition,
    /// offset)` `deblob-origin` header carries, never re-derived.
    pub origin: SourceCursor,
    pub outcome: StreamOutcome,
    /// [`deblob_core::id::SchemaRef::header_value`]'s output: a `sch_`/
    /// `cand_` id, or the bounded literal `"unresolved"`/`"malformed"`/
    /// `"tombstone"` — never a payload fragment.
    pub schema_ref: String,
    /// Not populated at Stage L1: the hot path resolves a `SchemaId`, never
    /// a `FamilyId` (that mapping lives in the registry's family record,
    /// spec §6) — fetching it here would mean an extra synchronous Redis
    /// round trip on the hot path purely for observability, which this
    /// tap must never add. `None` until a later stage threads it through.
    pub family_id: Option<String>,
    /// Set only for `Quarantined` — the SAME bounded
    /// `deblob-quarantine-reason` header code, never the underlying
    /// parse-error text or a payload fragment.
    pub reason: Option<String>,
    /// Top-level field count of the parsed payload (`0` for a `Malformed`
    /// or tombstone record, where no shape was parsed) — bounded, never a
    /// field name or value.
    pub fields_count: u32,
    /// The record's actual source topic (Hermes review gap 1: same fix as
    /// `deblob_match::discovery::DiscoveryMsg::source` — `cursor.topic`,
    /// never a static config value).
    pub source: Option<String>,
}

/// The four outcomes the live-stream tap distinguishes (Stage L1).
///
/// `NewCandidate` is used for EVERY `Provisional` classification: the hot
/// path mints a candidate id purely from the raw shape digest without
/// querying candidate-existence state (that determination is the cold
/// lane's/`EvidenceStore`'s job, downstream via the discovery-topic
/// consumer) — distinguishing "genuinely brand new" from "already
/// accumulating evidence" here would require an extra synchronous lookup on
/// the hot path, which this tap must never add (spec §3.1: the hot path is
/// deterministic-only, never waits on anything beyond the LRU/registry
/// lookup it already performs). `MatchedCandidate` is reserved for a later
/// stage that threads cold-lane state into this decision — a documented,
/// deliberate deferral, the same posture as source-scoped clustering being
/// deferred past this stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamOutcome {
    Tagged,
    NewCandidate,
    MatchedCandidate,
    Quarantined,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_outcome_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&StreamOutcome::Tagged).unwrap(),
            "\"tagged\""
        );
        assert_eq!(
            serde_json::to_string(&StreamOutcome::NewCandidate).unwrap(),
            "\"new_candidate\""
        );
        assert_eq!(
            serde_json::to_string(&StreamOutcome::MatchedCandidate).unwrap(),
            "\"matched_candidate\""
        );
        assert_eq!(
            serde_json::to_string(&StreamOutcome::Quarantined).unwrap(),
            "\"quarantined\""
        );
    }

    #[test]
    fn stream_event_serializes_without_payload_fields() {
        let ev = StreamEvent {
            ts_ms: 1_700_000_000_000,
            lane: "hot",
            origin: SourceCursor {
                topic: "events.raw".to_string(),
                partition: 0,
                offset: 42,
            },
            outcome: StreamOutcome::Tagged,
            schema_ref: "sch_aaaa".to_string(),
            family_id: None,
            reason: None,
            fields_count: 3,
            source: Some("events.raw".to_string()),
        };
        let rendered = serde_json::to_value(&ev).unwrap();
        assert_eq!(rendered["outcome"], "tagged");
        assert_eq!(rendered["origin"]["topic"], "events.raw");
        assert_eq!(rendered["fields_count"], 3);
        // No `payload`/`value`/`body` key ever leaves this type.
        assert!(rendered.get("payload").is_none());
    }
}
