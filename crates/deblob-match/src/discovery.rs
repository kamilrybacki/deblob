//! The discovery-topic wire type (spec §3.1-3.2, §4): the transport
//! envelope `deblob-kafka::Relay::run` produces for every `Provisional`
//! classification, and `deblob::coldlane::ColdLane` is the eventual
//! consumer-side counterpart of. Lives in `deblob-match` (rather than
//! `deblob::coldlane`, where it originated) so `deblob-kafka` can depend on
//! it without depending on the `deblob` package itself — see this crate's
//! `lib.rs` docs for why that split exists.

use bytes::Bytes;
use deblob_core::envelope::SourceCursor;
use serde::{Deserialize, Serialize};

/// One message forwarded to the discovery topic for cold-lane processing.
/// Carries the RAW payload bytes — unlike the stats-only evidence
/// `ColdLane::ingest` appends to the `EvidenceStore`, this is the transport
/// envelope between the hot path and the cold-lane consumer, not a
/// permanent record (spec §9 governs what gets *persisted*, not what's in
/// flight on the discovery topic).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMsg {
    pub cand_id: String,
    pub payload: Bytes,
    pub source: String,
    pub cursor: SourceCursor,
}
