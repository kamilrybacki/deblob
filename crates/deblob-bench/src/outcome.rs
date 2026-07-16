//! Classifies a `deblob-schema-id` header value into the coarse tag
//! outcome the bench report cares about (spec §5: "match/candidate/
//! unresolved/quarantine rates"). Deliberately does NOT depend on
//! `deblob-core::id::SchemaRef` — `deblob-bench` treats Deblob as a black
//! box observed purely through the Kafka wire contract (spec §3.1's
//! producer/measurer split), the same way a real operator's monitoring
//! would. The four literal/`sch_`/`cand_` shapes classified here match
//! `SchemaRef::header_value` byte-for-byte (`crates/deblob-core/src/
//! id.rs`), verified against the real header producer in
//! `deblob_kafka::headers`'s own tests.

/// The header key this module classifies. Read-only concern here (the
/// relay owns writing it) — kept alongside the classifier so every
/// `deblob-schema-id` consumer in this crate (`crate::measurer`) shares one
/// definition.
pub const SCHEMA_ID_HEADER: &str = "deblob-schema-id";

/// The coarse fate a tagged (or quarantined) record was assigned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TagOutcome {
    /// `sch_...` — resolved to a published schema.
    Known,
    /// `cand_...` — resolved to a provisional (not-yet-promoted) candidate.
    Provisional,
    /// The literal `unresolved` — a registry-outage degrade (spec §10:
    /// "never mint a fresh cand_ during an outage").
    Unresolved,
    /// The literal `malformed` — routed to quarantine, never the hot path.
    Malformed,
    /// The literal `tombstone` — a Kafka null-value record, passed through
    /// untouched.
    Tombstone,
    /// Any other header value: a future Deblob tag shape this bench build
    /// predates, or genuinely malformed telemetry. Never panics on it.
    Unknown,
}

/// Classifies one `deblob-schema-id` header value. Pure — no I/O, so it is
/// unit-testable independent of any Kafka header type.
pub fn classify(schema_id_header_value: &str) -> TagOutcome {
    if schema_id_header_value.starts_with("sch_") {
        TagOutcome::Known
    } else if schema_id_header_value.starts_with("cand_") {
        TagOutcome::Provisional
    } else {
        match schema_id_header_value {
            "unresolved" => TagOutcome::Unresolved,
            "malformed" => TagOutcome::Malformed,
            "tombstone" => TagOutcome::Tombstone,
            _ => TagOutcome::Unknown,
        }
    }
}

/// Running per-scenario tally of every [`TagOutcome`] observed by the
/// measurer, folded straight into the JSON report (`crate::report`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct TagOutcomeCounts {
    pub known: u64,
    pub provisional: u64,
    pub unresolved: u64,
    pub malformed: u64,
    pub tombstone: u64,
    pub unknown: u64,
}

impl TagOutcomeCounts {
    /// Increments the counter matching `outcome`.
    pub fn record(&mut self, outcome: TagOutcome) {
        match outcome {
            TagOutcome::Known => self.known += 1,
            TagOutcome::Provisional => self.provisional += 1,
            TagOutcome::Unresolved => self.unresolved += 1,
            TagOutcome::Malformed => self.malformed += 1,
            TagOutcome::Tombstone => self.tombstone += 1,
            TagOutcome::Unknown => self.unknown += 1,
        }
    }

    /// The sum of every bucket — total classified records observed.
    pub fn total(&self) -> u64 {
        self.known
            + self.provisional
            + self.unresolved
            + self.malformed
            + self.tombstone
            + self.unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_schema_ids() {
        assert_eq!(
            classify("sch_01hz8k9c9x9x9x9x9x9x9x9x9x"),
            TagOutcome::Known
        );
        assert_eq!(classify("sch_"), TagOutcome::Known);
    }

    #[test]
    fn classifies_provisional_candidate_ids() {
        assert_eq!(
            classify("cand_01hz8k9c9x9x9x9x9x9x9x9x9x"),
            TagOutcome::Provisional
        );
        assert_eq!(classify("cand_"), TagOutcome::Provisional);
    }

    #[test]
    fn classifies_the_three_literal_reserved_values() {
        assert_eq!(classify("unresolved"), TagOutcome::Unresolved);
        assert_eq!(classify("malformed"), TagOutcome::Malformed);
        assert_eq!(classify("tombstone"), TagOutcome::Tombstone);
    }

    #[test]
    fn classifies_anything_else_as_unknown() {
        assert_eq!(classify(""), TagOutcome::Unknown);
        assert_eq!(classify("garbage"), TagOutcome::Unknown);
        assert_eq!(
            classify("Sch_uppercase_prefix_not_matched"),
            TagOutcome::Unknown
        );
    }

    #[test]
    fn counts_accumulate_per_bucket_independently() {
        let mut counts = TagOutcomeCounts::default();
        counts.record(TagOutcome::Known);
        counts.record(TagOutcome::Known);
        counts.record(TagOutcome::Provisional);
        counts.record(TagOutcome::Unresolved);
        counts.record(TagOutcome::Malformed);
        counts.record(TagOutcome::Tombstone);
        counts.record(TagOutcome::Unknown);

        assert_eq!(
            counts,
            TagOutcomeCounts {
                known: 2,
                provisional: 1,
                unresolved: 1,
                malformed: 1,
                tombstone: 1,
                unknown: 1,
            }
        );
        assert_eq!(counts.total(), 7);
    }
}
