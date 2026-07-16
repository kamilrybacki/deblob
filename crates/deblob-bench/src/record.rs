//! The generator's output type: a JSON payload plus the label the bench
//! expects Deblob to assign it, so downstream measurement can check
//! observed-vs-expected without re-deriving the label from the bytes.

/// What the generator expects Deblob to do with a [`GeneratedRecord`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// A structurally valid record belonging to schema family
    /// `schema_family` (an index into the generator's family pool),
    /// carrying only known fields for that family (subject to
    /// `optional_field_churn`).
    WellFormed { schema_family: usize },
    /// Invalid JSON (duplicate key, `NaN` literal, or truncated body).
    /// Expected to hit Deblob's quarantine path, never the hot path.
    Malformed,
    /// A structurally valid record belonging to `schema_family` but
    /// carrying a compatible drift beyond its known-optional set (a novel
    /// added field, or a widened type).
    Drifted { schema_family: usize },
}

/// One generated (or fixture-sourced) stream element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedRecord {
    /// The JSON payload, exactly as it would be produced onto the ingest
    /// topic.
    pub bytes: Vec<u8>,
    /// The label this record was constructed to satisfy.
    pub expected: RecordKind,
}
