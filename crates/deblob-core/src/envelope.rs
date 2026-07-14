//! Envelope and tagging types. Spec §4.

use crate::id::SchemaRef;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SourceCursor {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub source: String,
    pub cursor: SourceCursor,
    pub producer: Option<String>,
    pub event_time_ms: Option<i64>,
    pub content_type: String,
    pub payload: bytes::Bytes,
    pub is_tombstone: bool,
}

#[derive(Debug, Clone)]
pub struct Tagged {
    pub envelope: Envelope,
    pub schema_ref: SchemaRef,
}

#[cfg(test)]
mod tests {
    #[test]
    fn quarantine_reason_snake_case() {
        let r = crate::error::QuarantineReason::DuplicateKey;
        assert_eq!(serde_json::to_string(&r).unwrap(), "\"duplicate_key\"");
    }
}
