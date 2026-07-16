//! Kafka header hygiene (spec §3.1-3.2): strip every inbound `deblob-*`
//! header, case-insensitive, before a record is re-produced, then attach
//! exactly one canonical `deblob-schema-id` header and one `deblob-origin`
//! header. Kafka allows duplicate header keys on the wire — that is exactly
//! why this has to be "strip every reserved header, then write exactly
//! one": a downstream reader must never be able to see two conflicting
//! `deblob-schema-id` values, whether from a spoofing/replay attempt on the
//! raw topic or from a bug upstream.
//!
//! Header values carry IDs, origin coordinates, or a short bounded reason
//! code ONLY — never a schema body, a full parse-error message, or model
//! output (spec §3.2, §11's "no ... messages in labels" extends here too).

use deblob_core::envelope::SourceCursor;
use deblob_core::error::QuarantineReason;
use deblob_core::id::SchemaRef;
use rdkafka::message::{BorrowedHeaders, Header, Headers, OwnedHeaders};

/// The reserved header-key prefix every hot-path-owned header uses.
pub const RESERVED_PREFIX: &str = "deblob-";

/// The canonical schema-tag header key.
pub const SCHEMA_ID_HEADER: &str = "deblob-schema-id";
/// The canonical origin header key: `<topic>/<partition>/<offset>`.
pub const ORIGIN_HEADER: &str = "deblob-origin";
/// The canonical quarantine-reason header key (bounded reason code only).
pub const QUARANTINE_REASON_HEADER: &str = "deblob-quarantine-reason";

/// True if `key` starts with `deblob-`, case-insensitive. This is a
/// PREFIX match, not an allowlist of specific known keys — it therefore
/// already covers every reserved sub-namespace by construction, including
/// P2-D Task 6's `deblob-semantic-*` (a producer-supplied semantic-axis
/// hint, e.g. a forged `deblob-semantic-unit` header, must never reach
/// storage or influence the `sem_` governance API): no separate constant or
/// check was needed to "extend" the strip to that namespace, since any
/// `deblob-`-prefixed key was already reserved before Task 6 existed. See
/// `strip_reserved_drops_deblob_semantic_hint_headers` below for the proof.
pub fn is_reserved(key: &str) -> bool {
    key.len() >= RESERVED_PREFIX.len()
        && key.as_bytes()[..RESERVED_PREFIX.len()].eq_ignore_ascii_case(RESERVED_PREFIX.as_bytes())
}

/// Copies every NON-reserved header from `headers` (in order) into a fresh
/// [`OwnedHeaders`], dropping every header whose key starts with `deblob-`
/// (case-insensitive). Kafka allows duplicate header keys, so this drops
/// ALL matches, not just the first — the spoofing defense spec §3.1
/// requires ("strip ALL inbound `deblob-*` headers").
pub fn strip_reserved(headers: Option<&BorrowedHeaders>) -> OwnedHeaders {
    let mut out = OwnedHeaders::new();
    if let Some(headers) = headers {
        for header in headers.iter() {
            if is_reserved(header.key) {
                continue;
            }
            out = out.insert(Header {
                key: header.key,
                value: header.value,
            });
        }
    }
    out
}

/// The `deblob-origin` header value: `<topic>/<partition>/<offset>` — the
/// source record's own coordinates, verbatim. Never re-derived from
/// anything but `cursor`, so replaying the same source offset through a
/// fresh relay always reproduces the identical string (spec §3.2 replay
/// determinism: "never mint fresh cand_/UUID during replay").
pub fn origin_value(cursor: &SourceCursor) -> String {
    format!("{}/{}/{}", cursor.topic, cursor.partition, cursor.offset)
}

/// The short, bounded quarantine-reason label — reused verbatim as the
/// `deblob-quarantine-reason` header value. NEVER the full parse-error
/// message or any payload fragment.
pub fn quarantine_reason_value(reason: QuarantineReason) -> &'static str {
    match reason {
        QuarantineReason::DuplicateKey => "duplicate_key",
        QuarantineReason::NonFiniteNumber => "non_finite_number",
        QuarantineReason::DepthExceeded => "depth_exceeded",
        QuarantineReason::SizeExceeded => "size_exceeded",
        QuarantineReason::FieldCountExceeded => "field_count_exceeded",
        QuarantineReason::KeyLengthExceeded => "key_length_exceeded",
        QuarantineReason::ParseError => "parse_error",
        QuarantineReason::Utf8Error => "utf8_error",
    }
}

/// Appends exactly one `deblob-schema-id` header (from `schema_ref`'s
/// canonical header value, spec §5's `SchemaRef::header_value`) and one
/// `deblob-origin` header (from `cursor`) onto `headers`.
///
/// Callers MUST have already run the inbound headers through
/// [`strip_reserved`] — this function only ever appends, it never removes,
/// so calling it on un-stripped headers would produce duplicate
/// `deblob-schema-id`/`deblob-origin` entries.
pub fn with_tag(
    headers: OwnedHeaders,
    schema_ref: &SchemaRef,
    cursor: &SourceCursor,
) -> OwnedHeaders {
    let schema_id_value = schema_ref.header_value();
    let origin = origin_value(cursor);
    headers
        .insert(Header {
            key: SCHEMA_ID_HEADER,
            value: Some(schema_id_value.as_bytes()),
        })
        .insert(Header {
            key: ORIGIN_HEADER,
            value: Some(origin.as_bytes()),
        })
}

/// Appends a `deblob-quarantine-reason` header carrying only the bounded
/// reason code — never the underlying parse-error text or payload.
pub fn with_quarantine_reason(headers: OwnedHeaders, reason: QuarantineReason) -> OwnedHeaders {
    headers.insert(Header {
        key: QUARANTINE_REASON_HEADER,
        value: Some(quarantine_reason_value(reason).as_bytes()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use deblob_core::id::{CandidateId, SchemaId};

    fn owned(pairs: &[(&str, &[u8])]) -> OwnedHeaders {
        let mut h = OwnedHeaders::new();
        for (k, v) in pairs {
            h = h.insert(Header {
                key: k,
                value: Some(*v),
            });
        }
        h
    }

    fn keys_of(h: &OwnedHeaders) -> Vec<String> {
        h.iter().map(|hdr| hdr.key.to_string()).collect()
    }

    #[test]
    fn is_reserved_matches_case_insensitively() {
        assert!(is_reserved("deblob-schema-id"));
        assert!(is_reserved("DEBLOB-SCHEMA-ID"));
        assert!(is_reserved("Deblob-Origin"));
        assert!(!is_reserved("deblo"));
        assert!(!is_reserved("content-type"));
        assert!(!is_reserved(""));
    }

    #[test]
    fn strip_reserved_drops_every_deblob_prefixed_header_case_insensitively() {
        let inbound = owned(&[
            ("deblob-schema-id", b"cand_spoofed"),
            ("DEBLOB-ORIGIN", b"evil/0/0"),
            ("content-type", b"application/json"),
            ("deblob-quarantine-reason", b"spoofed"),
            ("x-trace-id", b"abc123"),
        ]);

        let stripped = strip_reserved(Some(inbound.as_borrowed()));
        let keys = keys_of(&stripped);

        assert_eq!(
            keys,
            vec!["content-type".to_string(), "x-trace-id".to_string()]
        );
    }

    #[test]
    fn strip_reserved_drops_duplicate_reserved_headers() {
        // Kafka allows duplicate header keys on the wire — a spoofing
        // attempt could send TWO deblob-schema-id headers. Both must go.
        let inbound = owned(&[
            ("deblob-schema-id", b"cand_a"),
            ("deblob-schema-id", b"cand_b"),
            ("keep-me", b"1"),
        ]);

        let stripped = strip_reserved(Some(inbound.as_borrowed()));
        assert_eq!(keys_of(&stripped), vec!["keep-me".to_string()]);
    }

    #[test]
    fn strip_reserved_of_none_is_empty() {
        let stripped = strip_reserved(None);
        assert_eq!(stripped.count(), 0);
    }

    /// P2-D Task 6: a producer-supplied `deblob-semantic-*` hint (spoofing
    /// an axis of the semantic-governance API, e.g. trying to smuggle a
    /// forged `unit`/`canonical_field_id` in via the ingest path rather
    /// than the authenticated `PUT .../semantic` endpoint) must be stripped
    /// before a record is re-produced, exactly like every other reserved
    /// header — it must never reach storage.
    #[test]
    fn strip_reserved_drops_deblob_semantic_hint_headers() {
        let inbound = owned(&[
            ("deblob-semantic-unit", b"Cel"),
            ("DEBLOB-SEMANTIC-CANONICAL-FIELD-ID", b"temperature.ambient"),
            ("deblob-semantic-event-type", b"user.created"),
            ("content-type", b"application/json"),
        ]);

        let stripped = strip_reserved(Some(inbound.as_borrowed()));

        assert_eq!(keys_of(&stripped), vec!["content-type".to_string()]);
    }

    #[test]
    fn with_tag_writes_exactly_one_schema_id_and_origin_header() {
        let base = owned(&[("content-type", b"application/json")]);
        let cursor = SourceCursor {
            topic: "raw".to_string(),
            partition: 3,
            offset: 42,
        };
        let schema_ref = SchemaRef::Known(SchemaId::from_digest(&[7u8; 32]));

        let tagged = with_tag(base, &schema_ref, &cursor);

        assert_eq!(
            keys_of(&tagged),
            vec![
                "content-type".to_string(),
                SCHEMA_ID_HEADER.to_string(),
                ORIGIN_HEADER.to_string(),
            ]
        );
        let schema_id_header = tagged
            .iter()
            .find(|h| h.key == SCHEMA_ID_HEADER)
            .expect("schema id header present");
        assert_eq!(
            schema_id_header.value.unwrap(),
            schema_ref.header_value().as_bytes()
        );
        let origin_header = tagged
            .iter()
            .find(|h| h.key == ORIGIN_HEADER)
            .expect("origin header present");
        assert_eq!(origin_header.value.unwrap(), b"raw/3/42");
    }

    #[test]
    fn with_tag_is_deterministic_across_calls() {
        // Replay determinism at the header-construction level: same
        // schema_ref + same cursor => byte-identical headers, every time.
        let cursor = SourceCursor {
            topic: "raw".to_string(),
            partition: 1,
            offset: 100,
        };
        let schema_ref = SchemaRef::Provisional(CandidateId::from_digest(&[9u8; 32]));

        let first = with_tag(OwnedHeaders::new(), &schema_ref, &cursor);
        let second = with_tag(OwnedHeaders::new(), &schema_ref, &cursor);

        let first_pairs: Vec<(String, Vec<u8>)> = first
            .iter()
            .map(|h| (h.key.to_string(), h.value.unwrap().to_vec()))
            .collect();
        let second_pairs: Vec<(String, Vec<u8>)> = second
            .iter()
            .map(|h| (h.key.to_string(), h.value.unwrap().to_vec()))
            .collect();
        assert_eq!(first_pairs, second_pairs);
    }

    #[test]
    fn with_quarantine_reason_writes_bounded_code_only() {
        let tagged = with_quarantine_reason(OwnedHeaders::new(), QuarantineReason::DuplicateKey);
        let header = tagged
            .iter()
            .find(|h| h.key == QUARANTINE_REASON_HEADER)
            .expect("quarantine reason header present");
        assert_eq!(header.value.unwrap(), b"duplicate_key");
    }
}
