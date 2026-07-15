//! HTTP header hygiene (spec §3.1, §4): strip every inbound `deblob-*`
//! header plus the hop-by-hop set, case-insensitive, before a request (or
//! response) is forwarded, then attach exactly one canonical
//! `deblob-schema-id` header and one `deblob-origin` header. Mirrors
//! `deblob-kafka::headers`' "strip every reserved header, then write
//! exactly one" pattern for the HTTP transport (spec §3.2's reuse note) —
//! a producer can never spoof its own tag by sending a `deblob-schema-id`
//! header, because [`strip_reserved_and_hop_by_hop`] always runs before
//! [`with_tag`] ever gets called.
//!
//! Header values carry IDs or an origin string ONLY — never a schema body
//! or payload fragment (spec §3.2).

use deblob_core::error::QuarantineReason;
use deblob_core::id::SchemaRef;
use http::header::{HeaderMap, HeaderName, HeaderValue};

/// The reserved header-key prefix every hot-path-owned header uses.
pub const RESERVED_PREFIX: &str = "deblob-";

/// The canonical schema-tag header key.
pub const SCHEMA_ID_HEADER: &str = "deblob-schema-id";
/// The canonical origin header key.
pub const ORIGIN_HEADER: &str = "deblob-origin";
/// The canonical quarantine-reason header key (bounded reason code only;
/// written by a future task's malformed-body handling).
pub const QUARANTINE_REASON_HEADER: &str = "deblob-quarantine-reason";

/// Hop-by-hop headers stripped before forwarding in either direction
/// (RFC 7230 §6.1, plus the `Proxy-*` request-header set spec §4 calls
/// out by name).
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
];

/// True if `name` starts with `deblob-`, case-insensitive.
pub fn is_reserved(name: &str) -> bool {
    name.len() >= RESERVED_PREFIX.len()
        && name.as_bytes()[..RESERVED_PREFIX.len()].eq_ignore_ascii_case(RESERVED_PREFIX.as_bytes())
}

/// True if `name` is one of the hop-by-hop headers in [`HOP_BY_HOP`],
/// case-insensitive.
fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h))
}

/// Copies every header from `headers` that is NEITHER reserved
/// (`deblob-*`, case-insensitive) NOR hop-by-hop into a fresh
/// [`HeaderMap`], preserving order and multi-value headers. `HeaderMap`
/// already normalizes names to lower-case internally, so `name.as_str()`
/// is already the value the case-insensitive checks above compare
/// against.
pub fn strip_reserved_and_hop_by_hop(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        let key = name.as_str();
        if is_reserved(key) || is_hop_by_hop(key) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

/// Writes exactly one `deblob-schema-id` header (from `schema_ref`'s
/// canonical header value) and one `deblob-origin` header (`origin`,
/// verbatim) onto `headers`, in place.
///
/// Uses `insert` (not `append`) so this is safe to call even if a caller
/// forgets to run inbound headers through
/// [`strip_reserved_and_hop_by_hop`] first — a stray inbound
/// `deblob-schema-id` would be overwritten, never left alongside the
/// canonical one. Callers should still always strip first: `insert` only
/// protects the two header keys this function itself writes, not every
/// other `deblob-*` header a spoofing attempt might send.
pub fn with_tag(headers: &mut HeaderMap, schema_ref: &SchemaRef, origin: &str) {
    let schema_id_value = schema_ref.header_value();
    headers.insert(
        HeaderName::from_static(SCHEMA_ID_HEADER),
        HeaderValue::from_str(&schema_id_value)
            .expect("SchemaRef::header_value is always ASCII-safe"),
    );
    headers.insert(
        HeaderName::from_static(ORIGIN_HEADER),
        HeaderValue::from_str(origin)
            .unwrap_or_else(|_| HeaderValue::from_static("invalid-origin")),
    );
}

/// The short, bounded quarantine-reason label — reused verbatim as the
/// `deblob-quarantine-reason` header value (spec §6). NEVER the full
/// parse-error message or any payload fragment. Mirrors
/// `deblob-kafka::headers::quarantine_reason_value` exactly, since both
/// transports quarantine against the same `QuarantineReason` enum (spec
/// §3.2 reuse).
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

/// Writes exactly one `deblob-quarantine-reason` header (the bounded
/// reason code from [`quarantine_reason_value`]) onto `headers`, in
/// place. Uses `insert`, matching [`with_tag`]'s overwrite-not-duplicate
/// contract.
pub fn with_quarantine_reason(headers: &mut HeaderMap, reason: QuarantineReason) {
    headers.insert(
        HeaderName::from_static(QUARANTINE_REASON_HEADER),
        HeaderValue::from_static(quarantine_reason_value(reason)),
    );
}

#[cfg(test)]
mod tests {
    use deblob_core::id::{CandidateId, SchemaId};

    use super::*;

    fn keys_of(h: &HeaderMap) -> Vec<String> {
        h.keys().map(|k| k.as_str().to_string()).collect()
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

    /// The exact hardening scenario spec §4 requires: a forged
    /// `Deblob-Schema-Id`, an unrelated `deblob-*` header, and every
    /// hop-by-hop header must ALL be dropped, while a normal header
    /// (`content-type`) survives untouched.
    #[test]
    fn strips_reserved_and_hop_by_hop() {
        let mut inbound = HeaderMap::new();
        inbound.insert("deblob-schema-id", HeaderValue::from_static("forged"));
        inbound.insert("deblob-anything", HeaderValue::from_static("x"));
        inbound.insert("connection", HeaderValue::from_static("keep-alive"));
        inbound.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        inbound.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        inbound.insert("te", HeaderValue::from_static("trailers"));
        inbound.insert("trailer", HeaderValue::from_static("x-checksum"));
        inbound.insert("upgrade", HeaderValue::from_static("h2c"));
        inbound.insert("proxy-connection", HeaderValue::from_static("keep-alive"));
        inbound.insert("proxy-authenticate", HeaderValue::from_static("Basic"));
        inbound.insert("proxy-authorization", HeaderValue::from_static("Basic abc"));
        inbound.insert("content-type", HeaderValue::from_static("application/json"));

        let stripped = strip_reserved_and_hop_by_hop(&inbound);

        assert_eq!(keys_of(&stripped), vec!["content-type".to_string()]);

        // Then `with_tag` writes exactly one `deblob-schema-id`.
        let mut tagged = stripped;
        let schema_ref = SchemaRef::Known(SchemaId::from_digest(&[7u8; 32]));
        with_tag(&mut tagged, &schema_ref, "http/test/1");

        let schema_id_headers: Vec<_> = tagged.get_all(SCHEMA_ID_HEADER).iter().collect();
        assert_eq!(schema_id_headers.len(), 1);
        assert_eq!(
            schema_id_headers[0].to_str().unwrap(),
            schema_ref.header_value()
        );
        let origin_headers: Vec<_> = tagged.get_all(ORIGIN_HEADER).iter().collect();
        assert_eq!(origin_headers.len(), 1);
        assert_eq!(origin_headers[0].to_str().unwrap(), "http/test/1");
    }

    #[test]
    fn strip_reserved_and_hop_by_hop_of_empty_map_is_empty() {
        let stripped = strip_reserved_and_hop_by_hop(&HeaderMap::new());
        assert!(stripped.is_empty());
    }

    #[test]
    fn with_tag_overwrites_rather_than_duplicates_when_called_on_unstripped_headers() {
        // Defense in depth: even if a caller forgot to strip first,
        // `with_tag`'s use of `insert` (not `append`) means the forged
        // value never survives alongside the canonical one.
        let mut headers = HeaderMap::new();
        headers.insert(SCHEMA_ID_HEADER, HeaderValue::from_static("cand_forged"));

        let schema_ref = SchemaRef::Provisional(CandidateId::from_digest(&[9u8; 32]));
        with_tag(&mut headers, &schema_ref, "http/test/2");

        let values: Vec<_> = headers.get_all(SCHEMA_ID_HEADER).iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_str().unwrap(), schema_ref.header_value());
    }

    #[test]
    fn with_quarantine_reason_writes_bounded_code_only() {
        let mut headers = HeaderMap::new();
        with_quarantine_reason(&mut headers, QuarantineReason::DuplicateKey);

        let values: Vec<_> = headers.get_all(QUARANTINE_REASON_HEADER).iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_str().unwrap(), "duplicate_key");
    }

    #[test]
    fn with_quarantine_reason_overwrites_rather_than_duplicates() {
        let mut headers = HeaderMap::new();
        with_quarantine_reason(&mut headers, QuarantineReason::ParseError);
        with_quarantine_reason(&mut headers, QuarantineReason::SizeExceeded);

        let values: Vec<_> = headers.get_all(QUARANTINE_REASON_HEADER).iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_str().unwrap(), "size_exceeded");
    }

    #[test]
    fn with_tag_is_deterministic_across_calls() {
        let schema_ref = SchemaRef::Unresolved;
        let mut first = HeaderMap::new();
        with_tag(&mut first, &schema_ref, "http/test/3");
        let mut second = HeaderMap::new();
        with_tag(&mut second, &schema_ref, "http/test/3");

        assert_eq!(
            first.get(SCHEMA_ID_HEADER).unwrap(),
            second.get(SCHEMA_ID_HEADER).unwrap()
        );
        assert_eq!(
            first.get(ORIGIN_HEADER).unwrap(),
            second.get(ORIGIN_HEADER).unwrap()
        );
    }
}
