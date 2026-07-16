//! The `bench-produce-ns` Kafka header: stamps each record this harness
//! produces with a monotonic wall-clock produce timestamp (nanoseconds
//! since `UNIX_EPOCH`) that survives the relay unchanged. Deblob's relay
//! strips every inbound header whose key starts with `deblob-`
//! (case-insensitive — see `deblob_kafka::headers::is_reserved`) before
//! re-producing onto the tagged topic, but leaves every OTHER header alone
//! — `bench-*` is not `deblob-*`, so it rides through untouched. The
//! measurer (`crate::measurer`) reads this header back off the tagged
//! topic to compute end-to-end latency without needing a shared clock or a
//! side-channel id map.

/// The header key. Deliberately `bench-` prefixed, never `deblob-` (spec
/// §3.1 brief: "use a `bench-*` header, which is NOT stripped").
pub const PRODUCE_NS_HEADER: &str = "bench-produce-ns";

/// Encodes `ns` (nanoseconds since `UNIX_EPOCH`) as the big-endian 8-byte
/// header value [`decode_produce_ns`] expects back.
pub fn encode_produce_ns(ns: u64) -> [u8; 8] {
    ns.to_be_bytes()
}

/// Decodes a `bench-produce-ns` header value back into nanoseconds.
/// `None` if `bytes` isn't exactly 8 bytes — a foreign or corrupted header
/// must never panic the measurer, just be treated as "no timestamp".
pub fn decode_produce_ns(bytes: &[u8]) -> Option<u64> {
    let array: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_be_bytes(array))
}

/// Current wall-clock time as nanoseconds since `UNIX_EPOCH`. Saturates to
/// `0` rather than panicking if the system clock is somehow set before the
/// epoch (a real, if rare, possibility this harness should never crash
/// over).
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_then_decode_round_trips() {
        for ns in [0u64, 1, 42, 1_753_660_800_000_000_000, u64::MAX] {
            let bytes = encode_produce_ns(ns);
            assert_eq!(
                decode_produce_ns(&bytes),
                Some(ns),
                "round trip failed for {ns}"
            );
        }
    }

    #[test]
    fn decode_rejects_wrong_length_bytes() {
        assert_eq!(decode_produce_ns(&[]), None);
        assert_eq!(decode_produce_ns(&[1, 2, 3]), None);
        assert_eq!(decode_produce_ns(&[0u8; 9]), None);
    }

    #[test]
    fn encode_is_big_endian_so_byte_order_is_stable_on_the_wire() {
        let bytes = encode_produce_ns(1);
        assert_eq!(bytes, [0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn now_ns_is_a_plausible_recent_timestamp() {
        // Sanity, not a strict correctness proof: must be well after the
        // Unix epoch and not absurdly far in the future (catches an
        // accidental seconds-vs-nanoseconds unit bug).
        let ns = now_ns();
        let year_2020_ns: u64 = 1_577_836_800_000_000_000;
        let year_2100_ns: u64 = 4_102_444_800_000_000_000;
        assert!(ns > year_2020_ns);
        assert!(ns < year_2100_ns);
    }
}
