#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    let _ = deblob_fingerprint::parse_bounded(data, &deblob_fingerprint::Limits::default());
    if let Ok(node) = deblob_fingerprint::parse_bounded(data, &deblob_fingerprint::Limits::default()) {
        let shape = deblob_fingerprint::shape_of(&node);
        let _ = deblob_fingerprint::fingerprint(&shape);
    }
});
