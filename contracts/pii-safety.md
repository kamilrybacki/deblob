# PII safety

Deblob never persists raw payloads, and no captured sample may carry raw payload
bytes or PII — this holds at the point a sample is first captured and again when
it is archived into the promotion evidence set or the training corpus. A sample
that would carry either must be dropped, not captured.

```contract
case SampleCaptureIsPiiSafe
entity:
  Sample
operation:
  capture
requires:
  sample.contains_raw_payload == false
  sample.contains_pii == false
ensures:
  result.status == Captured
```

```contract
case SampleArchiveIsPiiSafe
entity:
  Sample
operation:
  archive
requires:
  sample.contains_raw_payload == false
  sample.contains_pii == false
ensures:
  result.status == Archived
```
