# Deblob P1 — progress ledger

Branch: p1-deterministic-core
Plan: docs/superpowers/plans/2026-07-14-deblob-p1.md
Base: d07768e

Task 1: complete (commits e1fbc92..58aede3, review clean — workspace scaffold + CI)
Task 2: complete (commits 58aede3..f6f1cdf, review clean — identity types)
  MINOR (for final review): id.rs parse() uppercases body before base32 decode → accepts mixed-case IDs, weakens lowercase canonical invariant (two casings decode to same digest but != as SchemaId). Not brief-tested; tighten later.
  MINOR: no FamilyId::new_v7/parse roundtrip test (listed in Produces).
Task 3: complete (commits f6f1cdf..610583a, review clean — envelope, errors, port traits; all signatures verified)
Task 4: complete (commits 610583a..ad53292, review clean — bounded JSON parser, security boundary; depth-bomb/NaN/dup-key-via-escape all verified safe)
  MINOR (final review): over-limit field/string is fully materialized before length rejection (bounded by max_bytes overall, not a security issue; could pre-check to fail faster).
  MINOR: unpaired low-surrogate escape → Utf8Error (semantic stretch, acceptable given fixed enum).
  NOTE: oversized string VALUES reuse SizeExceeded (no dedicated variant) — adjudicated reasonable.
Task 5: complete (commits 2313009..879bef1, review Important fixed — shape extraction + canonical hashing; value/key-order independence, empty-array distinction, deterministic BTree canon, pinned golden all verified; fingerprint version tag now derived from CANONICALIZER const)
  MINOR (final review): canonical_bytes object-key escaping covers only " \ and <0x20 controls; not guaranteed to round-trip a generic JSON parser (e.g. U+2028). Fine for preimage injectivity; doc note for consumers.
