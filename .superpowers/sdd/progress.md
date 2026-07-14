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
Task 6: complete (commits 879bef1..4bd83d4, review Important fixed — schema monoid; associativity/commutativity/identity proptest-proven, generalized_fingerprint != raw shape fp verified)
  DEVIATION FROM BRIEF (intentional): brief said "bool flags OR'd"; corrected int_only to AND-merge (universal "all numbers integer" claim; identity().int_only=true) while keeping existential _seen flags OR. Reviewer-confirmed brief defect; int_only has zero consumers until P2.
  MINOR: added direct generalized-vs-raw fingerprint assert_ne test (was implicit).
Task 7: complete (commits 4bd83d4..d3eb2a8, two Important fixed + focused re-review clean — Redis schema vault, atomic Lua publication; CHECKPOINT CLOSED)
  FIX A: immutability compare narrowed to canonical+canonicalizer only (schema now stored as HASH) — idempotent retries with fresh provenance no longer wrongly violate.
  FIX B: Registry::publish now returns authoritative FamilyVersion (HINCRBY-allocated); caller-supplied record.version never trusted; get_schema/list_schemas return authoritative version. TRAIT SIGNATURE CHANGED in deblob-core (only impl so far).
  MINOR (final review): version cast i64->u32 truncates rather than errors (unreachable in practice).
