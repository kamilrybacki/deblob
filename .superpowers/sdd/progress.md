# Deblob P1 — progress ledger

Branch: p1-deterministic-core
Plan: docs/superpowers/plans/2026-07-14-deblob-p1.md
Base: d07768e

Task 1: complete (commits e1fbc92..58aede3, review clean — workspace scaffold + CI)
Task 2: complete (commits 58aede3..f6f1cdf, review clean — identity types)
  MINOR (for final review): id.rs parse() uppercases body before base32 decode → accepts mixed-case IDs, weakens lowercase canonical invariant (two casings decode to same digest but != as SchemaId). Not brief-tested; tighten later.
  MINOR: no FamilyId::new_v7/parse roundtrip test (listed in Produces).
Task 3: complete (commits f6f1cdf..610583a, review clean — envelope, errors, port traits; all signatures verified)
