# Deblob P2-D Polish — progress ledger

Branch: p2d-polish
Plan: docs/superpowers/plans/2026-07-16-deblob-p2d-polish.md
Base: main @ 8531000 (P2-D merged)

Task 1: complete (commit 1849858, review ✅+Approved — drift loops 1..version (all priors, current excluded, v1 included), None-prior=no-drift, algorithm unchanged, zero-mutation, per-prior error non-aborting; non-adjacent v1<->v3 drift fires RED->GREEN vs real Redis; 142 deblob, 348 workspace, clean. MINOR: linear read cost per annotation, families small.)
Task 2: complete (Registry::get_family/list_family_versions read `deblob:family:<fam_id>`'s `next_version` HGET — the same field HINCRBY writes in PUBLISH_SCRIPT, no write-path change; versions derived as 1..=current, contiguous by construction; get_family 200/404, get_family_versions 200/404 (existence-checked via get_family first), both already under require_bearer; 9 fake Registry impls updated across 8 files; 2 new real-Redis IT (registry_it.rs) + 5 new api_it (200/404 x2 + 401x2); 348 workspace lib/bins, 47 deblob-redis (incl IT), 147 deblob (incl IT), fmt/clippy -D warnings clean; report .superpowers/sdd/task-2-report.md)
Task 3: implemented (commit 7f2bc84 — typed EnumValue{Null,Bool,Number(text),String} + Vec<EnumMapping>; string "true" != bool true PROVEN (golden), non-enum sem_ stable, numeric 1/1.0/1e0 preserved, 522 tests/46 suites incl real Redis, clean); review ✅+Approved (identity-checkpoint).
  Review: all 7 invariants ✅ — non-enum sem_ UNCHANGED (encoding change isolated to typed_value_bytes/encode_enum_semantics), string!=bool real (tag from Rust discriminant), numeric 1/1.0/1e0 preserved, deterministic sort, ripple (canon/signature/vocab all 3 sites) consistent.
  Deviations OK: InvalidEnumNumber fail-closes at digest layer before storage (vocab could reject earlier, not required, both ->422); 422 = axum default for valid-JSON-wrong-shape.
ALL 3 POLISH TASKS COMPLETE + REVIEWED. Next: merge to main.
