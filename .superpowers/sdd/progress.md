# Deblob P2-D — progress ledger

Branch: p2d-semantic-fingerprint
Plan: docs/superpowers/plans/2026-07-15-deblob-p2d.md
Spec: docs/superpowers/specs/2026-07-15-deblob-p2d-semantic-fingerprint.md
Base: 12ca08a (docs commit; branch off main e882ddd)
Hermes review deblob-p2d-01: FOLDED (deblob-p2d-hermes-review.md authoritative; amended spec + Tasks 1-7 in commit below)

Task 1: complete (revision commit 45060c5, review ✅+Approved — folded Hermes §1/§3: namespaced Unit, typed PathSegment, event_type/temporal/numeric axes, privacy_class OUT of sem_ onto SchemaRecord, SemanticFingerprint=newtype not None-variant; 22/22 core, 224/224 workspace lib/bins, fmt/clippy clean; orig 20c5349 superseded)
  MINOR (final review): case-sensitivity + no-privacy_class tests true-by-construction (no validation logic yet, correct at this stage).

USER REQUEST (mid-run): add deterministic semantic-similarity search (vector-DB-like) over schemas -> NEW Task 9 (Hermes' path-independent signature pulled forward from P4). Hermes design Q fired. Execute ALL phases autonomously (user asleep).
Task 2: complete (commit eb38fd1, review ✅+Approved — new deblob-semantic crate; version-addressed UCUM/ISO4217/namespaced-meaning tables (immutable const), injectable field-id/event-type registries (empty-default reject), validate_metadata walks all 5 axes w/ typed VocabError first-offender; 20/20 crate, 244/244 workspace, clean)
  NOTE (P4 product decision): NAMESPACE_CODES baked as immutable table not injectable registry (namespaces may be operator-specific like field-ids); acceptable per brief text, flag for P4.
Task 3: complete (commit pending — byte-level canonical protocol (canon.rs) + sem_ digest (digest.rs) in deblob-semantic; typed paths w/ NFC + anti-ambiguity, presence-bitmap attribute encoding (no null tags), numeric_scale/enum-keys via hand-rolled no-float decimal canonicalizer, event_type at fixed schema position, duplicate-path/duplicate-enum-key guards, empty->Ok(None) never a sentinel, sch_ never in preimage (SemanticMetadata has no schema-id field); 63/63 crate, 272/272 workspace lib/bins, fmt/clippy clean; report .superpowers/sdd/task-3-report.md)
  NOTE (self-review, flagged for reviewer): used a 1-byte presence bitmap per optional-attribute struct rather than literally zero bytes for absent attributes, to guarantee self-delimiting framing/injectivity — read "not a null tag" as ruling out a 3-state null-vs-absent distinction, not a presence discriminator.
Task 4: pending
Task 5: pending (amended: append-only revisions + active pointer + ETag)
Task 6: pending
Task 7: pending (amended: drift + same-sem_ diagnostic, strength-classified)
Task 9: pending (NEW — path-independent semantic signature + similarity neighbor search; Hermes design Q pending)
Task 8: pending (capstone — runs LAST, after 9)
