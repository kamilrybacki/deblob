# SLM synthetic training-corpus generator — implementation report

Branch: `slm-corpus-gen`. Spec: `docs/superpowers/specs/2026-07-16-slm-corpus-generator.md`.

## What was built

A new `deblob-eval generate` subcommand + a `crates/deblob-eval/src/generate/`
module (split into `mod.rs`, `fields.rs`, `families.rs`, `variants.rs` — ~1950
lines total including tests). Purely additive: no product-crate (`deblob`,
`deblob-http`, `deblob-kafka`, `deblob-match`, `deblob-redis`,
`deblob-semantic`) changes. `deblob-eval`'s own `Cargo.toml`/`lib.rs`/
`main.rs` gained the new module, two new path deps (`deblob-fingerprint`,
`deblob-monoid`) and two new crates (`rand`, `rand_chacha`, both already used
elsewhere in the workspace by `deblob-bench` at the same versions).

## Design

### Base families (`families.rs`)

`build_families` samples 3-7 fields per family from a 20-entry static pool
(`fields.rs::FIELD_POOL` — strings, enums, numbers, bools, an array, two
nested-object shapes) using a Fisher-Yates shuffle driven by a single
`ChaCha8Rng` seeded from `GenerateConfig::seed`. Each family's canonical
identity (`SchemaId`) is computed by running a **fixed placeholder document**
(no RNG — values never affect a canonical fingerprint) through the exact
same deterministic pipeline the product uses:
`deblob_fingerprint::{parse_bounded, shape_of, fingerprint}`. Collisions
(two families sampling the identical field set) are detected and resampled
from the same RNG stream, so output stays seed-deterministic. `FamilyId`s are
**not** `FamilyId::new_v7()` (wall-clock `Uuid::now_v7()` — would break
determinism) but 16 RNG bytes formatted as a UUID string and parsed via
`FamilyId::parse`.

Families are split into train/holdout (Hermes' partition rule, spec §5) by a
deterministic ~80/20 shuffle of family indices; every variant later derived
from a family inherits **that family's** partition, so no sibling variant is
ever split across train/test.

### Structural distance (`families::jaccard_distance`)

A field-**name-blind** heuristic: 1 − Jaccard similarity over the multiset of
field *type labels* (string/number/bool/array/object), optionally descending
one level into nested objects. This is deliberately what makes a renamed
(`false_split`) or semantically-swapped (`incompatible_similarity`) variant
register as *structurally close* to its true family (same types, different
names/meaning) while a `new_family` candidate (genuinely different type
composition) registers as far — directly mirroring spec §3's "near-neighbour
structurally close but semantically wrong" framing. `retrieved` top-k lists
are built by sorting real families (restricted to the candidate's own
partition, so no cross-partition schema-id leakage) by this distance; `rank`
falls out of the sort rather than being forced to a target value.

### The six transformations (`variants.rs`)

Every case's `expected` label is set **directly by which `VariantKind` branch
ran** — no matcher, no LLM, ever invoked:

| `VariantKind` | transformation | decision | traps |
|---|---|---|---|
| `Exact` | same fields, fresh values | `MatchSchema(Exact)` | — |
| `CompatibleDriftAddOptional` / `WidenNullability` / `Wrapper` / `EnumValue` | add optional field / null a field sometimes / wrap in `{data,meta}` / inject an out-of-pool enum value | `MatchSchema(CompatibleDrift)` | — |
| `FalseSplitSnakeCamel` / `VendorPrefix` / `Abbrev` | same field **types**, every NAME renamed (snake→camel, `vnd_` prefix, vowel-dropped abbreviation) | `MatchSchema(CompatibleDrift)` | `false_split_trap=true` |
| `IncompatibleUnitSwap` | same names/types, one numeric field's magnitude bucket deliberately shifted (visible discriminator) | `MatchSchema(IncompatibleSimilarity)` | `false_merge_trap=true`, `gold_schema_id=None` |
| `IncompatibleGenericNames` | identical stats to a real near-neighbour family; offered as that neighbour's own schema | `MatchSchema(IncompatibleSimilarity)` | `false_merge_trap=true` |
| `NewFamily` | one of 3 hand-authored templates never in `FIELD_POOL` | `NewCandidate(Structural\|Semantic)` | — |
| `AbstainInsufficientEvidence` | 1-3 observations only | `Abstain(InsufficientEvidence)` | — |
| `AbstainAmbiguous` | fields blended from two same-partition families, retrieved distances forced to an explicit tie | `Abstain(Ambiguous)` | — |
| `AbstainCandidateMissing` | candidate genuinely is family X, but X is excluded from `retrieved` | `Abstain(CandidateMissing)`, `gold_schema_id=Some(X)`, `gold_rank=None` | — |

Per-family case mix follows Hermes' 25/20/15/20/20 composition target
(`variants::bucket_counts`, largest-remainder allocation), with sub-flavors
round-robin-cycled by `family_index` for corpus variety, in a fixed block
order (exact → drift → incompatible → new_family → abstain) for full
determinism.

### Candidate construction

Each variant generates N synthetic JSON documents (`fields::gen_document`,
20-90 observations for most variants, 1-3 for `insufficient_evidence`), folds
them through `deblob_monoid::Profile::{from_node,merge}` (same pipeline a
real candidate cluster uses), then `deblob_slm::CandidateProfileView::from_profile`
produces the redacted view stored on the `EvalCase` — byte-for-byte what a
real endpoint would see, not a hand-rolled approximation.

### Fine-tune export

`render_finetune_jsonl` renders each case through
`deblob_slm::build_prompt(&case.candidate, &case.retrieved, &allowed_ids)` —
the *exact* PII-safe builder the shadow lane uses — and pairs it with
`serde_json::to_value(&case.expected.decision)` (the exact
`submit_semantic_decision` tool-call shape) as `gold_tool_call`, one JSON
object per line: `{case_name, partition, prompt, gold_tool_call}`. Because
`prompt` is built only from `CandidateProfileView` (stats-only) and
`retrieved` (ids/distances), no raw payload value can structurally reach it.

## Real run (acceptance §8)

```
deblob-eval generate --families 20 --variants-per-family 8 --seed 1 \
  --out /tmp/gen-corpus --finetune-jsonl /tmp/gen-corpus-finetune.jsonl
```

Output:

```
wrote 160 generated case(s) to /tmp/gen-corpus

generated 160 case(s) across 20 families
case mix by category:
  known_exact                 40  (25.0%)
  compatible_drift            40  (25.0%)
  incompatible_unsafe         20  (12.5%)
  new_family                  40  (25.0%)
  ambiguous_adversarial       20  (12.5%)
partition split:
  train                      128
  test                        32
traps: false_merge=20 false_split=20

wrote fine-tune JSONL (160 lines) to /tmp/gen-corpus-finetune.jsonl
```

Confirmed `deblob-eval --corpus /tmp/gen-corpus` (no `--base-url`) proceeds
past corpus loading and only fails later at endpoint-config resolution —
proving `load_corpus` accepted all 160 generated files with zero errors,
using the production loader unmodified.

Note on the printed mix: with `--variants-per-family 8`, the ideal
25/20/15/20/20 split (2.0/1.6/1.2/1.6/1.6 slots) has a 3-way tie on the
fractional remainder (drift/new_family/abstain all at 0.6); the
largest-remainder tie-break (by bucket index) credits drift and new_family
the extra slot, landing at 25/25/12.5/25/12.5 for this specific M. Larger
`--variants-per-family` values converge closer to the exact target; this is
a rounding artifact of small M, not a bug, and is within spec §5's "roughly
match" tolerance.

## Tests (`generate/mod.rs`, 11 new unit tests, all passing)

- `generated_cases_load_back_through_the_corpus_loader` — writes to a temp
  dir, reloads via `corpus::load_corpus`, zero errors.
- `every_generated_case_is_self_valid` — `EvalCase::validate()` on every
  generated case.
- `exact_variant_labels_exact_relation` / `unit_swap_variant_is_incompatible_similarity_with_false_merge_trap` /
  `rename_variant_is_compatible_drift_with_false_split_trap` — the three
  acceptance-mandated label assertions.
- `partition_by_family_holds` — same family ⇒ same partition; no schema id
  referenced by both a Train and a Test case.
- `finetune_jsonl_never_contains_raw_field_values` — none of `FIELD_POOL`'s
  enum literal values (length ≥ 3) appear as a bare quoted token anywhere in
  the rendered JSONL.
- `same_seed_produces_byte_identical_output` / `different_seed_produces_different_output`.
- `case_names_are_unique`, `format_summary_reports_every_category_and_partition`.

Full suite: `cargo test -p deblob-eval` → 40 passed (29 pre-existing + 11
new), `cargo build -p deblob-eval` clean, `cargo fmt -p deblob-eval`
idempotent, `cargo clippy -p deblob-eval -- -D warnings` → no issues (one
`clippy::doc_lazy_continuation` false-positive from a doc-comment line
starting with `+` was fixed by rewording).

## Files touched

- `crates/deblob-eval/src/generate/mod.rs` (new) — public API + tests
- `crates/deblob-eval/src/generate/fields.rs` (new) — field pool + document generation
- `crates/deblob-eval/src/generate/families.rs` (new) — base families + distance heuristic
- `crates/deblob-eval/src/generate/variants.rs` (new) — the six transformations
- `crates/deblob-eval/src/lib.rs` — `pub mod generate;` + re-exports
- `crates/deblob-eval/src/main.rs` — `Command::Generate` subcommand, `GenerateArgs`, `run_generate`
- `crates/deblob-eval/Cargo.toml` — `deblob-fingerprint`, `deblob-monoid`, `rand`, `rand_chacha`
- `Cargo.lock` — updated for the above

## Concerns / follow-ups

1. **Case-mix rounding at small M** (see above) — cosmetic, self-corrects at
   larger `--variants-per-family`.
2. **`IncompatibleGenericNames` degenerate fallback**: if a family's
   partition has no other same-partition family (only possible with very
   small `--families`, e.g. `--families 1`), it falls back to referencing
   itself as the "lookalike" — harmless but slightly less realistic; not
   triggered at the recommended `--families >= ~10`.
3. This generator's structural-distance heuristic (`jaccard_distance`) is a
   **generator-internal plausibility proxy**, not the product's real
   retrieval algorithm (`deblob-match`) — intentional per spec §7 ("no
   matcher invocation"), but worth flagging so nobody mistakes `distance`
   values in generated cases for ground truth about the real retriever's
   behavior.
4. Fine-tune JSONL currently emits every case (both partitions) tagged with
   `partition`; a downstream fine-tune step is expected to filter to
   `train` and hold `test` out, per spec §5 ("the generator emits the
   split; the fine-tune must honour it").
