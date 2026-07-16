# SLM Synthetic Training-Corpus Generator — Design Spec

- **Date:** 2026-07-16
- **Status:** Draft
- **Motivation:** P3 eval proved zero-shot small models fail the semantic-authority gate (decision accuracy 28%, wrong-valid 56–88%) — the model can't make Deblob's decision without a fine-tune. The 25-case golden corpus is a *test set*, far too small to train on. But **Deblob owns the ground truth**: the canonicalizer + monoid can *deterministically* generate labeled `(candidate, top-k, gold decision)` cases at scale. This builds that generator — the highest-leverage step toward a competent fine-tuned model.
- **Scope:** an additive `deblob-eval generate` subcommand (reuses the `EvalCase` corpus types so output round-trips through the existing loader). NO product-crate changes. Output feeds both the eval (bigger test set) and a fine-tune (prompt→gold-tool-call JSONL).

## 1. What it produces

`EvalCase` JSON files (`crates/deblob-eval/src/corpus.rs` types, byte-compatible with the golden corpus) — `{ name, category, candidate: CandidateProfileView, retrieved: Vec<FamilyCandidate>, expected: { decision, gold_schema_id, gold_rank, false_merge_trap, false_split_trap } }` — where **every label is derived from a known deterministic transformation**, not an LLM. Plus a fine-tune export (§4).

## 2. Case generation (ground-truth by construction)

For a seed set of base "family" schemas (parameterizable count), each with a canonical structural fingerprint, generate variants with a **known relation**:

| Variant | Transformation | `expected.decision` | traps |
|---|---|---|---|
| **exact** | reorder keys / whitespace (same canonical) | `match_schema(Exact)` | — (calibration/control) |
| **compatible_drift** | add optional field, widen nullability, add wrapper around unchanged payload, add enum value | `match_schema(CompatibleDrift)` | — |
| **false_split** | rename fields (snake↔camel, vendor-prefix, abbrev), reorder, add sparse optional — **same family, different surface** | `match_schema(CompatibleDrift)` (the model MUST recognize it as same-family) | `false_split_trap=true` |
| **incompatible_similarity** (**false-merge trap**) | same structure, different MEANING: swapped units (Cel↔degF), same generic names different semantics (`id`/`status`/`type`), one discriminator differs, same envelope / unrelated payload | `match_schema(IncompatibleSimilarity)` (an accepted-match=false result) | `false_merge_trap=true` |
| **new_family** | a genuinely different structure | `new_candidate(Structural)` | — |
| **abstain** | insufficient evidence (low obs count), ambiguous (two near-equidistant families), gold-absent (gold NOT in top-k) | `abstain(<cause>)` | — |

The base schemas + transformations reuse the deterministic tools: `deblob-fingerprint` (canonical shape), `deblob-monoid` (profile / generalization). A variant's `candidate` is its monoid-profile view; the label is set by which transformation produced it.

## 3. Retrieved top-k construction

For each case, build a realistic `retrieved: Vec<FamilyCandidate>` = the **gold family** (at a chosen `gold_rank` ∈ {1,2,3}, or ABSENT for the gold-absent abstain case) + near-neighbour distractor families (other base families, ranked by real structural distance). Set `expected.gold_schema_id` + `gold_rank` accordingly. Distances must be consistent (gold closer for match cases; for incompatible-similarity, the near-neighbour is structurally close but semantically wrong — that's the trap).

## 4. Fine-tune export

Also emit a JSONL where each line is `{ messages/prompt, gold_tool_call }` — the case rendered through the SAME PII-safe prompt builder the shadow lane uses (`deblob-slm::prompt`) → the exact prompt the model will see in production, paired with the gold `submit_semantic_decision` tool-call. This is directly fine-tunable (Needle/FunctionGemma). Never leak raw values (the prompt builder already redacts).

## 5. Partition discipline (Hermes' review — mandatory)

**Family/time-separated splits, never random.** Tag each case with a `partition` (train / holdout) BY FAMILY — all variants of a family go to the same partition, so a fine-tune's holdout never contains a train family's sibling version. The generator emits the split; the fine-tune must honour it. Report the case-mix distribution (must roughly match the design's corpus composition: ~25% known/exact, 20% drift, 15% incompatible/related, 20% new, 20% ambiguous/abstain).

## 6. CLI

`deblob-eval generate --out <dir> --families <N> --variants-per-family <M> --seed <S> [--finetune-jsonl <path>]` — deterministic (same seed → byte-identical corpus). Prints the generated case-mix + partition summary.

## 7. Non-goals

- No LLM in the loop (ground truth is deterministic — the whole point).
- No actual fine-tuning (that's the next step, external — this produces the DATA).
- No new relations/contract changes (uses the existing 3-way contract).
- No embeddings.

## 8. Acceptance

`deblob-eval generate` produces N×M `EvalCase` files that (a) load back through the existing corpus loader with zero errors, (b) carry correct labels (a unit test asserts an exact-variant → `Exact`, a unit-swap variant → `IncompatibleSimilarity` + `false_merge_trap`, a rename variant → `CompatibleDrift` + `false_split_trap`), (c) partition by family, (d) emit a fine-tune JSONL whose prompts contain NO raw values. Deterministic by seed.
