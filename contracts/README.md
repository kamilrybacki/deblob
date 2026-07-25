# Business-rule contracts

Formal, machine-checked statements of the **operational invariants** Deblob holds
about its own behaviour — the "model proposes, deterministic code decides, human
approves" governance surface. They are written in [Tauto](https://github.com/kamilrybacki/tauto)'s
declarative contract DSL (embedded in Markdown) and verified in CI (Lean 4 proofs).

## What is checked

`tauto verify` (run with `--lean-check` in CI) proves the rule set is:

- **consistent** — no two rules contradict each other for the same operation;
- **live** — no rule is dead (an unsatisfiable `requires` that can never fire);
- **well-typed** — every field/state/operation resolves against `_glossary.md`;
- **Lean-proved** — the generated Lean 4 workspace compiles (`lake build`).

This guards the *specification* of Deblob's rules: if someone edits a threshold or
adds a rule that conflicts with an existing invariant, CI goes red. It does not
(yet) reconcile the rules against live runtime state — that is a later step using
Tauto's observed-states / `reconcile` path.

## The invariants (files)

| File | Invariant |
|------|-----------|
| `promotion.md` | Auto-promote only with ≥50 samples, ≥10 min age, settled + corroborated + domain-coherent shape, and never from the heuristic fallback. |
| `umbrella.md` | Umbrella consolidation always requires human (HITL) approval, within a coherent domain. |
| `trust-gate.md` | The SLM runs only on the cold lane (never the hot path); its proposal is accepted only when deterministically corroborated, no weaker than the heuristic, and never inventing a `schema_id`. |
| `pii-safety.md` | No captured or archived sample may carry a raw payload or PII. |
| `_glossary.md` | Canonical entities/fields/operations the rules reference. |

## Editing

Change a rule → CI re-verifies on the PR (path-filtered to `contracts/**`). To check
locally without a Lean toolchain, run the parse + conflict + dead-rule scan:

```bash
tauto verify contracts/            # add --lean-check if you have lake in PATH
```

or reproduce CI exactly with the Lean-equipped image:

```bash
docker run --rm -v "$PWD/contracts:/contracts:ro" \
  ghcr.io/kamilrybacki/tauto-lake:5eb3d41 \
  verify /contracts --output /tmp/ws --lean-check
```

Translating a prose rule into the DSL is easier via Tauto's `POST /api/v1/translate`
(the deployed `tauto` service, SLM front door) — but review the emitted DSL for
faithfulness before adding it here; "it compiles" is not "it is faithful".
