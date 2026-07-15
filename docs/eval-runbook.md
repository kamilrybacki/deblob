# Eval harness runbook: running `deblob-eval` against a real endpoint

`crates/deblob-eval` scores a CONFIGURED `deblob_slm::SemanticInferencer`
endpoint against the golden corpus (`crates/deblob-eval/corpus/*.json`,
25 seed cases as of P2-A/B Task 8) and prints a wrong-valid/false-merge
led report. CI only ever runs it against a wiremock-mocked endpoint
(`crates/deblob-eval/tests/eval_it.rs`) — this document is the procedure
for pointing it at a REAL model. The eval harness measures a given
endpoint; it does not tune, fine-tune, quantize, or serve one.

## Configuration

Three environment variables, or the equivalent CLI flags on the
`deblob-eval` binary:

| Env var | CLI flag | Required | Notes |
|---|---|---|---|
| `DEBLOB_SLM_BASE_URL` | `--base-url` | yes | An OpenAI-compatible base URL, e.g. `http://localhost:8000/v1`. `HttpInferencer` POSTs to `{base_url}/chat/completions`. |
| `DEBLOB_SLM_MODEL` | `--model` | yes | The model id to request. |
| `DEBLOB_SLM_API_TOKEN` | *(none — env only)* | no | Bearer token, if the endpoint requires auth. **There is intentionally no `--api-token` flag** — a CLI flag lands in shell history and `ps`; this repo's global constraint is "env-only, never logged, never in TOML" (`docs/superpowers/plans/2026-07-14-deblob-p2ab.md` § Global constraints). The token is never printed, never written to the JSON report, and never included in any error message. |

A flag always overrides its env var when both are set (see
`resolve_slm_config` in `crates/deblob-eval/src/main.rs`). Missing
`base_url`/`model` from both sources is a hard error naming both the flag
and the env var — it never silently defaults to a real network call.

```bash
export DEBLOB_SLM_BASE_URL="http://localhost:8000/v1"
export DEBLOB_SLM_MODEL="granite-4.0-h-1b-instruct-Q4_K_M"
export DEBLOB_SLM_API_TOKEN="…"   # only if the endpoint requires auth

cd crates/deblob-eval   # so the default `corpus` directory resolves
cargo run --release --bin deblob-eval -- --json-out /tmp/deblob-eval-report.json
```

Other flags: `--corpus <dir>` (default `corpus`, run from
`crates/deblob-eval/` or pass an absolute path), `--k <1|3|5>` (see
below; omit to run all three), `--timeout-ms` / `--max-concurrency`
(defaults `30000` / `4`).

### The `--k` retrieval-budget ablation

Hermes' Task 3 review: *"the eval evaluates k = 1, 3, 5."* Every corpus
case already bakes in a fixed retrieved top-k (Task 3's structural
retrieval, `top_k = 3` default — no seed case retrieves more than 3
candidates). `--k` truncates each case's retrieved set to `rank <= k`
before it is sent to the endpoint, simulating a smaller retrieval budget.
Omit `--k` to run all three (1, 3, 5) in one invocation, each producing
its own labeled report section. Because no seed case exceeds 3 retrieved
candidates today, `k=3` and `k=5` currently coincide with the untruncated
corpus — `k=1` is the only budget that changes what the endpoint actually
sees. The printed `recall@1`/`recall@3`/`recall@5`/`mrr` figures are
**not** affected by `--k` — they are computed from the corpus's own
`expected.gold_rank` (Task 3's already-recorded retrieval result), not
from what this particular run sent to the endpoint.

## Recommended first model: IBM Granite 4.0 Nano 1B

Per `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Task 8 —
default model targets" (authoritative):

**Start with IBM Granite 4.0 Nano 1B** (dense/instruct variant; ~1.6B
actual parameters — eval the exact QUANTIZED artifact you intend to
serve, not the reference weights). Why this one first:

- **Apache 2.0** — no licensing friction for a self-served homelab/CI
  endpoint.
- **Native OpenAI-schema tool calling** — the Task 2 `HttpInferencer`
  request shape (a single required `submit_semantic_decision` tool,
  `additionalProperties: false`, forced `tool_choice`) is exactly the
  interface this model is trained against; no prompt-engineering
  workaround is needed to get structured output out of it.
- **Governed-extraction fit** — the 3-way contract (`match_schema` /
  `new_candidate` / `abstain`, fixed enums, no free-text rationale) is a
  constrained-classification task, which is what this model class targets.
- **Small enough to self-serve** on modest hardware via any
  OpenAI-compatible server (llama.cpp-server, vLLM, Ollama, LM Studio),
  keeping the eval loop fast and cheap to iterate on.

This is the deployable zero/few-shot baseline — run the FULL corpus
against it (`--k` omitted, all three budgets) before touching anything
else.

### Second: FunctionGemma 270M

An efficiency-specialist LOWER BOUND, not a candidate to ship zero-shot:
native function-calling tokens (eval its native FunctionGemma formatting,
not a generic chat-JSON prompt — a generic prompt will underrate it), NOT
expected to be production-reliable without an eventual fine-tune on
Deblob's own decisions/hard-negatives/abstentions. Review its license
before any deployment decision (unlike Granite's unambiguous Apache 2.0).
Run it to establish the efficiency floor of the corpus, not as the
default lane.

### Do NOT start with a 3-4B model

Only if **both** Granite 4.0 Nano 1B and FunctionGemma 270M fail the
risk-coverage gate (below), add **SmolLM3 3B** as a capability CEILING —
its only purpose is to separate "the model is too weak" from "the
candidate model set is wrong," before touching retrieval, embeddings, or
the prompt. If SmolLM3 3B also fails, the problem is very unlikely to be
"try a bigger model" — revisit retrieval (Task 3) and the prompt (Task 4)
first, per Hermes' Task 3 review: *"the SLM cannot recover a schema
omitted from top-k."*

## How to read the report

The `report()` function (`crates/deblob-eval/src/report.rs`) prints a
`--- HEADLINE (go/no-go gates; Hermes' Task 5 review) ---` block first,
on purpose — these three numbers are what to look at before anything
else:

- **Wrong-valid rate** — schema-valid (parsed, contract-conformant)
  output that is nonetheless the WRONG answer. Tracked from a counter
  independent of `schema_valid_rate`, so a 100% schema-valid rate can
  never hide it. Go-live gate: **≤ 0.5%**.
- **False-merge rate** — an ACCEPTED match to the WRONG family. **The
  hard go-live gate is ZERO.** False merges corrupt identity (an
  unrelated schema's data becomes indistinguishable from the family it
  was wrongly merged into, and that damage does not self-heal) — this is
  not symmetric with false splits, which merely delay a legitimate merge
  and are repairable later.
- **False-split rate** — the model declined a match it should have
  accepted. Reduces coverage, does not corrupt anything; a much softer
  concern than false-merge.

The full go-live gate (statistical-power floor, per-relation precision
thresholds, coverage floor, worst-slice floor, injection/repeat-agreement
checks, latency/error SLOs) is documented in full in
[`docs/shadow-golive-gate.md`](shadow-golive-gate.md) — **link, don't
duplicate**: that document is the authoritative checklist a human
reviews against the SHADOW LOG (not this eval harness's corpus report)
before any SLM decision is ever applied live. This eval harness's report
is the offline, pre-shadow signal: it tells you whether a candidate
endpoint is even worth pointing the shadow classifier at. A candidate
endpoint failing the corpus-level wrong-valid/false-merge numbers in this
report has no business being wired into shadow in the first place; a
candidate that passes here still has to earn go-live through the shadow
log's much larger, live-traffic-derived sample per
`docs/shadow-golive-gate.md`.

Everything below the headline block (parse/schema-valid/exact accuracy,
abstention precision/recall, recall@k/MRR, relation confusion, novel-
family recall/precision, gold-absent abstention, per-category worst-
slice, prompt-injection resistance, repair rate, failure classes,
latency, tokens, cache-hit rate) is diagnostic detail for root-causing a
headline-number failure, not an independent go/no-go signal on its own.

## What this harness does NOT do

- **It does not tune, fine-tune, quantize, or serve a model.** It only
  measures whatever endpoint you point it at.
- **It does not gate or block anything automatically.** No exit code or
  CI step fails a build on a bad wrong-valid/false-merge number today —
  the numbers are for a human to read and decide with, same as
  `docs/shadow-golive-gate.md`'s explicit "documented, NOT automated in
  P2" stance.
- **It never talks to the cold lane, the registry, Redis, or any live
  Deblob state.** The golden corpus is its only ground truth.

## Corpus-growth TODO before a real go-live decision

Per `docs/superpowers/plans/deblob-p2ab-hermes-review.md` § "Mandatory"
corpus cases, three case types are explicitly deferred beyond the Task 6
seed corpus and are NOT yet represented under `crates/deblob-eval/corpus/`:

1. **Heterogeneous arrays** (an array field whose elements are not
   structurally uniform — mixed shapes/types within one array).
2. **Redaction collisions** (two distinct families whose only
   distinguishing field(s) are exactly the field(s) redaction strips,
   making them look identical post-redaction — a case that specifically
   probes whether the PII-safe prompt builder's redaction can itself
   manufacture a false-merge risk).
3. **Quantized-vs-reference comparison** (running the SAME model at two
   quantization levels through the eval and diffing the reports via
   `regression_delta` in `crates/deblob-eval/src/metrics.rs` — needed
   because "eval the exact quantized artifact you intend to serve" above
   is only actionable once there is a reference point to compare it
   against).

Add these cases (and re-run the growing corpus against whichever model
is leading the risk-coverage curve) before treating this harness's
report as sufficient evidence for a go-live decision — the current 25-case
seed corpus is breadth-of-category coverage for TDD, not a
statistically-powered evaluation set.
