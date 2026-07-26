# SLM fine-tune bake-off — candidate bases vs the Qwen2.5-0.5B baseline (2026-07-26)

Extends the Arm-C validation ([experiment-arm-c-validation-results.md](experiment-arm-c-validation-results.md))
to more base models, under the SAME corpus, LoRA config and eval, so we can ask:
does any lighter/other base beat the established `Qwen2.5-0.5B-Instruct` on Deblob's
3-way decision task? Runner: [`deploy/experiment/modal/bakeoff.py`](../deploy/experiment/modal/bakeoff.py)
(a parametrized fork of `validate.py`). Raw result JSONs: `docs/artifacts/slm-bakeoff-2026-07-26/`.

## Setup (identical across all runs)

- **Data:** `deblob-eval generate --families 40 --variants-per-family 10 --seed 7` →
  400 ground-truth-labeled cases, **family-partitioned 320 train / 80 held-out** (a
  held-out family's siblings never appear in train — no leakage).
- **Train:** LoRA r=16 α=32, 4 epochs, T4, batch 1 / seq 512, gradient checkpointing, lr 2e-4.
- **Eval:** greedy-generate a tool-call per held-out prompt; score against EXTERNAL corpus
  gold (never the gate's own predicate). `before` = base model 0-shot; `after` = fine-tuned.

## Result — `after` (base 0-shot → fine-tuned), ranked by decision accuracy

| Base model | params | parse_rate | **decision_match** | dec+relation | exact | final_loss | notes |
|---|---|---|---|---|---|---|---|
| **ibm-granite/granite-4.0-350m** | 350M | 0 → **100%** | 0 → **60.0%** | 10.0% | 10.0% | 0.068 | **best** — hybrid (`granitemoehybrid`): needs `use_cache=false` + explicit `q,k,v,o_proj` target-modules |
| LiquidAI/LFM2.5-1.2B-Instruct | 1.2B | 0 → 93.8% | 0 → **60.0%** | 10.0% | 6.3% | 0.033 | hybrid conv+GQA: target-modules `q,k,v,out_proj,w1,w2,w3` |
| Qwen2.5-0.5B-Instruct *(Arm-C baseline)* | 0.5B | 0 → 96.3% | 0 → 46.3% | 6.3% | 6.3% | ~0.15 | established Deblob model; PEFT auto target-modules |
| openbmb/MiniCPM5-1B | 1B | 0 → 80.0% | 0 → 41.3% | 7.5% | 3.8% | 0.12 | dense transformer; underperforms Qwen despite 2× the params |
| Cactus-Compute/needle | 26M | — | — | — | — | — | **NOT RUN** — custom-JAX, not PEFT-LoRA-able + no x86 serving (see `deploy/experiment/22-model-cactus.yaml`) |

## Reading it

- **decision_match is the metric that matters** (the 3-way `match_schema` / `new_candidate` /
  `abstain` classify). `relation` and especially `schema_id` (the opaque 50-char hash) stay low
  for everyone **by design** — the model is not supposed to memorize identity; structural
  retrieval supplies `schema_id` and the trust gate corroborates it.
- **Two candidates beat the established Qwen 0.5B on decision accuracy: `granite-4.0-350m` and
  `LFM2.5-1.2B`, both at 60% vs Qwen's 46%.**
- **`granite-4.0-350m` is the standout:** best decision AND best parse (100%) at the *smallest*
  size (350M). A strong candidate to replace or supplement Qwen 0.5B on Deblob's decision lane —
  pending the caveats below.
- **Bigger ≠ better here:** MiniCPM5-1B (1B) lands *below* Qwen (0.5B). Size did not buy accuracy
  on this bounded, structured task.
- **Zero-shot `before` = 0% for all** — the base models don't emit Deblob's exact tool-call
  format cold. This measures fine-tune *lift*, consistent with the P3 "NO-GO on zero-shot small
  models" finding ([p3-slm-golive-decision](p3-slm-golive-decision-2026-07-16.md)).

## Caveats (do not over-read this)

- **Single seed (7), one run per model, 80 held-out examples, 4 epochs, r=16.** Apples-to-apples
  with the Arm-C proof, but NOT statistically robust — no multi-seed confidence intervals. A 60%
  vs 46% gap on n=80 is suggestive, not conclusive.
- Evaluates the **HF bf16 model on a T4**, not the *served* artifact. The real production question
  (merged GGUF + quantization + CPU latency/RSS) is unmeasured — and the Granite hybrid's
  **CPU-serving story is unverified** (Ollama/llama.cpp support for `granitemoehybrid` on the
  worker nodes is the gating unknown before it could replace Qwen live).
- Best decision is still **60% — wrong 40% of the time.** This *reinforces* the thesis: no small
  model, fine-tuned or not, is a schema *authority*. It stays a proposer behind the deterministic
  gate.

## Next levers

1. Multi-seed (≥3) + wider held-out for confidence intervals on the granite-vs-qwen gap.
2. Merge → GGUF → quantize the top candidate and re-eval the **served** artifact + CPU p95/RSS.
3. More data / epochs / DPO stage (governance edits) to push decision past 60%.
4. Needle: a separate JAX + ARM path if ever pursued (out of scope for this T4/PEFT bake-off).

## Reproduce

```bash
cargo run -p deblob-eval -- generate --out /tmp/bo --families 40 --variants-per-family 10 \
  --seed 7 --finetune-jsonl /tmp/deblob_finetune.jsonl
modal run deploy/experiment/modal/bakeoff.py --model ibm-granite/granite-4.0-350m \
  --jsonl /tmp/deblob_finetune.jsonl --target-modules q_proj,k_proj,v_proj,o_proj --no-cache
modal run deploy/experiment/modal/bakeoff.py --model LiquidAI/LFM2.5-1.2B-Instruct \
  --jsonl /tmp/deblob_finetune.jsonl --target-modules q_proj,k_proj,v_proj,out_proj,w1,w2,w3
modal run deploy/experiment/modal/bakeoff.py --model openbmb/MiniCPM5-1B --jsonl /tmp/deblob_finetune.jsonl
```

## Live switch (2026-07-26)

The bake-off winner is now the **live** Deblob SLM: swapped `qwen2.5:0.5b` →
`granite4:350m` across `deploy/console/live/{33-deblob-config,40-ollama,50-namer-controller,51-namer-benchmark}.yaml`
(committed `0f02de9`), rolled out ollama (which also fixed a pre-existing stuck
ollama pod) + deblob. Ollama serves `granite4:350m` natively (708 MB pull).

**Live namer-benchmark through the real namer path** (base + few-shot, NOT the
fine-tuned adapter — that's a separate promotion step), FEWSHOT variant:
- format-valid names: **16/16 (100%)**, 0 errors, licensed 15/16, accept-vs-heuristic 4/16
- load p50 **368 ms** (stays resident — keep-alive works), prompt_eval p50 1137 ms, eval p50 361 ms
- wall p50 **8.7 s**, p95 16.5 s — over the 15 s prod budget on 12% (2/16) of calls (the
  degrade-to-heuristic path covers that tail).

So granite4:350m is a viable live base for the naming lane. Still open: promote the
*fine-tuned* Granite adapter (merge→GGUF→import) for the decision lane, and multi-seed
the bake-off before treating the 60% > 46% gap as final.

### Optimization: BF16 → Q4_K_M

The initial live tag `granite4:350m` was **BF16** (708 MB) → it ~doubled ollama's
resident memory (552 → 1059 Mi working set) vs the old Q4 qwen. Fixed by switching to
the **Q4_K_M** GGUF (`hf.co/ibm-granite/granite-4.0-h-350m-GGUF:Q4_K_M`, 222 MB weights,
committed `217fe67`).

Live re-benchmark (FEWSHOT — the production variant) + measured memory:

| metric | qwen Q4 | granite BF16 | **granite Q4_K_M** |
|---|---|---|---|
| ollama working set | 552 Mi | 1059 Mi | **443 Mi** |
| ollama RSS | ~660 Mi | 1550 Mi | **656 Mi** |
| model on disk | ~374 MB | 708 MB | **222 MB** |
| format-valid (few-shot) | — | 100% | **100%** |
| wall p50 / p95 | — | 8.7 / 16.5 s | **5.7 / 10.9 s** |
| eval p50 | — | 361 ms | **168 ms** |
| over 15 s budget | — | 12% | **0%** |

Net: quality held (100% valid, 0 errors, accept-vs-heuristic even improved 4→8/16),
latency dropped ~34%, and memory is now **below the old qwen** — the OOM buffer is fully
restored (443 Mi = 15% of the 3 Gi limit). CPU spikes to the 4-core limit *during* a
generation (Q4 uses all cores for fast eval) then idles at ~1m. COMPACT variant degrades
at Q4 (38% valid) but the live lane uses FEWSHOT.
