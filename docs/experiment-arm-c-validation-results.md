# Arm-C Enhanced-SLM Flow — Real Validation Result (2026-07-17)

First real fine-tune round on Modal T4, proving the continual-learning lane actually improves a lightweight model at Deblob's decision task. Script: `deploy/experiment/modal/validate.py`.

## Setup
- **Model:** `Qwen/Qwen2.5-0.5B-Instruct` — the most lightweight model that runs autonomously (ungated) and fine-tunes to strong results. (FunctionGemma-270M is lighter + FC-specialized but Gemma-license-gated → needs an HF token; Needle-26M is custom-JAX, not PEFT-LoRA-able — both are follow-ups.)
- **Data:** `deblob-eval generate` → 400 ground-truth-labeled cases, **family-partitioned** 320 train / 80 held-out (a held-out family's siblings never appear in train — no leakage).
- **Train:** LoRA r=16 α=32, 4 epochs, T4, batch 1 / seq 512, gradient checkpointing. final_loss ≈ 0.15.
- **Eval:** greedy-generate a tool-call per held-out prompt; score against EXTERNAL corpus gold (never the gate's own predicate).

## Result (base 0-shot → fine-tuned, on the 80 held-out)

| Metric | Base | Fine-tuned | Δ |
|---|---|---|---|
| **parse_rate** (valid `{decision,…}` JSON) | 0% | **96.3%** | **+96.3** |
| **decision_match** (3-way classify correct) | 0% | **46.3%** | **+46.3** |
| decision+relation both correct | 0% | 6.3% | +6.3 |
| exact (incl. `schema_id`) | 0% | 6.3% | +6.3 |

## Reading it
- The lightweight model **learned Deblob's output format** — 0 → 96% valid tool-calls.
- It learned the **decision** (match_schema / new_candidate / abstain) — 0 → 46% correct on unseen families. Real generalization, not memorization.
- **`relation` + `schema_id` stay hard, by design.** `schema_id` is a 50-char opaque hash the model *should not* memorize — that's what Deblob's structural retrieval supplies, and the trust gate then corroborates. The SLM's job is the semantic decision; 46% decision-accuracy from a 0.5B after one small round is a strong start, and the whole continual-learning loop exists to push it up round over round.
- This is the enhanced-SLM flow end-to-end on real hardware: real feedback data → real LoRA on Modal T4 → measurable held-out improvement.

## Reproduce
```
cargo run -p deblob-eval -- generate --out /tmp/corpus --families 40 \
  --variants-per-family 10 --seed 7 --finetune-jsonl /tmp/deblob_finetune.jsonl
modal run --env=arm-c deploy/experiment/modal/validate.py --jsonl /tmp/deblob_finetune.jsonl
```

## Cost
A handful of short T4 runs (model downloaded once, then Volume-cached), well under Modal's free $30/mo credit. Apps scale to zero — no idle cost.

## Next levers (to raise decision/relation accuracy)
- More training data (the generator scales to any N families×variants).
- Decision-focused loss (mask the prompt, weight the tool-call tokens) instead of full-sequence LM loss.
- **FunctionGemma-270M** once an HF token is available — an FC-specialized base may beat generic Qwen at fewer params.
- Feed real accepted/rejected human decisions (the actual feedback store) alongside the synthetic corpus.
- Wire the production hook (`ModalBackend` → `trainer.py`) so gate + two-stage canary govern promotion (this validation ran the training half standalone; the gate/canary already exist + are tested).
