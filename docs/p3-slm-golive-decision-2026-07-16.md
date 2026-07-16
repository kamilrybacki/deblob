# P3 — SLM Go-Live Decision (evidence-gated)

Date: 2026-07-16. Endpoint: ollama on k3s worker (no GPU). Eval: deblob-eval over the
25-case golden corpus. Gate: docs/shadow-golive-gate.md (human-reviewed; zero false merges = hard gate).
Raw report: docs/p3-slm-eval-2026-07-16.txt.

## Decision: NO-GO for live SLM (on zero-shot small models)

Evaluated granite3.1-moe:1b (zero-shot, no Deblob fine-tune). Across retrieval k=1/3/5:

| metric | k=1 | k=3 | k=5 | gate |
|---|---|---|---|---|
| False-merge (HARD GATE) | 0% | 0% | 0% | 0 ✅ |
| JSON parse / schema-valid | 4% | 64% | 100% | ~100% |
| Wrong-valid | 4%* | 56% | 88% | ≤0.5% ❌ |
| Decision-choice accuracy | 28% | 28% | 28% | high ❌ |
| False-split | 100% | 100% | 100% | low ❌(repairable) |
| Retrieval recall@3 | — | 92.3% | 92.3% | ≥95% ⚠️ (deterministic, not the model) |

(*k=1 wrong-valid low only because almost nothing parsed.)

## Findings
1. **Zero-shot tiny models fail the semantic-authority gate.** They emit well-formed tool-calls
   but pick the WRONG decision ~88% of the time (decision accuracy 28%). Exactly as the design
   anticipated ("expect eventual fine-tune on Deblob decisions/hard-negatives").
2. **The safety architecture HOLDS even with a bad model.** False-merge = 0% across ALL configs —
   the deterministic guards (is_accepted_match(IncompatibleSimilarity=false), bias-false-split-over-
   false-merge, top-k id constraint) prevent a wrong model from corrupting identity. Worst case is
   reduced coverage (false-splits), which is repairable. This validates the shadow-lane design.
3. **Retrieval (deterministic, structural) is fine** — recall@3 92.3%. The bottleneck is the model's
   judgment, NOT the candidate set. No embeddings needed yet.
4. **Infra + eval harness + endpoint work end-to-end** (proven live on k3s).
5. **CPU latency:** granite-2b dense is impractically slow on CPU (>2min/req); granite-moe-1b is
   fast (~2.3s warmup) — MoE/tiny is the right class for the homelab (no GPU).

## Path to GO (per the design + Kamil's Cactus Needle pointer)
- **Fine-tune a tiny FC-native model on Deblob's decision corpus + hard-negatives**, then re-eval
  against the gate. Candidates: **Cactus Needle (26M, function-call-native, CPU-milliseconds,
  finetunable, MIT)** — Kamil's suggestion, a strong fit; or FunctionGemma-270M (design Tier-1).
- Needle integration needs the deferred `LocalInferencer` (in-proc llama.cpp/GGUF) or an
  OpenAI-shim — it has no /v1 server out of the box.
- Keep the SLM lane SHADOW-only until a fine-tuned model clears: schema-valid ~100%, wrong-valid
  ≤0.5%, zero false merges, false-split materially reduced, recall@3 ≥95%, injection-resistant.

## Bottom line
The relay is unblocked (14→598 rec/s), retrieval + safety guards are solid, and the eval pipeline
is proven — but NO zero-shot model clears semantic authority. P3 go-live is gated on a Deblob
fine-tune of a tiny FC model (Needle/FunctionGemma). Shadow stays shadow.
