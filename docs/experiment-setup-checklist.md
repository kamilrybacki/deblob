# Deblob Experiment — Your Setup Checklist

What *you* need to provide before the comparative experiment (deterministic vs SLM vs continual, across models) can run end-to-end. Everything code-side is built; this is accounts, keys, and weights.

Legend: 🟢 free / no account · 🟡 account needed · 🔴 decision or cost needed.

## 1. Corpora — 🟢 nothing to do
- **Synthetic** corpus is generated in-repo (deterministic). Works today, no setup.
- **GitHub Archive** (gharchive.org) and **Wikimedia EventStreams** are public, no account — the loaders parse committed fixtures for tests; for a real run the manifests can `wget` a sample. No credentials.

## 2. Inference model weights — 🟡 mostly automatic
Model endpoints run **on the k3s workers, CPU-only** (no GPU needed for inference):
- **Ollama** (Granite 3.1-MoE 1B, Qwen2.5 1.5B) — pulls weights automatically on first run. Nothing to do. 🟢
- **llama.cpp** (FunctionGemma 270M GGUF) — needs the GGUF file provisioned (HF download, public). Init-container/PVC per `deploy/experiment/README.md`. 🟡 (public, no token if the repo is open)
- **Cactus / Needle 26M** — 🔴 **the one real blocker.** No verified Linux container image exists (Cactus ships aarch64 wheels + a source-built CLI). Your call:
  - **(a)** build a Cactus Linux image yourself (most work), or
  - **(b)** run Needle through its JAX repo as a separate diagnostic (spec already labels Needle's path `needle-custom`, distinct from the LoRA models), or
  - **(c)** drop Needle from the roster and run the experiment on Granite/Qwen/FunctionGemma (fully supported today).
  - Recommendation: start with **(c)**, add Needle later via (a)/(b) — it doesn't block anything else.

## 3. Remote fine-tune (arm C only) — 🟡 one account + one token
Arm C's retrain round is the **only** thing that leaves the cluster (workers have no GPU; confirmed). It's a provider-neutral `TrainingJob`, default backend **HF Jobs**:
- **Hugging Face account + Pro subscription** — HF Jobs requires Pro (~$9/mo). 🔴 (small recurring cost)
- **HF write token** — generate at huggingface.co/settings/tokens, scope: write. 🟡
- **An HF model repo** (private) for the trained adapter artifacts. 🟡
- **Cost:** ~$0.067 per retrain round on a T4; **100 rounds ≈ $6.67**. A hard **$0.50/round budget ceiling is enforced in code** before any job is submitted.
- Only the arms A/B (deterministic + zero-shot SLM) need *none* of this — they run fully on-cluster today.

### Optional alternatives (not required for the default path)
- **Modal** (fallback backend) — 🟡 account, ~$30/mo starter credit covers many rounds. Add later, config-swap, no code change.
- **Together / Fireworks** (managed fine-tune API) — 🔴 only worth it for a *Qwen-only* per-model experiment; **$4/job minimum makes them ~40–400× costlier than raw GPU** for these tiny models, and they can't cover Granite MoE or Needle. Skip unless you specifically want the Qwen managed comparison.

## 4. Secrets to create — 🟡
Create the k8s Secret from the template (real file is gitignored):
- `deploy/experiment/35-experiment-secret.example.yaml` → fill `HF_TOKEN` (+ any optional API keys) → apply as the real Secret, or route through Vault→sops→k8s per the homelab pattern.
- **Never** put tokens in the ConfigMap or images — the manifests read them env-from-Secret only.

## 5. Repo push — 🟡 your go-ahead
- Remote `github.com/kamilrybacki/deblob.git` is noted but **not yet configured locally**. Say the word and I'll add it + push `main` and the `deblob-experiment` branch (or open a PR).

## Minimum to run something real, today
Arms **A0/A1/B0/B1/B2** on the **synthetic + GitHub/Wikimedia** corpora with **Granite + Qwen + FunctionGemma**, all on-cluster, need only step 2 (weights, mostly automatic). That already produces the headline **risk-vs-coverage at zero-false-merge** plot + the **B2 redundancy verdict** + the four-layer breakdown — i.e. the "does the SLM lane earn its keep" answer — **without any paid account**.

Add step 3 (HF Pro + token) only when you want the **arm-C continual-learning trajectory** with real fine-tuning.
