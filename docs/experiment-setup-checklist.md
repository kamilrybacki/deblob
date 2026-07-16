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

## 3. Remote fine-tune (arm C only) — 🟡 one account + one token pair
Arm C's retrain round is the **only** thing that leaves the cluster (workers have no GPU; confirmed). It's a provider-neutral `TrainingJob`; **Modal is the chosen backend** — T4 GPUs + the free ~$30/mo starter credit is the cheapest *real*-training path (no paid subscription tier required):
- **Create a Modal account** — modal.com, free signup, no card required to start. 🟢
- **Generate a token pair** — modal.com → Settings → API Tokens → "New Token" gives you `MODAL_TOKEN_ID` + `MODAL_TOKEN_SECRET`. Headless: the SAME pair authenticates both `modal deploy deploy/experiment/modal/trainer.py` (deploying the trainer app, one-time) and `ModalBackend`'s submit/poll calls at runtime — `ModalCredentials::from_env` reads exactly these two env vars, no browser login flow anywhere in the hook. 🟡
- **Set a spend cap** — modal.com → Settings → Billing → usage limit, so nothing can exceed the free credit even if the code-side guard were ever bypassed. This is IN ADDITION to (not instead of) the code-side `max_usd_ceiling`/`max_runtime_minutes` guard, which is enforced TWICE: once by the generic hook (`validate_budget`), and again inside `ModalBackend::submit` itself before any network call is made.
- **Deploy the trainer app once** — `modal deploy deploy/experiment/modal/trainer.py`; copy the printed web-endpoint URL into `30-experiment-config.yaml`'s `modal_endpoint_base`.
- **Cost:** T4 is billed per-second; the free credit covers a substantial number of rounds for these tiny (270M–1.5B param) models. `trainer.py`'s module docstring documents the image + base-model-weight caching that keeps cold-start cost down (the single biggest avoidable per-round cost).
- Only the arms A/B (deterministic + zero-shot SLM) need *none* of this — they run fully on-cluster today.

### Kept working (not the default path)
- **HF Jobs** (`HfJobsBackend`) — still config-selectable (`backend = "hfjobs"` in `30-experiment-config.yaml`) if you'd rather run on Hugging Face's infrastructure instead. Needs an HF account + Pro subscription (~$9/mo) + a write token; see the commented-out `hf_token_secret_ref`/`output_repo`/`hardware_flavor` keys in that same file and `HF_TOKEN` in the Secret template.
- **Together / Fireworks** (managed fine-tune API) — 🔴 only worth it for a *Qwen-only* per-model experiment; **$4/job minimum makes them ~40–400× costlier than raw GPU** for these tiny models, and they can't cover Granite MoE or Needle. Skip unless you specifically want the Qwen managed comparison.

## 4. Secrets to create — 🟡
Create the k8s Secret from the template (real file is gitignored):
- `deploy/experiment/35-experiment-secret.example.yaml` → fill `MODAL_TOKEN_ID` + `MODAL_TOKEN_SECRET` (and `HF_TOKEN` only if using the `hfjobs` fallback, + any other optional keys) → apply as the real Secret, or route through Vault→sops→k8s per the homelab pattern.
- **Never** put tokens in the ConfigMap or images — the manifests read them env-from-Secret only.

## 5. Repo push — 🟡 your go-ahead
- Remote `github.com/kamilrybacki/deblob.git` is noted but **not yet configured locally**. Say the word and I'll add it + push `main` and the `deblob-experiment` branch (or open a PR).

## Minimum to run something real, today
Arms **A0/A1/B0/B1/B2** on the **synthetic + GitHub/Wikimedia** corpora with **Granite + Qwen + FunctionGemma**, all on-cluster, need only step 2 (weights, mostly automatic). That already produces the headline **risk-vs-coverage at zero-false-merge** plot + the **B2 redundancy verdict** + the four-layer breakdown — i.e. the "does the SLM lane earn its keep" answer — **without any paid account**.

Add step 3 (a free Modal account + token pair) only when you want the **arm-C continual-learning trajectory** with real fine-tuning.
