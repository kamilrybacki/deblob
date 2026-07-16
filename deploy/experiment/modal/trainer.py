"""Modal-side LoRA trainer for Deblob's arm-C continual-learning fine-tune
hook (spec §7/§8; Rust seam: `crates/deblob-experiment/src/continual/
training_job/modal.rs`'s `ModalBackend`).

Deploy-side Python only — NOT compiled by cargo, NOT part of any Rust
crate's build. `python3 -m py_compile` is this file's own gate (see the
Task 6 report). Deploy with `modal deploy deploy/experiment/modal/
trainer.py` from an environment with `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET`
set (headless — `modal deploy` reads the same two env vars the Rust side
does; no browser login flow in this path).

## Wire contract with `ModalBackend` (Rust)

Two HTTP routes, both behind Modal's own **proxy-auth** gate
(`requires_proxy_auth=True` below) — Modal itself validates the
`Modal-Key`/`Modal-Secret` headers `ModalBackend` sends (sourced from
`MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET` on the Rust side); this file never
re-implements that check.

- `POST /submit` — body is `ModalTrainingRequestBody` (Rust), a flat JSON
  object: `base_bundle_digest`, `dataset_digest`, `feedback_cutoff`,
  `trainer_image_digest`, `method` (`"lora-sft"` | `"needle-custom"` |
  other — the WIRE STRING, never a Rust-enum tag shape), `lora`
  (`{rank, alpha, learning_rate, epochs}`), `replay_manifest_digest`,
  `seed`, `budget_max_usd`, `budget_max_runtime_minutes`, `output_uri`,
  `cached_image_tag`, `cached_volume_name`. Spawns [`train_lora`]
  asynchronously (`.spawn()`, never `.remote()` — submit must return
  immediately) and responds `{"job_id": "<modal FunctionCall object_id>"}`.
- `GET /status/{job_id}` — polls the spawned call
  (`modal.FunctionCall.from_id(job_id).get(timeout=0)`) and responds
  `{"status": "running"}` | `{"status": "done", "artifact_digests":
  {...}}` | `{"status": "failed", "reason": "..."}`. NEVER returns raw
  weights — digests only (separation of duties: promotion stays in
  Deblob's `model_registry`, this trainer's job ends at "trained +
  uploaded").

## KNOWN GAP — how the trainer finds its actual training DATA

`TrainingJobSpec`'s wire fields (spec §8's literal list, reused verbatim
by every backend — `FakeBackend`/`HfJobsBackend`/`ModalBackend` alike) are
digests/strings, never raw bytes: `dataset_digest`/
`replay_manifest_digest` name the exported replay JSONL's content hash,
they do not CARRY it. `HfJobsBackend` has the identical gap (see its own
module docs: "wiring a real endpoint is a deploy-time concern"). This
trainer resolves both the replay JSONL and the base model bundle from a
small content-addressed MANIFEST expected on the shared `base_model_cache`
Volume (`/cache/manifest.json`, see [`resolve_manifest_entry`]) — populated
out-of-band by whatever deploy step exports `ReplaySet::to_jsonl()`/builds
each `ModelBundle` today. Filling that manifest-population step in is
explicitly OUT OF SCOPE here (same posture as the Task 4 report's own
disclosed gap #2) — this file fails loudly (never silently) if a digest
isn't found.

## Needle caveat (spec §8, task ask: do NOT claim LoRA parity for Needle)

`method == "needle-custom"` is a SEPARATE path (JAX/CUDA, not PEFT/TRL
LoRA) that this trainer does not implement — [`train_lora`] raises
immediately with a clearly labeled error rather than silently running a
LoRA pass against a model family it was never validated against. See
[`NEEDLE_CUSTOM_METHOD`]'s docstring.

## Caching / spend-cap (Hermes' caveat: cold starts are billed)

- `TRAINER_IMAGE` is a single pinned, cached Modal Image — reused across
  every job instead of rebuilt per submit (image builds are billed compute
  time). `cached_image_tag` on the wire body is an AUDIT TRAIL of which
  pin a given job ran against, not something this file re-resolves.
- `base_model_cache` is a persistent named `modal.Volume` — base-model
  weights download ONCE and are reused by every subsequent cold start
  instead of re-downloaded per round (the single biggest avoidable cost on
  a pay-per-second T4).
- `budget_max_usd`/`budget_max_runtime_minutes` are the SAME ceiling
  `ModalBackend::submit` already enforced before this job was ever spawned
  (Rust-side `validate_budget`) — `budget_max_runtime_minutes` is ALSO
  checked here as a best-effort in-process wall-clock guard (see
  [`train_lora`]), since Modal's own per-function `timeout=` is fixed at
  deploy time, not settable per call.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import modal

# ---------------------------------------------------------------------
# App, image, volume — see the module docstring's caching/spend-cap note.
# ---------------------------------------------------------------------

APP_NAME = "deblob-experiment-trainer"
app = modal.App(APP_NAME)

# Pinned dependency set — bump deliberately, not implicitly, so
# `cached_image_tag` (the Rust-side audit trail) stays meaningful.
TRAINER_IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*",
    "transformers==4.44.*",
    "peft==0.12.*",
    "trl==0.9.*",
    "accelerate==0.33.*",
    "huggingface_hub==0.24.*",
    "fastapi==0.115.*",
)

# MUST match `ModalConfig.cached_volume_name` on the Rust side (see
# `crates/deblob-experiment/src/continual/training_job/modal.rs`) — kept
# as a literal here, not derived from the request body, so the SAME
# Volume backs every job regardless of what a caller passes.
BASE_MODEL_CACHE_VOLUME_NAME = "deblob-base-models"
base_model_cache = modal.Volume.from_name(
    BASE_MODEL_CACHE_VOLUME_NAME, create_if_missing=True
)
CACHE_MOUNT = "/cache"

# Optional — only needed if a base model repo is gated. Uncomment the
# `secrets=[...]` line on `train_lora` below once this secret exists
# (`modal secret create deblob-experiment-hf-token HF_TOKEN=...`).
# HF_READ_SECRET = modal.Secret.from_name("deblob-experiment-hf-token")

# ---------------------------------------------------------------------
# Spec §5 roster -> HF repo id. **VERIFY at deploy time** — these are
# best-effort mappings, not independently re-confirmed by this task (same
# disclosure posture as `deploy/experiment/22-model-cactus.yaml`'s own
# "UNRESOLVED CONCERN" callout). Keyed by the manifest entry's
# `model_key`, resolved via `resolve_manifest_entry` below — NOT parsed
# out of `base_bundle_digest` itself (an opaque content hash).
# ---------------------------------------------------------------------
LORA_MODEL_REPOS = {
    "granite-3.1-moe-1b": "ibm-granite/granite-3.1-1b-a400m-instruct",
    "qwen2.5-1.5b-instruct": "Qwen/Qwen2.5-1.5B-Instruct",
    "functiongemma-270m": "google/functiongemma-270m-it",
}

# The one training method this file refuses to run a LoRA pass for — see
# the module docstring's Needle caveat.
NEEDLE_CUSTOM_METHOD = "needle-custom"


class NeedleNotSupportedError(RuntimeError):
    """Raised immediately for `method == "needle-custom"` — Needle's own
    method is a SEPARATE JAX/CUDA path this trainer does not implement
    (task ask: never silently claim LoRA parity for it). A future Needle
    trainer is a distinct file/App, not a branch bolted onto this one."""


def resolve_manifest_entry(digest: str) -> dict[str, Any]:
    """Looks `digest` (a `base_bundle_digest` or `dataset_digest`) up in
    the content-addressed manifest at `{CACHE_MOUNT}/manifest.json` — see
    the module docstring's KNOWN GAP note. Raises `KeyError` (never
    silently substitutes a default) if the digest is unknown; a caller
    with a real bundle/dataset MUST have populated this manifest first.
    """
    manifest_path = Path(CACHE_MOUNT) / "manifest.json"
    if not manifest_path.exists():
        raise KeyError(
            f"no manifest at {manifest_path} — populate it out-of-band "
            f"before submitting a job (see trainer.py's KNOWN GAP note)"
        )
    manifest = json.loads(manifest_path.read_text())
    try:
        return manifest[digest]
    except KeyError as e:
        raise KeyError(f"digest {digest!r} not found in {manifest_path}") from e


def sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def load_replay_lines(dataset_digest: str) -> list[dict[str, Any]]:
    """Resolves + reads the exported replay JSONL (`ReplaySet::to_jsonl`'s
    output — one `{case_name, partition, prompt, gold_tool_call,
    replay_stratum}` object per line, see `deblob_eval::generate::
    render_finetune_jsonl`'s doc) via the same manifest as the base model.
    """
    entry = resolve_manifest_entry(dataset_digest)
    jsonl_path = Path(entry["local_path"])
    lines = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                lines.append(json.loads(line))
    return lines


def sanity_check(model, tokenizer, replay_lines: list[dict[str, Any]]) -> dict[str, float]:
    """Cheap post-training smoke check — NOT a substitute for Deblob's own
    gate/canary (`deblob::model_registry`), which is the real evaluation
    and runs entirely on the Deblob side after this trainer returns
    digests. This only catches "training obviously broke the model"
    (NaN/inf loss, or the tokenizer/model failing to run at all) before
    spending time uploading a broken artifact.
    """
    import torch

    sample = replay_lines[: min(8, len(replay_lines))]
    if not sample:
        return {"sanity_checked_examples": 0}

    total_loss = 0.0
    model.eval()
    with torch.no_grad():
        for line in sample:
            prompt = line["prompt"]
            target = json.dumps(line["gold_tool_call"], sort_keys=True)
            text = f"{prompt}\n{target}"
            inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=1024).to(
                model.device
            )
            outputs = model(**inputs, labels=inputs["input_ids"])
            loss_value = float(outputs.loss.detach().cpu())
            if not (loss_value == loss_value) or loss_value in (float("inf"), float("-inf")):
                raise RuntimeError(
                    f"sanity check failed: non-finite loss ({loss_value}) on "
                    f"case {line.get('case_name', '?')!r} — refusing to upload"
                )
            total_loss += loss_value
    return {
        "sanity_checked_examples": len(sample),
        "sanity_mean_loss": total_loss / len(sample),
    }


def upload_artifacts(local_dir: Path, output_uri: str) -> None:
    """Copies everything under `local_dir` to `output_uri` (external
    storage) — task ask: "artifacts persisted to output_uri, not left on
    the ephemeral container." Supports `s3://` (via `boto3`, imported
    lazily so a non-S3 deploy never needs it installed) today; any other
    scheme is a documented TODO, never a silent no-op.
    """
    if output_uri.startswith("s3://"):
        import boto3

        bucket, _, prefix = output_uri[len("s3://") :].partition("/")
        s3 = boto3.client("s3")
        for path in local_dir.rglob("*"):
            if path.is_file():
                key = f"{prefix}/{path.relative_to(local_dir)}".lstrip("/")
                s3.upload_file(str(path), bucket, key)
        return
    raise NotImplementedError(
        f"upload_artifacts: unsupported output_uri scheme in {output_uri!r} "
        "— add a branch here before using a non-s3:// output_uri"
    )


@app.function(
    image=TRAINER_IMAGE,
    gpu="T4",
    volumes={CACHE_MOUNT: base_model_cache},
    timeout=60 * 60,  # 1h hard ceiling; see the in-function soft check below
    # secrets=[HF_READ_SECRET],  # uncomment once the base model is gated
)
def train_lora(request: dict[str, Any]) -> dict[str, Any]:
    """The actual training job — spawned (never called synchronously) by
    the `/submit` web route below. `request` is `ModalTrainingRequestBody`
    (Rust) as JSON. Returns `{"artifact_digests": {"training_checkpoint":
    "...", "quantized_weights": "..."}, "metrics": {...}}` — digests only,
    never raw weights (separation of duties, see module docstring).
    """
    import torch
    from peft import LoraConfig, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer

    started_at = time.monotonic()
    budget_seconds = float(request["budget_max_runtime_minutes"]) * 60.0

    method = request["method"]
    if method == NEEDLE_CUSTOM_METHOD:
        raise NeedleNotSupportedError(
            "method=needle-custom requires the SEPARATE JAX/CUDA Needle "
            "path (spec §8) — this LoRA/PEFT trainer does not implement "
            "it and makes no claim of parity; see trainer.py's module "
            "docstring."
        )

    base_entry = resolve_manifest_entry(request["base_bundle_digest"])
    model_key = base_entry["model_key"]
    if model_key not in LORA_MODEL_REPOS:
        raise KeyError(
            f"model_key {model_key!r} (from base_bundle_digest "
            f"{request['base_bundle_digest']!r}) is not in LORA_MODEL_REPOS "
            "— add it (and VERIFY the repo id) before training this family"
        )
    repo_id = LORA_MODEL_REPOS[model_key]

    replay_lines = load_replay_lines(request["dataset_digest"])
    if not replay_lines:
        raise RuntimeError(
            f"dataset_digest {request['dataset_digest']!r} resolved to an "
            "empty replay set — refusing to train on zero examples"
        )

    torch.manual_seed(request["seed"])

    tokenizer = AutoTokenizer.from_pretrained(
        repo_id, cache_dir=f"{CACHE_MOUNT}/hf-cache"
    )
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    base_model = AutoModelForCausalLM.from_pretrained(
        repo_id, cache_dir=f"{CACHE_MOUNT}/hf-cache", torch_dtype=torch.bfloat16
    ).to("cuda")

    lora_cfg = request["lora"]
    peft_config = LoraConfig(
        r=lora_cfg["rank"],
        lora_alpha=lora_cfg["alpha"],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(base_model, peft_config)

    texts = [
        f"{line['prompt']}\n{json.dumps(line['gold_tool_call'], sort_keys=True)}"
        for line in replay_lines
    ]
    encodings = tokenizer(
        texts, truncation=True, max_length=1024, padding=True, return_tensors="pt"
    )
    dataset = torch.utils.data.TensorDataset(
        encodings["input_ids"], encodings["attention_mask"]
    )
    loader = torch.utils.data.DataLoader(dataset, batch_size=2, shuffle=True)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lora_cfg["learning_rate"])
    model.train()
    for _epoch in range(lora_cfg["epochs"]):
        for input_ids, attention_mask in loader:
            if time.monotonic() - started_at > budget_seconds:
                raise RuntimeError(
                    f"training exceeded its {request['budget_max_runtime_minutes']}"
                    "-minute budget — aborting rather than overrunning the "
                    "spend cap (see module docstring's spend-cap note)"
                )
            input_ids = input_ids.to("cuda")
            attention_mask = attention_mask.to("cuda")
            outputs = model(
                input_ids=input_ids, attention_mask=attention_mask, labels=input_ids
            )
            outputs.loss.backward()
            optimizer.step()
            optimizer.zero_grad()

    metrics = sanity_check(model, tokenizer, replay_lines)

    local_dir = Path(tempfile.mkdtemp(prefix="deblob-modal-artifact-"))
    try:
        adapter_dir = local_dir / "adapter"
        model.save_pretrained(str(adapter_dir))
        tokenizer.save_pretrained(str(adapter_dir))
        (local_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "base_bundle_digest": request["base_bundle_digest"],
                    "dataset_digest": request["dataset_digest"],
                    "replay_manifest_digest": request["replay_manifest_digest"],
                    "method": method,
                    "seed": request["seed"],
                    "lora": lora_cfg,
                    "metrics": metrics,
                },
                sort_keys=True,
            )
        )

        adapter_files = sorted(p for p in adapter_dir.rglob("*") if p.is_file())
        training_checkpoint_digest = hashlib.sha256(
            b"".join(sha256_of_file(p).encode() for p in adapter_files)
        ).hexdigest()
        training_checkpoint_digest = f"sha256:{training_checkpoint_digest}"

        # NOTE — simplification, disclosed: no real quantization pass runs
        # yet (would need e.g. llama.cpp's `convert_lora_to_gguf.py` +
        # `quantize`, not bundled in `TRAINER_IMAGE` today). This digest
        # is computed over the SAME adapter files as a placeholder so the
        # `QUANTIZED_WEIGHTS_KEY` the Rust pipeline reads is always
        # populated — wiring real quantization later does not change the
        # contract, only this one line.
        quantized_weights_digest = training_checkpoint_digest.replace(
            "sha256:", "sha256:quant-", 1
        )

        upload_artifacts(local_dir, request["output_uri"])
    finally:
        # Never leave the trained artifact on the ephemeral container.
        shutil.rmtree(local_dir, ignore_errors=True)

    return {
        "artifact_digests": {
            "training_checkpoint": training_checkpoint_digest,
            "quantized_weights": quantized_weights_digest,
        },
        "metrics": metrics,
    }


@app.function(image=TRAINER_IMAGE)
@modal.asgi_app(requires_proxy_auth=True)
def web():
    """The `/submit` + `/status/{job_id}` routes `ModalBackend` (Rust)
    talks to. `requires_proxy_auth=True` makes Modal itself enforce the
    `Modal-Key`/`Modal-Secret` header pair on every request to this app —
    no custom auth check needed here, and no way to reach either route
    without the SAME token pair `ModalCredentials::from_env` (Rust) reads
    from `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET`.
    """
    from fastapi import FastAPI, HTTPException, Request

    web_app = FastAPI()

    @web_app.post("/submit")
    async def submit(request: Request) -> dict[str, str]:
        body = await request.json()
        try:
            call = train_lora.spawn(body)
        except Exception as e:  # noqa: BLE001 — surfaced to the caller, never swallowed
            raise HTTPException(status_code=400, detail=str(e)) from e
        return {"job_id": call.object_id}

    @web_app.get("/status/{job_id}")
    async def status(job_id: str) -> dict[str, Any]:
        try:
            call = modal.functions.FunctionCall.from_id(job_id)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=404, detail=f"unknown job_id: {e}") from e
        try:
            result = call.get(timeout=0)
        except TimeoutError:
            return {"status": "running"}
        except Exception as e:  # noqa: BLE001 — a failed remote job, not a bug here
            return {"status": "failed", "reason": str(e)}
        return {"status": "done", "artifact_digests": result["artifact_digests"]}

    return web_app
