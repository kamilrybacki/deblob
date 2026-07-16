"""One-shot Modal validation of Deblob's Arm-C enhanced-SLM flow:
real LoRA fine-tune on a T4, then base-vs-fine-tuned on a family-held-out
test split. Proves the continual-learning loop actually improves the
model at Deblob's 3-way decision task (not the production hook — that's
trainer.py; this is the "does real training help" proof).

Run: modal run --env=arm-c modal_arm_c_validate.py
Data passed in from the local entrypoint (train/test JSONL lines).
"""

import json

import modal

app = modal.App("deblob-arm-c-validate")

IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*",
    "transformers==4.44.*",
    "peft==0.12.*",
    "accelerate==0.33.*",
    "huggingface_hub==0.24.*",
)
CACHE = modal.Volume.from_name("deblob-base-models", create_if_missing=True)
REPO = "Qwen/Qwen2.5-0.5B-Instruct"


def _build_text(prompt: str, gold) -> str:
    return f"{prompt}\n{json.dumps(gold, sort_keys=True)}"


def _score(model, tok, test_lines) -> dict:
    """Greedy-generate a completion per test prompt. Measures, against
    EXTERNAL corpus gold (never the model's own predicate):
      - parse_rate: output is valid tool-call JSON with a `decision`
      - decision_match: predicted `decision` == gold (3-way classify)
      - decision_relation_match: `decision`+`relation` both correct
        (the semantically meaningful call; ignores the opaque schema_id
        hash the model cannot memorize)
      - exact_match: full {decision,relation,schema_id} byte-exact (strict)
    """
    import torch

    model.eval()
    parsed = decision_ok = decrel_ok = exact_ok = 0
    for line in test_lines:
        prompt = line["prompt"]
        gold = line["gold_tool_call"]
        inputs = tok(prompt + "\n", return_tensors="pt", truncation=True, max_length=1536).to(
            model.device
        )
        with torch.no_grad():
            out = model.generate(
                **inputs, max_new_tokens=96, do_sample=False, pad_token_id=tok.pad_token_id
            )
        gen = tok.decode(out[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True).strip()
        start = gen.find("{")
        end = gen.rfind("}")
        if start == -1 or end == -1 or end <= start:
            continue
        try:
            obj = json.loads(gen[start : end + 1])
        except Exception:
            continue
        if not isinstance(obj, dict) or "decision" not in obj:
            continue
        parsed += 1
        if obj.get("decision") == gold.get("decision"):
            decision_ok += 1
            if obj.get("relation") == gold.get("relation"):
                decrel_ok += 1
        if json.dumps(obj, sort_keys=True) == json.dumps(gold, sort_keys=True):
            exact_ok += 1
    n = len(test_lines)
    r = lambda x: round(x / n, 4) if n else 0.0
    return {
        "n": n,
        "parse_rate": r(parsed),
        "decision_match": r(decision_ok),
        "decision_relation_match": r(decrel_ok),
        "exact_match": r(exact_ok),
    }


@app.function(image=IMAGE, gpu="T4", volumes={"/cache": CACHE}, timeout=60 * 45)
def train_and_eval(train_lines: list, test_lines: list, seed: int = 7) -> dict:
    import os

    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    import torch
    from peft import LoraConfig, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer

    torch.manual_seed(seed)
    tok = AutoTokenizer.from_pretrained(REPO, cache_dir="/cache/hf-cache")
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token

    def fresh_base():
        return AutoModelForCausalLM.from_pretrained(
            REPO, cache_dir="/cache/hf-cache", torch_dtype=torch.bfloat16
        ).to("cuda")

    # --- BEFORE: base model on held-out ---
    base = fresh_base()
    before = _score(base, tok, test_lines)
    del base
    torch.cuda.empty_cache()

    # --- TRAIN: LoRA on the train split ---
    trainable = fresh_base()
    trainable.config.use_cache = False
    trainable.gradient_checkpointing_enable()
    model = get_peft_model(
        trainable,
        LoraConfig(r=16, lora_alpha=32, lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"),
    )
    model.enable_input_require_grads()
    texts = [_build_text(l["prompt"], l["gold_tool_call"]) for l in train_lines]
    enc = tok(texts, truncation=True, max_length=512, padding="max_length", return_tensors="pt")
    ds = torch.utils.data.TensorDataset(enc["input_ids"], enc["attention_mask"])
    loader = torch.utils.data.DataLoader(ds, batch_size=1, shuffle=True)
    opt = torch.optim.AdamW(model.parameters(), lr=2e-4)
    model.train()
    losses = []
    for epoch in range(4):
        for input_ids, attn in loader:
            input_ids = input_ids.to("cuda")
            attn = attn.to("cuda")
            out = model(input_ids=input_ids, attention_mask=attn, labels=input_ids)
            out.loss.backward()
            opt.step()
            opt.zero_grad()
            losses.append(float(out.loss.detach().cpu()))

    # --- AFTER: fine-tuned model on the SAME held-out ---
    after = _score(model, tok, test_lines)

    return {
        "model": REPO,
        "train_n": len(train_lines),
        "test_n": len(test_lines),
        "epochs": 4,
        "lora": {"r": 16, "alpha": 32},
        "final_loss": round(sum(losses[-10:]) / max(1, len(losses[-10:])), 4),
        "before": before,
        "after": after,
        "delta": {
            k: round(after[k] - before[k], 4)
            for k in ("parse_rate", "decision_match", "decision_relation_match", "exact_match")
        },
    }


@app.local_entrypoint()
def main(jsonl: str):
    train_lines, test_lines = [], []
    with open(jsonl) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            (train_lines if o.get("partition") == "train" else test_lines).append(o)
    print(f"train={len(train_lines)} test={len(test_lines)} — launching T4 round...")
    result = train_and_eval.remote(train_lines, test_lines)
    print("=== ARM-C VALIDATION RESULT ===")
    print(json.dumps(result, indent=2))
