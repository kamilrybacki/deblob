"""Arm-C fine-tune v2 — fixes for the mode-collapse found in v1.
  1. COMPLETION-ONLY loss (mask the prompt; loss only on the tool-call).
  2. BALANCED sampling across decision classes (match/new/abstain).
  3. Per-decision recall scoring (esp. new_candidate — v1 was 0/16).
The prompt already carries per-candidate `distance=` + an ALLOWED id set,
so the signal to detect "new" exists; v1 just never learned to use it.

Run: modal run --env=arm-c modal_arm_c_v2.py --jsonl <finetune.jsonl>
"""

import json

import modal

app = modal.App("deblob-arm-c-v2")

IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*", "transformers==4.44.*", "peft==0.12.*",
    "accelerate==0.33.*", "huggingface_hub==0.24.*",
)
CACHE = modal.Volume.from_name("deblob-base-models", create_if_missing=True)
REPO = "Qwen/Qwen2.5-0.5B-Instruct"


def _decision(line) -> str:
    return line["gold_tool_call"].get("decision", "?")


def _score(model, tok, test_lines, collect=False):
    import torch
    from collections import Counter

    model.eval()
    parsed = 0
    dec_ok = Counter()
    dec_tot = Counter()
    match_id_ok = 0
    match_tot = 0
    examples = []
    for line in test_lines:
        gold = line["gold_tool_call"]
        gd = gold.get("decision")
        dec_tot[gd] += 1
        if gd == "match_schema":
            match_tot += 1
        inputs = tok(line["prompt"] + "\n", return_tensors="pt", truncation=True,
                     max_length=1536).to(model.device)
        with torch.no_grad():
            out = model.generate(**inputs, max_new_tokens=64, do_sample=False,
                                 pad_token_id=tok.pad_token_id)
        gen = tok.decode(out[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True).strip()
        obj = None
        s, e = gen.find("{"), gen.rfind("}")
        if s != -1 and e > s:
            try:
                c = json.loads(gen[s:e + 1])
                if isinstance(c, dict) and "decision" in c:
                    obj = c
            except Exception:
                obj = None
        pd = obj.get("decision") if obj else None
        if obj:
            parsed += 1
        if pd == gd:
            dec_ok[gd] += 1
            if gd == "match_schema" and obj.get("schema_id") == gold.get("schema_id"):
                match_id_ok += 1
        if collect:
            examples.append({"case": line.get("case_name"), "gold": gold, "pred": obj})
    n = len(test_lines)
    rec = {d: round(dec_ok[d] / dec_tot[d], 3) for d in dec_tot}
    m = {
        "n": n,
        "parse_rate": round(parsed / n, 3),
        "decision_match": round(sum(dec_ok.values()) / n, 3),
        "recall_by_decision": rec,
        "match_schema_id_correct": round(match_id_ok / match_tot, 3) if match_tot else 0.0,
        "support": dict(dec_tot),
    }
    return (m, examples) if collect else (m, [])


def _balance(train_lines):
    """Oversample minority decision classes so the model can't collapse to
    the majority. Caps the multiplier so we don't overfit the minority."""
    from collections import defaultdict

    by = defaultdict(list)
    for l in train_lines:
        by[_decision(l)].append(l)
    target = max(len(v) for v in by.values())
    out = []
    for d, rows in by.items():
        reps = min(4, max(1, round(target / len(rows))))  # cap 4x
        out.extend(rows * reps)
    return out, {d: len(v) for d, v in by.items()}, {d: len(v) * min(4, max(1, round(target / len(v)))) for d, v in by.items()}


@app.function(image=IMAGE, gpu="T4", volumes={"/cache": CACHE}, timeout=60 * 45)
def train_and_eval(train_lines, test_lines, seed=7, epochs=4, rank=16):
    import os
    os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
    import torch
    from peft import LoraConfig, get_peft_model
    from transformers import AutoModelForCausalLM, AutoTokenizer

    torch.manual_seed(seed)
    tok = AutoTokenizer.from_pretrained(REPO, cache_dir="/cache/hf-cache")
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token

    def fresh():
        return AutoModelForCausalLM.from_pretrained(
            REPO, cache_dir="/cache/hf-cache", torch_dtype=torch.bfloat16).to("cuda")

    before, _ = _score(fresh(), tok, test_lines)
    torch.cuda.empty_cache()

    base = fresh()
    base.config.use_cache = False
    base.gradient_checkpointing_enable()
    model = get_peft_model(base, LoraConfig(
        r=rank, lora_alpha=rank * 2, lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"))
    model.enable_input_require_grads()

    bal, orig_dist, bal_dist = _balance(train_lines)

    # --- build COMPLETION-ONLY examples: mask everything before the tool-call ---
    MAXLEN = 640
    built = []
    for l in bal:
        prompt = l["prompt"] + "\n"
        completion = json.dumps(l["gold_tool_call"], sort_keys=True) + tok.eos_token
        p_ids = tok(prompt, add_special_tokens=False)["input_ids"]
        c_ids = tok(completion, add_special_tokens=False)["input_ids"]
        ids = (p_ids + c_ids)[:MAXLEN]
        labels = ([-100] * len(p_ids) + c_ids)[:MAXLEN]
        # ensure at least some completion tokens survive truncation
        if all(x == -100 for x in labels):
            continue
        built.append((ids, labels))

    opt = torch.optim.AdamW(model.parameters(), lr=2e-4)
    model.train()
    losses = []
    order = list(range(len(built)))
    for ep in range(epochs):
        g = torch.Generator().manual_seed(seed + ep)
        for i in torch.randperm(len(order), generator=g).tolist():
            ids, labels = built[i]
            input_ids = torch.tensor([ids], device="cuda")
            lab = torch.tensor([labels], device="cuda")
            out = model(input_ids=input_ids, labels=lab)
            out.loss.backward()
            opt.step()
            opt.zero_grad()
            losses.append(float(out.loss.detach().cpu()))

    after, examples = _score(model, tok, test_lines, collect=True)
    return {
        "model": REPO, "epochs": epochs, "rank": rank,
        "train_n": len(train_lines), "balanced_n": len(built), "test_n": len(test_lines),
        "orig_decision_dist": orig_dist, "balanced_decision_dist": bal_dist,
        "final_loss": round(sum(losses[-20:]) / max(1, len(losses[-20:])), 4),
        "before": before, "after": after, "examples": examples,
    }


@app.local_entrypoint()
def main(jsonl: str, epochs: int = 4, rank: int = 16):
    tr, te = [], []
    for line in open(jsonl):
        line = line.strip()
        if not line:
            continue
        o = json.loads(line)
        (tr if o.get("partition") == "train" else te).append(o)
    print(f"train={len(tr)} test={len(te)} epochs={epochs} rank={rank} — launching...")
    r = train_and_eval.remote(tr, te, epochs=epochs, rank=rank)
    with open(jsonl.replace(".jsonl", "") + "_v2_result.json", "w") as f:
        json.dump(r, f, indent=2)
    summary = {k: v for k, v in r.items() if k != "examples"}
    print("=== V2 RESULT ===")
    print(json.dumps(summary, indent=2))
