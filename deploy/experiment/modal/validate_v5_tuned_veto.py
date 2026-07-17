"""Arm-C fine-tune v3 — adds Hermes' key lever: FINITE HYPOTHESIS SCORING.
Same corrected training as v2 (completion-only loss + balanced sampling),
but at inference, instead of free generation, we enumerate every LEGAL
completion (match x relation x allowed-id; new_candidate x novelty;
abstain x cause), score each by length-normalized conditional log-likelihood,
and pick the argmax. Reports free-gen AND finite-scoring side by side, plus
a margin-based abstention knob. This is a bounded classification task; finite
scoring is far more stable than autoregressive generation for a 0.5B.

Run: modal run --env=arm-c modal_arm_c_v3.py --jsonl <finetune.jsonl>
"""

import json
import re

import modal

app = modal.App("deblob-arm-c-v5")

IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*", "transformers==4.44.*", "peft==0.12.*",
    "accelerate==0.33.*", "huggingface_hub==0.24.*",
)
CACHE = modal.Volume.from_name("deblob-base-models", create_if_missing=True)
REPO = "Qwen/Qwen2.5-0.5B-Instruct"

RELATIONS = ["exact", "compatible_drift", "incompatible_similarity"]
NOVELTIES = ["structural", "semantic"]
CAUSES = ["insufficient_evidence", "ambiguous", "candidate_missing"]


def _decision(line):
    return line["gold_tool_call"].get("decision", "?")


def _allowed_ids(prompt):
    m = re.search(r"ALLOWED schema_id SET[^\n]*\n\[([^\]]*)\]", prompt)
    if not m:
        return []
    return [x.strip() for x in m.group(1).split(",") if x.strip()]


def _legal_completions(prompt):
    """Every canonical legal tool-call for this case (Hermes' finite set)."""
    ids = _allowed_ids(prompt)
    comps = []
    for sid in ids:
        for rel in RELATIONS:
            comps.append({"decision": "match_schema", "relation": rel, "schema_id": sid})
    for nov in NOVELTIES:
        comps.append({"decision": "new_candidate", "novelty": nov})
    for cause in CAUSES:
        comps.append({"cause": cause, "decision": "abstain"})
    return comps


def _balance(train_lines):
    from collections import defaultdict

    by = defaultdict(list)
    for l in train_lines:
        by[_decision(l)].append(l)
    target = max(len(v) for v in by.values())
    out = []
    dist = {}
    for d, rows in by.items():
        reps = min(4, max(1, round(target / len(rows))))
        out.extend(rows * reps)
        dist[d] = len(rows) * reps
    return out, dist


def _top_distances(prompt):
    """Parse rank1/rank2 structural distances from the prompt (gate signal)."""
    ds = [float(m) for m in re.findall(r"distance=([0-9.]+)", prompt)]
    d1 = ds[0] if ds else 1.0
    d2 = ds[1] if len(ds) > 1 else 1.0
    return d1, d2


# Gate thresholds (mirror crates/deblob policy): distance<=0.15, margin>=0.10
POLICY_MAX_DISTANCE = 0.15
POLICY_MIN_MARGIN = 0.10
NEW_VETO_DISTANCE = 0.40  # only force new_candidate when TRULY far (drift/false-split reach ~0.28)


def _score_free(model, tok, test_lines):
    import torch
    from collections import Counter

    model.eval()
    ok = Counter(); tot = Counter()
    for l in test_lines:
        gd = l["gold_tool_call"].get("decision"); tot[gd] += 1
        inp = tok(l["prompt"] + "\n", return_tensors="pt", truncation=True, max_length=1536).to(model.device)
        with torch.no_grad():
            out = model.generate(**inp, max_new_tokens=48, do_sample=False, pad_token_id=tok.pad_token_id)
        gen = tok.decode(out[0][inp["input_ids"].shape[1]:], skip_special_tokens=True)
        s, e = gen.find("{"), gen.rfind("}")
        pd = None
        if s != -1 and e > s:
            try:
                pd = json.loads(gen[s:e + 1]).get("decision")
            except Exception:
                pd = None
        if pd == gd:
            ok[gd] += 1
    return {"decision_match": round(sum(ok.values()) / len(test_lines), 3),
            "recall": {d: round(ok[d] / tot[d], 3) for d in tot}}


def _seq_logprob(model, tok, prompt, completion):
    import torch

    p_ids = tok(prompt + "\n", add_special_tokens=False)["input_ids"]
    c_ids = tok(json.dumps(completion, sort_keys=True), add_special_tokens=False)["input_ids"]
    ids = torch.tensor([(p_ids + c_ids)[-1600:]], device=model.device)
    with torch.no_grad():
        logits = model(ids).logits[0]
    logp = torch.log_softmax(logits, dim=-1)
    # sum log-prob of the completion tokens (positions after the prompt)
    start = len(ids[0]) - len(c_ids)
    total = 0.0
    for j, tokid in enumerate(c_ids):
        pos = start + j - 1
        if pos < 0:
            continue
        total += float(logp[pos, tokid])
    return total / max(1, len(c_ids))  # length-normalized


def _score_finite(model, tok, test_lines, margin_abstain=0.0, collect=False):
    from collections import Counter

    model.eval()
    ok = Counter(); tot = Counter()
    schema_ok = 0; match_tot = 0
    examples = []
    for l in test_lines:
        gold = l["gold_tool_call"]; gd = gold.get("decision"); tot[gd] += 1
        if gd == "match_schema":
            match_tot += 1
        comps = _legal_completions(l["prompt"])
        if not comps:
            continue
        scored = sorted(((c, _seq_logprob(model, tok, l["prompt"], c)) for c in comps),
                        key=lambda x: x[1], reverse=True)
        best, best_s = scored[0]
        second_s = scored[1][1] if len(scored) > 1 else -1e9
        pred = best
        # DETERMINISTIC vetoes over the SLM proposal (Hermes: safety outside model).
        d1, d2 = _top_distances(l["prompt"])
        if d1 > NEW_VETO_DISTANCE:
            # nearest known schema FAR beyond even drift range -> genuinely new
            pred = {"decision": "new_candidate", "novelty": "structural"}
        elif d1 <= POLICY_MAX_DISTANCE and (d2 - d1) < POLICY_MIN_MARGIN and best.get("decision") == "match_schema":
            # in the confident-match zone but two candidates near-equidistant -> ambiguous
            pred = {"decision": "abstain", "cause": "ambiguous"}
        # else: 0.15 < d1 <= 0.40 middle zone -> trust the SLM finite-scoring pick
        if pred.get("decision") == gd:
            ok[gd] += 1
            if gd == "match_schema" and pred.get("schema_id") == gold.get("schema_id"):
                schema_ok += 1
        if collect:
            examples.append({"case": l.get("case_name"), "gold": gold, "pred": pred,
                             "margin": round(best_s - second_s, 3)})
    n = len(test_lines)
    return ({"decision_match": round(sum(ok.values()) / n, 3),
             "recall": {d: round(ok[d] / tot[d], 3) for d in tot},
             "match_schema_id_correct": round(schema_ok / match_tot, 3) if match_tot else 0.0,
             "support": dict(tot)}, examples)


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
    base = AutoModelForCausalLM.from_pretrained(
        REPO, cache_dir="/cache/hf-cache", torch_dtype=torch.bfloat16).to("cuda")
    base.config.use_cache = False
    base.gradient_checkpointing_enable()
    model = get_peft_model(base, LoraConfig(
        r=rank, lora_alpha=rank * 2, lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"))
    model.enable_input_require_grads()

    bal, bal_dist = _balance(train_lines)
    built = []
    for l in bal:
        p_ids = tok(l["prompt"] + "\n", add_special_tokens=False)["input_ids"]
        c_ids = tok(json.dumps(l["gold_tool_call"], sort_keys=True) + tok.eos_token,
                    add_special_tokens=False)["input_ids"]
        ids = (p_ids + c_ids)[:640]
        labels = ([-100] * len(p_ids) + c_ids)[:640]
        if any(x != -100 for x in labels):
            built.append((ids, labels))

    opt = torch.optim.AdamW(model.parameters(), lr=2e-4)
    model.train()
    losses = []
    for ep in range(epochs):
        g = torch.Generator().manual_seed(seed + ep)
        for i in torch.randperm(len(built), generator=g).tolist():
            ids, labels = built[i]
            out = model(input_ids=torch.tensor([ids], device="cuda"),
                        labels=torch.tensor([labels], device="cuda"))
            out.loss.backward(); opt.step(); opt.zero_grad()
            losses.append(float(out.loss.detach().cpu()))

    finite, examples = _score_finite(model, tok, test_lines, margin_abstain=0.0, collect=True)
    return {
        "model": REPO, "epochs": epochs, "rank": rank,
        "balanced_dist": bal_dist, "final_loss": round(sum(losses[-20:]) / 20, 4),
        "eval_finite_scoring": finite,
        "examples": examples,
    }


@app.local_entrypoint()
def main(jsonl: str, epochs: int = 3, rank: int = 16):
    tr, te = [], []
    for line in open(jsonl):
        line = line.strip()
        if line:
            o = json.loads(line)
            (tr if o.get("partition") == "train" else te).append(o)
    te = te[:120]
    print(f"train={len(tr)} test={len(te)} epochs={epochs} — launching v5 (tuned vetoes: new>0.40, abstain-on-tie in match zone)...")
    r = train_and_eval.remote(tr, te, epochs=epochs, rank=rank)
    with open(jsonl.replace(".jsonl", "") + "_v5_result.json", "w") as f:
        json.dump(r, f, indent=2)
    print("=== V5 RESULT ===")
    print(json.dumps({k: v for k, v in r.items() if k != "examples"}, indent=2))
