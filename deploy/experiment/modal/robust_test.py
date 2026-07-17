"""Robust test of the PRODUCTION-TRAINED adapter: load base Qwen2.5-0.5B +
the LoRA adapter the deployed trainer just wrote to the Volume, run the
finite-hypothesis-scoring + deterministic-veto decision path on the
family-held-out test set, and confirm the scores. Proves the real hook
output (not the validation harness's in-process model) actually works.

Run: modal run --env=arm-c robust_test.py --jsonl <finetune.jsonl>
     --adapter arm-c-artifacts/live-round-1/adapter
"""

import json
import re

import modal

app = modal.App("deblob-arm-c-robust-test")
IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*", "transformers==4.44.*", "peft==0.12.*",
    "accelerate==0.33.*", "huggingface_hub==0.24.*",
)
CACHE = modal.Volume.from_name("deblob-base-models", create_if_missing=True)
REPO = "Qwen/Qwen2.5-0.5B-Instruct"
RELATIONS = ["exact", "compatible_drift", "incompatible_similarity"]
NOVELTIES = ["structural", "semantic"]
CAUSES = ["insufficient_evidence", "ambiguous", "candidate_missing"]
POLICY_MAX_DISTANCE, POLICY_MIN_MARGIN, NEW_VETO_DISTANCE = 0.15, 0.10, 0.40


def _allowed_ids(p):
    m = re.search(r"ALLOWED schema_id SET[^\n]*\n\[([^\]]*)\]", p)
    return [x.strip() for x in m.group(1).split(",") if x.strip()] if m else []


def _legal(p):
    c = []
    for sid in _allowed_ids(p):
        for rel in RELATIONS:
            c.append({"decision": "match_schema", "relation": rel, "schema_id": sid})
    c += [{"decision": "new_candidate", "novelty": n} for n in NOVELTIES]
    c += [{"cause": ca, "decision": "abstain"} for ca in CAUSES]
    return c


def _dists(p):
    ds = [float(x) for x in re.findall(r"distance=([0-9.]+)", p)]
    return (ds[0] if ds else 1.0), (ds[1] if len(ds) > 1 else 1.0)


@app.function(image=IMAGE, gpu="T4", volumes={"/cache": CACHE}, timeout=60 * 30)
def evaluate(test_lines, adapter_path, veto=True):
    import torch
    from collections import Counter
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(REPO, cache_dir="/cache/hf-cache")
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    base = AutoModelForCausalLM.from_pretrained(
        REPO, cache_dir="/cache/hf-cache", torch_dtype=torch.bfloat16).to("cuda")
    model = PeftModel.from_pretrained(base, f"/cache/{adapter_path}").to("cuda")
    model.eval()

    def logp(prompt, comp):
        p_ids = tok(prompt + "\n", add_special_tokens=False)["input_ids"]
        c_ids = tok(json.dumps(comp, sort_keys=True), add_special_tokens=False)["input_ids"]
        ids = torch.tensor([(p_ids + c_ids)[-1600:]], device="cuda")
        with torch.no_grad():
            lg = torch.log_softmax(model(ids).logits[0], dim=-1)
        start = len(ids[0]) - len(c_ids)
        return sum(float(lg[start + j - 1, t]) for j, t in enumerate(c_ids) if start + j - 1 >= 0) / max(1, len(c_ids))

    ok = Counter(); tot = Counter(); sid_ok = 0; mtot = 0
    for l in test_lines:
        g = l["gold_tool_call"]; gd = g.get("decision"); tot[gd] += 1
        if gd == "match_schema":
            mtot += 1
        comps = _legal(l["prompt"])
        if not comps:
            continue
        scored = sorted(((c, logp(l["prompt"], c)) for c in comps), key=lambda x: x[1], reverse=True)
        pred = scored[0][0]
        if veto:
            d1, d2 = _dists(l["prompt"])
            if d1 > POLICY_MAX_DISTANCE:
                pred = {"decision": "new_candidate", "novelty": "structural"}
            elif (d2 - d1) < POLICY_MIN_MARGIN and pred.get("decision") == "match_schema":
                pred = {"decision": "abstain", "cause": "ambiguous"}
        if pred.get("decision") == gd:
            ok[gd] += 1
            if gd == "match_schema" and pred.get("schema_id") == g.get("schema_id"):
                sid_ok += 1
    n = len(test_lines)
    return {"decision_match": round(sum(ok.values()) / n, 3),
            "recall": {d: round(ok[d] / tot[d], 3) for d in tot},
            "match_schema_id_correct": round(sid_ok / mtot, 3) if mtot else 0.0,
            "support": dict(tot), "adapter": adapter_path}


@app.local_entrypoint()
def main(jsonl: str, adapter: str = "arm-c-artifacts/live-round-1/adapter"):
    te = [json.loads(l) for l in open(jsonl) if json.loads(l).get("partition") == "test"][:120]
    print(f"test={len(te)} adapter={adapter} — evaluating production-trained adapter...")
    r = evaluate.remote(te, adapter, veto=True)
    print("=== ROBUST TEST (production adapter, finite scoring + veto) ===")
    print(json.dumps(r, indent=2))
    open(jsonl.replace(".jsonl", "") + "_robust_result.json", "w").write(json.dumps(r, indent=2))
