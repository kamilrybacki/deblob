"""Dump per-example predictions from the PRODUCTION-trained adapter for the
report: event field-shape -> identified schema decision/relation -> gold ->
verdict, plus top-k distances, a 3x3 decision confusion matrix, and
per-scenario accuracy. Finite-scoring + deterministic veto (v4 policy).
"""

import json
import re

import modal

app = modal.App("deblob-examples-dump")
IMAGE = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch==2.4.*", "transformers==4.44.*", "peft==0.12.*",
    "accelerate==0.33.*", "huggingface_hub==0.24.*",
)
CACHE = modal.Volume.from_name("deblob-base-models", create_if_missing=True)
REPO = "Qwen/Qwen2.5-0.5B-Instruct"
RELATIONS = ["exact", "compatible_drift", "incompatible_similarity"]
NOVELTIES = ["structural", "semantic"]
CAUSES = ["insufficient_evidence", "ambiguous", "candidate_missing"]
PMAX, PMARGIN, NEWVETO = 0.15, 0.10, 0.40


def _ids(p):
    m = re.search(r"ALLOWED schema_id SET[^\n]*\n\[([^\]]*)\]", p)
    return [x.strip() for x in m.group(1).split(",") if x.strip()] if m else []


def _legal(p):
    c = [{"decision": "match_schema", "relation": r, "schema_id": s} for s in _ids(p) for r in RELATIONS]
    c += [{"decision": "new_candidate", "novelty": n} for n in NOVELTIES]
    c += [{"cause": ca, "decision": "abstain"} for ca in CAUSES]
    return c


def _dists(p):
    ds = [float(x) for x in re.findall(r"distance=([0-9.]+)", p)]
    return (ds[0] if ds else 1.0), (ds[1] if len(ds) > 1 else 1.0)


def _fields(p):
    out = []
    for m in re.finditer(r'path=\[(.*?)\] depth=\d+ present=\S+ explicit_null=\d+ types=\[([^\]]*)\]', p):
        name = m.group(1).replace('"', "").replace(" > ", ".")
        typ = m.group(2).replace('"', "")
        out.append(f"{name}:{typ}")
    return out[:7]


def _obs(p):
    m = re.search(r"observation_count: (\d+)", p)
    return int(m.group(1)) if m else 0


@app.function(image=IMAGE, gpu="T4", volumes={"/cache": CACHE}, timeout=60 * 30)
def dump(test_lines, adapter_path):
    import torch
    from collections import Counter, defaultdict
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer

    tok = AutoTokenizer.from_pretrained(REPO, cache_dir="/cache/hf-cache")
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    base = AutoModelForCausalLM.from_pretrained(REPO, cache_dir="/cache/hf-cache",
                                                torch_dtype=torch.bfloat16).to("cuda")
    model = PeftModel.from_pretrained(base, f"/cache/{adapter_path}").to("cuda")
    model.eval()

    def lp(prompt, comp):
        pi = tok(prompt + "\n", add_special_tokens=False)["input_ids"]
        ci = tok(json.dumps(comp, sort_keys=True), add_special_tokens=False)["input_ids"]
        ids = torch.tensor([(pi + ci)[-1600:]], device="cuda")
        with torch.no_grad():
            g = torch.log_softmax(model(ids).logits[0], dim=-1)
        st = len(ids[0]) - len(ci)
        return sum(float(g[st + j - 1, t]) for j, t in enumerate(ci) if st + j - 1 >= 0) / max(1, len(ci))

    scen = lambda cn: re.sub(r"^gen_\d+_\d+_", "", cn or "?")
    confusion = defaultdict(Counter)
    by_scen = defaultdict(lambda: [0, 0])
    examples = []
    for l in test_lines:
        g = l["gold_tool_call"]; gd = g["decision"]
        comps = _legal(l["prompt"])
        if not comps:
            continue
        scored = sorted(((c, lp(l["prompt"], c)) for c in comps), key=lambda x: x[1], reverse=True)
        pred, best, second = scored[0][0], scored[0][1], (scored[1][1] if len(scored) > 1 else -1e9)
        d1, d2 = _dists(l["prompt"])
        vetoed = None
        if d1 > PMAX:
            pred = {"decision": "new_candidate", "novelty": "structural"}; vetoed = "distance>0.15→new"
        elif (d2 - d1) < PMARGIN and pred.get("decision") == "match_schema":
            pred = {"decision": "abstain", "cause": "ambiguous"}; vetoed = "margin<0.10→abstain"
        pd = pred["decision"]
        confusion[gd][pd] += 1
        ok = pd == gd
        by_scen[scen(l["case_name"])][0] += 1
        by_scen[scen(l["case_name"])][1] += int(ok)
        examples.append({
            "scenario": scen(l["case_name"]), "obs": _obs(l["prompt"]),
            "fields": _fields(l["prompt"]), "d1": round(d1, 3), "d2": round(d2, 3),
            "gold": g, "pred": pred, "ok": ok, "vetoed": vetoed,
            "margin": round(best - second, 2),
        })
    return {
        "n": len(examples),
        "confusion": {k: dict(v) for k, v in confusion.items()},
        "by_scenario": {k: {"n": v[0], "correct": v[1]} for k, v in by_scen.items()},
        "examples": examples,
    }


@app.local_entrypoint()
def main(jsonl: str, adapter: str = "arm-c-artifacts/live-round-1/adapter"):
    te = [json.loads(l) for l in open(jsonl) if json.loads(l).get("partition") == "test"]
    print(f"test={len(te)} — dumping per-example predictions...")
    r = dump.remote(te, adapter)
    out = jsonl.replace(".jsonl", "") + "_examples_dump.json"
    open(out, "w").write(json.dumps(r, indent=2))
    print(f"wrote {r['n']} examples + confusion + by_scenario to {out}")
    print("confusion:", json.dumps(r["confusion"]))
    print("by_scenario:", json.dumps(r["by_scenario"]))
