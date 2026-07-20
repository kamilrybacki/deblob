# More AI / LLM / Compute Ingest Sources — Joint Research Report
run: `jr-deblob-ai-compute-202202` · 2026-07-20 · agents: Claude Code + Hermes

## Executive summary
Round 2, scoped to **AI/LLM/compute** sources NOT already built (round 1 shipped Vast.ai, RunPod,
gpucloudprices, OVHcloud, Scaleway, OpenRouter, HF models, PyPI, crates.io, npm, Docker Hub).
- **13 new sources**, all live-checked with real HTTP codes; **10 are no-auth**, 3 need a holdable free key.
- Best no-auth, clean-JSON, schema-diverse additions: **HF benchmark-leaderboard API**, **LMArena leaderboard**,
  **HF Daily Papers** `[C+H]`, **TechPowerUp** + **D-Central** GPU specs, **EvalPlus**, **HF datasets/datasets-server**,
  **Ollama model library**, **MLPerf system manifests**.
- Holdable-key (worth it): **Artificial Analysis** (eval+latency+pricing), **OpenAlex** + **Semantic Scholar** (paper graphs).
- **Rejected for the `curl+jq` pattern:** arXiv (Atom **XML**), HELM (multi-GB GCS, multi-stage), Papers with Code
  (**retired** Jul-2025), SWE-bench raw (1.18 MB — use the HF benchmark API instead), derived Arena JSON (not authoritative).
- Both agents agree on **strict PII projection**: jq-allowlist only; drop authors, submitters, users, notes, comments,
  abstracts, affiliations, Arena conversations, per-instance/logs.

## Tier A — no-auth, ship-ready
| # | Source `[attr]` | Endpoint | Why the schema is interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 1 | **HF benchmark-leaderboard API** `[H, 200: cais/hle 69 entries]` | discovery `https://huggingface.co/api/datasets?filter=benchmark:official` → `https://huggingface.co/api/datasets/{id}/leaderboard` | generic leaderboard entries (model, scores, metadata) across HLE / SWE-bench / many official benchmarks — one clean interface, many shapes | daily | omit `author`/notes/PR meta; use a distinct `src_hf_benchmark_<dataset>` identity per benchmark |
| 2 | **LMArena leaderboard** `[H]` | `https://datasets-server.huggingface.co/first-rows?dataset=lmarena-ai%2Fleaderboard-dataset&config=text_style_control&split=latest` | authoritative rankings: rating bounds, variance, votes, rank, category, license, pub date | daily | `first-rows` truncates; official dataset lags the live Arena page |
| 3 | **HF Daily Papers** `[C+H, verified 200, 50]` | `https://huggingface.co/api/daily_papers?p=0&limit=50&sort=publishedAt` | trending AI papers: ids, timestamps, upvotes, optional repos, evolving nested meta | 15 min–6 h | keep only id/title/timestamps/upvotes/repo; drop authors/summary/comments/thumbnails |
| 4 | **TechPowerUp GPU specs** `[H, 200]` | `https://www.techpowerup.com/gpu-specs/api/v1/cards?q=RTX%205090` | rich device schema: arch, process, memory, bandwidth, clocks, compute units, TDP, bus, dims, release | weekly | allowlist a fixed accelerator set; research-use documented, commercial licensing differs; no numeric rate limit found |
| 5 | **D-Central AI GPU DB** `[H, 200, CC-BY 4.0]` | `https://d-central.tech/wp-json/dc/v1/ai-gpus` (static: `.../wp-content/uploads/data/dcentral-ai-gpu-database.json`) | VRAM, bandwidth, FP16, INT8, TDP, arch, inference tier, segment, verification date | weekly | drop `ai_notes`; retain attribution |
| 6 | **EvalPlus** `[H, 200, ~34 KB]` | `https://raw.githubusercontent.com/evalplus/evalplus.github.io/main/results.json` | nullable HumanEval/HumanEval+/MBPP/MBPP+ scores, model size, prompted, open-data flag (~125 models) | daily | `to_entries[]`; watch repo commit time — leaderboard looks stale |
| 7 | **HF datasets + datasets-server** `[C, verified 200]` | `https://huggingface.co/api/datasets?sort=downloads&limit=50` · `https://datasets-server.huggingface.co/splits?dataset={id}` (+ `/rows`,`/statistics`,`/first-rows`) | dataset registry + typed feature/statistics shapes — deeply nested, very varied | 6–24 h | public datasets anon; gated need a token |
| 8 | **Ollama model library** `[C, verified 200, 232]` | `https://yuma-shintani.github.io/ollama-model-library/model.json` (community JSON mirror; ollama.com/library itself is HTML) | local-model registry: name/tags/sizes/updated — on-theme for the homelab Ollama | daily | third-party mirror (no official Ollama registry API); verify freshness |
| 9 | **BigCodeBench + Open LLM Leaderboard** `[H]` | `https://datasets-server.huggingface.co/first-rows?dataset=bigcode%2Fbigcodebench-results&config=default&split=train` · `...open-llm-leaderboard%2Fcontents...` | wide sparse: model type, total/active params, MoE, CO₂, precision, normalized scores | daily | unwrap `.rows[].row`; both visibly stale (~months) but strong **schema pressure** |
| 10 | **MLPerf Inference/Training system manifests** `[H, 200, ~49 fields]` | discovery `https://api.github.com/repos/mlcommons/inference_results_v5.1/contents/closed/NVIDIA/systems` → raw `.../systems/B200-SXM-180GBx8_TRT.json` | hardware/topology inventory: accelerators, memory, CPU, net, storage, cooling, OS, CUDA/TensorRT/cuDNN, drivers | weekly | **JSON = hw inventory only**; scores are CSV. Anon GitHub API = 60 req/h → follow `download_url` |

## Tier B — free, holdable key (hold in-cluster like a secret)
| # | Source `[attr]` | Endpoint | Why interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 11 | **Artificial Analysis** `[H, 401 unauth confirmed]` | `https://artificialanalysis.ai/api/v2/data/llms/models` (`x-api-key`) | independently-measured evals + latency/throughput/TTFT + USD token pricing, stable model/creator ids | 6–24 h | free key; **1000 req/day**; attribution mandatory |
| 12 | **OpenAlex** `[H, anon 200 but docs now ask for key]` | `https://api.openalex.org/works?search=large%20language%20model&filter=from_publication_date:YYYY-MM-DD&select=id,doi,title,publication_date,type,cited_by_count,primary_topic,open_access&per-page=100` | nested topic/open-access/query metadata paper graph | daily | free key = $1/day pool; **validate dates** (sample had future pub dates); omit authors |
| 13 | **Semantic Scholar Graph** `[H, anon 429]` | `https://api.semanticscholar.org/graph/v1/paper/search/bulk?query=...&fields=paperId,title,year,publicationDate,venue,citationCount,openAccessPdf&limit=100` | scholarly graph metadata | daily | anon shared-throttled (429) → **holdable key**, 1 req/s; omit author/abstract/pdf |

## Conflicts & adjudication
No hard conflicts — the split was clean (Claude = model/dataset registries + Ollama; Hermes = benchmarks/leaderboards/papers/HW). Corroborations:
- **HF Daily Papers** independently verified by both → `[C+H]`, strongest tier.
- **datasets-server** mechanism verified by Claude (`/splits` 200) and used by Hermes for LMArena/OpenLLM/BigCodeBench first-rows → shared-confidence transport.

## Rejected / dead (don't build as `curl+jq` collectors)
- **arXiv** `export.arxiv.org/api/query` — no-auth but **Atom XML** → needs a deterministic XML→JSON step first (≤1 req/3s, daily is enough).
- **HELM** — public but **multi-GB, multi-stage GCS** (`storage.googleapis.com/.../crfm-helm-public/...`); ingest only summaries if ever.
- **Papers with Code** — **retired Jul-2025**, API redirects to HF Trending HTML; frozen GitHub dumps = one-time backfill only.
- **SWE-bench** raw `leaderboards.json` — 1.18 MB; prefer the HF benchmark API (#1) for aggregate scores.
- **Derived Arena JSON** (`api.wulong.dev/arena-ai-leaderboards`) — no-auth clean `{meta,models}` but **not authoritative** (parses Arena + LLM transform); freshness signal only.

## Claude-verified live (200, no token)
`huggingface.co/api/datasets` · `datasets-server.huggingface.co/splits` · `huggingface.co/api/daily_papers` ·
`yuma-shintani.github.io/ollama-model-library/model.json`. (Key-gated, deprioritized: Together `/v1/models` → 401.)

## Hermes-verified live (with HTTP codes)
LMArena dataset, HF `/api/datasets/{id}/leaderboard` (cais/hle 200, 69), EvalPlus (200), TechPowerUp (200),
D-Central (200), HF Daily Papers (200), Open LLM Leaderboard, BigCodeBench (200), MLPerf manifest (200, 49 fields),
OpenAlex (anon 200), Semantic Scholar (anon 429), Artificial Analysis (401 unauth = key needed), derived Arena (200).

## Suggested second-wave build (no-auth, highest signal)
`events.ai.benchmarks` (HF benchmark-leaderboard API, per-benchmark identity) · `events.ai.arena` (LMArena) ·
`events.ai.papers` (HF Daily Papers) · `events.hw.gpu-specs` (TechPowerUp + D-Central) · `events.ai.evalplus`.
Same collector pattern; jq-allowlist; per-benchmark `src_` identity; watch stale leaderboards via repo commit time.

## Method note
Split: Claude = inference/model-serving registries + GPU-cloud availability + HF datasets (empirical curl probes);
Hermes = benchmarks/leaderboards/paper-feeds/HW specs + Obsidian vault. Dispatched 20:02 CEST; Hermes ran an
exhaustive pass (40-iter budget, ran live code to verify HTTP codes), COMPLETE 20:19 (6/6), full report also saved to its
vault `research/Deblob-AI-Compute-Sources-JR-202202.md`. No timeout.
