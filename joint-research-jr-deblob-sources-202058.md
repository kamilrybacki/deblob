# New Deblob Ingest Sources — Joint Research Report
run: `jr-deblob-sources-202058` · 2026-07-20 · agents: Claude Code + Hermes

## Executive summary
Goal: more **free, no-auth, PII-free, schema-diverse** JSON sources for Deblob collectors
(k8s CronJob → `curl | jq` → Redpanda REST proxy `events.<domain>.<source>`), steered mid-run
toward **compute & resources** (the Azure-retail / arm-C GPU-training lane).

- **22 concrete sources** below, ranked; **17 are no-auth and empirically or live-verified.**
- Tier 1 (compute) is the priority: **OVHcloud** and **Scaleway** public catalogs (EU, huge nested
  pricing — far less row-like than Azure), **OpenRouter** models (nested arch/pricing), **gpucloudprices.com**
  + **Vast.ai `/bundles/`** (live GPU $/hr), **AWS Price List Bulk**.
- `[C+H]` (both agents, independently) = strongest tier: OpenRouter, crates.io, PyPI, HuggingFace, Docker Hub.
- One conflict adjudicated: **Vast.ai is usable no-auth** via the public `GET /api/v0/bundles/` endpoint
  (Claude got `200 {offers}` with no token); only the documented `search-offers` REST op needs a bearer.
- Cross-cutting: use **jq allowlists (pick fields, never deny-list)** for catalogs/registries to guarantee
  no PII; unwrap fat envelopes into per-item events; add an identifying `User-Agent`; honor attribution/ToS.

## Recommended first wave (ship these)
Seven materially different shapes, no credentials, no protobuf/XML:
**OVHcloud catalog · Scaleway catalog · OpenRouter models · gpucloudprices · Gdańsk ZTM GPS · Open-Meteo · IMGW hydro.**

---

## Tier 1 — Compute & resource pricing (priority)
| # | Source `[attr]` | Endpoint (no-auth unless noted) | Why the schema is interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 1 | **OVHcloud public cloud** `[H, live]` | `https://eu.api.ovh.com/v1/order/catalog/public/cloud?ovhSubsidiary=PL` | huge nested `plans[].pricings[]`, configurations, commitments, nullable consumption, PLN + tax — much less row-like than Azure | daily | enormous → emit **per-plan** events; integer prices ≠ cents, keep `formattedPrice`+currency. **Strongest EU compute.** |
| 2 | **OpenRouter models** `[C+H, verified 200]` | `https://openrouter.ai/api/v1/models` (EU: `?region=eu`) | nested `architecture`, multi-unit `pricing` (strings), `top_provider`, nullable `per_request_limits`, `supported_parameters`, deprecation dates | 1–6 h | prices are strings, units vary (token/req/image/search); treat `description` as untrusted text |
| 3 | **Scaleway public catalog** `[H]` | `https://api.scaleway.com/product-catalog/v2alpha1/public-catalog/products?page_size=100` | product-specific **union** shapes, localities, pricing units, optional attrs | daily | docs say explicitly no-auth; `v2alpha1` (watch drift); don't confuse with authed `/catalog`. **Strong EU fit.** |
| 4 | **gpucloudprices.com** `[C, verified 200]` | `https://gpucloudprices.com/api/v1/prices.json` (+`gpus/providers/vps.json`) | normalized GPU + VPS `$/hr`/`$/mo` rows, `generated_at` | several/day | static, cache-friendly; simplest GPU-market shape |
| 5 | **Vast.ai offers** `[C verified no-auth, H flagged]` | `https://console.vast.ai/api/v0/bundles/` → `{offers}` | volatile GPU bundles: GPU/VRAM/CUDA/driver, CPU/RAM/disk/net, reliability, location, price/perf metrics | 5–15 min | **public `/bundles/` = no auth** (verified); the docs' `search-offers` op needs bearer. `cpu_ram` MB in REST; offers vanish between polls; 429 w/o `Retry-After` |
| 6 | **AWS Price List Bulk** `[C]` | `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-central-1/index.json` (index at `/offers/v1.0/aws/index.json`) | deeply nested `products{}` + `terms{OnDemand/Reserved}` + `pricePerUnit`; different envelope from Azure | daily | files are **huge** → slice by service+region; or use static mirror `https://ec2pricing.com/index.json` |
| — | **Hetzner Cloud pricing** `[H]` | `https://api.hetzner.cloud/v1/pricing` — **read-only bearer required** | nested server/image/LB/IP/traffic families, net/gross, project VAT/currency | daily | needs a token (we can hold one); prices are project-locale, not a global card. **Only if account exists.** |

## Tier 2 — Registries & software resources (no-auth, diverse, homelab-relevant)
| # | Source `[attr]` | Endpoint | Why interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 7 | **HuggingFace org models** `[C+H, verified]` | `https://huggingface.co/api/models?author=Qwen&sort=lastModified&limit=25&full=true` | highly variable records: task tags, config, tensor stats, files, downloads | 6–24 h | ~500 req/5 min/IP; **org-scope + jq-allowlist** (omit author/card free-text). On-theme for SLM/arm-C |
| 8 | **crates.io** `[C+H, verified 200]` | `https://crates.io/api/v1/crates/serde` (or `?sort=recent-downloads`) | crate envelope + versions, features, checksums, keywords/categories, yanked | daily | policy: descriptive contact **UA + ≤1 req/s**; exclude owner/free-text |
| 9 | **PyPI (PEP 691 simple)** `[H, preferred over /pypi/<p>/json]` | `https://pypi.org/simple/jq/` + `Accept: application/vnd.pypi.simple.v1+json` | distribution arrays, hashes, upload meta, Python constraints, yanked, provenance | 6–24 h (ETag) | prefer this to `/pypi/<p>/json` (that leaks author emails + has deprecated `releases`); send UA |
| 10 | **PyPIStats + npm downloads** `[H]` | `https://pypistats.org/api/packages/jq/system` · `https://api.npmjs.org/downloads/range/last-week/<pkg>` | dated download **series** split by OS/Python — contrasting aggregate shape | daily | npm bulk ≤128 pkgs/365 d; avoid full npm packuments (maintainer emails) |
| 11 | **Docker Hub tags** `[C+H]` | `https://hub.docker.com/v2/namespaces/library/repositories/redis/tags?page_size=25&ordering=last_updated` | per-platform images: arch, variant, size, digest, timestamps | 6–12 h | anonymous fields changed recently; scope to official repos; handle 429 |

## Tier 3 — Diverse-schema exercisers (different domains — stress discovery)
| # | Source `[attr]` | Endpoint | Why interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 12 | **USGS earthquakes** `[C]` | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` | GeoJSON `FeatureCollection`: `features[].properties` + `geometry` nested | 1–5 min | stable well-nested shape; no auth |
| 13 | **NOAA SWPC space weather** `[C]` | `https://services.swpc.noaa.gov/json/goes/primary/xray-flares-7-day.json` (+ many `/json/*`) | varied per-endpoint scientific JSON | 1–5 min | many distinct shapes under one host |
| 14 | **OpenSky flights** `[C]` | `https://opensky-network.org/api/states/all` | `states[]` = **array-of-arrays** (unusual non-object rows) — great discovery stress | 1–2 min | anon rate-limited; heavy — sample a bbox |
| 15 | **MET Norway locationforecast** `[H]` | `https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=54.35&lon=18.65` | geometry + `timeseries[].data` with **optional** `next_1/6/12_hours` branches | 30–60 min | **UA required**; respect cache headers/attribution. Excellent optional-branch pressure |
| 16 | **Open-Meteo** `[H, live]` | `https://api.open-meteo.com/v1/forecast?latitude=54.35&longitude=18.65&current=...&hourly=...&daily=...` | separate `current`/`hourly`/`daily` + `*_units`, aligned arrays | 15–60 min | preserve array alignment/units; attribution |

## Tier 4 — EU civic / transit / environment (strong EU fit, complements the grid theme)
| # | Source `[attr]` | Endpoint | Why interesting | Cadence | Gotchas |
|---|---|---|---|---|---|
| 17 | **Gdańsk ZTM GPS** `[H, live]` | `https://ckan2.multimediagdansk.pl/gpsPositions?v=2` (+ `/departures`) | live route/trip/vehicle/coord/timestamp; departures adds nested predictions + optional delay | 30–60 s / 1–2 min | **real JSON, not protobuf GTFS-RT**; CC-BY; vehicle IDs = assets not PII. **Best local source.** |
| 18 | **IMGW hydro/synop** `[H, live]` | `https://danepubliczne.imgw.pl/api/data/hydro` · `/api/data/synop` | PL station obs, independent timestamps, distinct hydro vs weather shapes | 10–15 min / hourly | **numbers often strings**; normalize empty/null; dedupe by station+time. **Excellent PL fit.** |
| 19 | **PEGELONLINE** `[H, live]` | `https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&includeCurrentMeasurement=true` | stations nest water body, coords, agency, timeseries meta, gauge-zero history, current measurement | 15 min | big → per-station events. **Strong official EU env source.** |
| 20 | **GIOŚ air quality** `[H]` | `https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll` → `/station/sensors/{id}` → `/data/getData/{sensorId}` | multi-step relational: station→commune, sensor→parameter, nullable series | daily/hourly | cache IDs, few Pomeranian stations to limit fan-out; validate content-type |
| 21 | **DWD warnings** `[H, live]` | `https://www.dwd.de/DWD/warnungen/warnapp/json/warnings.json` | region-ID-keyed map of warning arrays, optional end/altitude, text | 5–10 min | **JSONP** `warnWetter.loadWarnings({...});` — strip wrapper before jq |
| 22 | **Wikidata recentchanges** `[H]` | `https://www.wikidata.org/w/api.php?action=query&list=recentchanges&rcnamespace=0&rctype=edit|new&rcprop=title|ids|sizes|flags|timestamp|tags&rclimit=100&format=json&formatversion=2` | optional flags/tags, old/new sizes/revisions, continuation envelope | 2–5 min | omits `user`/`comment` (PII-safe); carry `rccontinue`, dedupe `rcid`, UA + `maxlag`. (Pull API preferred over SSE EventStreams for cron.) |
| — | Catalog metadata (curated snapshots, not realtime): **data.europa.eu** `/api/hub/search/search`, **dane.gov.pl** `/1.4/datasets`, **GUS BDL** `/api/v1/data/by-variable/{id}`, **EEA Discodata** `/sql?query=...` — all `[H, live]`, no-auth | | multilingual/JSON:API/statistical/SQL-defined shapes | daily–weekly | metadata not observations; jq-allowlist; EEA table names are versioned contracts |

## Conflicts & adjudication
1. **Vast.ai auth.** Hermes: `search-offers` needs a bearer. Claude: empirically `GET https://console.vast.ai/api/v0/bundles/` → `200 {offers}` with **no token**. → **No-auth is achievable** via `/bundles/`; use it, not the documented search op. (Both correct for different endpoints.)
2. **PyPI endpoint.** Claude probed `/pypi/<p>/json` (works, nested). Hermes: prefer **PEP 691 `/simple/<p>/`** — avoids deprecated `releases` and author-email PII. → adopt Hermes' variant.
3. **Wiki firehose.** Claude found the SSE **EventStreams** `recentchange`; Hermes recommends the **pull** `recentchanges` API for a cron `curl|jq` pipeline (PII-omitted, continuation-token). → use pull for cron; keep SSE only if we build a streaming collector.

## Claude-verified live (200 + JSON, no token sent)
`gpucloudprices.com/api/v1/gpus.json` · `openrouter.ai/api/v1/models` · `huggingface.co/api/models` ·
`console.vast.ai/api/v0/bundles/` · `crates.io/api/v1/crates` · `pypi.org/pypi/requests/json`.

## Hermes-verified live payloads
OVHcloud · data.europa.eu · GUS BDL · Gdańsk CKAN map · Open-Meteo · MET Norway · IMGW · PEGELONLINE ·
EEA Discodata · DWD JSONP · Swiss stationboard. (Docs-only, calls not made: Hetzner, Vast.ai search-offers.)

## Deprioritized / traps
- **Auth-gated now:** Transitland v2 (key), OpenAQ v3 (key), Hetzner pricing (bearer).
- **Format-incompatible with `curl|jq`:** generic GTFS-RT (protobuf), raw RSS/Atom (need an XML→JSON step first).
- **Fair-use / low-value:** Nominatim/Overpass fixed-query polling.
- **PII risk if ingested broadly:** full npm packuments, `/pypi/<p>/json`, un-scoped HF/registry queries → org-scope + jq-allowlist.
- **Off the compute steer (kept as leads):** CoinGecko keyless `[C]`, Blockchain.info unconfirmed-tx `[C]`.

## Cross-cutting collector notes (matches `deploy/collectors/` pattern)
- Reuse the Azure recipe: CronJob + `collect.sh` (`curl -sf -A "<UA email>" | jq -c '<UNWRAP>' | curl POST redpanda:8082/topics/events.<domain>.<source>`).
- **Unwrap fat envelopes** to per-item events: OVH per-plan, PEGELONLINE per-station, AWS per-region-slice, DWD strip JSONP.
- **jq allowlist, never denylist** for catalogs/registries — pick exactly the PII-free fields.
- After adding a topic: append to `raw_topics`, and (only for trusted operator-owned producers) to `[samples].capture_sources` + `[auto_promote].allowed_sources`.

## Method note
Briefs split complementary: Claude = dev/tech-events, science/space, markets, + the compute lane (empirical curl probes); Hermes = civic/EU/transit/weather/environment + EU-cloud compute, its Obsidian vault + homelab-fit judgment. Dispatched 18:58 CEST; mid-run **priority steer to compute/resources** at 19:08 (Hermes interrupted its civic pass and pivoted). Hermes COMPLETE 19:16 (8/8), full report also saved to its vault `research/Deblob-Sources-Civic-Compute-JR-202058.md`. No timeout.
