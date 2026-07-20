# Data to Optimize AI Compute — Joint Research Report
run: `jr-compute-optim-202313` · 2026-07-20 · agents: Claude Code + Hermes

## Executive summary
Signals for **when / where / how** to run ML workloads cheaper & greener. All live-checked 2026-07-20.
- **7 no-auth, buildable** optimization feeds → carbon-aware + cost-aware + spot-timing scheduling.
- Standouts: **Energinet CO2EmisProg** (5-min DK carbon + next-day *forecast*), **PSE** (PL renewable/curtailment windows), **RTE éCO2mix** (FR carbon+mix), **Energy-Charts price** (EU day-ahead), **AWS Spot Advisor** (interruption+savings), **Elecz cheapest-hours** (`run/wait/stop` signal), **aWATTar** `[C+H]`.
- **Held-key (worth it):** Electricity Maps v4 (flow-traced carbon), WattTime signal-index (free 0–100 marginal "run-now" percentile), Fingrid (grid-frequency veto).
- **Rejected:** ENTSO-E (XML + 3-day token), CO2signal (retired), Nord Pool (paid terms), GSF/Google cloud-PUE + Epoch (CSV), carbonbench (down), ML.ENERGY (HF-gated), Open-Meteo (low-specificity).
- Design rule (Hermes): keep **carbon and price as separate streams**, joined downstream by zone+interval — don't hardwire a carbon-vs-cost weighting at ingest.

## Tier A — no-auth, buildable now
| # | Source `[attr]` | Endpoint | Optimization value | Cadence | Gotchas |
|---|---|---|---|---|---|
| 1 | **Energinet (DK)** `[H, 200]` | `https://api.energidataservice.dk/dataset/CO2Emis?start=now-PT15M&columns=Minutes5UTC,PriceArea,CO2Emission` · `.../CO2EmisProg?limit=100` · `.../Elspotprices?columns=HourUTC,PriceArea,SpotPriceEUR&limit=100` | 5-min regional carbon **+ next-day carbon forecast** + spot price — dispatch batch to cleanest forecast interval, confirm live | 5 min / forecast 30–60 min | `{records[]}` envelope; forecast is estimated (not marginal); 1 req/update-interval |
| 2 | **PSE (PL)** `[H, 200]` | `https://api.raporty.pse.pl/api/pk5l-wp` (PV/wind/demand fcst) · `/price-fcst` · `/rce-pln` (15-min price) · `/his-wlk-cal` (mix/load) · `/poze-redoze` (curtailment) | low-demand/high-renewable windows in Poland; **curtailment = otherwise-spilled renewable** flexible compute can absorb | fcst hourly, actual/price 15 min | OData `$filter`/`$select` **must be URL-encoded**; endpoint names case-sensitive; market signal not gCO₂ |
| 3 | **RTE éCO2mix (FR)** `[H, 200]` | `https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records?select=date_heure,taux_co2,consommation,prevision_j1,prevision_j,eolien,solaire,hydraulique,nucleaire,bioenergies&order_by=date_heure desc&limit=100` | pick low-`taux_co2` windows; J-1 forecast improves pre-scheduling; distinguishes renewable-rich vs nuclear-heavy | 15 min | `{results[]}`; carbon = generation-in-FR estimate; RT records later consolidated; 50k calls/mo |
| 4 | **Energy-Charts price** `[H, 200]` (extends our grid collector) | `https://api.energy-charts.info/price?bzn=PL&start=YYYY-MM-DD&end=YYYY-MM-DD` (zones PL/DE-LU/AT/FR/DK1/DK2/NL/SE4/…) | schedule movable work into min-price quarter-hours; pair w/ carbon by timestamp | after day-ahead auction, retry hourly | **parallel arrays** `unix_seconds[]`+`price[]` → jq transpose; validate equal length, reject nulls; excludes taxes |
| 5 | **AWS Spot Advisor** `[C, 200, 1.2 MB]` | `https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json` | `spot_advisor[region][os][instance] = {r:interrupt-rate 0–4, s:savings%}` — where/what to run interruptible GPU/CPU cheapest | daily | big → emit per region×instance; `ranges[]` maps `r`→"<5%…>20%" |
| 6 | **Elecz cheapest-hours** `[C, 200]` | `https://elecz.com/signal/cheapest-hours?zone=DE` (40+ zones) | ready-made **`current_hour_signal` = low/med/high (run/wait/stop)** + `next_cheap_hour` + `hours_until_next_cheap` + `data_complete` | hourly | derived from ENTSO-E/Octopus; community service (no SLA); loop zones |
| 7 | **aWATTar (AT/DE)** `[C+H, 200]` | `https://api.awattar.at/v1/marketdata` · `https://api.awattar.de/v1/marketdata` | simple next-day EPEX spot price, no account | daily ~14:00 CET | `{data[]}` hourly; 100 calls/day; Energy-Charts is finer — keep as independent AT/DE check |

## Tier B — free but held key (2-stage or header; hold in-cluster)
| Source | Endpoint / auth | Why | Gotcha |
|---|---|---|---|
| **Electricity Maps v4** `[H]` | `api.electricitymap.org/v4/carbon-intensity/{latest,forecast}?zone=PL` + `/renewable-energy/latest`; `auth-token` header (`/v4/zones` public) | flow-traced consumption carbon + forecast across many zones, normalized | free plan ~1 zone/50 rph; confirm redistribution terms; hourly |
| **WattTime v3** `[H]` | Basic-auth `GET /login` → 30-min bearer → `/v3/signal-index?region=&signal_type=co2_moer` | **free signal-index = 0–100 percentile of current MOER vs next-24h** = marginal run-now-vs-wait; conceptually best for shifting | 2-stage (login→token); raw forecast/historical are paid; 401 unauth |
| **Fingrid (FI)** `[H]` | `data.fingrid.fi/api/datasets/177/data?startTime=&endTime=`; `x-api-key` (free reg) | grid-frequency **stress veto** — don't start big jobs during Nordic under-frequency | 10k/day, 1 req/2s; Nordic-wide not FI-carbon |

## My lane — Claude-verified live (no token)
`spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json` (200, 1.2 MB) · `elecz.com/signal/cheapest-hours?zone=DE` (200, `current_hour_signal`) · `api.awattar.de/v1/marketdata` (200, 24 rows) · HF `datasets-server` first-rows.

## Conflicts & adjudication
- **aWATTar** `[C+H]` corroborated. **carbonbench.ai** `[C]` — brilliant concept (carbon+cost+speed per model/region) but returned **HTTP 500 DB-auth error** at test → not dependable; retry later. **Nord Pool** — Hermes got 200 JSON but its terms forbid unauthenticated redistribution → rejected on ToS, not tech.

## Rejected / not-ingestible (documented so we don't re-chase)
- **ENTSO-E Transparency** — authoritative pan-EU but **XML** + token (email request, ≤3 days). Needs XML→JSON sidecar.
- **CO2signal** — retired (522), migrated to Electricity Maps.
- **GSF cloud-region PUE** (`datasets.thegreenwebfoundation.org/...json`) — Cloudflare 403 from cluster; canonical data is **CSV** on GitHub. **Google `region-carbon-info`** — CSV. **Epoch AI** (`all_ai_models.csv`, `gpu_clusters.csv`, ML-hardware, data-centers, FLOP/$, training cost/power) — **CSV** → needs a parser in the collector image; rich compute-economics, worth adding a CSV sidecar later.
- **ML.ENERGY** (energy/token, perf/watt) — raw dataset HF-**gated** (needs HF_TOKEN); leaderboard JSON URL not public. **AI Energy Score** HF datasets = benchmark *inputs* `{text}`, not results. **Open-Meteo** — only a renewable proxy; PSE/RTE give direct system forecasts.

## Method note
Split: Claude = efficiency benchmarks + compute economics + spot-price timing (empirical curl probes); Hermes = carbon-aware grid + EU energy markets + datacenter/PUE + vault. Dispatched 23:13 CEST; Hermes ran live-code verification, COMPLETE 10/10; full report also in its vault `research/`. No timeout.
