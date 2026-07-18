# greenwindow — cheap + green compute-window advisor (spec, 2026-07-18)

**Goal.** A free API that tells you *when and where* to run a workload for the best
**cost × carbon** tradeoff — crossing electricity-grid signals (price + carbon
intensity, incl. forecasts) against **compute** prices (cloud spot + GPU rental).
Concretely for this homelab: "schedule the arm-C training / k8s batch jobs into the
cheapest-greenest window." Deblob is the schema layer over the heterogeneous
upstreams; the console's OpenAPI export documents the published API.

## Two families of upstream (all free / redistributable; attribution per source)
### Grid (price + carbon)
| Source | Auth | Coverage | Notes |
|---|---|---|---|
| **carbonintensity.org.uk** | none | GB | clean JSON, current + 48h **forecast** + generation mix — the P0 anchor |
| **ENTSO-E Transparency** | free token | EU incl. **Poland** | day-ahead prices + generation; XML API; token = user TODO |
| ElectricityMaps / Ember | free tier | global | carbon intensity, fallback/cross-check |

### Compute (price)
| Source | Auth | Notes |
|---|---|---|
| **Azure Retail Prices** | **none** | `prices.azure.com/api/retail/prices` — global incl. PL regions, clean JSON |
| AWS EC2 Spot advisor / Price List | none (advisor JSON) | spot interruption + price hints |
| GCP Cloud Billing Catalog | key | spot/preemptible |
| GPU rental (RunPod, Vast.ai) | varies | arm-C-relevant; **Vast.ai egress is currently blocked from the cluster (000)** — revisit |

## Proven pipeline (validated 2026-07-18)
`collector (curl) → Redpanda pandaproxy POST /topics/events.raw → Deblob relay
→ fingerprint → schema/candidate discovered`. Verified end-to-end: a produced
`{from,to,intensity}` record surfaced as candidate `cand_xevuz…` (sample_count 2).
- pandaproxy reachable cross-pod at `redpanda.deblob.svc.cluster.local:8082` (binds
  0.0.0.0; no Service edit needed).
- Cluster egress OK to carbonintensity + Azure retail (Vast.ai blocked).
- **Collector = pure curl** in `curlimages/curl` — no image build, no Kafka client.

## Data flow
1. **Collectors** (in-cluster CronJobs, worker-pinned, ns `deblob`) poll each upstream on its own cadence.
2. Each raw response → `events.raw` via pandaproxy → **Deblob tags + versions the schema** (drift as upstream APIs change).
3. A **normalizer** maps each source into a common `signal` record: `{zone, kind:price|carbon|compute, ts, value, unit, horizon, provider?, region?, raw_ref}`, stored in Redis keyed by (zone/region, ts).
4. A **scorer** crosses grid × compute into a `window` score per (region, time-slot).
5. **Read API** (thin, read-only) behind the edge: `/windows`, `/grid/{zone}`, `/compute`, `/recommend?workload=…`, `/openapi.json` (Deblob-derived).

## Read-API surface (v1 sketch)
`GET /grid/{zone}` (current+forecast) · `GET /compute?provider=&region=` ·
`GET /windows?region=&hours=48` (ranked cheap+green slots) ·
`GET /recommend?kind=gpu|cpu&hours=` · `GET /openapi.json` · `/attribution`

## Phases
- **P0 (now):** carbonintensity collector → Redpanda → Deblob discovers the grid schemas. Proves the permanent wire on a schedule. ✅ pipeline validated; building the CronJob.
- **P1:** Azure retail collector (compute side) + normalizer → Redis `signal` store; ENTSO-E once the free token is seeded (user TODO, like Modal).
- **P2:** scorer (`window` = cost×carbon) + read-API skeleton behind the edge (`greenwindow.<domain>`), OpenAPI from Deblob.
- **P3:** GPU rental (RunPod; revisit Vast.ai egress), `/recommend`, attribution + docs. Optional: a controller that *acts* — defer k8s batch/arm-C jobs to the best window.

## Hygiene / legality
Attribution per source (carbonintensity OGL, ENTSO-E ToS, Azure ToS); polite
intervals + caching; collectors on workers, never the edge; tokens via k8s Secret,
never baked. **User TODO:** register a free ENTSO-E API token for PL/EU grid data.

## Open decisions (P1+)
1. Collector home: keep under `deblob/deploy/collectors/` for now, or split into a `greenwindow` repo + Helm chart (GitOps) at P1?
2. Read store: reuse in-cluster Redis (recommended) vs Postgres.
