# homelab-intel — spec (draft 2026-07-18)

**Goal.** A free, homelab-hosted API that answers, for any self-hosted app:
*is it actively maintained? what's the latest release? are there open CVEs? is
this version EOL?* — the enrichment/join that no single free API publishes today.
Deblob is the schema layer: it discovers, versions, and drift-tracks every
upstream's response shape, and the console's OpenAPI export auto-documents the
published output.

## Upstreams (all redistributable — attribution required)
| Source | License / basis | Gives us | Notes |
|---|---|---|---|
| **awesome-selfhosted-data** | CC BY-SA 3.0 | the **catalog** — app list, categories, license, source repo, GitHub stars/last-commit | git repo of `software/*.yml`; the backbone key everything joins on |
| **GitHub Releases API** | public API (ToS) | latest release + notes per app repo | 5000/hr with a token; ties to existing FeedCord/#releases |
| **OSV.dev** | open (OSV schema; GCS bucket) | known CVEs per app/version | RustSec/GHSA/PyPA aggregated; `modified` feeds for incremental |
| **endoflife.date** | public API | EOL status per mapped product | pure JSON, tiny, homelab-relevant |

CC BY-SA 3.0 → the derived catalog data must carry attribution + be shared alike;
bake attribution into every response + a `/LICENSE` + `/attribution` endpoint.

## Data flow
1. **Collector** (in-cluster, scheduled, GitOps via helm/ArgoCD) polls each upstream.
2. Each raw upstream response → Redpanda `events.raw` with a `source` header →
   **Deblob tags + versions the schema** (this is where drift across upstream API
   changes becomes visible + governed).
3. Collector also writes **normalized, joined** records (keyed by app) to a store.
4. **Read API** (thin, read-only) serves the joined data behind the edge.
5. The read API's **OpenAPI is derived from Deblob's learned schemas** (the export
   we just shipped) — the schema registry documents the product.

## Deblob's role (why this isn't just another scraper)
The four upstreams have very different, independently-evolving JSON shapes. Deblob
gives the pipeline a governed schema registry: new upstream fields surface as
drift/new versions, malformed responses quarantine, and the published contract is
generated from discovered shapes rather than hand-maintained.

## Read-API surface (v1 sketch)
- `GET /apps` — catalogue (paginated) · `GET /apps/{slug}` — the joined intel record
- `GET /apps/{slug}/releases` · `/cves` · `/eol`
- `GET /categories` · `GET /openapi.json` (Deblob-derived) · `/attribution`

## Legality / hygiene
Attribution + share-alike for CC BY-SA; respect each upstream's rate limits
(incremental `modified` feeds, cache, polite intervals); collector runs in-cluster
on a worker (not the edge); token secrets via k8s Secret, never baked.

## Phased plan
- **P0 (first wire):** one upstream → Redpanda → Deblob, permanent scheduled collector. Proves the pipeline end-to-end.
- **P1:** catalogue (awesome-selfhosted) + join store + read-API skeleton behind the edge.
- **P2:** add releases + CVEs + EOL joins, keyed by app.
- **P3:** OpenAPI publish (Deblob-derived) + attribution + docs; optional public-read vs Authelia.

## Open decisions (need your call)
1. **First upstream to wire:** `awesome-selfhosted` catalogue (the backbone) **or** `endoflife.date` (cleanest pure-JSON, fastest to prove the loop)?
2. **Collector home:** a new `homelab-intel` repo with its own Helm chart (GitOps, matches your stack) **or** start it inside the existing `deblob` deploy dir and split out later?
3. **Join/read store:** reuse the in-cluster **Redis** (already there) **or** a small **Postgres**?
