# Deblob's "Final Boss" — Amorphous Source — Joint Research Report
run: `jr-deblob-finalboss-210956` · 2026-07-21 · agents: Claude Code + Hermes

## Executive summary
Two agents, two excellent finalists on **different axes** — a real split worth reading:
- **[C] Bluesky Jetstream** — a live JSON firehose of the WHOLE ATProto ecosystem. Live-measured: **19 distinct schemas in 9 SECONDS**, nesting depth **8**, self-describing (`$type`/collection NSID). Not just Bluesky (post/like/repost/follow/block/threadgate/profile) but unrelated apps on the same stream: `dev.sensorthings.observationBatch` (IoT), `fm.teal.alpha.feed.play` (music scrobbles), `at.podping` (podcasts), `app.studynext.task`, `place.stream.livestream`, `social.coves.community.post`. → **50-100+ families/hour**, evolving in real time. **Catch: WebSocket (WSS), not HTTP curl+jq.**
- **[H] Wikidata `wbgetentities`** — maximal SINGLE-DOCUMENT amorphousness. Live-measured: depth **13**, **42/50 distinct claim-property keysets** from 50 entities, Item/Property/Lexeme roots, polymorphic values (time/quantity/coord/monolingual/external-id/geo-shape…), **30-80 meaningful families**. **Perfect curl+jq fit** (two bounded calls). **Catch: 72% of live entities are humans → mandatory PII quarantine; 1-2 MiB payloads; a sampler, not a firehose.**
- **GitHub events** (Claude's other candidate) is **out** — the 2025 payload-trim gutted it: live sample was PushEvent-dominated (95/100), depth 1. No longer amorphous.

**Recommendation:** **Bluesky Jetstream is THE final boss** — the truer *amorphous firehose*: many genuinely unrelated apps, real-time discovery + drift/evolution, cleaner families, lower PII — accept one small infra add (a WS-capable collector). **Wikidata is the curl+jq-native "hard mode"** — no new infra, the ultimate dynamic-map-key + quarantine stress test. They showcase *different* Deblob capabilities; ingesting both is defensible.

## The two finalists
### 🥇 Bluesky Jetstream `[C, live-verified]`
- **Endpoint:** `wss://jetstream2.us-east.bsky.network/subscribe` (4 official public instances, no auth). Optional `?wantedCollections=` filter (omit = ALL).
- **Why maximally amorphous:** one firehose carries every app built on ATProto. Each record self-labels via `$type`/`collection` NSID; lexicons are open + evolving (new apps → new schemas appear live). Envelope kinds: commit/identity/account. Live 9-sec sample: **19 collections, depth 8**, spanning social + IoT + music + podcast + productivity + livestream + community apps.
- **Family estimate:** 19 in 9 s → **50-100+ distinct families/hour**, growing as new lexicons launch. Showcases discovery + clustering-by-type + **drift/evolution in real time** + quarantine (deletes/malformed).
- **curl+jq fit:** ✗ native — it's WSS. Fix = add a WS reader. Two clean options: (a) **add `websocat` (single static binary) to the greenwindow collector image** → bounded CronJob `timeout 60 websocat wss://… | jq -c '{records:[…]}' | curl POST redpanda` (matches the existing pattern); or (b) a small always-on **streaming consumer Deployment** (WS → Redpanda), which is architecturally right for a firehose and gives Deblob its first true stream source.
- **PII/ToS:** records include DIDs (pseudonymous) + user content (post `text`, profile `description`). Deblob discards raw after shape extraction, so **shape discovery needs no values**. Guards: do NOT add Jetstream to `[samples].capture_sources`; **exclude it from the NAS Bronze archive** (or jq-null free-text leaves) so user content never lands on disk. No auth, public by design.

### 🥈 Wikidata canonical entity JSON `[H, live-verified]`
- **Endpoints (two bounded calls/CronJob):** discover changed IDs → `w/api.php?action=query&list=recentchanges&rcnamespace=0&rctype=edit|new&rcprop=title|ids|timestamp|flags&rclimit=50&format=json&formatversion=2&maxlag=5`; then fetch → `w/api.php?action=wbgetentities&ids=Q…|Q…&props=info|claims&format=json&formatversion=2&maxlag=5` (batches of **5-10**, not the 50 max — payloads are 1-2 MiB). No key; identified `User-Agent` + `Accept-Encoding: gzip` + honor `429`/`Retry-After`/`maxlag`.
- **Why maximally amorphous:** `claims` is a dynamic map keyed by thousands of Property IDs; statements carry qualifiers/references/`somevalue`/`novalue`; values are polymorphic (entity/time/quantity/coordinate/monolingual/url/external-id/media/math/geo-shape/tabular); three roots (Item/Property/Lexeme, Lexemes add lemmas/forms/senses). Depth **13**. Live: **42/50 distinct claim keysets**.
- **Family estimate:** **30-80 meaningful** (astronomy, proteins/genes, taxa, diseases, chemicals, geo, orgs, creative/scholarly works, software, events, Properties, Lexemes) — **but hundreds+ raw** if Deblob keys families on literal Property IDs. This is itself the stress test: **does Deblob over-split on dynamic map keys?**
- **curl+jq fit:** ✓ excellent (native HTTP JSON). Also a bonus real-world **quarantine fixture**: `maxlag` returns HTTP 200 with a structured `.error` body — Deblob must inspect the body, not just the status.
- **PII/ToS:** CC0 data, but **36/50 live entities were humans**. Mandatory gate (Hermes): drop RecentChanges `user`/`comment`; **quarantine ALL humans** (`claims.P31[].mainsnak.datavalue.value.id == Q5`) + untyped Items; omit labels/aliases/descriptions/sitelinks; quarantine unknown literal/URL/external-id fields until classified.

## Conflicts & adjudication
The agents disagree, cleanly, on the decisive axis:
- **Fit to the existing curl+jq CronJob pattern** → **Wikidata wins** (native HTTP; Jetstream needs a WS reader).
- **True "amorphous firehose" + real-time evolution + PII-cleanliness** → **Jetstream wins** (many unrelated live apps, self-describing, mostly-structural; Wikidata is a human-dominated *sampler* with family-explosion risk).

**Verdict:** For a "show the true capabilities" showpiece, **Jetstream edges it** — a live stream where you can literally watch dozens of unrelated app-schemas get discovered, clustered, and drift, is the most vivid demonstration, and the WS reader is a one-time ~1-line image add (or a small consumer). **Wikidata is the strongest choice if "no new infra / strict curl+jq" is a hard rule** — and it uniquely stresses two capabilities Jetstream doesn't: dynamic-map-key handling (over-split resistance) and PII quarantine at scale. Reasonable to ship **Jetstream as the boss now, Wikidata as hard-mode next.**

## Also-rans (Hermes, live-checked)
- **Crossref `/works`** — safest one-call fallback (no PII-human dominance): 83 distinct top-level keysets/100 works, 30 declared work types, depth 7, but bounded by one bibliographic super-schema (**20-40 families**). Drop `abstract` (copyrighted JATS-in-JSON), contributor identity.
- **Data.gov Catalog v4** — needs a free key; DCAT envelope limits it (**10-30**).
- **OpenAlex** — clean but typed fan-in across 6 endpoints, not one polymorphic stream (**7-15**).
- **DBpedia entity JSON** — amorphous but *noisy*: pulls the whole linked graph (COVID-19 = 41,904 subjects), depth only 4.
- **Wikipedia REST summary** — great fit but intentionally regular (**2-5**).

## Method note
Split: Claude = event-stream firehoses (Jetstream, GitHub events, Wikimedia EventStreams, JSON-LD), quantified live via a WS probe; Hermes = knowledge-graph amorphousness (Wikidata/Crossref/OpenAlex/DBpedia/data.gov/Wikipedia), all live-checked with code + a full PII-quarantine design, report also in its vault `research/Deblob-Final-Boss-Amorphous-Source-JR-210956.md`. Dispatched ~09:58 CEST; Hermes COMPLETE 7/7. No timeout.
