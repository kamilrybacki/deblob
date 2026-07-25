# Deblob clean-rebuild data-quality — Joint Research Report
run: jr-deblob-rebuild-221850 · 2026-07-22 · agents: Claude Code + Hermes

## Executive summary
Full from-scratch wipe (Redis 21,152→0, samples→0, 44 Redpanda topics deleted+recreated),
then a ~1h quality assessment of the rebuilt corpus. **Verdict: operationally healthy
rebuild** — and the from-scratch run surfaced two real defects, both fixed this session:
1. **Domain-gate promotion/naming race** (my find) → cross-domain umbrella leaked → fixed **b30** (read promote-stamped `provenance.source`, not namer-async `name_meta.source`).
2. **Retention-backstop regression** (Hermes find, I verified+fixed) → the wipe dropped `retention.bytes` caps on gdansk/wikidata → reapplied live + made durable in topic-bootstrap.

## Corpus metrics (~1h post-wipe) `[C]`
| Metric | Now | Pre-wipe |
|---|---|---|
| Published schemas | 19 | 42 |
| Families | 19 | 42 |
| Annotated | **19/19** | 40 |
| Value-profiled | **19/19** | — |
| Umbrellas | 4 (1 rejected as cross-domain) | 6 |
| Candidates (forming, 7d TTL) | 142 | — |
| Relay lag | **0** | 0 |
| Quarantine | **0** | 0 |
| Sources producing | 24 | — |
| Naming: heuristic / slm / human | 18 / **1** / 0 | 41 / **0** / 1 |

## Key findings

### 1. Domain-gate leak — found + fixed (b30) `[C]`
On the rebuild, schemas promote *faster than the namer names them*. The gate read the
ingest source from `name_meta.source` (namer-async), so a promoted-but-unnamed schema
read as domain-unknown and **slipped past the gate** → a cross-domain umbrella leaked:
`umb_ec77735649` = `events.energy.pse-pl` + `events.carbon.fr` (Energy) + `events.weather.metno`
(**Geo**). **Fix:** prefer `provenance.source` (stamped at PROMOTE, b23, always present) via a
shared `provenance_source` helper for both the neighbor gate and the umbrella gate.
**Validated:** post-b30 re-propose SUPPRESSED `umb_ec77735649` (2 clusters, 3 members); the
stale leaked provisional was rejected. Neighbor gate remained correct throughout (RunPod →
compute-only, enforced).

### 2. Retention-backstop regression — flagged + verified + fixed `[H→C]`
Topic recreate dropped the `retention.bytes` caps that had been set by one-off `rpk` on the
high-volume durable topics; `topic-bootstrap` only durably reapplied firehose's `retention.ms`.
**Verified:** `events.transit.gdansk` (264k/day, NAS-archived) and `events.kg.wikidata` both had
`retention.bytes = -1` (unbounded, 30-day). **Fixed:** reapplied 512 MiB/partition live + added
the alter-config to `topic-bootstrap` so future recreations keep it. (`retention.bytes` is
per-partition → ×4 ≈ 2 GiB/topic ceiling.)

### 3. Naming — 18/1 is healthy, not a failure `[H]`
The design is `human > accepted-SLM-refinement > heuristic`. A final `heuristic` may mean the
SLM repeated the heuristic, was absent/timed-out, or failed a deterministic gate — so **1/19 is
the accepted-divergent-proposal rate, not the SLM call-success rate.** Judge the SLM by
human-accept-without-edit + hallucination, NOT override frequency; do **not** push the 0.5B for
more output. The ollama emptyDir fix is *proven* (1 accepted SLM name vs 0/42 before). `[C+H]`
- **Mechanical-token gap** (`Grid Utc Dtime Records`, raw Polish IMGW tokens): fix
  **deterministically** with a versioned, source-aware token dictionary — preserve acronyms
  (UTC/GPU/CPU/IMGW), canonicalize `dtime`/`datetime` as temporal plumbing, add source-scoped
  Polish aliases, refine `events.env.imgw → Environmental` to hydro/synoptic. The
  normalizer-version idempotency can trigger a safe rename. NOT a rebuild-health blocker. `[H]`

### 4. Steady-state estimate `[H]`
19 schemas in 1h is strong. Expected mature: **~30–45 at 24–48h, ~40–55 at 7d** (envelope 35–60).
40 sources ≠ 40 schemas (some below threshold, some emit multiple shapes). Candidate count (142)
is benign cold-start variation (7-day TTL) — **not** a quality metric. `<30 at 48h with 35+
sources producing` would warrant inspecting promotion-rejection reasons.

### 5. Breadth skew — a storage/sampling concern, not a schema-quality one `[C+H]`
firehose = 1.1M/day (920× ai.papers, ~70% of traffic) but its policy is already right (1h
retention, excluded from samples + NAS archive; effective window ~46k records). After 50
observations a repeated shape adds little schema evidence; low-volume authoritative sources
carry more semantic breadth per record. Transit is the next durable-volume contributor (in the
archive) — now capped (finding #2).

## Conflicts & adjudication
None material. Hermes' storage-regression hypothesis ("verify — I did no live inspection") was
**confirmed** by my live `rpk topic describe` (retention.bytes = -1) and fixed. Hermes noted
"before activating IDF, ensure df is schema/source-based, not raw-event-frequency" — our IDF df
*is* per-schema (`SCARD` of the posting set), so Jetstream repetition can't dominate it; concern
already handled (IDF stays dormant regardless). `[C]`

## Health `[C]`
deblob b30 1/1 · relay lag 0 · quarantine 0 · auto-promote firing (gated ≥50 samples, saw 71) ·
19/19 value-profiled · no errors/panics/OOM (the two "Error" pods are stale pre-wipe job history).

## Action items
- ✅ Domain-gate leak fixed (b30, deployed) + leaked umbrella rejected.
- ✅ Retention backstop reapplied (live + durable).
- ⏳ **Deterministic naming token-dictionary** (acronyms, `dtime`, source-scoped IMGW/Polish) —
  Hermes-recommended, deterministic, normalizer-version-idempotent. Not urgent.
- 🔭 Re-check corpus at 24–48h vs the 30–45 window; watch transit disk under the new cap.

## Sources
Live: `rpk topic describe`, deblob mgmt API, redis-vault, deblob logs. Deblob @ main (b29→b30).
Hermes vault: `research/Deblob-Clean-Rebuild-Health-JR-221850.md` + prior naming/domain-gate/
umbrella design reports. External: Kafka topic-configs; JSONoid streaming schema discovery
(arXiv 2307.03113).

## Method note
Full wipe by Claude (user-authorized, "everything incl. event buffer"). Claude covered live
metrics + similarity/domain-gate/umbrella verification + both fixes; Hermes covered
rebuild-health judgment, naming philosophy, steady-state estimate, and the retention regression.
~15 min parallel, Hermes returned COMPLETE.
