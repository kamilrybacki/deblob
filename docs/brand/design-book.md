# Deblob Design Book

- **Status:** v1 baseline (Hermes `deblob-identity-01`, 2026-07-14). **Mascot revision in progress** (`deblob-identity-02`): client direction is mascot-forward, referencing the Poring/Ditto blob-creature archetype (evoke, never copy). Sections marked ⚠ are expected to change.

## 1. Identity concept — The Schema Imprint ⚠ (mascot revision pending)

Core metaphor: an irregular blob passes through a boundary and leaves a precise, permanent imprint. The mark combines: organic left edge (unknown input), ordered right edge (deterministic structure), concentric internal lines (fingerprint unique to the schema). Identity is about **recognition and permanence**, not "AI transformation."

Degradation ladder: full logo (mark + lowercase `deblob` wordmark) → project mark alone → favicon (solid teal tile, one white contour, squared endpoint) → 16 px monochrome silhouette → terminal `[~>#] deblob` → plain-text `blob ~~~> [sch]`.

## 2. Logo / mark ⚠ (mascot revision pending)

**Selected: Schema Imprint** — near-square on 24×24 grid; left boundary two restrained Bézier bulges; top/bottom/right resolve into rounded rectangular tile (corner radius 3u); three negative-space contours arc from left, right endpoints snap to vertical grid; one small square endpoint = assigned identity. Internal stroke 2u @ 24px; clear space ≥ half mark width; never rotate (left→right transformation is the meaning); monochrome master is canonical.

Rejected: Tagged Blob (reads as luggage tags), Lane Convergence (kept as secondary diagram language for hot/cold-path illustrations only).

Wordmark: `[mark] deblob` — always lowercase, no stylized capital D, no letterform gimmicks (braces/binary/circuits).

## 3. Palette

### Dark theme
| Token | Name | Hex | Role |
|---|---|---|---|
| canvas | Vault 950 | `#0B1118` | main background |
| surface | Vault 900 | `#111A24` | cards, CLI blocks |
| surface-raised | Slate 800 | `#182430` | raised panels |
| border | Slate 650 | `#2B3A49` | rules, inactive |
| text | Mist 100 | `#E7EDF3` | primary text |
| text-muted | Mist 400 | `#9AA8B6` | secondary text |
| identity | Imprint Teal | `#35D0B2` | approved schema identity |
| hot | Relay Amber | `#F5A524` | synchronous/hot path |
| cold | Discovery Blue | `#5AA7FF` | cold discovery lane |
| danger | Quarantine Red | `#F06A6A` | invalid/quarantined |
| provisional | Candidate Amber | `#FFC857` | `cand_` identities |

### Light theme
| Token | Name | Hex | Role |
|---|---|---|---|
| canvas | Mist 050 | `#F7F9FB` | main background |
| surface | White | `#FFFFFF` | cards |
| surface-raised | Mist 100 | `#EEF2F6` | secondary panels |
| border | Slate 200 | `#D6DEE6` | rules |
| text | Vault 900 | `#17202A` | primary text |
| text-muted | Slate 600 | `#556575` | secondary text |
| identity | Registry Teal | `#087F6D` | approved schema identity |
| hot | Relay Ochre | `#A85A00` | hot path |
| cold | Discovery Blue | `#1769AA` | cold path |
| danger | Quarantine Red | `#B4232A` | invalid/quarantined |
| provisional | Candidate Ochre | `#8A5A00` | `cand_` identities |

### Semantic rules
- **Teal** = approved schema identities + successful registry resolution. **Amber** = hot path + provisional. **Blue** = async discovery/sampling/SLM. **Red** = malformed/rejected/quarantined only — never ordinary drift.
- Neutral slate ≥ 70% of any interface.
- WCAG AA for all text/controls; darker light-theme accents for text, bright accents for icons/borders/fills on dark.
- Never color-only state: approved = teal + check/imprint; provisional = amber + dotted outline; discovery = blue + branch icon; quarantine = red + stop icon. Charts vary line style/marker too.
- 2 px focus ring, 2 px offset. CLI honors `NO_COLOR`.

## 4. Typography ⚠ (may soften with mascot direction)

- **Display — Space Grotesk** (OFL), weights 600/700: project name, landing headings, social cards.
- **Body — Source Sans 3** (OFL), 400/600/700: docs, README prose, UI labels.
- **Mono — IBM Plex Mono** (OFL), 400/600, tabular numerals: CLI, schema IDs, offsets, metrics, config.

Rules: identifiers (`sch_…`, `cand_…`, `fam_…`) always mono; never uppercase the wordmark; sentence-case headings; prose ≤ ~72ch; schema IDs truncate mid, never at prefix: `sch_7M4K2W…F9Q`.

## 5. Voice & naming

Voice: careful data-plane operator — terse, calm, concrete, evidence-first; slightly playful only in project-level copy, never in errors.

Good: `Schema matched: sch_7M4K2W` · `Candidate staged from 842 records.` · `Registry unavailable; record tagged unresolved.` · `Promotion refused: required field removed.`
Bad: `Amazing! Your blob has been deblobbed ✨` · `AI confidence is 97%.` · `Oopsie—Redis went away!`

Tagline: **Give every blob a permanent shape.**
Descriptor: *Continuous schema discovery and identity tagging for uncontrolled data.*

### CLI hierarchy
```
deblob relay kafka | proxy http | inspect
deblob schema show|verify · candidate list|promote|reject · family history
deblob vault doctor · config check
```
Nouns for managed objects, verbs for actions; lowercase kebab flags; singular resource nouns; `show` (never mixed get/describe/view); governance actions are explicit verbs; never `approve-ai`/`accept-model`.

### Error codes
`DBL-<range><nn>` — 1xxx input/decoding, 2xxx vault/identity, 3xxx transport/relay, 4xxx policy/promotion, 5xxx semantic inference. Examples: `DBL-1101 duplicate JSON key`, `DBL-2202 immutable schema conflict`, `DBL-3103 relay transaction aborted`, `DBL-5203 SLM returned unknown schema ID`. Every error: stable code, one-line cause, structured context, actionable next step.

### Metrics
Prefix `deblob_`; base units; counters `_total`; durations `_seconds`. Never put schema/candidate/producer IDs, topics, or error messages in labels — IDs belong in logs/traces. Canonical set: `deblob_relay_records_total`, `deblob_relay_transactions_total{result}`, `deblob_schema_matches_total{result}`, `deblob_candidates_active`, `deblob_candidate_promotions_total{result}`, `deblob_cold_lane_lag_records`, `deblob_registry_operation_duration_seconds`, `deblob_slm_decisions_total{decision}`, `deblob_quarantine_records_total{reason}`.

## 6. Applications

- **README header** (~1200×360): Vault 950 bg; amber irregular records enter left, one teal imprint emerges as `sch_…`, thin blue branch drops to discovery lane; no screenshots/model logos/badge walls; install visible without deep scroll. ⚠ hero composition may gain mascot.
- **CLI `--help`**: `[~>#] deblob 0.1.0 / shape → identity` banner; command names bold neutral; `sch_` teal, `cand_` amber, cold-lane blue; errors red prefix only; no ANSI when piped/JSON/`NO_COLOR`; no logo on normal relay startup.
- **Docs site**: dark-first + full light mode; neutral nav; teal = canonical contracts + selected nav; diagrams: solid amber = sync, dashed blue = async, teal tile = immutable identity, red branch = quarantine; slate code theme; every diagram legible in monochrome; one standard lifecycle diagram reused.
- **Grafana**: accents only, no reskin. Teal exact/approved, amber provisional/hot saturation, blue discovery throughput/lag, red quarantine/persistence/txn aborts, gray totals. Grafana green reserved for infra health — never for schema identity.
- **Stickers**: die-cut mark, teal/white/Vault-950 only, ≥32 mm; secondary maintainer sticker `[~>#] GIVE BLOBS SHAPE` (Plex Mono uppercase — merch only). ⚠ mascot sticker incoming.
- **Social card** (1200×630): left mark+name+tagline; right `{ messy payload } → cand_… → sch_…` amber→teal with thin blue annotation; repo URL small; no feature checklist.

## 7. Anti-patterns ⚠ (item 6 overruled by client; revision pending)

1. Generic AI branding — no glowing brains, node clouds, sparkles, magic-wand transforms, robot mascots.
2. Crypto aesthetics — no cyan-purple gradients, glass hexagons, coins, chain cubes, neon "protocol" pages.
3. Liquid-chrome blob art — blob is input state, not the brand's final form; no glossy 3D metaballs.
4. Biometric claims — imprint stays abstract; never a realistic human fingerprint; never imply authn.
5. Vault clichés — no padlocks, vault wheels, shield-around-database.
6. ~~No playful blob mascots~~ — **OVERRULED 2026-07-14**: identity is mascot-forward per client (Poring/Ditto archetype, original species). Retained spirit: mascot never appears in errors, quarantine, security messaging, or operational dashboards.
7. Dense architecture diagrams as logos — logo must survive 16 px, one color.
8. Rainbow semantic overload — teal/amber/blue/red roles fixed; extra series use neutral ramps.
9. AI-generated inconsistency — one SVG master, one monochrome master, tokenized colors, documented clear space.
10. Cute operational language — no "goo", no "blob jail", no "making it official"; humor lives in stickers/release notes only.
