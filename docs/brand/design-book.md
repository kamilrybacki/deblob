# Deblob Design Book

- **Status:** v2 — mascot-forward identity (Hermes `deblob-identity-02`, 2026-07-14). Supersedes v1's Schema-Imprint-only direction per client override: identity references the Poring/Ditto blob-creature archetype — **evoked, never copied** (original species, no Poring drop silhouette, no Ditto face).
- v1 elements retained: full semantic palette, voice, error codes, metric naming, application rules (§ below). v1's abstract "Schema Imprint" mark survives as the schema core inside the mascot and as diagram language.

## 1. Mascot — The Imprintling

An original gelatinous creature that cannot hold a stable outer shape but carries one perfectly rigid **schema core** inside its body. The visual gag: *the body mimics shapes; the core records the shape that survived validation.* It mimics and proposes; deterministic code fingerprints and policy decides — the mascot is never portrayed as omniscient.

**Personality:** curious, industrious, eager to imitate; forms an extra lobe or wrong corner before settling; proud when its core receives an identity; abstention is in-character — when uncertain the core stays blank.

**Selected form (Option A):** low, WIDE gelatinous body (not a teardrop), irregular perimeter, crisp square schema core visible in the abdomen. Shallow two-lobe top, no pointed apex. One optional pseudopod for gestures. Asymmetric face: left eye vertical rounded capsule, right eye small rounded square, mouth open diamond/chevron — **never two dots + straight line** (Ditto trade dress). Rejected: Mimic Mantle (tile-on-back reads as accessory), Lane Jelly (color/transparency-dependent, dies at favicon size).

**State variants:** unknown = blank core · candidate = amber dotted core with `?` · matched = teal/white core with grid · promoted = core emits one restrained square "stamp" · abstain = body looks sideways, core shows em dash (not a sad face) · malformed = **no mascot** — operational UI handles it plainly.

### Construction (master 32×32 grid)

- Body bounds x=2–30, y=5–27; flat visual base ~y=26 (no spherical underside).
- Top contour: two broad lobes joined by a shallow saddle; left/right contours unequal; max three major bulges total; no sharp points; body radii ≈ 6–9 units.
- Schema core: 9×9 units, optical center ≈ (17,18), radius 2 units, **axis-aligned even when the body leans or stretches**.
- Outline 1.5 units at master size. Pseudopod stays inside a 34×32 extended box; removed from the canonical mark.

### Degradation ladder

| Context | Treatment |
|---|---|
| Hero illustration | full body, face, core, pose, mimicry gag |
| Primary mascot mark | neutral pose, face, schema core |
| 24–32 px | silhouette + core; drop mouth and minor contour |
| 16 px favicon | teal asymmetric silhouette with square core cutout |
| Monochrome | solid body with negative-space core (primary reproduction test) |
| Unicode terminal | `(~▦)` |
| Strict ASCII | `(~#)` — a signature, not a portrait; no large terminal mascot art |

## 2. Palette

**Decision: the creature stays teal.** Pink/lavender body would sit too close to the cited characters and break the semantic system. Emotional register comes from silhouette, motion, expression — not borrowed color. Lilac = illustration-only secondary accent.

### Mascot colors

| Token | Name | Hex | Use |
|---|---|---|---|
| mascot-body-dark | Imprint Teal | `#35D0B2` | body on dark |
| mascot-body-light | Registry Teal | `#20B89A` | body on light |
| mascot-shadow | Deep Jelly | `#148F7A` | lower body shadow |
| mascot-highlight | Jelly Mint | `#A6F3E2` | highlight, ≤15% of body |
| mascot-outline-dark | Vault Ink | `#0B1118` | outline + face |
| mascot-outline-light | Deep Teal | `#056B5C` | outline on light |
| mascot-core-light | Core White | `#F7F9FB` | schema core |
| mascot-core-dark | Core Slate | `#17202A` | inverted core |
| mascot-accent | Mimic Lilac | `#B9A7FF` | illustration props + release art ONLY |

### Semantic palette (unchanged from v1)

| Role | Dark | Light |
|---|---|---|
| canvas | Vault 950 `#0B1118` | Mist 050 `#F7F9FB` |
| surface | Vault 900 `#111A24` | White `#FFFFFF` |
| surface-raised | Slate 800 `#182430` | Mist 100 `#EEF2F6` |
| border | Slate 650 `#2B3A49` | Slate 200 `#D6DEE6` |
| text | Mist 100 `#E7EDF3` | Vault 900 `#17202A` |
| text-muted | Mist 400 `#9AA8B6` | Slate 600 `#556575` |
| identity | Imprint Teal `#35D0B2` | Registry Teal `#087F6D` |
| hot | Relay Amber `#F5A524` | Relay Ochre `#A85A00` |
| cold | Discovery Blue `#5AA7FF` | Discovery Blue `#1769AA` |
| provisional | Candidate Amber `#FFC857` | Candidate Ochre `#8A5A00` |
| danger | Quarantine Red `#F06A6A` | Quarantine Red `#B4232A` |

**Rules:** mascot body never turns red/amber/blue to represent state — state lives in the schema core, an adjacent badge, or a caption. Mimic Lilac never in charts, alerts, CLI state, or policy controls. No pink cheeks in the canonical mark (limited lilac cheek/prop detail OK in large editorial illustrations). Neutral slate ≥70% of any interface; WCAG AA; never color-only state (approved = teal + imprint icon, provisional = amber + dotted outline, discovery = blue + branch, quarantine = red + stop); 2 px focus ring / 2 px offset; CLI honors `NO_COLOR`.

## 3. Typography

| Role | Face | License | Weights | Use |
|---|---|---|---|---|
| Display + wordmark | **Fredoka** (replaces Space Grotesk) | OFL | 600 wordmark, 600–700 hero | wordmark, landing headings, release cards, stickers |
| Body | Source Sans 3 | OFL | 400/600/700 | docs, UI, release notes |
| Mono | IBM Plex Mono | OFL | 400/600 | CLI, identifiers, config, metrics |

Deliberate contrast: *Fredoka = creature and personality · Source Sans 3 = explanation and governance · IBM Plex Mono = machine identity.*

Fredoka restrictions: no ultra-bold inflation; never for long documentation text; no all-caps except short merch copy; wordmark starts from Fredoka SemiBold but is a controlled logo asset, not live text. Identifiers (`sch_…`, `cand_…`, `fam_…`) always mono; mid-truncation only (`sch_7M4K2W…F9Q`); sentence-case headings; prose ≤ ~72ch.

## 4. Mascot placement rules

**Full mascot encouraged:** README hero, landing page, social cards, stickers/shirts/conference material, release-note headers, 404, empty candidate list ("No candidates waiting. The Imprintling has nothing new to shape."), first-run onboarding, tutorial intros, non-critical doc callouts, unknown→candidate→schema explainers, community announcements.

**Simplified mark only:** docs navigation, favicon, repo avatar, CLI `--help` + version, Grafana dashboard title (once, monochrome, header only), footer, report headers.

```
(~#) deblob 0.2.0
     shape → identity
```

Never printed during daemon startup, health checks, or structured output.

**Mascot prohibited:** CLI errors, quarantine/malformed reports, security docs, vulnerability advisories, authn/authz failures, data-loss/durability warnings, Redis persistence failures, Kafka transaction aborts, incident notifications, Grafana alert panels, policy rejection dialogs, promotion approval controls, PII/evidence-retention screens, audit logs, all machine-readable output. There: neutral typography, stable codes, direct language — `DBL-2204 AOF persistence is unhealthy; promotions are frozen.` Never "The Imprintling is scared and cannot reach its vault!" No mascot expressions tied to health state anywhere.

## 5. Lockups

- **Primary horizontal** `[Imprintling] deblob`: mascot height 1.25× wordmark cap height; wordmark starts 0.45× mascot body width right; optical baseline aligned to mascot physical baseline; core center near wordmark x-height center; mascot faces three-quarters TOWARD the wordmark; no tagline inside primary lockup.
- **Compact** (nav/repo headers): simplified mark (no mouth/pseudopod), 1.1× cap height. **Vertical** (stickers/avatars/square social): mascot centered above, gap = half core width.
- **Clear space:** `C` = schema core width; ≥1C every side; co-branding separator ≥1.5C away.
- **Minimum sizes:** full horizontal 120 px, compact 80 px, mascot alone 32 px; <24 px use simplified core-and-silhouette; 16 px favicon master only.
- **Wordmark:** always lowercase `deblob` (body copy spells "Deblob"); manual kerning on `de`/`bl`/`ob`; no `b`→mascot swap; no `o`→eye/core/blob; no braces/brackets/cursor decoration. Mascot and wordmark may each appear alone.

## 6. Voice & naming (v1, unchanged)

Careful data-plane operator: terse, calm, concrete, evidence-first; playfulness only in project-level copy, never errors.

Tagline: **Give every blob a permanent shape.** Descriptor: *Continuous schema discovery and identity tagging for uncontrolled data.*

CLI: nouns for objects, verbs for actions — `deblob relay kafka · proxy http · inspect · schema show|verify · candidate list|promote|reject · family history · vault doctor · config check`. `show` only (no get/describe/view mix); explicit verbs for governance; never `approve-ai`.

Error codes `DBL-<range><nn>`: 1xxx input/decoding, 2xxx vault/identity, 3xxx transport/relay, 4xxx policy/promotion, 5xxx semantic inference. Every error: stable code, one-line cause, structured context, actionable next step.

Metrics: `deblob_` prefix, base units, `_total` counters, `_seconds` durations; no IDs/topics/messages in labels. Canonical set: `deblob_relay_records_total`, `deblob_relay_transactions_total{result}`, `deblob_schema_matches_total{result}`, `deblob_candidates_active`, `deblob_candidate_promotions_total{result}`, `deblob_cold_lane_lag_records`, `deblob_registry_operation_duration_seconds`, `deblob_slm_decisions_total{decision}`, `deblob_quarantine_records_total{reason}`.

## 7. Applications (v1 base + mascot deltas)

- **README hero** (~1200×360): Vault 950 bg; Imprintling watches amber irregular records enter left, one teal `sch_…` imprint emerges, thin blue branch to discovery lane; no badge walls; install visible early.
- **Docs site:** dark-first + full light; diagrams: solid amber = sync, dashed blue = async, teal tile = immutable identity, red branch = quarantine; monochrome-legible; one standard lifecycle diagram reused.
- **Grafana:** accents only — teal exact/approved, amber provisional/hot, blue discovery, red quarantine/aborts; Grafana green reserved for infra health; monochrome mark once in header, nothing in panels.
- **Stickers:** die-cut Imprintling (teal/white/Vault 950), ≥32 mm; maintainer sticker `(~#) GIVE BLOBS SHAPE` (Plex Mono uppercase — merch only).
- **Social card** (1200×630): left lockup + tagline; right `{ messy payload } → cand_… → sch_…` amber→teal, thin blue annotation, repo URL small.

## 8. Anti-patterns (v2)

1. **No copying the reference characters:** no Poring pointed-drop silhouette / centered apex / pink bouncy sphere; no Ditto two-dot-eyes-straight-mouth / flat lavender body / recognizable-Pokémon transform poses; no franchise names, staging, or merch layouts. The Imprintling stays wide, asymmetric, core-bearing.
2. **No generative kawaii drift:** canonical model sheet (front, three-quarter, side silhouette, face construction, core dimensions, 5 approved expressions, 5 approved deformations, exact palette, forbidden silhouettes) — generated art is redrawn/checked against it; never publish first-pass image-model output as canonical.
3. **No inconsistent anatomy:** one body; ≤1 pseudopod; no fingers/feet/tail/permanent ears/horns/antennae/crown; core rigid + axis-aligned; face geometry fixed. *The body may deform; the species may not.*
4. **No kawaii operational theater:** no crying mascot for outages, angry for malformed, sleeping for paused relays, jailed for quarantine; mascot never asks users to approve schemas; no cute language around PII/data loss/security/compat failures.
5. **No AI-magic vocabulary:** no "magically understands", "AI discovers the truth", sparkles on promotion, neural backgrounds, glowing brains. Mascot mimics and proposes; deterministic code fingerprints; policy decides.
6. **No glossy blob cliché:** no liquid chrome, photoreal slime, heavy transparency, metaballs, lava-lamp animation, food-gelatin. Flat vector fills, one shadow, one highlight max.
7. **No pastel semantic collapse:** teal/amber/blue/red roles fixed; lilac decorative only; never recolor operational state to match an illustration.
8. **No infantilized documentation:** mascot may introduce a concept, then technical content starts immediately; no speech bubbles through reference docs.
9. **No mascot-first security posture:** trust comes from explicit behavior, auditability, stable identifiers, compatibility policy, test evidence, clear failure modes. *Cute is the invitation — not the assurance.*
10. **No uncontrolled variants:** official assets = canonical teal, monochrome, candidate-core, approved-core, five editorial poses, silhouette-preserving seasonal variants. **The schema core is the non-negotiable identifier — an illustration without it is a generic blob, not Deblob.**
