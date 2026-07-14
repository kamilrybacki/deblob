# Deblob — Unified Claude Design Brief

One document, two deliverables, run in order. Normative source is `design-book.md` (v2, Imprintling). This brief merges the **design-system reference page** (Claude-authored) with the **mascot illustration set** (Hermes-authored, run `deblob-mascot-prompt-01`). Produce the mascot art first, then the design-system page, then swap the real mascot into the page's empty-state slot.

Shared palette (every prompt below uses these exact values):

| Token | Hex | Token | Hex |
|---|---|---|---|
| Imprint Teal (body/identity dark) | `#35D0B2` | Registry Teal (body/identity light) | `#20B89A` / `#087F6D` |
| Deep Jelly (shadow) | `#148F7A` | Jelly Mint (highlight) | `#A6F3E2` |
| Vault Ink (outline/face) | `#0B1118` | Core White | `#F7F9FB` |
| Relay Amber (hot/provisional) | `#F5A524` | Candidate Amber (core border) | `#FFC857` |
| Discovery Blue (cold) | `#5AA7FF` | Quarantine Red (danger) | `#F06A6A` |
| Mimic Lilac (illustration-only) | `#B9A7FF` | Vault 950 (canvas) | `#0B1118` |

---

# PART A — Mascot illustration set (run first)

Six prompts. Prepend the **Style preamble** to any of them. Order: hero → model sheet → 4 state variants → favicon. Each produces standalone art; the model sheet is the canonical reference all others must match.

## A0. Style preamble (prepend to every mascot prompt)

```text
Clean flat-vector mascot artwork, controlled Bézier curves, bold readable silhouette, consistent Vault Ink outline, one flat shadow shape maximum, one small highlight shape maximum. No gradients. No glossy rendering. No translucent slime. No wet texture. No chrome. No 3D. No airbrush. No painterly texture. No photorealism. No excessive internal detail. The silhouette and schema core must remain legible when reduced.

ORIGINALITY AND IP GUARDRAILS:
Create a new species. Evoke only the broad emotional category of a friendly gelatinous fantasy creature. Do not imitate, reference, remix, or resemble any existing game, anime, or media character. Specifically avoid a pointed drop silhouette, centered apex, pink spherical drop, flat lavender transform-creature body, two-dot eyes, straight-line mouth, or recognizable franchise pose.

NEGATIVE CONSTRAINTS:
no teardrop, no water-drop silhouette, no pointed top, no centered apex, no pink blob body, no lavender blob body, no two-dot eyes, no straight-line mouth, no Ditto-like face, no Poring-like shape, no Pokémon likeness, no Ragnarok likeness, no franchise likeness, no arms, no hands, no fingers, no legs, no feet, no tail, no ears, no horns, no antennae, no crown, no wings, no slime drips, no glossy slime, no liquid chrome, no gelatin food texture, no transparency, no gradients, no sparkles, no hearts, no neural-network motif, no robot body, no text unless explicitly requested, no wordmark, no watermark
```

## A1. Master hero

```text
Create original production-ready brand artwork for "The Imprintling," the Deblob open-source project mascot.

CHARACTER:
A friendly gelatinous creature with a wide, low, asymmetrical body. The body has a flat visual base, a shallow two-lobe top joined by a soft saddle, and unequal side contours. It is not round, not teardrop-shaped, and has no pointed apex. The creature has no feet, legs, arms, fingers, tail, ears, horns, or antennae.

FACE:
One narrow vertical rounded-capsule left eye, one small rounded-square right eye, and one small open-diamond mouth. Preserve the asymmetric face exactly. Do not use two dots and a line.

SCHEMA CORE:
Embed one rigid, perfectly axis-aligned rounded square in the abdomen. Fill the core with Core White #F7F9FB, outline it in Vault Ink #0B1118, and place a simple teal schema grid inside: two horizontal divisions and one short vertical division, using Imprint Teal #35D0B2. The core represents a successfully matched schema. It must look crisp and geometric, not electronic, holographic, glowing, or detached.

POSE AND COMPOSITION:
Full-body neutral three-quarter pose. The creature faces toward the right, where a future lowercase wordmark would sit, but include no text or wordmark. Give the body a subtle rightward visual attention without adding a neck or limbs. Keep the mascot entirely visible. Place it slightly left of center with generous empty negative space on the right. Calm, curious, competent expression—not excited, sleepy, sad, or triumphant.

GEOMETRY:
Base the character on a 32×32 construction grid. Approximate body bounds: x=2–30 and y=5–27. Schema core approximately 9×9 grid units, optically centered near the lower-middle abdomen. Use controlled Bézier curves. Keep the core axis-aligned even though the body is shown in three-quarter pose.

PALETTE:
- Body: Imprint Teal #35D0B2
- Lower body shadow: Deep Jelly #148F7A
- Single small highlight: Jelly Mint #A6F3E2
- Outline and face: Vault Ink #0B1118
- Core: Core White #F7F9FB
- Core grid: Imprint Teal #35D0B2
- Background: solid Vault 950 #0B1118
Do not use Mimic Lilac in this master hero.

STYLE:
Crisp flat-vector logo mascot illustration. Bold silhouette. Consistent outline. Exactly one flat shadow shape and one small highlight shape. No gradients, transparency, texture, lighting effects, or environmental scene. Suitable for an open-source README hero and scalable SVG recreation.

OUTPUT:
Wide 16:9 composition, preferably 1600×900 or larger. Flat solid background. No typography. No border. Keep the character's contour and schema core readable at 128 px high.

EXPLICIT NEGATIVE PROMPT:
no teardrop, no water-drop body, no pointed top, no centered apex, no spherical pink blob, no lavender transform-creature, no two-dot eyes, no straight-line mouth, no Ditto face, no Poring silhouette, no Pokémon likeness, no Ragnarok likeness, no existing franchise character, no arms, no hands, no fingers, no legs, no feet, no tail, no ears, no horns, no antennae, no crown, no wings, no slime drips, no glossy slime, no wet surface, no liquid chrome, no translucent gelatin, no 3D render, no gradients, no photorealism, no food, no sparkles, no neural network, no robot, no text, no logo wordmark, no watermark
```

## A2. Canonical model sheet

```text
Create a canonical flat-vector turnaround model sheet for an original mascot called "The Imprintling."

CHARACTER:
The Imprintling is a wide, low, asymmetrical gelatinous creature with a rigid square schema core embedded in its abdomen. Its body has a flat visual base, a shallow two-lobe top joined by a soft saddle, unequal side contours, and no pointed apex. It has no feet, legs, arms, fingers, tail, ears, horns, crown, or antennae.

FACE:
The left eye is one narrow vertical rounded capsule. The right eye is one small rounded square. The mouth is one small open diamond. Preserve these exact features in every view where the face is visible. Never use two dot eyes or a straight-line mouth.

SCHEMA CORE:
One rigid, axis-aligned 9×9-unit rounded square embedded in the abdomen. Core fill is Core White #F7F9FB with a Vault Ink #0B1118 outline. Inside it, use a minimal teal schema grid with two horizontal divisions and one short vertical division. The core remains geometrically square in every view; do not perspective-warp it into an ellipse or organic shape.

MODEL-SHEET LAYOUT:
Show exactly three equally scaled canonical views, left to right:
1. Flat front view.
2. Neutral three-quarter view facing right.
3. Clean right-facing side silhouette.
Use orthographic turnaround presentation, not three separate illustration styles. Place each view on the same baseline. Keep proportions, body mass, face placement, core size, outline thickness, and color identical across views. No action poses. No extra expressions. No accessories. No labels, arrows, measurements, title, or decorative typography.

CONSTRUCTION:
Use an implied 32×32 grid for each view.
- Body bounds approximately x=2–30, y=5–27.
- Flat baseline around y=26.
- Maximum three large contour bulges.
- Core approximately 9×9 units.
- One narrow capsule eye, one rounded-square eye, one diamond mouth.
- In side view, show the near-side edge of the embedded schema core without turning it into a protruding device.

PALETTE:
- Body: Registry Teal #20B89A
- Shadow: Deep Jelly #148F7A
- Highlight: Jelly Mint #A6F3E2
- Outline and face: Vault Ink #0B1118
- Core: Core White #F7F9FB
- Core grid: Imprint Teal #35D0B2
- Background: Core White #F7F9FB
- Optional thin construction dividers: neutral slate #D6DEE6
Do not use pink or lavender on the character.

STYLE:
Professional character-design reference sheet, crisp flat vector, controlled Bézier curves, consistent outline, one shadow shape maximum per view, one small highlight maximum per view. No gradients, no transparency, no glossy material, no 3D, no painterly texture.

OUTPUT:
Landscape 4:3 or 3:2 canvas, minimum 1800 px wide. Generous spacing between views. No cropping.

NEGATIVE PROMPT:
no teardrop, no pointed apex, no droplet, no pink spherical blob, no lavender blob, no two-dot eyes, no straight-line mouth, no Ditto-like face, no Poring-like silhouette, no Pokémon likeness, no Ragnarok likeness, no franchise character, no arms, no hands, no legs, no feet, no tail, no ears, no horns, no antennae, no crown, no wings, no slime drips, no glossy slime, no chrome, no translucency, no gradients, no 3D, no text, no annotations, no costume, no accessories, no inconsistent anatomy, no inconsistent proportions, no watermark
```

## A3. State variant — Unknown (blank core)

```text
Create a standalone square flat-vector mascot illustration of "The Imprintling" in the UNKNOWN schema state.

The Imprintling is an original wide, low, asymmetrical teal gelatinous creature. It has a flat visual base, a shallow two-lobe top with a soft saddle, unequal side contours, and no pointed apex. It has no arms, hands, legs, feet, tail, ears, horns, crown, or antennae.

Its face consists of exactly:
- one narrow vertical rounded-capsule left eye;
- one small rounded-square right eye;
- one small open-diamond mouth.
Do not use two dot eyes or a straight-line mouth.

Embed one rigid, perfectly axis-aligned rounded square schema core in the abdomen. For the UNKNOWN state, the core is completely blank: Core White #F7F9FB fill, Vault Ink #0B1118 outline, no grid, no icon, no question mark, and no glow. The blank core means no schema identity has been resolved.

Pose the full creature in a neutral three-quarter view facing right. Expression is attentive and curious, not confused or sad. Center it with generous padding.

PALETTE:
body #35D0B2; shadow #148F7A; one highlight #A6F3E2; outline and face #0B1118; blank core #F7F9FB; background #0B1118.

STYLE:
Crisp flat vector, controlled Bézier curves, bold readable silhouette, one flat shadow maximum, one small highlight maximum, no gradients, no transparency, no 3D, no glossy slime.

OUTPUT:
Square 1:1 composition, no text, no border, no wordmark, no watermark.

NEGATIVE PROMPT:
no teardrop, no pointed top, no pink blob, no lavender blob, no two-dot eyes, no line mouth, no Ditto face, no Poring shape, no franchise likeness, no arms, no hands, no legs, no feet, no tail, no ears, no antennae, no slime drips, no glossy slime, no liquid chrome, no translucent gelatin, no gradient, no sparkles, no sad face, no question mark, no schema grid, no text
```

## A4. State variant — Candidate (amber dotted core + `?`)

```text
Create a standalone square flat-vector mascot illustration of "The Imprintling" in the PROVISIONAL CANDIDATE schema state.

The Imprintling is an original wide, low, asymmetrical teal gelatinous creature. It has a flat visual base, a shallow two-lobe top joined by a soft saddle, unequal side contours, and no pointed apex. It has no arms, hands, legs, feet, tail, ears, horns, crown, or antennae.

Its face consists of exactly:
- one narrow vertical rounded-capsule left eye;
- one small rounded-square right eye;
- one small open-diamond mouth.
Never use two dot eyes or a straight-line mouth.

Embed one rigid, perfectly axis-aligned rounded square schema core in the abdomen. For the CANDIDATE state:
- Core fill: Core White #F7F9FB.
- Core border: Candidate Amber #FFC857.
- Border style: evenly spaced square-edged dots, not a glowing neon line.
- Center symbol: one simple Vault Ink #0B1118 question mark.
- No teal schema grid yet.
The body must remain teal; only the core communicates candidate state.

Pose the creature in a neutral three-quarter view facing right. Expression is alert and constructively uncertain, not anxious, sad, or pleading. Center it with generous padding.

PALETTE:
body #35D0B2; shadow #148F7A; one highlight #A6F3E2; outline and face #0B1118; core #F7F9FB; candidate border #FFC857; optional tiny non-semantic prop accent #B9A7FF only if composition requires it; background #0B1118.

STYLE:
Crisp flat vector, controlled Bézier curves, bold silhouette, one flat shadow maximum, one small highlight maximum. No gradients, transparency, glow, 3D, or glossy rendering.

OUTPUT:
Square 1:1 composition, no caption, no wordmark, no border, no watermark.

NEGATIVE PROMPT:
no teardrop, no pointed top, no pink body, no lavender body, no two-dot eyes, no straight-line mouth, no Ditto face, no Poring silhouette, no franchise likeness, no arms, no hands, no legs, no feet, no tail, no ears, no antennae, no glossy slime, no liquid chrome, no translucent gelatin, no gradients, no glowing core, no teal body recolored amber, no sad expression, no text other than the single question-mark core symbol
```

## A5. State variant — Promoted (core emits square stamp)

```text
Create a standalone square flat-vector mascot illustration of "The Imprintling" in the PROMOTED schema state.

The Imprintling is an original wide, low, asymmetrical teal gelatinous creature. It has a flat visual base, a shallow two-lobe top joined by a soft saddle, unequal side contours, and no pointed apex. It has no arms, hands, legs, feet, tail, ears, horns, crown, or antennae.

Its face consists of exactly:
- one narrow vertical rounded-capsule left eye;
- one small rounded-square right eye;
- one small open-diamond mouth.
Never use two dot eyes or a straight-line mouth.

Embed one rigid, perfectly axis-aligned rounded square schema core in the abdomen:
- Core fill: Core White #F7F9FB.
- Core outline: Vault Ink #0B1118.
- Core content: minimal Imprint Teal #35D0B2 schema grid with two horizontal divisions and one short vertical division.

Show promotion with exactly one restrained square "stamp" emitted from the core toward the right:
- The emitted stamp is a small Core White #F7F9FB square with a teal grid.
- It sits approximately one core-width to the right of the body.
- Connect it with at most two short square motion marks.
- No rays, sparkles, confetti, glow, crown, trophy, or magic effect.

Pose the creature in a stable neutral three-quarter view facing right. It may lean forward slightly, conveying completion without triumphal exaggeration. Center the combined mascot and stamp with generous padding.

PALETTE:
body #35D0B2; shadow #148F7A; one highlight #A6F3E2; outline and face #0B1118; core and stamp #F7F9FB; schema grid #35D0B2; background #0B1118.

STYLE:
Crisp flat-vector brand illustration, controlled Bézier curves, one shadow maximum, one highlight maximum, no gradients, transparency, glow, 3D, or glossy rendering.

OUTPUT:
Square 1:1 composition, no text, no wordmark, no outer border, no watermark.

NEGATIVE PROMPT:
no teardrop, no pointed apex, no pink blob, no lavender blob, no two-dot eyes, no line mouth, no Ditto face, no Poring shape, no franchise likeness, no arms, no hands, no legs, no feet, no tail, no ears, no antennae, no glossy slime, no liquid chrome, no translucent gelatin, no gradients, no magic sparkles, no confetti, no crown, no trophy, no glowing aura, no multiple stamps, no text
```

## A6. State variant — Abstain (leaning body, em-dash core)

```text
Create a standalone square flat-vector mascot illustration of "The Imprintling" in the ABSTAIN schema state.

The Imprintling is an original wide, low, asymmetrical teal gelatinous creature. It has a flat visual base, a shallow two-lobe top joined by a soft saddle, unequal side contours, and no pointed apex. It has no arms, hands, legs, feet, tail, ears, horns, crown, or antennae.

Its face consists of exactly:
- one narrow vertical rounded-capsule left eye;
- one small rounded-square right eye;
- one small open-diamond mouth.
Never use two dot eyes or a straight-line mouth.

Embed one rigid, perfectly axis-aligned rounded square schema core in the abdomen. For the ABSTAIN state:
- Core fill: Core White #F7F9FB.
- Core outline: Vault Ink #0B1118.
- Core symbol: one centered horizontal em dash in Vault Ink #0B1118.
- No schema grid, question mark, warning icon, or red color.

Pose:
The gelatinous body leans gently away from the implied decision direction while the schema core remains perfectly upright and axis-aligned. The eyes glance slightly sideways using their fixed capsule and rounded-square construction. Expression is calm and deliberate—not sad, frightened, ashamed, asleep, or broken. Abstention should read as a correct controlled decision.

PALETTE:
body #35D0B2; shadow #148F7A; one highlight #A6F3E2; outline, face, and em dash #0B1118; core #F7F9FB; background #0B1118.

STYLE:
Crisp flat-vector brand illustration, controlled Bézier curves, bold silhouette, exactly one flat shadow maximum and one small highlight maximum. No gradients, transparency, glow, 3D, or glossy rendering.

OUTPUT:
Square 1:1 composition, no caption, no wordmark, no border, no watermark.

NEGATIVE PROMPT:
no teardrop, no pointed top, no pink body, no lavender body, no two-dot eyes, no straight-line mouth, no Ditto face, no Poring silhouette, no franchise likeness, no arms, no hands, no legs, no feet, no tail, no ears, no antennae, no glossy slime, no chrome, no translucent gelatin, no gradients, no red warning state, no warning triangle, no sad face, no crying, no sleeping, no broken core, no question mark, no schema grid, no text other than the single em-dash symbol
```

## A7. Favicon / 16 px

```text
Design a production favicon for the Deblob open-source project, derived from its original Imprintling mascot.

Create a minimal asymmetric gelatinous silhouette on an exact 16×16 pixel construction grid.

SILHOUETTE:
- Wide and low, approximately 14 pixels wide by 11 pixels high.
- Flat two- or three-pixel baseline.
- Shallow two-lobe top joined by a one-pixel saddle.
- Left and right contours visibly unequal.
- No pointed apex.
- No teardrop or circular silhouette.
- No face, eyes, mouth, limbs, tail, ears, antennae, or decorative details.

SCHEMA CORE:
Cut one rigid axis-aligned square from the lower-middle of the silhouette.
- Core size: 4×4 pixels.
- Core must be true square negative space, not a rounded organic hole.
- Position it one pixel right of optical center.
- Do not include an internal grid at 16 px.

COLOR:
Primary version:
- Silhouette: Imprint Teal #35D0B2
- Core cutout: transparent
- Preview background: Vault 950 #0B1118
The same silhouette must also work as:
- solid black with transparent core;
- solid white with transparent core;
- Registry Teal #20B89A on Core White #F7F9FB.

STYLE:
Pixel-snapped flat vector icon. No antialias-dependent detail. No outline thinner than one pixel. No gradient, shadow, highlight, texture, transparency within the body, or 3D effect. Strong recognition at native 16×16 size.

OUTPUT:
Present the canonical icon large for inspection plus an exact native 16×16 preview. Keep the icon itself free of framing, text, letters, badges, and rounded app-icon containers. Prefer SVG-like geometry suitable for manual reconstruction.

NEGATIVE PROMPT:
no teardrop, no pointed top, no water droplet, no circle, no letter D, no letter B, no face, no two-dot eyes, no mouth, no Ditto likeness, no Poring likeness, no franchise character, no arms, no legs, no feet, no tail, no ears, no antennae, no slime drips, no glossy surface, no chrome, no gradients, no shadow, no highlight, no detailed schema grid, no text, no outer app tile, no watermark
```

---

# PART B — Design-system reference page (run second)

Build a single self-contained HTML page: the **Deblob Design System** reference — a living style guide a contributor opens to see every token, component, and state in one place. Treatment: utilitarian-but-polished reference documentation (Stripe/Radix docs, not a marketing splash). Dark-first, full light mode, theme-aware (respect `prefers-color-scheme` AND a `data-theme` override on `:root`, both directions).

**Tokens** — define as CSS custom properties on `:root`, redefine per theme:

- Dark: canvas `#0B1118` · surface `#111A24` · surface-raised `#182430` · border `#2B3A49` · text `#E7EDF3` · text-muted `#9AA8B6` · identity `#35D0B2` · hot `#F5A524` · cold `#5AA7FF` · provisional `#FFC857` · danger `#F06A6A`
- Light: canvas `#F7F9FB` · surface `#FFFFFF` · surface-raised `#EEF2F6` · border `#D6DEE6` · text `#17202A` · text-muted `#556575` · identity `#087F6D` · hot `#A85A00` · cold `#1769AA` · provisional `#8A5A00` · danger `#B4232A`
- Illustration-only accent (never a status color, never in charts/CLI/alerts): Mimic Lilac `#B9A7FF`

**Semantic law** (demonstrate it): teal = approved schema + successful resolution; amber = hot path + provisional; blue = async discovery/sampling/SLM; red = malformed/rejected/quarantined ONLY. Neutral slate ≥70% of any surface. Never encode state by color alone — always pair with icon or label.

**Type:** display/wordmark = Fredoka (OFL); body/UI/docs = Source Sans 3 (OFL); mono (all identifiers/offsets/metrics/config/CLI) = IBM Plex Mono (OFL), tabular numerals on. CSP blocks font CDNs — DO NOT link webfonts; use graceful fallback stacks (Fredoka→"Trebuchet MS",sans-serif; Source Sans 3→system-ui; IBM Plex Mono→ui-monospace,Menlo,Consolas) and note production embeds real OFL fonts via `@font-face`. Identifiers always mono; mid-truncate long IDs never at the prefix (`sch_7M4K2W…F9Q`); wordmark always lowercase `deblob`; sentence-case headings; prose ≤72ch.

**Page sections in order:**
1. Header: compact `deblob` wordmark (Fredoka, lowercase) + descriptor "Continuous schema discovery and identity tagging for uncontrolled data." + light/dark toggle stamping `data-theme` on `:root`.
2. Color: swatch grid both themes (name, hex, role); separate fenced row for Mimic Lilac labeled "illustration only — never a status color".
3. Typography: three-font scale with live specimens; type-scale ramp; identifier-truncation demo.
4. Schema identity tokens: `sch_` (teal), `cand_` (amber/provisional), `fam_…@v3` (neutral) in mono with mid-truncation.
5. Components (real styled HTML, each captioned):
   - Buttons: primary (identity teal), neutral, danger — default/hover/focus(2px ring, 2px offset)/disabled.
   - Status pills: Known (teal + imprint/check icon), Provisional (amber + dotted outline), Discovery (blue + branch icon), Unresolved (neutral + dash), Quarantine (red + stop icon), Tombstone (neutral) — each pairs color WITH icon+label (demonstrate "never color alone").
   - Schema record card: family name, `sch_` id (mono, truncated), version chip, provenance line.
   - Candidate card: `cand_` id, sample count, first/last seen, state pill, disabled "Promote" button (note: authenticated/audited boundary).
   - Table: schema list (family, schema id, version, first seen) with tabular-nums + slate header.
   - Code/CLI transcript: slate theme (NOT purple), `deblob relay kafka` output with `sch_` teal, `cand_` amber inline; NO_COLOR note.
   - Error callout: neutral styling, `DBL-2204 AOF persistence is unhealthy; promotions are frozen.` — NO mascot, red code prefix only. Caption: "operational surfaces stay plain."
   - Empty state: **the real Unknown-state Imprintling art from Part A3** (until available, a wide rounded teal blob silhouette with a square core cutout, flat fill, one shadow, one highlight — NOT glossy, NOT a teardrop, NO two-dot-eyes) + copy "No candidates waiting. The Imprintling has nothing new to shape." Caption: "mascot allowed here — no fault or governance decision."
6. Diagram language: hot/cold-lane figure — solid amber = sync, dashed blue = async, teal tile = immutable identity, red side branch = quarantine. Legible in monochrome.
7. Motion & a11y: 2px focus ring, `prefers-reduced-motion` respected, WCAG AA, NO_COLOR for CLI.
8. Mascot placement matrix: two columns — allowed (README hero, stickers, 404, empty states, release notes, onboarding) vs prohibited (CLI errors, quarantine, security docs, alerts, audit logs, promotion controls, all machine-readable output).

**Hard constraints:** self-contained (inline CSS, inline SVG icons + mascot stand-in, no external requests); body never scrolls horizontally (wide content scrolls in its own `overflow-x` container); no glossy 3D blob / teardrop / Ditto face / liquid chrome / sparkles / glowing brains / crypto gradients / padlock-vault clichés; the schema core (rigid square) is mandatory in the mascot stand-in. Favicon: 🫧. Title: "Deblob Design System".

---

## Run order

1. **A2 model sheet** — establishes canon. Approve before generating variants.
2. **A1 hero** + **A3–A6 state variants** + **A7 favicon** — all must match the sheet.
3. **Part B** design-system page — swap the A3 Unknown-state art into section 5's empty-state slot; embed the favicon.
4. Cross-check every output against `design-book.md` §8 anti-patterns before declaring canonical.
