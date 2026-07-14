# Claude Design prompt — Deblob Design System

Paste the block below into Claude Design (or any capable frontend-generating agent) to produce the Deblob design-system reference page. It is self-contained: every token value is inline, so no external file access is needed. Companion prompt for mascot illustration lives in `claude-design-prompt-mascot.md` (Hermes-authored).

---

```
Build a single self-contained HTML page: the **Deblob Design System** reference — a living style guide a contributor opens to see every token, component, and state in one place. Deblob is an open-source Rust service that gives every message in a data stream a permanent schema identity; the brand centers on "the Imprintling", a wide asymmetric teal blob-creature with a rigid square "schema core" in its abdomen (body mimics shapes, core records the shape that survived validation). Tone: a careful data-plane operator — terse, technical, calm — with the mascot providing warmth ONLY in non-operational surfaces.

TREATMENT: utilitarian-but-polished reference documentation, not a marketing landing page. Real typographic hierarchy, generous spacing, no oversized hero. Think Stripe/Radix design-system docs, not a product splash. Dark-first, full light mode, theme-aware (respect prefers-color-scheme AND a data-theme override on :root, both directions).

=== TOKENS (define as CSS custom properties on :root, redefine per theme) ===

Dark theme:
  canvas #0B1118 · surface #111A24 · surface-raised #182430 · border #2B3A49
  text #E7EDF3 · text-muted #9AA8B6
  identity #35D0B2 · hot #F5A524 · cold #5AA7FF · provisional #FFC857 · danger #F06A6A
Light theme:
  canvas #F7F9FB · surface #FFFFFF · surface-raised #EEF2F6 · border #D6DEE6
  text #17202A · text-muted #556575
  identity #087F6D · hot #A85A00 · cold #1769AA · provisional #8A5A00 · danger #B4232A
Illustration-only accent (NEVER a status color, never in charts/CLI/alerts): Mimic Lilac #B9A7FF

Semantic law — encode in the page and demonstrate it:
  identity/teal = approved schema + successful registry resolution
  hot/amber = synchronous hot path + provisional activity
  cold/blue = async discovery lane / sampling / SLM
  danger/red = malformed / rejected / quarantined ONLY (never ordinary drift)
  Neutral slate must be ≥70% of any surface. Never encode state by color alone — always pair with icon or label.

=== TYPE ===
Display/headings + wordmark: Fredoka (rounded, SIL OFL). Body/UI/docs prose: Source Sans 3 (OFL). Mono (all identifiers, offsets, metrics, config, CLI): IBM Plex Mono (OFL), tabular numerals on.
Because the Artifact CSP blocks font CDNs, DO NOT link webfonts — use a graceful system fallback stack per role (Fredoka→"Trebuchet MS",sans-serif rounded fallback; Source Sans 3→system-ui; IBM Plex Mono→ui-monospace,Menlo,Consolas). Note in the page that production embeds the real OFL fonts as @font-face.
Rules to show: identifiers always mono (sch_…, cand_…, fam_…); mid-truncate long IDs never at the prefix (sch_7M4K2W…F9Q); wordmark always lowercase "deblob"; sentence-case headings; prose ≤72ch.

=== PAGE SECTIONS (in order) ===
1. Header: compact "deblob" wordmark (lowercase, Fredoka) + one-line descriptor "Continuous schema discovery and identity tagging for uncontrolled data." + a light/dark toggle that stamps data-theme on :root.
2. Color: swatch grid for both themes, each chip showing token name, hex, and role. Separate, clearly-fenced row for Mimic Lilac labeled "illustration only — never a status color".
3. Typography: the three-font scale with live specimens; a type-scale ramp; the identifier-truncation demo.
4. Schema identity tokens: show the three ID forms rendered in mono with correct accent — sch_ (teal), cand_ (amber/provisional), fam_…@v3 (neutral). Include the mid-truncation treatment.
5. Components — build these as real styled HTML, each with a short caption:
   - Buttons: primary (identity teal), neutral, danger. Show default/hover/focus(2px ring, 2px offset)/disabled.
   - Status pills / badges: Known (teal + imprint/check icon), Provisional (amber + dotted outline), Discovery (blue + branch icon), Unresolved (neutral + dash), Quarantine (red + stop icon), Tombstone (neutral). Each pairs color WITH icon+label — demonstrate the "never color alone" rule.
   - Schema record card: family name, sch_ id (mono, truncated), version chip, small provenance line.
   - Candidate card: cand_ id, sample count, first/last seen, state pill, a disabled "Promote" governance button (note promotion is an authenticated/audited boundary).
   - Table: schema list with columns (family, schema id, version, first seen) using tabular-nums and a slate header.
   - Code block / CLI transcript: slate theme (NOT purple), showing `deblob relay kafka` output with sch_ teal, cand_ amber inline; include a NO_COLOR note.
   - Error callout: neutral styling, stable code format `DBL-2204 AOF persistence is unhealthy; promotions are frozen.` — NO mascot, no color beyond a red code prefix. Caption: "operational surfaces stay plain."
   - Empty state: mascot placeholder box (drawn as a simple inline SVG stand-in — a wide rounded teal blob silhouette with a small square core cutout, flat fill, one shadow, one highlight; NOT glossy, NOT a teardrop, NO two-dot-eyes) + copy "No candidates waiting. The Imprintling has nothing new to shape." Caption: "mascot allowed here — no fault or governance decision."
6. Diagram language: a small hot/cold-lane figure — solid amber line = synchronous path, dashed blue line = async discovery, teal tile = immutable identity, red side branch = quarantine. Must read in monochrome too.
7. Motion & a11y notes: 2px focus ring everywhere, prefers-reduced-motion respected, WCAG AA contrast, NO_COLOR support noted for CLI.
8. Mascot placement matrix: a two-column table — "mascot allowed" (README hero, stickers, 404, empty states, release notes, onboarding) vs "mascot prohibited" (CLI errors, quarantine, security docs, alerts, audit logs, promotion controls, all machine-readable output).

=== HARD CONSTRAINTS ===
- Self-contained: all CSS inline, any icon as inline SVG, the mascot stand-in as inline SVG. No external requests.
- Page body never scrolls horizontally; wide content (tables, code) scrolls inside its own overflow-x container.
- Do NOT render a glossy 3D blob, a teardrop/Poring shape, Ditto's two-dot-eyes+line face, liquid chrome, sparkles, glowing brains, neural backgrounds, crypto gradients, or padlock/vault-wheel clichés.
- The schema core (rigid square) is the mascot's non-negotiable identifier — the stand-in SVG must include it.
- Favicon: 🫧
- Title: "Deblob Design System"
```

---

## Notes for whoever runs this

- The prompt targets a **reference/style-guide page**, not the product UI (Deblob is a headless service in P1; its only real UI is the CLI + management API JSON). The design system exists to keep README, docs site, Grafana accents, and future UI coherent.
- Mascot here is a deliberately crude inline-SVG **stand-in** — the real Imprintling comes from the mascot illustration prompt (`claude-design-prompt-mascot.md`). Swap it in once art exists.
- Pair with the design book (`docs/brand/design-book.md`) as the normative source; this prompt is a faithful projection of it.
