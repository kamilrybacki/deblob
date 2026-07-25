# Why unrelated Deblob schemas score as "close" — Investigation
run: `jr-deblob-similarity-220904` · 2026-07-22 · agents: Claude Code (Hermes unreachable — discord-mcp crash-looping, 76 restarts; loop in when Discord recovers)

## Executive summary
**Root cause: the similarity has no notion of how DISCRIMINATIVE a field is.** Deblob's semantic similarity is a weighted multiset-Jaccard over `canonical_field_id`s, but the weight table (`deblob-semantic/signature.rs`) weights by feature *class only* (`Field=12`, `Event=24`, …) with **no IDF / document-frequency term**, and `has_anchor()` counts *any* cfid as an anchor. So the ubiquitous `cfid_timestamp` — present in **62% of schemas** — weighs exactly as much as a rare, meaningful cfid, and a schema annotated with only `cfid_timestamp` is a valid anchor. Two unrelated schemas that share *only* `timestamp` therefore score `12/24 = 0.50` and render as "medium" similarity. This is the textbook missing-IDF / stop-word failure of set-similarity over structured records, made worse by a sparse heuristic annotation (median 3 cfids/schema, mostly generic). **The fix is IDF/document-frequency down-weighting of ubiquitous cfids, plus excluding generic fields from the anchor set** — the change is contained to `signature.rs` (bump `WEIGHTS_VERSION` v1→v2) and needs a rebuild.

## Evidence (code + live)
- **Live smoking gun.** Query schema "Verify Human Name" has ONE cfid: `cfid_timestamp`. Its top neighbors, all `0.50 / medium`:
  - "Wikidata Revid Bot Records" (civic.wikidata) — cfids {name, timestamp} — **shared: {timestamp}**
  - "Carbon Intensity Readings" (carbon.fr) — {carbon, timestamp} — **shared: {timestamp}**
  - "HuggingFace Dataset Downloads" (registry.hf-datasets) — {count, timestamp} — **shared: {timestamp}**
  Three unrelated domains, "similar" purely on `timestamp`.
- **Scoring** (`signature.rs`): exact rational weighted-multiset-Jaccard, `numerator = Σ w_f·min(cA,cB)`, `denominator = Σ w_f·max(cA,cB)`. Weights are per feature-class: `Event 24, Field 12, FieldIdns 10, FieldUnit 8, … Temporal 1`. **Every `canonical_field_id` gets the flat Field weight 12** — no per-cfid rarity term. Query {ts:12} vs neighbor {carbon:12, ts:12} → num=12, den=24 → 0.50.
- **Anchor definition** (`has_anchor`): true if `event_type` OR **any** `canonical_field_id` OR any `identifier_namespace`. So `cfid_timestamp` alone makes a schema anchored + eligible for neighbors. `strength`: `0.50 → Medium`.
- **Document frequency across the 40 annotated schemas** (the IDF the model is missing):
  `cfid_timestamp 62%` · `cfid_name 45%` · `cfid_price/count/region ~28%` · `cfid_status 25%` — generic, high-frequency; vs the discriminative tail `latitude/longitude 12%` · `carbon/power/unit 10%` · `currency 8%` · `value 2%`. IDF would give `timestamp` weight `log(40/25)≈0.47` vs `carbon` `log(40/4)≈2.3` — ~5× — so sharing `carbon` would rank far above sharing `timestamp`.
- **Contributing factor — sparse heuristic annotation.** The consolidation-controller maps only field-names it recognizes to a small vocab, so most schemas get 1–3 mostly-generic cfids; the signal is dominated by stop-words.

## External consensus (the fix is well-established)
- **IDF is *the* factor** that makes schema-matching beat plain kNN-Jaccard/cosine on structured + textual data — a term "should be discounted if it occurs in many other documents (common words: and/the/inc/str)" (Sparkly, Paulsen/Govind/Doan, UW-Madison) `[C]`. Directly maps: `cfid_timestamp` = "the/and" of the schema corpus.
- **IDF-weighted Jaccard** "smoothly discounts ubiquitous tokens and accentuates rare, content-discriminative vocabulary" — the exact generalization of Deblob's unweighted-per-class Jaccard `[C]`.
- **BM25**: IDF weights matches on rare terms more heavily; common terms "carry little discriminating power" `[C]`.
- Plain Jaccard's known weakness: "rare terms are more informative than frequent; Jaccard doesn't consider this" (pyimagesearch) `[C]`.

## Recommended fix (contained to `deblob-semantic/signature.rs`, WEIGHTS_VERSION v1→v2 → rebuild)
1. **IDF / document-frequency per-cfid weighting (principled, primary).** Multiply each cfid's Field weight by `idf(cfid) = log(N / df(cfid))` (or a smoothed variant), where `df` = number of published schemas carrying that cfid. Needs a small corpus doc-frequency table maintained alongside the semantic index and refreshed as the corpus grows. Auto-suppresses `timestamp`/`name` without hand-curation and scales.
2. **Anchor discriminativeness gate (cheap, immediate, complements #1).** Exclude generic cfids (`timestamp`, `id`/`name`, and any cfid over a df threshold, e.g. >40–50%) from the ANCHOR set, and require ≥1 shared *distinctive* anchor for a match. A schema annotated with only `cfid_timestamp` then has NoAnchor → no false neighbors. Ships without corpus stats; can be a static stop-list first, then df-driven.
3. **Recalibrate strength bands (secondary).** `0.50` shouldn't read "medium" when it's one shared stop-word — but this is cosmetic; fix ranking (#1/#2) first.
4. **Annotation hygiene (upstream).** The consolidation-controller could skip generic-only signatures / map more distinctive fields — but the fundamental fix is weighting, not annotation.
5. **Neighbors should borrow the umbrella lane's discipline.** The umbrella promotion path already has eligibility / cannot-link gates + per-domain false-merge calibration (jointly designed `jr-umbrella`); the neighbors/similarity lane runs *without* them. Applying the same discriminative gating (or at least the anchor gate) to neighbors closes the gap. **[flag for Hermes when reachable — it co-designed those gates and knows the intended `anchor` semantics.]**

## Confidence & gaps
- HIGH: the mechanism, the live example, the df distribution, and that IDF is the standard fix are all directly verified in code + live data + literature.
- OPEN (for Hermes): was flat-per-class weighting an intentional v1 simplification with IDF planned for v2? Do the umbrella cannot-link gates already encode a generic-attribute exclusion that neighbors should reuse? Homelab-fit of maintaining a live df table vs a static stop-list.
- Method note: Hermes could not be reached — the `discord-mcp` cellarette backend is crash-looping (76 restarts; "Proxy 'discord' is not connected"), a separate homelab issue. Single-agent findings; Hermes' design-intent input pending Discord recovery.
