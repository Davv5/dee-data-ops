---
title: feat: Chart Style Guide
type: feat
status: active
date: 2026-05-05
---

# Chart Style Guide

## Overview

A new reference document — `docs/conventions/chart_style_guide.md` — that companions the just-shipped `docs/conventions/mart_presentation_style_guide.md`. The mart presentation guide includes a small Loop-3 encoding lookup (data shape → default chart). This new guide expands that lookup into a deeper, citation-grounded reference for chart selection: organized by the question the chart answers, with priority-ordered chart options per question, anti-patterns per category, and cross-cutting honesty rules.

The guide is domain-general (works whether the engagement is fashion, healthcare, B2B SaaS, or real estate) and audience-dual (operators consuming charts + engineers/designers choosing them).

## Problem Frame

Engineers and designers building dashboards across our engagements pick charts ad-hoc. The mart presentation guide solved the *language* problem (forbidden vocabulary, declarative titles, the say-out-loud test) but only sketches the *visual encoding* problem in a five-row lookup table. That table is too thin to act as a real selection reference: it doesn't say *why* one encoding beats another, what the alternatives are, when to deviate, or what to never do.

The result is the same drift the mart presentation guide caught at the language layer: locally-scaled bars, dual y-axes, decorative 3D, color-only encoding, area-encoded numbers without legends, truncated zero baselines on bar charts. A real chart-selection reference would catch these systematically.

## Requirements Trace

- R1. Provide a question-first chart taxonomy organized by the operator question (compare amounts / show distribution / change over time / part-to-whole / correlation / ranking / deviation / flow / spatial). Priority-ordered options per question with stated tradeoffs.
- R2. Catalog anti-patterns per category (3D pie, dual y-axis, locally-scaled small multiples without disclosure, etc.) with cited rationale.
- R3. Establish cross-cutting honesty rules (lie factor, zero-baseline rule, color-blind safety, scale disclosure, takeaway annotation) drawn from named sources.
- R4. Be domain-general — examples and rules must not assume any one engagement's vocabulary.
- R5. Companion (not duplicate) the mart presentation guide. Cross-link in both directions. The Loop-3 table in the mart presentation guide stays as a quick-reference; this new guide is the deep reference behind it.
- R6. Cite established practice: Munzner, Cairo, Few, Tufte, Knaflic, FT Visual Vocabulary, Datawrapper Academy, Observable Plot, Andy Kirk.
- R7. Match the tone and structure of `docs/conventions/dbt_style_guide.md` and the mart presentation guide so reviewers recognize the same family of document.

## Scope Boundaries

- Not a chart-making tutorial. Will not teach matplotlib / D3 / Plotly / Observable syntax.
- Not framework-specific. Works regardless of whether the dashboard is built in Next.js + Recharts, Looker, Metabase, or static SVG.
- Not a brand-voice guide. The mart presentation guide owns voice/copy; this guide owns visual encoding.
- Not a duplicate of the mart presentation guide's Loop-3 table — that table stays as the quick reference; this guide is the depth behind it.

### Deferred to Follow-Up Work

- An automated linter / audit skill that scans dashboard component code for anti-pattern violations (e.g., dual y-axis usage). Mirrors the deferred `mart-presentation-audit` skill.
- Per-engagement chart pattern catalog (e.g., a Dee-specific gallery showing the lollipop, dot-plot, and mini-funnel patterns from the Lead Magnets redesign as house-style examples).

## Context & Research

### Relevant Code and Patterns

- `docs/conventions/dbt_style_guide.md` — format and tone precedent. Sectional, numbered rules, examples, citations.
- `docs/conventions/mart_presentation_style_guide.md` (just shipped) — companion document. Has Loop-3 encoding lookup (data shape → default chart) that this guide will deepen. Cross-link in both directions.
- `.claude/rules/mart-naming.md` — referenced by mart presentation guide; no direct dependency here.
- `agency-work/clients/dee-rich-off-clothes/dashboard-app/...` (in Cabinet, not data ops repo) — concrete examples of patterns the Lead Magnets redesign introduced (proportional funnel band, dot plot on shared $ axis, horizontal lollipops, per-offer mini-funnels, identity-tab table). These can be referenced as worked examples.

### External References

- Munzner, *Visualization Analysis and Design* (2014) — what/why/how framework, channel effectiveness ranking.
- Cairo, *The Truthful Art*, *How Charts Lie* — five qualities, deception vectors.
- Few, *Show Me the Numbers*, *Now You See It* — graph selection matrix, bullet graph, anti-pie stance.
- Tufte, *Visual Display of Quantitative Information*, *Envisioning Information* — data-ink, lie factor, sparklines, small multiples.
- Knaflic, *Storytelling with Data* — chart inventory, takeaway-annotation rule.
- Financial Times Visual Vocabulary — nine-category taxonomy by question.
- Datawrapper Academy — choropleth rules, pie guidance.
- Observable Plot, Andy Kirk (visualisingdata.com) — modern chart-family taxonomies.

### Institutional Learnings

- The mart presentation guide pattern works: Tests passed against all 5 D-DEE tabs. Mirror its structure.
- Lead Magnets redesign produced multiple novel encodings (proportional funnel band, dot plot on $ axis, lollipops, mini-funnels, identity-tab tables) that should appear in the worked-example section so the guide isn't disconnected from real practice.

## Key Technical Decisions

- **Question-first taxonomy, not chart-first.** Reader comes with a question ("how do I show change over time?"), not a chart name. This matches FT Visual Vocabulary and Few's selection matrix and avoids the failure mode where someone reaches for the chart they know rather than the chart that fits.
- **Priority-ordered options per question, not exhaustive.** Each question gets 3–6 options ranked best-default to niche. Exhaustive catalogs (datavizproject's ~150 types) are referenced in the appendix, not duplicated.
- **Anti-patterns per question, plus cross-cutting honesty rules.** Local anti-patterns (per question) catch wrong-fit choices; cross-cutting honesty rules (one section, applied universally) catch deception.
- **Cite verbatim where possible.** Like the mart presentation guide and dbt_style_guide.md, use quoted source text when the original phrasing is canonical (Tufte's lie-factor definition; Few's pie critique; Cairo's five qualities).
- **No code samples.** This is a selection guide, not a tutorial. Code lives in framework docs and per-engagement components.
- **Match dbt_style_guide.md heading structure.** Top-level sections, no frontmatter beyond what dbt_style_guide.md uses (which is none — it's a plain `# Chart Style Guide` header). The mart presentation guide also follows this pattern.

## Open Questions

### Resolved During Planning

- **Where does this live?** `docs/conventions/chart_style_guide.md`, alongside `dbt_style_guide.md` and `mart_presentation_style_guide.md`.
- **Should it have `paths:` frontmatter (rule-style auto-load)?** No — it's a style guide, not a rule. Reference documentation consulted on demand. Same as the mart presentation guide.
- **Should the Loop-3 table in the mart presentation guide be removed and replaced with a pointer here?** No — the table stays as a quick-reference. This guide is the depth behind it. Both serve different reading postures (skim vs. study).
- **Do we ship per-engagement examples (Dee's lollipops/mini-funnels)?** Yes, as a worked-example section near the end so the guide isn't disconnected from real practice. But examples are illustrative, not prescriptive — engagements may invent new patterns.

### Deferred to Implementation

- Exact chart count per category (target ~3–6 options each, but the right number depends on what the literature naturally surfaces).
- Whether to embed any inline ASCII / Mermaid sketches for chart shapes (probably not — references to canonical sources cover this).

## Implementation Units

- [ ] U1. **Write `docs/conventions/chart_style_guide.md`**

**Goal:** Author the chart style guide as a single self-contained reference document.

**Requirements:** R1, R2, R3, R4, R6, R7

**Dependencies:** None.

**Files:**
- Create: `docs/conventions/chart_style_guide.md`

**Approach:**
- Open with "Why this guide exists" — tie back to mart presentation guide's Loop 3, name the gap.
- "What this guide is not" — boundaries (not a tutorial, not framework-specific, not a duplicate of Loop 3 quick reference).
- "Grounded in established practice" — list cited sources upfront. Same pattern as mart presentation guide.
- "How to use this guide" — start with the operator's question, find the matching section, choose top-priority option unless a tradeoff applies.
- Question-first taxonomy (the body): nine sections, one per question (compare amounts / distribution / change over time / part-to-whole / correlation / ranking / deviation / flow / spatial). Each section has: priority-ordered chart list with one-sentence tradeoffs, anti-patterns inline.
- "Cross-cutting honesty rules" — twelve rules drawn from research. Lie factor; bars start at zero; no dual y-axes without explicit disclosure; no 3D for 2D data; no rainbow color scales; color is never the only encoding; show the scale; small-multiples scale-sharing; no cherry-picked time windows; no ordering bias; area-encoded numbers need a legend; annotate the takeaway.
- "Channel effectiveness reference" — Munzner's ranking (position > length > angle/area > color hue, etc.). One table.
- "Worked examples" — three or four examples drawn from real engagements showing how the taxonomy lands in practice. Reference (don't reproduce) the Lead Magnets redesign's lollipops, dot-plot, mini-funnels, proportional funnel band.
- "Related rules and conventions" — cross-link mart-naming, mart presentation guide, operator-mode, data-modeling-process, dbt_style_guide.
- "Helpful Reference Links" appendix — full citations with URLs.

**Patterns to follow:**
- Mirror `docs/conventions/mart_presentation_style_guide.md` structure: opening "Why this guide exists" + "What this guide is not" + "Grounded in established practice" + body sections + cross-cutting rules + worked examples + related rules + helpful reference links.
- Use the same heading depth (## for top-level sections, ### for sub-sections, **bold** for inline term highlights).
- Match the citation style: `*Source Name (Year)*` inline; full URLs in the appendix.

**Test scenarios:**
- Coverage: Every question category from FT Visual Vocabulary's nine has a section.
- Coverage: Every honesty rule from Cairo's *How Charts Lie* deception vectors is captured.
- Citation density: Every priority chart claim is traceable to a named source (no invented opinions).
- Cross-tab consistency: Re-running the mart presentation guide's 12-question pre-ship checklist against this guide passes (no internal jargon, no mart names, declarative section titles, etc.).
- Domain-generality: A grep for engagement-specific terms (Dee, fashion, magnet, retention, customer 360) outside the explicit "Worked examples" section returns zero hits.

**Verification:**
- The guide ships as a single file at `docs/conventions/chart_style_guide.md`.
- The file passes the mart presentation guide's pre-ship checklist when applied to its own copy.
- A reader presented with a fresh data shape can find the right section in under five seconds (matches mart presentation guide's five-second test, applied recursively).

---

- [ ] U2. **Cross-reference from `docs/conventions/mart_presentation_style_guide.md`**

**Goal:** Add a one- to two-paragraph pointer in the mart presentation guide's Loop 3 section so readers know where the deep reference lives.

**Requirements:** R5

**Dependencies:** U1

**Files:**
- Modify: `docs/conventions/mart_presentation_style_guide.md`

**Approach:**
- Open the file's Loop 3 (Encoding) section and append a short cross-reference paragraph immediately after the encoding table.
- Wording: "The table above is a quick reference. For the full chart-selection taxonomy — every common operator question with priority-ordered chart options, anti-patterns, and cross-cutting honesty rules — read `docs/conventions/chart_style_guide.md`."
- Optionally also add a line under the "Related rules and conventions" section at the bottom.

**Patterns to follow:**
- Match the existing cross-references already in the mart presentation guide (the lines pointing to `mart-naming.md`, `operator-mode.md`, `data-modeling-process.md`, `dbt_style_guide.md`).

**Test scenarios:**
- Edge case: the cross-reference appears in BOTH the Loop 3 body AND the Related Rules section, so a reader skimming either way can find it.
- Test expectation: none — pure documentation cross-link, no behavior to test.

**Verification:**
- Both files reference each other. A reader starting from either can find the other in one click.

## System-Wide Impact

- **Documentation graph:** Adds a new node in `docs/conventions/`. Cross-linked from `mart_presentation_style_guide.md`. No code paths touched.
- **Skill / agent loading:** The data-engineer agent and any skill that already reads `docs/conventions/dbt_style_guide.md` and `mart_presentation_style_guide.md` should also be made aware of this guide. That awareness is implicit (agents discover docs via Read on demand) and does not require a config change.
- **Engagement onboarding:** When `engagement-init` template infrastructure is restored, both style guides should be shipped together to new engagements as a pair.
- **Unchanged invariants:** The mart presentation guide's Loop 3 table stays in place and authoritative for the quick-lookup posture. This guide is additive depth, not a replacement.

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| Guide drifts from mart presentation guide's tone | Mirror its structure section-for-section; same opening, same closing, same citation style. |
| Guide becomes too academic / unread | Lead with the question-first taxonomy. Worked examples near the end ground the abstractions in real practice. Cap length around 600–900 lines (compare to mart presentation guide's ~300). |
| Loop 3 table in mart presentation guide and this guide drift apart | Treat the Loop 3 table as the quick reference and note in both files that the deep version is the source of truth. Anyone editing one should re-read the other. |
| Citations rot (broken URLs) | Cite source + year + canonical title in addition to URL. URL rot doesn't break the citation. |

## Documentation Notes

- After both files land, the `WORKLOG.md` entry should note "Chart Style Guide added; companion to Mart Presentation Style Guide; lives at `docs/conventions/chart_style_guide.md`."
- The `mart-collapse` skill, the `mart-roadmap-rank` skill, and the data-engineer agent's lifecycle docs all benefit from a one-line awareness pointer once both guides have stabilized — but those edits are out of scope here.

## Sources & References

- Companion guide: `docs/conventions/mart_presentation_style_guide.md` (just shipped on this branch).
- Format precedent: `docs/conventions/dbt_style_guide.md`.
- Research synthesis (this session): citation appendix from chart-literature research covering Munzner, Cairo, Few, Tufte, Knaflic, FT Visual Vocabulary, Datawrapper, Observable Plot, Andy Kirk.
