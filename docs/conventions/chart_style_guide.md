# Chart Style Guide

## Why this guide exists

The Mart Presentation Style Guide (in this directory) contains a small Loop-3 lookup that maps a data shape to a default chart. That table is a quick reference — five rows, no rationale, no alternatives, no deviation rules. It tells you what to reach for. It doesn't tell you why, when to deviate, or what to never do.

This guide is the depth behind that table. It exists so that an engineer or designer building a dashboard — for any engagement, in any domain — can:

- Find the right chart for the question they're answering, not the chart they happen to remember.
- Choose between 3–6 ranked options per question, each with a stated tradeoff.
- Recognize anti-patterns inline (no 3D pie, no dual y-axes, no decorative gauges).
- Apply twelve cross-cutting honesty rules to every chart that ships.

The guide answers one question:

> Given the question this chart needs to answer, which visualization carries the data honestly and clearly — and which choices would mislead?

It is **domain-general**. It applies whether the engagement is a fashion mentorship, a healthcare practice, a B2B SaaS, or a real estate brokerage. Domain-specific examples appear only in the "Worked examples" section near the end.

## What this guide is not

- **Not a charting tutorial.** It does not teach matplotlib, D3, Plotly, Recharts, Vega-Lite, or Observable Plot syntax. Code lives in framework documentation and per-engagement components.
- **Not framework-specific.** The selection rules apply whether the dashboard is built in Next.js, Looker, Metabase, Tableau, Power BI, or a static SVG.
- **Not a brand-voice guide.** The Mart Presentation Style Guide owns voice, copy, and substitution; this guide owns visual encoding.
- **Not a duplicate of the Mart Presentation Guide's Loop-3 table.** That table stays as the quick-skim reference. This guide is the source of truth for the encoding decisions behind it.

## Grounded in established practice

Each rule below cites an established source. We borrow from the canon so the guide isn't private opinion.

- Tamara Munzner — *Visualization Analysis and Design* (2014). What/why/how framework; channel effectiveness ranking for ordered and categorical attributes.
- Alberto Cairo — *The Truthful Art* (2016); *How Charts Lie* (2019). The five qualities (truthful, functional, beautiful, insightful, enlightening); the deception vectors.
- Stephen Few — *Show Me the Numbers* (2nd ed., 2012); *Now You See It* (2009); the Graph Selection Matrix. Bullet graph specification; the case against pie, gauge, and dial.
- Edward Tufte — *Visual Display of Quantitative Information* (1983/2001); *Envisioning Information* (1990). Data-ink ratio, lie factor, sparklines, small multiples, the zero-baseline rule for bars.
- Cole Nussbaumer Knaflic — *Storytelling with Data* (2015). Twelve-chart inventory; takeaway-annotation rule.
- Financial Times Visual Vocabulary (Smith, A. et al.). Nine-category taxonomy by question — the structure this guide adopts for the body.
- Datawrapper Academy — practical per-chart guides; choropleth and pie rules.
- Observable Plot — layered grammar of graphics (marks, channels, scales, transforms, facets).
- Andy Kirk — *Data Visualisation: A Handbook for Data Driven Design* (3rd ed., 2024); chart-family taxonomy.

URLs in the appendix.

## How to use this guide

1. Start with the operator's **question** — what decision does the chart support?
2. Find the matching section below. Nine categories, drawn from the FT Visual Vocabulary.
3. Read the priority-ordered chart options. Choose the top option unless a stated tradeoff applies.
4. Check the inline anti-patterns. If your draft chart matches one, choose differently.
5. Apply the twelve cross-cutting honesty rules to the finished chart.
6. If you deviate from a default, leave a one-line note in the per-mart presentation file (see Mart Presentation Style Guide → "The operator-facing glossary") explaining why.

## The body — chart selection by operator question

Each section below answers one operator question. Charts are listed in priority order, best default first. Tradeoffs and anti-patterns appear inline.

### 1. "I want to compare amounts (magnitude)"

How big is each category compared to the others?

1. **Horizontal bar chart.** Default. Long category labels stay readable; bars sort cleanly by value. Few and Knaflic both make this the workhorse for nominal comparison.
2. **Vertical column chart.** Use when categories are short (months, ratings) or naturally ordered.
3. **Lollipop / dot plot.** Use when bars feel visually heavy at high category counts (>15 categories) or when multiple metrics share the same row. Same data as a bar; less ink. (FT Visual Vocabulary.)
4. **Paired bar / paired column.** Comparing two series across the same categories (e.g., this period vs. last period).
5. **Proportional symbol / isotype.** Only for impressionistic readings; the reader needs a legend to recover precise values.

**Anti-patterns:**
- Never **3D bars**. They distort length perception. (Cairo, Tufte, Few — all condemn.)
- Never **truncate the zero baseline on bars**. Bars encode length; length needs a true zero, or the lie factor inflates. (Tufte.)
- Never **rainbow hues to encode magnitude** — color hue has no perceptual ordering. Use sequential single-hue.
- Avoid **radar / spider charts** when comparing >3 dimensions or when the chart invites cross-axis comparison. (Few.)

### 2. "I want to show distribution"

How are these values spread? What's typical, what's extreme?

1. **Histogram.** Single variable, continuous. Bin width is a design decision — show it on the chart or in the caption.
2. **Boxplot.** Multiple groups, summary statistics, outliers visible. Requires N ≥ ~10 per group; below that, use a strip plot.
3. **Violin plot.** When the *shape* of the distribution matters (bimodality, skew) and you have enough data to justify the smoothing.
4. **Strip plot / dot plot / barcode plot.** Small N where every observation matters and you don't want summary statistics to obscure individual records.
5. **Population pyramid.** Two-sided distribution (e.g., demographics, two cohorts).
6. **Cumulative curve (ECDF).** When threshold questions dominate ("what share is below X?"). More information-dense than a histogram for the same plot real estate.

**Anti-patterns:**
- **Bin widths chosen to mislead.** Over-smoothing hides modes; under-smoothing fabricates noise. Pick on data, not on aesthetics.
- **Pie charts for distribution.** Pies encode part-to-whole, not distribution.
- **Boxplot at N < 10.** Summary statistics on tiny samples mislead. Show the dots.

### 3. "I want to show change over time"

What's the trend? When did it shift?

1. **Line chart.** Default for continuous time, especially many points or many series.
2. **Column chart.** Discrete periods, few points, when the *value at each point* (not the trend between them) is the message.
3. **Area chart.** Single series where magnitude matters. **Stacked area only** when the parts are stable and part-to-whole composition is the question. Avoid when middle series are volatile — only the bottom series is readable.
4. **Slopegraph.** Two time points, many categories. Emphasizes rank change. (Knaflic; Tufte's parallel-coordinates lineage.)
5. **Connected scatterplot.** Two metrics evolving jointly over time. Powerful but takes a moment for readers to parse — annotate the time direction.
6. **Calendar heatmap / horizon chart.** Dense longitudinal patterns where overall shape matters more than precise values.
7. **Sparkline.** Inline trend in the line of a sentence or table cell. (Tufte.)
8. **Fan chart.** Forecast with uncertainty bands. (FT Visual Vocabulary.)

**Anti-patterns:**
- **Dual y-axes** unless: each axis is color-coded to its series, both axes are explicitly labeled with units, AND the caption states that the two scales are independent. Cairo: "the single most common deception in business charts."
- **Stacked area with volatile mid-series.** Only the bottom series is readable; the rest distort.
- **Truncated y-axis without explicit annotation.** Tufte's exception: line charts may use a non-zero baseline when the data range is far from zero, but the baseline must be labeled and called out.
- **Cherry-picked time windows.** Start and end points must be defensible — full available range, full cycle, fiscal year. If you start the chart "post-event," annotate the cut.

### 4. "I want to show part-to-whole"

How does this whole break down?

1. **Stacked bar (100%).** Preferred default for comparing composition across categories. Reads in linear order; works at any number of parts.
2. **Treemap.** Many parts, hierarchy, large value range.
3. **Waterfall.** Sequential additive decomposition (start → drivers → end).
4. **Pie / donut.** Acceptable only for ~2–4 slices with very different sizes. Never for precise comparison. Few: *"Pie charts communicate information poorly… Our visual perception is not designed to accurately assign quantitative values to 2-D areas."* Knaflic recommends avoiding entirely.
5. **Gridplot, Voronoi, Venn, arc diagram.** Niche; usually require legends or explicit reading instructions.

**Anti-patterns:**
- **3D pie.** Cairo, Tufte, Few, Knaflic — unanimous.
- **Exploded pie.** Adds chartjunk without adding information.
- **Donut with a KPI in the hole that doesn't equal the total.** Mismatched aggregations confuse the reader.
- **>5 pie slices.** Sub-1% slices become visually indistinguishable. Switch to a horizontal bar.
- **Stacked bars without 100% normalization** when the question is composition.

### 5. "I want to show correlation / relationship"

Are these two variables related? How?

1. **Scatterplot.** Two continuous variables, the canonical answer.
2. **Bubble chart.** Third variable encoded as area; the size legend is mandatory and must declare *area*, not diameter.
3. **Connected scatterplot.** When time is the implicit third axis.
4. **XY heatmap / hexbin.** When N is large enough that scatter overplots and individual points are no longer readable.
5. **Parallel coordinates.** Many continuous dimensions. Powerful but readability degrades fast — use only when the question genuinely involves >3 variables.

**Anti-patterns:**
- **Implying causation from correlation.** Cairo's repeated warning. The chart shows the relationship; the caption must not overclaim.
- **Bubble area encoding diameter** by mistake. A 4× value rendered as a 4× diameter is a 16× area — lie factor 4.
- **Fitted trend lines without showing the points.** The points carry the uncertainty.

### 6. "I want to show ranking"

Who's first? Who's last? Did rank change?

1. **Ordered horizontal bar.** Default. Bars sorted by value; rank is the order.
2. **Lollipop / dot strip plot.** When bars feel visually heavy or when only the position matters (not the magnitude difference).
3. **Slopegraph.** Ranking change between two time points. Emphasizes who moved.
4. **Bump chart.** Ranking change across many time points.
5. **Ordered proportional symbol.** Reader-impressionistic only.

**Anti-patterns:**
- **Alphabetical ordering when the question is ranking.** Few: ordering bias. Sort by the metric.
- **Ranking implicit but not declared.** "We're #2 in the market" with bars in random order.
- **Top-N truncation without a "+N more" indicator.** The reader doesn't know what was excluded.

### 7. "I want to show deviation from a target / reference"

How are we doing against the goal?

1. **Bullet graph.** Few's invention; replaces gauges and dials. Actual vs. target with qualitative bands (poor / acceptable / good). Compact, dense, honest.
2. **Diverging bar chart.** Variance from zero or from a benchmark, signed.
3. **Diverging stacked bar.** Likert-style sentiment around a neutral center.
4. **Surplus/deficit filled line.** Over/under a reference threshold over time. (FT Visual Vocabulary.)
5. **Waterfall.** Bridge from forecast to actual.

**Anti-patterns:**
- **Gauges and speedometers.** Few's repeated critique — low data-ink, no context, occupy disproportionate space.
- **Dial KPIs without a comparison band.** A speedometer at "75" without context could be wonderful or terrible.
- **"Red / green only" encoding** for performance. Colorblind users can't distinguish.

### 8. "I want to show flow / movement / process"

How do things move between states or steps?

1. **Sankey diagram.** Volumes between states (funnel, cohort flows, revenue source breakdown, energy/material flows).
2. **Chord diagram.** Bidirectional flows among a closed set.
3. **Network / node-link diagram.** Relational structure where the topology matters more than the volumes.
4. **Waterfall.** Sequential single-path flow, additive.
5. **Proportional funnel band** (per-step horizontal bars, scaled by absolute count). Compact alternative to a Sankey for linear funnels.

**Anti-patterns:**
- **Sankey with >~12 nodes per side.** Becomes spaghetti; readers can't trace flows.
- **Chord diagrams when flows are unidirectional.** Use a Sankey.
- **Networks without a layout algorithm declared in the caption.** Force-directed, hierarchical, and circular layouts produce different stories from the same edges.

### 9. "I want to show geographic / spatial"

Where is this happening?

1. **Choropleth.** Regional rates and ratios. **Never raw counts** — large-area bias makes Alaska or Texas always win. (Datawrapper Academy.)
2. **Proportional symbol map.** Counts and totals, since symbols don't depend on region area.
3. **Dot density.** Distribution of discrete events.
4. **Cartogram.** When population or exposure should drive area, not landmass.
5. **Flow map.** Origin–destination volumes.
6. **Heat map (geographic).** Densities; requires kernel and bandwidth disclosure.

**Anti-patterns:**
- **Choropleth of counts.** Use a proportional symbol or normalize to a rate first.
- **Choropleth on a rainbow scale.** No perceptual ordering, colorblind-hostile.
- **Maps without scale or projection note.** Mercator distortion, equal-area distortion — both lie about size.

## Cross-cutting honesty rules

These apply to every chart, regardless of category. They are what distinguish a *style guide* from a *tutorial*.

1. **Lie factor near 1.** Tufte: *"the representation of numbers, as physically measured on the surface of the graphic itself, should be directly proportional to the numerical quantities represented."* Visual length, area, or angle must match the data ratio.

2. **Bars start at zero. Always.** Bars encode length; truncating produces lie-factor distortion. Lines may use a non-zero baseline when the data range is far from zero — but the baseline must be **labeled and called out** in the caption. (Tufte's explicit exception.)

3. **No dual y-axes** unless: each axis is color-coded to its series, both axes are labeled with units, and the caption states that the two scales are independent. Cairo flags dual axes as the most common deception in business charts.

4. **No 3D for 2D data.** Cairo, Tufte, Few, Knaflic — unanimous. 3D distorts area perception and adds chartjunk.

5. **No rainbow color scales for ordered data.** Use sequential (single-hue light → dark) or diverging (two-hue around a neutral midpoint). Rainbow has no perceptual ordering and fails colorblind users. (Datawrapper Academy; Cairo.)

6. **Color is never the only encoding.** Always pair color with shape, label, position, or texture. Approximately 8% of men have red-green color vision deficiency. Test palettes in a colorblind simulator.

7. **Show the scale.** Every axis labeled with units. Every legend present. Bubble charts must declare that area (not diameter) encodes value.

8. **Small multiples must share a scale — or disclose otherwise.** Tufte's small multiples are powerful *because* they share a scale. If panels use independent scales, the caption must say so. Otherwise readers cross-compare falsely.

9. **No cherry-picked time windows.** Start and end points must be defensible (full available range, fiscal year, full cycle). If the window is non-standard, annotate the cut.

10. **No ordering bias.** Default to value-ordered for ranking, time-ordered for time series, alphabetical only when the reader will look up by name.

11. **Area-encoded numbers need a legend.** Treemaps, bubbles, isotypes, choropleths — none are precisely readable without a reference.

12. **Annotate the takeaway.** Knaflic's core stance: a chart without a stated insight is a chart without a job. The chart title states the finding ("Q3 churn doubled in SMB"), not the dimensions ("Churn by segment by quarter").

Cairo's five qualities are the test the chart must pass: **truthful, functional, beautiful, insightful, enlightening** — in that order. A beautiful chart that distorts is worse than an ugly chart that doesn't.

## Channel effectiveness reference

When designing a custom chart or evaluating an unfamiliar one, fall back to Munzner's channel ranking. The most accurate channels are at the top.

**For ordered (quantitative or ordinal) attributes:**

| Rank | Channel | Notes |
|------|---------|-------|
| 1 | Position on a common scale | Most accurate. Default for any ordered data. |
| 2 | Position on an unaligned scale | Slightly less accurate but still strong. |
| 3 | Length (1D size) | Bars use this. |
| 4 | Tilt / angle | Pie slices use this — and that's why pies are imprecise. |
| 5 | Area (2D size) | Treemaps, bubbles. Imprecise; needs a legend. |
| 6 | Depth (3D position) | Avoid for quantitative data. |
| 7 | Color luminance | Sequential colormaps. |
| 8 | Color saturation | Less accurate than luminance. |
| 9 | Curvature | Niche. |
| 10 | Volume (3D size) | Worst; avoid. |

**For categorical attributes:**

| Rank | Channel | Notes |
|------|---------|-------|
| 1 | Spatial region | Most distinguishable. |
| 2 | Color hue | Strong default for ~8 or fewer categories. |
| 3 | Motion | Animation; rare in static dashboards. |
| 4 | Shape | Marker shape; useful for small overlay sets. |

(After Cleveland & McGill 1984; Heer & Bostock 2010; consolidated in Munzner 2014.)

If a chart uses a low-ranked channel for the most important data, it's a candidate for redesign.

## Worked examples

These come from real engagement work. They illustrate how the taxonomy lands in practice. The examples are illustrative, not prescriptive — every engagement may invent new patterns.

### Example 1 — A magnet-to-money funnel, where stages drop unevenly

**Question category:** Change over time, but for *funnel position* rather than calendar time. Combines part-to-whole drop with magnitude.

**Wrong-fit options:** A vertical column chart of the five funnel stages would read as a category comparison, not a sequence. A pie of the surviving multi-pay segment would lose all the upstream drops.

**Right-fit choice:** A proportional funnel band — five horizontal segments, each scaled to absolute count, with ember-colored drop annotations between segments labeling the magnitude of each loss. Reads left-to-right as the actual funnel progression. Replaces what would have been five disconnected KPI tiles with one connected story.

**Honesty rules in play:** Bars start at zero (each segment's length is exact). Color encodes signal — the terminal "Buyers" segment uses the engagement's accent color; demoted "Multi-pay" uses faint surface to demote. Drop annotations make the loss explicit rather than letting the reader compute the gap.

### Example 2 — Comparing offer types where buyer count and revenue diverge

**Question category:** Compare amounts, but on two correlated dimensions (revenue + buyer count) plus a third (median time-to-purchase).

**Wrong-fit options:** Locally-scaled bars (the leader always 100%) lose absolute scale. A scatterplot of revenue × buyers would solve the comparison but lose the inherent ranking. A radar chart for three variables across six categories would be unreadable.

**Right-fit choice:** A dot plot on a shared $ axis. Each offer type is a row; dot position encodes revenue (absolute scale), dot size encodes buyer count, and a small mini-strip on the right encodes median time-to-purchase as a tick on a 0–90 day scale. Three variables, all at high-effectiveness channels (position, area, position).

**Honesty rules in play:** Area-encoded buyer count carries a legend. Absolute scale on the $ axis prevents top-bar-always-100% deception. The N/A row uses a dashed-outline gray dot — visually demoted, not hidden.

### Example 3 — Per-category conversion shape, where six funnels need cross-comparison

**Question category:** Flow / process, repeated across categories.

**Wrong-fit options:** A single Sankey for all six offer types becomes spaghetti. A stacked bar of conversion percentages loses the shape (steep drops at one stage vs. gradual decline are indistinguishable).

**Right-fit choice:** Per-category mini-funnels in a 2×3 grid (small multiples). Each card has five horizontally tiled bars (Opps → Touched → Called → Booked → Paid), color-graded by stage, scaled within-row by retention percentage. Cross-comparison surfaces leaks at a glance: high-converting categories show shallow drops; weak categories collapse at one specific stage.

**Honesty rules in play:** Small multiples disclose — the within-row scaling is documented in the column-header. Color carries category meaning (the "Paid" terminus uses the accent), not decoration.

### Example 4 — A buyer worklist where identity gaps must be visible without a separate column

**Question category:** Identity / per-row data with a status overlay.

**Wrong-fit options:** A standard table with a "data quality" column would bury the gap. A red row would signal "this row is bad" rather than "we couldn't match this person."

**Right-fit choice:** Enriched table with a 2-pixel left-edge tab per row (green = active subscription, ember = identity bridge gap, none = ordinary), a 32×32 avatar with initials (teal fill for matched buyers, gray for unmatched), and an inline ember chip on the latest-magnet column for "No magnet." Status is encoded structurally; the row stays readable.

**Honesty rules in play:** Color encodes signal (green / ember / none), not decoration. The unknown is demoted (gray avatar, italicized "No prior magnet recorded") without being hidden. Every gap is labeled with a fix path — "Identity bridge gap — fix at source."

## Related rules and conventions

- `docs/conventions/mart_presentation_style_guide.md` — companion guide for marts-to-operator-UI translation. Contains the Loop-3 quick-reference encoding lookup that this guide deepens.
- `.claude/rules/mart-naming.md` — table-layer naming conventions.
- `.claude/rules/operator-mode.md` — voice and tone defaults for operator-facing communication.
- `.claude/rules/data-modeling-process.md` — grain selection and the upstream DQ-test posture.
- `docs/conventions/dbt_style_guide.md` — SQL style, model configuration, naming and field conventions.

## Changelog and lessons learned

- **2026-05-05** — Initial draft. Companion to `mart_presentation_style_guide.md` (shipped same day on this branch). Question-first taxonomy adapted from FT Visual Vocabulary; channel ranking from Munzner; honesty rules consolidated from Tufte, Cairo, Few, and Knaflic. Worked examples drawn from the Lead Magnets redesign retroactive sweep.

## Helpful Reference Links

Books and primary sources:

- [Visualization Analysis and Design — Tamara Munzner (companion site)](https://www.cs.ubc.ca/~tmm/vadbook/)
- [The Truthful Art — Alberto Cairo](https://www.amazon.com/Truthful-Art-Data-Charts-Communication/dp/0321934075)
- [How Charts Lie — Alberto Cairo](https://wwnorton.com/books/9781324001560)
- [Show Me the Numbers — Stephen Few (Graph Selection Matrix PDF)](https://www.perceptualedge.com/articles/misc/Graph_Selection_Matrix.pdf)
- [Selecting the Right Graph — Few (PDF)](https://www.perceptualedge.com/articles/ie/the_right_graph.pdf)
- [Now You See It — Few (Perceptual Edge library)](https://www.perceptualedge.com/library.php)
- [Tufte's Principles of Data-Ink](https://jtr13.github.io/cc19/tuftes-principles-of-data-ink.html)
- [Tufte-isms summary — IEEE Spectrum](https://spectrum.ieee.org/tufteisms)
- [Storytelling with Data — Knaflic](https://www.storytellingwithdata.com/)

Practitioner references:

- [Financial Times Visual Vocabulary (GitHub)](https://github.com/Financial-Times/chart-doctor/tree/main/visual-vocabulary)
- [Datawrapper Academy](https://academy.datawrapper.de/)
- [Datawrapper — choropleth maps](https://academy.datawrapper.de/article/134-what-to-consider-when-creating-choropleth-maps)
- [Datawrapper — pie charts](https://academy.datawrapper.de/article/127-what-to-consider-when-creating-a-pie-chart)
- [Observable Plot — marks, channels, scales](https://observablehq.com/plot/features/marks)
- [Andy Kirk — Visualising Data](https://visualisingdata.com/)
- [datavizproject.com](https://datavizproject.com/) — chart catalog by family
