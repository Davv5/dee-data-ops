# Mart Presentation Style Guide

## Why this guide exists

A **mart** is a wide, denormalized table built so analysts can answer business questions in one query. It is structured for *engineers*. Every dashboard, BI view, walkthrough deck, and screenshot we hand to a client is structured for *operators* — non-technical decision-makers.

When the translation from mart → operator surface is improvised, jargon leaks. `dbt-mart` ends up in an eyebrow. `lead_magnet_buyer_detail · lead_magnet_detail` prints in an audit footer. "Rolls up", "window-attributed", "uncategorized" survive in subheads because they read smoothly to a data person. The same concept gets three names across three tabs ("Matched Buyers", "Customers", "matched paid buyers").

This guide is the standard for that translation. It is **domain-general** — it works whether the client is a fashion mentorship, a B2B SaaS, a healthcare practice, or a real estate brokerage. Specific vocabulary changes by client; the rules don't.

The guide answers one question:

> How do we turn a data mart into a surface that supports exactly one business decision for a non-technical operator — without ever requiring them to learn the data engineer's vocabulary?

## What this guide is not

- **It is not `mart-naming.md`.** That rule governs how a mart **table** is named in the warehouse (drop `fct_`/`dim_`, plural entities, singular `_detail`/`_summary`). This guide governs how the mart's **columns and rows** become user-facing copy and visualizations.
- **It is not a chart-making tutorial.** Visual encoding choices reference established practice (Tufte, Few, Knaflic). The guide names the defaults; it does not teach charting.
- **It is not a brand voice guide.** Each engagement may carry its own brand voice. This guide governs the floor (no jargon, declarative titles, demoted unknowns), not the ceiling (humor, register, taglines).

## Grounded in established practice

Each rule below cites either an external principle or an internal convention we already follow consistently.

**External principles consulted while drafting:**

- Stephen Few — *Information Dashboard Design*; *Common Pitfalls in Dashboard Design*
- Edward Tufte — data-ink ratio, small multiples, sparklines
- Cole Nussbaumer Knaflic — *Storytelling with Data*
- Tableau — Visual Best Practices (5-second rule, Z-layout)
- Microsoft — Power BI dashboard guidance; Writing Style Guide
- GOV.UK content design; plainlanguage.gov; Shopify Polaris; Atlassian Design System; Mailchimp Content Style Guide
- dbt Labs — *Stakeholder-friendly model names*; *On the Importance of Naming*; *How we style our dbt models*
- Nielsen Norman Group — empty-state and error-message guidelines; Carbon Design System empty states pattern

URLs in the appendix.

**Internal conventions made explicit:**

- KPI titles are imperative noun phrases ("Matched Buyers", "Repeat Paid", "Recovery Queue") — never "Total X" or "Count of Y."
- Helper text uses concrete units and evidence counts ("58 paid payments", "$X per buyer").
- The `_label` suffix in SQL is a translation contract: any column ending `_label` is human-readable, ready to display verbatim. Any column NOT ending `_label` requires translation before it surfaces.
- `_rate` and `_count` columns are pre-computed in SQL so the UI never does math.

These four conventions already hold across the existing operating surfaces. The guide elevates them from happy accidents to rules.

## Three loops every mart goes through before it ships

Every column that appears in front of an operator passes three tests, in order: **Selection** (does it belong on the surface), **Language** (what does it read as), **Encoding** (what does it look like).

### Loop 1 — Selection: what data should be shown

For each candidate mart column, answer Y/N to three questions:

1. **Decision test.** Does an operator make a decision based on this column? (Reorder a queue, follow up, refund, fix a data source, schedule a call.)
2. **Subtraction test.** If we removed this column from the UI, would the operator be dumber about reality?
3. **Honesty test.** When this column is missing/null/unknown, is its absence labeled in plain English with a fix path?

Only **Y / Y / Y** columns reach the surface. Others stay queryable in the mart (analysts can pull them in BigQuery), but they do not render.

This gate is what prevents a 6-equal-tile KPI strip and a column dump. It enforces Few's *one surface, one decision* and Power BI's *every dashboard page should support one decision*.

### Loop 2 — Language: how surfaced columns are presented

#### The triplet

Every surfaced column gets a triplet, written down once per mart:

| Warehouse | Mart alias | Operator label |
|---|---|---|
| `total_net_revenue_after_refunds` | `net_revenue` | "Collected Net" |
| `is_multi_payment_buyer` | `is_payment_plan_buyer` | "On a payment plan" |
| `purchase_magnet_attribution_flag` | `attribution` | "How we credit the sale" |
| `pre_purchase_funnel_path` | `funnel_path` | "How they got to checkout" |
| `latest_prior_lead_magnet_name` | `last_magnet` | "Most recent magnet they engaged with" |

The **operator label** has to pass the **say-out-loud test**: *"Would the client say this on a Zoom call to a customer or teammate?"* If you can't write the operator label, the column isn't ready to surface.

#### Forbidden vocabulary

These tokens must never appear on an operator surface (dashboard, walkthrough, BI view, screenshot, exported report):

- Database / pipeline jargon: `dbt`, `mart`, `dim_`, `fct_`, `stg_`, `bq-ingest`, `dbt-mart`, "schema", "warehouse" (as a noun for the data layer), "pipeline" (when referring to ETL).
- Raw column names in any user-visible string: `*_id`, `*_sk`, `*_flag`, `*_at` (when shown without translation), and any literal column reference (`pre_purchase_funnel_path`, `lead_magnet_buyer_detail`, etc.).
- Modeling terminology: "rolls up", "window-attributed", "attribution flag", "quality flag", "uncategorized", "fact", "dimension", "grain", "denormalized".
- Internal product names that don't mean anything outside the team: `customer-360` (use "full customer history" or whatever the operator says), `bq-ingest-report`, `Customer 360` is acceptable as a UI label only when the team uses that name out loud.
- Discipline-specific abbreviations the operator wouldn't use: "MoM Δ", "ARR cohort", "MRR delta", "T-30 retention", unless the operator role uses these phrases in conversation.

When in doubt, say it out loud. If it sounds like data jargon, it is data jargon.

#### Substitution table

| Bad (do not surface) | Good (operator-friendly) |
|---|---|
| "Opportunities" (CRM term) | "Lead signups" / "Magnet pickups" / "Prospects" — pick the one that matches what the operator says |
| "Multi-pay" / "Multi-payment buyer" | "On a payment plan" |
| "No work logged" | "Never followed up" |
| "Touched" (sales-ops term) | "Got a follow-up" / "Was contacted" |
| "Latest known X" | "X they engaged with most recently" / "Most recent X" |
| "Window-attributed" | (drop entirely; the time window is named elsewhere) |
| "Uncategorized offer type" | "Offer type unknown" |
| "Coverage" (as a KPI standalone) | "Buyers we can credit to a magnet" / specific to the metric |
| "lead_magnet_buyer_detail · lead_magnet_detail" (in audit) | "Buyer-level + opportunity-level data" |
| "Rolls up to the most recent magnet" | "Is credited to the most recent magnet" |
| "N/A" (as a value cell) | "Not yet recorded" / "Waiting on data from [source]" / specific to context |

Engagements may extend the substitution table with their own client-specific entries. Extensions live in the per-engagement glossary (below), not here.

#### Declarative-title rule

Section headings (H1s, H2s, panel titles) are sentences that answer an operator question, not utilitarian noun phrases.

| Bad | Good |
|---|---|
| "Activity vs Money" | "Where opportunities turn into payments." |
| "Offer Types Panel" | "Which formats show up before money." |
| "Buyer Drilldowns" | "The buyers behind the number." |
| "Attribution Honesty" | "Latest known magnet, not magic credit." |
| "Top Magnets" | "The eight magnets that produced money." |

Verb-first when possible. Active voice. Grade-7 reading level (per Polaris and GOV.UK). One thought per heading.

### Loop 3 — Encoding: how surfaced data is displayed

Each data **shape** has a default visualization. Match the shape to the question.

| Data shape | Default encoding | Source |
|---|---|---|
| Single key conversion (the answer to the page's headline question) | Display-scale anchor number, upper-left, with delta if recent change matters | Few; Tableau Z-layout |
| Sequence with retention (n → n−1 → n−2…) | Proportional funnel band, segments scaled by absolute count, ember drop-annotations between segments | — |
| Categorical comparison on a $ axis | Dot plot (dot size = volume, position = absolute $) or horizontal lollipop | Tufte; Few |
| Share-of-total comparison | Stacked bar (single bar). Pair two stacked bars (count vs revenue) when divergence is the story. Never pie with more than 3 slices. | Knaflic |
| Per-category conversion shape | Small-multiples mini-funnels, color-graded by step | Tufte (small multiples) |
| Identity / per-row data | Enriched table: avatar with initials + offer-type chip + sparkbar for relative spend + threshold-colored % | — |
| Distribution | Histogram or strip plot with P25 / P50 / P75 markers | Few |
| Time series | Sparkline inline with a number; small-multiples for cross-segment time comparison | Tufte (sparklines) |
| Unknown / data gap | Demoted styling: gray, dashed outline, italic, lower contrast. **Never** identical to a real value. | NN/g; Carbon Design System |

Defaults are starting points. Deviations require a one-line justification in the per-mart presentation file.

The table above is the **quick reference**. For the full chart-selection taxonomy — every common operator question with priority-ordered chart options, anti-patterns per category, and twelve cross-cutting honesty rules — read `docs/conventions/chart_style_guide.md`. That guide is the source of truth for visual encoding decisions; this table stays as the skim-level lookup.

## Cross-cutting rules

These apply across all surfaces, not just one.

1. **One surface, one decision.** Each page, slide, or PDF supports exactly one operator decision. If the page serves two, split it. *(Few; Power BI.)*
2. **Upper-left is the answer.** The headline metric or sentence occupies the top-left; supporting context flows down and right. *(Tableau Z-layout; Few.)*
3. **Five-second test.** A non-technical operator should state what the surface is for in five seconds. If they can't, reduce density before adding anything. *(Tableau Visual Best Practices.)*
4. **Color encodes signal, never decoration.** Threshold-driven (≥X green, <Y ember), state-driven (active / canceled), or category-driven (offer type). One accent for "act on this," one alert for "something is wrong," gray for everything else. Reuse the same color for the same entity across every surface. *(Knaflic; Power BI.)*
5. **No internal terms surface, ever.** See forbidden-vocabulary list. This is non-negotiable.
6. **Section titles are sentences.** Declarative, verb-first, answering an operator question. See declarative-title rule above.
7. **Every chart has absolute scale.** Axis labels, max marker, tick reference. Locally-scaled bars (the top bar always 100% wide, the runner-up squeezed to 40%) lose absolute magnitude. The exception is intentional within-row ratio comparison; this gets a one-line justification.
8. **Every gap is labeled with a fix path.** Not "N/A" but "Identity bridge gap — fix at source." Not blank but "Waiting on data from Stripe (last sync: 2 hr ago)." *(NN/g empty-state guidelines.)*
9. **Every metric carries provenance.** One-sentence definition, time window, last-updated timestamp. Accessible from the surface (tooltip or audit drawer) — not buried two pages deep. *(Few "context"; Power BI.)*
10. **Empty / loading / error states are designed.** Empty: "No buyers yet — first record will appear here." Loading: skeleton matching the panel's eventual shape. Error: ember left-border with one-line cause + retry, scoped to the panel, not nuking the whole page.

## The operator-facing glossary

Every engagement maintains an operator-facing glossary — the warehouse → mart → operator label triplet table for every column that surfaces. It lives next to the mart definition:

```
2-dbt/models/marts/
  ├── lead_magnet_buyer_detail.sql
  ├── lead_magnet_buyer_detail__presentation.md   ← the glossary
  ├── revenue_detail.sql
  ├── revenue_detail__presentation.md
  …
```

The glossary file format:

```markdown
# {Mart name} — Presentation

## Headline operator question

(one sentence — the single decision this mart supports)

## Surfaced columns

| Mart alias | Operator label | Default encoding | Notes |
|---|---|---|---|
| net_revenue | "Collected Net" | KPI tile (currency) | Refunds already netted |
| buyers | "Customers" | KPI tile (count) | First-purchase event count |
| …

## Columns excluded from the surface

| Mart alias | Why excluded |
|---|---|
| contact_sk | Internal join key |
| mart_refreshed_at | Provenance only — surfaces in audit drawer, never in body |
| …

## Canonical phrasings used on this surface

| Concept | Variant chosen | Why |
|---|---|---|
| The person who paid | "Customers" | Matches Retention; Lead Magnets says "Buyers" only on first-purchase context |
| …

## Cross-references

- Mart definition: `lead_magnet_buyer_detail.sql`
- Surfaces consuming this mart: `/lead-magnets`, `/customers/{id}`
```

The glossary is reviewed when a new column is added to the mart and when a new surface consumes it. A mart isn't ready for an operator surface until its glossary file is filled in.

## Canonical phrasings — cross-tab consistency

When the same concept appears across multiple surfaces of one engagement, pick one canonical phrasing. Don't let drift accumulate.

The cross-tab analysis of D-DEE's existing surfaces surfaced unresolved drift. These are example resolutions; each engagement runs the same exercise:

| Concept | Variants found | Canonical |
|---|---|---|
| The person who paid | "Matched Buyers" / "Customers" / "matched paid buyers" | "Customers" generally; "Buyers" only when scoped to first-purchase |
| Total revenue minus refunds | "Collected Net" / "Revenue" / "Total" | "Collected Net" |
| Repeat behavior — three distinct concepts conflated under similar names | "Repeat Paid" / "Repeat Payment Type" / "Customer-Month States" | Distinguish: **Repeat customer** (person paid more than once) / **Repeat payment** (single transaction in a payment-plan series) / **Repeat month** (subscription month) |
| Source-of-truth section | "Data Honesty" / "Audit Tables" / "Source Tables And Audit" | "Data Honesty" for quality-flag context; "Source tables" for mart references |

Engagement onboarding: the data-engineer agent runs this exercise as part of Phase 1 (Discovery) when more than one surface consumes the same mart layer.

## Pre-ship checklist

A reviewer answers all twelve questions before merging an operator-facing change.

1. Does any user-facing string reference a column name? (Reject.)
2. Does any user-facing string reference a mart table name, including in audit footers? (Reject.)
3. Does any section heading read as a utilitarian noun phrase instead of a declarative sentence? (Reject.)
4. Does any KPI carry decorative color (not threshold/state/category-driven)? (Reject.)
5. Does any chart use locally-scaled bars without an absolute reference? (Reject unless within-row comparison is the explicit intent.)
6. Does any "N/A" / null / unknown render at the same weight as a real value? (Reject.)
7. Does the page have one anchor metric in the upper-left? (Or has the absence been justified in writing?)
8. Has the operator-facing glossary been filled in for every column shown? (Reject if missing.)
9. Has the forbidden-vocabulary list been searched against every user-facing string? (Reject any hits.)
10. Five-second test — can a non-technical operator state what the surface is for in five seconds? (If not, reduce density before adding anything.)
11. Would the client say each H1, H2, eyebrow, and KPI label out loud on a Zoom call without flinching? (If not, rewrite.)
12. Does every metric carry definition + time window + last-updated timestamp accessible from the surface (tooltip, audit drawer, or footer)?

A future skill (`mart-presentation-audit`, follow-up task) will run this checklist programmatically against a view component + its mart. For now, reviewers run it by hand.

## Worked example — Lead Magnets retroactive sweep

The Lead Magnets operating view (`/lead-magnets` in the D-DEE dashboard) is the reference implementation of this guide. A retroactive sweep against the checklist surfaces concrete defects:

| Surface element | Failure mode | Checklist Q | Fix |
|---|---|---|---|
| `dbt-mart` eyebrow | Internal term in user-facing string | Q9 | Remove; replace with "BUYER TRUTH" or similar declarative eyebrow |
| `lead_magnet_buyer_detail · lead_magnet_detail` printed in audit footer | Mart-name leak | Q2 | Replace with "Buyer-level + opportunity-level data" |
| `pre_purchase_funnel_path` quoted in subhead | Column-name leak | Q1 | Replace with "How they got to checkout" |
| "WINDOW-ATTRIBUTED" eyebrow | Modeling jargon | Q9 | Drop entirely; the time window is named elsewhere |
| "Multi-pay" funnel step | Compressed jargon | Q9, Q11 | "On a payment plan" |
| "Opportunities" funnel step | CRM jargon | Q9 | "Magnet pickups" or "Lead signups" — match what Dee says |
| "No work logged" leak bucket | SDR-ops jargon | Q9 | "Never followed up" |
| "Latest known magnet, not magic credit" subhead "rolls up" phrasing | BI/SQL jargon | Q9 | "Every customer's revenue is credited to the most recent magnet…" |
| "Coverage" KPI label | Compressed; fails say-out-loud | Q11 | "Buyers we can credit to a magnet" |
| "Uncategorized offer type" attribution flag | Taxonomy jargon | Q9 | "Offer type unknown" |
| 6 equal-weight KPI tiles | No anchor metric | Q7 | One display-scale anchor + sub-KPIs ranked by importance |
| Decorative chip chromas (green "buyer truth", blue "source-aware", amber triangle) | Color is decoration, not signal | Q4 | One semantic system: ink-on-white chips with one accent color for state |
| 2px locally-scaled bars in Offer Types | No absolute scale | Q5 | Dot plot on shared $ axis |

Every defect maps to a checklist question. If a defect doesn't map, the checklist is incomplete — extend it.

## Related rules and conventions

- `docs/conventions/chart_style_guide.md` — companion guide for visual encoding. Expands the Loop-3 quick-reference table above into a full chart-selection taxonomy organized by operator question, with anti-patterns per category and twelve cross-cutting honesty rules.
- `.claude/rules/mart-naming.md` — table-layer naming (drop `fct_`/`dim_`, plural entities, singular `_detail`/`_summary`). This guide is its UI counterpart.
- `.claude/rules/operator-mode.md` — voice and tone defaults for operator-facing communication.
- `.claude/rules/data-modeling-process.md` — grain selection and the upstream DQ-test posture that a well-presented mart depends on.
- `docs/conventions/dbt_style_guide.md` — SQL style, model configuration, naming and field conventions.

## Changelog and lessons learned

- **2026-05-05** — Initial draft. Grounded in the Lead Magnets redesign retroactive sweep + cross-tab analysis of all 5 D-DEE operating views (Speed-to-Lead, Lead Magnets, Revenue, Retention, Customer 360). Substitution table seeded from concrete failure modes observed in those views.

## Helpful Reference Links

External:

- [Information Dashboard Design — Stephen Few (PDF)](https://public.magendanz.com/Temp/Information%20Dashboard%20Design.pdf)
- [Common Pitfalls in Dashboard Design — Few / Perceptual Edge](https://www.perceptualedge.com/articles/Whitepapers/Common_Pitfalls.pdf)
- [Tufte's Principles of Data-Ink](https://jtr13.github.io/cc19/tuftes-principles-of-data-ink.html)
- [Sparklines History — Tufte](https://www.edwardtufte.com/notebook/sparklines-history-by-tufte-1324-to-now/)
- [Storytelling with Data — Knaflic (PDF)](https://cdn.bookey.app/files/pdf/book/en/storytelling-with-data.pdf)
- [Tableau Visual Best Practices](https://help.tableau.com/current/blueprint/en-us/bp_visual_best_practices.htm)
- [Power BI Dashboard Design Tips — Microsoft](https://learn.microsoft.com/en-us/power-bi/create-reports/service-dashboards-design-tips)
- [GOV.UK Content Design](https://www.gov.uk/guidance/content-design/writing-for-gov-uk)
- [plainlanguage.gov Guidelines](https://plainlanguage.gov/guidelines/)
- [Shopify Polaris — Voice and Tone](https://polaris.shopify.com/content/voice-and-tone)
- [Atlassian Design System — Voice and Tone Principles](https://atlassian.design/content/voice-and-tone-principles/)
- [Mailchimp Content Style Guide — Voice and Tone](https://styleguide.mailchimp.com/voice-and-tone/)
- [Microsoft Writing Style Guide](https://learn.microsoft.com/en-us/style-guide/welcome/)
- [dbt — Stakeholder-friendly model names](https://docs.getdbt.com/blog/stakeholder-friendly-model-names)
- [dbt — On the Importance of Naming](https://docs.getdbt.com/blog/on-the-importance-of-naming)
- [NN/g — Error-Message Guidelines](https://www.nngroup.com/articles/error-message-guidelines/)
- [Carbon Design System — Empty States Pattern](https://carbondesignsystem.com/patterns/empty-states-pattern/)

Internal:

- `.claude/rules/mart-naming.md`
- `.claude/rules/operator-mode.md`
- `.claude/rules/data-modeling-process.md`
- `docs/conventions/dbt_style_guide.md`
