---
paths: ["dbt/models/marts/**"]
---

# Mart Naming Conventions

The marts layer is the **business-facing surface** of the dbt project — the schema that dashboards, BI tools, and client stakeholders connect to directly. It follows different naming rules than `staging` and `warehouse` because its audience is different.

## Rule 1 — Drop `fct_` / `dim_` prefixes

Staging and warehouse use Kimball technical naming (`stg_`, `dim_`, `fct_`). Those prefixes are useful internally for reasoning about grain and role, but they're noise to non-data stakeholders.

In `marts/`, use **business-friendly names** that describe what the table represents in the client's language.

| Warehouse (keep Kimball) | Marts (business-friendly) |
|---|---|
| `fct_calls_booked` | `sales_activity_detail` |
| `fct_revenue` | `revenue_detail` |
| `dim_contacts` | `contacts` |
| `dim_offers` | `offers` |

> "Businesses aren't super familiar with facts and dimensions and it's not really a helpful naming convention for them... make it easier and more friendly names for the business to use."
> — *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook

## Rule 2 — Fewer, wider marts over many narrow ones

Resist the urge to create a mart per dashboard or per report. That pattern creates fluff and drift.

Instead, build **one wide, denormalized table per business domain** that serves many dashboards via slice-and-dice in the BI tool. A second mart is added only when a genuinely different grain emerges (e.g., `sales_activity_detail` at the booked-call grain vs. `revenue_detail` at the payment-transaction grain).

> "I typically don't like to create Marts tables one to one for each report... I think that can get a little messy and create a lot of fluff in your data that becomes outdated."
> — *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook

## Rule 3 — Always materialize as tables

Marts are consumed by BI tools directly. Query performance matters at the dashboard-refresh layer. Configure at the directory level in `dbt_project.yml`, not per-model.

> "Marts should always be configured as tables."
> — *`dbt_style_guide.md`*

## Rule 4 — Pluralize entity names; singular for detail/summary

- Entity tables (customer-like): plural — `contacts`, `offers`, `sdrs`
- Detail / summary marts: singular noun phrase ending in `_detail` or `_summary` — `sales_activity_detail`, `revenue_summary`

## Rule 5 — Schema-per-audience as the scaling lever

When a client grows beyond a single stakeholder group, split the marts layer into audience-specific schemas controlled by warehouse-level permissions:

```
marts_sdr          → SDR-facing dashboards (no payment-plan details)
marts_leadership   → sales leadership (full funnel + rep leaderboard)
marts_finance      → CFO / finance (full revenue + refunds + collections)
```

Do not split schemas prematurely. Start with a single `marts` schema; split only when permissions or audience divergence requires it.

## Rule 6 — Only `marts` gets business-friendly naming

This rule does **not** apply to staging or warehouse. In those layers:
- `staging/` always uses `stg_<source>__<table>.sql`
- `warehouse/dimensions/` always uses `dim_<entity>`
- `warehouse/facts/` always uses `fct_<business_process>`

The prefix switch is exactly what makes the mart layer recognizable as "the part the business touches."

## Lessons Learned

- (Add entries here as issues are discovered)
