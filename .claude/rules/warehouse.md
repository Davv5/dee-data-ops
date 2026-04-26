---
paths: ["2-dbt/models/warehouse/**"]
---

# Warehouse-layer conventions

Load when working on any file under `2-dbt/models/warehouse/`. Detailed style reference: `docs/conventions/dbt_style_guide.md`. Modeling principles are grounded in the Data Ops NotebookLM corpus (query via the `ask-corpus` skill).

## Structure

- **Star schema, Kimball-style.** Facts at the center (verbs), dimensions around them (nouns/adjectives). Never blend them.
- **Two subdirectories:** `warehouse/dimensions/` and `warehouse/facts/`.
- **Materialized as tables.** Configured at the `warehouse/` directory level in `dbt_project.yml` via `+materialized: table`.
- **Only selects from staging layer.** Never from `source(...)`. Never from Raw. Marts never appear upstream of warehouse.
- **Build order: dimensions first, then facts** — facts need dim surrogate keys to join on.

## Naming

- Dimensions: `dim_<entity>.sql` (e.g. `dim_contact.sql`, `dim_offer.sql`, `dim_calendar_dates.sql`)
- Facts: `fct_<event>.sql` (e.g. `fct_meetings.sql`, `fct_payments.sql`, `fct_opportunities.sql`)
- Surrogate keys: `<entity>_sk`
- Natural keys: `<entity>_id`
- Foreign keys in facts: `<referenced_entity>_sk`

## Surrogate keys — the single most load-bearing decision

Every dimension has a surrogate key generated via `dbt_utils.generate_surrogate_key`. Every fact carries FK surrogate keys to every dim it joins to.

```sql
{{ dbt_utils.generate_surrogate_key(['contact_id', 'location_id']) }} as contact_sk
```

- Hash the combination that defines uniqueness — not just `id`.
- Testable: every `<entity>_sk` column gets `unique` + `not_null` tests.
- SCD interaction: when any attribute hashed into the SK changes, the hash changes → new row.

## Dimension rules

- **Wide, flat, denormalized.** Pre-join state/zip/address/etc into the dim. Do not create dim-to-dim joins downstream.
- **Never join dimension to dimension directly.** Widen the dim or handle flattening at the mart layer. Dim-to-dim joins blow up with many-to-many.
- **SCD Type 2 where attribute history matters.** Use `snapshots/` + `dbt_utils`-generated `dim_contact_snapshot.sql`. Required metadata columns: `active_from`, `active_to`, `is_active`.
- **Join pattern for current values:** `join dim on fact.dim_sk = dim.sk and dim.is_active = true`. Omit `is_active` for as-of-event joins (use date range instead).

## Fact rules

> **Confirm the grain BEFORE writing any SQL.** "Lowest granularity you can justify" is the rule; "what is one row of this fact?" is the question to answer in plain English first — in the plan document or the `_facts__docs.md` — before writing the model. The grain decision is the load-bearing decision in the whole model; downstream parity drift usually traces back here. (source: `data-modeling-process.md` Step 2; "[AE] The Order in which I Model Data" — Joshua Kim, Medium)

- **Lowest granularity you can justify.** Transactions > order line items > orders. You can roll up in a mart; you can't roll down.
- **Contents: surrogate key + FK surrogate keys + numeric aggregables + event-grain timestamps.**
- **No descriptive text in facts.** Names, addresses, labels live in dims.
- **Dim lookups grab the SK only.** Join dim to get `<entity>_sk`, not to grab descriptive columns. Those come from joining the dim at query time (or at the mart layer).

### Fact × Dim join — verify uniqueness before joining

Before joining a fact to a dim on a non-surrogate key (or to any dim where SCD Type 2 may have multiple rows for the same natural key), verify the dim's join key is unique. Otherwise the fact fans out and the headline metric quietly doubles.

```sql
-- BAD: dim_product has SCD Type 2 history; product_id is not unique
select f.*, d.product_name
from {{ ref('fct_orders') }} f
left join {{ ref('dim_product') }} d on f.product_id = d.product_id

-- GOOD: collapse to the current row before join
with current_dim_product as (
    select * from {{ ref('dim_product') }}
    qualify row_number() over (partition by product_id order by updated_at desc) = 1
)
select f.*, d.product_name
from {{ ref('fct_orders') }} f
left join current_dim_product d on f.product_id = d.product_id
```

> **`COUNT(1)` after every join.** If row count changes unexpectedly, the join key isn't unique. Catching this once saves hours of debugging. (source: `data-modeling-process.md` macro #4; Kim "last tip")

## Required per-directory YAML

- `dimensions/_dimensions__models.yml` — model declarations + tests
- `dimensions/_dimensions__docs.md` — column descriptions (for `dbt docs generate`)
- `facts/_facts__models.yml` — model declarations + tests
- `facts/_facts__docs.md` — column descriptions

Minimum tests:

```yaml
models:
  - name: dim_contact
    columns:
      - name: contact_sk
        tests: [unique, not_null]

  - name: fct_payments
    columns:
      - name: payment_sk
        tests: [unique, not_null]
      - name: contact_sk
        tests:
          - not_null
          - relationships:
              to: ref('dim_contact')
              field: contact_sk
```

Every fact FK must have a `relationships` test. No exceptions.

### Six-perspective DQ check (warehouse + marts)

`unique` + `not_null` + `relationships` are the floor, not the ceiling. For warehouse + mart models, cover all six perspectives where they apply:

1. **Uniqueness** — `unique` on PK / surrogate key
2. **Nullability** — `not_null` on join keys and load-bearing columns
3. **Referential integrity** — `relationships` test on every fact FK
4. **Range** — `dbt_expectations.expect_column_values_to_be_between` on bounded numerics (e.g. `amount_cents > 0`, `pct_field BETWEEN 0 AND 100`)
5. **Freshness** — `freshness` block in source YAML; `dbt_expectations.expect_row_values_to_have_recent_data` for derived models with a known SLA
6. **Volume** — row-count vs. prior run for incremental models; `expect_table_row_count_to_be_between` against an absolute floor for marts

> **Test as far upstream as possible.** Bronze/Silver tests catch issues at the source where they're cheap to fix. Marts get unit-test-grade verification once before deploy, not on every refresh — running a full DQ suite per refresh is expensive and finds nothing the upstream tests didn't already find. (source: `data-modeling-process.md` Step 6; Kim Step 6)

### AI-readability for `_<dir>__docs.md`

`dbt docs` descriptions and the per-dir `__docs.md` files are read by humans AND by AI agents (NL2SQL skills, the `ask-corpus` skill). Optimize for both:

- **Lead with grain + PK in one line.** "One row per booking × touch-event. PK: `booking_id`, `touch_id`."
- **One line of purpose.** "Used by the Speed-to-Lead headline metric and the SDR leaderboard."
- **Concise descriptions per column.** A few words that describe what the column means, not paragraphs. Long descriptions hurt NL2SQL search precision.
- **Avoid duplicating obvious info.** "The contact ID" adds nothing. "Canonical contact ID, joined back to GHL via `dim_contact.contact_id`" is useful.

## Materialization nuance

The directory-level defaults handle the common case (`staging` = view, `warehouse` = table, `marts` = table). The judgment call is on **intermediate models** (e.g., `models/intermediate/`-shaped models) that don't fall under the directory defaults.

**Physicalize when:**

- Frequent reuse across many downstream models (one expensive computation, many readers)
- Expensive computation (large window functions, big joins, heavy aggregations)
- The downstream needs a stable point-in-time snapshot

**Don't physicalize when:**

- Used by exactly one downstream model
- Cheap to recompute
- The data is highly volatile and a stale physicalized copy would be worse than a fresh recompute

> **Silver Layer scalability is the hardest thing to fix later.** Build for scalability from the first model, even if it feels overkill. Many narrow tables you'll regret; one wide reusable table you won't. (source: Kim Step 4)

## Incremental + idempotency

Any incremental model must prove it can run hundreds of times with the same final result.

- The `unique_key` config field must point at a column that is genuinely unique within the incremental window.
- Filter incremental rows on a watermark column with strictly-increasing semantics (`_ingested_at`, `event_ts`, `updated_at`) — never on `current_timestamp()` or `now()`.
- For late-arriving rows, use `merge` strategy with a lookback window in the `is_incremental()` block (e.g., 7 days) so a delayed row doesn't get permanently dropped.
- Run the model twice in dev against the same target and verify the row count + content is identical.

> **Idempotency check is non-negotiable for incremental models.** A non-idempotent DAG drifts silently — every retry doubles or skips rows. (source: Kim Step 5)

## Clustering

BigQuery clustering reduces lookup cost for filtered queries but increases load cost. Apply selectively:

- **Cluster** when the table is large (> 10M rows or > 10 GB) AND a column is frequently used as a filter or join key in downstream models or BI queries
- **Don't cluster** small tables (the load overhead exceeds any scan benefit), or columns with very high cardinality (each cluster block has too few rows to skip), or columns nobody filters on

Document the clustering decision in `_<dir>__docs.md` per table, not in the model SQL — the rationale travels with the catalog.

## Identity spine (the load-bearing dim)

`dim_contact` is the golden-contact dimension — the canonical entity every downstream join references. Build rules:

- **Anchor on GHL** (`stg_ghl__contacts`) — v1 uses GHL as the **single** anchor for contact identity. No multi-source bridges in v1.
- Canonical join keys reserved for future cross-source enrichment: `email_norm`, `email_canon` (gmail dot/plus normalized), `phone_last10`
- Surrogate key: `generate_surrogate_key(['location_id', 'contact_id'])`
- SCD Type 2 on attribute changes (assigned user, email, pipeline state)

When v1+N adds non-GHL sources, bridges resolve to the GHL `contact_id` upstream of `dim_contact` (in staging or an intermediate model) — never by widening `dim_contact` with multi-source key fan-out.

## Lessons learned

*(Populate as warehouse-layer issues arise.)*
