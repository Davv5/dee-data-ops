---
paths: ["2-dbt/models/staging/**"]
---

# Staging conventions

Load this rule when working on any file under `2-dbt/models/staging/`. Detailed style reference: `docs/conventions/dbt_style_guide.md`.

## Structure

- **Staging models are 1:1 with raw source tables.** One view per Raw table that downstream logic uses. Not one per every Raw table.
- **Always materialized as views.** Storage is not duplicated. Configured at the `staging/` directory level in `dbt_project.yml` via `+materialized: view`.
- **Only staging models may select from `source(...)`.** All other layers select via `ref(...)`. Never re-declare a downstream model as a source in another `sources.yml` — breaks lineage.
- **No joins in staging.** It is 1:1. If you think you need a join in a staging view, you actually need an intermediate model or a warehouse model.

## Naming

- Filename: `stg_<source>__<table>.sql` — double underscore between source and table name, plural table names (e.g. `stg_ghl__contacts.sql`, `stg_calendly__scheduled_events.sql`).
- Column names: `snake_case`, business terminology over source terminology.
- Primary key column: `<entity>_id` (e.g. `contact_id`, not just `id`).
- Timestamps: `<event>_at` in UTC (e.g. `created_at`, `updated_at`). Non-UTC columns take a suffix like `_pt`.
- Booleans: `is_` or `has_` prefix (e.g. `is_active`, `has_payment`).
- Money: decimal (e.g. `19.99`), not cents. If cents is the source, suffix `_in_cents`.

## What goes in a staging view

- Renaming columns to business terminology
- Casting types (`cast` in SELECT, `safe_cast` only in WHERE / filter contexts — see CAST vs SAFE_CAST below)
- Rounding decimals, currency conversions where needed
- JSON flattening (`json_value`, `json_extract_array`, `unnest`)
- Simple `case when` transforms
- Filtering out test/bad rows (filtered in the `where` clause of the `final` CTE)

### CAST vs SAFE_CAST

Use `CAST` in SELECT clauses and `SAFE_CAST` only inside WHERE / filter conditions.

- **`CAST` in SELECT** — fails loudly if a row has an unparseable value. That failure is a feature: the bad row surfaces immediately, you fix it upstream, downstream stays clean.
- **`SAFE_CAST` in SELECT** — silently returns NULL on parse failure. The bad row propagates as NULL all the way to the mart, where someone notices a metric is wrong months later.
- **`SAFE_CAST` in WHERE** — fine. You're filtering, not transforming; a NULL on a filter side just means "this row doesn't match," which is the right semantics.

```sql
-- BAD: silent NULL propagation
select safe_cast(amount as numeric) as amount from source

-- GOOD: loud failure on bad data
select cast(amount as numeric) as amount from source

-- ALSO GOOD: safe_cast in a filter
select * from source where safe_cast(event_ts as timestamp) >= '2026-01-01'
```

(source: `data-modeling-process.md` Step 5; `dbt_style_guide.md` SQL section; "[AE] The Order in which I Model Data" — Joshua Kim, Medium)

## What does NOT go in a staging view

- Joins to other tables (including other staging views or dims)
- Aggregations (`group by`, window functions)
- Business logic that spans multiple sources
- SCD logic — that lives in `warehouse/` snapshots + dims

## Required per-directory YAML

Each `staging/<source>/` directory must include:

- `_<source>__sources.yml` — declares `source('raw_<source>', '<table>')` for every Raw table used
- `_<source>__models.yml` — declares model tests (at minimum `unique` + `not_null` on the primary key)

Example:

```yaml
# 2-dbt/models/staging/ghl/_ghl__models.yml
version: 2
models:
  - name: stg_ghl__contacts
    columns:
      - name: contact_id
        tests: [unique, not_null]
      - name: email_norm
        tests: [not_null]
```

## CTE structure (enforced by style guide)

```sql
with

source as (
    select * from {{ source('raw_ghl', 'contacts') }}
),

final as (
    select
        contact_id,
        location_id,
        lower(trim(email)) as email_norm,
        created_at,
        updated_at
    from source
    where contact_id is not null
)

select * from final
```

- `{{ ref() }}`/`{{ source() }}` lives in CTEs at the top of the file, never inline
- `final` CTE is the last CTE; the file ends with `select * from final`
- Four-space indent; trailing commas; field names lowercase; `group by 1,2,...`; explicit `inner join`/`left join`

## Lessons learned

*(Populate as staging-layer issues arise. Example entries:)*

- When JSON flattening GHL contacts, the timestamp field returns both epoch-ms and ISO-8601 strings depending on call. The CAST-in-SELECT rule above means cast in SELECT and let one form fail loudly; do NOT silently coerce both with `safe_cast` — that hides the underlying schema inconsistency that should be fixed in the extractor.
- Calendly `invitee_email` can be null; do not rely on it as a non-null PK source. Use `invitee_uuid`.
- **No `join` keyword in any file under `2-dbt/models/staging/**`.** Sub-selects and `unnest` are fine; any row-combining logic must live in `warehouse/` or `marts/`. Enforced by the `no-joins-in-staging` pre-commit hook.
- **No hard-coded `entity_type` filters that assume a legacy single-table GHL raw layout.** Per-entity raw tables (`raw_ghl.ghl__<entity>_raw`) do not carry an `entity_type` column — any `WHERE entity_type = '...'` filter will silently return zero rows.
- **`QUALIFY ROW_NUMBER() OVER (...) = 1` in staging is a code smell, not a fix.** It usually means the upstream raw table has a true grain different from what staging assumes. Push the dedupe upstream (extractor) or surface the grain mismatch as a known issue rather than papering over it. (source: `data-modeling-process.md` macro #2; "[AE] The Order in which I Model Data" — Joshua Kim, Medium "last tip")
