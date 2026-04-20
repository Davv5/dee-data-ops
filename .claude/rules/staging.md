---
paths: ["dbt/models/staging/**"]
---

# Staging conventions

Load this rule when working on any file under `dbt/models/staging/`. Detailed style reference: `dbt_style_guide.md`.

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
- Casting types (`safe_cast`, `cast`)
- Rounding decimals, currency conversions where needed
- JSON flattening (`json_value`, `json_extract_array`, `unnest`)
- Simple `case when` transforms
- Filtering out test/bad rows (filtered in the `where` clause of the `final` CTE)

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
# dbt/models/staging/ghl/_ghl__models.yml
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

- When JSON flattening GHL contacts, always `safe_cast` timestamps — the source returns both epoch-ms and ISO-8601 strings depending on call. Without `safe_cast` one form silently fails.
- Calendly `invitee_email` can be null; do not rely on it as a non-null PK source. Use `invitee_uuid`.
- **No `join` keyword in any file under `dbt/models/staging/**`.** Sub-selects and `unnest` are fine; any row-combining logic must live in `warehouse/` or `marts/`. Enforced by the `no-joins-in-staging` pre-commit hook.
- **No hard-coded `entity_type` filters that assume a legacy single-table GHL raw layout.** Per-entity raw tables (`raw_ghl.ghl__<entity>_raw`) do not carry an `entity_type` column — any `WHERE entity_type = '...'` filter will silently return zero rows.
