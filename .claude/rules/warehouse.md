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

- **Lowest granularity you can justify.** Transactions > order line items > orders. You can roll up in a mart; you can't roll down.
- **Contents: surrogate key + FK surrogate keys + numeric aggregables + event-grain timestamps.**
- **No descriptive text in facts.** Names, addresses, labels live in dims.
- **Dim lookups grab the SK only.** Join dim to get `<entity>_sk`, not to grab descriptive columns. Those come from joining the dim at query time (or at the mart layer).

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

## Identity spine (the load-bearing dim)

`dim_contact` is the golden-contact dimension — the canonical entity every downstream join references. Build rules:

- **Anchor on GHL** (`stg_ghl__contacts`) — v1 uses GHL as the **single** anchor for contact identity. No multi-source bridges in v1.
- Canonical join keys reserved for future cross-source enrichment: `email_norm`, `email_canon` (gmail dot/plus normalized), `phone_last10`
- Surrogate key: `generate_surrogate_key(['location_id', 'contact_id'])`
- SCD Type 2 on attribute changes (assigned user, email, pipeline state)

When v1+N adds non-GHL sources, bridges resolve to the GHL `contact_id` upstream of `dim_contact` (in staging or an intermediate model) — never by widening `dim_contact` with multi-source key fan-out.

## Lessons learned

*(Populate as warehouse-layer issues arise.)*
