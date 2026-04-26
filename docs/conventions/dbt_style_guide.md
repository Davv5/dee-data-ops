# dbt Style Guide

## Model Naming

Our models (typically) fit into three main categories: staging, warehouse, marts. For more detail about aspects of this structure, check out [the dbt best practices](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview). 

The file and naming structures are as follows (example):

```
ssp_analytics
├── .github
│   ├── workflows
│   │   ├── ci.yml
│   │   ├── daily_refresh.yml
│   │   └── post_merge_deploy.yml
│   └── pull_request_template.md
├── _project_docs
│   ├── automation
│   │   │   └── profiles.yml
│   └── style_guide.md
├── analyses
├── seeds
│   └── some_data.csv
├── snapshots
├── tests
│   └── assert_some_test_scenario.sql
├── macros
│   ├── _macros__definitions.yml
│   ├── _macros__docs.md
│   └── generate_schema_name.sql
├── models
│   ├── marts
│   │   ├── _marts__docs.md
│   │   ├── _marts__models.yml
│   │   └── nba_games_detail.sql
│   ├── staging
│   │   ├── nba
│   │   │   ├── _nba__docs.md
│   │   │   ├── _nba__models.yml
│   │   │   ├── _nba__sources.yml
│   │   │   ├── stg_nba__games.sql
│   │   │   └── stg_nba__teams.sql
│   │   └── gsheets
│   │       ├── _gsheets__models.yml
│   │       ├── _gsheets__sources.yml
│   │       ├── stg_gsheets__franchise_actives.yml
│   │       ├── stg_gsheets__franchise_general_managers.yml
│   │       └── stg_gsheets__franchise_head_coaches.sql
│   ├── warehouse
│   │   ├── dimensions
│   │   │   ├── _dimensions__docs.md
│   │   │   ├── _dimensions__models.yml
│   │   │   ├── dim_calendar_dates_.sql
│   │   │   ├── dim_games.sql
│   │   │   └── dim_teams.sql
│   │   └── facts
│   │       ├── _facts__docs.yml
│   │       ├── _facts__models.yml
│   │       └── fct_games_played.sql
├── README.md
├── dbt_project.yml
├── packages.yml
└── requirements.txt
```

- All objects should be plural, such as: `stg_nba__teams`
- Staging models are 1:1 with each source table and named with the following convention: `stg_<source>__<table_name>.sql`
  - [Additional context on Staging models](https://docs.getdbt.com/guides/best-practices/how-we-structure/2-staging)
- Marts contain all of the useful data about a *particular entity* at a granular level and should lean towards being wide and denormalized.
  - [Additional context on Marts models](https://docs.getdbt.com/guides/best-practices/how-we-structure/4-marts)
- Intermediate tables (if needed) should help break apart complex or lengthy logic and follow the following convention: `int_[entity]s_[verb]s.sql`
  - [Additional context on Intermediate models](https://docs.getdbt.com/guides/best-practices/how-we-structure/3-intermediate)

## Model configuration

- Model-specific attributes (like sort/dist keys) should be specified in the model.
- If a particular configuration applies to all models in a directory, it should be specified in the `dbt_project.yml` file.
- In-model configurations should be specified like this:

```python
{{
  config(
    materialized = 'table',
    sort = 'id',
    dist = 'id'
  )
}}
```

- Marts should always be configured as tables

## dbt conventions

- Only `stg_` models (or `base_` models if your project requires them) should select from `source`s.
- All other models should only select from other models.

## Testing

- Every subdirectory should contain a `.yml` file, in which each model in the subdirectory is tested. For staging folders, there will be both `_sourcename__sources.yml` as well as `_sourcename__models.yml`. For other folders, the structure should be `_foldername__models.yml` (example `_finance__models.yml`).
- At a minimum, unique and not_null tests should be applied to the primary key of each model.

## Naming and field conventions

- Schema, table and column names should be in `snake_case`.
- Use names based on the *business* terminology, rather than the source terminology.
- Each model should have a primary key.
- The primary key of a model should be named `<object>_id`, e.g. `account_id` – this makes it easier to know what `id` is being referenced in downstream joined models.
- For base/staging models, fields should be ordered in categories, where identifiers are first and timestamps are at the end.
- Timestamp columns should be named `<event>_at`, e.g. `created_at`, and should be in UTC. If a different timezone is being used, this should be indicated with a suffix, e.g `created_at_pt`.
- Booleans should be prefixed with `is`_ or `has_`.
- Price/revenue fields should be in decimal currency (e.g. `19.99` for $19.99; many app databases store prices as integers in cents). If non-decimal currency is used, indicate this with suffix, e.g. `price_in_cents`.
- Avoid reserved words as column names
- Consistency is key! Use the same field names across models where possible, e.g. a key to the `customers` table should be named `customer_id` rather than `user_id`.

## CTEs

For more information about why we use so many CTEs, check out [this discourse post](https://discourse.getdbt.com/t/why-the-fishtown-sql-style-guide-uses-so-many-ctes/1091).

- All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do
- CTEs with confusing or noteable logic should be commented
- CTEs that are duplicated across models should be pulled out into their own models
- create a `final` or similar CTE that you select from as your last line of code. This makes it easier to debug code within a model (without having to comment out code!)
- CTEs should be formatted like this:

```sql
with

events as (

    ...

),

-- CTE comments go here
filtered_events as (

    ...

)

select * from filtered_events
```

## SQL style guide

- Use trailing commas
- Indents should be four spaces (except for predicates, which should line up with the `where` keyword)
- Lines of SQL should be no longer than [80 characters](https://stackoverflow.com/questions/29968499/vertical-rulers-in-visual-studio-code)
- Field names and function names should all be lowercase
- The `as` keyword should be used when aliasing a field or table
- Fields should be stated before aggregates / window functions
- Aggregations should be executed as early as possible before joining to another table.
- Ordering and grouping by a number (eg. group by 1, 2) is preferred over listing the column names (see [this rant](https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/) for why). Note that if you are grouping by more than a few columns, it may be worth revisiting your model design.
- Prefer `union all` to `union` [*](http://docs.aws.amazon.com/redshift/latest/dg/c_example_unionall_query.html)
- Avoid table aliases in join conditions (especially initialisms) – it's harder to understand what the table called "c" is compared to "customers".
- If joining two or more tables, *always* prefix your column names with the table alias. If only selecting from one table, prefixes are not needed.
- Be explicit about your join (i.e. write `inner join` instead of `join`). `left joins` are normally the most useful, `right joins` often indicate that you should change which table you select `from` and which one you `join` to.
- *DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEWLINES ARE CHEAP, BRAIN TIME IS EXPENSIVE*

### Example SQL

```sql
with

my_data as (

    select * from {{ ref('my_data') }}

),

some_cte as (

    select * from {{ ref('some_cte') }}

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5

    from some_cte
    group by 1

),

final as (

    select [distinct]
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from my_data
    left join some_cte_agg  
        on my_data.id = some_cte_agg.id
    where my_data.field_1 = 'abc'
        and (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )
    having count(*) > 1

)

select * from final

```

- Your join should list the "left" table first (i.e. the table you are selecting `from`):

```sql
select
    trips.*,
    drivers.rating as driver_rating,
    riders.rating as rider_rating

from trips
left join users as drivers
    on trips.driver_id = drivers.user_id
left join users as riders
    on trips.rider_id = riders.user_id

```

## SQL writing discipline (Kim's process maxims)

These maxims complement the SQL style rules above with the *order of decisions* and *anti-patterns to watch for* when writing a model. Sourced from [Joshua Kim, "[AE] The Order in which I Model Data" — Medium, April 2026](https://joshua-data.medium.com/my-analytics-engineering-process-en-435445038897). The full process arc lives in `.claude/rules/data-modeling-process.md` (auto-loads on `2-dbt/models/**`).

### Decide output type and partition first

Before writing the SELECT, name:

- The materialization (view / table / incremental / ephemeral) — usually inherited from the directory default in `dbt_project.yml`, but if you override, document why
- The partition column (BigQuery `partition_by` config) for any table > 1M rows or with a date dimension
- The unique key (for incremental models)

If you can't name these in one sentence each, you don't yet know what the model is. Stop and revisit grain.

### Preprocessing belongs in the FIRST CTE

The first CTE (typically `source`) is the home for all defensive cleanup: `TRIM`, `LOWER`, `NULLIF('', value)`, `REGEXP_REPLACE`, `COALESCE`, `CAST`. Get the data into a sane shape immediately so every downstream CTE can assume clean inputs.

```sql
with

source as (
    select
        nullif(trim(lower(email)), '') as email,
        coalesce(country, 'unknown') as country,
        cast(amount as numeric) as amount,
        cast(created_at as timestamp) as created_at
    from {{ source('raw_x', 'orders') }}
),

-- downstream CTEs assume clean inputs from here on
…
```

### `CAST` in SELECT, `SAFE_CAST` only in WHERE

Use `CAST` (loud failure) in SELECT clauses; reserve `SAFE_CAST` (silent NULL) for WHERE / filter conditions. A bad row should surface immediately and force a fix upstream — not propagate as NULL into the marts.

```sql
-- BAD: silent NULL propagation into downstream models
select safe_cast(amount as numeric) as amount from source

-- GOOD: loud failure, fix the data upstream
select cast(amount as numeric) as amount from source

-- ALSO GOOD: safe_cast in a filter (a NULL just means "row doesn't match")
select * from source where safe_cast(event_ts as timestamp) >= '2026-01-01'
```

(Cross-references: `.claude/rules/staging.md` "What goes in a staging view" / "CAST vs SAFE_CAST".)

### Unify column naming at the top CTE

Every source emits the same logical concept under different column names (`user_id`, `user`, `user_idx`). Collapse them in the first CTE so every downstream CTE refers to one canonical name. The further down the chain, the cleaner the SQL.

```sql
with

source as (
    select
        user_idx as user_id,        -- source A's name
        product_id,
        purchase_amount as amount   -- source A's name; matches the canonical name
    from {{ source('raw_a', 'transactions') }}
),
…
```

### Variableize constants and business rules

Magic strings and dates that encode business rules belong in `set` statements at the top of the file or as Jinja variables in `dbt_project.yml`. Don't sprinkle `'2025-01-01'` or `'qualified_lead'` across multiple CTEs.

```sql
{% set go_live_date = '2025-01-01' %}
{% set qualified_status_codes = ['confirmed', 'shipped', 'delivered'] %}

with

source as (
    select * from {{ ref('stg_orders') }}
    where status in {{ qualified_status_codes | join("','") | wrap_in_quotes }}
      and order_date >= '{{ go_live_date }}'
),
…
```

### `FULL OUTER UNION ALL BY NAME` for column-mismatched UNIONs (BigQuery)

When two sources contribute to the same UNION but have non-overlapping columns, use BigQuery's `FULL OUTER UNION ALL BY NAME` — columns line up by name, missing columns become NULL automatically. Far more readable than `cast(null as string) as country` per side.

```sql
-- old way: tedious, error-prone
select id, name, country from source_a
union all
select id, name, cast(null as string) as country from source_b

-- BigQuery: cleaner, harder to misalign
(select id, name, country from source_a)
full outer union all by name
(select id, name from source_b)
```

> Verify the UNION result column order + types match what downstream expects; the implicit NULL fill makes mistakes silent.

### WHERE conditions disappear top-to-bottom

In a well-designed CTE chain, `WHERE` filters cluster at the top — usually in the source CTE or the first transform. Each subsequent CTE narrows by aggregation, projection, or join — but if you find yourself adding a `WHERE` in CTE #4, that's a signal you missed a filter at the top OR you joined too early and are filtering after the fan-out.

A `WHERE` in a lower CTE that filters out rows the join just produced is the common bug: the join shouldn't have produced those rows in the first place.

### Verify row count after every join

`COUNT(1)` before and after each join while developing. If row count changes unexpectedly, the join key isn't unique on the right-hand side and you have a fan-out. Catching this once saves an evening of head-scratching when the headline metric drifts.

```sql
-- during development, run as ad-hoc queries:
select count(1) from {{ ref('fct_orders') }};                                   -- 100k
select count(1) from {{ ref('fct_orders') }} f
  left join {{ ref('dim_product') }} d on f.product_id = d.product_id;          -- 250k → fan-out!
```

The fix is upstream (verify dim PK uniqueness; collapse SCD Type 2 history before the join — see `warehouse.md` "Fact × Dim join — verify uniqueness before joining").

### `QUALIFY ROW_NUMBER() = 1` is a symptom

Every time you reach for `QUALIFY ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) = 1` to dedupe, ask: why isn't `x` the PK upstream? If the upstream's grain is correct, you don't need this. Once is fine; twice is a pattern; three times is debt that compounds — every change to upstream now has to consider the dedupe behavior in N downstream models.

(Cross-references: `.claude/rules/data-modeling-process.md` macro #2; `.claude/rules/staging.md` lessons-learned.)

## YAML style guide

- Indents should be two spaces
- List items should be indented
- Use a new line to separate list items that are dictionaries where appropriate
- Lines of YAML should be no longer than 80 characters.

### Example YAML

```yaml
version: 2

models:
  - name: events
    columns:
      - name: event_id
        description: This is a unique identifier for the event
        tests:
          - unique
          - not_null

      - name: event_time
        description: "When the event occurred in UTC (eg. 2018-01-01 12:00:00)"
        tests:
          - not_null

      - name: user_id
        description: The ID of the user who recorded the event
        tests:
          - not_null
          - relationships:
              to: ref('users')
              field: id
```

## Jinja style guide

- When using Jinja delimiters, use spaces on the inside of your delimiter, like `{{ this }}` instead of `{{this}}`
- Use newlines to visually indicate logical blocks of Jinja

## Helpful Reference Links

- [https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)
- [https://discourse.getdbt.com/t/why-the-fishtown-sql-style-guide-uses-so-many-ctes/1091](https://discourse.getdbt.com/t/why-the-fishtown-sql-style-guide-uses-so-many-ctes/1091)
- [https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/](https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/)
- [https://docs.getdbt.com/docs/about/viewpoint](https://docs.getdbt.com/docs/about/viewpoint)
- [https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)

