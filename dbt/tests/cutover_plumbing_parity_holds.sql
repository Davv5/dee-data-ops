-- U4a HARD GATE: cutover plumbing-parity test.
--
-- Fails if any row returns with `within_tolerance = false`.
--
-- What it does: compares the Merge dbt chain running against the frozen U4a
-- snapshot (in the `u4a_parity` schema) vs a time-travel snapshot of
-- `dee-data-ops-prod` at the moment the snapshot was captured. See
-- `docs/parity/cutover-speed-to-lead-plumbing-parity.sql` for the
-- human-readable version of the same query.
--
-- Retire this test once U4a signs off, the cutover (U5) lands, and the
-- `u4a_parity` schema + `raw_snapshot_u4a_<date>` dataset are deleted.
--
-- Preconditions to run this test:
--   1. `ops/bq/snapshot_gtm_raw.sh` has been executed within the last 7 days
--      (BigQuery time-travel window on dee-data-ops-prod).
--   2. `dbt build --target plumbing_parity --vars '{raw_schema_override:
--       raw_snapshot_u4a_<date>}'` has been run and materialized the Merge
--       side into the `u4a_parity` dataset.
--   3. `parity_baseline_ts` var matches the `PARITY_BASELINE_TS` line from
--      `ops/bq/.last_snapshot_ts`.
--
-- Usage (from the dbt/ directory):
--   dbt test --select cutover_plumbing_parity_holds \
--     --target plumbing_parity \
--     --vars '{raw_schema_override: raw_snapshot_u4a_20260423,
--              parity_baseline_ts: "2026-04-23T22:12:26Z"}'

{% set baseline_ts = var('parity_baseline_ts', '2026-04-23T22:12:26Z') %}

with
merge_fct_stl as (
    select count(*) as n from {{ ref('fct_speed_to_lead_touch') }}
),
merge_mart_stl as (
    select count(*) as n from {{ ref('speed_to_lead_detail') }}
),
merge_bookings as (
    select count(*) as n from {{ ref('fct_calls_booked') }}
),
merge_touches as (
    select count(*) as n from {{ ref('fct_outreach') }}
),
merge_revenue as (
    select
        count(*)                                  as n_rows,
        coalesce(sum(gross_amount), 0)            as gross_sum,
        coalesce(sum(net_amount), 0)              as net_sum
    from {{ ref('fct_revenue') }}
),
merge_contacts as (
    select count(*) as n from {{ ref('dim_contacts') }}
),

prod_fct_stl as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_speed_to_lead_touch`
        for system_time as of timestamp '{{ baseline_ts }}'
),
prod_mart_stl as (
    select count(*) as n
    from `dee-data-ops-prod.marts.speed_to_lead_detail`
        for system_time as of timestamp '{{ baseline_ts }}'
),
prod_bookings as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_calls_booked`
        for system_time as of timestamp '{{ baseline_ts }}'
),
prod_touches as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_outreach`
        for system_time as of timestamp '{{ baseline_ts }}'
),
prod_revenue as (
    select
        count(*)                                  as n_rows,
        coalesce(sum(gross_amount), 0)            as gross_sum,
        coalesce(sum(net_amount), 0)              as net_sum
    from `dee-data-ops-prod.warehouse.fct_revenue`
        for system_time as of timestamp '{{ baseline_ts }}'
),
prod_contacts as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.dim_contacts`
        for system_time as of timestamp '{{ baseline_ts }}'
),

checks as (
    select 'fct_speed_to_lead_touch row count' as metric,
           cast((select n from merge_fct_stl) as float64)   as merge_value,
           cast((select n from prod_fct_stl)  as float64)   as prod_value,
           0.0 as tolerance_abs, 0.0 as tolerance_pct, 'exact' as tolerance_type
    union all
    select 'speed_to_lead_detail row count',
           cast((select n from merge_mart_stl) as float64),
           cast((select n from prod_mart_stl)  as float64),
           0.0, 0.0, 'exact'
    union all
    select 'fct_calls_booked row count',
           cast((select n from merge_bookings) as float64),
           cast((select n from prod_bookings)  as float64),
           0.0, 0.0, 'exact'
    union all
    select 'fct_outreach row count',
           cast((select n from merge_touches) as float64),
           cast((select n from prod_touches)  as float64),
           0.0, 0.0, 'exact'
    union all
    select 'fct_revenue row count',
           cast((select n_rows from merge_revenue) as float64),
           cast((select n_rows from prod_revenue)  as float64),
           0.0, 0.0, 'exact'
    union all
    select 'fct_revenue gross_amount sum',
           (select gross_sum from merge_revenue),
           (select gross_sum from prod_revenue),
           0.0, 0.0, 'exact'
    union all
    select 'fct_revenue net_amount sum',
           (select net_sum from merge_revenue),
           (select net_sum from prod_revenue),
           0.0, 0.0, 'exact'
    union all
    select 'dim_contacts row count (±0.1%)',
           cast((select n from merge_contacts) as float64),
           cast((select n from prod_contacts)  as float64),
           0.0, 0.001, 'pct'
)

-- dbt tests fail by returning rows. Emit one row per parity violation with
-- the metric name, merge value, prod value, and the delta — so the failure
-- message itself tells the operator which gate is red.
select
    metric,
    merge_value,
    prod_value,
    merge_value - prod_value                          as delta,
    safe_divide(merge_value - prod_value, prod_value) as delta_pct
from checks
where
    (tolerance_type = 'exact' and abs(merge_value - prod_value) > tolerance_abs)
    or (tolerance_type = 'pct'
        and abs(safe_divide(merge_value - prod_value, prod_value))
            > tolerance_pct)
