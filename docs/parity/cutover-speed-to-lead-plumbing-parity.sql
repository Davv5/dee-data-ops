-- ---------------------------------------------------------------------------
-- U4a plumbing-parity SQL — Speed-to-Lead cutover proof.
--
-- Compares the Merge dbt chain running against the frozen U4a snapshot
-- (project-41542e21-470f-4589-96d.raw_snapshot_u4a_<YYYYMMDD>) vs the frozen
-- dee-data-ops-prod baseline (via BigQuery time-travel).
--
-- Inputs (treat as substitution variables — either edit before running OR
-- pass via `bq query --parameter`):
--   @snapshot_project   project-41542e21-470f-4589-96d
--   @snapshot_schema    raw_snapshot_u4a_<YYYYMMDD>   (ignored at this level;
--                                                      downstream models
--                                                      live in u4a_parity)
--   @parity_schema      u4a_parity                     (where dbt writes)
--   @baseline_project   dee-data-ops-prod
--   @baseline_ts        2026-04-23T22:12:26Z          (from ops/bq/.last_snapshot_ts)
--
-- Result grain: one row per checked metric. `delta` is `merge_value` minus
-- `prod_value` when numeric. `within_tolerance` is the parity verdict.
--
-- Usage:
--   bq query --use_legacy_sql=false --project_id=project-41542e21-470f-4589-96d \
--     < docs/parity/cutover-speed-to-lead-plumbing-parity.sql
--
-- If any row returns `within_tolerance = FALSE`, the cutover has NOT proven
-- out. See the PR-body write-up for the current 2026-04-23 run's diagnosis.
-- ---------------------------------------------------------------------------

with
-- ─────────────────────────── MERGE SIDE (snapshot) ───────────────────────────
merge_fct_stl as (
    select count(*) as n
    from `project-41542e21-470f-4589-96d.u4a_parity.fct_speed_to_lead_touch`
),
merge_mart_stl as (
    select count(*) as n
    from `project-41542e21-470f-4589-96d.u4a_parity.speed_to_lead_detail`
),
merge_bookings as (
    select count(*) as n
    from `project-41542e21-470f-4589-96d.u4a_parity.fct_calls_booked`
),
merge_touches as (
    select count(*) as n
    from `project-41542e21-470f-4589-96d.u4a_parity.fct_outreach`
),
merge_revenue as (
    select
        count(*)                                   as n_rows,
        coalesce(sum(gross_amount), 0)             as gross_sum,
        coalesce(sum(net_amount), 0)               as net_sum
    from `project-41542e21-470f-4589-96d.u4a_parity.fct_revenue`
),
merge_contacts as (
    select count(*) as n
    from `project-41542e21-470f-4589-96d.u4a_parity.dim_contacts`
),
merge_headline_7d as (
    -- Locked metric: % of Calendly-booked calls with a human SDR CALL/SMS
    -- touch within 5 minutes, SDR-attributed denominator. 7-day trailing
    -- window anchored at the snapshot date.
    select
        count(distinct case when is_sdr_touch then booking_id end)
            as bookings_sdr_denom,
        count(distinct case
            when is_sdr_touch and is_within_5_min_sla and is_first_touch
            then booking_id
        end) as bookings_sdr_within_5min
    from `project-41542e21-470f-4589-96d.u4a_parity.speed_to_lead_detail`
    where booked_date >= date_sub(date('2026-04-23'), interval 7 day)
),

-- ─────────────────────────── PROD SIDE (time-travel) ─────────────────────────
prod_fct_stl as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_speed_to_lead_touch`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_mart_stl as (
    select count(*) as n
    from `dee-data-ops-prod.marts.speed_to_lead_detail`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_bookings as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_calls_booked`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_touches as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.fct_outreach`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_revenue as (
    select
        count(*)                                   as n_rows,
        coalesce(sum(gross_amount), 0)             as gross_sum,
        coalesce(sum(net_amount), 0)               as net_sum
    from `dee-data-ops-prod.warehouse.fct_revenue`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_contacts as (
    select count(*) as n
    from `dee-data-ops-prod.warehouse.dim_contacts`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
),
prod_headline_7d as (
    select
        count(distinct case when is_sdr_touch then booking_id end)
            as bookings_sdr_denom,
        count(distinct case
            when is_sdr_touch and is_within_5_min_sla and is_first_touch
            then booking_id
        end) as bookings_sdr_within_5min
    from `dee-data-ops-prod.marts.speed_to_lead_detail`
        for system_time as of timestamp '2026-04-23T22:12:26Z'
    where booked_date >= date_sub(date('2026-04-23'), interval 7 day)
),

-- ─────────────────────────────── DIFFS ───────────────────────────────────────
checks as (
    -- Exact-match checks. Tolerance = 0.
    select 'fct_speed_to_lead_touch row count' as metric,
           cast((select n from merge_fct_stl)     as float64) as merge_value,
           cast((select n from prod_fct_stl)      as float64) as prod_value,
           0.0                                                as tolerance_abs,
           0.0                                                as tolerance_pct,
           'exact'                                            as tolerance_type
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
    select 'stl_headline_7d bookings_sdr_denom',
           cast((select bookings_sdr_denom from merge_headline_7d) as float64),
           cast((select bookings_sdr_denom from prod_headline_7d)  as float64),
           0.0, 0.0, 'exact'
    union all
    select 'stl_headline_7d bookings_sdr_within_5min',
           cast((select bookings_sdr_within_5min from merge_headline_7d) as float64),
           cast((select bookings_sdr_within_5min from prod_headline_7d)  as float64),
           0.0, 0.0, 'exact'
    -- Tolerance check: dim_contacts row count within ±0.1% per plan.
    union all
    select 'dim_contacts row count (±0.1%)',
           cast((select n from merge_contacts) as float64),
           cast((select n from prod_contacts)  as float64),
           0.0,
           0.001,
           'pct'
)

select
    metric,
    merge_value,
    prod_value,
    merge_value - prod_value                                   as delta,
    safe_divide(merge_value - prod_value, prod_value)          as delta_pct,
    tolerance_type,
    case
        when tolerance_type = 'exact'
            then abs(merge_value - prod_value) <= tolerance_abs
        when tolerance_type = 'pct'
            then abs(safe_divide(merge_value - prod_value, prod_value))
                 <= tolerance_pct
        else false
    end                                                        as within_tolerance
from checks
order by within_tolerance, metric;
