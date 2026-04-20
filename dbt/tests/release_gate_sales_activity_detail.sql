-- Release gate: fail the build if sales_activity_detail row count deviates
-- more than ±5% from the oracle 'Calls Booked' snapshot (3,141 as of 2026-03-19).
-- Tolerance is deliberately tight — see `client_v1_scope_speed_to_lead.md` for the
-- reasoning behind treating Calendly as the system-of-record for the booking grain.

with

mart as (
    select count(*) as n from {{ ref('sales_activity_detail') }}
),

oracle as (
    select cast(value as int64) as n
    from {{ ref('oracle_dashboard_metrics_20260319') }}
    where metric = 'Calls Booked'
),

delta as (
    select
        mart.n                                              as mart_count,
        oracle.n                                            as oracle_count,
        abs(mart.n - oracle.n) / oracle.n                   as pct_delta
    from mart, oracle
)

select *
from delta
where pct_delta > 0.05
