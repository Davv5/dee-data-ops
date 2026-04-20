-- Release-gate test for revenue_detail.
-- Three assertions, any one failure returns rows and fails the test:
--   1. total revenue within 5% of the 2026-03-19 oracle dashboard
--   2. payment-row count within 5% of the oracle total (1423)
--   3. unmatched revenue share stays under 10% — hard stop on silent
--      attribution decay.
-- Parity gaps explained by the known Fanbasis zero-row state should be
-- handled by widening tolerance in the PR, not by filtering rows.

with

mart as (

    select
        sum(net_amount)                                     as mart_revenue,
        count(*)                                            as mart_count,
        sum(
            case
                when match_status = 'unmatched' then net_amount
                else 0
            end
        )                                                   as unmatched_revenue
    from {{ ref('revenue_detail') }}

),

oracle as (

    select
        cast(
            replace(replace(value, '$', ''), ',', '')
            as numeric
        )                                                   as oracle_revenue
    from {{ ref('oracle_dashboard_metrics_20260319') }}
    where metric = 'Total Revenue (USD)'

),

delta as (

    select
        mart.mart_revenue,
        oracle.oracle_revenue,
        abs(mart.mart_revenue - oracle.oracle_revenue)
        / oracle.oracle_revenue                             as revenue_pct_delta,
        mart.mart_count,
        mart.unmatched_revenue / mart.mart_revenue          as unmatched_share
    from mart
    cross join oracle

)

select *
from delta
where revenue_pct_delta > 0.05
    or mart_count < 1350
    or mart_count > 1494
    or unmatched_share > 0.10
