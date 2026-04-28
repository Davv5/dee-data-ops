-- Asserts the refunds aggregation in `revenue_detail` matches the
-- underlying `fct_refunds` total per `source_platform`. Catches:
--   (a) refund-side filter regression in revenue_detail's aggregation CTE
--       (e.g. a stray WHERE that drops a row),
--   (b) join-key drift if either side renames `parent_payment_id` /
--       `payment_id` / `source_platform`,
--   (c) accidental fan-out if the pre-aggregation is removed and the
--       refunds CTE joins payments at refund-grain instead of payment-grain.
--
-- $0.01 tolerance per source_platform is the float64-money floor — see
-- the float64 tech-debt note in `.claude/state/project-state.md`.
-- Returns one row per offending source_platform; test passes when zero
-- rows return.

with

mart_refunds as (

    select
        source_platform,
        sum(refunds_total_amount)                       as mart_total
    from {{ ref('revenue_detail') }}
    group by 1

),

fact_refunds as (

    select
        source_platform,
        sum(refund_amount)                              as fact_total
    from {{ ref('fct_refunds') }}
    group by 1

),

mismatches as (

    select
        coalesce(m.source_platform, f.source_platform) as source_platform,
        coalesce(m.mart_total, 0)                      as mart_total,
        coalesce(f.fact_total, 0)                      as fact_total,
        abs(coalesce(m.mart_total, 0) - coalesce(f.fact_total, 0))
                                                       as abs_diff
    from mart_refunds m
    full outer join fact_refunds f
        on m.source_platform = f.source_platform

)

select *
from mismatches
where abs_diff > 0.01
