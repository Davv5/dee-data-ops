-- Asserts the refunds aggregation in `revenue_detail` matches the
-- underlying `fct_refunds` total per `source_platform`. Catches:
--   (a) refund-side filter regression in revenue_detail's aggregation CTE
--       (e.g. a stray WHERE that drops a row),
--   (b) join-key drift if either side renames `parent_payment_id` /
--       `payment_id` / `source_platform`,
--   (c) accidental fan-out if the pre-aggregation is removed and the
--       refunds CTE joins payments at refund-grain instead of payment-grain.
--
-- Money columns through fct_refunds and revenue_detail are NUMERIC end-to-end
-- (Fanbasis-only refund path). Tolerance kept at $0.01 as a precision floor
-- in case downstream summation introduces rounding via mixed-type arithmetic;
-- with both sides NUMERIC this is effectively exact.
-- Returns one row per offending source_platform; test passes when zero
-- rows return.

with

mart_refunds as (

    select
        source_platform,
        sum(refunds_total_amount)                       as mart_total
    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_detail`
    group by 1

),

fact_refunds as (

    select
        source_platform,
        sum(refund_amount)                              as fact_total
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_refunds`
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