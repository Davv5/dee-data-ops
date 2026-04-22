-- Weekly headline trend for Speed-to-Lead — feeds the smart-scalar trend
-- indicator on Page 1. Grain: one row per ISO week (Monday-starting) for the
-- last 12 completed weeks. The in-progress current week is excluded so the
-- trend comparator never flips on partial data.
--
-- Correctness fix (2026-04-22): the previous version computed `median_mins`
-- and `pct_within_5min` over *all* bookings (SDR + non-SDR + no-touch), so
-- bookings with a NULL `minutes_to_first_sdr_touch` still sat in the denom
-- for the ratio, and early weeks saw a ~38,000-minute median because the
-- approx_quantiles pool was dominated by SDR-unattributed rows. Split the
-- funnel so the median / P90 / 5-min-SLA rate are SDR-scoped, while the
-- 1-hour reachability metric stays denominated on total bookings.
--
-- Partition-pruned on booked_at (upstream mart is partitioned on booked_at,
-- day granularity). The 12-week filter is wide enough to fit into a handful
-- of daily partitions on either side of the boundary.
{{ config(materialized='table') }}

with weeks as (
    select
        week_start
    from unnest(
        generate_date_array(
            date_trunc(date_sub(current_date(), interval 12 week), isoweek),
            date_sub(date_trunc(current_date(), isoweek), interval 1 week),
            interval 1 week
        )
    ) as week_start
),

bookings_windowed as (
    select
        date_trunc(date(booked_at), isoweek) as week_start,
        first_toucher_role,
        minutes_to_first_sdr_touch,
        is_within_5_min_sla,
        had_any_sdr_activity_within_1_hr
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp(
            date_sub(date_trunc(current_date(), isoweek), interval 12 week)
          )
      and booked_at <  timestamp(date_trunc(current_date(), isoweek))
),

weekly_all as (
    select
        week_start,
        count(*)                                         as bookings,
        countif(first_toucher_role = 'SDR')              as sdr_attributed,
        countif(first_toucher_role = 'SDR' and is_within_5_min_sla)
                                                         as within_5min,
        countif(had_any_sdr_activity_within_1_hr)        as with_1hr_activity
    from bookings_windowed
    group by week_start
),

-- Quantile pool restricted to SDR-attributed bookings so no-touch rows can't
-- pollute the median / P90 with NULLs or astronomically large diffs.
weekly_sdr_only as (
    select
        week_start,
        approx_quantiles(minutes_to_first_sdr_touch, 2)[offset(1)]
                                                         as median_mins_sdr_only,
        approx_quantiles(minutes_to_first_sdr_touch, 10)[offset(9)]
                                                         as p90_mins_sdr_only
    from bookings_windowed
    where first_toucher_role = 'SDR'
    group by week_start
)

select
    w.week_start,
    coalesce(a.bookings, 0)                              as bookings,
    coalesce(a.sdr_attributed, 0)                        as sdr_attributed,
    coalesce(a.within_5min, 0)                           as within_5min,
    round(
        safe_divide(
            coalesce(a.within_5min, 0),
            nullif(coalesce(a.sdr_attributed, 0), 0)
        ) * 100,
        1
    )                                                    as pct_within_5min,
    round(s.median_mins_sdr_only, 1)                     as median_mins_sdr_only,
    round(s.p90_mins_sdr_only, 1)                        as p90_mins_sdr_only,
    round(
        safe_divide(
            coalesce(a.with_1hr_activity, 0),
            nullif(coalesce(a.bookings, 0), 0)
        ) * 100,
        1
    )                                                    as pct_with_1hr_activity
from weeks w
left join weekly_all     a on a.week_start = w.week_start
left join weekly_sdr_only s on s.week_start = w.week_start
order by w.week_start
