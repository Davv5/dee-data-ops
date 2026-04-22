-- Weekly headline trend for Speed-to-Lead — feeds the smart-scalar trend
-- indicator on Page 1. Grain: one row per ISO week (Monday-starting) for the
-- last 12 completed weeks. The in-progress current week is excluded so the
-- trend comparator never flips on partial data.
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
    where booked_at >= timestamp_sub(
            timestamp(date_trunc(current_date(), isoweek)),
            interval 12 week
          )
      and booked_at <  timestamp(date_trunc(current_date(), isoweek))
),

weekly as (
    select
        week_start,
        countif(first_toucher_role = 'SDR')              as sdr_attributed,
        countif(is_within_5_min_sla)                     as within_5min,
        countif(had_any_sdr_activity_within_1_hr)        as with_1hr_activity,
        count(*)                                         as bookings,
        approx_quantiles(minutes_to_first_sdr_touch, 2)[offset(1)]
                                                         as median_mins
    from bookings_windowed
    group by week_start
)

select
    w.week_start,
    round(
        safe_divide(
            coalesce(wk.within_5min, 0),
            nullif(coalesce(wk.sdr_attributed, 0), 0)
        ) * 100,
        1
    )                                                    as pct_within_5min,
    round(wk.median_mins, 1)                             as median_mins,
    round(
        safe_divide(
            coalesce(wk.with_1hr_activity, 0),
            nullif(coalesce(wk.bookings, 0), 0)
        ) * 100,
        1
    )                                                    as pct_with_1hr_activity
from weeks w
left join weekly wk on wk.week_start = w.week_start
order by w.week_start
