-- 30-day daily trend for the scorecard sparklines on Page 1.
-- Shape: one row per day. Looker scorecards with sparkline=date + metric col
-- will render current value + 30-day trend line.
{{ config(materialized='table') }}

with days as (
    select
        date_day
    from unnest(
        generate_date_array(
            date_sub(current_date(), interval 29 day),
            current_date()
        )
    ) as date_day
),
daily as (
    select
        date(booked_at) as booking_date,
        count(*) as bookings,
        countif(first_toucher_role = 'SDR') as sdr_attributed,
        countif(is_within_5_min_sla) as within_5min,
        countif(had_any_sdr_activity_within_1_hr) as with_1hr_activity
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp_sub(current_timestamp(), interval 30 day)
    group by booking_date
)

select
    d.date_day as booking_date,
    coalesce(daily.bookings, 0) as bookings,
    coalesce(daily.sdr_attributed, 0) as sdr_attributed,
    coalesce(daily.within_5min, 0) as within_5min,
    coalesce(daily.with_1hr_activity, 0) as with_1hr_activity,
    round(
        safe_divide(
            coalesce(daily.within_5min, 0),
            nullif(coalesce(daily.sdr_attributed, 0), 0)
        ) * 100,
        1
    ) as pct_within_5min
from days d
left join daily on daily.booking_date = d.date_day
order by d.date_day
