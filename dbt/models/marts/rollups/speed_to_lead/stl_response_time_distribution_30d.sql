-- Cumulative response-time distribution — replaces the "two arbitrary thresholds"
-- pattern with a full curve (1 min, 5 min, 15 min, 1 hr, 4 hr, 24 hr).
-- Grain: one row per threshold (6 rows). SDR-attributed bookings only, last 30 days.
{{ config(materialized='table') }}

with base as (
    select minutes_to_first_sdr_touch
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp(date_sub(current_date(), interval 30 day))
      and first_toucher_role = 'SDR'
),
totals as (
    select count(*) as bookings_total from base
),
thresholds as (
    select '≤1 min'  as threshold_label, 1 as threshold_sort, 1     as threshold_minutes union all
    select '≤5 min'  as threshold_label, 2 as threshold_sort, 5     as threshold_minutes union all
    select '≤15 min' as threshold_label, 3 as threshold_sort, 15    as threshold_minutes union all
    select '≤1 hr'   as threshold_label, 4 as threshold_sort, 60    as threshold_minutes union all
    select '≤4 hr'   as threshold_label, 5 as threshold_sort, 240   as threshold_minutes union all
    select '≤24 hr'  as threshold_label, 6 as threshold_sort, 1440  as threshold_minutes
)

select
    t.threshold_label,
    t.threshold_sort,
    round(
        safe_divide(
            countif(base.minutes_to_first_sdr_touch <= t.threshold_minutes),
            nullif(totals.bookings_total, 0)
        ) * 100,
        1
    ) as pct_within,
    countif(base.minutes_to_first_sdr_touch <= t.threshold_minutes) as bookings_within,
    totals.bookings_total as bookings_total
from thresholds t
cross join base
cross join totals
group by t.threshold_label, t.threshold_sort, totals.bookings_total
order by t.threshold_sort
