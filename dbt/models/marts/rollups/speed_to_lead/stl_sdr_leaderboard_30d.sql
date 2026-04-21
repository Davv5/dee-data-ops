-- 30-day SDR leaderboard — bookings claimed + SLA hit rate + median time.
-- Backs the primary results table on Page 1. Grain: one row per SDR.
{{ config(materialized='table') }}

select
    coalesce(first_toucher_name, '(unassigned)') as sdr_name,
    count(*) as bookings,
    countif(is_within_5_min_sla) as within_5min,
    countif(had_any_sdr_activity_within_1_hr) as with_1hr_activity,
    round(
        safe_divide(
            countif(is_within_5_min_sla),
            nullif(count(*), 0)
        ) * 100,
        1
    ) as pct_within_5min,
    round(
        approx_quantiles(minutes_to_first_sdr_touch, 2)[offset(1)],
        1
    ) as median_mins,
    round(
        avg(minutes_to_first_sdr_touch),
        1
    ) as mean_mins,
    countif(close_outcome = 'won') as closed_won,
    round(
        safe_divide(
            countif(close_outcome = 'won'),
            nullif(count(*), 0)
        ) * 100,
        1
    ) as pct_closed_won
from {{ ref('sales_activity_detail') }}
where first_toucher_role = 'SDR'
  and booked_at >= timestamp_sub(current_timestamp(), interval 30 day)
group by sdr_name
order by bookings desc
