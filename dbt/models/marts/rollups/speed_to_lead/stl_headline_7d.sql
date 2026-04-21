-- Headline 7-day scorecards for Speed-to-Lead Page 1.
-- One row; each scorecard in Looker picks its column.
{{ config(materialized='table') }}

select
    count(*) as bookings_7d,
    countif(first_toucher_role = 'SDR') as sdr_attributed_7d,
    countif(is_within_5_min_sla) as within_5min_7d,
    round(
        safe_divide(
            countif(is_within_5_min_sla),
            nullif(countif(first_toucher_role = 'SDR'), 0)
        ) * 100,
        1
    ) as pct_within_5min_7d,
    round(
        approx_quantiles(minutes_to_first_sdr_touch, 2)[offset(1)],
        1
    ) as median_mins_7d,
    round(
        safe_divide(
            countif(had_any_sdr_activity_within_1_hr),
            nullif(countif(first_toucher_role = 'SDR'), 0)
        ) * 100,
        1
    ) as pct_with_1hr_activity_7d,
    current_timestamp() as computed_at
from {{ ref('sales_activity_detail') }}
where booked_at >= timestamp_sub(current_timestamp(), interval 7 day)
