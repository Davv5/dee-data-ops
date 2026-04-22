-- Outcome overlay on lead_source — complements the volume-only
-- `stl_daily_volume_by_source` rollup with speed + show + close rates.
-- Grain: one row per lead_source. Last 30 days.
--
-- Show-rate derivation: mart has no show_outcome column. We fall back to
-- "close_outcome IS NOT NULL implies showed". See `stl_outcome_by_touch_bucket_30d`
-- for the same assumption and caveat.
{{ config(materialized='table') }}

select
    coalesce(lead_source, 'unknown') as lead_source,
    count(*) as bookings,
    round(
        safe_divide(
            countif(first_toucher_role = 'SDR' and is_within_5_min_sla),
            nullif(count(*), 0)
        ) * 100,
        1
    ) as pct_within_5min,
    round(
        safe_divide(
            countif(close_outcome is not null),
            nullif(count(*), 0)
        ) * 100,
        1
    ) as show_rate_pct,
    round(
        safe_divide(
            countif(close_outcome = 'won'),
            nullif(count(*), 0)
        ) * 100,
        1
    ) as close_rate_pct
from {{ ref('sales_activity_detail') }}
where booked_at >= timestamp(date_sub(current_date(), interval 30 day))
group by lead_source
order by bookings desc
