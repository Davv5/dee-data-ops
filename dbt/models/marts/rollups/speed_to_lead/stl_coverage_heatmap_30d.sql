-- Day-of-week × hour-of-day coverage heatmap — surfaces SDR coverage gaps.
-- Grain: one row per (day_of_week, hour_of_day) combination that has ≥1 booking
-- in the last 30 days. Empty (day, hour) cells are skipped; the BI heatmap
-- handles visual gaps.
--
-- BigQuery's EXTRACT(DAYOFWEEK ...) returns Sun=1..Sat=7. Remap to Mon=1..Sun=7
-- so the heatmap reads Monday→Sunday top-to-bottom.
{{ config(materialized='table') }}

with base as (
    select
        booked_at,
        first_toucher_role,
        is_within_5_min_sla,
        -- Remap: Sun=1→7, Mon=2→1, Tue=3→2, Wed=4→3, Thu=5→4, Fri=6→5, Sat=7→6
        case extract(dayofweek from booked_at)
            when 1 then 7  -- Sun
            when 2 then 1  -- Mon
            when 3 then 2  -- Tue
            when 4 then 3  -- Wed
            when 5 then 4  -- Thu
            when 6 then 5  -- Fri
            when 7 then 6  -- Sat
        end as day_sort,
        case extract(dayofweek from booked_at)
            when 1 then 'Sun'
            when 2 then 'Mon'
            when 3 then 'Tue'
            when 4 then 'Wed'
            when 5 then 'Thu'
            when 6 then 'Fri'
            when 7 then 'Sat'
        end as day_of_week,
        extract(hour from booked_at) as hour_of_day
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp(date_sub(current_date(), interval 30 day))
)

select
    day_of_week,
    day_sort,
    hour_of_day,
    count(*) as bookings,
    case
        when count(*) = 0 then null
        else round(
            safe_divide(
                countif(first_toucher_role = 'SDR' and is_within_5_min_sla),
                nullif(count(*), 0)
            ) * 100,
            1
        )
    end as pct_within_5min
from base
group by day_of_week, day_sort, hour_of_day
order by day_sort, hour_of_day
