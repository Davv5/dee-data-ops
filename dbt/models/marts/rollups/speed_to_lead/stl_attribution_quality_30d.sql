-- Attribution-quality mix — backs the donut / bar on Page 1.
-- Transparency signal: how much of the volume is clean vs DQ-flagged.
{{ config(materialized='table') }}

select
    attribution_quality_flag as flag,
    count(*) as bookings,
    round(
        count(*) * 100.0
        / sum(count(*)) over (),
        1
    ) as pct_of_total
from {{ ref('sales_activity_detail') }}
where booked_at >= timestamp_sub(current_timestamp(), interval 30 day)
group by attribution_quality_flag
order by bookings desc
