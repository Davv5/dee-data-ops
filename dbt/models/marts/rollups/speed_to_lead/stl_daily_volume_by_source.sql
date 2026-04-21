-- Daily booking volume stacked by lead source — powers the main area chart.
-- Top-10 sources by volume; everything else bucketed as 'other' so the stack
-- doesn't blow out to 90 series in the legend.
{{ config(materialized='table') }}

with ranked_sources as (
    select
        coalesce(lead_source, 'unknown') as lead_source,
        count(*) as n
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp_sub(current_timestamp(), interval 90 day)
    group by lead_source
    order by n desc
    limit 10
),
top_sources as (
    select lead_source from ranked_sources
),
labelled as (
    select
        date(booked_at) as booking_date,
        case
            when coalesce(lead_source, 'unknown') in (select lead_source from top_sources)
                then coalesce(lead_source, 'unknown')
            else 'other'
        end as lead_source_grouped
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp_sub(current_timestamp(), interval 90 day)
)

select
    booking_date,
    lead_source_grouped as lead_source,
    count(*) as bookings
from labelled
group by booking_date, lead_source_grouped
order by booking_date, lead_source_grouped
