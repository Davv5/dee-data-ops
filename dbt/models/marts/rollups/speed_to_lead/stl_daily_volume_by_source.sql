-- Daily booking volume stacked by lead source — powers the main area chart.
-- Top-10 sources by volume; everything else bucketed as 'other' so the stack
-- doesn't blow out to 90 series in the legend.
--
-- Single-scan implementation: read the mart once into `bookings_90d`, then
-- derive the top-10 label set via a windowed rank over the per-source totals
-- in a CTE. Final projection LEFT JOINs daily-grouped volume to the top-10
-- set and coalesces non-members to 'other'. Avoids the double-scan +
-- subquery-in-IN pattern of the previous version.
{{ config(materialized='table') }}

with bookings_90d as (
    select
        date(booked_at) as booking_date,
        coalesce(lead_source, 'unknown') as lead_source
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp_sub(current_timestamp(), interval 90 day)
),

daily_by_source as (
    select
        booking_date,
        lead_source,
        count(*) as bookings
    from bookings_90d
    group by booking_date, lead_source
),

source_totals_ranked as (
    select
        lead_source,
        row_number() over (order by sum(bookings) desc) as source_rank
    from daily_by_source
    group by lead_source
),

top_sources as (
    select lead_source
    from source_totals_ranked
    where source_rank <= 10
)

select
    d.booking_date,
    case
        when t.lead_source is not null then d.lead_source
        else 'other'
    end as lead_source,
    sum(d.bookings) as bookings
from daily_by_source d
left join top_sources t
  on t.lead_source = d.lead_source
group by
    d.booking_date,
    case
        when t.lead_source is not null then d.lead_source
        else 'other'
    end
order by booking_date, lead_source
