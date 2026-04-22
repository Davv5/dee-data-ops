-- Outcome bucketed by first-touch time — closes the loop from speed → outcome.
-- Grain: one row per touch_bucket (7 buckets). Last 30 days.
--
-- Show-rate derivation: the mart does not carry an explicit show_outcome column
-- (no `show`/`no_show` field on sales_activity_detail). We fall back to the
-- rule "a close implies a show" — i.e. close_outcome IS NOT NULL ('won'/'lost'/'pending')
-- treats the call as showed. This over-estimates show-rate by counting 'pending'
-- as showed; revisit when show-outcome lands in the mart.
{{ config(materialized='table') }}

with base as (
    select
        minutes_to_first_sdr_touch,
        close_outcome,
        case
            when minutes_to_first_sdr_touch is null                     then 'No SDR touch'
            when minutes_to_first_sdr_touch < 5                         then '0-5 min'
            when minutes_to_first_sdr_touch < 15                        then '5-15 min'
            when minutes_to_first_sdr_touch < 60                        then '15-60 min'
            when minutes_to_first_sdr_touch < 240                       then '1-4 hr'
            when minutes_to_first_sdr_touch < 1440                      then '4-24 hr'
            else                                                             '24+ hr'
        end as touch_bucket,
        case
            when minutes_to_first_sdr_touch is null                     then 7
            when minutes_to_first_sdr_touch < 5                         then 1
            when minutes_to_first_sdr_touch < 15                        then 2
            when minutes_to_first_sdr_touch < 60                        then 3
            when minutes_to_first_sdr_touch < 240                       then 4
            when minutes_to_first_sdr_touch < 1440                      then 5
            else                                                             6
        end as bucket_sort
    from {{ ref('sales_activity_detail') }}
    where booked_at >= timestamp(date_sub(current_date(), interval 30 day))
)

select
    touch_bucket,
    bucket_sort,
    count(*) as bookings,
    countif(close_outcome = 'won') as closed_won,
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
from base
group by touch_bucket, bucket_sort
order by bucket_sort
