-- Lead-level drill-down for Page 1's detail table — last 30 days, most recent
-- first. Every row is a specific lead + the SDR who reached them + SLA hit/miss.
-- Client-facing: PII is already in the upstream mart; Looker share permissions
-- govern visibility, not the rollup layer.
{{ config(materialized='table') }}

select
    booked_at,
    full_name,
    email,
    first_toucher_name as sdr_name,
    minutes_to_first_sdr_touch as mins_to_touch,
    is_within_5_min_sla,
    had_any_sdr_activity_within_1_hr,
    lead_source,
    first_touch_campaign,
    close_outcome,
    lost_reason,
    attribution_quality_flag
from {{ ref('sales_activity_detail') }}
where booked_at >= timestamp_sub(current_timestamp(), interval 30 day)
order by booked_at desc
