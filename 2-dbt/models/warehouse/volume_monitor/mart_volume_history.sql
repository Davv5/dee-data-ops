{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'mart_name'],
    schema='warehouse'
) }}

-- Daily row-count snapshot per mart. One row per (mart, day). Feeds the
-- volume_drift warn-test which compares today's count against prior-day.

with
snapshot as (
    select current_date() as snapshot_date, 'lead_journey' as mart_name, count(*) as row_count
    from {{ ref('lead_journey') }}
    union all
    select current_date(), 'revenue_detail', count(*)
    from {{ ref('revenue_detail') }}
)

select *
from snapshot
{% if is_incremental() %}
where snapshot_date > (select coalesce(max(snapshot_date), date '1900-01-01') from {{ this }})
{% endif %}
