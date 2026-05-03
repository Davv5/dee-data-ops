-- Asserts lead_magnet_detail preserves one row per GHL opportunity.
-- This is the core grain contract for the lead-magnet mart: opportunities
-- may have repeated contacts, but opportunity_id must remain lossless.

with

staging as (

    select count(*) as opportunity_count
    from {{ ref('stg_ghl__opportunities') }}

),

mart as (

    select count(*) as opportunity_count
    from {{ ref('lead_magnet_detail') }}

),

comparison as (

    select
        staging.opportunity_count as staging_opportunities,
        mart.opportunity_count    as mart_opportunities
    from staging
    cross join mart

)

select *
from comparison
where staging_opportunities != mart_opportunities
