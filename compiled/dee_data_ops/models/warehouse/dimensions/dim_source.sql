-- Lead-source conformed dimension. Surfaces all distinct lead_source values
-- from dim_contacts and enriches them with human-readable descriptions and
-- the is_paid / is_organic flags from the stl_lead_source_lookup seed.
--
-- NOTE: The actual lead_source values in dim_contacts are campaign / content
-- labels (e.g. "ig blueprint case study", "AI Brand Prompts", "dbb-ig") —
-- not the channel-taxonomy values the seed template assumed (google_ads, etc.).
-- The seed covers the inferrable high-volume channel-level values (TikTok,
-- Instagram, YouTube, email, outbound, skool). Campaign-specific labels that
-- cannot be reliably classified as paid/organic without business context emit
-- is_paid = NULL and are flagged in WORKLOG "Open threads" for David.
--
-- The __unknown__ row is emitted unconditionally so that fct_speed_to_lead_touch
-- rows with NULL lead_source always resolve to a non-NULL source_sk.
-- (source: "The Struggle of Data Modeling (Facts vs Dimensions)", Data Ops
-- notebook — descriptive string values belong in the dimension, not the fact.)

with

-- All distinct lead_source values from the contacts dim (including NULL)
all_sources as (

    select distinct lead_source
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_contacts`

),

-- Seed with human descriptions + is_paid flag for known values
seed as (

    select
        lead_source,
        source_description,
        cast(is_paid as bool)           as is_paid
    from `project-41542e21-470f-4589-96d`.`STG`.`stl_lead_source_lookup`

),

-- The explicit __unknown__ sentinel row for NULL lead_sources
unknown_sentinel as (

    select
        '__unknown__'                   as lead_source,
        'No lead-source attribution captured' as source_description,
        cast(null as bool)              as is_paid

),

-- Coalesce NULL lead_source values from dim_contacts to __unknown__
coalesced_sources as (

    select coalesce(lead_source, '__unknown__') as lead_source
    from all_sources

),

-- Union in the sentinel so __unknown__ is always present
all_sources_with_sentinel as (

    select lead_source from coalesced_sources
    union distinct
    select lead_source from unknown_sentinel

),

-- Left-join the seed enrichment; unseeded values get NULL description / is_paid
enriched as (

    select
        a.lead_source,
        coalesce(s.source_description, u.source_description) as source_description,
        coalesce(s.is_paid,            u.is_paid)            as is_paid
    from all_sources_with_sentinel a
    left join seed s          on lower(s.lead_source) = lower(a.lead_source)
    left join unknown_sentinel u on a.lead_source = '__unknown__'

),

final as (

    select
        to_hex(md5(cast(coalesce(cast(lead_source as string), '_dbt_utils_surrogate_key_null_') as string)))
                                        as source_sk,

        lead_source,
        source_description,
        is_paid,

        -- is_organic: derived when is_paid is known; NULL when unknown
        case
            when is_paid is null then cast(null as bool)
            else not is_paid
        end                             as is_organic

    from enriched

)

select * from final