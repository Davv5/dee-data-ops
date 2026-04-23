{{
    config(
        materialized='table',
        partition_by={
            'field': 'booked_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

-- Wide mart for the Speed-to-Lead domain.
-- Grain: one row per (booking × touch-event). Bookings with zero outbound
-- human SDR touches emit ONE row with touched_at = NULL (no-touch rows).
-- Downstream consumers: Metabase `Speed-to-Lead` dashboard (via native-SQL
-- aggregations). Sibling mart `sales_activity_detail` is kept for Page 2/Page 3
-- dashboards pending their own refactors. Built on `fct_speed_to_lead_touch`
-- to preserve lowest-grain show_outcome column — see that fact's header comment.
-- Column order is the BI-layer contract — new columns go at the END of the list.
-- Source: "How to Create a Data Modeling Pipeline (3 Layer Approach)",
-- Data Ops notebook.

with

fact as (

    select
        speed_to_lead_touch_sk,
        booking_sk,
        touch_sk,
        contact_sk,
        sdr_sk,
        source_sk,
        booked_at,
        touched_at,
        minutes_to_touch,
        is_first_touch,
        is_within_5_min_sla,
        had_any_sdr_activity_within_1_hr,
        is_sdr_touch,
        channel,
        event_status,
        cancelled_at,
        show_outcome,
        close_outcome,
        closed_at,
        lost_reason,
        attribution_quality_flag,
        attribution_era,
        era_flag,
        mart_refreshed_at
    from {{ ref('fct_speed_to_lead_touch') }}

),

sdr as (

    select
        sdr_sk,
        sdr_name,
        email                               as sdr_email
    from {{ ref('dim_sdr') }}

),

src as (

    select
        source_sk,
        lead_source,
        source_description,
        is_paid                             as is_paid_source
    from {{ ref('dim_source') }}

),

contacts as (

    select
        contact_sk,
        contact_id,
        email,
        contact_name                        as full_name,
        -- Multi-touch attribution stubs (NULL until Track E invitee staging ships)
        cast(null as string)                as first_touch_campaign,
        cast(null as string)                as first_touch_source,
        cast(null as string)                as first_touch_medium,
        cast(null as string)                as last_touch_campaign,
        cast(null as string)                as last_touch_source,
        cast(null as string)                as last_touch_medium
    from {{ ref('dim_contacts') }}

),

-- fct_calls_booked carries scheduled_for + pipeline_stage_sk FK
bookings as (

    select
        booking_sk,
        scheduled_for,
        pipeline_stage_sk
    from {{ ref('fct_calls_booked') }}

),

final as (

    select
        -- PK (business-friendly rename at mart layer per .claude/rules/mart-naming.md)
        fact.speed_to_lead_touch_sk         as speed_to_lead_touch_id,
        fact.booking_sk                     as booking_id,
        cont.contact_id,
        cont.email,
        cont.full_name,

        -- Booking timing
        fact.booked_at,
        bk.scheduled_for,
        date(fact.booked_at)                as booked_date,
        fact.event_status,
        fact.cancelled_at,

        -- SDR identity
        sdr.sdr_name,
        sdr.sdr_email,

        -- Touch metrics
        fact.touched_at,
        fact.channel,
        fact.minutes_to_touch,
        fact.is_first_touch,
        fact.is_within_5_min_sla,
        fact.had_any_sdr_activity_within_1_hr,
        fact.is_sdr_touch,

        -- Lead source
        src.lead_source,
        src.source_description,
        src.is_paid_source,

        -- Outcome
        fact.show_outcome,
        fact.close_outcome,
        fact.closed_at,
        case
            when fact.close_outcome = 'won'
                then timestamp_diff(fact.closed_at, fact.booked_at, minute)
        end                                 as cycle_time_booking_to_close_min,
        fact.lost_reason,

        -- Attribution
        fact.attribution_quality_flag,
        fact.attribution_era,
        fact.era_flag,

        -- Pipeline (NULL in dev — fct_calls_booked.pipeline_stage_sk is stubbed
        -- as NULL until the GHL opportunity stage-sync ships in a later track)
        ps.pipeline_name,
        ps.stage_name,
        ps.is_booked_stage,
        cast(null as int64)                 as days_since_stage_change,

        -- Multi-touch attribution stubs (NULL until Track E invitee staging ships)
        cont.first_touch_campaign,
        cont.first_touch_source,
        cont.first_touch_medium,
        cont.last_touch_campaign,
        cont.last_touch_source,
        cont.last_touch_medium,

        -- Tenant
        cast('Dee' as string)               as client,

        fact.mart_refreshed_at

    from fact
    left join contacts cont
        on cont.contact_sk = fact.contact_sk
    left join sdr
        on sdr.sdr_sk = fact.sdr_sk
    left join src
        on src.source_sk = fact.source_sk
    left join bookings bk
        on bk.booking_sk = fact.booking_sk
    left join {{ ref('dim_pipeline_stages') }} ps
        on ps.pipeline_stage_sk = bk.pipeline_stage_sk

)

select * from final
