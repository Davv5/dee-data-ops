-- Grain: one row per Calendly booking event (`stg_calendly__events.event_id`).
-- Speed-to-Lead denominator source — every confirmed booking, including
-- bookings that never landed on a GHL opportunity.
--
-- Caveat on contact_sk: `stg_calendly__events` carries no invitee email
-- (invitee records are a sibling Calendly table, owed as
-- `stg_calendly__event_invitees` per Track C's open threads). Until the
-- invitee staging lands, `contact_sk` resolves to NULL; the relationships
-- test auto-excludes NULLs, and the mart layer tolerates unmatched events.
-- When invitee staging ships, widen the join here — do not widen
-- `dim_contacts`.

with

events as (

    select * from {{ ref('stg_calendly__events') }}

),

invitees as (

    select * from {{ ref('stg_calendly__event_invitees') }}

),

contacts as (

    select
        contact_sk,
        email_norm
    from {{ ref('dim_contacts') }}

),

-- Diagnostic join to the opportunity layer: when the booked Calendly event
-- eventually lands on a GHL opportunity (by invitee email) the resulting
-- opportunity carries assigned_user_id + pipeline_stage_id we surface on
-- the fact. Today the join axis doesn't exist (no invitee email on events),
-- so these columns are NULL; structure preserved for forward compatibility.
opportunities as (

    select
        opportunity_id,
        contact_id,
        assigned_user_id,
        pipeline_id,
        pipeline_stage_id
    from {{ ref('stg_ghl__opportunities') }}

),

pipeline_stages as (

    select
        pipeline_id,
        stage_id,
        pipeline_stage_sk
    from {{ ref('dim_pipeline_stages') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['events.event_id']) }}
                                                                as booking_sk,

        contacts.contact_sk                                     as contact_sk,
        cast(null as string)                                    as assigned_user_sk,
        cast(null as string)                                    as pipeline_stage_sk,

        events.event_id                                         as calendly_event_id,
        events.event_type_id,
        events.event_name,

        events.booked_at,
        events.scheduled_for,
        events.scheduled_until,

        events.status                                           as event_status,

        case
            when lower(events.status) in ('canceled', 'cancelled')
            then events.updated_at
            else null
        end                                                     as cancelled_at,

        events.location_type,
        events.active_invitee_count,
        events.is_deleted

    from events
    left join invitees
      on invitees.event_id = events.event_id
    left join contacts
      on contacts.email_norm = invitees.invitee_email_norm

)

select * from final
