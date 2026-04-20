-- Staging view over Fivetran's raw_calendly.event_invitee.
--
-- The invitee table is the ONLY source of booking-level email — required to
-- bridge Calendly bookings to GHL contacts. `stg_calendly__events` carries
-- the event shell but no invitee email; this view is its companion.
--
-- For events with multiple invitees (group sessions), pick the earliest-
-- created active invitee as the "primary" via a qualify row_number.
-- Downstream facts join 1:1 on event_id.

with

source as (

    select * from {{ source('raw_calendly', 'event_invitee') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by uri
        order by _fivetran_synced desc
    ) = 1

),

parsed as (

    select
        uri                                                             as invitee_id,
        event_uri                                                       as event_id,

        email                                                           as invitee_email,
        lower(trim(email))                                              as invitee_email_norm,
        name                                                            as invitee_name,

        status                                                          as invitee_status,
        rescheduled,
        cancel_reason,
        canceled_by,
        canceler_type,

        timezone                                                        as invitee_timezone,
        text_reminder_number,

        tracking_utm_source,
        tracking_utm_medium,
        tracking_utm_campaign,

        no_show_created_at,
        no_show_uri,

        cast(created_at as timestamp)                                   as invitee_created_at,
        cast(updated_at as timestamp)                                   as invitee_updated_at,

        cast(_fivetran_synced as timestamp)                             as _ingested_at

    from deduped
    where coalesce(_fivetran_deleted, false) = false

),

-- Primary invitee per event: earliest-created active invitee. Events with
-- no active invitee keep whatever the latest invitee is (cancelled events
-- still belong in the fact table).
primary_per_event as (

    select *
    from parsed
    qualify row_number() over (
        partition by event_id
        order by
            case when lower(invitee_status) = 'active' then 0 else 1 end,
            invitee_created_at asc
    ) = 1

),

final as (

    select * from primary_per_event

)

select * from final
