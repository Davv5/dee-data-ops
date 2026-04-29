-- U3 blob-shim (2026-04-23): replaces the pre-U3 dual-source staging
-- (Fivetran `raw_calendly.event_invitee` + Cloud Run poller
-- `raw_calendly.invitees`) with a single blob-shim that filters
-- `Raw.calendly_objects_raw` by `entity_type = 'event_invitees'` and
-- JSON-decodes payload_json.
--
-- The invitee table is the ONLY source of booking-level email — required
-- to bridge Calendly bookings to GHL contacts. This staging is the
-- companion to `stg_calendly__events`.
--
-- TODO: retire when Calendly Phase-2 per-object tables
-- (`raw_calendly.calendly__event_invitees_raw`) start landing data.

with

source as (

    select
        json_value(payload_json, '$.uri')                               as uri,
        json_value(payload_json, '$.event')                             as event_uri,
        json_value(payload_json, '$.email')                             as email,
        json_value(payload_json, '$.name')                              as name,
        json_value(payload_json, '$.status')                            as status,

        safe_cast(json_value(payload_json, '$.rescheduled') as bool)    as rescheduled,

        -- Cancellation shape on invitees: `cancel_url` is always present,
        -- but the actual cancellation reason (when status='canceled') is
        -- at `$.cancellation.reason`. Map both paths; rely on status to
        -- gate meaning downstream.
        coalesce(
            json_value(payload_json, '$.cancellation.reason'),
            json_value(payload_json, '$.cancel_url')
        )                                                               as cancel_reason,
        json_value(payload_json, '$.cancellation.cancelled_by')         as canceled_by,
        json_value(payload_json, '$.cancellation.canceler_type')        as canceler_type,

        json_value(payload_json, '$.timezone')                          as timezone,
        json_value(payload_json, '$.text_reminder_number')              as text_reminder_number,

        json_value(payload_json, '$.tracking.utm_source')               as tracking_utm_source,
        json_value(payload_json, '$.tracking.utm_medium')               as tracking_utm_medium,
        json_value(payload_json, '$.tracking.utm_campaign')             as tracking_utm_campaign,

        safe_cast(json_value(payload_json, '$.no_show.created_at') as timestamp) as no_show_created_at,
        json_value(payload_json, '$.no_show.uri')                       as no_show_uri,

        safe_cast(json_value(payload_json, '$.created_at') as timestamp) as invitee_created_at,
        safe_cast(json_value(payload_json, '$.updated_at') as timestamp) as invitee_updated_at,

        ingested_at                                                     as _ingested_at,

        -- Blob shim has no soft-delete flag; downstream filters on status
        -- if needed.
        false                                                           as is_deleted,

        'blob_shim'                                                     as _source_path
    from `project-41542e21-470f-4589-96d`.`Raw`.`calendly_objects_raw`
    where entity_type = 'event_invitees'

),

deduped as (

    select *
    from source
    where is_deleted = false
    qualify row_number() over (
        partition by uri
        order by _ingested_at desc
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

        invitee_created_at,
        invitee_updated_at,

        _ingested_at,
        _source_path

    from deduped

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