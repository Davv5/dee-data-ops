-- Staging view over Calendly invitee data.
--
-- Track X (2026-04-22): dual-run overlap update.
-- During overlap, unions Fivetran's raw_calendly.event_invitee with the Cloud
-- Run poller's raw_calendly.invitees. Deduplicates by uri (invitee URI) keeping
-- the most recent effective timestamp. `_source_path` column supports
-- 24h reconciliation queries.
--
-- After Fivetran is paused: remove the union + coalesce, keep only the
-- invitees source and _ingested_at. (Track X runbook step.)
--
-- The invitee table is the ONLY source of booking-level email -- required to
-- bridge Calendly bookings to GHL contacts. `stg_calendly__events` carries
-- the event shell but no invitee email; this view is its companion.
--
-- Track X regression fix (2026-04-23): `raw_calendly.invitees` is created by
-- the Cloud Run poller on its first successful write (WRITE_APPEND +
-- CREATE_IF_NEEDED in ingestion/calendly/extract.py). Before that write, the
-- table does not exist -- which broke the prod `dbt build` immediately after
-- the Track X merge with:
--   Not found: Table dee-data-ops:raw_calendly.invitees was not found in
--   location US
--
-- Fix: gate the poller_source CTE behind `adapter.get_relation()`. When the
-- poller table exists, staging unions both pipelines. When it does not exist
-- yet, staging falls back to the Fivetran source alone (same shape as
-- pre-Track X, with the same output columns preserved). Once the poller has
-- its first successful run, subsequent dbt invocations pick it up
-- automatically with no staging-model edit needed.

{% set poller_relation = adapter.get_relation(
    database=source('raw_calendly', 'invitees').database,
    schema=source('raw_calendly', 'invitees').schema,
    identifier=source('raw_calendly', 'invitees').identifier
) %}

with

fivetran_source as (

    select
        uri,
        event_uri,
        email,
        name,
        status,
        rescheduled,
        cancel_reason,
        canceled_by,
        canceler_type,
        timezone,
        text_reminder_number,
        tracking_utm_source,
        tracking_utm_medium,
        tracking_utm_campaign,
        no_show_created_at,
        no_show_uri,
        cast(created_at as timestamp)                                   as invitee_created_at,
        cast(updated_at as timestamp)                                   as invitee_updated_at,
        cast(null as timestamp)                                         as _ingested_at,
        cast(_fivetran_synced as timestamp)                             as _fivetran_synced,
        coalesce(_fivetran_deleted, false)                              as is_deleted,
        'fivetran'                                                      as _source_path
    from {{ source('raw_calendly', 'event_invitee') }}

){%- if poller_relation is not none %},

poller_source as (

    -- Cloud Run poller writes payload-as-JSON from /scheduled_events/{uuid}/invitees
    select
        json_value(payload, '$.uri')                                    as uri,
        json_value(payload, '$.event._event_uri')                       as event_uri,
        json_value(payload, '$.email')                                  as email,
        json_value(payload, '$.name')                                   as name,
        json_value(payload, '$.status')                                 as status,
        safe_cast(json_value(payload, '$.rescheduled') as bool)         as rescheduled,
        json_value(payload, '$.cancel_url')                             as cancel_reason,  -- poller doesn't expose cancel_reason; placeholder
        cast(null as string)                                            as canceled_by,
        cast(null as string)                                            as canceler_type,
        json_value(payload, '$.timezone')                               as timezone,
        cast(null as string)                                            as text_reminder_number,
        json_value(payload, '$.tracking.utm_source')                    as tracking_utm_source,
        json_value(payload, '$.tracking.utm_medium')                    as tracking_utm_medium,
        json_value(payload, '$.tracking.utm_campaign')                  as tracking_utm_campaign,
        cast(null as timestamp)                                         as no_show_created_at,
        cast(null as string)                                            as no_show_uri,
        cast(json_value(payload, '$.created_at') as timestamp)          as invitee_created_at,
        cast(json_value(payload, '$.updated_at') as timestamp)          as invitee_updated_at,
        _ingested_at,
        cast(null as timestamp)                                         as _fivetran_synced,
        false                                                           as is_deleted,
        'cloud_run'                                                     as _source_path
    from {{ source('raw_calendly', 'invitees') }}

){%- endif %},

combined as (

    select * from fivetran_source
    {%- if poller_relation is not none %}
    union all
    select * from poller_source
    {%- endif %}

),

deduped as (

    select *
    from combined
    where is_deleted = false
    qualify row_number() over (
        partition by uri
        order by coalesce(_ingested_at, _fivetran_synced) desc
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

        coalesce(_ingested_at, _fivetran_synced)                        as _ingested_at,
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
