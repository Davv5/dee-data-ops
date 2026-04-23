-- U3 blob-shim (2026-04-23): replaces the pre-U3 dual-source staging
-- (Fivetran `raw_calendly.event` + Cloud Run poller
-- `raw_calendly.scheduled_events`) with a single blob-shim that filters
-- `Raw.calendly_objects_raw` by `entity_type = 'scheduled_events'` and
-- JSON-decodes payload_json.
--
-- This is load-bearing for Speed-to-Lead: `booked_at` (Calendly's
-- `created_at`) is the start clock for the metric; `scheduled_for`
-- (Calendly's `start_time`) is the meeting time.
--
-- TODO: retire when Calendly Phase-2 per-object tables
-- (`raw_calendly.calendly__scheduled_events_raw`) start landing data.

with

source as (

    select
        json_value(payload_json, '$.uri')                               as uri,
        json_value(payload_json, '$.event_type')                        as event_type_uri,
        json_value(payload_json, '$.name')                              as name,
        json_value(payload_json, '$.status')                            as status,
        json_value(payload_json, '$.location.type')                     as location_type,

        safe_cast(json_value(payload_json, '$.start_time') as timestamp)   as start_time,
        safe_cast(json_value(payload_json, '$.end_time') as timestamp)     as end_time,
        safe_cast(json_value(payload_json, '$.created_at') as timestamp)   as created_at,
        safe_cast(json_value(payload_json, '$.updated_at') as timestamp)   as updated_at,

        json_value(payload_json, '$.cancellation.reason')               as cancel_reason,
        json_value(payload_json, '$.cancellation.cancelled_by')         as canceled_by,
        json_value(payload_json, '$.cancellation.canceler_type')        as canceler_type,

        safe_cast(json_value(payload_json, '$.invitees_counter.active') as int64) as invitees_active,
        safe_cast(json_value(payload_json, '$.invitees_counter.limit') as int64)  as invitees_limit,

        -- Blob shim has no soft-delete flag; default false. If downstream
        -- ever needs real deletion tracking, the Cancellation status
        -- (`canceled`) already covers the logical case.
        false                                                           as is_deleted,

        ingested_at                                                     as _ingested_at,
        'blob_shim'                                                     as _source_path
    from {{ source('raw_calendly', 'calendly_objects_raw') }}
    where entity_type = 'scheduled_events'

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by uri
        order by _ingested_at desc
    ) = 1

),

parsed as (

    select
        uri                                                             as event_id,

        event_type_uri                                                  as event_type_id,
        name                                                            as event_name,
        status,
        location_type,

        start_time                                                      as scheduled_for,
        end_time                                                        as scheduled_until,
        created_at                                                      as booked_at,
        updated_at,

        cancel_reason,
        canceled_by                                                     as cancelled_by,
        canceler_type                                                   as cancelled_by_type,

        invitees_active                                                 as active_invitee_count,
        invitees_limit                                                  as invitee_limit,

        is_deleted,

        _ingested_at,
        _source_path
    from deduped

),

final as (

    select * from parsed

)

select * from final
