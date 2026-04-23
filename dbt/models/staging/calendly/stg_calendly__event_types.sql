-- U3 blob-shim (2026-04-23): new staging over Calendly event types
-- (the set of bookable appointment templates in a user's Calendly account).
-- Filters `Raw.calendly_objects_raw` by `entity_type = 'event_types'`.
--
-- Event types resolve opaque `event_type_uri` foreign keys on scheduled
-- events into human-readable names (e.g. "30 Minute Meeting",
-- "Strategy Call"). No downstream mart consumes this today — it is
-- scaffolded in U3 so the shim is complete for the three Calendly
-- entity_types present in `Raw.calendly_objects_raw`.
--
-- TODO: retire when Calendly Phase-2 per-object tables
-- (`raw_calendly.calendly__event_types_raw`) start landing data.

with

source as (

    select
        json_value(payload_json, '$.uri')                               as uri,
        json_value(payload_json, '$.name')                              as name,
        json_value(payload_json, '$.slug')                              as slug,
        json_value(payload_json, '$.kind')                              as kind,
        json_value(payload_json, '$.scheduling_url')                    as scheduling_url,
        safe_cast(json_value(payload_json, '$.duration') as int64)      as duration_minutes,
        safe_cast(json_value(payload_json, '$.active') as bool)         as is_active,
        json_value(payload_json, '$.color')                             as color,
        json_value(payload_json, '$.type')                              as event_kind,

        safe_cast(json_value(payload_json, '$.created_at') as timestamp) as created_at,
        safe_cast(json_value(payload_json, '$.updated_at') as timestamp) as updated_at,

        ingested_at                                                     as _ingested_at
    from {{ source('raw_calendly', 'calendly_objects_raw') }}
    where entity_type = 'event_types'

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
        uri                                                             as event_type_id,

        name                                                            as event_type_name,
        slug,
        kind,
        event_kind,

        scheduling_url,
        duration_minutes,
        is_active,
        color,

        created_at                                                      as event_type_created_at,
        updated_at                                                      as event_type_updated_at,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
