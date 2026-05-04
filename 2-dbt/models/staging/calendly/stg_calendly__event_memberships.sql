-- Calendly scheduled events carry host/calendar ownership in
-- `event_memberships[]`. Keep it as a source-shaped staging child model so
-- marts can use host evidence without reparsing raw JSON.

with

source as (

    select
        json_value(payload_json, '$.uri')                              as event_id,
        ingested_at                                                    as _ingested_at,
        payload_json
    from {{ source('raw_calendly', 'calendly_objects_raw') }}
    where entity_type = 'scheduled_events'

),

memberships as (

    select
        source.event_id,
        json_value(membership, '$.user')                               as calendly_user_uri,
        nullif(json_value(membership, '$.user_email'), '')             as user_email,
        nullif(lower(trim(json_value(membership, '$.user_email'))), '')
                                                                        as user_email_norm,
        nullif(json_value(membership, '$.user_name'), '')              as user_name,
        safe_cast(
            json_value(membership, '$.buffered_start_time') as timestamp
        )                                                              as buffered_start_at,
        safe_cast(
            json_value(membership, '$.buffered_end_time') as timestamp
        )                                                              as buffered_end_at,
        source._ingested_at,
        membership                                                     as membership_json
    from source,
        unnest(ifnull(json_query_array(source.payload_json, '$.event_memberships'), []))
            as membership

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'event_id',
            'calendly_user_uri',
            'user_email_norm'
        ]) }}                                                          as event_membership_sk,

        event_id,
        calendly_user_uri,
        user_email_norm,
        user_email,
        user_name,
        buffered_start_at,
        buffered_end_at,
        _ingested_at,
        membership_json

    from memberships
    qualify row_number() over (
        partition by event_id, calendly_user_uri, user_email_norm
        order by _ingested_at desc
    ) = 1

)

select * from final
