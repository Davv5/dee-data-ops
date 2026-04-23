-- U3 column-rename (2026-04-23): see stg_ghl__contacts.sql header.
-- NB: raw_ghl.ghl__messages_raw is 0 rows as of U1 preflight — source
-- passes zero rows through; not a U3 bug.
with source as (

    select
        entity_id                                                       as id,
        _ingested_at,
        to_json_string(payload_json)                               as payload
    from {{ source('ghl', 'messages') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _ingested_at desc
    ) = 1

),

parsed as (

    select
        id                                                              as message_id,

        json_value(payload, '$.conversationId')                         as conversation_id,
        json_value(payload, '$.contactId')                              as contact_id,
        json_value(payload, '$.userId')                                 as user_id,
        json_value(payload, '$.locationId')                             as location_id,

        json_value(payload, '$.messageType')                            as message_type,
        json_value(payload, '$.direction')                              as direction,
        json_value(payload, '$.status')                                 as status,
        json_value(payload, '$.source')                                 as message_source,
        json_value(payload, '$.contentType')                            as content_type,

        cast(json_value(payload, '$.type') as int64)                    as type_code,

        cast(json_value(payload, '$.dateAdded') as timestamp)           as sent_at,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
