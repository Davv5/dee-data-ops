with source as (

    select
        id,
        _ingested_at,
        payload
    from {{ source('ghl', 'conversations') }}

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
        id                                                              as conversation_id,

        json_value(payload, '$.contactId')                              as contact_id,
        json_value(payload, '$.assignedTo')                             as assigned_user_id,
        json_value(payload, '$.locationId')                             as location_id,

        json_value(payload, '$.type')                                   as conversation_type,
        json_value(payload, '$.lastMessageType')                        as last_message_type,
        json_value(payload, '$.lastMessageDirection')                   as last_message_direction,
        json_value(payload, '$.lastOutboundMessageAction')              as last_outbound_action,

        timestamp_millis(
            cast(json_value(payload, '$.lastMessageDate') as int64)
        )                                                               as last_message_at,
        timestamp_millis(
            cast(json_value(payload, '$.lastManualMessageDate') as int64)
        )                                                               as last_manual_message_at,
        timestamp_millis(
            cast(json_value(payload, '$.dateAdded') as int64)
        )                                                               as created_at,
        timestamp_millis(
            cast(json_value(payload, '$.dateUpdated') as int64)
        )                                                               as updated_at,

        cast(json_value(payload, '$.unreadCount') as int64)             as unread_count,
        cast(json_value(payload, '$.inbox') as bool)                    as in_inbox,
        cast(json_value(payload, '$.attributed') as bool)               as is_attributed,
        cast(
            json_value(payload, '$.isLastMessageInternalComment') as bool
        )                                                               as is_last_message_internal_comment,

        json_value(payload, '$.fullName')                               as contact_full_name,
        json_value(payload, '$.email')                                  as contact_email,
        json_value(payload, '$.phone')                                  as contact_phone,
        json_value(payload, '$.companyName')                            as company_name,

        json_extract_string_array(payload, '$.tags')                    as tags,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
