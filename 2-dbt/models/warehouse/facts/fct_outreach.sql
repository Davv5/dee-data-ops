-- Grain: one row per outbound human CALL/SMS. Repointed 2026-04-30 to read
-- from bq-ingest-built `Core.fct_ghl_outbound_calls` + `Core.fct_ghl_conversations`
-- (see `2-dbt/models/warehouse/_core__sources.yml`) instead of the retired
-- `STG.stg_ghl__messages`. The original source was reading from a permanently-
-- empty view (`raw_ghl_v2.ghl__messages_raw` filtered on a non-existent
-- `entity_type='messages'` — actual entities are `outbound_call_logs` +
-- `conversations`). PR #142 retired the dbt-side speed-to-lead chain that
-- duplicated `Marts.fct_speed_to_lead`; this PR repairs `lead_journey`'s
-- `assigned_sdr_name` + `number_of_dials` columns by re-establishing
-- fct_outreach as a thin wrapper over the canonical bq-ingest facts.
--
-- Manual-vs-automation gate: the locked v1 metric requires "human only"
-- touches. Outbound CALLS are inherently human (a user clicks dial in GHL).
-- Outbound SMS may be automation; we filter via the conversation-level
-- `last_manual_message_at IS NOT NULL` signal preserved in
-- `stg_ghl__conversations`. SMS messages whose parent conversation has no
-- manual message ever are excluded.
--
-- user_sk is resolved via LEFT JOIN to dim_users rather than hashed
-- directly from the message's user_id, because GHL payloads carry user_ids
-- for system / automation actors that never appear in /users/search. Those
-- rows get user_sk NULL (unattributed) instead of emitting a dangling SK
-- that breaks the relationships test.

with

outbound_calls as (

    select
        location_id,
        call_log_id                                                     as message_id,
        cast(null as string)                                            as conversation_id,
        contact_id,

        nullif(trim(coalesce(
            json_value(payload_json, '$.assignedTo'),
            json_value(payload_json, '$.assigned_to'),
            json_value(payload_json, '$.userId'),
            json_value(payload_json, '$.user_id'),
            json_value(payload_json, '$.ownerId'),
            json_value(payload_json, '$.owner_id'),
            json_value(payload_json, '$.agentId'),
            json_value(payload_json, '$.agent_id')
        )), '')                                                         as user_id,

        'TYPE_CALL'                                                     as message_type,
        coalesce(call_status, 'unknown')                                as status,
        cast(null as string)                                            as message_source,
        coalesce(call_started_at, event_ts, updated_at_ts, ingested_at) as sent_at

    from {{ source('ghl_core', 'fct_ghl_outbound_calls') }}
    where lower(coalesce(direction_norm, '')) = 'outbound'
        and coalesce(call_started_at, event_ts, updated_at_ts, ingested_at) is not null

),

outbound_sms as (

    select
        location_id,
        message_id,
        conversation_id,
        contact_id,

        nullif(trim(coalesce(
            json_value(payload_json, '$.assignedTo'),
            json_value(payload_json, '$.assigned_to'),
            json_value(payload_json, '$.userId'),
            json_value(payload_json, '$.user_id'),
            json_value(payload_json, '$.ownerId'),
            json_value(payload_json, '$.owner_id')
        )), '')                                                         as user_id,

        'TYPE_SMS'                                                      as message_type,
        coalesce(message_status_norm, 'unknown')                        as status,
        nullif(trim(lower(json_value(payload_json, '$.source'))), '')   as message_source,
        coalesce(message_created_at, event_ts, updated_at_ts, ingested_at) as sent_at

    from {{ source('ghl_core', 'fct_ghl_conversations') }}
    where lower(coalesce(direction_norm, '')) = 'outbound'
        and regexp_contains(
            lower(coalesce(message_type_norm, '')),
            r'sms|text|whatsapp|type_sms'
        )
        and coalesce(message_created_at, event_ts, updated_at_ts, ingested_at) is not null

),

manual_conversations as (

    -- Conversations with at least one manual (human) message at any point.
    -- The exclusion lever per CLAUDE.local.md locked metric: GHL's
    -- `lastManualMessageDate` payload field is distinct from `lastMessageDate`,
    -- enabling the human-vs-automation distinction.
    select conversation_id
    from {{ ref('stg_ghl__conversations') }}
    where last_manual_message_at is not null

),

unioned as (

    -- Calls bypass the manual gate (outbound calls are inherently human).
    select * from outbound_calls

    union all

    -- SMS must belong to a conversation that has at least one manual message.
    select s.*
    from outbound_sms s
    inner join manual_conversations mc using (conversation_id)

),

manual_touches as (

    -- Dedupe across the two GHL entity types: an outbound call is sometimes
    -- logged in BOTH `outbound_call_logs` (the call entity) AND
    -- `conversations` (the message-grain entity that captures the call as a
    -- TYPE_CALL message). Both rows share the same id, so a UNION ALL emits
    -- duplicate `message_id`s. Prefer the call row (richer call_status +
    -- duration than the conversation-message representation).
    --
    -- Note (data-modeling-process.md macro #2): QUALIFY ROW_NUMBER() = 1 is
    -- a code smell when used to mask an upstream grain bug. Here it's not
    -- masking — it's reconciling two GHL entity types that legitimately
    -- log the same physical event from different perspectives.
    select * from unioned
    qualify row_number() over (
        partition by message_id, location_id
        order by case when message_type = 'TYPE_CALL' then 1 else 2 end,
                 sent_at
    ) = 1

),

contacts as (

    select
        contact_id,
        location_id,
        contact_sk
    from {{ ref('dim_contacts') }}

),

users as (

    select
        user_id,
        user_sk
    from {{ ref('dim_users') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['manual_touches.message_id']) }}
                                                                as touch_sk,

        contacts.contact_sk,

        users.user_sk,

        manual_touches.message_id,
        manual_touches.conversation_id,
        manual_touches.location_id,

        manual_touches.sent_at                                  as touched_at,

        case
            when manual_touches.message_type = 'TYPE_CALL' then 'call'
            when manual_touches.message_type = 'TYPE_SMS'  then 'sms'
        end                                                     as channel,

        manual_touches.message_source,
        manual_touches.status                                   as message_status,

        'ghl_native'                                            as match_method,
        1.00                                                    as match_score

    from manual_touches
    left join contacts
        on  manual_touches.location_id = contacts.location_id
        and manual_touches.contact_id  = contacts.contact_id
    left join users
        on manual_touches.user_id = users.user_id

)

select * from final
