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
-- Architecture (post-adversarial-review of PR #143):
--   - Calls come ONLY from `Core.fct_ghl_outbound_calls`. The `conversations`
--     entity also carries call mirrors (1,487 `type_call`/`type_custom_call`
--     rows in this tenant) but those are duplicates of `outbound_call_logs`
--     entries with sparser data. We exclude them from the SMS branch via
--     a strict `message_type_norm = 'type_sms'` filter rather than deduping
--     across streams (per `data-modeling-process.md` macro #2: avoid QUALIFY
--     ROW_NUMBER as a downstream dedupe).
--   - SMS comes ONLY from `Core.fct_ghl_conversations` filtered to
--     `message_type_norm = 'type_sms'`.
--
-- Manual-vs-automation gates (locked v1 metric: human only — automated
-- workflow messages do not count):
--   - CALLS: filter out `payload_json.$.source = 'workflow'` (173 rows in
--     this tenant as of 2026-04-30 — GHL workflow-issued calls). `app` and
--     `api` source values count as human (CallTools/dialer integrations
--     issue `api`; clicked-dial in the GHL app issues `app`). Adversarial
--     reviewer flagged the `api` ambiguity — defer the strict filter until
--     a downstream consumer surfaces a need; current-tenant analysis
--     shows api ≈ legitimate dialer software, not generative bots.
--   - SMS: must belong to a conversation with `last_manual_message_at IS
--     NOT NULL` (preserved from original; `stg_ghl__conversations` is the
--     source of truth for this signal — GHL exposes
--     `lastManualMessageDate` distinct from `lastMessageDate`).
--
-- user_sk is resolved via LEFT JOIN to dim_users rather than hashed
-- directly from the message's user_id, because GHL payloads carry user_ids
-- for system / automation actors that never appear in /users/search. Those
-- rows get user_sk NULL (unattributed) instead of emitting a dangling SK
-- that breaks the relationships test.
--
-- contact_sk is resolved via LEFT JOIN to dim_contacts (vs. computed hash)
-- so orphan touches whose contact isn't in dim_contacts (historical /
-- deleted) emit NULL contact_sk, matching the user_sk pattern.

with

outbound_calls_raw as (

    select * from {{ source('ghl_core', 'fct_ghl_outbound_calls') }}
    where lower(coalesce(direction_norm, '')) = 'outbound'
        and coalesce(call_started_at, event_ts, updated_at_ts, ingested_at) is not null
        -- Exclude GHL workflow-issued automated calls (the locked-metric
        -- exclusion lever for the call channel).
        and lower(coalesce(json_value(payload_json, '$.source'), '')) != 'workflow'

),

outbound_calls as (

    -- Dedupe re-poll artifacts on call_log_id: Core.fct_ghl_outbound_calls
    -- carries duplicates when bq-ingest re-polls a call before its state
    -- finalizes. Keep the most-recently-touched record (updated_at_ts DESC,
    -- ingested_at DESC) — matches the original stg_ghl__messages dedupe
    -- pattern and aligns with the warehouse-rule maxim "most recent state
    -- preferred." Tiebreaker addresses adversarial review finding #2.
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
        nullif(trim(lower(json_value(payload_json, '$.source'))), '')   as message_source,
        coalesce(call_started_at, event_ts, updated_at_ts, ingested_at) as sent_at

    from outbound_calls_raw
    qualify row_number() over (
        partition by call_log_id, location_id
        order by updated_at_ts desc nulls last,
                 ingested_at desc nulls last
    ) = 1

),

outbound_sms_raw as (

    select * from {{ source('ghl_core', 'fct_ghl_conversations') }}
    where lower(coalesce(direction_norm, '')) = 'outbound'
        -- Strict 'type_sms' filter excludes the call-mirrors that the
        -- conversations entity also carries (`type_call`,
        -- `type_custom_call`) — those are duplicates of
        -- `outbound_call_logs` entries with sparser data. Calls come from
        -- the canonical `outbound_calls` CTE above.
        and lower(coalesce(message_type_norm, '')) = 'type_sms'
        and coalesce(message_created_at, event_ts, updated_at_ts, ingested_at) is not null

),

outbound_sms as (

    -- Same re-poll dedupe pattern as calls. message_id is the natural key.
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

    from outbound_sms_raw
    qualify row_number() over (
        partition by message_id, location_id
        order by updated_at_ts desc nulls last,
                 ingested_at desc nulls last
    ) = 1

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

manual_touches as (

    -- Calls already exclude `source='workflow'` upstream. Pass through.
    select * from outbound_calls

    union all

    -- SMS must belong to a conversation that has at least one manual message.
    select s.*
    from outbound_sms s
    inner join manual_conversations mc using (conversation_id)

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
