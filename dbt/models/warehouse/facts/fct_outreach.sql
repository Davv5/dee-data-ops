-- Grain: one row per outbound human CALL/SMS. Source is
-- `stg_ghl__messages` filtered to `direction = 'outbound'` and message_type
-- in ('TYPE_CALL','TYPE_SMS'). Automation is excluded by joining to
-- `stg_ghl__conversations` and requiring `last_manual_message_at is not
-- null` at the conversation level — a manual-touch signal GHL surfaces
-- natively. No role filter here: per Track D's decisions the SDR filter
-- moves to the mart so this fact stays reusable for AE/Closer analysis.
--
-- contact match is GHL-native (`contact_id` on the message row) so
-- match_method = 'ghl_native', match_score = 1.00 for every row.
-- user_sk is resolved via LEFT JOIN to dim_users rather than hashed
-- directly from the message's user_id, because GHL /messages carries
-- user_ids for system/automation actors that never appear in
-- /users/search. Those rows get user_sk NULL (unattributed) instead
-- of emitting a dangling SK that breaks the relationships test.

with

messages as (

    select *
    from {{ ref('stg_ghl__messages') }}
    where lower(direction) = 'outbound'
        and message_type in ('TYPE_CALL', 'TYPE_SMS')

),

conversations as (

    select
        conversation_id,
        last_manual_message_at
    from {{ ref('stg_ghl__conversations') }}

),

manual_touches as (

    select
        messages.*
    from messages
    inner join conversations
        on messages.conversation_id = conversations.conversation_id
    where conversations.last_manual_message_at is not null

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

        {{ dbt_utils.generate_surrogate_key(
            ['manual_touches.location_id', 'manual_touches.contact_id']
        ) }}                                                    as contact_sk,

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
    left join users
        on manual_touches.user_id = users.user_id

)

select * from final
