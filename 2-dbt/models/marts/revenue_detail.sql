{{
    config(
        materialized = 'table',
    )
}}

-- Payment-grain revenue reconciliation mart.
-- Every payment appears, including unmatched ones — the unmatched rows are
-- the "transparency" signal for Page 3 of the dashboard. Do NOT filter them.

with

payments as (

    select * from {{ ref('fct_revenue') }}

),

bridge as (

    select * from {{ ref('bridge_identity_contact_payment') }}

),

contacts as (

    -- dim_contacts doesn't expose multi-touch + lead-magnet columns yet
    -- (Track E ships single-touch utm_* only). NULL-stub them to keep the
    -- mart's column contract stable; Page 3 revenue-by-campaign tiles
    -- render empty until upstream enrichment lands.
    select
        contact_sk,
        contact_id,
        email                                 as contact_email,
        cast(null as string)                  as first_touch_campaign,
        cast(null as string)                  as first_touch_source,
        cast(null as string)                  as first_touch_medium,
        cast(null as string)                  as last_touch_campaign,
        cast(null as string)                  as last_touch_source,
        cast(null as string)                  as lead_magnet_first_engaged
    from {{ ref('dim_contacts') }}

),

opportunities as (

    select
        contact_id,
        assigned_user_id,
        opportunity_updated_at
    from {{ ref('stg_ghl__opportunities') }}

),

users as (

    select
        user_id,
        name                        as user_name,
        role                        as user_role
    from {{ ref('dim_users') }}

),

-- Latest Closer per contact: join opportunities to the users dim filtered to
-- Closer role, then take the most recently updated opportunity per contact.
closer_lookup as (

    select
        opportunities.contact_id,
        users.user_name             as closer_name,
        users.user_role             as closer_role
    from opportunities
    inner join users
        on opportunities.assigned_user_id = users.user_id
    where users.user_role = 'Closer'
    qualify row_number() over (
        partition by opportunities.contact_id
        order by opportunities.opportunity_updated_at desc
    ) = 1

),

final as (

    select
        payments.payment_id,
        payments.source_platform,
        payments.transaction_date,
        payments.gross_amount,
        payments.net_amount,
        payments.currency,
        payments.product,

        -- Identity link (NULL contact_sk means unmatched — kept on purpose)
        bridge.contact_sk,
        contacts.contact_email      as contact_email_if_matched,
        bridge.match_method,
        bridge.match_score,
        bridge.bridge_status        as match_status,

        -- P3 #11 payment attribution
        payments.payment_method,
        payments.card_issue_country,
        case
            when payments.card_issue_country is null then null
            when payments.card_issue_country = 'US' then false
            else true
        end                         as is_international_card,

        -- P0 #1 campaign attribution (only populated when matched)
        contacts.first_touch_campaign,
        contacts.first_touch_source,
        contacts.first_touch_medium,
        contacts.last_touch_campaign,
        contacts.last_touch_source,
        contacts.lead_magnet_first_engaged,

        -- P0 #2 closer attribution (only populated when matched + closer known)
        closer_lookup.closer_name,
        closer_lookup.closer_role,

        -- DQ transparency flag: every payment lands in exactly one bucket
        case
            when bridge.bridge_status = 'unmatched' then 'unmatched'
            when bridge.bridge_status = 'ambiguous_multi_candidate'
                then 'ambiguous_contact_match'
            when
                bridge.bridge_status = 'matched'
                and closer_lookup.closer_name is null
                then 'role_unknown'
            else 'clean'
        end                         as attribution_quality_flag,

        current_timestamp()         as mart_refreshed_at

    from payments
    left join bridge
        on payments.source_platform = bridge.source_platform
       and payments.payment_id      = bridge.payment_id
    left join contacts
        on bridge.contact_sk = contacts.contact_sk
    left join closer_lookup
        on contacts.contact_id = closer_lookup.contact_id

)

select * from final
