{{
    config(
        materialized = 'table',
    )
}}

-- Buyer-journey revenue mart.
--
-- Grain: one row per matched paid contact (`contact_sk`).
-- This mart sits beside `revenue_detail`: payment reconciliation remains
-- payment-grain there, while this table answers the operator question:
-- which buyer path produced money, from magnet to touch to booking to payment.
--
-- Source-process grounding: marts are wide, business-facing contracts
-- (source: `mart-naming.md`, Data Ops notebook citation). Reuse the
-- buyer-grain lead-magnet mart instead of re-implementing its attribution
-- window logic.

with

buyers as (

    select * from {{ ref('lead_magnet_buyer_detail') }}

),

latest_prior_opportunities as (

    select
        opportunity_id,
        assigned_user_sk,
        assigned_user_id,
        assigned_user_name,
        assigned_user_role,
        touches_count,
        call_count,
        sms_count,
        successful_call_count,
        direct_bookings_count,
        window_bookings_count,
        payment_count,
        net_revenue_after_refunds
    from {{ ref('lead_magnet_detail') }}

),

refunds_per_payment as (

    select
        source_platform,
        parent_payment_id,
        sum(refund_amount)                                             as refunds_total_amount,
        count(*)                                                       as refunds_count
    from {{ ref('fct_refunds') }}
    group by 1, 2

),

payments_net as (

    select
        payments.payment_id,
        payments.source_platform,
        payments.contact_sk,
        payments.transaction_date,
        payments.gross_amount,
        payments.net_amount,
        payments.product,
        payments.source_product_id,
        payments.source_product_internal_name,
        payments.source_product_price,
        payments.source_service_id,
        payments.source_service_title,
        payments.source_service_internal_name,
        payments.source_service_price,
        payments.source_service_payment_id,
        payments.source_fund_release_on,
        payments.source_fund_released,
        payments.payment_method,
        payments.card_issue_country,
        payments.is_refunded,
        payments.match_method,
        payments.match_score,
        payments.bridge_status,

        case
            when payments.source_platform = 'stripe'
                then payments.net_amount
            else
                payments.net_amount
                - coalesce(refunds_per_payment.refunds_total_amount, 0)
        end                                                           as net_amount_after_refunds,

        coalesce(refunds_per_payment.refunds_total_amount, 0)          as refunds_total_amount,
        coalesce(refunds_per_payment.refunds_count, 0)                 as refunds_count

    from {{ ref('fct_payments') }} as payments
    left join refunds_per_payment
        on payments.source_platform = refunds_per_payment.source_platform
       and payments.payment_id      = refunds_per_payment.parent_payment_id
    where payments.is_paid = true
        and payments.contact_sk is not null

),

ranked_payments as (

    select
        payments_net.*,

        row_number() over (
            partition by payments_net.contact_sk
            order by
                payments_net.transaction_date,
                payments_net.source_platform,
                payments_net.payment_id
        )                                                               as payment_sequence,

        row_number() over (
            partition by payments_net.contact_sk
            order by
                payments_net.transaction_date desc,
                payments_net.source_platform desc,
                payments_net.payment_id desc
        )                                                               as payment_recency

    from payments_net

),

first_payments as (

    select
        contact_sk,
        payment_id                                                     as first_payment_id,
        source_platform                                                as first_payment_source_platform,
        transaction_date                                               as first_purchase_at,
        product                                                        as first_purchase_product,
        payment_method                                                 as first_purchase_payment_method,
        card_issue_country                                             as first_purchase_card_issue_country,
        gross_amount                                                   as first_purchase_gross_amount,
        net_amount_after_refunds                                       as first_purchase_net_revenue
    from ranked_payments
    where payment_sequence = 1

),

latest_payments as (

    select
        contact_sk,
        payment_id                                                     as latest_payment_id,
        source_platform                                                as latest_payment_source_platform,
        transaction_date                                               as latest_purchase_at,
        product                                                        as latest_purchase_product,
        payment_method                                                 as latest_purchase_payment_method,
        net_amount_after_refunds                                       as latest_payment_net_revenue
    from ranked_payments
    where payment_recency = 1

),

payment_summary as (

    select
        contact_sk,
        count(*)                                                       as paid_payments_count,
        count(distinct source_platform)                                as payment_source_platform_count,
        countif(source_platform = 'stripe')                            as stripe_payments_count,
        countif(source_platform = 'fanbasis')                          as fanbasis_payments_count,
        countif(source_platform = 'fanbasis' and payment_method = 'upfront')
                                                                        as fanbasis_upfront_payments_count,
        countif(source_platform = 'fanbasis' and payment_method = 'auto_renew')
                                                                        as fanbasis_auto_renew_payments_count,
        count(distinct if(source_platform = 'fanbasis', source_service_payment_id, null))
                                                                        as fanbasis_service_payment_ids_count,
        sum(if(source_platform = 'fanbasis', gross_amount, 0))           as fanbasis_gross_revenue,
        sum(if(source_platform = 'fanbasis', net_amount_after_refunds, 0))
                                                                        as fanbasis_net_revenue_after_refunds,
        countif(source_platform = 'fanbasis' and is_refunded)            as fanbasis_refunded_payments_count,
        countif(source_platform = 'fanbasis' and source_fund_released)   as fanbasis_released_payments_count,
        countif(
            source_platform = 'fanbasis'
            and source_fund_released is not null
            and not source_fund_released
        )                                                               as fanbasis_unreleased_payments_count,
        max(if(source_platform = 'fanbasis', source_fund_release_on, null))
                                                                        as latest_fanbasis_fund_release_on,
        countif(
            regexp_contains(
                lower(coalesce(product, '')),
                r'(split pay|deposit|balance|payment plan|auto renew|auto_renew)'
            )
        )                                                               as plan_named_payments_count,
        count(distinct coalesce(product, 'unknown'))                    as purchased_product_count,
        sum(gross_amount)                                              as total_gross_revenue,
        sum(net_amount_after_refunds)                                  as total_net_revenue_after_refunds,
        sum(refunds_total_amount)                                      as total_refunds_amount,
        sum(refunds_count)                                             as total_refunds_count,
        string_agg(distinct coalesce(product, 'unknown'), ' | ')       as purchased_products
    from payments_net
    group by 1

),

product_revenue as (

    select
        contact_sk,
        coalesce(product, 'unknown')                                   as product_name,
        sum(net_amount_after_refunds)                                  as product_net_revenue,
        count(*)                                                       as product_payments_count
    from payments_net
    group by 1, 2

),

top_products as (

    select
        contact_sk,
        product_name                                                   as top_product_by_net_revenue,
        product_net_revenue                                            as top_product_net_revenue,
        product_payments_count                                         as top_product_payments_count
    from product_revenue
    qualify row_number() over (
        partition by contact_sk
        order by product_net_revenue desc, product_payments_count desc, product_name
    ) = 1

),

outreach_before_first_purchase as (

    select
        buyers.contact_sk,
        count(outreach.touch_sk)                                       as touches_before_purchase_count,
        countif(outreach.channel = 'call')                             as calls_before_purchase_count,
        countif(outreach.channel = 'sms')                              as sms_before_purchase_count,
        countif(
            outreach.channel = 'call'
            and lower(coalesce(outreach.message_status, '')) in ('answered', 'completed')
        )                                                              as successful_calls_before_purchase_count,

        min(outreach.touched_at)                                       as first_touch_before_purchase_at,
        min(if(outreach.channel = 'call', outreach.touched_at, null))  as first_call_before_purchase_at,
        min(
            if(
                outreach.channel = 'call'
                and lower(coalesce(outreach.message_status, '')) in ('answered', 'completed'),
                outreach.touched_at,
                null
            )
        )                                                              as first_successful_call_before_purchase_at,

        array_agg(
            if(
                outreach.touch_sk is not null,
                struct(
                    outreach.user_sk as user_sk,
                    outreach.channel as channel,
                    outreach.touched_at as touched_at
                ),
                null
            )
            ignore nulls
            order by outreach.touched_at
            limit 1
        )[safe_offset(0)].user_sk                                      as first_touch_user_sk,

        array_agg(
            if(
                outreach.channel = 'call',
                struct(
                    outreach.user_sk as user_sk,
                    outreach.touched_at as touched_at
                ),
                null
            )
            ignore nulls
            order by outreach.touched_at
            limit 1
        )[safe_offset(0)].user_sk                                      as first_call_user_sk,

        array_agg(
            if(
                outreach.channel = 'call'
                and lower(coalesce(outreach.message_status, '')) in ('answered', 'completed'),
                struct(
                    outreach.user_sk as user_sk,
                    outreach.touched_at as touched_at
                ),
                null
            )
            ignore nulls
            order by outreach.touched_at
            limit 1
        )[safe_offset(0)].user_sk                                      as first_successful_call_user_sk

    from buyers
    left join {{ ref('fct_outreach') }} as outreach
        on outreach.contact_sk = buyers.contact_sk
       and outreach.touched_at <= buyers.first_purchase_at
    group by 1

),

latest_booking_before_purchase as (

    select
        buyers.contact_sk,
        bookings.booking_sk                                           as latest_booking_sk,
        bookings.calendly_event_id                                    as latest_booking_event_id,
        bookings.event_name                                           as latest_booking_event_name,
        bookings.booked_at                                            as latest_booking_booked_at,
        bookings.scheduled_for                                        as latest_booking_scheduled_for,
        bookings.event_status                                         as latest_booking_status,
        bookings.assigned_user_sk                                     as latest_booking_assigned_user_sk
    from buyers
    inner join {{ ref('fct_calls_booked') }} as bookings
        on bookings.contact_sk = buyers.contact_sk
       and bookings.booked_at <= buyers.first_purchase_at
    qualify row_number() over (
        partition by buyers.contact_sk
        order by bookings.booked_at desc, bookings.booking_sk desc
    ) = 1

),

users as (

    select
        user_sk,
        user_id,
        name,
        email,
        role
    from {{ ref('dim_users') }}

),

fathom_call_invitees as (

    select
        call_id,
        participant_email_norm,
        is_external
    from {{ ref('stg_fathom__call_invitees') }}

),

operator_identity_aliases as (

    select
        nullif(lower(trim(alias_email)), '')                           as alias_email_norm,
        nullif(canonical_user_id, '')                                  as canonical_user_id,
        display_name,
        role,
        alias_type,
        confidence
    from {{ ref('operator_identity_aliases') }}

),

operator_identities as (

    select
        lower(trim(email))                                             as operator_email_norm,
        user_sk,
        user_id,
        name,
        role,
        'dim_users'                                                    as identity_type,
        'high'                                                         as identity_confidence
    from users
    where email is not null

    union all

    select
        operator_identity_aliases.alias_email_norm                     as operator_email_norm,
        users.user_sk,
        coalesce(
            operator_identity_aliases.canonical_user_id,
            users.user_id
        )                                                             as user_id,
        coalesce(
            users.name,
            operator_identity_aliases.display_name
        )                                                             as name,
        coalesce(users.role, operator_identity_aliases.role)           as role,
        operator_identity_aliases.alias_type                           as identity_type,
        operator_identity_aliases.confidence                           as identity_confidence
    from operator_identity_aliases
    left join users
        on operator_identity_aliases.canonical_user_id = users.user_id
    where operator_identity_aliases.alias_email_norm is not null

),

operator_speech_aliases as (

    select
        nullif(lower(trim(spoken_alias)), '')                          as spoken_alias_norm,
        nullif(canonical_user_id, '')                                  as canonical_user_id,
        display_name,
        role,
        alias_type,
        confidence
    from {{ ref('operator_speech_aliases') }}

),

operator_spoken_identities as (

    select
        lower(split(name, ' ')[safe_offset(0)])                        as spoken_alias_norm,
        user_sk,
        user_id,
        name,
        role,
        'dim_users_first_name'                                         as identity_type,
        'high'                                                         as identity_confidence
    from users
    where name is not null

    union all

    select
        operator_speech_aliases.spoken_alias_norm,
        users.user_sk,
        coalesce(
            operator_speech_aliases.canonical_user_id,
            users.user_id
        )                                                             as user_id,
        coalesce(
            users.name,
            operator_speech_aliases.display_name
        )                                                             as name,
        coalesce(users.role, operator_speech_aliases.role)             as role,
        operator_speech_aliases.alias_type                             as identity_type,
        operator_speech_aliases.confidence                             as identity_confidence
    from operator_speech_aliases
    left join users
        on operator_speech_aliases.canonical_user_id = users.user_id
    where operator_speech_aliases.spoken_alias_norm is not null

),

fathom_near_latest_booking as (

    select
        buyers.contact_sk,
        fathom.call_id                                             as latest_booking_fathom_call_id,
        fathom.scheduled_start_at                                  as latest_booking_fathom_scheduled_start_at,
        fathom.recorded_by_email                                   as latest_booking_fathom_recorded_by_email,
        fathom.recorded_by_name                                    as latest_booking_fathom_recorded_by_name,
        fathom.is_revenue_relevant                                 as latest_booking_fathom_is_revenue_relevant,
        abs(
            timestamp_diff(
                fathom.scheduled_start_at,
                latest_booking_before_purchase.latest_booking_scheduled_for,
                second
            )
        )                                                          as latest_booking_fathom_schedule_delta_seconds,
        operator_identities.user_sk                                as latest_booking_fathom_user_sk,
        operator_identities.user_id                                as latest_booking_fathom_user_id,
        coalesce(
            operator_identities.name,
            fathom.recorded_by_name
        )                                                          as latest_booking_fathom_user_name,
        coalesce(
            operator_identities.role,
            'unknown'
        )                                                          as latest_booking_fathom_user_role,
        operator_identities.identity_type                          as latest_booking_fathom_identity_type,
        operator_identities.identity_confidence                    as latest_booking_fathom_identity_confidence

    from buyers
    inner join latest_booking_before_purchase
        on buyers.contact_sk = latest_booking_before_purchase.contact_sk
    inner join {{ ref('stg_fathom__calls') }} as fathom
        on latest_booking_before_purchase.latest_booking_scheduled_for is not null
       and fathom.scheduled_start_at <= buyers.first_purchase_at
       and abs(
            timestamp_diff(
                fathom.scheduled_start_at,
                latest_booking_before_purchase.latest_booking_scheduled_for,
                minute
            )
        ) <= 15
    left join operator_identities
        on lower(trim(fathom.recorded_by_email))
           = operator_identities.operator_email_norm
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            fathom.is_revenue_relevant desc,
            operator_identities.role = 'Closer' desc,
            abs(
                timestamp_diff(
                    fathom.scheduled_start_at,
                    latest_booking_before_purchase.latest_booking_scheduled_for,
                    second
                )
            ),
            fathom.scheduled_start_at desc,
            fathom.call_id
    ) = 1

),

fathom_by_contact_email_before_purchase as (

    select
        buyers.contact_sk,
        fathom.call_id                                             as contact_email_fathom_call_id,
        fathom.scheduled_start_at                                  as contact_email_fathom_scheduled_start_at,
        fathom.recorded_by_email                                   as contact_email_fathom_recorded_by_email,
        fathom.recorded_by_name                                    as contact_email_fathom_recorded_by_name,
        fathom.is_revenue_relevant                                 as contact_email_fathom_is_revenue_relevant,
        timestamp_diff(
            buyers.first_purchase_at,
            fathom.scheduled_start_at,
            hour
        )                                                          as contact_email_fathom_hours_to_purchase,
        operator_identities.user_sk                                as contact_email_fathom_user_sk,
        operator_identities.user_id                                as contact_email_fathom_user_id,
        coalesce(
            operator_identities.name,
            fathom.recorded_by_name
        )                                                          as contact_email_fathom_user_name,
        coalesce(
            operator_identities.role,
            'unknown'
        )                                                          as contact_email_fathom_user_role,
        operator_identities.identity_type                          as contact_email_fathom_identity_type,
        operator_identities.identity_confidence                    as contact_email_fathom_identity_confidence

    from buyers
    inner join fathom_call_invitees
        on fathom_call_invitees.participant_email_norm = buyers.email_norm
       and fathom_call_invitees.is_external
    inner join {{ ref('stg_fathom__calls') }} as fathom
        on fathom.call_id = fathom_call_invitees.call_id
       and fathom.scheduled_start_at <= buyers.first_purchase_at
       and fathom.scheduled_start_at >= timestamp_sub(
            buyers.first_purchase_at,
            interval 90 day
        )
    left join operator_identities
        on lower(trim(fathom.recorded_by_email))
           = operator_identities.operator_email_norm
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            fathom.is_revenue_relevant desc,
            operator_identities.role = 'Closer' desc,
            fathom.scheduled_start_at desc,
            fathom.call_id desc
    ) = 1

),

calendly_host_by_contact_email_fathom as (

    select
        buyers.contact_sk,
        calendly_events.event_id                                    as contact_email_calendly_event_id,
        calendly_event_memberships.calendly_user_uri                as contact_email_calendly_host_user_uri,
        calendly_event_memberships.user_email                       as contact_email_calendly_host_email,
        calendly_event_memberships.user_email_norm                  as contact_email_calendly_host_email_norm,
        calendly_event_memberships.user_name                        as contact_email_calendly_host_name,
        abs(
            timestamp_diff(
                calendly_events.scheduled_for,
                fathom_by_contact_email_before_purchase.contact_email_fathom_scheduled_start_at,
                minute
            )
        )                                                          as contact_email_calendly_host_match_minutes,
        operator_identities.user_sk                                as contact_email_calendly_host_user_sk,
        operator_identities.user_id                                as contact_email_calendly_host_user_id,
        coalesce(
            operator_identities.name,
            calendly_event_memberships.user_name
        )                                                          as contact_email_calendly_host_user_name,
        coalesce(
            operator_identities.role,
            'unknown'
        )                                                          as contact_email_calendly_host_user_role,
        operator_identities.identity_type                          as contact_email_calendly_host_identity_type,
        operator_identities.identity_confidence                    as contact_email_calendly_host_identity_confidence

    from buyers
    inner join fathom_by_contact_email_before_purchase
        on buyers.contact_sk = fathom_by_contact_email_before_purchase.contact_sk
    inner join {{ ref('stg_calendly__event_invitees') }} as calendly_invitees
        on calendly_invitees.invitee_email_norm = buyers.email_norm
    inner join {{ ref('stg_calendly__events') }} as calendly_events
        on calendly_events.event_id = calendly_invitees.event_id
       and abs(
            timestamp_diff(
                calendly_events.scheduled_for,
                fathom_by_contact_email_before_purchase.contact_email_fathom_scheduled_start_at,
                minute
            )
        ) <= 120
    inner join {{ ref('stg_calendly__event_memberships') }} as calendly_event_memberships
        on calendly_event_memberships.event_id = calendly_events.event_id
    left join operator_identities
        on calendly_event_memberships.user_email_norm
           = operator_identities.operator_email_norm
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            abs(
                timestamp_diff(
                    calendly_events.scheduled_for,
                    fathom_by_contact_email_before_purchase.contact_email_fathom_scheduled_start_at,
                    minute
                )
            ),
            operator_identities.role = 'Closer' desc,
            calendly_events.booked_at desc,
            calendly_event_memberships.calendly_user_uri
    ) = 1

),

latest_booking_calendly_host as (

    select
        buyers.contact_sk,
        calendly_event_memberships.calendly_user_uri                as latest_booking_calendly_host_user_uri,
        calendly_event_memberships.user_email                       as latest_booking_calendly_host_email,
        calendly_event_memberships.user_email_norm                  as latest_booking_calendly_host_email_norm,
        calendly_event_memberships.user_name                        as latest_booking_calendly_host_name,
        operator_identities.user_sk                                as latest_booking_calendly_host_user_sk,
        operator_identities.user_id                                as latest_booking_calendly_host_user_id,
        coalesce(
            operator_identities.name,
            calendly_event_memberships.user_name
        )                                                          as latest_booking_calendly_host_user_name,
        coalesce(
            operator_identities.role,
            'unknown'
        )                                                          as latest_booking_calendly_host_user_role,
        operator_identities.identity_type                          as latest_booking_calendly_host_identity_type,
        operator_identities.identity_confidence                    as latest_booking_calendly_host_identity_confidence
    from buyers
    inner join latest_booking_before_purchase
        on buyers.contact_sk = latest_booking_before_purchase.contact_sk
    inner join {{ ref('stg_calendly__event_memberships') }} as calendly_event_memberships
        on latest_booking_before_purchase.latest_booking_event_id
           = calendly_event_memberships.event_id
    left join operator_identities
        on calendly_event_memberships.user_email_norm
           = operator_identities.operator_email_norm
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            operator_identities.role = 'Closer' desc,
            case operator_identities.identity_confidence
                when 'high' then 3
                when 'medium' then 2
                when 'low' then 1
                else 0
            end desc,
            calendly_event_memberships.calendly_user_uri
    ) = 1

),

latest_booking_event_name_operator as (

    select
        buyers.contact_sk,
        regexp_extract(
            lower(latest_booking_before_purchase.latest_booking_event_name),
            r'\(([^)]+)\)\s*$'
        )                                                          as latest_booking_event_name_operator_alias,
        operator_spoken_identities.user_sk                         as latest_booking_event_name_operator_user_sk,
        operator_spoken_identities.user_id                         as latest_booking_event_name_operator_user_id,
        operator_spoken_identities.name                            as latest_booking_event_name_operator_name,
        operator_spoken_identities.role                            as latest_booking_event_name_operator_role,
        operator_spoken_identities.identity_type                   as latest_booking_event_name_operator_identity_type,
        operator_spoken_identities.identity_confidence             as latest_booking_event_name_operator_identity_confidence
    from buyers
    inner join latest_booking_before_purchase
        on buyers.contact_sk = latest_booking_before_purchase.contact_sk
    left join operator_spoken_identities
        on regexp_extract(
            lower(latest_booking_before_purchase.latest_booking_event_name),
            r'\(([^)]+)\)\s*$'
        ) = operator_spoken_identities.spoken_alias_norm
    where latest_booking_before_purchase.latest_booking_event_name is not null
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            operator_spoken_identities.role = 'Closer' desc,
            case operator_spoken_identities.identity_confidence
                when 'high' then 3
                when 'medium' then 2
                when 'low' then 1
                else 0
            end desc,
            operator_spoken_identities.name
    ) = 1

),

fathom_transcript_self_intro_closer as (

    select
        buyers.contact_sk,
        transcript_segments.call_id                               as transcript_closer_call_id,
        transcript_segments.segment_index                         as transcript_closer_segment_index,
        transcript_segments.segment_text                          as transcript_closer_evidence_text,
        operator_spoken_identities.user_sk                        as transcript_closer_user_sk,
        operator_spoken_identities.user_id                        as transcript_closer_user_id,
        operator_spoken_identities.name                           as transcript_closer_user_name,
        operator_spoken_identities.role                           as transcript_closer_user_role,
        operator_spoken_identities.identity_type                  as transcript_closer_identity_type,
        operator_spoken_identities.identity_confidence            as transcript_closer_identity_confidence
    from buyers
    inner join fathom_by_contact_email_before_purchase
        on buyers.contact_sk = fathom_by_contact_email_before_purchase.contact_sk
       and fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
       and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
    inner join {{ ref('stg_fathom__transcript_segments') }} as transcript_segments
        on fathom_by_contact_email_before_purchase.contact_email_fathom_call_id
           = transcript_segments.call_id
       and transcript_segments.speaker_email_norm = lower(trim(
            fathom_by_contact_email_before_purchase.contact_email_fathom_recorded_by_email
        ))
       and transcript_segments.segment_index < 200
    inner join operator_spoken_identities
        on operator_spoken_identities.role = 'Closer'
       and regexp_contains(
            lower(transcript_segments.segment_text),
            concat(
                r'\b(my name.?s|name is|this is|it.?s|i.?m)\s+',
                operator_spoken_identities.spoken_alias_norm,
                r'\b'
            )
        )
    qualify row_number() over (
        partition by buyers.contact_sk
        order by
            operator_spoken_identities.identity_confidence = 'high' desc,
            transcript_segments.segment_index,
            operator_spoken_identities.name
    ) = 1

),

assembled as (

    select
        buyers.contact_sk,
        buyers.contact_id,
        buyers.email_norm,
        buyers.contact_name,
        buyers.phone,
        buyers.contact_created_at,

        first_payments.first_payment_id,
        first_payments.first_payment_source_platform,
        first_payments.first_purchase_at,
        first_payments.first_purchase_product,
        case
            when regexp_contains(lower(coalesce(first_payments.first_purchase_product, '')), r'inner\s+cirlce|inner circle|ic\s*2')
                then 'Inner Circle'
            when regexp_contains(lower(coalesce(first_payments.first_purchase_product, '')), r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)')
                then 'Rich Off Clothes'
            when regexp_contains(lower(coalesce(first_payments.first_purchase_product, '')), r'brand scaling|blueprint|accelerator')
                then 'Brand Scaling'
            when regexp_contains(lower(coalesce(first_payments.first_purchase_product, '')), r'tgf')
                then 'TGF'
            when first_payments.first_purchase_product is null
                or lower(first_payments.first_purchase_product) in (
                    'subscription creation',
                    'subscription update'
                )
                then 'Unknown / historical Stripe'
            else initcap(first_payments.first_purchase_product)
        end                                                           as first_purchase_product_family,
        first_payments.first_purchase_payment_method,
        first_payments.first_purchase_card_issue_country,
        first_payments.first_purchase_gross_amount,
        first_payments.first_purchase_net_revenue,

        latest_payments.latest_payment_id,
        latest_payments.latest_payment_source_platform,
        latest_payments.latest_purchase_at,
        latest_payments.latest_purchase_product,
        latest_payments.latest_purchase_payment_method,
        latest_payments.latest_payment_net_revenue,

        payment_summary.paid_payments_count,
        payment_summary.payment_source_platform_count,
        payment_summary.stripe_payments_count,
        payment_summary.fanbasis_payments_count,
        payment_summary.fanbasis_upfront_payments_count,
        payment_summary.fanbasis_auto_renew_payments_count,
        payment_summary.fanbasis_service_payment_ids_count,
        payment_summary.fanbasis_gross_revenue,
        payment_summary.fanbasis_net_revenue_after_refunds,
        payment_summary.fanbasis_refunded_payments_count,
        payment_summary.fanbasis_released_payments_count,
        payment_summary.fanbasis_unreleased_payments_count,
        payment_summary.latest_fanbasis_fund_release_on,
        payment_summary.plan_named_payments_count,
        payment_summary.purchased_product_count,
        payment_summary.total_gross_revenue,
        payment_summary.total_net_revenue_after_refunds,
        payment_summary.total_refunds_amount,
        payment_summary.total_refunds_count,
        safe_divide(
            payment_summary.total_net_revenue_after_refunds,
            payment_summary.paid_payments_count
        )                                                             as average_net_revenue_per_payment,
        payment_summary.purchased_products,

        top_products.top_product_by_net_revenue,
        case
            when regexp_contains(lower(coalesce(top_products.top_product_by_net_revenue, '')), r'inner\s+cirlce|inner circle|ic\s*2')
                then 'Inner Circle'
            when regexp_contains(lower(coalesce(top_products.top_product_by_net_revenue, '')), r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)')
                then 'Rich Off Clothes'
            when regexp_contains(lower(coalesce(top_products.top_product_by_net_revenue, '')), r'brand scaling|blueprint|accelerator')
                then 'Brand Scaling'
            when regexp_contains(lower(coalesce(top_products.top_product_by_net_revenue, '')), r'tgf')
                then 'TGF'
            when lower(top_products.top_product_by_net_revenue) in (
                'unknown',
                'subscription creation',
                'subscription update'
            )
                then 'Unknown / historical Stripe'
            else initcap(top_products.top_product_by_net_revenue)
        end                                                           as top_product_family,
        top_products.top_product_net_revenue,
        top_products.top_product_payments_count,

        payment_summary.paid_payments_count > 1                       as is_multi_payment_buyer,
        (
            payment_summary.paid_payments_count > 1
            or payment_summary.fanbasis_auto_renew_payments_count > 0
            or payment_summary.plan_named_payments_count > 0
        )                                                             as is_payment_plan_buyer,
        case
            when payment_summary.fanbasis_auto_renew_payments_count > 0
                then 'auto_renew_plan'
            when payment_summary.paid_payments_count > 1
                then 'multi_payment_plan'
            when payment_summary.plan_named_payments_count > 0
                then 'plan_named_single_payment'
            else 'single_payment'
        end                                                           as payment_plan_status,
        case
            when payment_summary.fanbasis_auto_renew_payments_count > 0
                then 'fanbasis_auto_renew_cash_only'
            when payment_summary.plan_named_payments_count > 0
                then 'name_inferred_plan_cash_only'
            when payment_summary.fanbasis_payments_count > 0
                then 'fanbasis_single_payment_cash'
            when payment_summary.stripe_payments_count > 0
                then 'historical_stripe_cash_only'
            else 'unknown_cash_only'
        end                                                           as payment_plan_truth_status,
        case
            when payment_summary.fanbasis_auto_renew_payments_count > 0
                then 'Fanbasis transaction rows show auto_renew payments, but subscriber schedule and remaining balance are not landed yet.'
            when payment_summary.plan_named_payments_count > 0
                then 'Product naming implies a plan, but the source currently exposes collected payments only.'
            when payment_summary.fanbasis_payments_count > 0
                then 'Fanbasis transaction rows support collected cash, product, and payout status.'
            when payment_summary.stripe_payments_count > 0
                then 'Historical Stripe cash is preserved; Fanbasis subscription schedule does not apply.'
            else 'No payment-plan source detail available.'
        end                                                           as payment_plan_truth_note,

        buyers.first_known_opportunity_id,
        buyers.first_known_lead_magnet_id,
        buyers.first_known_lead_magnet_name,
        buyers.first_known_lead_magnet_category,
        buyers.first_known_lead_magnet_offer_type,
        buyers.first_known_opportunity_created_at,

        buyers.latest_prior_opportunity_id,
        buyers.latest_prior_lead_magnet_id,
        buyers.latest_prior_lead_magnet_name,
        buyers.latest_prior_lead_magnet_category,
        buyers.latest_prior_lead_magnet_offer_type,
        buyers.latest_prior_is_true_lead_magnet,
        buyers.latest_prior_is_launch,
        buyers.latest_prior_is_waitlist,
        buyers.latest_prior_is_sales_pipeline,
        buyers.latest_prior_taxonomy_confidence,
        buyers.latest_prior_opportunity_created_at,
        buyers.latest_prior_contact_opportunity_sequence,
        buyers.has_latest_prior_magnet_before_first_purchase,
        buyers.has_any_known_magnet,
        buyers.first_known_magnet_after_first_purchase,
        buyers.days_latest_prior_magnet_to_first_purchase,
        buyers.days_first_known_magnet_to_first_purchase,
        buyers.purchase_magnet_attribution_flag,

        latest_prior_opportunities.assigned_user_sk                   as latest_prior_assigned_user_sk,
        latest_prior_opportunities.assigned_user_id                   as latest_prior_assigned_user_id,
        latest_prior_opportunities.assigned_user_name                 as latest_prior_assigned_user_name,
        latest_prior_opportunities.assigned_user_role                 as latest_prior_assigned_user_role,
        coalesce(latest_prior_opportunities.touches_count, 0)         as latest_prior_window_touches_count,
        coalesce(latest_prior_opportunities.call_count, 0)            as latest_prior_window_call_count,
        coalesce(latest_prior_opportunities.sms_count, 0)             as latest_prior_window_sms_count,
        coalesce(latest_prior_opportunities.successful_call_count, 0) as latest_prior_window_successful_call_count,
        coalesce(latest_prior_opportunities.direct_bookings_count, 0) as latest_prior_window_direct_bookings_count,
        coalesce(latest_prior_opportunities.window_bookings_count, 0) as latest_prior_window_bookings_count,
        coalesce(latest_prior_opportunities.payment_count, 0)         as latest_prior_window_payment_count,
        coalesce(latest_prior_opportunities.net_revenue_after_refunds, 0)
                                                                        as latest_prior_window_net_revenue,

        buyers.bookings_before_first_purchase_count,
        buyers.active_bookings_before_first_purchase_count,
        buyers.canceled_bookings_before_first_purchase_count,
        buyers.first_booking_before_first_purchase_at,
        buyers.latest_booking_before_first_purchase_at,
        buyers.has_booking_before_first_purchase,
        buyers.has_active_booking_before_first_purchase,

        latest_booking_before_purchase.latest_booking_sk,
        latest_booking_before_purchase.latest_booking_event_id,
        latest_booking_before_purchase.latest_booking_event_name,
        latest_booking_before_purchase.latest_booking_booked_at,
        latest_booking_before_purchase.latest_booking_scheduled_for,
        latest_booking_before_purchase.latest_booking_status,
        latest_booking_before_purchase.latest_booking_assigned_user_sk,
        latest_booking_user.user_id                                   as latest_booking_assigned_user_id,
        latest_booking_user.name                                      as latest_booking_assigned_user_name,
        latest_booking_user.role                                      as latest_booking_assigned_user_role,

        fathom_near_latest_booking.latest_booking_fathom_call_id,
        fathom_near_latest_booking.latest_booking_fathom_scheduled_start_at,
        fathom_near_latest_booking.latest_booking_fathom_recorded_by_email,
        fathom_near_latest_booking.latest_booking_fathom_recorded_by_name,
        fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant,
        fathom_near_latest_booking.latest_booking_fathom_schedule_delta_seconds,
        fathom_near_latest_booking.latest_booking_fathom_user_sk,
        fathom_near_latest_booking.latest_booking_fathom_user_id,
        fathom_near_latest_booking.latest_booking_fathom_user_name,
        fathom_near_latest_booking.latest_booking_fathom_user_role,
        fathom_near_latest_booking.latest_booking_fathom_identity_type,
        fathom_near_latest_booking.latest_booking_fathom_identity_confidence,

        fathom_by_contact_email_before_purchase.contact_email_fathom_call_id,
        fathom_by_contact_email_before_purchase.contact_email_fathom_scheduled_start_at,
        fathom_by_contact_email_before_purchase.contact_email_fathom_recorded_by_email,
        fathom_by_contact_email_before_purchase.contact_email_fathom_recorded_by_name,
        fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant,
        fathom_by_contact_email_before_purchase.contact_email_fathom_hours_to_purchase,
        fathom_by_contact_email_before_purchase.contact_email_fathom_user_sk,
        fathom_by_contact_email_before_purchase.contact_email_fathom_user_id,
        fathom_by_contact_email_before_purchase.contact_email_fathom_user_name,
        fathom_by_contact_email_before_purchase.contact_email_fathom_user_role,
        fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type,
        fathom_by_contact_email_before_purchase.contact_email_fathom_identity_confidence,

        calendly_host_by_contact_email_fathom.contact_email_calendly_event_id,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_uri,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_email,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_email_norm,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_name,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_match_minutes,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_sk,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_id,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_role,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_identity_type,
        calendly_host_by_contact_email_fathom.contact_email_calendly_host_identity_confidence,

        fathom_transcript_self_intro_closer.transcript_closer_call_id,
        fathom_transcript_self_intro_closer.transcript_closer_segment_index,
        fathom_transcript_self_intro_closer.transcript_closer_evidence_text,
        fathom_transcript_self_intro_closer.transcript_closer_user_sk,
        fathom_transcript_self_intro_closer.transcript_closer_user_id,
        fathom_transcript_self_intro_closer.transcript_closer_user_name,
        fathom_transcript_self_intro_closer.transcript_closer_user_role,
        fathom_transcript_self_intro_closer.transcript_closer_identity_type,
        fathom_transcript_self_intro_closer.transcript_closer_identity_confidence,

        latest_booking_event_name_operator.latest_booking_event_name_operator_alias,
        latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk,
        latest_booking_event_name_operator.latest_booking_event_name_operator_user_id,
        latest_booking_event_name_operator.latest_booking_event_name_operator_name,
        latest_booking_event_name_operator.latest_booking_event_name_operator_role,
        latest_booking_event_name_operator.latest_booking_event_name_operator_identity_type,
        latest_booking_event_name_operator.latest_booking_event_name_operator_identity_confidence,

        latest_booking_calendly_host.latest_booking_calendly_host_user_uri,
        latest_booking_calendly_host.latest_booking_calendly_host_email,
        latest_booking_calendly_host.latest_booking_calendly_host_email_norm,
        latest_booking_calendly_host.latest_booking_calendly_host_name,
        latest_booking_calendly_host.latest_booking_calendly_host_user_sk,
        latest_booking_calendly_host.latest_booking_calendly_host_user_id,
        latest_booking_calendly_host.latest_booking_calendly_host_user_name,
        latest_booking_calendly_host.latest_booking_calendly_host_user_role,
        latest_booking_calendly_host.latest_booking_calendly_host_identity_type,
        latest_booking_calendly_host.latest_booking_calendly_host_identity_confidence,

        coalesce(outreach_before_first_purchase.touches_before_purchase_count, 0)
                                                                        as touches_before_purchase_count,
        coalesce(outreach_before_first_purchase.calls_before_purchase_count, 0)
                                                                        as calls_before_purchase_count,
        coalesce(outreach_before_first_purchase.sms_before_purchase_count, 0)
                                                                        as sms_before_purchase_count,
        coalesce(outreach_before_first_purchase.successful_calls_before_purchase_count, 0)
                                                                        as successful_calls_before_purchase_count,
        outreach_before_first_purchase.first_touch_before_purchase_at,
        outreach_before_first_purchase.first_call_before_purchase_at,
        outreach_before_first_purchase.first_successful_call_before_purchase_at,

        outreach_before_first_purchase.first_touch_user_sk,
        first_touch_user.user_id                                      as first_touch_user_id,
        first_touch_user.name                                         as first_touch_user_name,
        first_touch_user.role                                         as first_touch_user_role,

        outreach_before_first_purchase.first_call_user_sk,
        first_call_user.user_id                                       as first_call_user_id,
        first_call_user.name                                          as first_call_user_name,
        first_call_user.role                                          as first_call_user_role,

        outreach_before_first_purchase.first_successful_call_user_sk,
        first_successful_call_user.user_id                            as first_successful_call_user_id,
        first_successful_call_user.name                               as first_successful_call_user_name,
        first_successful_call_user.role                               as first_successful_call_user_role,

        timestamp_diff(
            first_payments.first_purchase_at,
            outreach_before_first_purchase.first_touch_before_purchase_at,
            hour
        )                                                             as hours_first_touch_to_purchase,
        timestamp_diff(
            first_payments.first_purchase_at,
            outreach_before_first_purchase.first_successful_call_before_purchase_at,
            hour
        )                                                             as hours_first_successful_call_to_purchase,
        timestamp_diff(
            first_payments.first_purchase_at,
            buyers.latest_booking_before_first_purchase_at,
            hour
        )                                                             as hours_latest_booking_to_purchase,

        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then latest_prior_opportunities.assigned_user_sk
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_sk
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then fathom_near_latest_booking.latest_booking_fathom_user_sk
            when latest_booking_user.role = 'Closer'
                then latest_booking_user.user_sk
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then fathom_transcript_self_intro_closer.transcript_closer_user_sk
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then latest_booking_calendly_host.latest_booking_calendly_host_user_sk
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_sk
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_sk
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type != 'team_account'
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_sk
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then fathom_near_latest_booking.latest_booking_fathom_user_sk
            when latest_booking_user.user_sk is not null
                then latest_booking_user.user_sk
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then latest_booking_calendly_host.latest_booking_calendly_host_user_sk
        end                                                           as credited_closer_user_sk,
        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then latest_prior_opportunities.assigned_user_id
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_id
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then fathom_near_latest_booking.latest_booking_fathom_user_id
            when latest_booking_user.role = 'Closer'
                then latest_booking_user.user_id
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then fathom_transcript_self_intro_closer.transcript_closer_user_id
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_id
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then latest_booking_calendly_host.latest_booking_calendly_host_user_id
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_id
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_id
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type != 'team_account'
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_id
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then fathom_near_latest_booking.latest_booking_fathom_user_id
            when latest_booking_user.user_sk is not null
                then latest_booking_user.user_id
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_id
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then latest_booking_calendly_host.latest_booking_calendly_host_user_id
        end                                                           as credited_closer_user_id,
        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then latest_prior_opportunities.assigned_user_name
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_name
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then fathom_near_latest_booking.latest_booking_fathom_user_name
            when latest_booking_user.role = 'Closer'
                then latest_booking_user.name
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then fathom_transcript_self_intro_closer.transcript_closer_user_name
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then latest_booking_event_name_operator.latest_booking_event_name_operator_name
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then latest_booking_calendly_host.latest_booking_calendly_host_user_name
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_name
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_name is not null
                and fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type != 'team_account'
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_name
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then fathom_near_latest_booking.latest_booking_fathom_user_name
            when latest_booking_user.user_sk is not null
                then latest_booking_user.name
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_name
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then latest_booking_calendly_host.latest_booking_calendly_host_user_name
            else 'Unassigned / unknown'
        end                                                           as credited_closer_name,
        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then latest_prior_opportunities.assigned_user_role
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_role
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then fathom_near_latest_booking.latest_booking_fathom_user_role
            when latest_booking_user.role = 'Closer'
                then latest_booking_user.role
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then fathom_transcript_self_intro_closer.transcript_closer_user_role
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then latest_booking_event_name_operator.latest_booking_event_name_operator_role
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then latest_booking_calendly_host.latest_booking_calendly_host_user_role
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_role
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_role
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_name is not null
                and fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type != 'team_account'
                then fathom_by_contact_email_before_purchase.contact_email_fathom_user_role
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then fathom_near_latest_booking.latest_booking_fathom_user_role
            when latest_booking_user.user_sk is not null
                then latest_booking_user.role
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_role
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then latest_booking_calendly_host.latest_booking_calendly_host_user_role
            else 'unknown'
        end                                                           as credited_closer_role,
        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then 'latest_prior_opportunity_closer'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then 'fathom_contact_email_revenue_call_recorder'
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then 'fathom_revenue_call_recorder'
            when latest_booking_user.role = 'Closer'
                then 'latest_booking_closer'
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then 'fathom_transcript_self_intro_closer'
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then 'latest_booking_event_name_closer'
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then 'latest_booking_calendly_host_closer'
            when latest_prior_opportunities.assigned_user_sk is not null
                then 'latest_prior_opportunity_owner'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then 'calendly_host_for_fathom_team_account'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                then 'fathom_contact_email_team_account'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_name is not null
                then 'fathom_contact_email_recorder'
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then 'fathom_call_recorder'
            when latest_booking_user.user_sk is not null
                then 'latest_booking_owner'
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then 'latest_booking_event_name_operator'
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then 'latest_booking_calendly_host'
            else 'unassigned'
        end                                                           as credited_closer_source,
        case
            when latest_prior_opportunities.assigned_user_role = 'Closer'
                then 'high'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_role = 'Closer'
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then 'high'
            when fathom_near_latest_booking.latest_booking_fathom_user_role = 'Closer'
                and fathom_near_latest_booking.latest_booking_fathom_is_revenue_relevant
                then 'high'
            when latest_booking_user.role = 'Closer'
                then 'medium'
            when fathom_transcript_self_intro_closer.transcript_closer_user_role = 'Closer'
                then coalesce(
                    fathom_transcript_self_intro_closer.transcript_closer_identity_confidence,
                    'medium'
                )
            when latest_booking_event_name_operator.latest_booking_event_name_operator_role = 'Closer'
                then 'medium'
            when latest_booking_calendly_host.latest_booking_calendly_host_user_role = 'Closer'
                then coalesce(
                    latest_booking_calendly_host.latest_booking_calendly_host_identity_confidence,
                    'medium'
                )
            when latest_prior_opportunities.assigned_user_sk is not null
                then 'medium'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                and calendly_host_by_contact_email_fathom.contact_email_calendly_host_user_name is not null
                then coalesce(
                    calendly_host_by_contact_email_fathom.contact_email_calendly_host_identity_confidence,
                    'low'
                )
            when fathom_by_contact_email_before_purchase.contact_email_fathom_identity_type = 'team_account'
                then 'low'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_name is not null
                and fathom_by_contact_email_before_purchase.contact_email_fathom_is_revenue_relevant
                then 'medium'
            when fathom_by_contact_email_before_purchase.contact_email_fathom_user_name is not null
                then 'low'
            when fathom_near_latest_booking.latest_booking_fathom_user_sk is not null
                then 'medium'
            when latest_booking_user.user_sk is not null
                then 'low'
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then coalesce(
                    latest_booking_event_name_operator.latest_booking_event_name_operator_identity_confidence,
                    'low'
                )
            when latest_booking_calendly_host.latest_booking_calendly_host_user_name is not null
                then 'low'
            else 'missing'
        end                                                           as credited_closer_confidence,

        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.user_sk
            when first_touch_user.user_sk is not null
                then first_touch_user.user_sk
        end                                                           as credited_setter_user_sk,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.user_id
            when first_touch_user.user_sk is not null
                then first_touch_user.user_id
        end                                                           as credited_setter_user_id,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.name
            when first_touch_user.user_sk is not null
                then first_touch_user.name
            else 'Unassigned / unknown'
        end                                                           as credited_setter_name,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.role
            when first_touch_user.user_sk is not null
                then first_touch_user.role
            else 'unknown'
        end                                                           as credited_setter_role,
        case
            when first_successful_call_user.user_sk is not null
                then 'first_successful_call_before_purchase'
            when first_touch_user.user_sk is not null
                then 'first_touch_before_purchase'
            else 'unassigned'
        end                                                           as credited_setter_source,

        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.user_sk
            when first_touch_user.user_sk is not null
                then first_touch_user.user_sk
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_sk
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk
            when latest_booking_user.user_sk is not null
                then latest_booking_user.user_sk
        end                                                           as best_available_operator_user_sk,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.user_id
            when first_touch_user.user_sk is not null
                then first_touch_user.user_id
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_id
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_user_id
            when latest_booking_user.user_sk is not null
                then latest_booking_user.user_id
        end                                                           as best_available_operator_user_id,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.name
            when first_touch_user.user_sk is not null
                then first_touch_user.name
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_name
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_name
            when latest_booking_user.user_sk is not null
                then latest_booking_user.name
            else 'Unassigned / unknown'
        end                                                           as best_available_operator_name,
        case
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.role
            when first_touch_user.user_sk is not null
                then first_touch_user.role
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_role
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then latest_booking_event_name_operator.latest_booking_event_name_operator_role
            when latest_booking_user.user_sk is not null
                then latest_booking_user.role
            else 'unknown'
        end                                                           as best_available_operator_role,
        case
            when first_successful_call_user.user_sk is not null
                then 'first_successful_call_before_purchase'
            when first_touch_user.user_sk is not null
                then 'first_touch_before_purchase'
            when latest_prior_opportunities.assigned_user_sk is not null
                then 'latest_prior_opportunity_owner'
            when latest_booking_event_name_operator.latest_booking_event_name_operator_user_sk is not null
                then 'latest_booking_event_name_operator'
            when latest_booking_user.user_sk is not null
                then 'latest_booking_owner'
            else 'unassigned'
        end                                                           as best_available_operator_source,

        case
            when buyers.has_active_booking_before_first_purchase
                then 'booked_before_purchase'
            when buyers.has_booking_before_first_purchase
                then 'canceled_booking_before_purchase'
            when outreach_before_first_purchase.first_successful_call_before_purchase_at is not null
                then 'reached_by_phone_before_purchase'
            when coalesce(outreach_before_first_purchase.touches_before_purchase_count, 0) > 0
                then 'worked_before_purchase'
            when buyers.has_latest_prior_magnet_before_first_purchase
                then 'magnet_before_purchase_no_work_logged'
            else 'buyer_without_known_pre_purchase_path'
        end                                                           as pre_purchase_funnel_path,

        buyers.attribution_era,
        buyers.utm_source,
        buyers.utm_medium,
        buyers.utm_campaign,

        case
            when buyers.purchase_magnet_attribution_flag in (
                'missing_taxonomy',
                'uncategorized_offer_type'
            )
                then buyers.purchase_magnet_attribution_flag
            when payment_summary.total_net_revenue_after_refunds < 0
                then 'negative_net_revenue'
            when buyers.contact_id is null
                then 'contact_not_matched'
            when buyers.purchase_magnet_attribution_flag = 'no_known_magnet'
                then 'no_known_magnet'
            else 'clean'
        end                                                           as revenue_funnel_quality_flag,

        current_timestamp()                                           as mart_refreshed_at

    from buyers
    inner join first_payments
        on buyers.contact_sk = first_payments.contact_sk
    inner join latest_payments
        on buyers.contact_sk = latest_payments.contact_sk
    inner join payment_summary
        on buyers.contact_sk = payment_summary.contact_sk
    left join top_products
        on buyers.contact_sk = top_products.contact_sk
    left join latest_prior_opportunities
        on buyers.latest_prior_opportunity_id = latest_prior_opportunities.opportunity_id
    left join outreach_before_first_purchase
        on buyers.contact_sk = outreach_before_first_purchase.contact_sk
    left join latest_booking_before_purchase
        on buyers.contact_sk = latest_booking_before_purchase.contact_sk
    left join fathom_near_latest_booking
        on buyers.contact_sk = fathom_near_latest_booking.contact_sk
    left join fathom_by_contact_email_before_purchase
        on buyers.contact_sk = fathom_by_contact_email_before_purchase.contact_sk
    left join calendly_host_by_contact_email_fathom
        on buyers.contact_sk = calendly_host_by_contact_email_fathom.contact_sk
    left join fathom_transcript_self_intro_closer
        on buyers.contact_sk = fathom_transcript_self_intro_closer.contact_sk
    left join latest_booking_event_name_operator
        on buyers.contact_sk = latest_booking_event_name_operator.contact_sk
    left join latest_booking_calendly_host
        on buyers.contact_sk = latest_booking_calendly_host.contact_sk
    left join users as latest_booking_user
        on latest_booking_before_purchase.latest_booking_assigned_user_sk
           = latest_booking_user.user_sk
    left join users as first_touch_user
        on outreach_before_first_purchase.first_touch_user_sk
           = first_touch_user.user_sk
    left join users as first_call_user
        on outreach_before_first_purchase.first_call_user_sk
           = first_call_user.user_sk
    left join users as first_successful_call_user
        on outreach_before_first_purchase.first_successful_call_user_sk
           = first_successful_call_user.user_sk

),

final as (

    select * from assembled

)

select * from final
