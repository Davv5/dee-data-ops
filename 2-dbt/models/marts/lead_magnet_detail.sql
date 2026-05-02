{{
    config(
        materialized = 'table',
    )
}}

-- Opportunity-grain lead magnet mart.
--
-- At D-DEE each GHL pipeline is approximately one lead-magnet funnel
-- (source: `stg_ghl__pipelines` model docs, grounded in the three-layer
-- architecture transcript: marts present business-friendly contracts).
--
-- Revenue, bookings, and outreach are attributed inside each contact's
-- opportunity window: opportunity_created_at <= event < next_opportunity_created_at.
-- This avoids double-counting contacts who opted into multiple magnets.

with

opportunities as (

    select
        opp.opportunity_id,
        opp.opportunity_name,
        opp.contact_id,
        contacts.contact_sk,
        contacts.email_norm,
        contacts.contact_name,
        contacts.phone,
        contacts.contact_created_at,
        contacts.attribution_era,
        contacts.utm_source,
        contacts.utm_medium,
        contacts.utm_campaign,

        opp.assigned_user_id,
        users.user_sk                                                   as assigned_user_sk,
        users.name                                                      as assigned_user_name,
        users.role                                                      as assigned_user_role,

        opp.pipeline_id                                                 as lead_magnet_id,
        stages.pipeline_name                                            as lead_magnet_name,
        stages.pipeline_stage_sk,
        opp.pipeline_stage_id                                           as lead_magnet_stage_id,
        stages.stage_name                                               as lead_magnet_stage_name,
        stages.stage_position                                           as lead_magnet_stage_position,
        stages.is_booked_stage,
        stages.show_in_funnel,

        opp.status                                                      as opportunity_status,
        opp.lead_source,
        opp.lost_reason_id,
        opp.monetary_value,
        opp.forecast_probability,
        opp.effective_probability,
        opp.opportunity_created_at,
        opp.opportunity_updated_at,
        opp.last_status_change_at,
        opp.last_stage_change_at,

        row_number() over (
            partition by opp.contact_id
            order by opp.opportunity_created_at, opp.opportunity_id
        )                                                               as contact_opportunity_sequence,

        row_number() over (
            partition by opp.contact_id
            order by opp.opportunity_created_at desc, opp.opportunity_id desc
        )                                                               as contact_opportunity_recency,

        count(*) over (
            partition by opp.contact_id
        )                                                               as contact_opportunity_count,

        lead(opp.opportunity_created_at) over (
            partition by opp.contact_id
            order by opp.opportunity_created_at, opp.opportunity_id
        )                                                               as next_opportunity_created_at

    from {{ ref('stg_ghl__opportunities') }} as opp
    left join {{ ref('dim_contacts') }} as contacts
        on contacts.contact_id = opp.contact_id
    left join {{ ref('dim_pipeline_stages') }} as stages
        on stages.pipeline_id = opp.pipeline_id
       and stages.stage_id    = opp.pipeline_stage_id
    left join {{ ref('dim_users') }} as users
        on users.user_id = opp.assigned_user_id

),

refunds_per_payment as (

    select
        source_platform,
        parent_payment_id,
        sum(refund_amount)                                             as refunds_total_amount
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
        case
            when payments.source_platform = 'stripe'
                then payments.net_amount
            else
                payments.net_amount
                - coalesce(refunds_per_payment.refunds_total_amount, 0)
        end                                                           as net_amount_after_refunds
    from {{ ref('fct_payments') }} as payments
    left join refunds_per_payment
        on payments.source_platform = refunds_per_payment.source_platform
       and payments.payment_id      = refunds_per_payment.parent_payment_id
    where payments.is_paid = true
        and payments.contact_sk is not null

),

outreach_per_opportunity as (

    select
        opportunities.opportunity_id,
        count(outreach.touch_sk)                                       as touches_count,
        countif(outreach.channel = 'call')                             as call_count,
        countif(outreach.channel = 'sms')                              as sms_count,
        countif(
            outreach.channel = 'call'
            and lower(coalesce(outreach.message_status, '')) in ('answered', 'completed')
        )                                                              as successful_call_count,
        min(outreach.touched_at)                                       as first_touch_at,
        max(outreach.touched_at)                                       as latest_touch_at,
        min(
            if(
                outreach.channel = 'call',
                outreach.touched_at,
                null
            )
        )                                                              as first_call_at,
        min(
            if(
                outreach.channel = 'call'
                and lower(coalesce(outreach.message_status, '')) in ('answered', 'completed'),
                outreach.touched_at,
                null
            )
        )                                                              as first_successful_call_at
    from opportunities
    left join {{ ref('fct_outreach') }} as outreach
        on outreach.contact_sk = opportunities.contact_sk
       and outreach.touched_at >= opportunities.opportunity_created_at
       and (
            opportunities.next_opportunity_created_at is null
            or outreach.touched_at < opportunities.next_opportunity_created_at
       )
    group by opportunities.opportunity_id

),

bookings_per_opportunity as (

    select
        opportunities.opportunity_id,
        countif(bookings.booking_time_opportunity_id = opportunities.opportunity_id)
                                                                        as direct_bookings_count,
        count(bookings.booking_sk)                                      as window_bookings_count,
        min(bookings.booked_at)                                         as first_booking_at,
        max(bookings.booked_at)                                         as latest_booking_at,
        countif(lower(coalesce(bookings.event_status, '')) in ('canceled', 'cancelled'))
                                                                        as canceled_bookings_count
    from opportunities
    left join {{ ref('fct_calls_booked') }} as bookings
        on bookings.contact_sk = opportunities.contact_sk
       and bookings.booked_at >= opportunities.opportunity_created_at
       and (
            opportunities.next_opportunity_created_at is null
            or bookings.booked_at < opportunities.next_opportunity_created_at
       )
    group by opportunities.opportunity_id

),

payments_per_opportunity as (

    select
        opportunities.opportunity_id,
        count(payments_net.payment_id)                                  as payment_count,
        sum(coalesce(payments_net.gross_amount, 0))                     as gross_revenue,
        sum(coalesce(payments_net.net_amount_after_refunds, 0))         as net_revenue_after_refunds,
        min(payments_net.transaction_date)                              as first_payment_at,
        max(payments_net.transaction_date)                              as latest_payment_at
    from opportunities
    left join payments_net
        on payments_net.contact_sk = opportunities.contact_sk
       and payments_net.transaction_date >= opportunities.opportunity_created_at
       and (
            opportunities.next_opportunity_created_at is null
            or payments_net.transaction_date < opportunities.next_opportunity_created_at
       )
    group by opportunities.opportunity_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['opportunities.opportunity_id']) }}
                                                                        as lead_magnet_opportunity_sk,

        opportunities.opportunity_id,
        opportunities.opportunity_name,
        opportunities.contact_id,
        opportunities.contact_sk,
        opportunities.email_norm,
        opportunities.contact_name,
        opportunities.phone,
        opportunities.contact_created_at,

        opportunities.lead_magnet_id,
        opportunities.lead_magnet_name,
        opportunities.pipeline_stage_sk,
        opportunities.lead_magnet_stage_id,
        opportunities.lead_magnet_stage_name,
        opportunities.lead_magnet_stage_position,
        coalesce(opportunities.show_in_funnel, false)                   as show_in_funnel,

        opportunities.opportunity_status,
        opportunities.lead_source,
        opportunities.lost_reason_id,
        opportunities.monetary_value,
        opportunities.forecast_probability,
        opportunities.effective_probability,
        opportunities.opportunity_created_at,
        opportunities.opportunity_updated_at,
        opportunities.last_status_change_at,
        opportunities.last_stage_change_at,
        opportunities.next_opportunity_created_at,

        opportunities.contact_opportunity_sequence,
        opportunities.contact_opportunity_recency,
        opportunities.contact_opportunity_count,
        opportunities.contact_opportunity_sequence = 1                  as is_first_opportunity_for_contact,
        opportunities.contact_opportunity_recency = 1                   as is_latest_opportunity_for_contact,
        opportunities.contact_opportunity_count > 1                     as is_multi_magnet_contact,

        opportunities.assigned_user_sk,
        opportunities.assigned_user_id,
        opportunities.assigned_user_name,
        opportunities.assigned_user_role,

        opportunities.attribution_era,
        opportunities.utm_source,
        opportunities.utm_medium,
        opportunities.utm_campaign,

        coalesce(outreach_per_opportunity.touches_count, 0)             as touches_count,
        coalesce(outreach_per_opportunity.call_count, 0)                as call_count,
        coalesce(outreach_per_opportunity.sms_count, 0)                 as sms_count,
        coalesce(outreach_per_opportunity.successful_call_count, 0)     as successful_call_count,
        outreach_per_opportunity.first_touch_at,
        outreach_per_opportunity.latest_touch_at,
        outreach_per_opportunity.first_call_at,
        outreach_per_opportunity.first_successful_call_at,

        coalesce(bookings_per_opportunity.direct_bookings_count, 0)     as direct_bookings_count,
        coalesce(bookings_per_opportunity.window_bookings_count, 0)     as window_bookings_count,
        bookings_per_opportunity.first_booking_at,
        bookings_per_opportunity.latest_booking_at,
        coalesce(bookings_per_opportunity.canceled_bookings_count, 0)   as canceled_bookings_count,

        coalesce(payments_per_opportunity.payment_count, 0)             as payment_count,
        coalesce(payments_per_opportunity.gross_revenue, 0)             as gross_revenue,
        coalesce(payments_per_opportunity.net_revenue_after_refunds, 0) as net_revenue_after_refunds,
        payments_per_opportunity.first_payment_at,
        payments_per_opportunity.latest_payment_at,

        coalesce(opportunities.is_booked_stage, false)                  as is_currently_booked_stage,
        lower(coalesce(opportunities.opportunity_status, '')) = 'won'   as is_won_status,
        lower(coalesce(opportunities.opportunity_status, '')) = 'lost'  as is_lost_status,
        lower(coalesce(opportunities.opportunity_status, '')) = 'abandoned'
                                                                        as is_abandoned_status,

        case
            when opportunities.contact_sk is null then 'contact_not_matched'
            when opportunities.lead_magnet_name is null then 'pipeline_not_mapped'
            when opportunities.contact_opportunity_count > 1 then 'multi_magnet_contact'
            else 'clean'
        end                                                            as attribution_quality_flag,

        current_timestamp()                                            as mart_refreshed_at

    from opportunities
    left join outreach_per_opportunity
        on outreach_per_opportunity.opportunity_id = opportunities.opportunity_id
    left join bookings_per_opportunity
        on bookings_per_opportunity.opportunity_id = opportunities.opportunity_id
    left join payments_per_opportunity
        on payments_per_opportunity.opportunity_id = opportunities.opportunity_id

)

select * from final
