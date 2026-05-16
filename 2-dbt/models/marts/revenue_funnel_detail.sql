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
        role
    from {{ ref('dim_users') }}

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
        latest_booking_before_purchase.latest_booking_booked_at,
        latest_booking_before_purchase.latest_booking_scheduled_for,
        latest_booking_before_purchase.latest_booking_status,
        latest_booking_before_purchase.latest_booking_assigned_user_sk,
        latest_booking_user.user_id                                   as latest_booking_assigned_user_id,
        latest_booking_user.name                                      as latest_booking_assigned_user_name,
        latest_booking_user.role                                      as latest_booking_assigned_user_role,

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
            when first_successful_call_user.user_sk is not null
                then first_successful_call_user.user_sk
            when first_touch_user.user_sk is not null
                then first_touch_user.user_sk
            when latest_prior_opportunities.assigned_user_sk is not null
                then latest_prior_opportunities.assigned_user_sk
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
