

-- Buyer-grain lead magnet mart.
--
-- One row per matched paid contact. This is intentionally separate from
-- `lead_magnet_detail` because buyers and opportunities are different grains.
-- Revenue here is buyer-level truth; lead-magnet fields are latest-known
-- opportunity context before the buyer's first purchase.

with

contacts as (

    select
        contact_sk,
        contact_id,
        email_norm,
        contact_name,
        phone,
        contact_created_at,
        attribution_era,
        utm_source,
        utm_medium,
        utm_campaign
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_contacts`

),

refunds_per_payment as (

    select
        source_platform,
        parent_payment_id,
        sum(refund_amount)                                             as refunds_total_amount
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_refunds`
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
        payments.payment_method,
        payments.card_issue_country,
        payments.match_method,
        payments.match_score,
        payments.bridge_status,

        case
            when payments.source_platform = 'stripe'
                then payments.net_amount
            else
                payments.net_amount
                - coalesce(refunds_per_payment.refunds_total_amount, 0)
        end                                                           as net_amount_after_refunds

    from `project-41542e21-470f-4589-96d`.`Core`.`fct_payments` as payments
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
        )                                                               as buyer_payment_sequence

    from payments_net

),

first_payments as (

    select
        contact_sk,
        payment_id                                                     as first_payment_id,
        source_platform                                                as first_payment_source_platform,
        transaction_date                                               as first_purchase_at,
        gross_amount                                                   as first_purchase_gross_amount,
        net_amount_after_refunds                                       as first_purchase_net_revenue,
        product                                                        as first_purchase_product,
        payment_method                                                 as first_purchase_payment_method,
        card_issue_country                                             as first_purchase_card_issue_country
    from ranked_payments
    where buyer_payment_sequence = 1

),

payment_summary as (

    select
        contact_sk,
        count(*)                                                       as paid_payments_count,
        count(distinct source_platform)                                as payment_source_platform_count,
        countif(source_platform = 'stripe')                            as stripe_payments_count,
        countif(source_platform = 'fanbasis')                          as fanbasis_payments_count,
        sum(gross_amount)                                              as total_gross_revenue,
        sum(net_amount_after_refunds)                                  as total_net_revenue_after_refunds,
        min(transaction_date)                                          as first_purchase_at,
        max(transaction_date)                                          as latest_purchase_at,
        string_agg(distinct coalesce(product, 'unknown'), ' | ')       as purchased_products
    from payments_net
    group by 1

),

first_known_magnets as (

    select
        contact_sk,
        opportunity_id                                                 as first_known_opportunity_id,
        lead_magnet_id                                                as first_known_lead_magnet_id,
        lead_magnet_reporting_name                                    as first_known_lead_magnet_name,
        lead_magnet_category                                          as first_known_lead_magnet_category,
        lead_magnet_offer_type                                        as first_known_lead_magnet_offer_type,
        opportunity_created_at                                        as first_known_opportunity_created_at
    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_detail`
    qualify row_number() over (
        partition by contact_sk
        order by opportunity_created_at, opportunity_id
    ) = 1

),

latest_prior_magnets as (

    select
        first_payments.contact_sk,
        lead_magnet_detail.opportunity_id                              as latest_prior_opportunity_id,
        lead_magnet_detail.lead_magnet_id                              as latest_prior_lead_magnet_id,
        lead_magnet_detail.lead_magnet_reporting_name                   as latest_prior_lead_magnet_name,
        lead_magnet_detail.lead_magnet_category                         as latest_prior_lead_magnet_category,
        lead_magnet_detail.lead_magnet_offer_type                       as latest_prior_lead_magnet_offer_type,
        lead_magnet_detail.is_true_lead_magnet                          as latest_prior_is_true_lead_magnet,
        lead_magnet_detail.is_launch                                    as latest_prior_is_launch,
        lead_magnet_detail.is_waitlist                                  as latest_prior_is_waitlist,
        lead_magnet_detail.is_sales_pipeline                            as latest_prior_is_sales_pipeline,
        lead_magnet_detail.taxonomy_confidence                          as latest_prior_taxonomy_confidence,
        lead_magnet_detail.opportunity_created_at                       as latest_prior_opportunity_created_at,
        lead_magnet_detail.contact_opportunity_sequence                 as latest_prior_contact_opportunity_sequence

    from first_payments
    left join `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_detail` as lead_magnet_detail
        on first_payments.contact_sk = lead_magnet_detail.contact_sk
       and lead_magnet_detail.opportunity_created_at <= first_payments.first_purchase_at
    qualify row_number() over (
        partition by first_payments.contact_sk
        order by
            lead_magnet_detail.opportunity_created_at desc,
            lead_magnet_detail.opportunity_id desc
    ) = 1

),

bookings_before_first_purchase as (

    select
        first_payments.contact_sk,
        count(bookings.booking_sk)                                      as bookings_before_first_purchase_count,
        countif(bookings.event_status = 'active')                       as active_bookings_before_first_purchase_count,
        countif(bookings.event_status = 'canceled')                     as canceled_bookings_before_first_purchase_count,
        min(bookings.booked_at)                                         as first_booking_before_first_purchase_at,
        max(bookings.booked_at)                                         as latest_booking_before_first_purchase_at
    from first_payments
    left join `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked` as bookings
        on first_payments.contact_sk = bookings.contact_sk
       and bookings.booked_at <= first_payments.first_purchase_at
    group by 1

),

final as (

    select
        payment_summary.contact_sk,
        contacts.contact_id,
        contacts.email_norm,
        contacts.contact_name,
        contacts.phone,
        contacts.contact_created_at,

        first_payments.first_payment_id,
        first_payments.first_payment_source_platform,
        first_payments.first_purchase_at,
        first_payments.first_purchase_gross_amount,
        first_payments.first_purchase_net_revenue,
        first_payments.first_purchase_product,
        first_payments.first_purchase_payment_method,
        first_payments.first_purchase_card_issue_country,

        payment_summary.latest_purchase_at,
        payment_summary.paid_payments_count,
        payment_summary.payment_source_platform_count,
        payment_summary.stripe_payments_count,
        payment_summary.fanbasis_payments_count,
        payment_summary.total_gross_revenue,
        payment_summary.total_net_revenue_after_refunds,
        safe_divide(
            payment_summary.total_net_revenue_after_refunds,
            payment_summary.paid_payments_count
        )                                                               as average_net_revenue_per_payment,
        payment_summary.paid_payments_count > 1                         as is_multi_payment_buyer,
        payment_summary.purchased_products,

        first_known_magnets.first_known_opportunity_id,
        first_known_magnets.first_known_lead_magnet_id,
        first_known_magnets.first_known_lead_magnet_name,
        first_known_magnets.first_known_lead_magnet_category,
        first_known_magnets.first_known_lead_magnet_offer_type,
        first_known_magnets.first_known_opportunity_created_at,

        latest_prior_magnets.latest_prior_opportunity_id,
        latest_prior_magnets.latest_prior_lead_magnet_id,
        latest_prior_magnets.latest_prior_lead_magnet_name,
        latest_prior_magnets.latest_prior_lead_magnet_category,
        latest_prior_magnets.latest_prior_lead_magnet_offer_type,
        coalesce(latest_prior_magnets.latest_prior_is_true_lead_magnet, false)
                                                                        as latest_prior_is_true_lead_magnet,
        coalesce(latest_prior_magnets.latest_prior_is_launch, false)     as latest_prior_is_launch,
        coalesce(latest_prior_magnets.latest_prior_is_waitlist, false)   as latest_prior_is_waitlist,
        coalesce(latest_prior_magnets.latest_prior_is_sales_pipeline, false)
                                                                        as latest_prior_is_sales_pipeline,
        latest_prior_magnets.latest_prior_taxonomy_confidence,
        latest_prior_magnets.latest_prior_opportunity_created_at,
        latest_prior_magnets.latest_prior_contact_opportunity_sequence,

        latest_prior_magnets.latest_prior_opportunity_id is not null     as has_latest_prior_magnet_before_first_purchase,
        first_known_magnets.first_known_opportunity_id is not null       as has_any_known_magnet,
        first_known_magnets.first_known_opportunity_created_at > first_payments.first_purchase_at
                                                                        as first_known_magnet_after_first_purchase,

        timestamp_diff(
            first_payments.first_purchase_at,
            latest_prior_magnets.latest_prior_opportunity_created_at,
            day
        )                                                               as days_latest_prior_magnet_to_first_purchase,
        timestamp_diff(
            first_payments.first_purchase_at,
            first_known_magnets.first_known_opportunity_created_at,
            day
        )                                                               as days_first_known_magnet_to_first_purchase,

        coalesce(bookings_before_first_purchase.bookings_before_first_purchase_count, 0)
                                                                        as bookings_before_first_purchase_count,
        coalesce(bookings_before_first_purchase.active_bookings_before_first_purchase_count, 0)
                                                                        as active_bookings_before_first_purchase_count,
        coalesce(bookings_before_first_purchase.canceled_bookings_before_first_purchase_count, 0)
                                                                        as canceled_bookings_before_first_purchase_count,
        bookings_before_first_purchase.first_booking_before_first_purchase_at,
        bookings_before_first_purchase.latest_booking_before_first_purchase_at,
        coalesce(bookings_before_first_purchase.bookings_before_first_purchase_count, 0) > 0
                                                                        as has_booking_before_first_purchase,
        coalesce(bookings_before_first_purchase.active_bookings_before_first_purchase_count, 0) > 0
                                                                        as has_active_booking_before_first_purchase,

        contacts.attribution_era,
        contacts.utm_source,
        contacts.utm_medium,
        contacts.utm_campaign,

        case
            when latest_prior_magnets.latest_prior_opportunity_id is not null
                and latest_prior_magnets.latest_prior_taxonomy_confidence = 'missing_taxonomy'
                then 'missing_taxonomy'
            when latest_prior_magnets.latest_prior_opportunity_id is not null
                and latest_prior_magnets.latest_prior_lead_magnet_offer_type = 'uncategorized'
                then 'uncategorized_offer_type'
            when latest_prior_magnets.latest_prior_opportunity_id is not null
                then 'latest_prior_magnet'
            when first_known_magnets.first_known_opportunity_id is not null
                then 'purchase_before_first_magnet'
            else 'no_known_magnet'
        end                                                             as purchase_magnet_attribution_flag,

        current_timestamp()                                             as mart_refreshed_at

    from payment_summary
    inner join first_payments
        on payment_summary.contact_sk = first_payments.contact_sk
    left join contacts
        on payment_summary.contact_sk = contacts.contact_sk
    left join first_known_magnets
        on payment_summary.contact_sk = first_known_magnets.contact_sk
    left join latest_prior_magnets
        on payment_summary.contact_sk = latest_prior_magnets.contact_sk
    left join bookings_before_first_purchase
        on payment_summary.contact_sk = bookings_before_first_purchase.contact_sk

)

select * from final