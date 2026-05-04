{{
    config(
        materialized = 'table',
    )
}}

-- Customer-month retention mart.
--
-- Grain: one row per matched paid contact per calendar month, from first
-- purchase month through the current month. Payment activity is the retention
-- spine because it is the audited cash truth. Fanbasis customer/subscriber
-- records are layered in as supporting lifecycle evidence, not treated as a
-- complete receivables/churn contract.

with

runtime as (

    select
        current_date('America/New_York')                    as as_of_date,
        date_trunc(current_date('America/New_York'), month) as current_month

),

buyers as (

    select
        contact_sk,
        contact_id,
        email_norm,
        contact_name,
        phone,
        contact_created_at,

        first_purchase_at,
        latest_purchase_at,
        date_trunc(date(first_purchase_at, 'America/New_York'), month)  as cohort_month,
        date_trunc(date(latest_purchase_at, 'America/New_York'), month) as latest_purchase_month,

        first_payment_id,
        first_payment_source_platform,
        first_purchase_product,
        first_purchase_product_family,
        first_purchase_net_revenue,
        latest_payment_id,
        latest_payment_source_platform,
        latest_purchase_product,

        paid_payments_count,
        stripe_payments_count,
        fanbasis_payments_count,
        fanbasis_auto_renew_payments_count,
        fanbasis_service_payment_ids_count,
        fanbasis_net_revenue_after_refunds,
        fanbasis_unreleased_payments_count,
        purchased_product_count,
        total_gross_revenue,
        total_net_revenue_after_refunds,
        total_refunds_amount,
        total_refunds_count,
        average_net_revenue_per_payment,
        purchased_products,
        top_product_by_net_revenue,
        top_product_family,
        top_product_net_revenue,
        top_product_payments_count,
        is_multi_payment_buyer,
        is_payment_plan_buyer,
        payment_plan_status,
        payment_plan_truth_status,

        latest_prior_opportunity_id,
        latest_prior_lead_magnet_id,
        latest_prior_lead_magnet_name,
        latest_prior_lead_magnet_category,
        latest_prior_lead_magnet_offer_type,
        latest_prior_opportunity_created_at,
        purchase_magnet_attribution_flag,

        credited_closer_user_sk,
        credited_closer_user_id,
        credited_closer_name,
        credited_closer_role,
        credited_closer_source,
        credited_closer_confidence,
        credited_setter_user_sk,
        credited_setter_user_id,
        credited_setter_name,
        credited_setter_role,
        credited_setter_source,
        best_available_operator_name,
        best_available_operator_source,
        pre_purchase_funnel_path,
        revenue_funnel_quality_flag
    from {{ ref('revenue_funnel_detail') }}

),

payments as (

    select
        payments.payment_id,
        payments.source_platform,
        payments.contact_sk,
        payments.transaction_date,
        date_trunc(date(payments.transaction_date, 'America/New_York'), month)
                                                                        as activity_month,
        payments.gross_amount,
        payments.net_amount,
        payments.product,
        payments.source_product_id,
        payments.source_service_id,
        payments.source_service_payment_id,
        payments.payment_method,
        payments.is_refunded,
        payments.source_fund_released
    from {{ ref('fct_payments') }} as payments
    where payments.is_paid = true
        and payments.contact_sk is not null

),

payments_by_month as (

    select
        contact_sk,
        activity_month,

        count(*)                                                       as paid_payments_in_month,
        countif(source_platform = 'stripe')                            as stripe_payments_in_month,
        countif(source_platform = 'fanbasis')                          as fanbasis_payments_in_month,
        countif(source_platform = 'fanbasis' and payment_method = 'upfront')
                                                                        as fanbasis_upfront_payments_in_month,
        countif(source_platform = 'fanbasis' and payment_method = 'auto_renew')
                                                                        as fanbasis_auto_renew_payments_in_month,
        count(distinct if(source_platform = 'fanbasis', source_service_payment_id, null))
                                                                        as fanbasis_service_payment_ids_in_month,
        countif(source_platform = 'fanbasis' and is_refunded)           as fanbasis_refunded_payments_in_month,
        countif(
            source_platform = 'fanbasis'
            and source_fund_released is not null
            and not source_fund_released
        )                                                              as fanbasis_unreleased_payments_in_month,
        count(distinct coalesce(product, 'unknown'))                   as purchased_products_in_month,
        sum(gross_amount)                                              as gross_revenue_in_month,
        sum(net_amount)                                                as net_revenue_before_refunds_in_month,
        string_agg(distinct coalesce(product, 'unknown'), ' | ')        as purchased_products_list_in_month
    from payments
    group by 1, 2

),

post_first_payments as (

    select
        buyers.contact_sk,
        count(*)                                                       as post_first_paid_payments_count,
        countif(payments.source_platform = 'stripe')                   as post_first_stripe_payments_count,
        countif(payments.source_platform = 'fanbasis')                 as post_first_fanbasis_payments_count,
        countif(
            payments.source_platform = 'fanbasis'
            and payments.payment_method = 'auto_renew'
        )                                                              as post_first_fanbasis_auto_renew_payments_count,
        sum(payments.net_amount)                                       as post_first_collected_net_revenue,
        min(payments.transaction_date)                                 as first_post_first_payment_at,
        max(payments.transaction_date)                                 as latest_post_first_payment_at
    from buyers
    inner join payments
        on payments.contact_sk = buyers.contact_sk
       and payments.payment_id != buyers.first_payment_id
       and payments.transaction_date >= buyers.first_purchase_at
    group by 1

),

post_first_bookings as (

    select
        buyers.contact_sk,
        count(*)                                                       as post_first_purchase_bookings_count,
        countif(
            regexp_contains(
                lower(coalesce(bookings.event_name, '')),
                r'check\s*up|check.?in|follow.?up|balance|payment|collect|remaining'
            )
        )                                                              as post_first_purchase_collection_bookings_count,
        min(bookings.booked_at)                                        as first_post_first_booking_at,
        max(bookings.booked_at)                                        as latest_post_first_booking_at,
        max(if(
            regexp_contains(
                lower(coalesce(bookings.event_name, '')),
                r'check\s*up|check.?in|follow.?up|balance|payment|collect|remaining'
            ),
            bookings.booked_at,
            null
        ))                                                             as latest_collection_booking_at,
        array_agg(
            bookings.event_name
            order by bookings.booked_at desc, bookings.booking_sk desc
            limit 1
        )[safe_offset(0)]                                               as latest_post_first_booking_name,
        array_agg(
            if(
                regexp_contains(
                    lower(coalesce(bookings.event_name, '')),
                    r'check\s*up|check.?in|follow.?up|balance|payment|collect|remaining'
                ),
                bookings.event_name,
                null
            )
            ignore nulls
            order by bookings.booked_at desc, bookings.booking_sk desc
            limit 1
        )[safe_offset(0)]                                               as latest_collection_booking_name
    from buyers
    inner join {{ ref('fct_calls_booked') }} as bookings
        on bookings.contact_sk = buyers.contact_sk
       and bookings.booked_at > buyers.first_purchase_at
    group by 1

),

refunds_by_month as (

    select
        contact_sk,
        date_trunc(date(refunded_at, 'America/New_York'), month)        as activity_month,
        count(*)                                                       as refunds_count_in_month,
        sum(refund_amount)                                             as refunds_amount_in_month,
        sum(refund_amount_net)                                         as refunds_amount_net_in_month
    from {{ ref('fct_refunds') }}
    where contact_sk is not null
    group by 1, 2

),

fanbasis_payment_identities as (

    select distinct
        payments.contact_sk,
        transactions.fan_id                                            as fanbasis_customer_id,
        lower(trim(transactions.fan_email))                            as fanbasis_email_norm
    from payments
    inner join {{ ref('stg_fanbasis__transactions') }} as transactions
        on payments.source_platform = 'fanbasis'
       and payments.payment_id      = transactions.payment_id
    where payments.source_platform = 'fanbasis'

),

subscriber_matches as (

    select
        fanbasis_payment_identities.contact_sk,
        subscribers.*
    from fanbasis_payment_identities
    inner join {{ ref('stg_fanbasis__subscribers') }} as subscribers
        on fanbasis_payment_identities.fanbasis_customer_id
           = subscribers.customer_id

    union distinct

    select
        buyers.contact_sk,
        subscribers.*
    from buyers
    inner join {{ ref('stg_fanbasis__subscribers') }} as subscribers
        on buyers.email_norm = subscribers.customer_email_norm

),

subscriber_latest as (

    select
        subscriber_matches.*
    from subscriber_matches
    qualify row_number() over (
        partition by contact_sk, subscriber_id
        order by subscription_updated_at desc nulls last, _ingested_at desc
    ) = 1

),

subscriber_by_contact as (

    select
        contact_sk,

        count(*)                                                       as fanbasis_subscriber_rows_count,
        count(distinct customer_id)                                    as fanbasis_customer_ids_count,
        count(distinct subscription_id)                                as fanbasis_subscription_ids_count,
        countif(lower(coalesce(subscription_status, '')) = 'active')    as active_fanbasis_subscription_count,
        countif(lower(coalesce(subscription_status, '')) = 'completed') as completed_fanbasis_subscription_count,
        countif(lower(coalesce(subscription_status, '')) = 'failed')    as failed_fanbasis_subscription_count,
        countif(lower(coalesce(subscription_status, '')) = 'onetime_service')
                                                                        as onetime_fanbasis_subscription_count,
        countif(lower(coalesce(service_type, '')) = 'subscription')     as fanbasis_subscription_service_count,
        countif(lower(coalesce(service_type, '')) = 'onetime')          as fanbasis_onetime_service_count,
        max(coalesce(auto_renew_count, 0))                              as max_fanbasis_auto_renew_count,
        logical_or(coalesce(has_charge_consent, false))                 as has_fanbasis_charge_consent,
        min(subscription_created_at)                                    as first_fanbasis_subscription_created_at,
        max(subscription_updated_at)                                    as latest_fanbasis_subscription_updated_at,
        max(completion_at)                                             as latest_fanbasis_completion_at,
        max(cancelled_at)                                              as latest_fanbasis_cancelled_at,
        array_to_string(
            array_agg(distinct customer_id ignore nulls order by customer_id),
            ' | '
        )                                                              as fanbasis_customer_ids,
        array_to_string(
            array_agg(distinct subscription_id ignore nulls order by subscription_id),
            ' | '
        )                                                              as fanbasis_subscription_ids,
        array_agg(
            struct(
                subscription_status,
                service_type,
                payment_frequency,
                product_id,
                product_title,
                subscription_updated_at
            )
            order by subscription_updated_at desc nulls last,
                _ingested_at desc,
                subscriber_id
            limit 1
        )[safe_offset(0)]                                               as latest_subscription
    from subscriber_latest
    group by 1

),

customer_matches as (

    select
        fanbasis_payment_identities.contact_sk,
        customers.*
    from fanbasis_payment_identities
    inner join {{ ref('stg_fanbasis__customers') }} as customers
        on fanbasis_payment_identities.fanbasis_customer_id
           = customers.customer_id

    union distinct

    select
        buyers.contact_sk,
        customers.*
    from buyers
    inner join {{ ref('stg_fanbasis__customers') }} as customers
        on buyers.email_norm = customers.customer_email_norm

),

customer_latest as (

    select
        customer_matches.*
    from customer_matches
    qualify row_number() over (
        partition by contact_sk, customer_id
        order by last_transaction_at desc nulls last, _ingested_at desc
    ) = 1

),

customer_by_contact as (

    select
        contact_sk,
        count(distinct customer_id)                                    as fanbasis_directory_customer_ids_count,
        sum(total_transactions)                                        as fanbasis_directory_total_transactions,
        sum(total_spent)                                               as fanbasis_directory_total_spent,
        max(last_transaction_at)                                       as fanbasis_directory_last_transaction_at,
        array_to_string(
            array_agg(distinct customer_id ignore nulls order by customer_id),
            ' | '
        )                                                              as fanbasis_directory_customer_ids
    from customer_latest
    group by 1

),

month_spine as (

    select
        buyers.contact_sk,
        activity_month
    from buyers
    cross join runtime
    cross join unnest(
        generate_date_array(
            buyers.cohort_month,
            runtime.current_month,
            interval 1 month
        )
    ) as activity_month

),

assembled as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'month_spine.contact_sk',
            'cast(month_spine.activity_month as string)'
        ]) }}                                                        as customer_retention_sk,

        buyers.contact_sk,
        buyers.contact_id,
        buyers.email_norm,
        buyers.contact_name,
        buyers.phone,
        buyers.contact_created_at,

        buyers.cohort_month,
        month_spine.activity_month,
        date_sub(date_add(month_spine.activity_month, interval 1 month), interval 1 day)
                                                                        as activity_month_end,
        runtime.as_of_date,
        runtime.current_month,
        date_diff(month_spine.activity_month, buyers.cohort_month, month)
                                                                        as months_since_first_purchase,
        date_diff(month_spine.activity_month, buyers.latest_purchase_month, month)
                                                                        as months_since_latest_purchase,

        buyers.first_purchase_at,
        buyers.latest_purchase_at,
        buyers.latest_purchase_month,

        buyers.first_payment_id,
        buyers.first_payment_source_platform,
        buyers.first_purchase_product,
        buyers.first_purchase_product_family,
        buyers.first_purchase_net_revenue,
        buyers.latest_payment_id,
        buyers.latest_payment_source_platform,
        buyers.latest_purchase_product,

        buyers.paid_payments_count                                      as lifetime_paid_payments_count,
        buyers.stripe_payments_count                                    as lifetime_stripe_payments_count,
        buyers.fanbasis_payments_count                                  as lifetime_fanbasis_payments_count,
        buyers.fanbasis_auto_renew_payments_count                       as lifetime_fanbasis_auto_renew_payments_count,
        buyers.fanbasis_service_payment_ids_count                       as lifetime_fanbasis_service_payment_ids_count,
        buyers.fanbasis_net_revenue_after_refunds                       as lifetime_fanbasis_net_revenue_after_refunds,
        buyers.fanbasis_unreleased_payments_count                       as lifetime_fanbasis_unreleased_payments_count,
        buyers.purchased_product_count                                  as lifetime_purchased_product_count,
        buyers.total_gross_revenue                                      as lifetime_gross_revenue,
        buyers.total_net_revenue_after_refunds                          as lifetime_net_revenue_after_refunds,
        buyers.total_refunds_amount                                     as lifetime_refunds_amount,
        buyers.total_refunds_count                                      as lifetime_refunds_count,
        buyers.average_net_revenue_per_payment,
        buyers.purchased_products                                       as lifetime_purchased_products,
        buyers.top_product_by_net_revenue,
        buyers.top_product_family,
        buyers.top_product_net_revenue,
        buyers.top_product_payments_count,
        buyers.is_multi_payment_buyer,
        buyers.is_payment_plan_buyer,
        buyers.payment_plan_status,
        buyers.payment_plan_truth_status,
        buyers.first_purchase_net_revenue                              as upfront_collected_net_revenue,
        coalesce(post_first_payments.post_first_paid_payments_count, 0) as post_first_paid_payments_count,
        coalesce(post_first_payments.post_first_stripe_payments_count, 0)
                                                                        as post_first_stripe_payments_count,
        coalesce(post_first_payments.post_first_fanbasis_payments_count, 0)
                                                                        as post_first_fanbasis_payments_count,
        coalesce(post_first_payments.post_first_fanbasis_auto_renew_payments_count, 0)
                                                                        as post_first_fanbasis_auto_renew_payments_count,
        coalesce(post_first_payments.post_first_collected_net_revenue, 0)
                                                                        as post_first_collected_net_revenue,
        post_first_payments.first_post_first_payment_at,
        post_first_payments.latest_post_first_payment_at,
        coalesce(post_first_bookings.post_first_purchase_bookings_count, 0)
                                                                        as post_first_purchase_bookings_count,
        coalesce(post_first_bookings.post_first_purchase_collection_bookings_count, 0)
                                                                        as post_first_purchase_collection_bookings_count,
        post_first_bookings.first_post_first_booking_at,
        post_first_bookings.latest_post_first_booking_at,
        post_first_bookings.latest_post_first_booking_name,
        post_first_bookings.latest_collection_booking_at,
        post_first_bookings.latest_collection_booking_name,
        coalesce(post_first_bookings.post_first_purchase_collection_bookings_count, 0) > 0
                                                                        as has_post_first_collection_booking,
        case
            when buyers.fanbasis_auto_renew_payments_count > 0
                then 'automated_fanbasis_plan'
            when coalesce(post_first_payments.post_first_paid_payments_count, 0) > 0
                and buyers.purchased_product_count > 1
                then 'manual_collection_or_upsell'
            when coalesce(post_first_payments.post_first_paid_payments_count, 0) > 0
                then 'manual_payment_plan_collected'
            when coalesce(post_first_bookings.post_first_purchase_collection_bookings_count, 0) > 0
                then 'collection_call_scheduled_no_repeat_payment'
            when buyers.payment_plan_status = 'plan_named_single_payment'
                then 'plan_named_cash_only'
            else 'single_payment_no_collection_signal'
        end                                                           as collection_motion_type,

        buyers.latest_prior_opportunity_id,
        buyers.latest_prior_lead_magnet_id,
        buyers.latest_prior_lead_magnet_name,
        buyers.latest_prior_lead_magnet_category,
        buyers.latest_prior_lead_magnet_offer_type,
        buyers.latest_prior_opportunity_created_at,
        buyers.purchase_magnet_attribution_flag,

        buyers.credited_closer_user_sk,
        buyers.credited_closer_user_id,
        buyers.credited_closer_name,
        buyers.credited_closer_role,
        buyers.credited_closer_source,
        buyers.credited_closer_confidence,
        buyers.credited_setter_user_sk,
        buyers.credited_setter_user_id,
        buyers.credited_setter_name,
        buyers.credited_setter_role,
        buyers.credited_setter_source,
        buyers.best_available_operator_name,
        buyers.best_available_operator_source,
        buyers.pre_purchase_funnel_path,
        buyers.revenue_funnel_quality_flag,

        coalesce(payments_by_month.paid_payments_in_month, 0)           as paid_payments_in_month,
        coalesce(payments_by_month.stripe_payments_in_month, 0)         as stripe_payments_in_month,
        coalesce(payments_by_month.fanbasis_payments_in_month, 0)       as fanbasis_payments_in_month,
        coalesce(payments_by_month.fanbasis_upfront_payments_in_month, 0)
                                                                        as fanbasis_upfront_payments_in_month,
        coalesce(payments_by_month.fanbasis_auto_renew_payments_in_month, 0)
                                                                        as fanbasis_auto_renew_payments_in_month,
        coalesce(payments_by_month.fanbasis_service_payment_ids_in_month, 0)
                                                                        as fanbasis_service_payment_ids_in_month,
        coalesce(payments_by_month.fanbasis_refunded_payments_in_month, 0)
                                                                        as fanbasis_refunded_payments_in_month,
        coalesce(payments_by_month.fanbasis_unreleased_payments_in_month, 0)
                                                                        as fanbasis_unreleased_payments_in_month,
        coalesce(payments_by_month.purchased_products_in_month, 0)      as purchased_products_in_month,
        coalesce(payments_by_month.gross_revenue_in_month, 0)           as gross_revenue_in_month,
        coalesce(payments_by_month.net_revenue_before_refunds_in_month, 0)
                                                                        as net_revenue_before_refunds_in_month,
        payments_by_month.purchased_products_list_in_month,
        coalesce(refunds_by_month.refunds_count_in_month, 0)            as refunds_count_in_month,
        coalesce(refunds_by_month.refunds_amount_in_month, 0)           as refunds_amount_in_month,
        coalesce(refunds_by_month.refunds_amount_net_in_month, 0)       as refunds_amount_net_in_month,
        coalesce(payments_by_month.net_revenue_before_refunds_in_month, 0)
        - coalesce(refunds_by_month.refunds_amount_in_month, 0)         as net_revenue_after_refunds_in_month,

        coalesce(subscriber_by_contact.fanbasis_subscriber_rows_count, 0)
                                                                        as fanbasis_subscriber_rows_count,
        coalesce(subscriber_by_contact.fanbasis_customer_ids_count, 0)  as fanbasis_customer_ids_count,
        coalesce(subscriber_by_contact.fanbasis_subscription_ids_count, 0)
                                                                        as fanbasis_subscription_ids_count,
        coalesce(subscriber_by_contact.active_fanbasis_subscription_count, 0)
                                                                        as active_fanbasis_subscription_count,
        coalesce(subscriber_by_contact.completed_fanbasis_subscription_count, 0)
                                                                        as completed_fanbasis_subscription_count,
        coalesce(subscriber_by_contact.failed_fanbasis_subscription_count, 0)
                                                                        as failed_fanbasis_subscription_count,
        coalesce(subscriber_by_contact.onetime_fanbasis_subscription_count, 0)
                                                                        as onetime_fanbasis_subscription_count,
        coalesce(subscriber_by_contact.fanbasis_subscription_service_count, 0)
                                                                        as fanbasis_subscription_service_count,
        coalesce(subscriber_by_contact.fanbasis_onetime_service_count, 0)
                                                                        as fanbasis_onetime_service_count,
        coalesce(subscriber_by_contact.max_fanbasis_auto_renew_count, 0)
                                                                        as max_fanbasis_auto_renew_count,
        coalesce(subscriber_by_contact.has_fanbasis_charge_consent, false)
                                                                        as has_fanbasis_charge_consent,
        subscriber_by_contact.first_fanbasis_subscription_created_at,
        subscriber_by_contact.latest_fanbasis_subscription_updated_at,
        subscriber_by_contact.latest_fanbasis_completion_at,
        subscriber_by_contact.latest_fanbasis_cancelled_at,
        subscriber_by_contact.fanbasis_customer_ids,
        subscriber_by_contact.fanbasis_subscription_ids,
        subscriber_by_contact.latest_subscription.subscription_status    as latest_fanbasis_subscription_status,
        subscriber_by_contact.latest_subscription.service_type           as latest_fanbasis_service_type,
        subscriber_by_contact.latest_subscription.payment_frequency      as latest_fanbasis_payment_frequency,
        subscriber_by_contact.latest_subscription.product_id             as latest_fanbasis_product_id,
        subscriber_by_contact.latest_subscription.product_title          as latest_fanbasis_product_title,

        coalesce(customer_by_contact.fanbasis_directory_customer_ids_count, 0)
                                                                        as fanbasis_directory_customer_ids_count,
        coalesce(customer_by_contact.fanbasis_directory_total_transactions, 0)
                                                                        as fanbasis_directory_total_transactions,
        coalesce(customer_by_contact.fanbasis_directory_total_spent, 0)  as fanbasis_directory_total_spent,
        customer_by_contact.fanbasis_directory_last_transaction_at,
        customer_by_contact.fanbasis_directory_customer_ids,

        month_spine.activity_month = buyers.cohort_month                as is_first_purchase_month,
        month_spine.activity_month = buyers.latest_purchase_month        as is_latest_purchase_month,
        coalesce(payments_by_month.paid_payments_in_month, 0) > 0        as is_paid_month,
        coalesce(payments_by_month.paid_payments_in_month, 0) > 0
        and month_spine.activity_month > buyers.cohort_month            as is_repeat_paid_month,
        coalesce(refunds_by_month.refunds_count_in_month, 0) > 0        as is_refund_month,
        coalesce(subscriber_by_contact.active_fanbasis_subscription_count, 0) > 0
                                                                        as has_active_fanbasis_subscription_now,
        month_spine.activity_month = runtime.current_month              as is_current_month,
        (
            coalesce(payments_by_month.paid_payments_in_month, 0) > 0
            or (
                month_spine.activity_month = runtime.current_month
                and coalesce(subscriber_by_contact.active_fanbasis_subscription_count, 0) > 0
            )
        )                                                              as is_observed_retained_month,

        case
            when
                coalesce(payments_by_month.paid_payments_in_month, 0) > 0
                and month_spine.activity_month = buyers.cohort_month
                then 'new_paid_month'
            when coalesce(payments_by_month.paid_payments_in_month, 0) > 0
                then 'repeat_paid_month'
            when coalesce(refunds_by_month.refunds_count_in_month, 0) > 0
                then 'refund_only_month'
            when
                month_spine.activity_month = runtime.current_month
                and coalesce(subscriber_by_contact.active_fanbasis_subscription_count, 0) > 0
                then 'active_subscriber_current_month_no_payment'
            when month_spine.activity_month > buyers.latest_purchase_month
                then 'post_latest_payment_month'
            else 'observed_gap_month'
        end                                                           as retention_state,

        case
            when coalesce(subscriber_by_contact.active_fanbasis_subscription_count, 0) > 0
                then 'active_fanbasis_subscription'
            when coalesce(subscriber_by_contact.completed_fanbasis_subscription_count, 0) > 0
                then 'completed_fanbasis_subscription'
            when coalesce(subscriber_by_contact.failed_fanbasis_subscription_count, 0) > 0
                then 'failed_fanbasis_subscription'
            when coalesce(subscriber_by_contact.onetime_fanbasis_subscription_count, 0) > 0
                then 'one_time_fanbasis_customer'
            when buyers.fanbasis_payments_count > 0
                then 'fanbasis_transaction_no_subscriber_record'
            else 'historical_stripe_or_no_subscriber_record'
        end                                                           as customer_lifecycle_status,

        case
            when buyers.total_net_revenue_after_refunds < 0
                then 'negative_lifetime_value'
            when buyers.top_product_family = 'Unknown / historical Stripe'
                then 'missing_product_family'
            when
                buyers.fanbasis_payments_count > 0
                and coalesce(subscriber_by_contact.fanbasis_subscriber_rows_count, 0) = 0
                then 'no_subscriber_record'
            else 'clean'
        end                                                           as retention_quality_flag,

        current_timestamp()                                           as mart_refreshed_at

    from month_spine
    inner join buyers
        on month_spine.contact_sk = buyers.contact_sk
    cross join runtime
    left join payments_by_month
        on month_spine.contact_sk     = payments_by_month.contact_sk
       and month_spine.activity_month = payments_by_month.activity_month
    left join refunds_by_month
        on month_spine.contact_sk     = refunds_by_month.contact_sk
       and month_spine.activity_month = refunds_by_month.activity_month
    left join post_first_payments
        on month_spine.contact_sk = post_first_payments.contact_sk
    left join post_first_bookings
        on month_spine.contact_sk = post_first_bookings.contact_sk
    left join subscriber_by_contact
        on month_spine.contact_sk = subscriber_by_contact.contact_sk
    left join customer_by_contact
        on month_spine.contact_sk = customer_by_contact.contact_sk

),

renewal_base as (

    select
        assembled.*,
        safe_cast(latest_fanbasis_payment_frequency as int64)            as latest_fanbasis_payment_frequency_days,
        date(latest_post_first_payment_at, 'America/New_York')           as latest_post_first_payment_date,
        date(latest_collection_booking_at, 'America/New_York')           as latest_collection_booking_date,
        case
            when latest_fanbasis_service_type = 'subscription'
                and latest_purchase_at is not null
                then timestamp_add(
                    latest_purchase_at,
                    interval coalesce(
                        safe_cast(latest_fanbasis_payment_frequency as int64),
                        30
                    ) day
                )
        end                                                             as expected_next_payment_at
    from assembled

),

classified as (

    select
        renewal_base.*,
        date(expected_next_payment_at, 'America/New_York')               as expected_next_payment_date,
        date_diff(
            date(expected_next_payment_at, 'America/New_York'),
            as_of_date,
            day
        )                                                               as days_until_expected_next_payment,
        date_diff(
            as_of_date,
            date(expected_next_payment_at, 'America/New_York'),
            day
        )                                                               as days_past_expected_payment,
        date_diff(
            as_of_date,
            latest_post_first_payment_date,
            day
        )                                                               as days_since_latest_post_first_payment,
        date_diff(
            as_of_date,
            latest_collection_booking_date,
            day
        )                                                               as days_since_latest_collection_booking,
        safe_divide(
            post_first_collected_net_revenue,
            nullif(lifetime_net_revenue_after_refunds, 0)
        )                                                               as post_first_collected_net_revenue_share,
        expected_next_payment_at is not null
        and as_of_date >= date(expected_next_payment_at, 'America/New_York')
        and customer_lifecycle_status in (
            'active_fanbasis_subscription',
            'failed_fanbasis_subscription'
        )
                                                                        as is_expected_payment_due_now,
        expected_next_payment_at is not null
        and as_of_date > date(expected_next_payment_at, 'America/New_York')
        and latest_purchase_month < current_month
        and customer_lifecycle_status in (
            'active_fanbasis_subscription',
            'failed_fanbasis_subscription'
        )                                                               as is_expected_payment_missed_now,
        (
            customer_lifecycle_status in (
                'active_fanbasis_subscription',
                'failed_fanbasis_subscription',
                'completed_fanbasis_subscription'
            )
            or is_payment_plan_buyer
        )                                                               as is_repeat_payment_eligible_now,

        case
            when lifetime_fanbasis_auto_renew_payments_count > 0
                then 'fanbasis_auto_renew_or_installment'
            when
                customer_lifecycle_status in (
                    'active_fanbasis_subscription',
                    'failed_fanbasis_subscription',
                    'completed_fanbasis_subscription'
                )
                and lifetime_paid_payments_count > 1
                then 'fanbasis_subscription_installment'
            when
                lifetime_paid_payments_count > 1
                and lifetime_purchased_product_count > 1
                then 'multi_product_repeat_or_upsell'
            when lifetime_paid_payments_count > 1
                then 'same_product_multi_payment'
            when customer_lifecycle_status = 'active_fanbasis_subscription'
                then 'active_subscription_no_repeat_paid_yet'
            when customer_lifecycle_status = 'failed_fanbasis_subscription'
                then 'failed_subscription_no_repeat_paid'
            when customer_lifecycle_status = 'completed_fanbasis_subscription'
                then 'completed_subscription_no_repeat_paid'
            when top_product_family = 'Unknown / historical Stripe'
                then 'historical_stripe_single_payment'
            else 'single_payment_no_repeat'
        end                                                             as repeat_payment_type,

        case
            when lifetime_net_revenue_after_refunds < 0
                then 'review_negative_value'
            when customer_lifecycle_status = 'failed_fanbasis_subscription'
                then 'failed_plan_recovery_needed'
            when
                customer_lifecycle_status = 'active_fanbasis_subscription'
                and latest_purchase_month = current_month
                then 'active_plan_paid_current_month'
            when
                customer_lifecycle_status = 'active_fanbasis_subscription'
                and expected_next_payment_at is not null
                and as_of_date >= date(expected_next_payment_at, 'America/New_York')
                then 'active_plan_due_no_payment_yet'
            when customer_lifecycle_status = 'active_fanbasis_subscription'
                then 'active_plan_not_yet_due'
            when customer_lifecycle_status = 'completed_fanbasis_subscription'
                then 'completed_plan_paid_off'
            when lifetime_paid_payments_count > 1
                then 'repeat_payment_observed'
            when customer_lifecycle_status = 'one_time_fanbasis_customer'
                then 'one_time_upsell_candidate'
            when top_product_family = 'Unknown / historical Stripe'
                then 'historical_stripe_product_review'
            else 'no_repeat_expected_yet'
        end                                                             as payment_plan_health_status,

        case
            when lifetime_net_revenue_after_refunds < 0
                then 'review_negative_value'
            when
                collection_motion_type = 'automated_fanbasis_plan'
                and customer_lifecycle_status = 'failed_fanbasis_subscription'
                then 'automated_plan_failed_recovery'
            when
                collection_motion_type = 'automated_fanbasis_plan'
                and expected_next_payment_at is not null
                and as_of_date >= date(expected_next_payment_at, 'America/New_York')
                and latest_purchase_month < current_month
                then 'automated_plan_due_no_payment'
            when collection_motion_type = 'automated_fanbasis_plan'
                then 'automated_plan_monitor'
            when collection_motion_type = 'manual_collection_or_upsell'
                then 'repeat_or_upsell_review'
            when
                collection_motion_type = 'manual_payment_plan_collected'
                and latest_post_first_payment_date >= date_sub(as_of_date, interval 45 day)
                then 'manual_collection_recently_collected'
            when collection_motion_type = 'manual_payment_plan_collected'
                then 'manual_collection_stale_review'
            when collection_motion_type = 'collection_call_scheduled_no_repeat_payment'
                then 'collection_call_no_payment_review'
            when collection_motion_type = 'plan_named_cash_only'
                then 'plan_named_collection_review'
            else 'no_collection_signal'
        end                                                             as collection_health_status
    from renewal_base

),

actioned as (

    select
        classified.*,
        case
            when payment_plan_health_status = 'failed_plan_recovery_needed'
                then 'recover_failed_payment'
            when payment_plan_health_status = 'active_plan_due_no_payment_yet'
                then 'collect_due_payment'
            when payment_plan_health_status = 'active_plan_not_yet_due'
                then 'watch_next_due_date'
            when payment_plan_health_status = 'active_plan_paid_current_month'
                then 'monitor_active_plan'
            when payment_plan_health_status = 'review_negative_value'
                then 'review_refund_or_chargeback'
            when collection_health_status in (
                'manual_collection_stale_review',
                'collection_call_no_payment_review',
                'plan_named_collection_review'
            )
                then 'review_manual_collection'
            when collection_health_status = 'repeat_or_upsell_review'
                then 'confirm_repeat_or_upsell'
            when collection_health_status = 'manual_collection_recently_collected'
                then 'monitor_manual_collection'
            when payment_plan_health_status = 'completed_plan_paid_off'
                then 'upsell_completed_customer'
            when payment_plan_health_status = 'one_time_upsell_candidate'
                then 'upsell_one_time_customer'
            when payment_plan_health_status = 'historical_stripe_product_review'
                then 'repair_historical_product'
            when payment_plan_health_status = 'repeat_payment_observed'
                then 'monitor_repeat_customer'
            else 'monitor'
        end                                                             as retention_operator_next_action
    from classified

),

final as (

    select
        actioned.*,
        sum(paid_payments_in_month) over (
            partition by contact_sk
            order by activity_month
            rows between unbounded preceding and current row
        )                                                            as cumulative_paid_payments,
        sum(gross_revenue_in_month) over (
            partition by contact_sk
            order by activity_month
            rows between unbounded preceding and current row
        )                                                            as cumulative_gross_revenue,
        sum(net_revenue_before_refunds_in_month) over (
            partition by contact_sk
            order by activity_month
            rows between unbounded preceding and current row
        )                                                            as cumulative_net_revenue_before_refunds,
        sum(refunds_amount_in_month) over (
            partition by contact_sk
            order by activity_month
            rows between unbounded preceding and current row
        )                                                            as cumulative_refunds_amount,
        sum(net_revenue_after_refunds_in_month) over (
            partition by contact_sk
            order by activity_month
            rows between unbounded preceding and current row
        )                                                            as cumulative_net_revenue_after_refunds
    from actioned

)

select * from final
