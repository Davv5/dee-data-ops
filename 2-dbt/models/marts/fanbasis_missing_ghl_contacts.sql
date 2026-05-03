{{ config(materialized='table') }}

-- Operator queue: Fanbasis buyers with paid revenue that still do not resolve
-- to a GHL contact. This is the action list for source-depth cleanup; do not
-- hide these buyers behind more fuzzy matching.

with

payments as (

    select * from {{ ref('fct_payments') }}

),

transactions as (

    select * from {{ ref('stg_fanbasis__transactions') }}

),

customers as (

    select * from {{ ref('stg_fanbasis__customers') }}

),

subscribers as (

    select * from {{ ref('stg_fanbasis__subscribers') }}

),

contacts as (

    select
        contact_sk,
        contact_id,
        contact_name,
        email_norm,
        regexp_replace(coalesce(phone, ''), r'[^0-9]', '') as phone_digits
    from {{ ref('dim_contacts') }}

),

fanbasis_unmatched_payments as (

    select
        payments.payment_id,
        payments.net_amount,
        payments.transaction_date,
        transactions.fan_id,
        transactions.fan_name,
        lower(trim(transactions.fan_email))                    as fan_email_norm,
        transactions.fan_email,
        transactions.fan_phone,
        transactions.product_title,
        transactions.service_title
    from payments
    inner join transactions
        on payments.source_platform = 'fanbasis'
       and payments.payment_id      = transactions.payment_id
    where payments.source_platform = 'fanbasis'
        and payments.is_paid
        and payments.contact_sk is null

),

subscriber_latest as (

    select
        subscribers.*
    from subscribers
    qualify row_number() over (
        partition by subscribers.customer_id
        order by subscribers.subscription_updated_at desc nulls last,
            subscribers.subscriber_id
    ) = 1

),

customer_latest as (

    select
        customers.*
    from customers
    qualify row_number() over (
        partition by customers.customer_email_norm
        order by customers.last_transaction_at desc nulls last,
            customers.customer_id
    ) = 1

),

enriched_unmatched_payments as (

    select
        fanbasis_unmatched_payments.fan_id                      as fanbasis_customer_id,
        customer_latest.customer_id                             as fanbasis_directory_customer_id,
        coalesce(
            subscriber_latest.customer_name,
            customer_latest.customer_name,
            fanbasis_unmatched_payments.fan_name
        )                                                       as buyer_name,
        coalesce(
            subscriber_latest.customer_email,
            customer_latest.customer_email,
            fanbasis_unmatched_payments.fan_email
        )                                                       as buyer_email,
        coalesce(
            subscriber_latest.customer_email_norm,
            customer_latest.customer_email_norm,
            fanbasis_unmatched_payments.fan_email_norm
        )                                                       as buyer_email_norm,
        coalesce(
            subscriber_latest.customer_phone,
            customer_latest.customer_phone,
            fanbasis_unmatched_payments.fan_phone
        )                                                       as buyer_phone,

        fanbasis_unmatched_payments.payment_id,
        fanbasis_unmatched_payments.net_amount,
        fanbasis_unmatched_payments.transaction_date,
        coalesce(
            fanbasis_unmatched_payments.product_title,
            fanbasis_unmatched_payments.service_title
        )                                                       as purchased_product,
        subscriber_latest.subscriber_id,
        subscriber_latest.subscription_id,
        subscriber_latest.subscription_status,
        subscriber_latest.product_id                             as subscriber_product_id,
        subscriber_latest.product_title                          as subscriber_product_title,
        subscriber_latest.auto_renew_count,
        subscriber_latest.has_charge_consent,
        customer_latest.total_transactions                       as fanbasis_total_transactions,
        customer_latest.total_spent                              as fanbasis_total_spent,
        customer_latest.last_transaction_at                      as fanbasis_last_transaction_at

    from fanbasis_unmatched_payments
    left join subscriber_latest
        on fanbasis_unmatched_payments.fan_id = subscriber_latest.customer_id
    left join customer_latest
        on fanbasis_unmatched_payments.fan_email_norm
           = customer_latest.customer_email_norm

),

rolled_up as (

    select
        fanbasis_customer_id,
        fanbasis_directory_customer_id,
        buyer_name,
        buyer_email,
        buyer_email_norm,
        buyer_phone,

        count(*)                                                as paid_payments_count,
        round(sum(net_amount), 2)                               as total_net_revenue,
        min(transaction_date)                                   as first_paid_at,
        max(transaction_date)                                   as latest_paid_at,
        string_agg(distinct purchased_product, ' | ')           as purchased_products,

        subscriber_id,
        subscription_id,
        subscription_status,
        subscriber_product_id,
        subscriber_product_title,
        auto_renew_count,
        has_charge_consent,
        fanbasis_total_transactions,
        fanbasis_total_spent,
        fanbasis_last_transaction_at
    from enriched_unmatched_payments
    group by
        fanbasis_customer_id,
        fanbasis_directory_customer_id,
        buyer_name,
        buyer_email,
        buyer_email_norm,
        buyer_phone,
        subscriber_id,
        subscription_id,
        subscription_status,
        subscriber_product_id,
        subscriber_product_title,
        auto_renew_count,
        has_charge_consent,
        fanbasis_total_transactions,
        fanbasis_total_spent,
        fanbasis_last_transaction_at

),

with_ghl_check as (

    select
        rolled_up.fanbasis_customer_id,
        rolled_up.fanbasis_directory_customer_id,
        rolled_up.buyer_name,
        rolled_up.buyer_email,
        rolled_up.buyer_email_norm,
        rolled_up.buyer_phone,
        rolled_up.paid_payments_count,
        rolled_up.total_net_revenue,
        rolled_up.first_paid_at,
        rolled_up.latest_paid_at,
        rolled_up.purchased_products,
        rolled_up.subscriber_id,
        rolled_up.subscription_id,
        rolled_up.subscription_status,
        rolled_up.subscriber_product_id,
        rolled_up.subscriber_product_title,
        rolled_up.auto_renew_count,
        rolled_up.has_charge_consent,
        rolled_up.fanbasis_total_transactions,
        rolled_up.fanbasis_total_spent,
        rolled_up.fanbasis_last_transaction_at,
        count(distinct contact_by_email.contact_sk)              as ghl_email_match_count,
        count(distinct contact_by_phone.contact_sk)              as ghl_phone_match_count,
        array_agg(
            distinct contact_by_email.contact_id ignore nulls
            limit 3
        )                                                       as sample_email_contact_ids,
        array_agg(
            distinct contact_by_phone.contact_id ignore nulls
            limit 3
        )                                                       as sample_phone_contact_ids
    from rolled_up
    left join contacts as contact_by_email
        on rolled_up.buyer_email_norm = contact_by_email.email_norm
    left join contacts as contact_by_phone
        on regexp_replace(coalesce(rolled_up.buyer_phone, ''), r'[^0-9]', '')
           = contact_by_phone.phone_digits
       and regexp_replace(coalesce(rolled_up.buyer_phone, ''), r'[^0-9]', '') != ''
    group by
        rolled_up.fanbasis_customer_id,
        rolled_up.fanbasis_directory_customer_id,
        rolled_up.buyer_name,
        rolled_up.buyer_email,
        rolled_up.buyer_email_norm,
        rolled_up.buyer_phone,
        rolled_up.paid_payments_count,
        rolled_up.total_net_revenue,
        rolled_up.first_paid_at,
        rolled_up.latest_paid_at,
        rolled_up.purchased_products,
        rolled_up.subscriber_id,
        rolled_up.subscription_id,
        rolled_up.subscription_status,
        rolled_up.subscriber_product_id,
        rolled_up.subscriber_product_title,
        rolled_up.auto_renew_count,
        rolled_up.has_charge_consent,
        rolled_up.fanbasis_total_transactions,
        rolled_up.fanbasis_total_spent,
        rolled_up.fanbasis_last_transaction_at

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'with_ghl_check.fanbasis_customer_id',
            'with_ghl_check.buyer_email_norm'
        ]) }}                                                    as missing_ghl_contact_sk,
        with_ghl_check.*,
        case
            when ghl_email_match_count = 0
                and ghl_phone_match_count = 0
                then 'create_ghl_contact'
            when ghl_email_match_count + ghl_phone_match_count = 1
                then 'repair_identity_bridge'
            else 'review_duplicate_ghl_contacts'
        end                                                       as recommended_action,
        to_json_string(struct(
            buyer_name as name,
            buyer_email as email,
            buyer_phone as phone,
            'Fanbasis paid buyer missing from GHL contact identity' as source,
            fanbasis_customer_id as fanbasis_customer_id,
            fanbasis_directory_customer_id as fanbasis_directory_customer_id
        ))                                                        as suggested_ghl_contact_payload_json,
        current_timestamp()                                       as mart_refreshed_at
    from with_ghl_check

)

select * from final
