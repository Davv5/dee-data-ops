-- Grain: one row per payment event, unioned across Stripe + Fanbasis.
-- Stripe rows are historical-only (Stripe banned at D-DEE per memory
-- `project_stripe_historical_only.md`); Fanbasis is the live forward-going
-- contributor.
--
-- `contact_sk` + bridge metadata come from
-- `bridge_identity_contact_payment`, joined on `(source_platform,
-- payment_id)` so a Stripe `charge_id` and a Fanbasis `payment_id` cannot
-- collide. Amounts land in major units (USD here; non-USD currency rows
-- pass through at face value — multi-currency handling is a mart-layer
-- concern).

with

stripe_payments as (

    select
        charge_id                                             as payment_id,
        'stripe'                                              as source_platform,

        cast(amount_minor as numeric) / 100                   as gross_amount,
        cast(amount_captured_minor - coalesce(amount_refunded_minor, 0) as numeric)
            / 100                                             as net_amount,
        currency,

        description                                           as product,
        cast(null as string)                                  as source_product_id,
        cast(null as string)                                  as source_product_internal_name,
        cast(null as numeric)                                 as source_product_price,
        cast(null as string)                                  as source_service_id,
        cast(null as string)                                  as source_service_title,
        cast(null as string)                                  as source_service_internal_name,
        cast(null as numeric)                                 as source_service_price,
        cast(null as string)                                  as source_service_payment_id,
        cast(null as timestamp)                               as source_fund_release_on,
        cast(null as bool)                                    as source_fund_released,

        billing_email,
        billing_country                                       as card_issue_country,
        outcome_network_status                                as payment_method,

        charged_at                                            as transaction_date,
        is_captured,
        is_paid,
        is_refunded
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_stripe__charges`

),

fanbasis_payments as (

    select
        payment_id                                            as payment_id,
        'fanbasis'                                            as source_platform,

        gross_amount,
        net_amount,
        currency,

        product_title                                         as product,
        product_id                                            as source_product_id,
        product_internal_name                                 as source_product_internal_name,
        product_price                                         as source_product_price,
        service_id                                            as source_service_id,
        service_title                                         as source_service_title,
        service_internal_name                                 as source_service_internal_name,
        service_price                                         as source_service_price,
        service_payment_id                                    as source_service_payment_id,
        fund_release_on                                       as source_fund_release_on,
        fund_released                                         as source_fund_released,

        fan_email                                             as billing_email,
        fan_country_code                                      as card_issue_country,
        payment_type                                          as payment_method,

        transaction_date,
        is_captured,
        is_paid,
        is_refunded
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_fanbasis__transactions`

),

unioned as (

    select * from stripe_payments
    union all
    select * from fanbasis_payments

),

bridge as (

    select
        source_platform,
        payment_id,
        contact_sk,
        match_method,
        match_score,
        bridge_status
    from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`

),

final as (

    select
        to_hex(md5(cast(coalesce(cast(unioned.source_platform as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(unioned.payment_id as string), '_dbt_utils_surrogate_key_null_') as string)))                                                  as payment_sk,

        bridge.contact_sk,

        unioned.payment_id,
        unioned.source_platform,

        unioned.transaction_date,
        unioned.gross_amount,
        unioned.net_amount,
        unioned.currency,

        unioned.product,
        unioned.source_product_id,
        unioned.source_product_internal_name,
        unioned.source_product_price,
        unioned.source_service_id,
        unioned.source_service_title,
        unioned.source_service_internal_name,
        unioned.source_service_price,
        unioned.source_service_payment_id,
        unioned.source_fund_release_on,
        unioned.source_fund_released,
        unioned.payment_method,
        unioned.card_issue_country,

        unioned.is_captured,
        unioned.is_paid,
        unioned.is_refunded,

        coalesce(bridge.match_method,  'unmatched')           as match_method,
        coalesce(bridge.match_score,   0.00)                  as match_score,
        coalesce(bridge.bridge_status, 'unmatched')           as bridge_status

    from unioned
    left join bridge
        on unioned.source_platform = bridge.source_platform
       and unioned.payment_id      = bridge.payment_id

)

select * from final