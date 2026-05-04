-- Grain: one row per payment event, unioned across Stripe + Fanbasis.
-- Stripe rows are historical-only (Stripe banned at D-DEE per memory
-- `project_stripe_historical_only.md`); Fanbasis is the live forward-going
-- contributor.
--
-- `contact_sk` + bridge metadata come from
-- `bridge_identity_contact_payment`, joined on `(source_platform,
-- payment_id)` so a Stripe `charge_id` and a Fanbasis `payment_id` cannot
-- collide. Reporting amounts land in settlement currency major units where
-- Stripe balance transactions are available; native presentment amounts stay
-- in source_* audit columns so non-USD historical Stripe charges do not get
-- displayed as USD at mart/dashboard time.

with

stripe_invoice_lines as (

    select
        json_value(invoices.payload_json, '$.id')                    as invoice_id,
        coalesce(
            json_value(invoice_line, '$.price.product'),
            json_value(invoice_line, '$.plan.product'),
            json_value(invoice_line, '$.pricing.price_details.product')
        )                                                           as product_id,
        coalesce(
            safe_cast(json_value(invoice_line, '$.price.unit_amount') as numeric) / 100,
            safe_cast(json_value(invoice_line, '$.plan.amount') as numeric) / 100,
            safe_cast(json_value(invoice_line, '$.amount') as numeric) / 100
        )                                                           as product_price,
        nullif(json_value(invoice_line, '$.description'), '')        as line_description,
        invoices.ingested_at
    from {{ source('raw_stripe', 'stripe_objects_raw') }} as invoices,
        unnest(ifnull(json_query_array(invoices.payload_json, '$.lines.data'), []))
            as invoice_line
    where invoices.object_type = 'invoices'
    qualify row_number() over (
        partition by
            json_value(invoices.payload_json, '$.id'),
            coalesce(
                json_value(invoice_line, '$.price.product'),
                json_value(invoice_line, '$.plan.product'),
                json_value(invoice_line, '$.pricing.price_details.product')
            ),
            nullif(json_value(invoice_line, '$.description'), '')
        order by invoices.ingested_at desc
    ) = 1

),

stripe_products as (

    select
        json_value(payload_json, '$.id')                             as product_id,
        nullif(json_value(payload_json, '$.name'), '')               as product_name,
        nullif(json_value(payload_json, '$.description'), '')        as product_description,
        ingested_at
    from {{ source('raw_stripe', 'stripe_objects_raw') }}
    where object_type = 'products'
    qualify row_number() over (
        partition by json_value(payload_json, '$.id')
        order by ingested_at desc
    ) = 1

),

stripe_invoice_products as (

    select
        stripe_invoice_lines.invoice_id,
        string_agg(distinct stripe_invoice_lines.product_id, ' | ' order by stripe_invoice_lines.product_id)
                                                                    as product_id,
        string_agg(distinct stripe_products.product_name, ' | ' order by stripe_products.product_name)
                                                                    as product_name,
        string_agg(distinct stripe_invoice_lines.line_description, ' | ' order by stripe_invoice_lines.line_description)
                                                                    as line_description,
        max(stripe_invoice_lines.product_price)                      as product_price
    from stripe_invoice_lines
    left join stripe_products
        on stripe_invoice_lines.product_id = stripe_products.product_id
    group by 1

),

stripe_product_repairs as (

    select
        payment_id,
        repaired_product,
        product_repair_source,
        product_repair_confidence,
        product_repair_note
    from {{ ref('bridge_stripe_payment_product_repair') }}

),

stripe_payments as (

    select
        charges.charge_id                                     as payment_id,
        'stripe'                                              as source_platform,

        case
            when charges.is_paid
                then coalesce(
                    cast(balance_transactions.amount_minor as numeric) / 100,
                    cast(charges.amount_minor as numeric) / 100
                )
            else 0
        end                                                   as gross_amount,
        case
            when not charges.is_paid
                then 0
            when balance_transactions.net_minor is not null
                then
                    cast(balance_transactions.net_minor as numeric) / 100
                    - (
                        cast(coalesce(charges.amount_refunded_minor, 0) as numeric)
                        * coalesce(balance_transactions.exchange_rate, 1)
                        / 100
                    )
            else
                cast(
                    coalesce(charges.amount_captured_minor, charges.amount_minor, 0)
                    - coalesce(charges.amount_refunded_minor, 0)
                    as numeric
                ) / 100
        end                                                   as net_amount,
        coalesce(balance_transactions.currency, charges.currency)
                                                              as currency,

        cast(charges.amount_minor as numeric) / 100           as source_presentment_gross_amount,
        case
            when charges.is_paid
                then cast(
                    coalesce(charges.amount_captured_minor, charges.amount_minor, 0)
                    - coalesce(charges.amount_refunded_minor, 0)
                    as numeric
                ) / 100
            else 0
        end                                                   as source_presentment_net_amount,
        charges.currency                                      as source_presentment_currency,
        cast(balance_transactions.fee_minor as numeric) / 100 as source_settlement_fee_amount,
        balance_transactions.exchange_rate                    as source_exchange_rate,
        charges.balance_transaction_id                        as source_balance_transaction_id,

        case
            when charges.description is null
                or lower(charges.description) in (
                    'subscription creation',
                    'subscription update'
                )
                then coalesce(
                    stripe_invoice_products.product_name,
                    stripe_invoice_products.line_description,
                    stripe_product_repairs.repaired_product,
                    charges.description
                )
            else charges.description
        end                                                   as product,
        case
            when stripe_invoice_products.product_name is not null
                then 'stripe_invoice_product'
            when stripe_invoice_products.line_description is not null
                then 'stripe_invoice_line_description'
            when
                charges.description is null
                or lower(charges.description) in (
                    'subscription creation',
                    'subscription update'
                )
                then stripe_product_repairs.product_repair_source
            else 'stripe_charge_description'
        end                                                   as product_attribution_source,
        case
            when stripe_invoice_products.product_name is not null
                then 'high'
            when stripe_invoice_products.line_description is not null
                then 'high'
            when
                charges.description is null
                or lower(charges.description) in (
                    'subscription creation',
                    'subscription update'
                )
                then stripe_product_repairs.product_repair_confidence
            else 'high'
        end                                                   as product_attribution_confidence,
        case
            when
                charges.description is null
                or lower(charges.description) in (
                    'subscription creation',
                    'subscription update'
                )
                then stripe_product_repairs.product_repair_note
        end                                                   as product_attribution_note,
        stripe_invoice_products.product_id                    as source_product_id,
        stripe_invoice_products.line_description              as source_product_internal_name,
        stripe_invoice_products.product_price                 as source_product_price,
        cast(null as string)                                  as source_service_id,
        cast(null as string)                                  as source_service_title,
        cast(null as string)                                  as source_service_internal_name,
        cast(null as numeric)                                 as source_service_price,
        cast(null as string)                                  as source_service_payment_id,
        cast(null as timestamp)                               as source_fund_release_on,
        cast(null as bool)                                    as source_fund_released,

        charges.billing_email,
        charges.billing_country                               as card_issue_country,
        charges.outcome_network_status                        as payment_method,

        charges.charged_at                                    as transaction_date,
        charges.is_captured,
        charges.is_paid,
        charges.is_refunded
    from {{ ref('stg_stripe__charges') }} as charges
    left join {{ ref('stg_stripe__balance_transactions') }} as balance_transactions
        on charges.balance_transaction_id = balance_transactions.balance_transaction_id
    left join stripe_invoice_products
        on charges.invoice_id = stripe_invoice_products.invoice_id
    left join stripe_product_repairs
        on charges.charge_id = stripe_product_repairs.payment_id

),

fanbasis_payments as (

    select
        payment_id                                            as payment_id,
        'fanbasis'                                            as source_platform,

        gross_amount,
        net_amount,
        currency,
        gross_amount                                          as source_presentment_gross_amount,
        net_amount                                            as source_presentment_net_amount,
        currency                                              as source_presentment_currency,
        fee_amount                                            as source_settlement_fee_amount,
        cast(null as numeric)                                 as source_exchange_rate,
        cast(null as string)                                  as source_balance_transaction_id,

        product_title                                         as product,
        if(product_title is null, null, 'fanbasis_product')    as product_attribution_source,
        if(product_title is null, null, 'high')                as product_attribution_confidence,
        cast(null as string)                                  as product_attribution_note,
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
    from {{ ref('stg_fanbasis__transactions') }}

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
    from {{ ref('bridge_identity_contact_payment') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(
            ['unioned.source_platform', 'unioned.payment_id']
        ) }}                                                  as payment_sk,

        bridge.contact_sk,

        unioned.payment_id,
        unioned.source_platform,

        unioned.transaction_date,
        unioned.gross_amount,
        unioned.net_amount,
        unioned.currency,
        unioned.source_presentment_gross_amount,
        unioned.source_presentment_net_amount,
        unioned.source_presentment_currency,
        unioned.source_settlement_fee_amount,
        unioned.source_exchange_rate,
        unioned.source_balance_transaction_id,

        unioned.product,
        unioned.product_attribution_source,
        unioned.product_attribution_confidence,
        unioned.product_attribution_note,
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
