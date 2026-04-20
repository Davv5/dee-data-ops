with

source as (

    select * from {{ source('raw_stripe', 'charge') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _fivetran_synced desc
    ) = 1

),

parsed as (

    select
        id                                                              as charge_id,

        customer_id,
        invoice_id,
        payment_intent_id,
        payment_method_id,
        balance_transaction_id,
        transfer_id,

        status,
        currency,

        amount                                                          as amount_minor,
        amount_captured                                                 as amount_captured_minor,
        amount_refunded                                                 as amount_refunded_minor,

        captured                                                        as is_captured,
        paid                                                            as is_paid,
        refunded                                                        as is_refunded,
        livemode                                                        as is_livemode,

        failure_code,
        failure_message,

        outcome_type,
        outcome_network_status,
        outcome_reason,
        outcome_risk_level,
        outcome_risk_score,
        outcome_seller_message,

        billing_detail_email                                            as billing_email,
        billing_detail_name                                             as billing_name,
        billing_detail_phone                                            as billing_phone,
        billing_detail_address_country                                  as billing_country,

        description,
        receipt_email,
        receipt_url,
        statement_descriptor,

        created                                                         as charged_at,

        _fivetran_synced                                                as _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
