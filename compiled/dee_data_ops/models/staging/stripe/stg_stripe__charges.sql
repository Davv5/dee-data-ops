-- U3 blob-shim (2026-04-23): source CTE filters `Raw.stripe_objects_raw` by
-- `object_type = 'charges'` and JSON-decodes the payload into the column shape
-- downstream expects. Body of the model (deduped / parsed / final) is
-- unchanged from the Fivetran-shape version.
--
-- TODO: retire when `raw_stripe.stripe__charges_raw` (U7) is populated — swap
-- the source CTE to read from the per-object source (raw_stripe.charges) with
-- matching columns and the rest of this file is untouched.

with

source as (

    -- Blob shim: produce the Fivetran-shape column set expected by `parsed`.
    select
        json_value(payload_json, '$.id')                                as id,

        json_value(payload_json, '$.customer')                          as customer_id,
        json_value(payload_json, '$.invoice')                           as invoice_id,
        json_value(payload_json, '$.payment_intent')                    as payment_intent_id,
        json_value(payload_json, '$.payment_method')                    as payment_method_id,
        json_value(payload_json, '$.balance_transaction')               as balance_transaction_id,
        json_value(payload_json, '$.transfer_data.destination')         as transfer_id,

        json_value(payload_json, '$.status')                            as status,
        json_value(payload_json, '$.currency')                          as currency,

        safe_cast(json_value(payload_json, '$.amount') as int64)            as amount,
        safe_cast(json_value(payload_json, '$.amount_captured') as int64)   as amount_captured,
        safe_cast(json_value(payload_json, '$.amount_refunded') as int64)   as amount_refunded,

        safe_cast(json_value(payload_json, '$.captured') as bool)       as captured,
        safe_cast(json_value(payload_json, '$.paid') as bool)           as paid,
        safe_cast(json_value(payload_json, '$.refunded') as bool)       as refunded,
        safe_cast(json_value(payload_json, '$.livemode') as bool)       as livemode,

        json_value(payload_json, '$.failure_code')                      as failure_code,
        json_value(payload_json, '$.failure_message')                   as failure_message,

        json_value(payload_json, '$.outcome.type')                      as outcome_type,
        json_value(payload_json, '$.outcome.network_status')            as outcome_network_status,
        json_value(payload_json, '$.outcome.reason')                    as outcome_reason,
        json_value(payload_json, '$.outcome.risk_level')                as outcome_risk_level,
        safe_cast(json_value(payload_json, '$.outcome.risk_score') as int64) as outcome_risk_score,
        json_value(payload_json, '$.outcome.seller_message')            as outcome_seller_message,

        json_value(payload_json, '$.billing_details.email')             as billing_detail_email,
        json_value(payload_json, '$.billing_details.name')              as billing_detail_name,
        json_value(payload_json, '$.billing_details.phone')             as billing_detail_phone,
        json_value(payload_json, '$.billing_details.address.country')   as billing_detail_address_country,

        json_value(payload_json, '$.description')                       as description,
        json_value(payload_json, '$.receipt_email')                     as receipt_email,
        json_value(payload_json, '$.receipt_url')                       as receipt_url,
        json_value(payload_json, '$.statement_descriptor')              as statement_descriptor,

        -- Stripe `created` is unix epoch seconds; preserve downstream cast.
        timestamp_seconds(
            safe_cast(json_value(payload_json, '$.created') as int64)
        )                                                               as created,

        ingested_at                                                     as _fivetran_synced
    from `project-41542e21-470f-4589-96d`.`Raw`.`stripe_objects_raw`
    where object_type = 'charges'

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