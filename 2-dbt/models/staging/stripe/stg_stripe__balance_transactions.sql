-- U3 blob-shim (2026-05-03): source CTE filters `Raw.stripe_objects_raw`
-- by `object_type = 'balance_transactions'` and JSON-decodes the payload
-- needed to reconcile historical Stripe charges into settlement currency.
--
-- Grain: one row per Stripe balance transaction. PK: `balance_transaction_id`.
-- These rows are the source of truth for historical Stripe settlement amount,
-- fee, net, currency, and exchange rate when a charge was made outside USD.

with

source as (

    select
        json_value(payload_json, '$.id')                                as id,
        json_value(payload_json, '$.source')                            as source_id,

        json_value(payload_json, '$.type')                              as type,
        json_value(payload_json, '$.reporting_category')                as reporting_category,
        json_value(payload_json, '$.description')                       as description,
        json_value(payload_json, '$.status')                            as status,
        json_value(payload_json, '$.currency')                          as currency,

        safe_cast(json_value(payload_json, '$.amount') as int64)        as amount,
        safe_cast(json_value(payload_json, '$.fee') as int64)           as fee,
        safe_cast(json_value(payload_json, '$.net') as int64)           as net,
        safe_cast(json_value(payload_json, '$.exchange_rate') as numeric) as exchange_rate,

        timestamp_seconds(
            safe_cast(json_value(payload_json, '$.available_on') as int64)
        )                                                              as available_on,
        timestamp_seconds(
            safe_cast(json_value(payload_json, '$.created') as int64)
        )                                                              as created,

        ingested_at                                                     as _fivetran_synced
    from {{ source('raw_stripe', 'stripe_objects_raw') }}
    where object_type = 'balance_transactions'

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
        id                                                              as balance_transaction_id,
        source_id,

        type                                                            as balance_transaction_type,
        reporting_category,
        description,
        status,
        currency,

        amount                                                          as amount_minor,
        fee                                                             as fee_minor,
        net                                                             as net_minor,
        exchange_rate,

        available_on                                                    as available_on_at,
        created                                                         as balance_transaction_created_at,

        _fivetran_synced                                                as _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
