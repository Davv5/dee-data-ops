{{ config(materialized='view') }}

-- Staging view for source fanbasis, table fanbasis_transactions_txn_raw.
-- 1:1 cleanup only: lowercase rename + light type casts on the JSON payload.
-- No joins, no aggregations, no business logic per `.claude/rules/staging.md`.
-- Downstream `fct_payments` ref()s this view; column shape is the union-arm
-- contract for the Fanbasis side of the Stripe + Fanbasis revenue fact.
--
-- Grain: one row per Fanbasis transaction. PK: `payment_id` (cast to string
-- for cross-platform uniqueness in the warehouse fact's surrogate-key hash).
--
-- Currency: Fanbasis payloads in D-DEE's tenant carry no `currency` field
-- (USD-only at this account). Hardcoded `'usd'` per the staging "rename and
-- light cast" mandate; if Fanbasis ever multi-currencies we surface the gap
-- here, not in the fact.
--
-- Refund detection: `$.refunds` is a JSON array; non-empty array => refunded.
-- Profile (2026-04-27): 9 of 466 rows have a non-empty refunds array.

with

source as (

    select * from {{ source('raw_fanbasis', 'fanbasis_transactions_txn_raw') }}

),

parsed as (

    select
        cast(json_value(payload_json, '$.id') as string)                                  as payment_id,

        cast(json_value(payload_json, '$.amount') as numeric)                             as gross_amount,
        cast(json_value(payload_json, '$.net_amount') as numeric)                         as net_amount,
        cast(json_value(payload_json, '$.fee_amount') as numeric)                         as fee_amount,
        'usd'                                                                             as currency,

        json_value(payload_json, '$.product.title')                                       as product_title,
        json_value(payload_json, '$.product.id')                                          as product_id,
        json_value(payload_json, '$.product.internal_name')                               as product_internal_name,
        json_value(payload_json, '$.product.description')                                 as product_description,
        cast(json_value(payload_json, '$.product.price') as numeric)                      as product_price,
        json_value(payload_json, '$.product.payment_link')                                as product_payment_link,

        json_value(payload_json, '$.service.title')                                       as service_title,
        json_value(payload_json, '$.service.id')                                          as service_id,
        json_value(payload_json, '$.service.internal_name')                               as service_internal_name,
        json_value(payload_json, '$.service.description')                                 as service_description,
        cast(json_value(payload_json, '$.service.price') as numeric)                      as service_price,
        json_value(payload_json, '$.service.payment_link')                                as service_payment_link,

        json_value(payload_json, '$.servicePayment.payment_type')                         as payment_type,
        json_value(payload_json, '$.servicePayment.id')                                   as service_payment_id,
        cast(json_value(payload_json, '$.servicePayment.fund_release_on') as timestamp)    as fund_release_on,
        case lower(json_value(payload_json, '$.servicePayment.fund_released'))
            when '1'     then true
            when 'true'  then true
            when '0'     then false
            when 'false' then false
        end                                                                               as fund_released,

        json_value(payload_json, '$.fan.id')                                              as fan_id,
        json_value(payload_json, '$.fan.email')                                           as fan_email,
        json_value(payload_json, '$.fan.name')                                            as fan_name,
        json_value(payload_json, '$.fan.phone')                                           as fan_phone,
        json_value(payload_json, '$.fan.country_code')                                    as fan_country_code,

        cast(json_value(payload_json, '$.transaction_date') as timestamp)                 as transaction_date,

        -- Fanbasis transactions in the raw landing are settled payments —
        -- no pending/auth-only state surfaces in the payload. Captured + paid
        -- are true by construction of the source.
        true                                                                              as is_captured,
        true                                                                              as is_paid,

        -- Refunds is a JSON array; non-empty => at least partial refund.
        coalesce(
            array_length(json_query_array(payload_json, '$.refunds')) > 0,
            false
        )                                                                                 as is_refunded,

        ingested_at                                                                       as _ingested_at
    from source
    where json_value(payload_json, '$.id') is not null

),

final as (

    select * from parsed

)

select * from final
