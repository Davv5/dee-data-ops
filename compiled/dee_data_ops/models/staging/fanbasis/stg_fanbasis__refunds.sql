

-- Staging view for Fanbasis refund events. Reads the same raw table
-- as `stg_fanbasis__transactions` (`Raw.fanbasis_transactions_txn_raw`)
-- but unnests the `$.refunds` JSON array — one row per refund event,
-- not one row per parent transaction. Multi-refund-per-payment is
-- supported even though current data is single-refund (9 of 466 raw
-- transactions, all single-element arrays as of 2026-04-28).
--
-- Grain: one row per refund event.
-- PK: `refund_id` (Fanbasis-native id, stringified).
-- FK: `payment_id` joins back to `stg_fanbasis__transactions.payment_id`
-- and to `fct_payments` via `(source_platform='fanbasis', payment_id)`.
--
-- Refund payload field map (from raw inspection):
--   $.refunds[*].id            → refund_id           (Fanbasis-native PK)
--   $.refunds[*].payment_id    → payment_id          (parent transaction)
--   $.refunds[*].amount        → refund_amount       (gross USD returned to customer)
--   $.refunds[*].amount_gross  → refund_amount_net   (amount minus processor fee)
--   $.refunds[*].fee           → refund_fee          (additional processor fee)
--   $.refunds[*].refund_cost   → refund_total_cost   (total cost to merchant)
--   $.refunds[*].created_at    → refunded_at
--
-- Currency: hardcoded `'usd'` to match the parent transaction (Fanbasis
-- payloads at this tenant carry no currency field — see
-- `stg_fanbasis__transactions.sql` header).

with

source as (

    select
        cast(json_value(payload_json, '$.id') as string)              as parent_payment_id,
        json_query_array(payload_json, '$.refunds')                   as refunds_array,
        ingested_at
    from `project-41542e21-470f-4589-96d`.`Raw`.`fanbasis_transactions_txn_raw`
    where json_value(payload_json, '$.id') is not null
        and array_length(json_query_array(payload_json, '$.refunds')) > 0

),

unnested as (

    select
        source.parent_payment_id,
        refund_json,
        source.ingested_at
    from source,
    unnest(source.refunds_array) as refund_json

),

parsed as (

    select
        cast(json_value(refund_json, '$.id') as string)               as refund_id,
        -- Use the OUTER transaction's `$.id` as the parent payment id,
        -- not the refund's own `$.payment_id`. The two should match by
        -- Fanbasis's API contract, but using the outer id makes the
        -- staging-side natural-key relationship robust against any
        -- future denormalization drift (a refund whose inner
        -- `payment_id` disagrees with its landing transaction id would
        -- silently misattribute the contact otherwise).
        parent_payment_id                                             as payment_id,

        cast(json_value(refund_json, '$.amount') as numeric)          as refund_amount,
        cast(json_value(refund_json, '$.amount_gross') as numeric)    as refund_amount_net,
        cast(json_value(refund_json, '$.fee') as numeric)             as refund_fee,
        cast(json_value(refund_json, '$.refund_cost') as numeric)     as refund_total_cost,
        'usd'                                                         as currency,

        cast(json_value(refund_json, '$.created_at') as timestamp)    as refunded_at,

        ingested_at                                                   as _ingested_at
    from unnested
    where json_value(refund_json, '$.id') is not null

),

final as (

    select * from parsed

)

select * from final