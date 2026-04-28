-- Asserts the two Fanbasis staging views agree on which transactions
-- have refunds. `stg_fanbasis__transactions.is_refunded` is derived
-- from `array_length($.refunds) > 0`; `stg_fanbasis__refunds` unnests
-- the same array. The set of `payment_id` values where `is_refunded =
-- true` should equal the set of distinct `payment_id` values that
-- appear in the refunds staging.
--
-- Catches: (a) extractor regression where `$.refunds` lands as a
-- JSON-string instead of an array (silent zero-rows in refunds view
-- but `is_refunded` still true), (b) refund-object missing `$.id` so
-- it gets dropped by the staging filter (parent has is_refunded=true
-- but no refund row exists), (c) inverse — refunds staging lands rows
-- whose parent transaction was filtered out.
--
-- Returns one row per offending payment_id; test passes when zero
-- rows return.

with transactions_with_refunds as (

    select payment_id
    from {{ ref('stg_fanbasis__transactions') }}
    where is_refunded = true

),

refunds_parents as (

    select distinct payment_id
    from {{ ref('stg_fanbasis__refunds') }}

),

mismatches as (

    select
        coalesce(t.payment_id, r.payment_id) as payment_id,
        case
            when t.payment_id is null then 'orphan_refund_no_parent'
            when r.payment_id is null then 'parent_marked_refunded_no_event'
        end                                  as failure_mode
    from transactions_with_refunds t
    full outer join refunds_parents r
        on t.payment_id = r.payment_id
    where t.payment_id is null
       or r.payment_id is null

)

select * from mismatches
