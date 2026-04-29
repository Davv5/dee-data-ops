-- Grain: one row per refund event (Fanbasis-only).
-- PK: `refund_sk`, surrogate over `(source_platform, refund_id)`.
--
-- Fanbasis is the only event-grain refund source today. Stripe staging
-- exposes only `amount_refunded_minor` aggregated per charge — that's
-- charge-grain, not refund-event-grain, so a Stripe arm here would
-- require an extractor change first. Stripe is also banned at D-DEE
-- (memory `project_stripe_historical_only.md`) so the asymmetry is
-- correct in practice: live forward-going refunds are 100% Fanbasis.
-- If Stripe historical refund timing ever matters, scaffold a separate
-- `stg_stripe__refunds` from the Stripe payload first; do not collapse
-- charge-grain refund_amount into this table (would silently double-
-- count multi-refund charges and lose temporal fidelity).
--
-- Contact attribution: refunds inherit the contact from the parent
-- payment, so the bridge is joined on `(source_platform='fanbasis',
-- parent payment_id)` — the same composite key `fct_payments.payment_sk`
-- is hashed over. This is not a fact-to-fact join (the bridge is not a
-- fact); the parent payment is referenced by its native id, not via
-- `fct_payments.payment_sk`.

with

fanbasis_refunds as (

    select
        refund_id,
        payment_id                                            as parent_payment_id,
        'fanbasis'                                            as source_platform,

        refund_amount,
        refund_amount_net,
        refund_fee,
        refund_total_cost,
        currency,

        refunded_at
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_fanbasis__refunds`

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
        to_hex(md5(cast(coalesce(cast(fanbasis_refunds.source_platform as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fanbasis_refunds.refund_id as string), '_dbt_utils_surrogate_key_null_') as string)))                                                  as refund_sk,

        bridge.contact_sk,

        fanbasis_refunds.refund_id,
        fanbasis_refunds.parent_payment_id,
        fanbasis_refunds.source_platform,

        fanbasis_refunds.refunded_at,

        fanbasis_refunds.refund_amount,
        fanbasis_refunds.refund_amount_net,
        fanbasis_refunds.refund_fee,
        fanbasis_refunds.refund_total_cost,
        fanbasis_refunds.currency,

        coalesce(bridge.match_method,  'unmatched')           as match_method,
        coalesce(bridge.match_score,   0.00)                  as match_score,
        coalesce(bridge.bridge_status, 'unmatched')           as bridge_status

    from fanbasis_refunds
    left join bridge
        on bridge.source_platform = fanbasis_refunds.source_platform
       and bridge.payment_id      = fanbasis_refunds.parent_payment_id

)

select * from final