-- Grain: one row per payment event, unioned across Stripe + Fanbasis.
-- Stripe rows are historical-only (Stripe banned at D-DEE per memory
-- `project_stripe_historical_only.md`); Fanbasis is the live forward-going
-- contributor.
--
-- `contact_sk` + bridge metadata come from
-- `bridge_identity_contact_payment`. Amounts land in major units (USD
-- here; non-USD currency rows pass through at face value — multi-currency
-- handling is a mart-layer concern).
--
-- Bridge gap (follow-up): `bridge_identity_contact_payment` currently sources
-- only from `stg_stripe__charges`, so Fanbasis `payment_id` rows will
-- left-join to a null bridge row and surface as `bridge_status = 'unmatched'`
-- via the coalesce in `final`. Extending the bridge to UNION-ALL Fanbasis is
-- a separate ticket (see staging view docstring).

with

stripe_payments as (

    select
        charge_id                                             as payment_id,
        'stripe'                                              as source_platform,

        amount_minor          / 100.0                         as gross_amount,
        (amount_captured_minor - coalesce(amount_refunded_minor, 0))
            / 100.0                                           as net_amount,
        currency,

        description                                           as product,

        billing_email,
        billing_country                                       as card_issue_country,
        outcome_network_status                                as payment_method,

        charged_at                                            as transaction_date,
        is_captured,
        is_paid,
        is_refunded
    from {{ ref('stg_stripe__charges') }}

),

fanbasis_payments as (

    select
        payment_id                                            as payment_id,
        'fanbasis'                                            as source_platform,

        gross_amount,
        net_amount,
        currency,

        product_title                                         as product,

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

        unioned.product,
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
        on unioned.payment_id = bridge.payment_id

)

select * from final
