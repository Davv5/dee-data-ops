-- Grain: one row per payment event, unioned across Stripe + Fanbasis.
-- Fanbasis CTE is a zero-row placeholder (extractor still waiting on
-- Week-0 creds) — shaped so the union is structurally valid and the
-- model ships today. When Fanbasis raw lands, swap the placeholder for
-- a ref() to the forthcoming Fanbasis staging model (name TBD) without
-- touching the downstream union.
--
-- `contact_sk` + bridge metadata come from
-- `bridge_identity_contact_payment`. Amounts land in major units (USD
-- here; non-USD currency rows pass through at face value — multi-currency
-- handling is a mart-layer concern).

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

    -- Placeholder: structural union-parity stub until the Fanbasis
    -- extractor + staging ship. Zero-row-producing `where false` keeps
    -- this model green without mocking payment volume.
    select
        cast(null as string)                                  as payment_id,
        'fanbasis'                                            as source_platform,

        cast(null as float64)                                 as gross_amount,
        cast(null as float64)                                 as net_amount,
        cast(null as string)                                  as currency,

        cast(null as string)                                  as product,

        cast(null as string)                                  as billing_email,
        cast(null as string)                                  as card_issue_country,
        cast(null as string)                                  as payment_method,

        cast(null as timestamp)                               as transaction_date,
        cast(null as bool)                                    as is_captured,
        cast(null as bool)                                    as is_paid,
        cast(null as bool)                                    as is_refunded
    where false

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
