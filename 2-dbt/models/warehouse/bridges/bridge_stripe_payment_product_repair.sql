-- Grain: one row per historical Stripe payment with a repaired product label.
--
-- Stripe direct charges often have no invoice, no charge description, and
-- empty metadata. This bridge repairs only those gaps with traceable context:
-- nearby GHL product/funnel context first, nearby Calendly booking context
-- second, and amount-pattern fallback last.

with

stripe_charges as (

    select
        charge_id                                                     as payment_id,
        'stripe'                                                      as source_platform,
        charged_at,
        currency                                                      as source_presentment_currency,
        cast(amount_minor as numeric) / 100                           as source_presentment_gross_amount
    from {{ ref('stg_stripe__charges') }}
    where is_paid

),

payment_bridge as (

    select
        payment_id,
        contact_sk
    from {{ ref('bridge_identity_contact_payment') }}
    where source_platform = 'stripe'
      and contact_sk is not null

),

contacts as (

    select
        contact_sk,
        contact_id
    from {{ ref('dim_contacts') }}

),

opportunity_context as (

    select
        stripe_charges.payment_id,
        opportunities.opportunity_id,
        pipeline_stages.pipeline_name,
        pipeline_stages.stage_name,
        opportunities.opportunity_name,
        concat(
            coalesce(pipeline_stages.pipeline_name, ''),
            ' ',
            coalesce(pipeline_stages.stage_name, ''),
            ' ',
            coalesce(opportunities.opportunity_name, '')
        )                                                             as context_text,
        abs(
            timestamp_diff(
                stripe_charges.charged_at,
                opportunities.opportunity_updated_at,
                hour
            )
        )                                                             as context_hours_distance
    from stripe_charges
    inner join payment_bridge
        on stripe_charges.payment_id = payment_bridge.payment_id
    inner join contacts
        on payment_bridge.contact_sk = contacts.contact_sk
    inner join {{ ref('stg_ghl__opportunities') }} as opportunities
        on contacts.contact_id = opportunities.contact_id
       and opportunities.opportunity_created_at
           <= timestamp_add(stripe_charges.charged_at, interval 30 day)
       and opportunities.opportunity_updated_at
           >= timestamp_sub(stripe_charges.charged_at, interval 90 day)
    left join {{ ref('dim_pipeline_stages') }} as pipeline_stages
        on opportunities.pipeline_stage_id = pipeline_stages.stage_id

),

opportunity_candidates as (

    select
        payment_id,
        opportunity_id                                               as context_id,
        pipeline_name                                                as context_name,
        stage_name                                                   as context_detail,
        case
            when regexp_contains(
                lower(context_text),
                r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)'
            )
                then 'Rich Of Clothes'
            when regexp_contains(
                lower(context_text),
                r'inner\s+cirlce|inner\s+circle|ic\s*(relaunch|2|2\.0)'
            )
                then 'Mind of Dee Inner Circle'
            when regexp_contains(lower(context_text), r'brand\s+scaling|blueprint')
                then 'Brand Scaling Blueprint'
        end                                                          as repaired_product,
        'ghl_pipeline_context'                                       as product_repair_source,
        'high'                                                       as product_repair_confidence,
        'Nearby GHL opportunity context matched a known product family.'
                                                                      as product_repair_note,
        case
            when regexp_contains(
                lower(context_text),
                r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)'
            )
                then 10
            when regexp_contains(
                lower(context_text),
                r'inner\s+cirlce|inner\s+circle|ic\s*(relaunch|2|2\.0)'
            )
                then 20
            when regexp_contains(lower(context_text), r'brand\s+scaling|blueprint')
                then 30
            else 99
        end                                                          as product_priority,
        context_hours_distance
    from opportunity_context

),

booking_context as (

    select
        stripe_charges.payment_id,
        calls.calendly_event_id,
        calls.event_name,
        abs(
            timestamp_diff(
                stripe_charges.charged_at,
                coalesce(calls.scheduled_for, calls.booked_at),
                hour
            )
        )                                                             as context_hours_distance
    from stripe_charges
    inner join payment_bridge
        on stripe_charges.payment_id = payment_bridge.payment_id
    inner join {{ ref('fct_calls_booked') }} as calls
        on payment_bridge.contact_sk = calls.contact_sk
       and coalesce(calls.scheduled_for, calls.booked_at)
           <= timestamp_add(stripe_charges.charged_at, interval 14 day)
       and coalesce(calls.scheduled_for, calls.booked_at)
           >= timestamp_sub(stripe_charges.charged_at, interval 90 day)

),

booking_candidates as (

    select
        payment_id,
        calendly_event_id                                            as context_id,
        event_name                                                   as context_name,
        cast(null as string)                                         as context_detail,
        case
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)'
            )
                then 'Rich Of Clothes'
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'inner\s+cirlce|inner\s+circle|ic\s*(relaunch|2|2\.0)'
            )
                then 'Mind of Dee Inner Circle'
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'brand\s+scaling|blueprint'
            )
                then 'Brand Scaling Blueprint'
        end                                                          as repaired_product,
        'calendly_booking_context'                                   as product_repair_source,
        'high'                                                       as product_repair_confidence,
        'Nearby Calendly booking context matched a known product family.'
                                                                      as product_repair_note,
        case
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'rich\s+(of|off|o0f)\s+clothes|(^|[^a-z])roc([^a-z]|$)'
            )
                then 15
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'inner\s+cirlce|inner\s+circle|ic\s*(relaunch|2|2\.0)'
            )
                then 25
            when regexp_contains(
                lower(coalesce(event_name, '')),
                r'brand\s+scaling|blueprint'
            )
                then 35
            else 99
        end                                                          as product_priority,
        context_hours_distance
    from booking_context

),

amount_candidates as (

    select
        payment_id,
        cast(null as string)                                         as context_id,
        cast(round(source_presentment_gross_amount, 2) as string)    as context_name,
        source_presentment_currency                                  as context_detail,
        case
            when round(source_presentment_gross_amount, 2) in (
                800, 875, 999, 1000, 1199, 1499, 1999, 2000, 2500
            )
                then 'Mind of Dee Inner Circle'
            when round(source_presentment_gross_amount, 2) in (
                45, 45.01, 50, 50.01, 75, 100, 100.01, 120, 125, 127,
                142.30, 150, 167, 170, 175, 180, 185, 200, 200.01, 240,
                250, 275, 300, 375, 399, 400, 440, 450, 454, 499,
                499.01, 500
            )
                then 'Brand Scaling Blueprint'
        end                                                          as repaired_product,
        'stripe_amount_pattern'                                      as product_repair_source,
        'medium'                                                     as product_repair_confidence,
        'No product metadata; repaired from known historical Stripe amount pattern.'
                                                                      as product_repair_note,
        80                                                           as product_priority,
        999999                                                       as context_hours_distance
    from stripe_charges

),

candidates as (

    select * from opportunity_candidates
    where repaired_product is not null

    union all

    select * from booking_candidates
    where repaired_product is not null

    union all

    select * from amount_candidates
    where repaired_product is not null

),

final as (

    select
        source_platform,
        payment_id,
        repaired_product,
        product_repair_source,
        product_repair_confidence,
        product_repair_note,
        context_id,
        context_name,
        context_detail
    from (
        select
            'stripe' as source_platform,
            candidates.*,
            row_number() over (
                partition by payment_id
                order by
                    product_priority,
                    context_hours_distance,
                    repaired_product
            ) as product_repair_rank
        from candidates
    )
    where product_repair_rank = 1

)

select * from final
