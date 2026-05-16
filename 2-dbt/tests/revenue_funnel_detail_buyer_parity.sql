-- Asserts revenue_funnel_detail preserves the same buyer set and buyer-level
-- net revenue as lead_magnet_buyer_detail. This catches accidental fan-out
-- from outreach, bookings, or payment/product rollups.

with

lead_magnet_buyers as (

    select
        contact_sk,
        total_net_revenue_after_refunds
    from {{ ref('lead_magnet_buyer_detail') }}

),

revenue_funnel as (

    select
        contact_sk,
        total_net_revenue_after_refunds
    from {{ ref('revenue_funnel_detail') }}

),

buyer_set_mismatches as (

    select
        coalesce(lead_magnet_buyers.contact_sk, revenue_funnel.contact_sk)
                                                                    as contact_sk,
        case
            when lead_magnet_buyers.contact_sk is null
                then 'extra_revenue_funnel_buyer'
            when revenue_funnel.contact_sk is null
                then 'missing_revenue_funnel_buyer'
        end                                                         as issue
    from lead_magnet_buyers
    full outer join revenue_funnel
        on lead_magnet_buyers.contact_sk = revenue_funnel.contact_sk
    where lead_magnet_buyers.contact_sk is null
        or revenue_funnel.contact_sk is null

),

totals as (

    select
        (select count(*) from lead_magnet_buyers)                   as lead_magnet_rows,
        (select count(*) from revenue_funnel)                       as revenue_funnel_rows,
        (
            select sum(total_net_revenue_after_refunds)
            from lead_magnet_buyers
        )                                                          as lead_magnet_net_revenue,
        (
            select sum(total_net_revenue_after_refunds)
            from revenue_funnel
        )                                                          as revenue_funnel_net_revenue

),

total_mismatches as (

    select
        cast(null as string)                                       as contact_sk,
        'buyer_count_or_revenue_total_mismatch'                    as issue
    from totals
    where lead_magnet_rows != revenue_funnel_rows
        or abs(
            lead_magnet_net_revenue - revenue_funnel_net_revenue
        ) > 0.01

),

final as (

    select * from buyer_set_mismatches
    union all
    select * from total_mismatches

)

select * from final
