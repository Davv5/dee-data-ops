-- Asserts the bridge contains exactly one row per upstream payment.
-- Catches the latent failure where a payment falls through every tier
-- (e.g., phone-only payment with no contact phone match in `dim_contacts`)
-- and is silently dropped from the bridge — which would propagate as a
-- NULL bridge join in `fct_payments` instead of `bridge_status =
-- 'unmatched'`. Per `_bridges__docs.md`: "every payment gets exactly
-- one row" — this test is the enforceable form of that promise.
--
-- Returns one row per `source_platform` whose bridge count differs from
-- staging count. Test passes when zero rows return.

with bridge_counts as (

    select
        source_platform,
        count(*) as bridge_n
    from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`
    group by 1

),

staging_counts as (

    select
        'stripe'                                                  as source_platform,
        count(*)                                                  as staging_n
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_stripe__charges`
    where charge_id is not null

    union all

    select
        'fanbasis'                                                as source_platform,
        count(*)                                                  as staging_n
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_fanbasis__transactions`
    where payment_id is not null

),

joined as (

    select
        staging_counts.source_platform,
        staging_counts.staging_n,
        coalesce(bridge_counts.bridge_n, 0)                       as bridge_n,
        staging_counts.staging_n - coalesce(bridge_counts.bridge_n, 0)
                                                                  as missing_from_bridge
    from staging_counts
    left join bridge_counts
        on bridge_counts.source_platform = staging_counts.source_platform

)

select * from joined
where missing_from_bridge != 0