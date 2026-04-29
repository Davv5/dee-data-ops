-- SDR-role-filtered conformed dimension on dim_users. Not a duplicate — a
-- subset view. Conformed across fct_speed_to_lead_touch and any future fact
-- that needs SDR-grain filtering (e.g., fct_coaching_events). The rename
-- of user_sk → sdr_sk is for business-readability in the mart; the hash is
-- identical so the two are interchangeable for join purposes.
-- (source: "Creating a Data Model w/ dbt: Facts", Data Ops notebook —
-- "You can still use the same dimensions... it's very common, it's called a
-- conformed dimension.")
--
-- SCD-2 note: active_from / active_to columns pass through from
-- dim_users_snapshot via a separate join pattern. For F1, current-state only
-- (is_active = true). F2 will layer in the snapshot as-of join for
-- role-at-touch-time accuracy.

with

users as (

    select
        user_sk,
        user_id,
        name,
        email,
        role,
        is_active
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users`
    where lower(role) = 'sdr'
      and is_active = true

),

snapshot_history as (

    select
        user_id,
        dbt_valid_from                                          as active_from,
        dbt_valid_to                                            as active_to
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users_snapshot`

),

-- Join to snapshot to surface active_from / active_to from SCD-2 history.
-- For current-row SDRs, take the open snapshot row (dbt_valid_to IS NULL).
latest_snapshot as (

    select
        user_id,
        active_from,
        active_to
    from snapshot_history
    where active_to is null

),

final as (

    select
        -- sdr_sk uses same generate_surrogate_key as dim_users.user_sk
        -- (both hash over user_id) so the keys are interchangeable
        to_hex(md5(cast(coalesce(cast(users.user_id as string), '_dbt_utils_surrogate_key_null_') as string)))
                                                                as sdr_sk,

        users.user_sk,
        users.user_id,
        users.name                                              as sdr_name,
        users.email,
        users.role,
        users.is_active,

        ls.active_from,
        ls.active_to

    from users
    left join latest_snapshot ls
        on ls.user_id = users.user_id

)

select * from final