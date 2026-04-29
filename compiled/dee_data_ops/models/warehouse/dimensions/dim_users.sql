with

ghl_users as (

    select * from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__users`

),

roster as (

    select * from `project-41542e21-470f-4589-96d`.`STG`.`ghl_sdr_roster`

),

joined as (

    select
        ghl_users.user_id,

        coalesce(
            ghl_users.full_name,
            concat(
                coalesce(ghl_users.first_name, ''),
                case
                    when ghl_users.last_name is not null then ' '
                    else ''
                end,
                coalesce(ghl_users.last_name, '')
            )
        )                                                    as name,

        ghl_users.first_name,
        ghl_users.last_name,
        ghl_users.email,
        ghl_users.phone,

        ghl_users.ghl_account_type,
        ghl_users.ghl_role,

        coalesce(roster.role, 'unknown')                     as role,

        not coalesce(ghl_users.is_deleted, false)            as is_active,

        ghl_users._ingested_at

    from ghl_users
    left join roster
        on ghl_users.user_id = roster.user_id

),

final as (

    select
        to_hex(md5(cast(coalesce(cast(user_id as string), '_dbt_utils_surrogate_key_null_') as string)))  as user_sk,

        user_id,
        name,
        first_name,
        last_name,
        email,
        phone,

        ghl_account_type,
        ghl_role,
        role,

        is_active,

        _ingested_at

    from joined

)

select * from final