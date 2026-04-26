-- U3 column-rename (2026-04-23): see stg_ghl__contacts.sql header.
-- NB: raw_ghl.ghl__users_raw is 0 rows as of U1 preflight — source
-- passes zero rows through; not a U3 bug.
with source as (

    select
        entity_id                                                       as id,
        _ingested_at,
        to_json_string(payload_json)                               as payload
    from {{ source('ghl', 'users') }}

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _ingested_at desc
    ) = 1

),

parsed as (

    select
        id                                              as user_id,

        json_value(payload, '$.email')                  as email,
        json_value(payload, '$.name')                   as full_name,
        json_value(payload, '$.firstName')              as first_name,
        json_value(payload, '$.lastName')               as last_name,
        json_value(payload, '$.phone')                  as phone,

        json_value(payload, '$.roles.type')             as ghl_account_type,
        json_value(payload, '$.roles.role')             as ghl_role,

        cast(json_value(payload, '$.deleted') as bool)  as is_deleted,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
