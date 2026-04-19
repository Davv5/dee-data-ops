with source as (

    select
        id,
        _ingested_at,
        payload
    from {{ source('ghl', 'contacts') }}

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
        id                                                    as contact_id,

        json_value(payload, '$.locationId')                   as location_id,
        json_value(payload, '$.assignedTo')                   as assigned_user_id,
        json_value(payload, '$.businessId')                   as business_id,

        json_value(payload, '$.contactName')                  as contact_name,
        json_value(payload, '$.firstName')                    as first_name,
        json_value(payload, '$.lastName')                     as last_name,
        json_value(payload, '$.firstNameRaw')                 as first_name_raw,
        json_value(payload, '$.lastNameRaw')                  as last_name_raw,
        json_value(payload, '$.companyName')                  as company_name,

        json_value(payload, '$.email')                        as email,
        json_value(payload, '$.phone')                        as phone,

        json_value(payload, '$.type')                         as contact_type,
        json_value(payload, '$.source')                       as lead_source,

        cast(json_value(payload, '$.dnd') as bool)            as is_dnd,

        json_value(payload, '$.city')                         as city,
        json_value(payload, '$.state')                        as state,
        json_value(payload, '$.postalCode')                   as postal_code,
        json_value(payload, '$.address1')                     as address1,
        json_value(payload, '$.country')                      as country,
        json_value(payload, '$.timezone')                     as timezone,
        json_value(payload, '$.website')                      as website,

        cast(json_value(payload, '$.dateOfBirth') as date)    as date_of_birth,

        cast(json_value(payload, '$.dateAdded') as timestamp)   as contact_created_at,
        cast(json_value(payload, '$.dateUpdated') as timestamp) as contact_updated_at,

        _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
