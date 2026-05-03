

-- Staging view for Fanbasis customer directory objects.
-- Grain: one row per Fanbasis customer from Raw.fanbasis_objects_raw.
-- Source: FanBasis API docs, "Customers > List Customers".

with

source as (

    select *
    from `project-41542e21-470f-4589-96d`.`Raw`.`fanbasis_objects_raw`
    where object_type = 'customers'

),

parsed as (

    select
        cast(json_value(payload_json, '$.id') as string)                as customer_id,
        json_value(payload_json, '$.name')                              as customer_name,
        json_value(payload_json, '$.email')                             as customer_email,
        lower(trim(json_value(payload_json, '$.email')))                as customer_email_norm,
        json_value(payload_json, '$.phone')                             as customer_phone,
        regexp_replace(
            coalesce(json_value(payload_json, '$.phone'), ''),
            r'[^0-9]',
            ''
        )                                                              as customer_phone_digits,
        json_value(payload_json, '$.country_code')                      as customer_country_code,

        cast(json_value(payload_json, '$.total_transactions') as int64) as total_transactions,
        cast(json_value(payload_json, '$.total_spent') as numeric)      as total_spent,
        cast(json_value(payload_json, '$.last_transaction_date') as timestamp)
                                                                       as last_transaction_at,

        ingested_at                                                     as _ingested_at
    from source
    where json_value(payload_json, '$.id') is not null

),

final as (

    select * from parsed

)

select * from final