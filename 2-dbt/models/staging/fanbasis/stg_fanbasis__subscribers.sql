{{ config(materialized='view') }}

-- Staging view for Fanbasis subscriber/customer subscription objects.
-- Grain: one row per subscriber object from Raw.fanbasis_objects_raw.
-- Source: FanBasis API docs, "Subscribers > List All Subscribers".

with

source as (

    select *
    from {{ source('raw_fanbasis', 'fanbasis_objects_raw') }}
    where object_type = 'subscribers'

),

parsed as (

    select
        cast(json_value(payload_json, '$.id') as string)                as subscriber_id,

        cast(json_value(payload_json, '$.customer.id') as string)       as customer_id,
        json_value(payload_json, '$.customer.name')                     as customer_name,
        json_value(payload_json, '$.customer.email')                    as customer_email,
        lower(trim(json_value(payload_json, '$.customer.email')))       as customer_email_norm,
        json_value(payload_json, '$.customer.phone')                    as customer_phone,
        regexp_replace(
            coalesce(json_value(payload_json, '$.customer.phone'), ''),
            r'[^0-9]',
            ''
        )                                                              as customer_phone_digits,
        json_value(payload_json, '$.customer.country_code')             as customer_country_code,

        cast(json_value(payload_json, '$.product.id') as string)        as product_id,
        json_value(payload_json, '$.product.title')                     as product_title,
        json_value(payload_json, '$.product.internal_name')             as product_internal_name,
        json_value(payload_json, '$.product.description')               as product_description,
        cast(json_value(payload_json, '$.product.price') as numeric)    as product_price,
        json_value(payload_json, '$.product.payment_link')              as product_payment_link,

        cast(json_value(payload_json, '$.subscription.id') as string)   as subscription_id,
        json_value(payload_json, '$.subscription.status')               as subscription_status,
        json_value(payload_json, '$.subscription.service_type')         as service_type,
        json_value(payload_json, '$.subscription.payment_frequency')    as payment_frequency,
        cast(json_value(payload_json, '$.subscription.completion_date') as timestamp)
                                                                       as completion_at,
        cast(json_value(payload_json, '$.subscription.cancelled_at') as timestamp)
                                                                       as cancelled_at,
        cast(json_value(payload_json, '$.subscription.auto_renew_count') as int64)
                                                                       as auto_renew_count,
        case lower(json_value(payload_json, '$.subscription.charge_consent'))
            when '1'     then true
            when 'true'  then true
            when '0'     then false
            when 'false' then false
        end                                                           as has_charge_consent,
        cast(json_value(payload_json, '$.subscription.created_at') as timestamp)
                                                                       as subscription_created_at,
        cast(json_value(payload_json, '$.subscription.updated_at') as timestamp)
                                                                       as subscription_updated_at,

        ingested_at                                                     as _ingested_at
    from source
    where json_value(payload_json, '$.id') is not null

),

final as (

    select * from parsed

)

select * from final
