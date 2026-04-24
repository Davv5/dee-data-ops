-- U3 blob-shim (2026-04-23): source CTE filters `Raw.stripe_objects_raw` by
-- `object_type = 'customers'` and JSON-decodes the payload into the column
-- shape downstream expects. Body of the model (deduped / parsed / final)
-- is unchanged from the Fivetran-shape version.
--
-- TODO: retire when `raw_stripe.stripe__customers_raw` (U7) is populated —
-- swap the source CTE to the per-object table and the rest is untouched.

with

source as (

    select
        json_value(payload_json, '$.id')                                as id,

        json_value(payload_json, '$.email')                             as email,
        json_value(payload_json, '$.name')                              as name,
        json_value(payload_json, '$.phone')                             as phone,
        json_value(payload_json, '$.description')                       as description,

        json_value(payload_json, '$.currency')                          as currency,

        safe_cast(json_value(payload_json, '$.balance') as int64)       as balance,

        json_value(payload_json, '$.address.city')                      as address_city,
        json_value(payload_json, '$.address.state')                     as address_state,
        json_value(payload_json, '$.address.postal_code')               as address_postal_code,
        json_value(payload_json, '$.address.country')                   as address_country,

        safe_cast(json_value(payload_json, '$.delinquent') as bool)     as delinquent,
        safe_cast(json_value(payload_json, '$.livemode') as bool)       as livemode,

        -- `is_deleted` is a Fivetran soft-delete flag. Stripe's native payload
        -- doesn't carry it — default to false. When U7 lands, per-object
        -- tables will either supply this column or the final CTE will
        -- coalesce to false on absence.
        cast(null as bool)                                              as is_deleted,

        json_value(payload_json, '$.invoice_prefix')                    as invoice_prefix,
        json_value(payload_json, '$.invoice_settings.default_payment_method') as invoice_settings_default_payment_method,

        json_value(payload_json, '$.tax_exempt')                        as tax_exempt,

        timestamp_seconds(
            safe_cast(json_value(payload_json, '$.created') as int64)
        )                                                               as created,

        ingested_at                                                     as _fivetran_synced
    from {{ source('raw_stripe', 'stripe_objects_raw') }}
    where object_type = 'customers'

),

deduped as (

    select *
    from source
    qualify row_number() over (
        partition by id
        order by _fivetran_synced desc
    ) = 1

),

parsed as (

    select
        id                                                              as customer_id,

        email,
        name                                                            as customer_name,
        phone,
        description,

        currency,

        balance                                                         as balance_minor,

        address_city,
        address_state,
        address_postal_code,
        address_country,

        delinquent                                                      as is_delinquent,
        livemode                                                        as is_livemode,
        coalesce(is_deleted, false)                                     as is_deleted,

        invoice_prefix,
        invoice_settings_default_payment_method                         as default_payment_method_id,

        tax_exempt,

        created                                                         as customer_created_at,

        _fivetran_synced                                                as _ingested_at
    from deduped

),

final as (

    select * from parsed

)

select * from final
