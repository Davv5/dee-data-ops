with

source as (

    select * from {{ source('raw_stripe', 'customer') }}

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
