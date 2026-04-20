-- D-DEE v1 offer stack. Hand-coded because the offer list is a business
-- artifact (not a source-system table) and the set is small enough that a
-- full seed file + seed-config coordination is unnecessary overhead. Update
-- this model when D-DEE changes the offer stack; downstream facts join on
-- offer_sk via offer_id.

with

offers as (

    select 'OFFER_DDEE_CORE'       as offer_id,
           'D-DEE Core'             as offer_name,
           'coaching'               as product_type,
           'high_ticket'            as offer_tier,
           true                     as is_active
    union all
    select 'OFFER_DDEE_PAYMENT_PLAN',
           'D-DEE Core (Payment Plan)',
           'coaching',
           'high_ticket',
           true

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['offer_id']) }}  as offer_sk,

        offer_id,
        offer_name,
        product_type,
        offer_tier,
        is_active

    from offers

)

select * from final
