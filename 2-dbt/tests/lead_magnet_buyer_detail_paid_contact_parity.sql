-- Asserts lead_magnet_buyer_detail preserves one row per matched paid contact.

with

paid_contacts as (

    select count(distinct contact_sk) as paid_contact_count
    from {{ ref('fct_payments') }}
    where is_paid = true
        and contact_sk is not null

),

mart as (

    select count(*) as buyer_count
    from {{ ref('lead_magnet_buyer_detail') }}

),

comparison as (

    select
        paid_contacts.paid_contact_count,
        mart.buyer_count
    from paid_contacts
    cross join mart

)

select *
from comparison
where paid_contact_count != buyer_count
