-- Asserts lead_magnet_buyer_detail preserves one row per matched paid contact.

with

paid_contacts as (

    select count(distinct contact_sk) as paid_contact_count
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_payments`
    where is_paid = true
        and contact_sk is not null

),

mart as (

    select count(*) as buyer_count
    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_buyer_detail`

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