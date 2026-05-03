
    
    

with all_values as (

    select
        match_method as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`
    group by match_method

)

select *
from all_values
where value_field not in (
    'email_exact','email_canonical','phone_last10','stripe_customer_email','stripe_customer_phone','fanbasis_conversation_email','fanbasis_unique_crm_name','billing_email_direct','unmatched'
)


