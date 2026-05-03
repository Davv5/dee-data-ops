
    
    

with all_values as (

    select
        attribution_quality_flag as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_detail`
    group by attribution_quality_flag

)

select *
from all_values
where value_field not in (
    'clean','unmatched','ambiguous_contact_match','payment_identity_only','role_unknown'
)


