
    
    

with all_values as (

    select
        recommended_action as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`fanbasis_missing_ghl_contacts`
    group by recommended_action

)

select *
from all_values
where value_field not in (
    'create_ghl_contact','repair_identity_bridge','review_duplicate_ghl_contacts'
)


