
    
    

with all_values as (

    select
        match_method as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`fct_outreach`
    group by match_method

)

select *
from all_values
where value_field not in (
    'ghl_native'
)


