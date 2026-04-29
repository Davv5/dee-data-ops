
    
    

with all_values as (

    select
        close_outcome as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`fct_speed_to_lead_touch`
    group by close_outcome

)

select *
from all_values
where value_field not in (
    'won','lost','pending'
)


