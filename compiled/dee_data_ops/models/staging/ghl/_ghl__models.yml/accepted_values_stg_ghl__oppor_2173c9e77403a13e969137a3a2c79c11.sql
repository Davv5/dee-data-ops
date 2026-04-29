
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities`
    group by status

)

select *
from all_values
where value_field not in (
    'open','won','lost','abandoned'
)


