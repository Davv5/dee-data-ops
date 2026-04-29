
    
    

with all_values as (

    select
        source_platform as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`fct_refunds`
    group by source_platform

)

select *
from all_values
where value_field not in (
    'fanbasis'
)


