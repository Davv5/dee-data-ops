
    
    

with all_values as (

    select
        attribution_era as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`dim_contacts`
    group by attribution_era

)

select *
from all_values
where value_field not in (
    'utm','pre_utm'
)


