
    
    

with all_values as (

    select
        mart_name as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`warehouse`.`mart_volume_history`
    group by mart_name

)

select *
from all_values
where value_field not in (
    'lead_journey','revenue_detail'
)


