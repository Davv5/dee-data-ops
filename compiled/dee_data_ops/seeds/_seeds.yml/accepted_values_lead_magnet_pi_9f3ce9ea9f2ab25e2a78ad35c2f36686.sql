
    
    

with all_values as (

    select
        lead_magnet_category as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`STG`.`lead_magnet_pipeline_taxonomy`
    group by lead_magnet_category

)

select *
from all_values
where value_field not in (
    'true_lead_magnet','launch_event','waitlist','sales_operating_pipeline','internal_test_retired'
)


