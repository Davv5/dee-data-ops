
    
    

with all_values as (

    select
        attribution_quality_flag as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_detail`
    group by attribution_quality_flag

)

select *
from all_values
where value_field not in (
    'clean','contact_not_matched','pipeline_not_mapped','multi_magnet_contact'
)


