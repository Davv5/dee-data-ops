





with validation_errors as (

    select
        pipeline_id, stage_id
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_pipeline_stages`
    group by pipeline_id, stage_id
    having count(*) > 1

)

select *
from validation_errors


