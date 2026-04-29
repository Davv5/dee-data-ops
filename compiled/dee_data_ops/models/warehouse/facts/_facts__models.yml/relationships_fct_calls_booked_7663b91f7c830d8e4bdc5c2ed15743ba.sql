
    
    

with child as (
    select pipeline_stage_sk as from_field
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
    where pipeline_stage_sk is not null
),

parent as (
    select pipeline_stage_sk as to_field
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_pipeline_stages`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


