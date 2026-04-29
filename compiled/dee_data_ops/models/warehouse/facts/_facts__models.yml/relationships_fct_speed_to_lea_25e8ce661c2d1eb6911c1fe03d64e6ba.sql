
    
    

with child as (
    select source_sk as from_field
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_speed_to_lead_touch`
    where source_sk is not null
),

parent as (
    select source_sk as to_field
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_source`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


