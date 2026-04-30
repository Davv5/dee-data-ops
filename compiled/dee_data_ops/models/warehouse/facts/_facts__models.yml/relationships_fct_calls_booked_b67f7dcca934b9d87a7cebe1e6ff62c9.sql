
    
    

with child as (
    select booking_time_opportunity_id as from_field
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
    where booking_time_opportunity_id is not null
),

parent as (
    select opportunity_id as to_field
    from `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


