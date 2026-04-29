
    
    

with child as (
    select assigned_user_sk as from_field
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
    where assigned_user_sk is not null
),

parent as (
    select user_sk as to_field
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


