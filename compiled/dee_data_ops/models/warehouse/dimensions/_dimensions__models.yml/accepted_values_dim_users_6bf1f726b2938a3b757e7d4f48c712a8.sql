
    
    

with all_values as (

    select
        role as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users`
    group by role

)

select *
from all_values
where value_field not in (
    'SDR','Setter','Triager','DM_Setter','Closer','Owner','unknown'
)


