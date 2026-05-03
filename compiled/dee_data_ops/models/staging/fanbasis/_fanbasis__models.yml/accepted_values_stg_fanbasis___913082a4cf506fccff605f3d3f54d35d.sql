
    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`STG`.`stg_fanbasis__transactions`
    group by payment_type

)

select *
from all_values
where value_field not in (
    'upfront','auto_renew'
)


