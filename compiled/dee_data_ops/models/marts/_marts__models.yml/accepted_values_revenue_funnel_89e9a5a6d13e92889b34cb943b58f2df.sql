
    
    

with all_values as (

    select
        payment_plan_status as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by payment_plan_status

)

select *
from all_values
where value_field not in (
    'auto_renew_plan','multi_payment_plan','plan_named_single_payment','single_payment'
)


