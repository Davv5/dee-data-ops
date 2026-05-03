
    
    

with all_values as (

    select
        payment_plan_truth_status as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by payment_plan_truth_status

)

select *
from all_values
where value_field not in (
    'fanbasis_auto_renew_cash_only','name_inferred_plan_cash_only','fanbasis_single_payment_cash','historical_stripe_cash_only','unknown_cash_only'
)


