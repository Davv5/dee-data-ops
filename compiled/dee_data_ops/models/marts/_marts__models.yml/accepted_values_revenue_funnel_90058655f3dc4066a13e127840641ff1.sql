
    
    

with all_values as (

    select
        best_available_operator_source as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by best_available_operator_source

)

select *
from all_values
where value_field not in (
    'first_successful_call_before_purchase','first_touch_before_purchase','latest_prior_opportunity_owner','latest_booking_owner','unassigned'
)


