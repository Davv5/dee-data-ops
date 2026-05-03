
    
    

with all_values as (

    select
        pre_purchase_funnel_path as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by pre_purchase_funnel_path

)

select *
from all_values
where value_field not in (
    'booked_before_purchase','canceled_booking_before_purchase','reached_by_phone_before_purchase','worked_before_purchase','magnet_before_purchase_no_work_logged','buyer_without_known_pre_purchase_path'
)


