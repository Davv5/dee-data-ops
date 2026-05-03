
    
    

with all_values as (

    select
        revenue_funnel_quality_flag as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by revenue_funnel_quality_flag

)

select *
from all_values
where value_field not in (
    'clean','missing_taxonomy','uncategorized_offer_type','negative_net_revenue','contact_not_matched','no_known_magnet'
)


