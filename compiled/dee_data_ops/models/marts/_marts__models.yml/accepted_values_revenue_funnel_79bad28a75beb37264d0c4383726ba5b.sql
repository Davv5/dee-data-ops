
    
    

with all_values as (

    select
        purchase_magnet_attribution_flag as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_funnel_detail`
    group by purchase_magnet_attribution_flag

)

select *
from all_values
where value_field not in (
    'latest_prior_magnet','purchase_before_first_magnet','no_known_magnet','missing_taxonomy','uncategorized_offer_type'
)


