
    
    

with all_values as (

    select
        lead_magnet_offer_type as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_detail`
    group by lead_magnet_offer_type

)

select *
from all_values
where value_field not in (
    'prompt_pack','template','guide_or_doc','resource_list','training_or_class','video_or_replay','giveaway','community','waitlist','sales_pipeline','launch_event','uncategorized'
)


