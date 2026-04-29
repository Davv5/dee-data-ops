
    
    

with dbt_test__target as (

  select campaign as unique_field
  from `project-41542e21-470f-4589-96d`.`STG`.`oracle_show_rate_by_campaign_20260319`
  where campaign is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


