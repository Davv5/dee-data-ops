
    
    

with dbt_test__target as (

  select period as unique_field
  from `project-41542e21-470f-4589-96d`.`STG`.`oracle_show_rate_by_period_20260319`
  where period is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


