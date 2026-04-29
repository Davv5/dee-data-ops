
    
    

with dbt_test__target as (

  select pipeline_stage as unique_field
  from `project-41542e21-470f-4589-96d`.`STG`.`oracle_revenue_by_stage_20260319`
  where pipeline_stage is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


