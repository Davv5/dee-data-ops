
    
    

with dbt_test__target as (

  select lead_source as unique_field
  from `project-41542e21-470f-4589-96d`.`STG`.`stl_lead_source_lookup`
  where lead_source is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


