
    
    

with dbt_test__target as (

  select missing_ghl_contact_sk as unique_field
  from `project-41542e21-470f-4589-96d`.`Marts`.`fanbasis_missing_ghl_contacts`
  where missing_ghl_contact_sk is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


