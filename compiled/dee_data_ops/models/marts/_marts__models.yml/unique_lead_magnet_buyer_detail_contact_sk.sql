
    
    

with dbt_test__target as (

  select contact_sk as unique_field
  from `project-41542e21-470f-4589-96d`.`Marts`.`lead_magnet_buyer_detail`
  where contact_sk is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


