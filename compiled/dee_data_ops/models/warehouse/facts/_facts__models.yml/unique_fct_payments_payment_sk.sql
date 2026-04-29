
    
    

with dbt_test__target as (

  select payment_sk as unique_field
  from `project-41542e21-470f-4589-96d`.`Core`.`fct_payments`
  where payment_sk is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


