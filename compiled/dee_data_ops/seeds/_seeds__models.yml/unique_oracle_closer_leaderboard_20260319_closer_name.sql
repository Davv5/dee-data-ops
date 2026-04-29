
    
    

with dbt_test__target as (

  select closer_name as unique_field
  from `project-41542e21-470f-4589-96d`.`STG`.`oracle_closer_leaderboard_20260319`
  where closer_name is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


