





with validation_errors as (

    select
        user_id, dbt_valid_from
    from `project-41542e21-470f-4589-96d`.`Core`.`dim_users_snapshot`
    group by user_id, dbt_valid_from
    having count(*) > 1

)

select *
from validation_errors


