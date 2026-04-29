





with validation_errors as (

    select
        source_platform, payment_id
    from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`
    group by source_platform, payment_id
    having count(*) > 1

)

select *
from validation_errors


