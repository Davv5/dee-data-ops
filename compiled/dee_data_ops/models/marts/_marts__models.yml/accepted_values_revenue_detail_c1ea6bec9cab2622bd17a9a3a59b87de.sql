
    
    

with all_values as (

    select
        match_status as value_field,
        count(*) as n_records

    from `project-41542e21-470f-4589-96d`.`Marts`.`revenue_detail`
    group by match_status

)

select *
from all_values
where value_field not in (
    'matched','ambiguous_multi_candidate','unmatched'
)


