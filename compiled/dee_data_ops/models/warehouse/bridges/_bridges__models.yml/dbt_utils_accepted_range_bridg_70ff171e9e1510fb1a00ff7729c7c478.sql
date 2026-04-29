

with meet_condition as(
  select *
  from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not match_score >= 0
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not match_score <= 1
)

select *
from validation_errors

