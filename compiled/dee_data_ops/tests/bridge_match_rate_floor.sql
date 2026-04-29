

-- Asserts the per-source matched-share clears the floor documented in
-- `_bridges__docs.md` ("Target match rate ≥ 70% across the unioned
-- bridge"). Returns one row per `source_platform` whose `matched`
-- share is below the threshold; test passes when zero rows return.
--
-- Severity is `warn` because new payment processors can land with
-- low initial match rates while contact backfill catches up — the
-- ≥70% target is a tuning trigger, not a hard ship gate. Hard
-- failures still come from `bridge_payment_count_parity` (no row
-- silently dropped) and `dbt_utils.unique_combination_of_columns`
-- (composite PK held).



with per_source as (

    select
        source_platform,
        countif(bridge_status = 'matched')                        as matched_n,
        count(*)                                                  as total_n,
        safe_divide(
            countif(bridge_status = 'matched'),
            count(*)
        )                                                         as matched_share
    from `project-41542e21-470f-4589-96d`.`Core`.`bridge_identity_contact_payment`
    group by 1

)

select
    source_platform,
    matched_n,
    total_n,
    matched_share
from per_source
where matched_share < 0.7