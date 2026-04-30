-- Singular test: when booking_time_opportunity_id is NULL (no opp picked at
-- booking time), assigned_user_sk and pipeline_stage_sk must also be NULL.
--
-- All three diagnostic columns are derived from the same opportunity_at_booking
-- CTE pick. If no opp pre-existed the booking, the CTE selects no row, and
-- all three columns are NULL. A row where booking_time_opportunity_id is NULL but
-- assigned_user_sk or pipeline_stage_sk is populated would mean the SKs were
-- derived from a different selection path — a bug in the fact's CTE chain
-- the docstring asserts cannot happen.
--
-- One-way implication only. The reverse (booking_time_opportunity_id NOT NULL ⟹
-- both SKs NOT NULL) does NOT hold: a picked opp can have a valid
-- assigned_user_id that doesn't match dim_users (orphan), or a
-- pipeline_stage_id that doesn't match dim_pipeline_stages — both legitimate.
-- Those cases keep booking_time_opportunity_id populated while the SK is NULL via
-- LEFT JOIN. This test asserts only the unambiguous direction.
--
-- Returns rows on failure. Zero rows = invariant holds.

select
    booking_sk,
    booking_time_opportunity_id,
    assigned_user_sk,
    pipeline_stage_sk
from {{ ref('fct_calls_booked') }}
where
    booking_time_opportunity_id is null
    and (
        assigned_user_sk is not null
        or pipeline_stage_sk is not null
    )
