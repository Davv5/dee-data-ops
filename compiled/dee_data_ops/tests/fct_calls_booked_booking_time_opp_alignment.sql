-- Singular test: when booking_time_opportunity_id is non-NULL, the user_sk
-- and pipeline_stage_sk derivable by joining back through that opp_id
-- must match the fact's assigned_user_sk and pipeline_stage_sk columns.
--
-- This is the *value-direction* complement to fct_calls_booked_diagnostic_sk_null_symmetry
-- (which asserts the NULL direction). Together the two tests assert the
-- invariant the fact's docstring promises: all three diagnostic columns
-- (booking_time_opportunity_id, assigned_user_sk, pipeline_stage_sk) are
-- derived from the same opportunity_at_booking pick.
--
-- Mechanism: re-derive what assigned_user_sk and pipeline_stage_sk SHOULD
-- be by joining stg_ghl__opportunities on booking_time_opportunity_id, then
-- dim_users / dim_pipeline_stages on the opp's assigned_user_id and
-- (pipeline_id, pipeline_stage_id). For non-NULL booking_time_opportunity_id
-- rows, the re-derived SKs must equal the fact's stored values.
--
-- Allows for legitimate LEFT JOIN orphans: re-derived SK can be NULL when
-- the opp's assigned_user_id doesn't match dim_users (former user, automation
-- actor) or the opp's pipeline_stage_id doesn't match dim_pipeline_stages
-- (orphan stage). In those cases the fact's stored SK is also NULL by the
-- same LEFT JOIN; the alignment still holds.
--
-- Returns rows on failure (any divergence between stored and re-derived SK).
-- Zero rows = alignment holds.

with fact as (
    select
        booking_sk,
        booking_time_opportunity_id,
        assigned_user_sk,
        pipeline_stage_sk
    from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked`
    where booking_time_opportunity_id is not null
),

re_derived as (
    select
        fact.booking_sk,
        fact.booking_time_opportunity_id,
        fact.assigned_user_sk    as stored_assigned_user_sk,
        fact.pipeline_stage_sk   as stored_pipeline_stage_sk,
        users.user_sk            as rederived_assigned_user_sk,
        stages.pipeline_stage_sk as rederived_pipeline_stage_sk
    from fact
    left join `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities` as opp
        on opp.opportunity_id = fact.booking_time_opportunity_id
    left join `project-41542e21-470f-4589-96d`.`Core`.`dim_users` as users
        on users.user_id = opp.assigned_user_id
    left join `project-41542e21-470f-4589-96d`.`Core`.`dim_pipeline_stages` as stages
        on stages.pipeline_id = opp.pipeline_id
       and stages.stage_id    = opp.pipeline_stage_id
)

select *
from re_derived
where
    -- IS DISTINCT FROM treats NULL = NULL as equal but NULL vs non-NULL as distinct.
    stored_assigned_user_sk    is distinct from rederived_assigned_user_sk
    or stored_pipeline_stage_sk is distinct from rederived_pipeline_stage_sk