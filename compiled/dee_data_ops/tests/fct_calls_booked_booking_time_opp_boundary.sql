-- Singular test: every non-NULL booking_time_opportunity_id must point at an
-- opportunity whose opportunity_created_at <= the booking's booked_at.
--
-- This is the load-bearing time-boundary rule the fact's docstring promises
-- ("active opp at booking time" = most-recent opp where opportunity_created_at
-- <= booked_at). If any row violates it, the fact's opportunity_at_booking
-- CTE has a bug — either the JOIN's ON clause or the QUALIFY ROW_NUMBER's
-- ORDER BY drifted in a way that lets a future-of-booking opp slip through.
--
-- This test directly catches the exact bug-class the docstring (lines 22-26)
-- attributes to the legacy mart rules: "broken time filter" allowing
-- post-booking opps to be selected. Asserting the invariant at the fact
-- layer means PR-2's mart collapse has structurally guaranteed temporal
-- correctness rather than relying on each mart to re-implement the filter.
--
-- Returns rows on failure (any booking_time_opportunity_id pointing at an opp
-- created after its booking). Zero rows = boundary rule holds.

select
    fact.booking_sk,
    fact.booked_at,
    fact.booking_time_opportunity_id,
    opp.opportunity_created_at,
    timestamp_diff(opp.opportunity_created_at, fact.booked_at, second)
        as opp_creation_seconds_after_booking
from `project-41542e21-470f-4589-96d`.`Core`.`fct_calls_booked` as fact
inner join `project-41542e21-470f-4589-96d`.`STG`.`stg_ghl__opportunities` as opp
    on opp.opportunity_id = fact.booking_time_opportunity_id
where
    -- Strict <= per the fact's selection rule. The sub-second case
    -- (workflow-created opp T+50ms after Calendly) is by-design EXCLUDED
    -- from being the selected opp, so any positive-diff row here is a bug.
    opp.opportunity_created_at > fact.booked_at