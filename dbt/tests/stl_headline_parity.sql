-- Singular test: headline metric computed from speed_to_lead_detail must match
-- the current stl_headline_7d output within 0.1 percentage points. The locked
-- definition (CLAUDE.local.md "Locked metric", 2026-04-19) is:
--   % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes,
--   SDR-attributed denominator.
--
-- This test RETURNS ROWS ON FAILURE. Zero rows = parity holds.
-- 0.1pp tolerance chosen because rounding in stl_headline_7d happens at 1 decimal.
--
-- NULL-safety: if BOTH sides return NULL (e.g., no data in the 7-day window
-- because fct_calls_booked.contact_sk = NULL staging gap is unfilled), the
-- WHERE clause evaluates as:
--   abs(NULL - NULL) > 0.1  →  NULL > 0.1  →  FALSE
-- which means the test would trivially pass on NULL == NULL.
-- To prevent this, the UNION at the bottom of this query emits an explicit
-- failure row when EITHER side is NULL, so NULL on either side is flagged
-- distinct from "both computed and equal."
--
-- Source: stl_headline_7d uses `countif(first_toucher_role = 'SDR')` as
-- denominator. The new calc uses `countif(is_sdr_touch and is_first_touch)`.
-- These must resolve to the same set of bookings for parity to hold.

with

new_calc as (
    select
        round(
            safe_divide(
                countif(is_within_5_min_sla and is_first_touch),
                nullif(countif(is_sdr_touch and is_first_touch), 0)
            ) * 100,
            1
        ) as pct_within_5min_7d
    from {{ ref('speed_to_lead_detail') }}
    where booked_at >= timestamp_sub(current_timestamp(), interval 7 day)
),

old_calc as (
    select pct_within_5min_7d
    from {{ ref('stl_headline_7d') }}
),

-- Rows emitted here trigger test failure.
-- Case 1: values present and differ by > 0.1pp.
parity_check as (
    select
        new_calc.pct_within_5min_7d as new_pct,
        old_calc.pct_within_5min_7d as old_pct,
        abs(new_calc.pct_within_5min_7d - old_calc.pct_within_5min_7d) as diff_pp,
        'values_differ' as failure_reason
    from new_calc, old_calc
    where abs(new_calc.pct_within_5min_7d - old_calc.pct_within_5min_7d) > 0.1
),

-- Case 2: either side is NULL (data gap — flag as failure, not pass).
null_check as (
    select
        new_calc.pct_within_5min_7d as new_pct,
        old_calc.pct_within_5min_7d as old_pct,
        cast(null as float64)       as diff_pp,
        'one_or_both_null'          as failure_reason
    from new_calc, old_calc
    where new_calc.pct_within_5min_7d is null
       or old_calc.pct_within_5min_7d is null
)

select * from parity_check
union all
select * from null_check
