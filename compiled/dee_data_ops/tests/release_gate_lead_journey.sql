

-- Release-gate parity test for the contact-grain mart.
--
-- Two assertions pulled against the 2026-03-19 oracle seed:
--   1. Row count within ±5% of `Total Leads` (15,598).
--   2. Applicant count within ±10% of `Applications Submitted` (6,113).
--
-- Tolerance rationale: the row count is anchored on `dim_contacts`,
-- which is 1:1 with GHL contacts — a tight ±5% is reasonable. The
-- applicant count depends on Typeform-to-contact join fidelity, so
-- it carries a wider ±10% tolerance.
--
-- Known failing assertion until upstream lands: `application_submitted`
-- is a NULL placeholder until the Typeform-answers pivot ships.
-- `countif(null)` returns 0, so `applicant_pct_delta` will equal 1.0
-- (-100%) and this test will fail on that assertion. Keep the test
-- as-is — it correctly signals that the applicant-join upstream is
-- still owed. Flips green automatically when the upstream ships.

with

mart as (

    select
        count(*)                                                as mart_rows,
        countif(application_submitted)                          as mart_applicants
    from `project-41542e21-470f-4589-96d`.`Marts`.`lead_journey`

),

oracle as (

    select
        cast((select value from `project-41542e21-470f-4589-96d`.`STG`.`oracle_dashboard_metrics_20260319` where metric = 'Total Leads') as int64)
                                                                as oracle_rows,
        cast((select value from `project-41542e21-470f-4589-96d`.`STG`.`oracle_dashboard_metrics_20260319` where metric = 'Applications Submitted') as int64)
                                                                as oracle_applicants

),

delta as (

    select
        mart.mart_rows,
        oracle.oracle_rows,
        safe_divide(abs(mart.mart_rows - oracle.oracle_rows), oracle.oracle_rows)
                                                                as row_pct_delta,
        mart.mart_applicants,
        oracle.oracle_applicants,
        safe_divide(abs(mart.mart_applicants - oracle.oracle_applicants), oracle.oracle_applicants)
                                                                as applicant_pct_delta
    from mart, oracle

)

select *
from delta
where row_pct_delta       > 0.05
   or applicant_pct_delta > 0.10