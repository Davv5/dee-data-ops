-- VALIDATION: semantic_foundation_terms
-- Purpose: Definition-of-done checks for frozen semantic terms in BI foundation.
-- Fails when any row emits status = FAIL.

WITH set_events AS (
  SELECT
    trigger_source_id,
    trigger_ts,
    golden_contact_key
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
  WHERE trigger_type = 'appointment_booking'
),
set_dupe_keys AS (
  SELECT
    COUNT(*) AS duplicate_key_count
  FROM (
    SELECT
      trigger_source_id
    FROM set_events
    WHERE trigger_source_id IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) > 1
  )
),
show_proxy_rollup AS (
  SELECT
    COALESCE(SUM(invitee_count), 0) AS total_show_proxy_invitees,
    COALESCE(
      SUM(
        CASE
          WHEN LOWER(COALESCE(event_status, '')) IN ('canceled', 'cancelled')
            OR LOWER(COALESCE(invitee_status, '')) <> 'active'
            THEN invitee_count
          ELSE 0
        END
      ),
      0
    ) AS invalid_show_proxy_invitees
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_calendly_status_week`
  WHERE attendance_bucket = 'showed_proxy'
),
application_rollup AS (
  SELECT
    COUNT(*) AS total_application_rows,
    COUNTIF(
      report_month IS NULL
      OR NULLIF(TRIM(form_name), '') IS NULL
      OR submission_count < 0
      OR distinct_submission_count < 0
      OR distinct_contact_count < 0
      OR distinct_submission_count > submission_count
      OR linked_opportunity_count > submission_count
    ) AS invalid_application_rows
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_applications_month`
),
won_rollup AS (
  SELECT
    COUNTIF(SAFE_CAST(net_amount AS NUMERIC) > 0 AND match_status = 'matched') AS total_won_rows,
    COUNTIF(
      SAFE_CAST(net_amount AS NUMERIC) > 0
      AND match_status = 'matched'
      AND (
        golden_contact_key IS NULL
        OR event_ts IS NULL
      )
    ) AS invalid_won_rows,
    COUNTIF(SAFE_CAST(net_amount AS NUMERIC) > 0 AND match_status <> 'matched') AS positive_net_unmatched_rows
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
)
SELECT
  'SET: required fields present' AS test_name,
  COUNTIF(
    trigger_source_id IS NOT NULL
    AND trigger_ts IS NOT NULL
    AND golden_contact_key IS NOT NULL
  ) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        trigger_source_id IS NOT NULL
        AND trigger_ts IS NOT NULL
        AND golden_contact_key IS NOT NULL
      ),
      NULLIF(COUNT(*), 0)
    ),
    1
  ) AS pct_populated,
  COUNTIF(
    trigger_source_id IS NULL
    OR trigger_ts IS NULL
    OR golden_contact_key IS NULL
  ) AS count_a,
  NULL AS count_b,
  NULL AS count_c,
  CASE
    WHEN COUNT(*) = 0 THEN 'WARN'
    WHEN COUNTIF(
      trigger_source_id IS NULL
      OR trigger_ts IS NULL
      OR golden_contact_key IS NULL
    ) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM set_events

UNION ALL

SELECT
  'SET: unique booking source id',
  (SELECT COUNT(*) FROM set_events) - d.duplicate_key_count,
  (SELECT COUNT(*) FROM set_events),
  ROUND(
    100 * SAFE_DIVIDE(
      (SELECT COUNT(*) FROM set_events) - d.duplicate_key_count,
      NULLIF((SELECT COUNT(*) FROM set_events), 0)
    ),
    1
  ),
  d.duplicate_key_count,
  NULL,
  NULL,
  CASE
    WHEN (SELECT COUNT(*) FROM set_events) = 0 THEN 'WARN'
    WHEN d.duplicate_key_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM set_dupe_keys d

UNION ALL

SELECT
  'SHOW_PROXY: status integrity',
  s.total_show_proxy_invitees - s.invalid_show_proxy_invitees,
  s.total_show_proxy_invitees,
  ROUND(
    100 * SAFE_DIVIDE(
      s.total_show_proxy_invitees - s.invalid_show_proxy_invitees,
      NULLIF(s.total_show_proxy_invitees, 0)
    ),
    1
  ),
  s.invalid_show_proxy_invitees,
  NULL,
  NULL,
  CASE
    WHEN s.total_show_proxy_invitees = 0 THEN 'WARN'
    WHEN s.invalid_show_proxy_invitees = 0 THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM show_proxy_rollup s

UNION ALL

SELECT
  'APPLICATION: rollup integrity',
  a.total_application_rows - a.invalid_application_rows,
  a.total_application_rows,
  ROUND(
    100 * SAFE_DIVIDE(
      a.total_application_rows - a.invalid_application_rows,
      NULLIF(a.total_application_rows, 0)
    ),
    1
  ),
  a.invalid_application_rows,
  NULL,
  NULL,
  CASE
    WHEN a.total_application_rows = 0 THEN 'WARN'
    WHEN a.invalid_application_rows = 0 THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM application_rollup a

UNION ALL

SELECT
  'WON: matched positive-net integrity',
  w.total_won_rows - w.invalid_won_rows,
  w.total_won_rows,
  ROUND(
    100 * SAFE_DIVIDE(
      w.total_won_rows - w.invalid_won_rows,
      NULLIF(w.total_won_rows, 0)
    ),
    1
  ),
  w.invalid_won_rows,
  w.positive_net_unmatched_rows,
  NULL,
  CASE
    WHEN w.total_won_rows = 0 THEN 'WARN'
    WHEN w.invalid_won_rows = 0 THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM won_rollup w
;
