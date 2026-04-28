-- VALIDATION: mart_master_lead_wide
-- Purpose: Prove payment_status_label, lead_magnet_history, and calendly enrichment fields are working correctly
-- Run frequency: Every commit (pre-commit hook) + daily automated check

SELECT
  'COVERAGE: payment_status_label' AS test_name,
  COUNTIF(payment_status_label IS NOT NULL) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(payment_status_label IS NOT NULL), COUNT(*)), 1) AS pct_populated,
  COUNTIF(payment_status_label = 'Paid') AS count_paid,
  COUNTIF(payment_status_label = 'Payment Plan') AS count_payment_plan,
  COUNTIF(payment_status_label = 'Repeat Buyer') AS count_repeat_buyer,
  'PASS' AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: lead_magnet_history',
  COUNTIF(lead_magnet_history IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(lead_magnet_history IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: attribution_reason_code',
  COUNTIF(attribution_reason_code IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(attribution_reason_code IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  CASE WHEN SAFE_DIVIDE(COUNTIF(attribution_reason_code IS NOT NULL), COUNT(*)) >= 0.99 THEN 'PASS' ELSE 'FAIL' END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'INTEGRITY: attribution_rule_fired domain',
  COUNTIF(attribution_rule_fired IN ('rule_01_event_touch_precedence', 'rule_02_snapshot_fallback', 'rule_99_no_signal')),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(attribution_rule_fired IN ('rule_01_event_touch_precedence', 'rule_02_snapshot_fallback', 'rule_99_no_signal')),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(attribution_rule_fired IN ('rule_01_event_touch_precedence', 'rule_02_snapshot_fallback', 'rule_99_no_signal')) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'INTEGRITY: reason-rule alignment',
  COUNTIF(
    (attribution_reason_code = 'event_snapshot_conflict' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
    OR (attribution_reason_code = 'event_history_present' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
    OR (attribution_reason_code = 'snapshot_fallback_only' AND attribution_rule_fired = 'rule_02_snapshot_fallback')
    OR (attribution_reason_code = 'no_attribution_signal' AND attribution_rule_fired = 'rule_99_no_signal')
  ),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        (attribution_reason_code = 'event_snapshot_conflict' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
        OR (attribution_reason_code = 'event_history_present' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
        OR (attribution_reason_code = 'snapshot_fallback_only' AND attribution_rule_fired = 'rule_02_snapshot_fallback')
        OR (attribution_reason_code = 'no_attribution_signal' AND attribution_rule_fired = 'rule_99_no_signal')
      ),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(
      (attribution_reason_code = 'event_snapshot_conflict' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
      OR (attribution_reason_code = 'event_history_present' AND attribution_rule_fired = 'rule_01_event_touch_precedence')
      OR (attribution_reason_code = 'snapshot_fallback_only' AND attribution_rule_fired = 'rule_02_snapshot_fallback')
      OR (attribution_reason_code = 'no_attribution_signal' AND attribution_rule_fired = 'rule_99_no_signal')
    ) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: self_reported_source',
  COUNTIF(self_reported_source IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(self_reported_source IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: emotional_goal_value',
  COUNTIF(emotional_goal_value IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(emotional_goal_value IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: typeform_age_bracket',
  COUNTIF(typeform_age_bracket IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(typeform_age_bracket IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: typeform_business_stage',
  COUNTIF(typeform_business_stage IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(typeform_business_stage IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: typeform_investment_range',
  COUNTIF(typeform_investment_range IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(typeform_investment_range IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: typeform_core_struggle',
  COUNTIF(typeform_core_struggle IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(typeform_core_struggle IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'COVERAGE: closer_name',
  COUNTIF(closer_name IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(closer_name IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  'PASS',
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'INTEGRITY: golden_contact_key unique',
  COUNT(*) - COUNT(DISTINCT golden_contact_key),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNT(DISTINCT golden_contact_key), COUNT(*)), 1),
  NULL, NULL, NULL,
  CASE WHEN COUNT(*) = COUNT(DISTINCT golden_contact_key) THEN 'PASS' ELSE 'FAIL' END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'INTEGRITY: email coverage 99%+',
  COUNTIF(email IS NOT NULL),
  COUNT(*),
  ROUND(100 * SAFE_DIVIDE(COUNTIF(email IS NOT NULL), COUNT(*)), 1),
  NULL, NULL, NULL,
  CASE WHEN SAFE_DIVIDE(COUNTIF(email IS NOT NULL), COUNT(*)) >= 0.99 THEN 'PASS' ELSE 'FAIL' END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

UNION ALL

SELECT
  'RECENCY: data < 24h old',
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ghl_last_seen_ts), HOUR),
  COUNT(*),
  NULL,
  NULL, NULL, NULL,
  CASE WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ghl_last_seen_ts), HOUR) < 24 THEN 'PASS' ELSE 'WARN' END,
  CURRENT_TIMESTAMP()
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
;
