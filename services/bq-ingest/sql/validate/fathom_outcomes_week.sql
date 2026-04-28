-- VALIDATION: fathom_outcomes_week
-- Purpose: Ensure weekly Fathom effectiveness marts are present and ratios stay bounded.

WITH outcomes AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_fathom_outcomes_week`
),
closer AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_fathom_closer_effectiveness_week`
)
SELECT
  'COVERAGE: outcomes week has rows' AS test_name,
  COUNT(*) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), COUNT(*)), 1) AS pct_populated,
  NULL AS count_a,
  NULL AS count_b,
  NULL AS count_c,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM outcomes

UNION ALL

SELECT
  'INTEGRITY: outcomes confidence counters in bounds',
  COUNTIF(
    high_confidence_revenue_call_count <= revenue_relevant_call_count
    AND revenue_relevant_call_count <= call_count
  ),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        high_confidence_revenue_call_count <= revenue_relevant_call_count
        AND revenue_relevant_call_count <= call_count
      ),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(
      high_confidence_revenue_call_count <= revenue_relevant_call_count
      AND revenue_relevant_call_count <= call_count
    ) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM outcomes

UNION ALL

SELECT
  'INTEGRITY: outcomes ratio columns in [0,1]',
  COUNTIF(
    (pct_calls_classified_by_calendly_anchor IS NULL OR (pct_calls_classified_by_calendly_anchor >= 0 AND pct_calls_classified_by_calendly_anchor <= 1))
    AND (pct_calls_promoted_behavioral IS NULL OR (pct_calls_promoted_behavioral >= 0 AND pct_calls_promoted_behavioral <= 1))
    AND
    (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
    AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
    AND (pct_linked_calls_moved_stage_within_48h IS NULL OR (pct_linked_calls_moved_stage_within_48h >= 0 AND pct_linked_calls_moved_stage_within_48h <= 1))
    AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
  ),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        (pct_calls_classified_by_calendly_anchor IS NULL OR (pct_calls_classified_by_calendly_anchor >= 0 AND pct_calls_classified_by_calendly_anchor <= 1))
        AND (pct_calls_promoted_behavioral IS NULL OR (pct_calls_promoted_behavioral >= 0 AND pct_calls_promoted_behavioral <= 1))
        AND
        (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
        AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
        AND (pct_linked_calls_moved_stage_within_48h IS NULL OR (pct_linked_calls_moved_stage_within_48h >= 0 AND pct_linked_calls_moved_stage_within_48h <= 1))
        AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
      ),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(
      (pct_calls_classified_by_calendly_anchor IS NULL OR (pct_calls_classified_by_calendly_anchor >= 0 AND pct_calls_classified_by_calendly_anchor <= 1))
      AND (pct_calls_promoted_behavioral IS NULL OR (pct_calls_promoted_behavioral >= 0 AND pct_calls_promoted_behavioral <= 1))
      AND
      (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
      AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
      AND (pct_linked_calls_moved_stage_within_48h IS NULL OR (pct_linked_calls_moved_stage_within_48h >= 0 AND pct_linked_calls_moved_stage_within_48h <= 1))
      AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
    ) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM outcomes

UNION ALL

SELECT
  'INTEGRITY: outcomes classification source counters in bounds',
  COUNTIF(
    COALESCE(calls_classified_by_calendly_anchor, 0) >= 0
    AND COALESCE(calls_promoted_behavioral, 0) >= 0
    AND COALESCE(calls_classified_by_fallback, 0) >= 0
    AND (COALESCE(calls_classified_by_calendly_anchor, 0) + COALESCE(calls_promoted_behavioral, 0) + COALESCE(calls_classified_by_fallback, 0)) <= COALESCE(call_count, 0)
  ),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        COALESCE(calls_classified_by_calendly_anchor, 0) >= 0
        AND COALESCE(calls_promoted_behavioral, 0) >= 0
        AND COALESCE(calls_classified_by_fallback, 0) >= 0
        AND (COALESCE(calls_classified_by_calendly_anchor, 0) + COALESCE(calls_promoted_behavioral, 0) + COALESCE(calls_classified_by_fallback, 0)) <= COALESCE(call_count, 0)
      ),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(
      COALESCE(calls_classified_by_calendly_anchor, 0) >= 0
      AND COALESCE(calls_promoted_behavioral, 0) >= 0
      AND COALESCE(calls_classified_by_fallback, 0) >= 0
      AND (COALESCE(calls_classified_by_calendly_anchor, 0) + COALESCE(calls_promoted_behavioral, 0) + COALESCE(calls_classified_by_fallback, 0)) <= COALESCE(call_count, 0)
    ) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM outcomes

UNION ALL

SELECT
  'COVERAGE: closer effectiveness has rows',
  COUNT(*) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), COUNT(*)), 1) AS pct_populated,
  NULL AS count_a,
  NULL AS count_b,
  NULL AS count_c,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM closer

UNION ALL

SELECT
  'INTEGRITY: closer ratio columns in [0,1]',
  COUNTIF(
    (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
    AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
    AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
    AND (discovery_win_rate IS NULL OR (discovery_win_rate >= 0 AND discovery_win_rate <= 1))
  ),
  COUNT(*),
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(
        (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
        AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
        AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
        AND (discovery_win_rate IS NULL OR (discovery_win_rate >= 0 AND discovery_win_rate <= 1))
      ),
      COUNT(*)
    ),
    1
  ),
  NULL, NULL, NULL,
  CASE
    WHEN COUNTIF(
      (linked_call_win_rate IS NULL OR (linked_call_win_rate >= 0 AND linked_call_win_rate <= 1))
      AND (pct_calls_moved_stage_within_48h IS NULL OR (pct_calls_moved_stage_within_48h >= 0 AND pct_calls_moved_stage_within_48h <= 1))
      AND (pct_calls_progressed_pipeline_within_48h IS NULL OR (pct_calls_progressed_pipeline_within_48h >= 0 AND pct_calls_progressed_pipeline_within_48h <= 1))
      AND (discovery_win_rate IS NULL OR (discovery_win_rate >= 0 AND discovery_win_rate <= 1))
    ) = COUNT(*) THEN 'PASS'
    ELSE 'FAIL'
  END,
  CURRENT_TIMESTAMP()
FROM closer
;
