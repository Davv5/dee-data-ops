-- VALIDATION: operations_kpi_panel
-- Purpose: Assert the Operations dashboard panel mart is populated and structurally valid.

WITH panel AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_operations_kpi_panel`
)

SELECT
  'EXISTS: rpt_operations_kpi_panel has rows' AS test_name,
  COUNT(*) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), NULLIF(COUNT(*), 0)), 1) AS pct_populated,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM panel

UNION ALL

SELECT
  'INTEGRITY: expected sections present' AS test_name,
  COUNT(DISTINCT section) AS passing_records,
  4 AS total_records,
  ROUND(100 * SAFE_DIVIDE(COUNT(DISTINCT section), 4), 1) AS pct_populated,
  CASE
    WHEN COUNT(DISTINCT IF(section = 'mart_freshness', section, NULL)) > 0
      AND COUNT(DISTINCT IF(section = 'raw_ingest_freshness', section, NULL)) > 0
      AND COUNT(DISTINCT IF(section = 'row_count_sanity', section, NULL)) > 0
      AND COUNT(DISTINCT IF(section = 'raw_to_mart_lag', section, NULL)) > 0
    THEN 'PASS' ELSE 'FAIL'
  END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM panel

UNION ALL

SELECT
  'INTEGRITY: key fields are populated' AS test_name,
  COUNTIF(section IS NOT NULL AND period_key IS NOT NULL AND metric_1_name IS NOT NULL) AS passing_records,
  COUNT(*) AS total_records,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(section IS NOT NULL AND period_key IS NOT NULL AND metric_1_name IS NOT NULL),
      NULLIF(COUNT(*), 0)
    ),
    1
  ) AS pct_populated,
  CASE
    WHEN COUNT(*) = COUNTIF(section IS NOT NULL AND period_key IS NOT NULL AND metric_1_name IS NOT NULL)
      THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  CURRENT_TIMESTAMP() AS validated_at
FROM panel
;
