-- Lead magnet URL + UTM propagation checks.
WITH base AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity`
),
coverage AS (
  SELECT
    COUNT(*) AS total_rows,
    COUNTIF(landing_page_url IS NOT NULL AND TRIM(landing_page_url) != '') AS landing_page_url_rows,
    COUNTIF(referrer_url IS NOT NULL AND TRIM(referrer_url) != '') AS referrer_url_rows,
    COUNTIF(utm_source IS NOT NULL AND TRIM(utm_source) != '') AS utm_source_rows,
    COUNTIF(utm_medium IS NOT NULL AND TRIM(utm_medium) != '') AS utm_medium_rows,
    COUNTIF(utm_campaign IS NOT NULL AND TRIM(utm_campaign) != '') AS utm_campaign_rows,
    COUNTIF(
      JSON_VALUE(submission_payload_json, '$.others.eventData.url_params.utm_source') IS NOT NULL
    ) AS payload_utm_source_rows,
    COUNTIF(
      JSON_VALUE(submission_payload_json, '$.others.eventData.url_params.utm_campaign') IS NOT NULL
    ) AS payload_utm_campaign_rows,
    COUNTIF(
      (utm_source IS NULL OR TRIM(utm_source) = '')
      AND JSON_VALUE(submission_payload_json, '$.others.eventData.url_params.utm_source') IS NOT NULL
    ) AS missing_utm_source_when_payload_present_rows,
    COUNTIF(
      (utm_campaign IS NULL OR TRIM(utm_campaign) = '')
      AND JSON_VALUE(submission_payload_json, '$.others.eventData.url_params.utm_campaign') IS NOT NULL
    ) AS missing_utm_campaign_when_payload_present_rows,
    COUNTIF(
      REGEXP_CONTAINS(
        LOWER(COALESCE(landing_page_url, '')),
        r'(?:[?&])(mcp_token|fbclid|gclid|ttclid|_hsenc|_hsmi|mc_eid|igshid|wbraid|gbraid)='
      )
    ) AS sensitive_token_rows
  FROM base
)
SELECT
  'lead_magnet_landing_page_url_coverage' AS check_name,
  SAFE_DIVIDE(landing_page_url_rows, total_rows) AS metric_value,
  CASE WHEN SAFE_DIVIDE(landing_page_url_rows, total_rows) >= 0.90 THEN 'PASS' ELSE 'WARN' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_referrer_url_coverage' AS check_name,
  SAFE_DIVIDE(referrer_url_rows, total_rows) AS metric_value,
  CASE WHEN SAFE_DIVIDE(referrer_url_rows, total_rows) >= 0.40 THEN 'PASS' ELSE 'WARN' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_utm_source_coverage' AS check_name,
  SAFE_DIVIDE(utm_source_rows, total_rows) AS metric_value,
  CASE WHEN SAFE_DIVIDE(utm_source_rows, total_rows) >= 0.70 THEN 'PASS' ELSE 'WARN' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_utm_medium_coverage' AS check_name,
  SAFE_DIVIDE(utm_medium_rows, total_rows) AS metric_value,
  CASE WHEN SAFE_DIVIDE(utm_medium_rows, total_rows) >= 0.70 THEN 'PASS' ELSE 'WARN' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_utm_campaign_coverage' AS check_name,
  SAFE_DIVIDE(utm_campaign_rows, total_rows) AS metric_value,
  CASE WHEN SAFE_DIVIDE(utm_campaign_rows, total_rows) >= 0.65 THEN 'PASS' ELSE 'WARN' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_landing_page_url_sensitive_token_rows' AS check_name,
  CAST(sensitive_token_rows AS FLOAT64) AS metric_value,
  CASE WHEN sensitive_token_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_utm_source_payload_propagation_gap' AS check_name,
  SAFE_DIVIDE(missing_utm_source_when_payload_present_rows, NULLIF(payload_utm_source_rows, 0)) AS metric_value,
  CASE
    WHEN SAFE_DIVIDE(missing_utm_source_when_payload_present_rows, NULLIF(payload_utm_source_rows, 0)) <= 0.05
      THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM coverage

UNION ALL

SELECT
  'lead_magnet_utm_campaign_payload_propagation_gap' AS check_name,
  SAFE_DIVIDE(missing_utm_campaign_when_payload_present_rows, NULLIF(payload_utm_campaign_rows, 0)) AS metric_value,
  CASE
    WHEN SAFE_DIVIDE(missing_utm_campaign_when_payload_present_rows, NULLIF(payload_utm_campaign_rows, 0)) <= 0.05
      THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM coverage
;
