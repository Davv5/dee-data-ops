-- Identity bridge diagnostics + weekly KPI trend pack.
-- Run after marts refresh (`ops/scripts/run_mart_models.sh`).

-- 1) Eligibility snapshot (source-key completeness + candidate-attempt rate).
WITH fb_payers AS (
  SELECT DISTINCT
    c.customer_id,
    LOWER(TRIM(c.email)) AS email_norm,
    CASE
      WHEN c.email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(c.email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(c.email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(c.email))
    END AS email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(c.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(c.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions` t
  JOIN `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers` c
    ON c.customer_id = t.customer_id
),
ghl AS (
  SELECT
    LOWER(TRIM(email)) AS email_norm,
    CASE
      WHEN email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(email))
    END AS email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`
),
method_flags AS (
  SELECT
    fb.customer_id,
    fb.email_norm IS NOT NULL AS has_email_norm,
    fb.phone_last10 IS NOT NULL AS has_phone_last10,
    EXISTS (SELECT 1 FROM ghl g WHERE g.email_norm = fb.email_norm AND fb.email_norm IS NOT NULL) AS can_attempt_email_exact,
    EXISTS (SELECT 1 FROM ghl g WHERE g.email_canon = fb.email_canon AND fb.email_canon IS NOT NULL) AS can_attempt_email_canonical,
    EXISTS (SELECT 1 FROM ghl g WHERE g.phone_last10 = fb.phone_last10 AND fb.phone_last10 IS NOT NULL) AS can_attempt_phone_last10
  FROM fb_payers fb
)
SELECT
  COUNT(*) AS fb_distinct_paying_customers,
  COUNTIF(has_email_norm) AS fb_with_email_norm,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(has_email_norm), COUNT(*)), 2) AS pct_fb_with_email_norm,
  COUNTIF(has_phone_last10) AS fb_with_phone_last10,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(has_phone_last10), COUNT(*)), 2) AS pct_fb_with_phone_last10,
  COUNTIF(can_attempt_email_exact OR can_attempt_email_canonical OR can_attempt_phone_last10) AS fb_with_any_method_candidate,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(can_attempt_email_exact OR can_attempt_email_canonical OR can_attempt_phone_last10), COUNT(*)), 2) AS pct_fb_with_any_method_candidate
FROM method_flags;

-- 2) Weekly coverage KPI (line-level + revenue-level).
WITH weekly AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(event_ts, ingested_at)), WEEK(MONDAY)) AS week_start,
    COUNT(*) AS payment_lines,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_revenue,
    COUNTIF(match_status = 'matched') AS matched_lines,
    SUM(CASE WHEN match_status = 'matched' THEN SAFE_CAST(net_amount AS NUMERIC) ELSE 0 END) AS matched_revenue
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  WHERE COALESCE(event_ts, ingested_at) IS NOT NULL
  GROUP BY 1
)
SELECT
  week_start,
  payment_lines,
  ROUND(net_revenue, 2) AS net_revenue,
  matched_lines,
  ROUND(100 * SAFE_DIVIDE(matched_lines, payment_lines), 2) AS matched_line_pct,
  ROUND(matched_revenue, 2) AS matched_revenue,
  ROUND(100 * SAFE_DIVIDE(matched_revenue, net_revenue), 2) AS matched_revenue_pct
FROM weekly
ORDER BY week_start DESC;

-- 3) Weekly method mix.
SELECT
  DATE_TRUNC(DATE(COALESCE(event_ts, ingested_at)), WEEK(MONDAY)) AS week_start,
  COALESCE(match_method, 'none') AS match_method,
  COUNT(*) AS payment_lines,
  ROUND(SUM(SAFE_CAST(net_amount AS NUMERIC)), 2) AS net_revenue
FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
WHERE COALESCE(event_ts, ingested_at) IS NOT NULL
GROUP BY 1, 2
ORDER BY week_start DESC, payment_lines DESC;

