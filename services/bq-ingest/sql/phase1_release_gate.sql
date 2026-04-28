-- Phase 1 release gate checks for the deterministic revenue operating system.
-- Each block must return exactly one row with:
--   gate_name, severity, comparison, status, metric_value,
--   pass_threshold, warn_threshold, message, details_json

-- GATE hard_revenue_visibility_gap_pct_30d
WITH fanbasis_source AS (
  SELECT
    'fanbasis' AS source_system,
    COUNT(DISTINCT transaction_id) AS source_row_count,
    COALESCE(SUM(SAFE_CAST(net_amount AS NUMERIC)), 0) AS source_net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
stripe_source AS (
  SELECT
    'stripe' AS source_system,
    COUNT(DISTINCT payment_id) AS source_row_count,
    COALESCE(
      SUM(
        SAFE_CAST(COALESCE(amount_captured, amount, 0) - COALESCE(amount_refunded, 0) AS NUMERIC)
      ),
      0
    ) AS source_net_amount
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    AND (
      COALESCE(paid, FALSE)
      OR LOWER(COALESCE(status, '')) IN ('succeeded', 'paid')
    )
),
source_totals AS (
  SELECT * FROM fanbasis_source
  UNION ALL
  SELECT * FROM stripe_source
),
unified_totals AS (
  SELECT
    source_system,
    COUNT(DISTINCT payment_id) AS unified_row_count,
    COALESCE(SUM(SAFE_CAST(net_amount AS NUMERIC)), 0) AS unified_net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY 1
),
comparison AS (
  SELECT
    s.source_system,
    s.source_row_count,
    COALESCE(u.unified_row_count, 0) AS unified_row_count,
    s.source_net_amount,
    COALESCE(u.unified_net_amount, 0) AS unified_net_amount,
    ABS(s.source_row_count - COALESCE(u.unified_row_count, 0)) AS row_gap_count,
    ABS(s.source_net_amount - COALESCE(u.unified_net_amount, 0)) AS net_gap_amount,
    CASE
      WHEN ABS(s.source_net_amount) < 0.01 THEN 0
      ELSE SAFE_DIVIDE(
        ABS(s.source_net_amount - COALESCE(u.unified_net_amount, 0)),
        ABS(s.source_net_amount)
      )
    END AS net_gap_pct
  FROM source_totals s
  LEFT JOIN unified_totals u
    USING (source_system)
)
SELECT
  'hard_revenue_visibility_gap_pct_30d' AS gate_name,
  'hard' AS severity,
  '<=' AS comparison,
  CASE
    WHEN MAX(COALESCE(net_gap_pct, 0)) <= 0.0001 AND SUM(row_gap_count) = 0 THEN 'PASS'
    WHEN MAX(COALESCE(net_gap_pct, 0)) <= 0.001 AND SUM(row_gap_count) <= 1 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(MAX(COALESCE(net_gap_pct, 0)) AS FLOAT64) AS metric_value,
  CAST(0.0001 AS FLOAT64) AS pass_threshold,
  CAST(0.001 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'max source-to-unified net gap pct over last 30d = %.6f; total row gap count = %d',
    MAX(COALESCE(net_gap_pct, 0)),
    SUM(row_gap_count)
  ) AS message,
  TO_JSON_STRING(
    ARRAY_AGG(
      STRUCT(
        source_system,
        source_row_count,
        unified_row_count,
        row_gap_count,
        source_net_amount,
        unified_net_amount,
        net_gap_amount,
        net_gap_pct
      )
      ORDER BY source_system
    )
  ) AS details_json
FROM comparison
;

-- GATE hard_critical_mart_age_minutes
WITH mart_freshness AS (
  SELECT 'Marts.mart_master_lead_wide' AS mart_name, MAX(mart_refreshed_at) AS mart_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  UNION ALL
  SELECT 'Marts.fct_payment_line_unified', MAX(mart_refreshed_at)
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  UNION ALL
  SELECT 'Marts.fct_speed_to_lead', MAX(mart_refreshed_at)
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
  UNION ALL
  SELECT 'Marts.rpt_campaign_funnel_month', MAX(mart_refreshed_at)
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_campaign_funnel_month`
),
mart_ages AS (
  SELECT
    mart_name,
    mart_refreshed_at,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), mart_refreshed_at, MINUTE) AS age_minutes
  FROM mart_freshness
)
SELECT
  'hard_critical_mart_age_minutes' AS gate_name,
  'hard' AS severity,
  '<=' AS comparison,
  CASE
    WHEN COUNTIF(mart_refreshed_at IS NULL) = 0 AND MAX(COALESCE(age_minutes, 999999)) <= 61 THEN 'PASS'
    WHEN COUNTIF(mart_refreshed_at IS NULL) = 0 AND MAX(COALESCE(age_minutes, 999999)) <= 90 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(MAX(COALESCE(age_minutes, 999999)) AS FLOAT64) AS metric_value,
  CAST(61 AS FLOAT64) AS pass_threshold,
  CAST(90 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'oldest critical mart refresh age = %d minutes',
    MAX(COALESCE(age_minutes, 999999))
  ) AS message,
  TO_JSON_STRING(
    ARRAY_AGG(
      STRUCT(
        mart_name,
        mart_refreshed_at,
        age_minutes
      )
      ORDER BY mart_name
    )
  ) AS details_json
FROM mart_ages
;

-- GATE hard_unmodeled_source_lag_minutes
WITH raw_latest AS (
  SELECT 'ghl' AS source_name, MAX(ingested_at) AS latest_raw_ts
  FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
  UNION ALL
  SELECT 'calendly', MAX(ingested_at)
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
  UNION ALL
  SELECT 'stripe', MAX(ingested_at)
  FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
  UNION ALL
  SELECT 'fathom', MAX(ingested_at)
  FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
),
critical_marts AS (
  SELECT MAX(mart_refreshed_at) AS mart_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  UNION ALL
  SELECT MAX(mart_refreshed_at)
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  UNION ALL
  SELECT MAX(mart_refreshed_at)
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
),
baseline AS (
  SELECT MIN(mart_refreshed_at) AS oldest_critical_mart_refresh
  FROM critical_marts
),
lag_by_source AS (
  SELECT
    r.source_name,
    r.latest_raw_ts,
    b.oldest_critical_mart_refresh AS mart_refreshed_at,
    GREATEST(
      TIMESTAMP_DIFF(r.latest_raw_ts, b.oldest_critical_mart_refresh, MINUTE),
      0
    ) AS unmodeled_lag_minutes
  FROM raw_latest r
  CROSS JOIN baseline b
)
SELECT
  'hard_unmodeled_source_lag_minutes' AS gate_name,
  'hard' AS severity,
  '<=' AS comparison,
  CASE
    WHEN COUNTIF(latest_raw_ts IS NULL OR mart_refreshed_at IS NULL) = 0
         AND MAX(COALESCE(unmodeled_lag_minutes, 999999)) <= 61 THEN 'PASS'
    WHEN COUNTIF(latest_raw_ts IS NULL OR mart_refreshed_at IS NULL) = 0
         AND MAX(COALESCE(unmodeled_lag_minutes, 999999)) <= 75 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(MAX(COALESCE(unmodeled_lag_minutes, 999999)) AS FLOAT64) AS metric_value,
  CAST(61 AS FLOAT64) AS pass_threshold,
  CAST(75 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'largest raw-to-mart lag across critical sources = %d minutes',
    MAX(COALESCE(unmodeled_lag_minutes, 999999))
  ) AS message,
  TO_JSON_STRING(
    ARRAY_AGG(
      STRUCT(
        source_name,
        latest_raw_ts,
        mart_refreshed_at,
        unmodeled_lag_minutes
      )
      ORDER BY source_name
    )
  ) AS details_json
FROM lag_by_source
;

-- GATE hard_deterministic_revenue_share_30d
WITH base AS (
  SELECT
    COALESCE(SUM(SAFE_CAST(net_amount AS NUMERIC)), 0) AS total_net_revenue,
    COALESCE(
      SUM(
        CASE
          WHEN match_status = 'matched' THEN SAFE_CAST(net_amount AS NUMERIC)
          ELSE 0
        END
      ),
      0
    ) AS crm_matched_net_revenue,
    COALESCE(
      SUM(
        CASE
          WHEN match_status = 'direct_sale_no_crm_contact' THEN SAFE_CAST(net_amount AS NUMERIC)
          ELSE 0
        END
      ),
      0
    ) AS direct_sale_deterministic_net_revenue
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
scored AS (
  SELECT
    total_net_revenue,
    crm_matched_net_revenue,
    direct_sale_deterministic_net_revenue,
    crm_matched_net_revenue + direct_sale_deterministic_net_revenue AS deterministic_covered_net_revenue,
    CASE
      WHEN total_net_revenue = 0 THEN 1.0
      ELSE SAFE_DIVIDE(crm_matched_net_revenue + direct_sale_deterministic_net_revenue, total_net_revenue)
    END AS deterministic_revenue_share
  FROM base
)
SELECT
  'hard_deterministic_revenue_share_30d' AS gate_name,
  'hard' AS severity,
  '>=' AS comparison,
  CASE
    WHEN deterministic_revenue_share >= 0.95 THEN 'PASS'
    WHEN deterministic_revenue_share >= 0.90 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(deterministic_revenue_share AS FLOAT64) AS metric_value,
  CAST(0.95 AS FLOAT64) AS pass_threshold,
  CAST(0.90 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'deterministic revenue coverage share over last 30d = %.4f',
    deterministic_revenue_share
  ) AS message,
  TO_JSON_STRING(
    STRUCT(
      total_net_revenue,
      crm_matched_net_revenue,
      direct_sale_deterministic_net_revenue,
      deterministic_covered_net_revenue,
      residual_not_deterministically_covered_net_revenue AS residual_net_revenue
    )
  ) AS details_json
FROM (
  SELECT
    total_net_revenue,
    crm_matched_net_revenue,
    direct_sale_deterministic_net_revenue,
    deterministic_covered_net_revenue,
    total_net_revenue - deterministic_covered_net_revenue AS residual_not_deterministically_covered_net_revenue,
    deterministic_revenue_share
  FROM scored
)
;

-- GATE hard_ambiguous_revenue_share_30d
WITH base AS (
  SELECT
    COALESCE(SUM(SAFE_CAST(net_amount AS NUMERIC)), 0) AS total_net_revenue,
    COALESCE(
      SUM(
        CASE
          WHEN match_status = 'ambiguous_multi_candidate' THEN SAFE_CAST(net_amount AS NUMERIC)
          ELSE 0
        END
      ),
      0
    ) AS ambiguous_net_revenue
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
scored AS (
  SELECT
    total_net_revenue,
    ambiguous_net_revenue,
    CASE
      WHEN total_net_revenue = 0 THEN 0.0
      ELSE SAFE_DIVIDE(ambiguous_net_revenue, total_net_revenue)
    END AS ambiguous_revenue_share
  FROM base
)
SELECT
  'hard_ambiguous_revenue_share_30d' AS gate_name,
  'hard' AS severity,
  '<=' AS comparison,
  CASE
    WHEN ambiguous_revenue_share <= 0.01 THEN 'PASS'
    WHEN ambiguous_revenue_share <= 0.02 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(ambiguous_revenue_share AS FLOAT64) AS metric_value,
  CAST(0.01 AS FLOAT64) AS pass_threshold,
  CAST(0.02 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'ambiguous revenue share over last 30d = %.4f',
    ambiguous_revenue_share
  ) AS message,
  TO_JSON_STRING(
    STRUCT(
      total_net_revenue,
      ambiguous_net_revenue
    )
  ) AS details_json
FROM scored
;

-- GATE hard_heuristic_truth_rows
WITH offending AS (
  SELECT
    'Marts.bridge_identity_contact_payment' AS table_name,
    COUNT(*) AS offending_row_count
  FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
  WHERE bridge_status = 'matched'
    AND COALESCE(match_method, '') NOT IN ('email_exact', 'email_canonical', 'phone_last10')

  UNION ALL

  SELECT
    'Marts.fct_payment_line_unified' AS table_name,
    COUNT(*) AS offending_row_count
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE match_status = 'matched'
    AND COALESCE(match_method, '') NOT IN ('email_exact', 'email_canonical', 'phone_last10', 'billing_email_direct')
)
SELECT
  'hard_heuristic_truth_rows' AS gate_name,
  'hard' AS severity,
  '<=' AS comparison,
  CASE
    WHEN SUM(offending_row_count) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status,
  CAST(SUM(offending_row_count) AS FLOAT64) AS metric_value,
  CAST(0 AS FLOAT64) AS pass_threshold,
  CAST(0 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'truth-layer rows using non-approved match methods = %d',
    SUM(offending_row_count)
  ) AS message,
  TO_JSON_STRING(
    ARRAY_AGG(
      STRUCT(
        table_name,
        offending_row_count
      )
      ORDER BY table_name
    )
  ) AS details_json
FROM offending
;

-- GATE soft_calendly_deterministic_email_link_rate_30d
WITH recent_invitees AS (
  SELECT
    invitee_id,
    invitee_email,
    contact_id,
    match_score,
    candidate_count,
    scheduled_start_time
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts`
  WHERE scheduled_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
scored AS (
  SELECT
    COUNTIF(invitee_email IS NOT NULL AND TRIM(invitee_email) != '') AS invitees_with_email,
    COUNTIF(
      invitee_email IS NOT NULL
      AND TRIM(invitee_email) != ''
      AND contact_id IS NOT NULL
      AND COALESCE(match_score, 0) >= 0.95
    ) AS deterministic_email_linked_invitees
  FROM recent_invitees
)
SELECT
  'soft_calendly_deterministic_email_link_rate_30d' AS gate_name,
  'soft' AS severity,
  '>=' AS comparison,
  CASE
    WHEN invitees_with_email = 0 THEN 'PASS'
    WHEN SAFE_DIVIDE(deterministic_email_linked_invitees, invitees_with_email) >= 0.90 THEN 'PASS'
    WHEN SAFE_DIVIDE(deterministic_email_linked_invitees, invitees_with_email) >= 0.85 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(
    CASE
      WHEN invitees_with_email = 0 THEN 1.0
      ELSE SAFE_DIVIDE(deterministic_email_linked_invitees, invitees_with_email)
    END
    AS FLOAT64
  ) AS metric_value,
  CAST(0.90 AS FLOAT64) AS pass_threshold,
  CAST(0.85 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'Calendly deterministic email link rate over last 30d = %.4f',
    CASE
      WHEN invitees_with_email = 0 THEN 1.0
      ELSE SAFE_DIVIDE(deterministic_email_linked_invitees, invitees_with_email)
    END
  ) AS message,
  TO_JSON_STRING(
    STRUCT(
      invitees_with_email,
      deterministic_email_linked_invitees
    )
  ) AS details_json
FROM scored
;

-- GATE soft_new_lead_touch_event_coverage_30d
WITH recent_leads AS (
  SELECT
    golden_contact_key,
    COALESCE(ghl_date_added_ts, ghl_first_seen_ts) AS lead_created_ts,
    COALESCE(touch_event_count, 0) AS touch_event_count,
    first_touch_source_confidence
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  WHERE golden_contact_key != 'ORPHAN_REVENUE'
    AND COALESCE(ghl_date_added_ts, ghl_first_seen_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
scored AS (
  SELECT
    COUNT(*) AS new_leads,
    COUNTIF(first_touch_source_confidence IN ('high', 'medium')) AS attributable_new_leads,
    COUNTIF(
      first_touch_source_confidence IN ('high', 'medium')
      AND touch_event_count > 0
    ) AS attributable_new_leads_with_touch_events
  FROM recent_leads
)
SELECT
  'soft_new_lead_touch_event_coverage_30d' AS gate_name,
  'soft' AS severity,
  '>=' AS comparison,
  CASE
    WHEN attributable_new_leads = 0 THEN 'PASS'
    WHEN SAFE_DIVIDE(attributable_new_leads_with_touch_events, attributable_new_leads) >= 0.70 THEN 'PASS'
    WHEN SAFE_DIVIDE(attributable_new_leads_with_touch_events, attributable_new_leads) >= 0.60 THEN 'WARN'
    ELSE 'FAIL'
  END AS status,
  CAST(
    CASE
      WHEN attributable_new_leads = 0 THEN 1.0
      ELSE SAFE_DIVIDE(attributable_new_leads_with_touch_events, attributable_new_leads)
    END
    AS FLOAT64
  ) AS metric_value,
  CAST(0.70 AS FLOAT64) AS pass_threshold,
  CAST(0.60 AS FLOAT64) AS warn_threshold,
  FORMAT(
    'new-lead event-touch coverage over attributable leads (last 30d) = %.4f',
    CASE
      WHEN attributable_new_leads = 0 THEN 1.0
      ELSE SAFE_DIVIDE(attributable_new_leads_with_touch_events, attributable_new_leads)
    END
  ) AS message,
  TO_JSON_STRING(
    STRUCT(
      new_leads,
      attributable_new_leads,
      attributable_new_leads_with_touch_events
    )
  ) AS details_json
FROM scored
;
