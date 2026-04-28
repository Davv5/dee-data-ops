-- Mart layer sanity checks (run in BigQuery console or CI).
-- Project/dataset must match your deployment.

SELECT 'Marts.dim_golden_contact' AS table_name, COUNT(*) AS row_count
FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact`;

SELECT 'Marts.mart_master_lead_wide', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`;

SELECT 'Marts.fct_fanbasis_payment_line', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`;

SELECT 'Marts.fct_payment_line_unified', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`;

SELECT 'Marts.bridge_identity_contact_payment', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`;

SELECT 'Marts.rpt_campaign_funnel_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_campaign_funnel_month`;

SELECT 'Marts.rpt_revenue_by_stage_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_revenue_by_stage_month`;

SELECT 'Marts.rpt_applications_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_applications_month`;

SELECT 'Marts.rpt_call_outcome_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_call_outcome_week`;

SELECT 'Marts.rpt_calendly_status_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_calendly_status_week`;

SELECT 'Marts.rpt_payment_reconciliation_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_payment_reconciliation_month`;

SELECT 'Marts.rpt_identity_quality_daily', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_identity_quality_daily`;

SELECT 'Marts.rpt_setter_identity_unknown_queue', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_setter_identity_unknown_queue`;

SELECT 'Marts.rpt_setter_identity_coverage_daily', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_setter_identity_coverage_daily`;

SELECT 'Marts.v_unified_dashboard_schema', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.v_unified_dashboard_schema`;

SELECT 'Marts.rpt_ghl_activity_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_ghl_activity_week`;

SELECT 'Marts.rpt_calendly_routing_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_calendly_routing_week`;

SELECT 'Marts.rpt_stripe_lifecycle_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_stripe_lifecycle_month`;

SELECT 'Marts.rpt_fanbasis_customer_month', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_fanbasis_customer_month`;

SELECT 'Marts.rpt_fathom_outcomes_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_fathom_outcomes_week`;

SELECT 'Marts.rpt_fathom_closer_effectiveness_week', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.rpt_fathom_closer_effectiveness_week`;

-- Match quality: payments without golden contact
SELECT
  match_status,
  COUNT(*) AS payment_lines,
  ROUND(SUM(SAFE_CAST(net_amount AS NUMERIC)), 2) AS net_amount_sum
FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
GROUP BY match_status
ORDER BY payment_lines DESC;

-- Source mix in unified payment layer.
SELECT
  source_system,
  COUNT(*) AS payment_lines,
  ROUND(SUM(SAFE_CAST(net_amount AS NUMERIC)), 2) AS net_amount_sum
FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
GROUP BY source_system
ORDER BY net_amount_sum DESC;

-- Identity bridge coverage and method mix
SELECT
  bridge_status,
  COUNT(*) AS customers
FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
GROUP BY bridge_status
ORDER BY customers DESC;

SELECT
  match_method,
  bridge_status,
  COUNT(*) AS customers
FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
GROUP BY 1, 2
ORDER BY customers DESC;

-- Attribution gaps on leads
SELECT
  attribution_gap_reason,
  campaign_reporting,
  COUNT(*) AS contacts
FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact`
GROUP BY 1, 2
ORDER BY contacts DESC
LIMIT 50;

-- Master lead wide quality distribution.
SELECT
  first_touch_source_confidence,
  last_touch_source_confidence,
  attribution_quality_flag,
  COUNT(*) AS leads
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
GROUP BY 1, 2, 3
ORDER BY leads DESC;

SELECT
  tracking_era,
  COUNT(*) AS leads
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
GROUP BY 1
ORDER BY leads DESC;

-- Typeform field coverage on leads that have at least one Typeform response.
WITH responder_leads AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  WHERE COALESCE(typeform_responses_count, 0) > 0
)
SELECT
  check_name,
  pct_populated,
  CASE WHEN pct_populated >= 0.80 THEN 'PASS' ELSE 'WARN' END AS result
FROM (
  SELECT
    'mart_typeform_age_bracket_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_age_bracket IS NOT NULL AND TRIM(typeform_age_bracket) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_business_stage_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_business_stage IS NOT NULL AND TRIM(typeform_business_stage) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_investment_range_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_investment_range IS NOT NULL AND TRIM(typeform_investment_range) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_core_struggle_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_core_struggle IS NOT NULL AND TRIM(typeform_core_struggle) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads
)
ORDER BY check_name;

-- Stage revenue surface
SELECT
  report_month,
  pipeline_name,
  stage_name,
  ROUND(SUM(revenue_net_sum), 2) AS revenue_net_sum
FROM `project-41542e21-470f-4589-96d.Marts.rpt_revenue_by_stage_month`
GROUP BY 1, 2, 3
ORDER BY report_month DESC, revenue_net_sum DESC
LIMIT 30;

-- Stripe/Fanbasis reconciliation surface
SELECT
  report_month,
  source_system,
  ROUND(gross_collected_amount, 2) AS gross_collected_amount,
  ROUND(refund_amount, 2) AS refund_amount,
  ROUND(dispute_amount, 2) AS dispute_amount,
  ROUND(adjusted_net_amount, 2) AS adjusted_net_amount,
  ROUND(unified_net_amount, 2) AS unified_net_amount
FROM `project-41542e21-470f-4589-96d.Marts.rpt_payment_reconciliation_month`
ORDER BY report_month DESC, source_system;

-- Identity quality by source/day
SELECT
  report_date,
  source_system,
  match_status,
  payment_line_count,
  ROUND(net_amount_sum, 2) AS net_amount_sum,
  ROUND(net_amount_share_within_source_day, 4) AS net_amount_share_within_source_day
FROM `project-41542e21-470f-4589-96d.Marts.rpt_identity_quality_daily`
ORDER BY report_date DESC, source_system, match_status;

-- Activity + lifecycle + outcomes quick surfaces
SELECT
  report_week,
  location_id,
  task_count,
  note_count,
  conversation_message_count,
  outbound_call_count
FROM `project-41542e21-470f-4589-96d.Marts.rpt_ghl_activity_week`
ORDER BY report_week DESC, location_id
LIMIT 30;

SELECT
  report_week,
  closer_name,
  call_count,
  ROUND(action_items_per_call, 2) AS action_items_per_call,
  ROUND(questions_per_call, 2) AS questions_per_call,
  ROUND(discovery_questions_per_call, 2) AS discovery_questions_per_call,
  ROUND(linked_call_win_rate, 4) AS linked_call_win_rate,
  ROUND(discovery_win_rate, 4) AS discovery_win_rate
FROM `project-41542e21-470f-4589-96d.Marts.rpt_fathom_closer_effectiveness_week`
ORDER BY report_week DESC, action_items_per_call DESC
LIMIT 30;

SELECT
  report_month,
  invoice_count,
  subscription_active_like_count,
  payment_gross_collected_sum
FROM `project-41542e21-470f-4589-96d.Marts.rpt_stripe_lifecycle_month`
ORDER BY report_month DESC;

-- Lead magnet URL surface checks (landing/referrer + URL-param UTM extraction).
SELECT
  COUNT(*) AS lead_magnet_rows,
  COUNTIF(landing_page_url IS NOT NULL AND TRIM(landing_page_url) != '') AS landing_page_url_populated,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(landing_page_url IS NOT NULL AND TRIM(landing_page_url) != ''),
      COUNT(*)
    ),
    4
  ) AS landing_page_url_pct,
  COUNTIF(referrer_url IS NOT NULL AND TRIM(referrer_url) != '') AS referrer_url_populated,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(referrer_url IS NOT NULL AND TRIM(referrer_url) != ''),
      COUNT(*)
    ),
    4
  ) AS referrer_url_pct,
  COUNTIF(utm_source IS NOT NULL AND TRIM(utm_source) != '') AS utm_source_populated,
  COUNTIF(utm_medium IS NOT NULL AND TRIM(utm_medium) != '') AS utm_medium_populated,
  COUNTIF(utm_campaign IS NOT NULL AND TRIM(utm_campaign) != '') AS utm_campaign_populated
FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity`;

SELECT
  landing_page_url,
  COUNT(*) AS submissions
FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity`
WHERE landing_page_url IS NOT NULL
GROUP BY landing_page_url
ORDER BY submissions DESC
LIMIT 25;

SELECT
  referrer_url,
  COUNT(*) AS submissions
FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity`
WHERE referrer_url IS NOT NULL
GROUP BY referrer_url
ORDER BY submissions DESC
LIMIT 25;
