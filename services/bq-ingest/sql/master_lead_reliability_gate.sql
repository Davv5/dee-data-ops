-- Master lead wide reliability gate.
-- Purpose: quantify how much of each metric is event-derived vs fallback-derived.

-- 1) Overall row count and touch presence.
SELECT
  COUNT(*) AS total_leads,
  COUNTIF(first_touch_source IS NOT NULL) AS first_touch_source_present,
  COUNTIF(last_touch_source IS NOT NULL) AS last_touch_source_present,
  COUNTIF(first_touch_event_ts IS NOT NULL) AS first_touch_event_ts_present,
  COUNTIF(last_touch_event_ts IS NOT NULL) AS last_touch_event_ts_present
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`;

-- 2) Confidence distribution.
SELECT
  first_touch_source_confidence,
  last_touch_source_confidence,
  attribution_quality_flag,
  COUNT(*) AS leads
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
GROUP BY 1, 2, 3
ORDER BY leads DESC;

-- 3) Source system usage for first/last touch derivation.
SELECT
  first_touch_source_used,
  last_touch_source_used,
  COUNT(*) AS leads
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
GROUP BY 1, 2
ORDER BY leads DESC;

-- 4) Tracking era distribution.
SELECT
  tracking_era,
  COUNT(*) AS leads
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
GROUP BY 1
ORDER BY leads DESC;

-- 5) Unified payment enrichment reliability.
SELECT
  COUNTIF(has_any_payment) AS leads_with_any_payment,
  COUNTIF(total_payment_count > 0) AS leads_with_payment_count,
  COUNTIF(total_net_revenue > 0) AS leads_with_positive_revenue,
  ROUND(SUM(total_net_revenue), 2) AS total_net_revenue,
  ROUND(SUM(fanbasis_net_revenue), 2) AS fanbasis_net_revenue,
  ROUND(SUM(stripe_net_revenue), 2) AS stripe_net_revenue
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`;
