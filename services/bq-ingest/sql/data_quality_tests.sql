-- Data quality tests for the GTM Lead Warehouse.
-- Each statement returns exactly one row:
--   test_name, status ('PASS'/'FAIL'/'WARN'), failing_rows, message
-- The Python runner executes each block, writes results to Raw.dq_test_results,
-- and exits 1 if any test has status='FAIL'.
-- Delimiter: -- TEST

-- TEST
SELECT
  'raw.ghl_contacts.freshness' AS test_name,
  CASE WHEN MAX(event_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
       THEN 'PASS' ELSE 'FAIL' END AS status,
  0 AS failing_rows,
  CONCAT('latest_event_ts=', CAST(MAX(event_ts) AS STRING)) AS message
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'contacts';

-- TEST
SELECT
  'raw.ghl_contacts.no_null_entity_id' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows with null entity_id') AS message
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'contacts' AND entity_id IS NULL;

-- TEST
SELECT
  'raw.fanbasis_transactions.no_null_transaction_id' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows with null transaction_id') AS message
FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`
WHERE transaction_id IS NULL;

-- TEST
SELECT
  'raw.fanbasis_transactions.freshness' AS test_name,
  CASE WHEN MAX(event_ts) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR)
       THEN 'PASS' ELSE 'WARN' END AS status,
  0 AS failing_rows,
  CONCAT('latest_event_ts=', CAST(MAX(event_ts) AS STRING)) AS message
FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`;

-- TEST
SELECT
  'core.fct_ghl_contacts.no_duplicate_contact_id' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' duplicate contact_ids') AS message
FROM (
  SELECT contact_id, COUNT(*) AS n
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`
  GROUP BY contact_id HAVING n > 1
);

-- TEST
SELECT
  'core.fct_ghl_opportunities.no_duplicate_opportunity_id' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' duplicate opportunity_ids') AS message
FROM (
  SELECT opportunity_id, COUNT(*) AS n
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`
  GROUP BY opportunity_id HAVING n > 1
);

-- TEST
SELECT
  'core.fct_fanbasis_transactions.no_duplicate_transaction_id' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' duplicate transaction_ids') AS message
FROM (
  SELECT transaction_id, COUNT(*) AS n
  FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions`
  GROUP BY transaction_id HAVING n > 1
);

-- TEST
SELECT
  'marts.dim_golden_contact.has_rows' AS test_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows') AS message
FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact`;

-- TEST
SELECT
  'marts.mart_master_lead_wide.has_rows' AS test_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows') AS message
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`;

-- TEST
SELECT
  'marts.fct_payment_line_unified.has_rows' AS test_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows') AS message
FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`;

-- TEST
SELECT
  'marts.fct_payment_line_unified.no_negative_net_amount' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows with net_amount < 0') AS message
FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
WHERE SAFE_CAST(net_amount AS NUMERIC) < 0;

-- TEST
SELECT
  'marts.fct_payment_line_unified.match_rate_above_80pct' AS test_name,
  CASE WHEN unmatched_pct <= 0.20 THEN 'PASS'
       WHEN unmatched_pct <= 0.35 THEN 'WARN'
       ELSE 'FAIL' END AS status,
  CAST(ROUND(unmatched_pct * total, 0) AS INT64) AS failing_rows,
  CONCAT(CAST(ROUND(unmatched_pct * 100, 1) AS STRING), '% unmatched payments') AS message
FROM (
  SELECT
    COUNTIF(match_status != 'matched') / COUNT(*) AS unmatched_pct,
    COUNT(*) AS total
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
);

-- TEST
SELECT
  'marts.dim_golden_contact.no_null_golden_contact_key' AS test_name,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  COUNT(*) AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' rows with null golden_contact_key') AS message
FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact`
WHERE golden_contact_key IS NULL;

-- TEST
SELECT
  'marts.rpt_campaign_funnel_month.has_recent_data' AS test_name,
  CASE WHEN MAX(report_month) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
       THEN 'PASS' ELSE 'FAIL' END AS status,
  0 AS failing_rows,
  CONCAT('latest_report_month=', CAST(MAX(report_month) AS STRING)) AS message
FROM `project-41542e21-470f-4589-96d.Marts.rpt_campaign_funnel_month`;

-- TEST
SELECT
  'marts.rpt_revenue_by_stage_month.has_recent_data' AS test_name,
  CASE WHEN MAX(report_month) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
       THEN 'PASS' ELSE 'FAIL' END AS status,
  0 AS failing_rows,
  CONCAT('latest_report_month=', CAST(MAX(report_month) AS STRING)) AS message
FROM `project-41542e21-470f-4589-96d.Marts.rpt_revenue_by_stage_month`;

-- TEST
SELECT
  'marts.mart_master_lead_wide.lead_count_not_zero_today' AS test_name,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failing_rows,
  CONCAT(CAST(COUNT(*) AS STRING), ' leads created in last 7 days') AS message
FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
WHERE ghl_date_added_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
