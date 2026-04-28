-- Ingestion parity checklist across API lanes (Raw -> Core).
-- Run after backfills / incrementals and before trusting downstream Studio metrics.

-- 1) Raw freshness by source lane.
SELECT 'ghl_objects_raw' AS raw_table, MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
UNION ALL
SELECT 'calendly_objects_raw', MAX(ingested_at)
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
UNION ALL
SELECT 'fanbasis_transactions_txn_raw', MAX(ingested_at)
FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`
UNION ALL
SELECT 'stripe_objects_raw', MAX(ingested_at)
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
UNION ALL
SELECT 'fathom_calls_raw', MAX(ingested_at)
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
ORDER BY raw_table;

-- 2) Latest backfill-state rows by source.
WITH latest_ghl AS (
  SELECT run_id, entity_type, status, pages_processed, rows_written, updated_at
  FROM `project-41542e21-470f-4589-96d.Raw.ghl_backfill_state`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY updated_at DESC) = 1
),
latest_cal AS (
  SELECT run_id, entity_type, status, pages_processed, rows_written, updated_at
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_backfill_state`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY updated_at DESC) = 1
),
latest_fan AS (
  SELECT run_id, 'transactions' AS entity_type, status, pages_processed, rows_written, updated_at
  FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_backfill_state`
  QUALIFY ROW_NUMBER() OVER (ORDER BY updated_at DESC) = 1
),
latest_stripe AS (
  SELECT run_id, object_type AS entity_type, status, pages_processed, rows_written, updated_at
  FROM `project-41542e21-470f-4589-96d.Raw.stripe_backfill_state`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY object_type ORDER BY updated_at DESC) = 1
),
latest_fathom AS (
  SELECT run_id, entity_type, status, pages_processed, rows_written, updated_at
  FROM `project-41542e21-470f-4589-96d.Raw.fathom_backfill_state`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_type ORDER BY updated_at DESC) = 1
)
SELECT 'ghl' AS source, * FROM latest_ghl
UNION ALL
SELECT 'calendly' AS source, * FROM latest_cal
UNION ALL
SELECT 'fanbasis' AS source, * FROM latest_fan
UNION ALL
SELECT 'stripe' AS source, * FROM latest_stripe
UNION ALL
SELECT 'fathom' AS source, * FROM latest_fathom
ORDER BY source, entity_type;

-- 3) Raw entity/object counts by source (current inventory).
SELECT 'ghl' AS source, entity_type AS object_name, COUNT(*) AS raw_rows, COUNT(DISTINCT entity_id) AS raw_distinct_ids
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
GROUP BY 1, 2
UNION ALL
SELECT 'calendly', entity_type, COUNT(*), COUNT(DISTINCT entity_id)
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
GROUP BY 1, 2
UNION ALL
SELECT 'fanbasis', 'transactions', COUNT(*), COUNT(DISTINCT transaction_id)
FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`
UNION ALL
SELECT 'stripe', object_type, COUNT(*), COUNT(DISTINCT object_id)
FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`
GROUP BY 1, 2
UNION ALL
SELECT 'fathom', entity_type, COUNT(*), COUNT(DISTINCT entity_id)
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
GROUP BY 1, 2
ORDER BY source, raw_rows DESC;

-- 4) Core parity table (directional Raw distinct -> Core distinct checks).
WITH parity AS (
  SELECT
    'ghl_contacts' AS check_name,
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw` WHERE entity_type='contacts') AS raw_distinct,
    (SELECT COUNT(DISTINCT contact_id) FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`) AS core_distinct,
    0.95 AS expected_min_ratio
  UNION ALL
  SELECT
    'ghl_opportunities',
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw` WHERE entity_type='opportunities'),
    (SELECT COUNT(DISTINCT opportunity_id) FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`),
    0.95
  UNION ALL
  SELECT
    'ghl_form_submissions',
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw` WHERE entity_type='form_submissions'),
    (SELECT COUNT(DISTINCT submission_id) FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_form_submissions`),
    0.95
  UNION ALL
  SELECT
    'calendly_scheduled_events',
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw` WHERE entity_type='scheduled_events'),
    (SELECT COUNT(DISTINCT scheduled_event_id) FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_scheduled_events`),
    0.95
  UNION ALL
  SELECT
    'calendly_event_invitees',
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw` WHERE entity_type='event_invitees'),
    (SELECT COUNT(DISTINCT invitee_id) FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees`),
    0.95
  UNION ALL
  SELECT
    'fanbasis_transactions',
    (SELECT COUNT(DISTINCT transaction_id) FROM `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`),
    (SELECT COUNT(DISTINCT transaction_id) FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions`),
    0.95
  UNION ALL
  SELECT
    'stripe_charges_to_payments',
    (SELECT COUNT(DISTINCT object_id) FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw` WHERE object_type='charges'),
    (SELECT COUNT(DISTINCT payment_id) FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments`),
    0.80
  UNION ALL
  SELECT
    'fathom_calls',
    (SELECT COUNT(DISTINCT entity_id) FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw` WHERE entity_type='calls'),
    (SELECT COUNT(DISTINCT call_id) FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls`),
    0.95
)
SELECT
  check_name,
  raw_distinct,
  core_distinct,
  ROUND(SAFE_DIVIDE(core_distinct, NULLIF(raw_distinct, 0)), 4) AS core_to_raw_ratio,
  expected_min_ratio,
  CASE
    WHEN raw_distinct = 0 THEN 'NO_RAW_DATA'
    WHEN SAFE_DIVIDE(core_distinct, NULLIF(raw_distinct, 0)) >= expected_min_ratio THEN 'PASS'
    WHEN SAFE_DIVIDE(core_distinct, NULLIF(raw_distinct, 0)) >= expected_min_ratio * 0.8 THEN 'WARN'
    ELSE 'FAIL'
  END AS parity_status
FROM parity
ORDER BY
  CASE parity_status
    WHEN 'FAIL' THEN 1
    WHEN 'WARN' THEN 2
    WHEN 'PASS' THEN 3
    ELSE 4
  END,
  check_name;
