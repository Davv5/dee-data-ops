-- Set run_id manually before running section A.
-- Example: SET run_id = 'fathom-20260402-123456-abcd1234';

-- A) checkpoint terminal status
SELECT
  entity_type,
  status,
  pages_processed,
  rows_written,
  error_text,
  updated_at
FROM `project-41542e21-470f-4589-96d.Raw.fathom_backfill_state`
WHERE run_id = @run_id
ORDER BY entity_type;

-- B) raw idempotency
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CONCAT(entity_type, ':', workspace_id, ':', entity_id)) AS distinct_keys,
  COUNT(*) - COUNT(DISTINCT CONCAT(entity_type, ':', workspace_id, ':', entity_id)) AS duplicate_rows,
  MIN(ingested_at) AS min_ingested_at,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`;

-- C) classification sanity
SELECT
  COUNT(*) AS total_calls,
  COUNTIF(is_internal_only) AS internal_calls,
  COUNTIF(is_revenue_relevant) AS revenue_calls,
  COUNTIF(is_revenue_relevant AND is_internal_only) AS conflicting_revenue_internal
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`;

-- D) contact bridge coverage
SELECT
  COUNT(DISTINCT c.call_id) AS total_calls,
  COUNT(DISTINCT IF(b.contact_id IS NOT NULL, c.call_id, NULL)) AS calls_linked_to_contact,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNT(DISTINCT IF(b.contact_id IS NOT NULL, c.call_id, NULL)),
      COUNT(DISTINCT c.call_id)
    ),
    2
  ) AS pct_calls_linked_to_contact
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` c
LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts` b
  ON b.call_id = c.call_id;

-- E) opportunity bridge coverage
SELECT
  COUNT(DISTINCT c.call_id) AS total_revenue_calls,
  COUNT(DISTINCT IF(o.opportunity_id IS NOT NULL, c.call_id, NULL)) AS revenue_calls_linked_to_opportunity,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNT(DISTINCT IF(o.opportunity_id IS NOT NULL, c.call_id, NULL)),
      COUNT(DISTINCT c.call_id)
    ),
    2
  ) AS pct_revenue_calls_linked_to_opportunity
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls` c
LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities` o
  ON o.call_id = c.call_id;
