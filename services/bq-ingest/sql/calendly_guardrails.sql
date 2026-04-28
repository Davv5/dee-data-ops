-- Guard Rail 1: raw dedupe integrity by entity
SELECT
  entity_type,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT entity_id) AS distinct_entity_ids,
  COUNT(*) - COUNT(DISTINCT entity_id) AS duplicate_rows
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
GROUP BY entity_type
ORDER BY duplicate_rows DESC, total_rows DESC;

-- Guard Rail 2: latest checkpoint state by entity/run
WITH latest AS (
  SELECT
    run_id,
    entity_type,
    status,
    pages_processed,
    rows_written,
    updated_at,
    error_text,
    ROW_NUMBER() OVER (PARTITION BY run_id, entity_type ORDER BY updated_at DESC) AS rn
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_backfill_state`
)
SELECT
  run_id,
  entity_type,
  status,
  pages_processed,
  rows_written,
  updated_at,
  error_text
FROM latest
WHERE rn = 1
ORDER BY updated_at DESC, run_id, entity_type;

-- Guard Rail 3: stale running entities (potential stuck executions)
WITH latest AS (
  SELECT
    run_id,
    entity_type,
    status,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY run_id, entity_type ORDER BY updated_at DESC) AS rn
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_backfill_state`
)
SELECT
  run_id,
  entity_type,
  status,
  updated_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) AS minutes_since_update
FROM latest
WHERE rn = 1
  AND status IN ('RUNNING', 'PAUSED_LIMIT_REACHED')
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated_at, MINUTE) > 20
ORDER BY minutes_since_update DESC;

-- Guard Rail 4: invitee queue drain health for latest run
WITH latest_run AS (
  SELECT run_id
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_backfill_state`
  ORDER BY updated_at DESC
  LIMIT 1
)
SELECT
  q.run_id,
  q.status,
  COUNT(*) AS events
FROM `project-41542e21-470f-4589-96d.Raw.calendly_invitee_backfill_state` q
JOIN latest_run r
  ON r.run_id = q.run_id
GROUP BY q.run_id, q.status
ORDER BY events DESC;

-- Guard Rail 5: webhook idempotency and unsupported event noise
SELECT
  webhook_event_type,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT webhook_event_id) AS distinct_event_ids,
  COUNT(*) - COUNT(DISTINCT webhook_event_id) AS duplicate_event_rows
FROM `project-41542e21-470f-4589-96d.Raw.calendly_webhook_events_raw`
GROUP BY webhook_event_type
ORDER BY total_rows DESC;
