-- Set run_id for checkpoint validation if needed.
-- Example: SET run_id = 'calendly-20260402-123456-abcd1234';

-- A) checkpoint state for a specific run
SELECT
  entity_type,
  status,
  pages_processed,
  rows_written,
  error_text,
  updated_at
FROM `project-41542e21-470f-4589-96d.Raw.calendly_backfill_state`
WHERE run_id = @run_id
ORDER BY entity_type;

-- B) raw dedupe and coverage
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CONCAT(entity_type, ':', entity_id)) AS distinct_entity_rows,
  COUNT(*) - COUNT(DISTINCT CONCAT(entity_type, ':', entity_id)) AS duplicate_rows,
  COUNTIF(entity_type = 'event_types') AS event_type_rows,
  COUNTIF(entity_type = 'routing_forms') AS routing_form_rows,
  COUNTIF(entity_type = 'routing_form_submissions') AS routing_form_submission_rows,
  COUNTIF(entity_type = 'scheduled_events') AS scheduled_event_rows,
  COUNTIF(entity_type = 'event_invitees') AS invitee_rows
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`;

-- B2) webhook raw quality
SELECT
  COUNT(*) AS webhook_total_rows,
  COUNT(DISTINCT webhook_event_id) AS webhook_distinct_ids,
  COUNTIF(webhook_event_type = 'invitee.created') AS invitee_created_rows,
  COUNTIF(webhook_event_type = 'invitee.canceled') AS invitee_canceled_rows
FROM `project-41542e21-470f-4589-96d.Raw.calendly_webhook_events_raw`;

-- C) Core row checks
SELECT 'Core.fct_calendly_scheduled_events' AS table_name, COUNT(*) AS row_count
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_scheduled_events`
UNION ALL
SELECT 'Core.fct_calendly_event_invitees', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees`
UNION ALL
SELECT 'Core.dim_calendly_event_types', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types`
UNION ALL
SELECT 'Core.fct_calendly_invitee_lifecycle', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_invitee_lifecycle`
UNION ALL
SELECT 'Core.dim_calendly_routing_forms', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.dim_calendly_routing_forms`
UNION ALL
SELECT 'Core.fct_calendly_routing_form_submissions', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_routing_form_submissions`
UNION ALL
SELECT 'Core.bridge_calendly_invitee_contacts', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts`
UNION ALL
SELECT 'Core.fct_calendly_match_diagnostics', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_match_diagnostics`;

-- D) contact match coverage
SELECT
  COUNT(*) AS total_invitees,
  COUNTIF(contact_id IS NOT NULL) AS invitees_linked_to_contact,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(contact_id IS NOT NULL), COUNT(*)), 2) AS pct_invitees_linked_to_contact,
  COUNTIF(match_confidence = 'high') AS high_confidence_links
FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts`;
