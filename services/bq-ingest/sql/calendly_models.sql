-- STG: scheduled events
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_scheduled_events` AS
SELECT
  entity_id AS scheduled_event_id,
  organization_uri,
  user_uri,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  JSON_VALUE(payload_json, '$.uri') AS scheduled_event_uri,
  COALESCE(JSON_VALUE(payload_json, '$.name'), JSON_VALUE(payload_json, '$.event_name')) AS event_name,
  JSON_VALUE(payload_json, '$.status') AS event_status,
  SAFE_CAST(JSON_VALUE(payload_json, '$.start_time') AS TIMESTAMP) AS scheduled_start_time,
  SAFE_CAST(JSON_VALUE(payload_json, '$.end_time') AS TIMESTAMP) AS scheduled_end_time,
  JSON_VALUE(payload_json, '$.event_type') AS event_type_uri,
  JSON_VALUE(payload_json, '$.location.type') AS location_type,
  JSON_VALUE(payload_json, '$.location.location') AS location_value,
  JSON_VALUE(payload_json, '$.calendar_event.external_id') AS external_calendar_event_id,
  JSON_VALUE(payload_json, '$.calendar_event.kind') AS external_calendar_event_kind,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type = 'scheduled_events';

-- STG: invitees
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_event_invitees` AS
SELECT
  entity_id AS invitee_id,
  parent_id AS scheduled_event_id,
  organization_uri,
  user_uri,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  JSON_VALUE(payload_json, '$.uri') AS invitee_uri,
  LOWER(TRIM(JSON_VALUE(payload_json, '$.email'))) AS invitee_email,
  JSON_VALUE(payload_json, '$.name') AS invitee_name,
  JSON_VALUE(payload_json, '$.status') AS invitee_status,
  JSON_VALUE(payload_json, '$.timezone') AS invitee_timezone,
  SAFE_CAST(JSON_VALUE(payload_json, '$.created_at') AS TIMESTAMP) AS invitee_created_at,
  SAFE_CAST(JSON_VALUE(payload_json, '$.updated_at') AS TIMESTAMP) AS invitee_updated_at,
  JSON_VALUE(payload_json, '$.tracking.utm_source') AS utm_source,
  JSON_VALUE(payload_json, '$.tracking.utm_medium') AS utm_medium,
  JSON_VALUE(payload_json, '$.tracking.utm_campaign') AS utm_campaign,
  JSON_VALUE(payload_json, '$.tracking.utm_term') AS utm_term,
  JSON_VALUE(payload_json, '$.tracking.utm_content') AS utm_content,
  -- Booking form custom question answers
  (SELECT JSON_VALUE(qa, '$.answer')
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.questions_and_answers')) AS qa
   WHERE REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(qa, '$.question'), '')), r'where|source|booking link|find|hear about')
   LIMIT 1) AS self_reported_source,
  (SELECT JSON_VALUE(qa, '$.answer')
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.questions_and_answers')) AS qa
   WHERE REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(qa, '$.question'), '')), r'goal|mean to you|achieve|motivat|important')
   LIMIT 1) AS emotional_goal_value,
  LOWER(JSON_VALUE(payload_json, '$.status')) = 'canceled' AS is_canceled,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type = 'event_invitees';

-- CORE: scheduled events fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_calendly_scheduled_events` AS
SELECT
  scheduled_event_id,
  scheduled_event_uri,
  organization_uri,
  user_uri,
  event_name,
  event_status,
  event_type_uri,
  scheduled_start_time,
  scheduled_end_time,
  location_type,
  location_value,
  external_calendar_event_id,
  external_calendar_event_kind,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_scheduled_events`;

-- CORE: invitees fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` AS
SELECT
  i.invitee_id,
  i.scheduled_event_id,
  e.event_name,
  e.event_status,
  e.event_type_uri,
  e.scheduled_start_time,
  e.scheduled_end_time,
  i.invitee_email,
  i.invitee_name,
  i.invitee_status,
  i.invitee_timezone,
  i.invitee_created_at,
  i.invitee_updated_at,
  i.utm_source,
  i.utm_medium,
  i.utm_campaign,
  i.utm_term,
  i.utm_content,
  i.self_reported_source,
  i.emotional_goal_value,
  i.is_canceled,
  i.organization_uri,
  i.user_uri,
  i.event_ts,
  i.updated_at_ts,
  i.ingested_at,
  i.backfill_run_id,
  i.is_backfill,
  i.payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_event_invitees` i
LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_scheduled_events` e
  ON e.scheduled_event_id = i.scheduled_event_id;

-- CORE: deterministic invitee -> GHL contact bridge
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` AS
WITH ghl_contacts AS (
  SELECT
    c.location_id,
    c.contact_id,
    c.email,
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
    c.full_name,
    c.phone,
    c.source,
    c.last_seen_ts,
    cft.utm_source_first,
    cft.utm_medium_first,
    cft.utm_campaign_first,
    cft.utm_term_first,
    cft.utm_content_first,
    cft.source_first,
    cft.assigned_to_user_id_first,
    cft.first_contact_ts
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts` c
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts_first_touch` cft
    ON cft.location_id = c.location_id
   AND cft.contact_id = c.contact_id
  WHERE c.email IS NOT NULL
),
invitees AS (
  SELECT
    invitee_id,
    scheduled_event_id,
    invitee_email,
    CASE
      WHEN invitee_email IS NULL THEN NULL
      WHEN SPLIT(invitee_email, '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(invitee_email, '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE invitee_email
    END AS invitee_email_canon,
    invitee_name,
    event_status,
    scheduled_start_time,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_term,
    utm_content
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees`
),
candidates AS (
  SELECT
    i.invitee_id,
    i.scheduled_event_id,
    i.invitee_email,
    i.invitee_name,
    i.event_status,
    i.scheduled_start_time,
    i.utm_source,
    i.utm_medium,
    i.utm_campaign,
    i.utm_term,
    i.utm_content,
    g.location_id,
    g.contact_id,
    g.email AS matched_contact_email,
    g.full_name AS matched_contact_name,
    g.phone AS matched_contact_phone,
    g.source AS matched_contact_source,
    g.utm_source_first,
    g.utm_medium_first,
    g.utm_campaign_first,
    g.utm_term_first,
    g.utm_content_first,
    g.source_first,
    g.assigned_to_user_id_first,
    g.first_contact_ts,
    g.last_seen_ts,
    'email_exact' AS match_method,
    1.00 AS match_score
  FROM invitees i
  JOIN ghl_contacts g
    ON i.invitee_email = g.email_norm

  UNION ALL

  SELECT
    i.invitee_id,
    i.scheduled_event_id,
    i.invitee_email,
    i.invitee_name,
    i.event_status,
    i.scheduled_start_time,
    i.utm_source,
    i.utm_medium,
    i.utm_campaign,
    i.utm_term,
    i.utm_content,
    g.location_id,
    g.contact_id,
    g.email AS matched_contact_email,
    g.full_name AS matched_contact_name,
    g.phone AS matched_contact_phone,
    g.source AS matched_contact_source,
    g.utm_source_first,
    g.utm_medium_first,
    g.utm_campaign_first,
    g.utm_term_first,
    g.utm_content_first,
    g.source_first,
    g.assigned_to_user_id_first,
    g.first_contact_ts,
    g.last_seen_ts,
    'email_canonical' AS match_method,
    0.95 AS match_score
  FROM invitees i
  JOIN ghl_contacts g
    ON i.invitee_email_canon = g.email_canon
   AND i.invitee_email_canon IS NOT NULL
),
ranked AS (
  SELECT
    c.*,
    COUNT(*) OVER (PARTITION BY c.invitee_id) AS candidate_count,
    ROW_NUMBER() OVER (
      PARTITION BY c.invitee_id
      ORDER BY c.match_score DESC, c.last_seen_ts DESC, c.contact_id
    ) AS rn
  FROM candidates c
)
SELECT
  i.invitee_id,
  i.scheduled_event_id,
  i.invitee_email,
  i.invitee_name,
  i.event_status,
  i.scheduled_start_time,
  i.utm_source,
  i.utm_medium,
  i.utm_campaign,
  i.utm_term,
  i.utm_content,
  r.location_id,
  r.contact_id,
  r.matched_contact_email,
  r.matched_contact_name,
  r.matched_contact_phone,
  r.matched_contact_source,
  r.utm_source_first,
  r.utm_medium_first,
  r.utm_campaign_first,
  r.utm_term_first,
  r.utm_content_first,
  r.source_first,
  r.assigned_to_user_id_first AS setter_at_first_contact,
  r.first_contact_ts,
  r.match_method,
  r.match_score,
  COALESCE(r.candidate_count, 0) AS candidate_count,
  CASE
    WHEN r.contact_id IS NULL THEN 'none'
    WHEN r.match_score >= 0.95 AND COALESCE(r.candidate_count, 0) = 1 THEN 'high'
    WHEN r.match_score >= 0.95 THEN 'medium'
    ELSE 'low'
  END AS match_confidence
FROM invitees i
LEFT JOIN ranked r
  ON r.invitee_id = i.invitee_id
 AND r.rn = 1;

-- STG: event types
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_event_types` AS
SELECT
  entity_id AS event_type_id,
  organization_uri,
  user_uri,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  JSON_VALUE(payload_json, '$.uri') AS event_type_uri,
  JSON_VALUE(payload_json, '$.name') AS event_type_name,
  JSON_VALUE(payload_json, '$.slug') AS event_type_slug,
  JSON_VALUE(payload_json, '$.kind') AS event_type_kind,
  SAFE_CAST(JSON_VALUE(payload_json, '$.duration') AS INT64) AS duration_minutes,
  SAFE_CAST(JSON_VALUE(payload_json, '$.active') AS BOOL) AS is_active,
  JSON_VALUE(payload_json, '$.pooling_type') AS pooling_type,
  JSON_VALUE(payload_json, '$.scheduling_url') AS scheduling_url,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type = 'event_types';

-- CORE: event type dimension
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` AS
SELECT
  event_type_id,
  event_type_uri,
  event_type_name,
  event_type_slug,
  event_type_kind,
  duration_minutes,
  is_active,
  pooling_type,
  scheduling_url,
  organization_uri,
  user_uri,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_event_types`;

-- STG: webhook lifecycle events (invitee created/canceled)
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_webhook_invitee_events` AS
SELECT
  webhook_event_id,
  webhook_event_type,
  scheduled_event_id,
  invitee_id,
  event_created_ts,
  delivery_ts,
  ingested_at,
  JSON_VALUE(payload_json, '$.created_at') AS payload_created_at_raw,
  payload_json,
  headers_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_webhook_events_raw`
WHERE webhook_event_type IN ('invitee.created', 'invitee.canceled');

-- CORE: invitee lifecycle fact (webhook truth)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_calendly_invitee_lifecycle` AS
SELECT
  w.webhook_event_id,
  w.webhook_event_type,
  w.scheduled_event_id,
  w.invitee_id,
  i.invitee_email,
  i.invitee_name,
  i.invitee_status,
  i.event_name,
  i.event_type_uri,
  i.scheduled_start_time,
  w.event_created_ts,
  w.delivery_ts,
  w.ingested_at,
  w.payload_json,
  w.headers_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_webhook_invitee_events` w
LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  ON i.invitee_id = w.invitee_id;

-- STG: routing forms
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_routing_forms` AS
SELECT
  entity_id AS routing_form_id,
  JSON_VALUE(payload_json, '$.uri') AS routing_form_uri,
  JSON_VALUE(payload_json, '$.name') AS routing_form_name,
  JSON_VALUE(payload_json, '$.status') AS routing_form_status,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type = 'routing_forms';

-- STG: routing form submissions
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_calendly_routing_form_submissions` AS
SELECT
  entity_id AS routing_form_submission_id,
  parent_id AS routing_form_id,
  JSON_VALUE(payload_json, '$.uri') AS routing_form_submission_uri,
  SAFE_CAST(JSON_VALUE(payload_json, '$.created_at') AS TIMESTAMP) AS submission_created_at,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type = 'routing_form_submissions';

-- CORE: routing form dim
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_calendly_routing_forms` AS
SELECT
  routing_form_id,
  routing_form_uri,
  routing_form_name,
  routing_form_status,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_routing_forms`;

-- CORE: routing form submissions fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_calendly_routing_form_submissions` AS
SELECT
  s.routing_form_submission_id,
  s.routing_form_id,
  f.routing_form_name,
  f.routing_form_status,
  s.routing_form_submission_uri,
  s.submission_created_at,
  s.event_ts,
  s.updated_at_ts,
  s.ingested_at,
  s.backfill_run_id,
  s.is_backfill,
  s.payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_calendly_routing_form_submissions` s
LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_routing_forms` f
  ON f.routing_form_id = s.routing_form_id;

-- CORE: coverage diagnostics for Calendly identity mapping
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_calendly_match_diagnostics` AS
WITH base AS (
  SELECT
    invitee_id,
    scheduled_event_id,
    invitee_email,
    contact_id,
    match_method,
    match_score,
    match_confidence,
    candidate_count,
    CASE
      WHEN invitee_email IS NULL THEN 'no_invitee_email'
      WHEN contact_id IS NULL THEN 'email_not_in_ghl'
      WHEN match_confidence = 'low' THEN 'low_confidence_match'
      WHEN candidate_count > 1 THEN 'multiple_contact_candidates'
      ELSE 'linked_high_confidence'
    END AS diagnostic_bucket
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts`
)
SELECT
  diagnostic_bucket,
  COUNT(*) AS invitee_count,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2) AS pct_invitees
FROM base
GROUP BY diagnostic_bucket
ORDER BY invitee_count DESC;
