-- STG: canonical Fathom calls
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_fathom_calls` AS
SELECT
  workspace_id,
  entity_id AS call_id,
  team_id,
  title,
  event_ts,
  ended_at_ts,
  updated_at_ts,
  ingested_at,
  source,
  external_participant_count,
  is_internal_only,
  is_revenue_relevant,
  classification_label,
  classification_confidence,
  classification_reason,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.meeting_title'), title) AS meeting_title,
  JSON_VALUE(payload_json, '$.meeting_type') AS meeting_type,
  JSON_VALUE(payload_json, '$.calendar_invitees_domains_type') AS invitees_domain_type,
  JSON_VALUE(payload_json, '$.recording_url') AS recording_url,
  JSON_VALUE(payload_json, '$.share_url') AS share_url,
  JSON_VALUE(payload_json, '$.recorded_by.email') AS recorded_by_email,
  JSON_VALUE(payload_json, '$.recorded_by.name') AS recorded_by_name,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
WHERE entity_type = 'calls';

-- STG: flattened meeting participants
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_fathom_participants` AS
SELECT
  c.workspace_id,
  c.call_id,
  c.team_id,
  c.event_ts,
  c.updated_at_ts,
  c.ingested_at,
  c.backfill_run_id,
  c.is_backfill,
  JSON_VALUE(p, '$.email') AS participant_email,
  JSON_VALUE(p, '$.name') AS participant_name,
  JSON_VALUE(p, '$.email_domain') AS participant_email_domain,
  SAFE_CAST(JSON_VALUE(p, '$.is_external') AS BOOL) AS is_external,
  JSON_VALUE(p, '$.response_status') AS response_status,
  p AS participant_json
FROM `project-41542e21-470f-4589-96d.STG.stg_fathom_calls` c,
UNNEST(IFNULL(JSON_QUERY_ARRAY(c.payload_json, '$.calendar_invitees'), [])) AS p;

-- CORE: one row per call
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` AS
SELECT
  workspace_id,
  call_id,
  team_id,
  meeting_title AS title,
  meeting_type,
  invitees_domain_type,
  event_ts,
  ended_at_ts,
  updated_at_ts,
  ingested_at,
  source,
  external_participant_count,
  is_internal_only,
  is_revenue_relevant,
  classification_label,
  classification_confidence,
  classification_reason,
  recorded_by_email,
  recorded_by_name,
  recording_url,
  share_url,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_fathom_calls`;

-- CORE: call -> GHL contact bridge (email match)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts` AS
WITH dedup_contacts AS (
  SELECT
    contact_id,
    location_id,
    email,
    full_name,
    phone,
    source,
    last_seen_ts,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(TRIM(email))
      ORDER BY last_seen_ts DESC, first_seen_ts DESC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`
  WHERE email IS NOT NULL
)
SELECT
  p.workspace_id,
  p.call_id,
  LOWER(TRIM(p.participant_email)) AS participant_email_normalized,
  p.participant_email,
  p.participant_name,
  p.is_external,
  c.location_id,
  c.contact_id,
  c.full_name AS ghl_contact_name,
  c.phone AS ghl_contact_phone,
  c.source AS ghl_contact_source,
  p.event_ts,
  p.updated_at_ts,
  p.ingested_at
FROM `project-41542e21-470f-4589-96d.STG.stg_fathom_participants` p
LEFT JOIN dedup_contacts c
  ON LOWER(TRIM(p.participant_email)) = LOWER(TRIM(c.email))
 AND c.rn = 1;

-- CORE: call -> opportunity bridge (latest opp for matched contact)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities` AS
WITH ranked AS (
  SELECT
    b.workspace_id,
    b.call_id,
    b.participant_email,
    b.contact_id,
    b.location_id,
    o.opportunity_id,
    o.pipeline_id,
    o.pipeline_name,
    o.pipeline_stage_id,
    o.stage_name,
    o.status AS opportunity_status,
    o.amount AS opportunity_amount,
    o.updated_at_ts,
    o.event_ts,
    ROW_NUMBER() OVER (
      PARTITION BY b.call_id, b.contact_id
      ORDER BY COALESCE(o.updated_at_ts, o.event_ts, TIMESTAMP '1970-01-01 00:00:00+00') DESC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts` b
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
    ON o.contact_id = b.contact_id
   AND o.location_id = b.location_id
  WHERE b.contact_id IS NOT NULL
)
SELECT
  workspace_id,
  call_id,
  participant_email,
  contact_id,
  location_id,
  opportunity_id,
  pipeline_id,
  pipeline_name,
  pipeline_stage_id,
  stage_name,
  opportunity_status,
  opportunity_amount,
  updated_at_ts,
  event_ts
FROM ranked
WHERE rn = 1;

-- CORE: candidate sales calls with Calendly event-type anchoring + pre-behavioral classification.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls` AS
WITH call_external_participants AS (
  SELECT
    c.call_id,
    COALESCE(c.ended_at_ts, c.event_ts) AS call_reference_ts,
    LOWER(TRIM(p.participant_email)) AS participant_email_norm
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` c
  JOIN `project-41542e21-470f-4589-96d.STG.stg_fathom_participants` p
    ON p.call_id = c.call_id
  WHERE COALESCE(p.is_external, FALSE) = TRUE
    AND p.participant_email IS NOT NULL
    AND COALESCE(c.ended_at_ts, c.event_ts) IS NOT NULL
),
calendly_invitees AS (
  SELECT
    LOWER(TRIM(i.invitee_email)) AS invitee_email_norm,
    i.event_type_uri,
    COALESCE(et.event_type_name, i.event_name) AS event_type_name,
    i.event_name,
    COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) AS invitee_reference_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = i.event_type_uri
  WHERE i.invitee_email IS NOT NULL
    AND COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
),
calendly_candidates AS (
  SELECT
    p.call_id,
    i.event_type_uri,
    i.event_type_name,
    i.event_name,
    ABS(TIMESTAMP_DIFF(p.call_reference_ts, i.invitee_reference_ts, HOUR)) AS calendly_match_hour_distance,
    ROW_NUMBER() OVER (
      PARTITION BY p.call_id
      ORDER BY ABS(TIMESTAMP_DIFF(p.call_reference_ts, i.invitee_reference_ts, HOUR)) ASC, i.invitee_reference_ts DESC
    ) AS rn
  FROM call_external_participants p
  JOIN calendly_invitees i
    ON i.invitee_email_norm = p.participant_email_norm
  WHERE ABS(TIMESTAMP_DIFF(p.call_reference_ts, i.invitee_reference_ts, HOUR)) <= 72
),
calendly_best AS (
  SELECT
    call_id,
    event_type_uri,
    event_type_name,
    event_name,
    calendly_match_hour_distance
  FROM calendly_candidates
  WHERE rn = 1
)
SELECT
  c.workspace_id,
  c.call_id,
  c.title,
  c.event_ts,
  c.ended_at_ts,
  c.updated_at_ts,
  c.external_participant_count,
  c.classification_label,
  c.classification_confidence,
  cb.event_type_uri AS calendly_event_type_uri,
  cb.event_type_name AS calendly_event_type_name,
  cb.event_name AS calendly_event_name,
  cb.calendly_match_hour_distance,
  CASE
    WHEN cb.call_id IS NOT NULL THEN 'sales'
    ELSE c.classification_label
  END AS classification_label_resolved_pre_behavior,
  CASE
    WHEN cb.call_id IS NOT NULL THEN GREATEST(COALESCE(c.classification_confidence, 0.0), 0.95)
    ELSE c.classification_confidence
  END AS classification_confidence_resolved_pre_behavior,
  CASE
    WHEN cb.call_id IS NOT NULL THEN 'calendly_event_type'
    WHEN COALESCE(c.is_revenue_relevant, FALSE) THEN 'fathom_fallback'
    ELSE 'unclassified'
  END AS classification_source_pre_behavior,
  CASE
    WHEN cb.call_id IS NOT NULL THEN TRUE
    WHEN COALESCE(c.is_revenue_relevant, FALSE) THEN TRUE
    ELSE FALSE
  END AS is_sales_meeting_pre_behavior,
  c.recorded_by_email,
  c.recorded_by_name,
  o.opportunity_id,
  o.pipeline_id,
  o.pipeline_name,
  o.pipeline_stage_id,
  o.stage_name,
  o.opportunity_status,
  o.opportunity_amount,
  c.share_url,
  c.recording_url,
  c.payload_json,
  c.ingested_at,
  c.backfill_run_id,
  c.is_backfill
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` c
LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities` o
  ON o.call_id = c.call_id
LEFT JOIN calendly_best cb
  ON cb.call_id = c.call_id;

-- CORE: call outcomes (basic extraction scaffold)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_call_outcomes` AS
SELECT
  c.workspace_id,
  c.call_id,
  c.title,
  c.event_ts,
  c.ended_at_ts,
  c.classification_label,
  ARRAY_LENGTH(IFNULL(JSON_QUERY_ARRAY(c.payload_json, '$.action_items'), [])) AS action_item_count,
  ARRAY_LENGTH(IFNULL(JSON_QUERY_ARRAY(c.payload_json, '$.next_steps'), [])) AS next_step_count,
  ARRAY_LENGTH(IFNULL(JSON_QUERY_ARRAY(c.payload_json, '$.questions'), [])) AS question_count,
  c.payload_json,
  c.ingested_at,
  c.backfill_run_id,
  c.is_backfill
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` c;

-- CORE: normalized GHL contact identity layer for deterministic/fallback matching
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_identity_ghl_contacts_normalized` AS
SELECT
  location_id,
  contact_id,
  email,
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
  SPLIT(LOWER(TRIM(email)), '@')[SAFE_OFFSET(1)] AS email_domain,
  phone,
  REGEXP_REPLACE(LOWER(TRIM(full_name)), r'[^a-z0-9 ]', '') AS name_norm,
  REGEXP_EXTRACT(LOWER(TRIM(full_name)), r'^([a-z0-9]+)') AS first_name_norm,
  full_name,
  source,
  first_seen_ts,
  last_seen_ts
FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`;

-- CORE: external Fathom participants in revenue-relevant calls with normalized identity keys
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.stg_fathom_external_participants_normalized` AS
SELECT
  c.call_id,
  c.workspace_id,
  c.event_ts AS call_event_ts,
  c.title AS call_title,
  p.participant_email,
  LOWER(TRIM(p.participant_email)) AS participant_email_norm,
  CASE
    WHEN p.participant_email IS NULL THEN NULL
    WHEN SPLIT(LOWER(TRIM(p.participant_email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
      REPLACE(
        SPLIT(SPLIT(LOWER(TRIM(p.participant_email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
        '.',
        ''
      ),
      '@gmail.com'
    )
    ELSE LOWER(TRIM(p.participant_email))
  END AS participant_email_canon,
  COALESCE(
    p.participant_email_domain,
    SPLIT(LOWER(TRIM(p.participant_email)), '@')[SAFE_OFFSET(1)]
  ) AS participant_domain,
  p.participant_name,
  REGEXP_REPLACE(LOWER(TRIM(p.participant_name)), r'[^a-z0-9 ]', '') AS participant_name_norm,
  REGEXP_EXTRACT(LOWER(TRIM(p.participant_name)), r'^([a-z0-9]+)') AS participant_first_name_norm,
  p.is_external,
  p.response_status
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls` c
LEFT JOIN `project-41542e21-470f-4589-96d.STG.stg_fathom_participants` p
  ON p.call_id = c.call_id
WHERE COALESCE(p.is_external, FALSE) = TRUE;

-- CORE: candidate contact matches across deterministic and fallback tiers
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_fathom_contact_match_candidates` AS
WITH participants AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Core.stg_fathom_external_participants_normalized`
),
contacts AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Core.dim_identity_ghl_contacts_normalized`
  WHERE email_norm IS NOT NULL
),
unioned AS (
  SELECT
    p.call_id,
    p.workspace_id,
    p.call_event_ts,
    p.call_title,
    p.participant_email,
    p.participant_email_norm,
    p.participant_email_canon,
    p.participant_domain,
    p.participant_name,
    p.participant_name_norm,
    p.participant_first_name_norm,
    c.location_id,
    c.contact_id,
    c.email AS matched_contact_email,
    c.full_name AS matched_contact_name,
    c.phone AS matched_contact_phone,
    c.last_seen_ts AS matched_contact_last_seen_ts,
    'email_exact' AS match_method,
    1.00 AS match_score
  FROM participants p
  JOIN contacts c
    ON p.participant_email_norm = c.email_norm

  UNION ALL

  SELECT
    p.call_id,
    p.workspace_id,
    p.call_event_ts,
    p.call_title,
    p.participant_email,
    p.participant_email_norm,
    p.participant_email_canon,
    p.participant_domain,
    p.participant_name,
    p.participant_name_norm,
    p.participant_first_name_norm,
    c.location_id,
    c.contact_id,
    c.email AS matched_contact_email,
    c.full_name AS matched_contact_name,
    c.phone AS matched_contact_phone,
    c.last_seen_ts AS matched_contact_last_seen_ts,
    'email_canonical' AS match_method,
    0.95 AS match_score
  FROM participants p
  JOIN contacts c
    ON p.participant_email_canon = c.email_canon
   AND p.participant_email_canon IS NOT NULL

  UNION ALL

  SELECT
    p.call_id,
    p.workspace_id,
    p.call_event_ts,
    p.call_title,
    p.participant_email,
    p.participant_email_norm,
    p.participant_email_canon,
    p.participant_domain,
    p.participant_name,
    p.participant_name_norm,
    p.participant_first_name_norm,
    c.location_id,
    c.contact_id,
    c.email AS matched_contact_email,
    c.full_name AS matched_contact_name,
    c.phone AS matched_contact_phone,
    c.last_seen_ts AS matched_contact_last_seen_ts,
    'name_domain_firstname' AS match_method,
    0.75 AS match_score
  FROM participants p
  JOIN contacts c
    ON p.participant_domain = c.email_domain
   AND p.participant_first_name_norm IS NOT NULL
   AND p.participant_first_name_norm = c.first_name_norm

  UNION ALL

  SELECT
    p.call_id,
    p.workspace_id,
    p.call_event_ts,
    p.call_title,
    p.participant_email,
    p.participant_email_norm,
    p.participant_email_canon,
    p.participant_domain,
    p.participant_name,
    p.participant_name_norm,
    p.participant_first_name_norm,
    c.location_id,
    c.contact_id,
    c.email AS matched_contact_email,
    c.full_name AS matched_contact_name,
    c.phone AS matched_contact_phone,
    c.last_seen_ts AS matched_contact_last_seen_ts,
    'name_domain_contains' AS match_method,
    0.60 AS match_score
  FROM participants p
  JOIN contacts c
    ON p.participant_domain = c.email_domain
   AND p.participant_first_name_norm IS NOT NULL
   AND c.name_norm LIKE CONCAT('%', p.participant_first_name_norm, '%')
),
dedup AS (
  SELECT
    call_id,
    workspace_id,
    call_event_ts,
    call_title,
    participant_email,
    participant_email_norm,
    participant_email_canon,
    participant_domain,
    participant_name,
    participant_name_norm,
    participant_first_name_norm,
    location_id,
    contact_id,
    ANY_VALUE(matched_contact_email) AS matched_contact_email,
    ANY_VALUE(matched_contact_name) AS matched_contact_name,
    ANY_VALUE(matched_contact_phone) AS matched_contact_phone,
    MAX(matched_contact_last_seen_ts) AS matched_contact_last_seen_ts,
    MAX(match_score) AS match_score,
    ARRAY_AGG(match_method ORDER BY match_score DESC LIMIT 1)[OFFSET(0)] AS match_method
  FROM unioned
  GROUP BY
    call_id,
    workspace_id,
    call_event_ts,
    call_title,
    participant_email,
    participant_email_norm,
    participant_email_canon,
    participant_domain,
    participant_name,
    participant_name_norm,
    participant_first_name_norm,
    location_id,
    contact_id
)
SELECT * FROM dedup;

-- CORE: best per-participant contact match with confidence and diagnostics metadata
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts_scored` AS
WITH participants AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Core.stg_fathom_external_participants_normalized`
),
candidate_counts AS (
  SELECT
    call_id,
    participant_email_norm,
    COUNT(DISTINCT contact_id) AS candidate_contact_count
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_contact_match_candidates`
  GROUP BY call_id, participant_email_norm
),
ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.call_id, c.participant_email_norm
      ORDER BY c.match_score DESC, c.matched_contact_last_seen_ts DESC, c.contact_id
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_contact_match_candidates` c
)
SELECT
  p.call_id,
  p.workspace_id,
  p.call_event_ts,
  p.call_title,
  p.participant_email,
  p.participant_email_norm,
  p.participant_email_canon,
  p.participant_domain,
  p.participant_name,
  p.participant_name_norm,
  p.participant_first_name_norm,
  p.is_external,
  COALESCE(cc.candidate_contact_count, 0) AS candidate_contact_count,
  r.location_id,
  r.contact_id,
  r.matched_contact_email,
  r.matched_contact_name,
  r.matched_contact_phone,
  r.match_method AS contact_match_method,
  r.match_score AS contact_match_score,
  CASE
    WHEN r.contact_id IS NULL THEN 'none'
    WHEN r.match_score >= 0.95 AND COALESCE(cc.candidate_contact_count, 0) = 1 THEN 'high'
    WHEN r.match_score >= 0.75 AND COALESCE(cc.candidate_contact_count, 0) <= 2 THEN 'medium'
    ELSE 'low'
  END AS contact_match_confidence,
  r.matched_contact_last_seen_ts
FROM participants p
LEFT JOIN candidate_counts cc
  ON cc.call_id = p.call_id
 AND cc.participant_email_norm = p.participant_email_norm
LEFT JOIN ranked r
  ON r.call_id = p.call_id
 AND r.participant_email_norm = p.participant_email_norm
 AND r.rn = 1;

-- CORE: temporal opportunity linkage from matched contacts
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities_scored` AS
WITH contact_matches AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts_scored`
  WHERE contact_id IS NOT NULL
    AND contact_match_confidence IN ('high', 'medium')
),
candidate_opps AS (
  SELECT
    cm.call_id,
    cm.workspace_id,
    cm.call_event_ts,
    cm.participant_email,
    cm.participant_email_norm,
    cm.contact_id,
    cm.location_id,
    cm.contact_match_method,
    cm.contact_match_score,
    cm.contact_match_confidence,
    o.opportunity_id,
    o.pipeline_id,
    o.pipeline_name,
    o.pipeline_stage_id,
    o.stage_name,
    o.status AS opportunity_status,
    o.amount AS opportunity_amount,
    COALESCE(o.updated_at_ts, o.event_ts) AS opportunity_reference_ts,
    ABS(TIMESTAMP_DIFF(cm.call_event_ts, COALESCE(o.updated_at_ts, o.event_ts), DAY)) AS day_distance,
    CASE
      WHEN LOWER(o.status) = 'won' THEN 0.25
      WHEN LOWER(o.status) = 'open' THEN 0.15
      WHEN LOWER(o.status) = 'lost' THEN 0.05
      ELSE 0.02
    END AS status_bonus
  FROM contact_matches cm
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
    ON o.contact_id = cm.contact_id
   AND o.location_id = cm.location_id
  WHERE o.opportunity_id IS NOT NULL
),
scored AS (
  SELECT
    *,
    GREATEST(0.0, 1.0 - SAFE_DIVIDE(day_distance, 60.0)) AS temporal_score,
    (
      contact_match_score * 0.50
      + GREATEST(0.0, 1.0 - SAFE_DIVIDE(day_distance, 60.0)) * 0.40
      + status_bonus
    ) AS opp_match_score
  FROM candidate_opps
),
ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.call_id, s.participant_email_norm
      ORDER BY s.opp_match_score DESC, s.day_distance ASC, s.opportunity_reference_ts DESC
    ) AS rn
  FROM scored s
)
SELECT
  call_id,
  workspace_id,
  call_event_ts,
  participant_email,
  participant_email_norm,
  contact_id,
  location_id,
  contact_match_method,
  contact_match_score,
  contact_match_confidence,
  opportunity_id,
  pipeline_id,
  pipeline_name,
  pipeline_stage_id,
  stage_name,
  opportunity_status,
  opportunity_amount,
  opportunity_reference_ts,
  day_distance,
  temporal_score,
  status_bonus,
  opp_match_score,
  CASE
    WHEN opp_match_score >= 0.80 THEN 'high'
    WHEN opp_match_score >= 0.65 THEN 'medium'
    ELSE 'low'
  END AS opp_match_confidence
FROM ranked
WHERE rn = 1;

-- CORE: classifier feature layer + deterministic anchors for 3-tier routing.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_features` AS
WITH ranked_call_opps AS (
  SELECT
    o.*,
    ROW_NUMBER() OVER (
      PARTITION BY o.call_id
      ORDER BY o.opp_match_score DESC, o.day_distance ASC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities_scored` o
),
opportunity_latest AS (
  SELECT
    o.location_id,
    o.opportunity_id,
    o.pipeline_id,
    o.pipeline_name,
    o.pipeline_stage_id,
    o.stage_name,
    o.status AS opportunity_status,
    o.last_stage_change_at,
    o.last_status_change_at,
    o.updated_at_ts,
    o.event_ts,
    o.ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY o.opportunity_id
      ORDER BY COALESCE(o.updated_at_ts, o.event_ts, o.ingested_at, o.last_stage_change_at, o.last_status_change_at) DESC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
),
joined AS (
  SELECT
    c.workspace_id,
    c.call_id,
    c.title,
    c.event_ts,
    c.ended_at_ts,
    COALESCE(c.ended_at_ts, c.event_ts) AS call_reference_ts,
    c.updated_at_ts,
    c.external_participant_count,
    c.classification_label,
    c.classification_confidence,
    c.calendly_event_type_uri,
    c.calendly_event_type_name,
    c.calendly_event_name,
    c.calendly_match_hour_distance,
    c.classification_label_resolved_pre_behavior,
    c.classification_confidence_resolved_pre_behavior,
    c.classification_source_pre_behavior,
    c.is_sales_meeting_pre_behavior,
    c.recorded_by_email,
    c.recorded_by_name,
    r.location_id AS matched_location_id,
    r.contact_id AS matched_contact_id,
    r.contact_match_method,
    r.contact_match_score,
    r.contact_match_confidence,
    r.opportunity_id,
    COALESCE(o.pipeline_id, r.pipeline_id) AS pipeline_id,
    COALESCE(o.pipeline_name, r.pipeline_name) AS pipeline_name,
    COALESCE(o.pipeline_stage_id, r.pipeline_stage_id) AS pipeline_stage_id,
    COALESCE(o.stage_name, r.stage_name) AS stage_name,
    COALESCE(o.opportunity_status, r.opportunity_status) AS opportunity_status,
    r.opportunity_amount,
    o.last_stage_change_at,
    o.last_status_change_at,
    r.opp_match_score,
    r.opp_match_confidence,
    r.day_distance AS opportunity_day_distance,
    c.share_url,
    c.recording_url,
    c.payload_json,
    c.ingested_at,
    c.backfill_run_id,
    c.is_backfill,
    fc.is_internal_only AS base_is_internal_only,
    fc.invitees_domain_type AS base_invitees_domain_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls` c
  LEFT JOIN ranked_call_opps r
    ON r.call_id = c.call_id
   AND r.rn = 1
  LEFT JOIN opportunity_latest o
    ON o.opportunity_id = r.opportunity_id
   AND o.rn = 1
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` fc
    ON fc.call_id = c.call_id
),
joined_with_velocity AS (
  SELECT
    j.*,
    CASE
      WHEN j.call_reference_ts IS NULL OR j.last_stage_change_at IS NULL THEN NULL
      ELSE TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR)
    END AS hours_to_stage_change_from_call,
    CASE
      WHEN j.call_reference_ts IS NULL OR j.last_status_change_at IS NULL THEN NULL
      ELSE TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR)
    END AS hours_to_status_change_from_call,
    CASE
      WHEN j.call_reference_ts IS NOT NULL AND j.last_stage_change_at > j.call_reference_ts THEN TRUE
      ELSE FALSE
    END AS moved_stage_after_call,
    CASE
      WHEN j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) <= 24
        THEN TRUE
      ELSE FALSE
    END AS moved_stage_within_24h,
    CASE
      WHEN j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) <= 48
        THEN TRUE
      ELSE FALSE
    END AS moved_stage_within_48h,
    CASE
      WHEN j.call_reference_ts IS NOT NULL AND j.last_status_change_at > j.call_reference_ts THEN TRUE
      ELSE FALSE
    END AS moved_status_after_call,
    CASE
      WHEN j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) <= 24
        THEN TRUE
      ELSE FALSE
    END AS moved_status_within_24h,
    CASE
      WHEN j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) <= 48
        THEN TRUE
      ELSE FALSE
    END AS moved_status_within_48h,
    CASE
      WHEN (
        j.call_reference_ts IS NOT NULL
        AND j.last_stage_change_at > j.call_reference_ts
      ) OR (
        j.call_reference_ts IS NOT NULL
        AND j.last_status_change_at > j.call_reference_ts
      ) THEN TRUE
      ELSE FALSE
    END AS pipeline_progressed_after_call,
    CASE
      WHEN (
        j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) <= 24
      ) OR (
        j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) <= 24
      ) THEN TRUE
      ELSE FALSE
    END AS pipeline_progressed_within_24h,
    CASE
      WHEN (
        j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_stage_change_at, j.call_reference_ts, HOUR) <= 48
      ) OR (
        j.call_reference_ts IS NOT NULL
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) > 0
        AND TIMESTAMP_DIFF(j.last_status_change_at, j.call_reference_ts, HOUR) <= 48
      ) THEN TRUE
      ELSE FALSE
    END AS pipeline_progressed_within_48h
  FROM joined j
),
call_outcomes AS (
  SELECT
    call_id,
    COALESCE(action_item_count, 0) AS action_item_count,
    COALESCE(next_step_count, 0) AS next_step_count,
    COALESCE(question_count, 0) AS question_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_call_outcomes`
),
participant_domain_rollup AS (
  SELECT
    p.call_id,
    COUNT(*) AS participant_external_count,
    COUNTIF(LOWER(TRIM(p.participant_domain)) IN ('gmail.com', 'googlemail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com')) AS participant_free_email_count,
    COUNTIF(LOWER(TRIM(p.participant_domain)) = 'fanbasis.com') AS participant_fanbasis_email_count,
    COUNTIF(
      p.participant_domain IS NOT NULL
      AND LOWER(TRIM(p.participant_domain)) NOT IN ('gmail.com', 'googlemail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'fanbasis.com')
    ) AS participant_corporate_email_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(
        DISTINCT LOWER(TRIM(p.participant_domain))
        IGNORE NULLS
        ORDER BY LOWER(TRIM(p.participant_domain))
        LIMIT 8
      ),
      '|'
    ) AS participant_domain_signature
  FROM `project-41542e21-470f-4589-96d.Core.stg_fathom_external_participants_normalized` p
  GROUP BY p.call_id
),
contact_opportunity_rollup AS (
  SELECT
    o.location_id,
    o.contact_id,
    COUNT(*) AS prior_opportunity_count,
    COUNTIF(LOWER(COALESCE(o.status, '')) = 'won') AS prior_won_opportunity_count,
    MAX(CASE WHEN LOWER(COALESCE(o.status, '')) = 'won' THEN 1 ELSE 0 END) = 1 AS has_prior_won_opportunity
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
  WHERE o.contact_id IS NOT NULL
  GROUP BY 1, 2
),
calendly_contact_candidates AS (
  SELECT
    j.call_id,
    b.invitee_id AS calendly_contact_invitee_id,
    i.event_type_uri AS calendly_contact_event_type_uri,
    COALESCE(et.event_type_name, i.event_name) AS calendly_contact_event_type_name,
    i.event_name AS calendly_contact_event_name,
    ABS(
      TIMESTAMP_DIFF(
        j.call_reference_ts,
        COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at),
        HOUR
      )
    ) AS calendly_contact_match_hour_distance,
    ROW_NUMBER() OVER (
      PARTITION BY j.call_id
      ORDER BY
        ABS(
          TIMESTAMP_DIFF(
            j.call_reference_ts,
            COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at),
            HOUR
          )
        ) ASC,
        COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) DESC
    ) AS rn
  FROM joined_with_velocity j
  JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.contact_id = j.matched_contact_id
   AND (j.matched_location_id IS NULL OR b.location_id = j.matched_location_id)
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = i.event_type_uri
  WHERE j.call_reference_ts IS NOT NULL
    AND COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
    AND ABS(
      TIMESTAMP_DIFF(
        j.call_reference_ts,
        COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at),
        DAY
      )
    ) <= 7
),
calendly_contact_best AS (
  SELECT
    call_id,
    calendly_contact_invitee_id,
    calendly_contact_event_type_uri,
    calendly_contact_event_type_name,
    calendly_contact_event_name,
    calendly_contact_match_hour_distance
  FROM calendly_contact_candidates
  WHERE rn = 1
)
SELECT
  j.workspace_id,
  j.call_id,
  j.title,
  j.event_ts,
  j.ended_at_ts,
  j.call_reference_ts,
  j.updated_at_ts,
  j.external_participant_count,
  j.classification_label,
  j.classification_confidence,
  j.calendly_event_type_uri,
  j.calendly_event_type_name,
  j.calendly_event_name,
  j.calendly_match_hour_distance,
  j.classification_label_resolved_pre_behavior,
  j.classification_confidence_resolved_pre_behavior,
  j.classification_source_pre_behavior,
  j.is_sales_meeting_pre_behavior,
  j.recorded_by_email,
  j.recorded_by_name,
  j.matched_location_id,
  j.matched_contact_id,
  j.contact_match_method,
  j.contact_match_score,
  j.contact_match_confidence,
  j.opportunity_id,
  j.pipeline_id,
  j.pipeline_name,
  j.pipeline_stage_id,
  j.stage_name,
  j.opportunity_status,
  j.opportunity_amount,
  j.last_stage_change_at,
  j.last_status_change_at,
  j.hours_to_stage_change_from_call,
  j.hours_to_status_change_from_call,
  j.moved_stage_after_call,
  j.moved_stage_within_24h,
  j.moved_stage_within_48h,
  j.moved_status_after_call,
  j.moved_status_within_24h,
  j.moved_status_within_48h,
  j.pipeline_progressed_after_call,
  j.pipeline_progressed_within_24h,
  j.pipeline_progressed_within_48h,
  COALESCE(j.base_is_internal_only, FALSE) AS is_internal_only,
  j.base_invitees_domain_type AS invitees_domain_type,
  COALESCE(o.action_item_count, 0) AS action_item_count,
  COALESCE(o.next_step_count, 0) AS next_step_count,
  COALESCE(o.question_count, 0) AS question_count,
  COALESCE(d.participant_external_count, 0) AS participant_external_count,
  COALESCE(d.participant_free_email_count, 0) AS participant_free_email_count,
  COALESCE(d.participant_corporate_email_count, 0) AS participant_corporate_email_count,
  COALESCE(d.participant_fanbasis_email_count, 0) AS participant_fanbasis_email_count,
  COALESCE(d.participant_domain_signature, '') AS participant_domain_signature,
  COALESCE(cor.prior_opportunity_count, 0) AS prior_opportunity_count,
  COALESCE(cor.prior_won_opportunity_count, 0) AS prior_won_opportunity_count,
  COALESCE(cor.has_prior_won_opportunity, FALSE) AS has_prior_won_opportunity,
  cb.calendly_contact_invitee_id,
  cb.calendly_contact_event_type_uri,
  cb.calendly_contact_event_type_name,
  cb.calendly_contact_event_name,
  cb.calendly_contact_match_hour_distance,
  CASE
    WHEN COALESCE(NULLIF(TRIM(j.calendly_event_type_name), ''), NULLIF(TRIM(j.calendly_event_name), '')) IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_calendly_event_anchor,
  CASE
    WHEN cb.calendly_contact_invitee_id IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_calendly_contact_anchor,
  CASE
    WHEN REGEXP_CONTAINS(
      LOWER(COALESCE(j.title, '')),
      r'(sales|discovery|demo|proposal|close|renewal|client|prospect|onboarding|growth call|strategy call|breakthrough call|consult)'
    ) THEN TRUE
    ELSE FALSE
  END AS title_has_sales_keyword,
  CASE
    WHEN REGEXP_CONTAINS(
      LOWER(COALESCE(j.title, '')),
      r'(mock call|role ?play|sales training|training|coaching|enablement|sdr tryouts?|tryouts?|group interview)'
    ) THEN TRUE
    ELSE FALSE
  END AS title_has_training_keyword,
  CASE
    WHEN REGEXP_CONTAINS(
      LOWER(COALESCE(j.title, '')),
      r'(team meeting|team sync|standup|all hands|internal|retro|sprint|1:1|one on one|huddle|check[- ]?in|debrief|review|ops meeting|content meeting|ads meeting|pitchdeck review|group call|sync call)'
    ) THEN TRUE
    ELSE FALSE
  END AS title_has_internal_keyword,
  CASE
    WHEN COALESCE(j.base_is_internal_only, FALSE) THEN TRUE
    WHEN LOWER(COALESCE(j.base_invitees_domain_type, '')) IN ('internal', 'mostly_internal') THEN TRUE
    WHEN COALESCE(d.participant_external_count, 0) = 0 THEN TRUE
    WHEN REGEXP_CONTAINS(
      LOWER(COALESCE(j.title, '')),
      r'(team meeting|team sync|standup|all hands|internal|retro|sprint|1:1|one on one|huddle|check[- ]?in|debrief|review|ops meeting|content meeting|ads meeting|pitchdeck review|group call|sync call|mock call|role ?play|sales training|training|coaching|enablement|sdr tryouts?|tryouts?|group interview)'
    ) THEN TRUE
    ELSE FALSE
  END AS internal_like_context,
  CASE
    WHEN j.opportunity_id IS NOT NULL AND COALESCE(j.pipeline_progressed_within_48h, FALSE) THEN TRUE
    ELSE FALSE
  END AS deterministic_sales_promotion,
  CASE
    WHEN COALESCE(NULLIF(TRIM(j.calendly_event_type_name), ''), NULLIF(TRIM(j.calendly_event_name), '')) IS NULL
      AND cb.calendly_contact_invitee_id IS NULL
      AND j.opportunity_id IS NULL
      AND NOT COALESCE(j.pipeline_progressed_after_call, FALSE)
      AND (
        (
          COALESCE(d.participant_external_count, 0) = 0
          AND REGEXP_CONTAINS(
            LOWER(COALESCE(j.title, '')),
            r'(meeting|check[- ]?in|review|interview|tryouts?|mock call|role ?play|training|coaching|enablement|team|internal|sync|impromptu|call review)'
          )
        )
        OR REGEXP_CONTAINS(
          LOWER(COALESCE(j.title, '')),
          r'(mock call|role ?play|sales training|training|coaching|enablement|sdr tryouts?|tryouts?|group interview)'
        )
        OR (
          REGEXP_CONTAINS(
            LOWER(COALESCE(j.title, '')),
            r'(team meeting|team sync|internal meeting|check[- ]?in|debrief|review|ops meeting|content meeting|ads meeting|pitchdeck review|group call|sync call)'
          )
          AND COALESCE(d.participant_external_count, 0) >= 2
        )
      ) THEN TRUE
    ELSE FALSE
  END AS deterministic_non_sales,
  CASE
    WHEN COALESCE(NULLIF(TRIM(j.calendly_event_type_name), ''), NULLIF(TRIM(j.calendly_event_name), '')) IS NOT NULL THEN 1
    WHEN cb.calendly_contact_invitee_id IS NOT NULL THEN 1
    WHEN j.opportunity_id IS NOT NULL AND COALESCE(j.pipeline_progressed_within_48h, FALSE) THEN 1
    WHEN COALESCE(NULLIF(TRIM(j.calendly_event_type_name), ''), NULLIF(TRIM(j.calendly_event_name), '')) IS NULL
      AND cb.calendly_contact_invitee_id IS NULL
      AND j.opportunity_id IS NULL
      AND NOT COALESCE(j.pipeline_progressed_after_call, FALSE)
      AND (
        (
          COALESCE(d.participant_external_count, 0) = 0
          AND REGEXP_CONTAINS(
            LOWER(COALESCE(j.title, '')),
            r'(meeting|check[- ]?in|review|interview|tryouts?|mock call|role ?play|training|coaching|enablement|team|internal|sync|impromptu|call review)'
          )
        )
        OR REGEXP_CONTAINS(
          LOWER(COALESCE(j.title, '')),
          r'(mock call|role ?play|sales training|training|coaching|enablement|sdr tryouts?|tryouts?|group interview)'
        )
        OR (
          REGEXP_CONTAINS(
            LOWER(COALESCE(j.title, '')),
            r'(team meeting|team sync|internal meeting|check[- ]?in|debrief|review|ops meeting|content meeting|ads meeting|pitchdeck review|group call|sync call)'
          )
          AND COALESCE(d.participant_external_count, 0) >= 2
        )
      ) THEN 0
    ELSE NULL
  END AS training_label,
  j.opp_match_score,
  j.opp_match_confidence,
  j.opportunity_day_distance,
  j.share_url,
  j.recording_url,
  j.payload_json,
  j.ingested_at,
  j.backfill_run_id,
  j.is_backfill
FROM joined_with_velocity j
LEFT JOIN call_outcomes o
  ON o.call_id = j.call_id
LEFT JOIN participant_domain_rollup d
  ON d.call_id = j.call_id
LEFT JOIN contact_opportunity_rollup cor
  ON cor.location_id = j.matched_location_id
 AND cor.contact_id = j.matched_contact_id
LEFT JOIN calendly_contact_best cb
  ON cb.call_id = j.call_id;

-- RAW: manual labels to feed active-learning retraining loop.
CREATE TABLE IF NOT EXISTS `project-41542e21-470f-4589-96d.Raw.fathom_sales_call_manual_labels` (
  call_id STRING NOT NULL,
  reviewed_label STRING NOT NULL,
  reviewed_by STRING,
  reviewed_at TIMESTAMP NOT NULL,
  review_notes STRING,
  is_high_impact BOOL,
  label_source STRING
)
PARTITION BY DATE(reviewed_at)
CLUSTER BY call_id;

-- CORE: high-confidence training rows (anchors/promotions/non-sales deterministic + reviewed labels).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_training` AS
WITH latest_manual_labels AS (
  SELECT
    call_id,
    LOWER(TRIM(reviewed_label)) AS reviewed_label
  FROM (
    SELECT
      call_id,
      reviewed_label,
      reviewed_at,
      ROW_NUMBER() OVER (
        PARTITION BY call_id
        ORDER BY reviewed_at DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Raw.fathom_sales_call_manual_labels`
    WHERE reviewed_label IS NOT NULL
  )
  WHERE rn = 1
),
labeled AS (
  SELECT
    f.call_id,
    CASE
      WHEN ml.reviewed_label = 'sales' THEN 1
      WHEN ml.reviewed_label IN ('non_sales', 'non-sales', 'nonsales') THEN 0
      ELSE f.training_label
    END AS label_sales,
    COALESCE(f.title_has_sales_keyword, FALSE) AS title_has_sales_keyword,
    COALESCE(f.title_has_internal_keyword, FALSE) AS title_has_internal_keyword,
    COALESCE(f.title_has_training_keyword, FALSE) AS title_has_training_keyword,
    COALESCE(f.participant_external_count, 0) AS participant_external_count,
    COALESCE(f.participant_free_email_count, 0) AS participant_free_email_count,
    COALESCE(f.participant_corporate_email_count, 0) AS participant_corporate_email_count,
    COALESCE(f.participant_fanbasis_email_count, 0) AS participant_fanbasis_email_count,
    COALESCE(f.action_item_count, 0) AS action_item_count,
    COALESCE(f.next_step_count, 0) AS next_step_count,
    COALESCE(f.question_count, 0) AS question_count,
    COALESCE(f.opportunity_id IS NOT NULL, FALSE) AS has_linked_opportunity,
    COALESCE(f.pipeline_progressed_within_48h, FALSE) AS pipeline_progressed_within_48h,
    COALESCE(f.moved_stage_within_48h, FALSE) AS moved_stage_within_48h,
    COALESCE(f.moved_status_within_48h, FALSE) AS moved_status_within_48h,
    COALESCE(f.matched_contact_id IS NOT NULL, FALSE) AS has_matched_contact,
    COALESCE(f.contact_match_score, 0.0) AS contact_match_score,
    COALESCE(f.has_prior_won_opportunity, FALSE) AS has_prior_won_opportunity,
    COALESCE(f.prior_won_opportunity_count, 0) AS prior_won_opportunity_count,
    COALESCE(f.prior_opportunity_count, 0) AS prior_opportunity_count,
    COALESCE(f.classification_confidence_resolved_pre_behavior, f.classification_confidence, 0.0) AS pre_behavior_confidence,
    COALESCE(f.is_sales_meeting_pre_behavior, FALSE) AS is_sales_meeting_pre_behavior,
    COALESCE(LOWER(TRIM(f.invitees_domain_type)), 'unknown') AS invitees_domain_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_features` f
  LEFT JOIN latest_manual_labels ml
    ON ml.call_id = f.call_id
)
SELECT
  *
FROM labeled
WHERE label_sales IS NOT NULL;

-- CORE: BQML binary classifier trained only on high-confidence rows.
CREATE OR REPLACE MODEL `project-41542e21-470f-4589-96d.Core.bqml_fathom_sales_call_classifier`
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['label_sales'],
  auto_class_weights = TRUE,
  data_split_method = 'NO_SPLIT'
) AS
WITH training AS (
  SELECT
    label_sales,
    title_has_sales_keyword,
    title_has_internal_keyword,
    title_has_training_keyword,
    participant_external_count,
    participant_free_email_count,
    participant_corporate_email_count,
    participant_fanbasis_email_count,
    action_item_count,
    next_step_count,
    question_count,
    has_linked_opportunity,
    pipeline_progressed_within_48h,
    moved_stage_within_48h,
    moved_status_within_48h,
    has_matched_contact,
    contact_match_score,
    has_prior_won_opportunity,
    prior_won_opportunity_count,
    prior_opportunity_count,
    pre_behavior_confidence,
    is_sales_meeting_pre_behavior,
    invitees_domain_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_training`
),
seed_rows AS (
  SELECT
    1 AS label_sales,
    TRUE AS title_has_sales_keyword,
    FALSE AS title_has_internal_keyword,
    FALSE AS title_has_training_keyword,
    1 AS participant_external_count,
    0 AS participant_free_email_count,
    1 AS participant_corporate_email_count,
    0 AS participant_fanbasis_email_count,
    1 AS action_item_count,
    1 AS next_step_count,
    2 AS question_count,
    TRUE AS has_linked_opportunity,
    TRUE AS pipeline_progressed_within_48h,
    TRUE AS moved_stage_within_48h,
    FALSE AS moved_status_within_48h,
    TRUE AS has_matched_contact,
    0.95 AS contact_match_score,
    TRUE AS has_prior_won_opportunity,
    1 AS prior_won_opportunity_count,
    2 AS prior_opportunity_count,
    0.95 AS pre_behavior_confidence,
    TRUE AS is_sales_meeting_pre_behavior,
    'external' AS invitees_domain_type
  UNION ALL
  SELECT
    0 AS label_sales,
    FALSE AS title_has_sales_keyword,
    TRUE AS title_has_internal_keyword,
    TRUE AS title_has_training_keyword,
    0 AS participant_external_count,
    0 AS participant_free_email_count,
    0 AS participant_corporate_email_count,
    1 AS participant_fanbasis_email_count,
    0 AS action_item_count,
    0 AS next_step_count,
    0 AS question_count,
    FALSE AS has_linked_opportunity,
    FALSE AS pipeline_progressed_within_48h,
    FALSE AS moved_stage_within_48h,
    FALSE AS moved_status_within_48h,
    FALSE AS has_matched_contact,
    0.0 AS contact_match_score,
    FALSE AS has_prior_won_opportunity,
    0 AS prior_won_opportunity_count,
    0 AS prior_opportunity_count,
    0.20 AS pre_behavior_confidence,
    FALSE AS is_sales_meeting_pre_behavior,
    'internal' AS invitees_domain_type
)
SELECT * FROM training
UNION ALL
SELECT * FROM seed_rows;

-- CORE: model scores for unresolved calls only.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_predictions` AS
WITH scoring_base AS (
  SELECT
    call_id,
    COALESCE(title_has_sales_keyword, FALSE) AS title_has_sales_keyword,
    COALESCE(title_has_internal_keyword, FALSE) AS title_has_internal_keyword,
    COALESCE(title_has_training_keyword, FALSE) AS title_has_training_keyword,
    COALESCE(participant_external_count, 0) AS participant_external_count,
    COALESCE(participant_free_email_count, 0) AS participant_free_email_count,
    COALESCE(participant_corporate_email_count, 0) AS participant_corporate_email_count,
    COALESCE(participant_fanbasis_email_count, 0) AS participant_fanbasis_email_count,
    COALESCE(action_item_count, 0) AS action_item_count,
    COALESCE(next_step_count, 0) AS next_step_count,
    COALESCE(question_count, 0) AS question_count,
    COALESCE(opportunity_id IS NOT NULL, FALSE) AS has_linked_opportunity,
    COALESCE(pipeline_progressed_within_48h, FALSE) AS pipeline_progressed_within_48h,
    COALESCE(moved_stage_within_48h, FALSE) AS moved_stage_within_48h,
    COALESCE(moved_status_within_48h, FALSE) AS moved_status_within_48h,
    COALESCE(matched_contact_id IS NOT NULL, FALSE) AS has_matched_contact,
    COALESCE(contact_match_score, 0.0) AS contact_match_score,
    COALESCE(has_prior_won_opportunity, FALSE) AS has_prior_won_opportunity,
    COALESCE(prior_won_opportunity_count, 0) AS prior_won_opportunity_count,
    COALESCE(prior_opportunity_count, 0) AS prior_opportunity_count,
    COALESCE(classification_confidence_resolved_pre_behavior, classification_confidence, 0.0) AS pre_behavior_confidence,
    COALESCE(is_sales_meeting_pre_behavior, FALSE) AS is_sales_meeting_pre_behavior,
    COALESCE(LOWER(TRIM(invitees_domain_type)), 'unknown') AS invitees_domain_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_features`
  WHERE training_label IS NULL
),
predictions AS (
  SELECT *
  FROM ML.PREDICT(
    MODEL `project-41542e21-470f-4589-96d.Core.bqml_fathom_sales_call_classifier`,
    (
      SELECT
        call_id,
        title_has_sales_keyword,
        title_has_internal_keyword,
        title_has_training_keyword,
        participant_external_count,
        participant_free_email_count,
        participant_corporate_email_count,
        participant_fanbasis_email_count,
        action_item_count,
        next_step_count,
        question_count,
        has_linked_opportunity,
        pipeline_progressed_within_48h,
        moved_stage_within_48h,
        moved_status_within_48h,
        has_matched_contact,
        contact_match_score,
        has_prior_won_opportunity,
        prior_won_opportunity_count,
        prior_opportunity_count,
        pre_behavior_confidence,
        is_sales_meeting_pre_behavior,
        invitees_domain_type
      FROM scoring_base
    )
  )
),
probabilities AS (
  SELECT
    p.call_id,
    SAFE_CAST(p.predicted_label_sales AS INT64) AS predicted_sales_label,
    COALESCE(
      MAX(IF(SAFE_CAST(prob.label AS INT64) = 1, prob.prob, NULL)),
      0.5
    ) AS p_sales,
    COALESCE(
      MAX(IF(SAFE_CAST(prob.label AS INT64) = 0, prob.prob, NULL)),
      0.5
    ) AS p_non_sales
  FROM predictions p
  LEFT JOIN UNNEST(p.predicted_label_sales_probs) prob
  GROUP BY p.call_id, predicted_sales_label
)
SELECT
  s.call_id,
  p.predicted_sales_label,
  p.p_sales,
  p.p_non_sales,
  ARRAY(
    SELECT reason
    FROM UNNEST([
      IF(s.title_has_sales_keyword, 'title_sales_keyword', NULL),
      IF(s.title_has_internal_keyword, 'title_internal_keyword', NULL),
      IF(s.title_has_training_keyword, 'title_training_keyword', NULL),
      IF(s.participant_fanbasis_email_count > 0, 'fanbasis_domain_participant', NULL),
      IF(s.participant_free_email_count > 0, 'free_email_participant', NULL),
      IF(s.action_item_count > 0, 'has_action_items', NULL),
      IF(s.next_step_count > 0, 'has_next_steps', NULL),
      IF(s.question_count > 2, 'many_questions', NULL),
      IF(s.pipeline_progressed_within_48h, 'pipeline_progressed_48h', NULL),
      IF(s.has_linked_opportunity, 'has_linked_opportunity', NULL),
      IF(s.has_prior_won_opportunity, 'prior_won_opportunity', NULL)
    ]) reason
    WHERE reason IS NOT NULL
  ) AS reason_features
FROM scoring_base s
LEFT JOIN probabilities p
  ON p.call_id = s.call_id;

-- CORE: unresolved/high-impact rows for weekly active-learning review.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_review_queue` AS
SELECT
  f.call_id,
  f.workspace_id,
  f.title,
  f.call_reference_ts,
  f.recorded_by_email,
  f.recorded_by_name,
  f.matched_contact_id,
  f.opportunity_id,
  f.pipeline_name,
  f.stage_name,
  f.opportunity_status,
  f.action_item_count,
  f.next_step_count,
  f.question_count,
  p.p_sales,
  p.reason_features AS model_reason_features,
  DATE_TRUNC(DATE(f.call_reference_ts), WEEK(MONDAY)) AS review_week,
  CASE
    WHEN f.opportunity_id IS NOT NULL THEN 'opportunity_linked'
    WHEN COALESCE(f.action_item_count, 0) + COALESCE(f.next_step_count, 0) > 0 THEN 'actionable_notes'
    WHEN COALESCE(f.external_participant_count, 0) >= 2 THEN 'multi_participant'
    ELSE 'standard'
  END AS impact_bucket,
  CASE
    WHEN p.p_sales IS NULL THEN 'no_model_score'
    WHEN p.p_sales >= 0.40 AND p.p_sales <= 0.60 THEN 'highest_uncertainty'
    ELSE 'medium_uncertainty'
  END AS uncertainty_bucket,
  CURRENT_TIMESTAMP() AS generated_at
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_features` f
LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_predictions` p
  ON p.call_id = f.call_id
WHERE f.training_label IS NULL
  AND (
    p.p_sales IS NULL
    OR (
      NOT (
        COALESCE(p.p_sales, 0.5) >= 0.95
        AND NOT COALESCE(f.title_has_internal_keyword, FALSE)
        AND NOT COALESCE(f.title_has_training_keyword, FALSE)
      )
      AND NOT (
        COALESCE(p.p_sales, 0.5) <= 0.30
        AND (
          COALESCE(f.title_has_internal_keyword, FALSE)
          OR COALESCE(f.title_has_training_keyword, FALSE)
          OR COALESCE(f.internal_like_context, FALSE)
        )
      )
    )
  );

-- CORE: enriched sales calls with strict classifier decision policy.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` AS
WITH latest_manual_labels AS (
  SELECT
    call_id,
    LOWER(TRIM(reviewed_label)) AS reviewed_label
  FROM (
    SELECT
      call_id,
      reviewed_label,
      reviewed_at,
      ROW_NUMBER() OVER (
        PARTITION BY call_id
        ORDER BY reviewed_at DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Raw.fathom_sales_call_manual_labels`
    WHERE reviewed_label IS NOT NULL
  )
  WHERE rn = 1
),
scored AS (
  SELECT
    f.*,
    p.predicted_sales_label,
    p.p_sales,
    p.p_non_sales,
    p.reason_features AS model_reason_features,
    ml.reviewed_label AS manual_reviewed_label,
    CASE
      WHEN ml.reviewed_label = 'sales' THEN 1
      WHEN ml.reviewed_label IN ('non_sales', 'non-sales', 'nonsales') THEN 0
      ELSE NULL
    END AS manual_label_binary
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_features` f
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_call_classifier_predictions` p
    ON p.call_id = f.call_id
  LEFT JOIN latest_manual_labels ml
    ON ml.call_id = f.call_id
)
SELECT
  s.workspace_id,
  s.call_id,
  s.title,
  s.event_ts,
  s.ended_at_ts,
  s.call_reference_ts,
  s.updated_at_ts,
  s.external_participant_count,
  s.classification_label,
  s.classification_confidence,
  s.calendly_event_type_uri,
  s.calendly_event_type_name,
  s.calendly_event_name,
  s.calendly_match_hour_distance,
  s.classification_label_resolved_pre_behavior,
  s.classification_confidence_resolved_pre_behavior,
  s.classification_source_pre_behavior,
  s.is_sales_meeting_pre_behavior,
  s.recorded_by_email,
  s.recorded_by_name,
  s.matched_location_id,
  s.matched_contact_id,
  s.contact_match_method,
  s.contact_match_score,
  s.contact_match_confidence,
  s.opportunity_id,
  s.pipeline_id,
  s.pipeline_name,
  s.pipeline_stage_id,
  s.stage_name,
  s.opportunity_status,
  s.opportunity_amount,
  s.last_stage_change_at,
  s.last_status_change_at,
  s.hours_to_stage_change_from_call,
  s.hours_to_status_change_from_call,
  s.moved_stage_after_call,
  s.moved_stage_within_24h,
  s.moved_stage_within_48h,
  s.moved_status_after_call,
  s.moved_status_within_24h,
  s.moved_status_within_48h,
  s.pipeline_progressed_after_call,
  s.pipeline_progressed_within_24h,
  s.pipeline_progressed_within_48h,
  CASE
    WHEN s.is_calendly_event_anchor THEN 'calendly_event_type'
    WHEN s.is_calendly_contact_anchor THEN 'calendly_contact_anchor'
    WHEN s.deterministic_sales_promotion THEN 'behavioral_promotion'
    WHEN s.deterministic_non_sales THEN 'non_sales_deterministic'
    WHEN s.manual_label_binary = 1 THEN 'human_review_label_sales'
    WHEN s.manual_label_binary = 0 THEN 'human_review_label_non_sales'
    WHEN COALESCE(s.p_sales, 0.5) >= 0.95
      AND NOT COALESCE(s.title_has_internal_keyword, FALSE)
      AND NOT COALESCE(s.title_has_training_keyword, FALSE)
      THEN 'sales_model_high'
    WHEN COALESCE(s.p_sales, 0.5) <= 0.30
      AND (
        COALESCE(s.title_has_internal_keyword, FALSE)
        OR COALESCE(s.title_has_training_keyword, FALSE)
        OR COALESCE(s.internal_like_context, FALSE)
      )
      THEN 'non_sales_model_high'
    ELSE 'review_queue'
  END AS classification_source,
  CASE
    WHEN s.is_calendly_event_anchor THEN 'sales'
    WHEN s.is_calendly_contact_anchor THEN 'sales'
    WHEN s.deterministic_sales_promotion THEN 'sales'
    WHEN s.deterministic_non_sales THEN 'non_sales'
    WHEN s.manual_label_binary = 1 THEN 'sales'
    WHEN s.manual_label_binary = 0 THEN 'non_sales'
    WHEN COALESCE(s.p_sales, 0.5) >= 0.95
      AND NOT COALESCE(s.title_has_internal_keyword, FALSE)
      AND NOT COALESCE(s.title_has_training_keyword, FALSE)
      THEN 'sales'
    WHEN COALESCE(s.p_sales, 0.5) <= 0.30
      AND (
        COALESCE(s.title_has_internal_keyword, FALSE)
        OR COALESCE(s.title_has_training_keyword, FALSE)
        OR COALESCE(s.internal_like_context, FALSE)
      )
      THEN 'non_sales'
    ELSE 'review'
  END AS resolved_classification_label,
  CASE
    WHEN s.is_calendly_event_anchor
      THEN GREATEST(COALESCE(s.classification_confidence_resolved_pre_behavior, s.classification_confidence, 0.0), 0.95)
    WHEN s.is_calendly_contact_anchor
      THEN GREATEST(COALESCE(s.classification_confidence_resolved_pre_behavior, s.classification_confidence, 0.0), 0.93)
    WHEN s.deterministic_sales_promotion
      THEN GREATEST(COALESCE(s.classification_confidence_resolved_pre_behavior, s.classification_confidence, 0.0), 0.90)
    WHEN s.deterministic_non_sales
      THEN GREATEST(COALESCE(s.classification_confidence_resolved_pre_behavior, s.classification_confidence, 0.0), 0.90)
    WHEN s.manual_label_binary IN (0, 1)
      THEN 1.00
    WHEN COALESCE(s.p_sales, 0.5) >= 0.95
      AND NOT COALESCE(s.title_has_internal_keyword, FALSE)
      AND NOT COALESCE(s.title_has_training_keyword, FALSE)
      THEN COALESCE(s.p_sales, 0.95)
    WHEN COALESCE(s.p_sales, 0.5) <= 0.30
      AND (
        COALESCE(s.title_has_internal_keyword, FALSE)
        OR COALESCE(s.title_has_training_keyword, FALSE)
        OR COALESCE(s.internal_like_context, FALSE)
      )
      THEN COALESCE(1.0 - s.p_sales, 0.70)
    ELSE COALESCE(ABS(COALESCE(s.p_sales, 0.5) - 0.5) * 2.0, COALESCE(s.classification_confidence_resolved_pre_behavior, s.classification_confidence, 0.0))
  END AS resolved_classification_confidence,
  CASE
    WHEN s.is_calendly_event_anchor THEN TRUE
    WHEN s.is_calendly_contact_anchor THEN TRUE
    WHEN s.deterministic_sales_promotion THEN TRUE
    WHEN s.manual_label_binary = 1 THEN TRUE
    WHEN s.deterministic_non_sales THEN FALSE
    WHEN s.manual_label_binary = 0 THEN FALSE
    WHEN COALESCE(s.p_sales, 0.5) >= 0.95
      AND NOT COALESCE(s.title_has_internal_keyword, FALSE)
      AND NOT COALESCE(s.title_has_training_keyword, FALSE)
      THEN TRUE
    ELSE FALSE
  END AS is_sales_meeting_resolved,
  s.predicted_sales_label,
  s.p_sales AS model_p_sales,
  s.p_non_sales AS model_p_non_sales,
  s.model_reason_features,
  CASE
    WHEN COALESCE(s.p_sales, 0.5) >= 0.95
      AND NOT COALESCE(s.title_has_internal_keyword, FALSE)
      AND NOT COALESCE(s.title_has_training_keyword, FALSE)
      THEN 'sales_model_high'
    WHEN COALESCE(s.p_sales, 0.5) <= 0.30
      AND (
        COALESCE(s.title_has_internal_keyword, FALSE)
        OR COALESCE(s.title_has_training_keyword, FALSE)
        OR COALESCE(s.internal_like_context, FALSE)
      )
      THEN 'non_sales_model_high'
    WHEN s.p_sales IS NULL THEN 'no_model_score'
    ELSE 'review_queue'
  END AS model_decision_band,
  s.action_item_count,
  s.next_step_count,
  s.question_count,
  s.is_internal_only,
  s.invitees_domain_type,
  s.internal_like_context,
  s.participant_external_count,
  s.participant_free_email_count,
  s.participant_corporate_email_count,
  s.participant_fanbasis_email_count,
  s.participant_domain_signature,
  s.prior_opportunity_count,
  s.prior_won_opportunity_count,
  s.has_prior_won_opportunity,
  s.is_calendly_event_anchor,
  s.is_calendly_contact_anchor,
  s.calendly_contact_invitee_id,
  s.calendly_contact_event_type_uri,
  s.calendly_contact_event_type_name,
  s.calendly_contact_event_name,
  s.calendly_contact_match_hour_distance,
  s.deterministic_sales_promotion,
  s.deterministic_non_sales,
  s.opp_match_score,
  s.opp_match_confidence,
  s.opportunity_day_distance,
  s.share_url,
  s.recording_url,
  s.payload_json,
  s.ingested_at,
  s.backfill_run_id,
  s.is_backfill
FROM scored s;

-- CORE: participant-level diagnostic buckets for linkage quality
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_match_diagnostics_participant` AS
SELECT
  c.call_id,
  c.workspace_id,
  c.call_event_ts,
  c.call_title,
  c.participant_email,
  c.participant_email_norm,
  c.participant_domain,
  c.participant_name,
  c.candidate_contact_count,
  c.contact_id,
  c.contact_match_method,
  c.contact_match_score,
  c.contact_match_confidence,
  o.opportunity_id,
  o.pipeline_id,
  o.pipeline_name,
  o.opportunity_status,
  o.opp_match_score,
  o.opp_match_confidence,
  CASE
    WHEN c.participant_email_norm IS NULL THEN 'no_participant_email'
    WHEN c.contact_id IS NULL AND c.candidate_contact_count > 1 THEN 'multiple_contact_candidates_no_winner'
    WHEN c.contact_id IS NULL THEN 'email_not_in_ghl_or_no_identity_match'
    WHEN c.contact_match_confidence = 'low' THEN 'low_confidence_contact_match'
    WHEN o.opportunity_id IS NULL THEN 'contact_found_no_opportunity'
    WHEN o.opp_match_confidence = 'low' THEN 'low_confidence_opportunity_match'
    ELSE 'linked_high_confidence'
  END AS diagnostic_bucket
FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts_scored` c
LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_opportunities_scored` o
  ON o.call_id = c.call_id
 AND o.participant_email_norm = c.participant_email_norm;

-- CORE: call-level diagnostic rollup for prioritization
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_match_diagnostics_call` AS
WITH agg AS (
  SELECT
    call_id,
    ANY_VALUE(workspace_id) AS workspace_id,
    MIN(call_event_ts) AS call_event_ts,
    ANY_VALUE(call_title) AS call_title,
    COUNT(*) AS external_participant_rows,
    COUNTIF(contact_id IS NOT NULL) AS matched_contact_rows,
    COUNTIF(opportunity_id IS NOT NULL) AS matched_opportunity_rows,
    COUNTIF(diagnostic_bucket = 'multiple_contact_candidates_no_winner') AS bucket_multi_candidates,
    COUNTIF(diagnostic_bucket = 'no_participant_email') AS bucket_no_email,
    COUNTIF(diagnostic_bucket = 'email_not_in_ghl_or_no_identity_match') AS bucket_no_identity,
    COUNTIF(diagnostic_bucket = 'low_confidence_contact_match') AS bucket_low_contact_conf,
    COUNTIF(diagnostic_bucket = 'contact_found_no_opportunity') AS bucket_no_opportunity,
    COUNTIF(diagnostic_bucket = 'low_confidence_opportunity_match') AS bucket_low_opp_conf,
    COUNTIF(diagnostic_bucket = 'linked_high_confidence') AS bucket_linked_high_conf
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_match_diagnostics_participant`
  GROUP BY call_id
)
SELECT
  call_id,
  workspace_id,
  call_event_ts,
  call_title,
  external_participant_rows,
  matched_contact_rows,
  matched_opportunity_rows,
  bucket_multi_candidates,
  bucket_no_email,
  bucket_no_identity,
  bucket_low_contact_conf,
  bucket_no_opportunity,
  bucket_low_opp_conf,
  bucket_linked_high_conf,
  ROUND(100 * SAFE_DIVIDE(matched_contact_rows, external_participant_rows), 2) AS pct_rows_matched_to_contact,
  ROUND(100 * SAFE_DIVIDE(matched_opportunity_rows, external_participant_rows), 2) AS pct_rows_matched_to_opportunity,
  CASE
    WHEN matched_opportunity_rows > 0 THEN 'linked_to_opportunity'
    WHEN matched_contact_rows > 0 THEN 'linked_to_contact_only'
    WHEN bucket_multi_candidates > 0 THEN 'multiple_contact_candidates_no_winner'
    WHEN bucket_no_email > 0 THEN 'no_participant_email'
    WHEN bucket_no_identity > 0 THEN 'email_not_in_ghl_or_no_identity_match'
    WHEN bucket_low_contact_conf > 0 THEN 'low_confidence_contact_match'
    WHEN bucket_no_opportunity > 0 THEN 'contact_found_no_opportunity'
    WHEN bucket_low_opp_conf > 0 THEN 'low_confidence_opportunity_match'
    WHEN external_participant_rows = 0 THEN 'no_external_participants'
    ELSE 'unknown'
  END AS primary_diagnostic_bucket
FROM agg;

-- CORE: high-level diagnostic summary
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_fathom_match_diagnostics_summary` AS
SELECT
  primary_diagnostic_bucket,
  COUNT(*) AS call_count,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2) AS pct_calls
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_match_diagnostics_call`
GROUP BY primary_diagnostic_bucket
ORDER BY call_count DESC;
