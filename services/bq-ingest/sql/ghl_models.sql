-- RAW: custom field definitions (latest snapshot)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Raw.ghl_custom_field_definitions` AS
SELECT
  JSON_VALUE(payload_json, '$.id') AS field_id,
  JSON_VALUE(payload_json, '$.locationId') AS location_id,
  JSON_VALUE(payload_json, '$.name') AS field_name,
  JSON_VALUE(payload_json, '$.fieldKey') AS field_key,
  JSON_VALUE(payload_json, '$.dataType') AS data_type,
  JSON_VALUE(payload_json, '$.objectType') AS object_type,
  SAFE_CAST(JSON_VALUE(payload_json, '$.position') AS INT64) AS position,
  (
    SELECT ARRAY_AGG(JSON_VALUE(opt))
    FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(payload_json, '$.picklistOptions'), CAST([] AS ARRAY<JSON>))) AS opt
  ) AS picklist_options,
  ingested_at AS refreshed_at
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'custom_field_definitions'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY JSON_VALUE(payload_json, '$.id')
  ORDER BY ingested_at DESC
) = 1;

-- STG: contact custom fields
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_contact_custom_fields` AS
SELECT
  entity_id AS contact_id,
  JSON_VALUE(payload_json, '$.locationId') AS location_id,
  JSON_VALUE(cf, '$.id') AS field_id,
  JSON_VALUE(cf, '$.value') AS field_value_raw,
  (
    SELECT ARRAY_AGG(JSON_VALUE(v))
    FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(cf, '$.value'), CAST([] AS ARRAY<JSON>))) AS v
  ) AS field_value_array,
  partition_date,
  ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`,
  UNNEST(IFNULL(JSON_QUERY_ARRAY(payload_json, '$.customFields'), CAST([] AS ARRAY<JSON>))) AS cf
WHERE entity_type = 'contacts'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY entity_id, JSON_VALUE(cf, '$.id')
  ORDER BY ingested_at DESC
) = 1;

-- CORE: contact custom fields fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_contact_custom_fields` AS
SELECT
  s.contact_id,
  s.location_id,
  s.field_id,
  d.field_name,
  d.field_key,
  d.data_type,
  d.object_type,
  s.field_value_raw,
  CASE
    WHEN UPPER(d.data_type) IN ('NUMERICAL', 'MONETARY')
      THEN SAFE_CAST(s.field_value_raw AS FLOAT64)
  END AS field_value_number,
  CASE
    WHEN UPPER(d.data_type) = 'DATE'
      THEN TIMESTAMP_MILLIS(SAFE_CAST(s.field_value_raw AS INT64))
  END AS field_value_date,
  CASE
    WHEN UPPER(d.data_type) IN ('CHECKBOX', 'FILE_UPLOAD')
      THEN s.field_value_array
  END AS field_value_array,
  CASE
    WHEN UPPER(d.data_type) NOT IN ('NUMERICAL', 'MONETARY', 'DATE', 'CHECKBOX', 'FILE_UPLOAD')
      THEN s.field_value_raw
  END AS field_value_text,
  s.ingested_at
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_contact_custom_fields` s
LEFT JOIN `project-41542e21-470f-4589-96d.Raw.ghl_custom_field_definitions` d
  ON s.field_id = d.field_id
 AND s.location_id = d.location_id
WHERE LOWER(d.object_type) = 'contact' OR d.object_type IS NULL;

-- STG: contacts
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_contacts` AS
SELECT
  location_id,
  entity_id AS contact_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.email'), JSON_VALUE(payload_json, '$.contact.email')) AS email,
  COALESCE(JSON_VALUE(payload_json, '$.firstName'), JSON_VALUE(payload_json, '$.first_name')) AS first_name,
  COALESCE(JSON_VALUE(payload_json, '$.lastName'), JSON_VALUE(payload_json, '$.last_name')) AS last_name,
  COALESCE(JSON_VALUE(payload_json, '$.name'), CONCAT(
    COALESCE(JSON_VALUE(payload_json, '$.firstName'), JSON_VALUE(payload_json, '$.first_name'), ''),
    ' ',
    COALESCE(JSON_VALUE(payload_json, '$.lastName'), JSON_VALUE(payload_json, '$.last_name'), '')
  )) AS full_name,
  COALESCE(JSON_VALUE(payload_json, '$.phone'), JSON_VALUE(payload_json, '$.contact.phone')) AS phone,
  COALESCE(JSON_VALUE(payload_json, '$.country'), JSON_VALUE(payload_json, '$.address.country')) AS country,
  COALESCE(JSON_VALUE(payload_json, '$.source'), JSON_VALUE(payload_json, '$.attribution.source')) AS source,
  COALESCE(JSON_VALUE(payload_json, '$.utmSource'), JSON_VALUE(payload_json, '$.utm_source')) AS utm_source,
  COALESCE(JSON_VALUE(payload_json, '$.utmMedium'), JSON_VALUE(payload_json, '$.utm_medium')) AS utm_medium,
  COALESCE(JSON_VALUE(payload_json, '$.utmCampaign'), JSON_VALUE(payload_json, '$.utm_campaign')) AS utm_campaign,
  COALESCE(JSON_VALUE(payload_json, '$.utmContent'), JSON_VALUE(payload_json, '$.utm_content')) AS utm_content,
  SAFE_CAST(JSON_VALUE(payload_json, '$.dateAdded') AS TIMESTAMP) AS date_added,
  SAFE_CAST(JSON_VALUE(payload_json, '$.dateUpdated') AS TIMESTAMP) AS date_updated,
  NULLIF(
    TRIM(
      COALESCE(
        JSON_VALUE(payload_json, '$.assignedTo'),
        JSON_VALUE(payload_json, '$.assignedTo.id')
      )
    ),
    ''
  ) AS assigned_to_user_id,
  SAFE_CAST((SELECT JSON_VALUE(cf, '$.value') FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.customFields')) AS cf WHERE JSON_VALUE(cf, '$.id') = 'aVSozwJcNkLXCJYB0D0f' LIMIT 1) AS INT64) AS number_of_dials,
  (SELECT JSON_VALUE(cf, '$.value') FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.customFields')) AS cf WHERE JSON_VALUE(cf, '$.id') = 'yf4y3j2DK5uGAIEANs9D' LIMIT 1) AS last_touch_point,
  ARRAY_TO_STRING(
    ARRAY(SELECT JSON_VALUE(t) FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.tags')) AS t ORDER BY 1),
    ', '
  ) AS tags_csv,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'contacts';

-- STG: opportunities
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_opportunities` AS
SELECT
  location_id,
  entity_id AS opportunity_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.contactId'), JSON_VALUE(payload_json, '$.contact.id')) AS contact_id,
  COALESCE(JSON_VALUE(payload_json, '$.pipelineId'), JSON_VALUE(payload_json, '$.pipeline.id')) AS pipeline_id,
  COALESCE(JSON_VALUE(payload_json, '$.pipelineStageId'), JSON_VALUE(payload_json, '$.pipelineStage.id')) AS pipeline_stage_id,
  COALESCE(JSON_VALUE(payload_json, '$.status'), JSON_VALUE(payload_json, '$.opportunityStatus')) AS status,
  COALESCE(JSON_VALUE(payload_json, '$.name'), JSON_VALUE(payload_json, '$.title')) AS opportunity_name,
  SAFE_CAST(
    COALESCE(
      JSON_VALUE(payload_json, '$.monetaryValue'),
      JSON_VALUE(payload_json, '$.amount'),
      JSON_VALUE(payload_json, '$.value')
    ) AS NUMERIC
  ) AS amount,
  JSON_VALUE(payload_json, '$.source') AS source,
  NULLIF(
    TRIM(
      COALESCE(
        JSON_VALUE(payload_json, '$.assignedTo'),
        JSON_VALUE(payload_json, '$.assignedTo.id')
      )
    ),
    ''
  ) AS assigned_to_user_id,
  SAFE_CAST(JSON_VALUE(payload_json, '$.effectiveProbability') AS NUMERIC) AS effective_probability,
  SAFE_CAST(JSON_VALUE(payload_json, '$.lastStageChangeAt') AS TIMESTAMP) AS last_stage_change_at,
  SAFE_CAST(JSON_VALUE(payload_json, '$.lastStatusChangeAt') AS TIMESTAMP) AS last_status_change_at,
  JSON_VALUE(payload_json, '$.attributions[0].medium') AS first_touch_medium,
  JSON_VALUE(payload_json, '$.attributions[0].utmSessionSource') AS first_touch_session_source,
  JSON_VALUE(payload_json, '$.attributions[0].url') AS first_touch_url,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'opportunities';

-- STG: forms
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_forms` AS
SELECT
  location_id,
  entity_id AS form_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.name'), JSON_VALUE(payload_json, '$.title')) AS form_name,
  JSON_VALUE(payload_json, '$.slug') AS slug,
  JSON_VALUE(payload_json, '$.type') AS form_type,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'forms';

-- STG: form submissions
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_form_submissions` AS
SELECT
  location_id,
  entity_id AS submission_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.formId'), JSON_VALUE(payload_json, '$.form.id')) AS form_id,
  COALESCE(JSON_VALUE(payload_json, '$.contactId'), JSON_VALUE(payload_json, '$.contact.id')) AS contact_id,
  COALESCE(JSON_VALUE(payload_json, '$.opportunityId'), JSON_VALUE(payload_json, '$.opportunity.id')) AS opportunity_id,
  COALESCE(JSON_VALUE(payload_json, '$.source'), JSON_VALUE(payload_json, '$.attribution.source')) AS source,
  COALESCE(JSON_VALUE(payload_json, '$.utmSource'), JSON_VALUE(payload_json, '$.utm_source')) AS utm_source,
  COALESCE(JSON_VALUE(payload_json, '$.utmMedium'), JSON_VALUE(payload_json, '$.utm_medium')) AS utm_medium,
  COALESCE(JSON_VALUE(payload_json, '$.utmCampaign'), JSON_VALUE(payload_json, '$.utm_campaign')) AS utm_campaign,
  COALESCE(JSON_VALUE(payload_json, '$.utmContent'), JSON_VALUE(payload_json, '$.utm_content')) AS utm_content,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'form_submissions';

-- STG: pipelines
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_pipelines` AS
SELECT
  location_id,
  entity_id AS pipeline_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.name'), JSON_VALUE(payload_json, '$.title')) AS pipeline_name,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'pipelines';

-- STG: pipeline stages
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_pipeline_stages` AS
SELECT
  location_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.pipelineId'),
    SPLIT(entity_id, ':')[SAFE_OFFSET(0)]
  ) AS pipeline_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.id'),
    JSON_VALUE(payload_json, '$._id'),
    JSON_VALUE(payload_json, '$.stageId'),
    SPLIT(entity_id, ':')[SAFE_OFFSET(1)]
  ) AS pipeline_stage_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(JSON_VALUE(payload_json, '$.name'), JSON_VALUE(payload_json, '$.title')) AS stage_name,
  SAFE_CAST(COALESCE(JSON_VALUE(payload_json, '$.position'), JSON_VALUE(payload_json, '$.order')) AS INT64) AS stage_order,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'pipeline_stages';

-- CORE: contacts dimension
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts` AS
SELECT
  location_id,
  contact_id,
  ANY_VALUE(email) AS email,
  ANY_VALUE(first_name) AS first_name,
  ANY_VALUE(last_name) AS last_name,
  ANY_VALUE(full_name) AS full_name,
  ANY_VALUE(phone) AS phone,
  ANY_VALUE(country) AS country,
  ANY_VALUE(source) AS source,
  ANY_VALUE(utm_source) AS utm_source,
  ANY_VALUE(utm_medium) AS utm_medium,
  ANY_VALUE(utm_campaign) AS utm_campaign,
  ANY_VALUE(utm_content) AS utm_content,
  ANY_VALUE(date_added) AS date_added,
  ANY_VALUE(date_updated) AS date_updated,
  ANY_VALUE(assigned_to_user_id) AS assigned_to_user_id,
  ANY_VALUE(number_of_dials) AS number_of_dials,
  ANY_VALUE(last_touch_point) AS last_touch_point,
  ANY_VALUE(tags_csv) AS tags_csv,
  MIN(COALESCE(event_ts, updated_at_ts, ingested_at)) AS first_seen_ts,
  MAX(COALESCE(updated_at_ts, event_ts, ingested_at)) AS last_seen_ts
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_contacts`
GROUP BY location_id, contact_id;

-- CORE: contacts first-touch attribution snapshot (earliest known contact state)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts_first_touch` AS
WITH ranked AS (
  SELECT
    location_id,
    contact_id,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    COALESCE(JSON_VALUE(payload_json, '$.utmTerm'), JSON_VALUE(payload_json, '$.utm_term')) AS utm_term,
    source,
    assigned_to_user_id,
    COALESCE(date_added, event_ts, updated_at_ts, ingested_at) AS first_touch_sort_ts,
    ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY location_id, contact_id
      ORDER BY
        COALESCE(date_added, event_ts, updated_at_ts, ingested_at) ASC,
        ingested_at ASC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_contacts`
)
SELECT
  location_id,
  contact_id,
  utm_source AS utm_source_first,
  utm_medium AS utm_medium_first,
  utm_campaign AS utm_campaign_first,
  utm_content AS utm_content_first,
  utm_term AS utm_term_first,
  source AS source_first,
  assigned_to_user_id AS assigned_to_user_id_first,
  first_touch_sort_ts AS first_contact_ts
FROM ranked
WHERE rn = 1;

-- CORE: forms dimension
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_ghl_forms` AS
SELECT
  location_id,
  form_id,
  ANY_VALUE(form_name) AS form_name,
  ANY_VALUE(slug) AS slug,
  ANY_VALUE(form_type) AS form_type,
  MIN(COALESCE(event_ts, updated_at_ts, ingested_at)) AS first_seen_ts,
  MAX(COALESCE(updated_at_ts, event_ts, ingested_at)) AS last_seen_ts
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_forms`
GROUP BY location_id, form_id;

-- CORE: opportunities fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` AS
WITH pipelines AS (
  SELECT location_id, pipeline_id, ANY_VALUE(pipeline_name) AS pipeline_name
  FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_pipelines`
  GROUP BY location_id, pipeline_id
),
stages AS (
  SELECT location_id, pipeline_id, pipeline_stage_id, ANY_VALUE(stage_name) AS stage_name
  FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_pipeline_stages`
  GROUP BY location_id, pipeline_id, pipeline_stage_id
)
SELECT
  o.location_id,
  o.opportunity_id,
  o.contact_id,
  o.pipeline_id,
  p.pipeline_name,
  o.pipeline_stage_id,
  s.stage_name,
  o.status,
  o.opportunity_name,
  o.amount,
  o.source,
  o.assigned_to_user_id,
  o.effective_probability,
  o.last_stage_change_at,
  o.last_status_change_at,
  o.first_touch_medium,
  o.first_touch_session_source,
  o.first_touch_url,
  o.event_ts,
  o.updated_at_ts,
  o.ingested_at,
  o.backfill_run_id,
  o.is_backfill,
  o.payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_opportunities` o
LEFT JOIN pipelines p
  ON p.location_id = o.location_id
 AND p.pipeline_id = o.pipeline_id
LEFT JOIN stages s
  ON s.location_id = o.location_id
 AND s.pipeline_id = o.pipeline_id
 AND s.pipeline_stage_id = o.pipeline_stage_id;

-- CORE: append-only daily pipeline stage snapshots (created once, populated by scheduler)
CREATE TABLE IF NOT EXISTS `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`
(
  snapshot_date DATE NOT NULL,
  location_id STRING,
  opportunity_id STRING NOT NULL,
  contact_id STRING,
  pipeline_id STRING,
  pipeline_name STRING,
  pipeline_stage_id STRING,
  stage_name STRING,
  status STRING,
  opportunity_name STRING,
  amount FLOAT64,
  assigned_to_user_id STRING,
  effective_probability FLOAT64,
  last_stage_change_at TIMESTAMP,
  days_in_current_stage INT64,
  snapshotted_at TIMESTAMP NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY pipeline_id, status
OPTIONS (
  require_partition_filter = FALSE,
  partition_expiration_days = NULL
);

-- CORE: form submissions fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_form_submissions` AS
SELECT
  s.location_id,
  s.submission_id,
  s.form_id,
  f.form_name,
  s.contact_id,
  s.opportunity_id,
  s.source,
  s.utm_source,
  s.utm_medium,
  s.utm_campaign,
  s.utm_content,
  s.event_ts,
  s.updated_at_ts,
  s.ingested_at,
  s.backfill_run_id,
  s.is_backfill,
  s.payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_form_submissions` s
LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_forms` f
  ON f.location_id = s.location_id
 AND f.form_id = s.form_id;

-- STG: tasks (optional entity; present when GHL task objects are ingested)
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_tasks` AS
SELECT
  location_id,
  entity_id AS task_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(
    JSON_VALUE(payload_json, '$.contactId'),
    JSON_VALUE(payload_json, '$.contact.id'),
    JSON_VALUE(payload_json, '$.assignedTo.contactId')
  ) AS contact_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.assignedTo'),
    JSON_VALUE(payload_json, '$.assignedTo.id'),
    JSON_VALUE(payload_json, '$.userId'),
    JSON_VALUE(payload_json, '$.ownerId')
  ) AS owner_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.title'),
    JSON_VALUE(payload_json, '$.name')
  ) AS task_title,
  COALESCE(
    JSON_VALUE(payload_json, '$.status'),
    JSON_VALUE(payload_json, '$.taskStatus')
  ) AS task_status,
  SAFE_CAST(COALESCE(JSON_VALUE(payload_json, '$.dueDate'), JSON_VALUE(payload_json, '$.due_date')) AS TIMESTAMP) AS due_at,
  SAFE_CAST(COALESCE(JSON_VALUE(payload_json, '$.completedAt'), JSON_VALUE(payload_json, '$.completed_at')) AS TIMESTAMP) AS completed_at,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'tasks';

-- STG: notes (optional entity; present when GHL notes are ingested)
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_notes` AS
SELECT
  location_id,
  entity_id AS note_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(
    JSON_VALUE(payload_json, '$.contactId'),
    JSON_VALUE(payload_json, '$.contact.id'),
    JSON_VALUE(payload_json, '$.association.contactId')
  ) AS contact_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.userId'),
    JSON_VALUE(payload_json, '$.createdBy'),
    JSON_VALUE(payload_json, '$.ownerId')
  ) AS author_user_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.body'),
    JSON_VALUE(payload_json, '$.note'),
    JSON_VALUE(payload_json, '$.text')
  ) AS note_text,
  SAFE_CAST(COALESCE(JSON_VALUE(payload_json, '$.createdAt'), JSON_VALUE(payload_json, '$.created_at')) AS TIMESTAMP) AS note_created_at,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'notes';

-- STG: conversations/messages (optional entity; present when GHL conversation exports are ingested)
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_conversations` AS
SELECT
  location_id,
  entity_id AS conversation_row_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(
    JSON_VALUE(payload_json, '$.conversationId'),
    JSON_VALUE(payload_json, '$.conversation.id'),
    JSON_VALUE(payload_json, '$.id')
  ) AS conversation_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.messageId'),
    JSON_VALUE(payload_json, '$.message.id'),
    entity_id
  ) AS message_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.contactId'),
    JSON_VALUE(payload_json, '$.contact.id'),
    JSON_VALUE(payload_json, '$.conversation.contactId'),
    JSON_VALUE(payload_json, '$.message.contactId')
  ) AS contact_id,
  -- Assigned rep for this conversation thread (GHL user ID → dim_team_members.ghl_user_id)
  NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo')), '') AS assigned_to_user_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.direction'),
    JSON_VALUE(payload_json, '$.messageDirection'),
    JSON_VALUE(payload_json, '$.message.direction'),
    JSON_VALUE(payload_json, '$.lastMessageDirection')
  ) AS direction,
  COALESCE(
    JSON_VALUE(payload_json, '$.messageType'),
    JSON_VALUE(payload_json, '$.lastMessageType'),
    JSON_VALUE(payload_json, '$.type'),
    JSON_VALUE(payload_json, '$.channel')
  ) AS message_type,
  COALESCE(
    JSON_VALUE(payload_json, '$.status'),
    JSON_VALUE(payload_json, '$.messageStatus')
  ) AS message_status,
  COALESCE(
    SAFE_CAST(JSON_VALUE(payload_json, '$.createdAt') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(payload_json, '$.created_at') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(payload_json, '$.timestamp') AS TIMESTAMP),
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(payload_json, '$.lastMessageDate') AS INT64)),
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(payload_json, '$.dateAdded') AS INT64)),
    TIMESTAMP_MILLIS(SAFE_CAST(JSON_VALUE(payload_json, '$.dateUpdated') AS INT64)),
    event_ts,
    updated_at_ts
  ) AS message_created_at,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type IN ('conversations', 'conversation_messages');

-- CORE: tasks fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_tasks` AS
SELECT
  location_id,
  task_id,
  contact_id,
  owner_id,
  task_title,
  LOWER(TRIM(task_status)) AS task_status,
  due_at,
  completed_at,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_tasks`;

-- CORE: notes fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_notes` AS
SELECT
  location_id,
  note_id,
  contact_id,
  author_user_id,
  note_text,
  note_created_at,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_notes`;

-- CORE: conversations/messages fact
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` AS
SELECT
  location_id,
  conversation_id,
  message_id,
  contact_id,
  assigned_to_user_id,
  LOWER(TRIM(direction)) AS direction_norm,
  LOWER(TRIM(message_type)) AS message_type_norm,
  LOWER(TRIM(message_status)) AS message_status_norm,
  message_created_at,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_conversations`;

-- CORE: attribution bridge
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_attribution` AS
WITH ranked_opps AS (
  SELECT
    s.location_id,
    s.submission_id,
    o.opportunity_id,
    o.pipeline_id,
    o.pipeline_name,
    o.pipeline_stage_id,
    o.stage_name,
    o.status AS opportunity_status,
    o.amount AS opportunity_amount,
    ROW_NUMBER() OVER (
      PARTITION BY s.location_id, s.submission_id
      ORDER BY
        ABS(
          TIMESTAMP_DIFF(
            COALESCE(o.updated_at_ts, o.event_ts, TIMESTAMP '1970-01-01 00:00:00+00'),
            COALESCE(s.updated_at_ts, s.event_ts, TIMESTAMP '1970-01-01 00:00:00+00'),
            SECOND
          )
        ),
        COALESCE(o.updated_at_ts, o.event_ts, TIMESTAMP '1970-01-01 00:00:00+00') DESC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_form_submissions` s
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
    ON o.location_id = s.location_id
   AND o.contact_id = s.contact_id
)
SELECT
  s.location_id,
  s.submission_id,
  s.form_id,
  f.form_name,
  s.contact_id,
  c.full_name AS contact_name,
  c.email AS contact_email,
  COALESCE(s.source, c.source) AS source,
  COALESCE(s.utm_source, c.utm_source) AS utm_source,
  COALESCE(s.utm_medium, c.utm_medium) AS utm_medium,
  COALESCE(s.utm_campaign, c.utm_campaign) AS utm_campaign,
  COALESCE(s.utm_content, c.utm_content) AS utm_content,
  ro.opportunity_id,
  ro.pipeline_id,
  ro.pipeline_name,
  ro.pipeline_stage_id,
  ro.stage_name,
  ro.opportunity_status,
  ro.opportunity_amount,
  s.event_ts AS submission_event_ts,
  s.updated_at_ts AS submission_updated_at_ts,
  s.ingested_at,
  s.backfill_run_id,
  s.is_backfill,
  s.payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_form_submissions` s
LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_forms` f
  ON f.location_id = s.location_id
 AND f.form_id = s.form_id
LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts` c
  ON c.location_id = s.location_id
 AND c.contact_id = s.contact_id
LEFT JOIN ranked_opps ro
  ON ro.location_id = s.location_id
 AND ro.submission_id = s.submission_id
 AND ro.rn = 1;

-- STG: outbound call logs (optional entity; requires GHL outbound call source ingestion)
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_ghl_outbound_call_logs` AS
SELECT
  location_id,
  entity_id AS call_log_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  COALESCE(
    JSON_VALUE(payload_json, '$.contactId'),
    JSON_VALUE(payload_json, '$.contact.id'),
    JSON_VALUE(payload_json, '$.conversation.contactId'),
    JSON_VALUE(payload_json, '$.message.contactId')
  ) AS contact_id,
  COALESCE(
    JSON_VALUE(payload_json, '$.direction'),
    JSON_VALUE(payload_json, '$.messageDirection'),
    JSON_VALUE(payload_json, '$.message.direction')
  ) AS direction,
  COALESCE(
    JSON_VALUE(payload_json, '$.status'),
    JSON_VALUE(payload_json, '$.callStatus'),
    JSON_VALUE(payload_json, '$.messageStatus')
  ) AS call_status,
  COALESCE(
    JSON_VALUE(payload_json, '$.channel'),
    JSON_VALUE(payload_json, '$.type'),
    JSON_VALUE(payload_json, '$.messageType')
  ) AS channel,
  COALESCE(
    SAFE_CAST(JSON_VALUE(payload_json, '$.createdAt') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(payload_json, '$.created_at') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(payload_json, '$.dateAdded') AS TIMESTAMP),
    SAFE_CAST(JSON_VALUE(payload_json, '$.timestamp') AS TIMESTAMP),
    event_ts,
    updated_at_ts
  ) AS call_started_at,
  SAFE_CAST(COALESCE(
    JSON_VALUE(payload_json, '$.duration'),
    JSON_VALUE(payload_json, '$.callDuration'),
    JSON_VALUE(payload_json, '$.call_duration'),
    JSON_VALUE(payload_json, '$.meta.callDuration'),
    JSON_VALUE(payload_json, '$.meta.duration')
  ) AS INT64) AS call_duration_sec,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'outbound_call_logs';

-- CORE: outbound call fact (normalized)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` AS
SELECT
  location_id,
  call_log_id,
  contact_id,
  LOWER(TRIM(direction)) AS direction_norm,
  LOWER(TRIM(channel)) AS channel_norm,
  call_status,
  call_started_at,
  call_duration_sec,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_ghl_outbound_call_logs`
WHERE contact_id IS NOT NULL
  AND call_started_at IS NOT NULL
  AND (
    LOWER(TRIM(direction)) = 'outbound'
    OR REGEXP_CONTAINS(LOWER(TRIM(direction)), r'out')
  );

-- CORE: booking -> first outbound call bridge (decision-grade)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_speed_to_lead_booking_to_call` AS
WITH bookings AS (
  SELECT
    b.invitee_id,
    b.scheduled_event_id,
    b.contact_id,
    b.match_confidence,
    COALESCE(
      i.invitee_created_at,
      i.event_ts,
      i.ingested_at
    ) AS booked_at
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
),
candidate_calls AS (
  SELECT
    b.invitee_id,
    b.scheduled_event_id,
    b.contact_id,
    b.match_confidence,
    b.booked_at,
    c.call_log_id,
    c.call_started_at,
    c.call_status,
    TIMESTAMP_DIFF(c.call_started_at, b.booked_at, MINUTE) AS speed_to_lead_minutes,
    ROW_NUMBER() OVER (
      PARTITION BY b.invitee_id
      ORDER BY c.call_started_at ASC
    ) AS rn
  FROM bookings b
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
    ON c.contact_id = b.contact_id
   AND c.call_started_at >= b.booked_at
)
SELECT
  invitee_id,
  scheduled_event_id,
  contact_id,
  match_confidence,
  booked_at,
  call_log_id AS first_outbound_call_id,
  call_started_at AS first_outbound_call_at,
  call_status AS first_outbound_call_status,
  speed_to_lead_minutes,
  CASE
    WHEN speed_to_lead_minutes IS NULL THEN 'no_outbound_call'
    WHEN speed_to_lead_minutes <= 1 THEN '0-1m'
    WHEN speed_to_lead_minutes <= 5 THEN '2-5m'
    WHEN speed_to_lead_minutes <= 15 THEN '6-15m'
    WHEN speed_to_lead_minutes <= 60 THEN '16-60m'
    WHEN speed_to_lead_minutes <= 240 THEN '61-240m'
    ELSE '240m+'
  END AS speed_to_lead_bucket
FROM candidate_calls
WHERE rn = 1;

-- NOTE: `Marts.mrt_speed_to_lead_daily` and `Marts.mrt_speed_to_lead_overall`
-- previously had legacy 11-column / no-trigger-type definitions HERE in
-- ghl_models.sql. They were removed 2026-04-29 because `marts.sql` is the
-- canonical owner of those marts (17 columns sourced from `Marts.fct_speed_to_lead`,
-- including lead-magnet/trigger-type splits the legacy version lacked).
-- Keeping both definitions caused a schema-flip race: model.ghl wrote the
-- legacy schema, then model.marts overwrote with the canonical schema in
-- the same hourly Job — leaving the table briefly in legacy form, and
-- permanently in legacy form for any operator running `model.ghl`
-- standalone outside the marts wrapper. See `.claude/state/project-state.md`
-- and `.claude/rules/bq-ingest.md` for the full architectural decision.
