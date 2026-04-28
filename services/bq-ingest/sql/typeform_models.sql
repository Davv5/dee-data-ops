-- STG: forms
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_typeform_forms` AS
SELECT
  entity_id AS form_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  JSON_VALUE(payload_json, '$.title') AS form_title,
  JSON_VALUE(payload_json, '$.state') AS form_state,
  SAFE_CAST(JSON_VALUE(payload_json, '$.created_at') AS TIMESTAMP) AS form_created_at,
  SAFE_CAST(JSON_VALUE(payload_json, '$.updated_at') AS TIMESTAMP) AS form_updated_at,
  COALESCE(
    NULLIF(TRIM(JSON_VALUE(payload_json, '$.url')), ''),
    NULLIF(TRIM(JSON_VALUE(payload_json, '$._links.display')), ''),
    NULLIF(TRIM(JSON_VALUE(payload_json, '$.self.href')), '')
  ) AS form_url,
  SAFE_CAST(JSON_VALUE(payload_json, '$.responses_count') AS INT64) AS responses_count,
  JSON_QUERY(payload_json, '$.fields') AS fields_json,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.typeform_objects_raw`
WHERE entity_type = 'forms';

-- STG: responses
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.STG.stg_typeform_responses` AS
SELECT
  entity_id AS response_id,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  JSON_VALUE(payload_json, '$.form_id') AS form_id,
  SAFE_CAST(JSON_VALUE(payload_json, '$.submitted_at') AS TIMESTAMP) AS submitted_at,
  SAFE_CAST(JSON_VALUE(payload_json, '$.landed_at') AS TIMESTAMP) AS landed_at,
  JSON_VALUE(payload_json, '$.ending.id') AS ending_id,
  JSON_VALUE(payload_json, '$.ending.ref') AS ending_ref,
  SAFE_CAST(JSON_VALUE(payload_json, '$.calculated.score') AS FLOAT64) AS form_score,
  JSON_QUERY(payload_json, '$.hidden') AS hidden_fields_json,
  JSON_QUERY(payload_json, '$.variables') AS variables_json,
  JSON_VALUE(payload_json, '$.respondent_id') AS respondent_id,
  JSON_VALUE(payload_json, '$.metadata.user_agent') AS user_agent,
  JSON_VALUE(payload_json, '$.metadata.platform') AS platform,
  JSON_VALUE(payload_json, '$.metadata.referer') AS referer,
  JSON_VALUE(payload_json, '$.metadata.network_id') AS network_id,
  -- Extract email from answers if present
  (SELECT JSON_VALUE(answer, '$.email')
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.type') = 'email'
   LIMIT 1) AS respondent_email,
  -- Extract name from answers if present
  (SELECT JSON_VALUE(answer, '$.text')
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.type') = 'short_text' AND REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')), r'name')
   LIMIT 1) AS respondent_name,
  -- Extract demographic/profile attributes with resilient fallback by field ref.
  (SELECT COALESCE(
      JSON_VALUE(answer, '$.choice.label'),
      JSON_VALUE(answer, '$.text'),
      SAFE_CAST(JSON_VALUE(answer, '$.number') AS STRING),
      SAFE_CAST(JSON_VALUE(answer, '$.boolean') AS STRING)
   )
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.field.ref') IN ('bc55c921-7266-485e-aa75-6510ee2bdea6')
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')), r'(^|[^a-z])age([^a-z]|$)')
   ORDER BY CASE WHEN JSON_VALUE(answer, '$.field.ref') IN ('bc55c921-7266-485e-aa75-6510ee2bdea6') THEN 0 ELSE 1 END
   LIMIT 1) AS respondent_age_bracket,
  (SELECT COALESCE(
      JSON_VALUE(answer, '$.choice.label'),
      JSON_VALUE(answer, '$.text')
   )
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.field.ref') IN ('faae0315-138c-494f-afd4-326e9ffdd579')
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')), r'business stage|where are you|current stage|currently')
   ORDER BY CASE WHEN JSON_VALUE(answer, '$.field.ref') IN ('faae0315-138c-494f-afd4-326e9ffdd579') THEN 0 ELSE 1 END
   LIMIT 1) AS respondent_business_stage,
  (SELECT COALESCE(
      JSON_VALUE(answer, '$.choice.label'),
      JSON_VALUE(answer, '$.text')
   )
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.field.ref') IN ('1e1ceeb0-cf9d-4bc0-b938-e7ba10895b61')
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')), r'invest|budget|spend|price range')
   ORDER BY CASE WHEN JSON_VALUE(answer, '$.field.ref') IN ('1e1ceeb0-cf9d-4bc0-b938-e7ba10895b61') THEN 0 ELSE 1 END
   LIMIT 1) AS respondent_investment_range,
  (SELECT COALESCE(
      JSON_VALUE(answer, '$.text'),
      JSON_VALUE(answer, '$.choice.label')
   )
   FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
   WHERE JSON_VALUE(answer, '$.field.ref') IN ('57d9575c-981a-4f3a-8d29-52eb3203bf3c')
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')), r'struggle|challenge|stuck|hardest|biggest')
   ORDER BY CASE WHEN JSON_VALUE(answer, '$.field.ref') IN ('57d9575c-981a-4f3a-8d29-52eb3203bf3c') THEN 0 ELSE 1 END
   LIMIT 1) AS respondent_core_struggle,
  JSON_QUERY(payload_json, '$.answers') AS answers_json,
  payload_json
FROM `project-41542e21-470f-4589-96d.Raw.typeform_objects_raw`
WHERE entity_type = 'responses';

-- CORE: forms
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_typeform_forms` AS
SELECT
  form_id,
  form_title,
  form_state,
  form_created_at,
  form_updated_at,
  form_url,
  responses_count,
  fields_json,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_typeform_forms`;

-- CORE: responses
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.dim_typeform_responses` AS
SELECT
  response_id,
  form_id,
  respondent_id,
  LOWER(TRIM(respondent_email)) AS respondent_email,
  respondent_name,
  NULLIF(TRIM(respondent_age_bracket), '') AS respondent_age_bracket,
  NULLIF(TRIM(respondent_business_stage), '') AS respondent_business_stage,
  NULLIF(TRIM(respondent_investment_range), '') AS respondent_investment_range,
  NULLIF(TRIM(respondent_core_struggle), '') AS respondent_core_struggle,
  submitted_at,
  landed_at,
  ending_id,
  ending_ref,
  form_score,
  hidden_fields_json,
  variables_json,
  user_agent,
  platform,
  referer,
  network_id,
  answers_json,
  event_ts,
  updated_at_ts,
  ingested_at,
  backfill_run_id,
  is_backfill,
  payload_json
FROM `project-41542e21-470f-4589-96d.STG.stg_typeform_responses`;

-- CORE: deterministic response -> GHL contact bridge
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts` AS
WITH ghl_contacts AS (
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
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10,
    full_name,
    phone,
    source,
    last_seen_ts
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts`
  WHERE email IS NOT NULL
     OR LENGTH(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', '')) >= 10
),
responses AS (
  SELECT
    response_id,
    form_id,
    respondent_id,
    respondent_email,
    LOWER(TRIM(respondent_email)) AS respondent_email_norm,
    respondent_name,
    submitted_at,
    CASE
      WHEN respondent_email IS NULL THEN NULL
      WHEN SPLIT(respondent_email, '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(respondent_email, '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE respondent_email
    END AS respondent_email_canon,
    CASE
      WHEN LENGTH(
        REGEXP_REPLACE(
          IFNULL(
            (
              SELECT
                COALESCE(
                  JSON_VALUE(answer, '$.phone_number'),
                  JSON_VALUE(answer, '$.text')
                )
              FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
              WHERE JSON_VALUE(answer, '$.type') = 'phone_number'
                 OR REGEXP_CONTAINS(
                   LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')),
                   r'phone'
                 )
              LIMIT 1
            ),
            ''
          ),
          r'[^0-9]',
          ''
        )
      ) >= 10
        THEN RIGHT(
          REGEXP_REPLACE(
            IFNULL(
              (
                SELECT
                  COALESCE(
                    JSON_VALUE(answer, '$.phone_number'),
                    JSON_VALUE(answer, '$.text')
                  )
                FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.answers')) AS answer
                WHERE JSON_VALUE(answer, '$.type') = 'phone_number'
                   OR REGEXP_CONTAINS(
                     LOWER(COALESCE(JSON_VALUE(answer, '$.field.title'), '')),
                     r'phone'
                   )
                LIMIT 1
              ),
              ''
            ),
            r'[^0-9]',
            ''
          ),
          10
        )
      ELSE NULL
    END AS respondent_phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`
),
matched_candidates AS (
  SELECT
    r.response_id,
    r.form_id,
    r.respondent_id,
    r.respondent_email,
    r.respondent_name,
    r.submitted_at,
    g.location_id,
    g.contact_id,
    g.email AS matched_contact_email,
    g.full_name AS matched_contact_name,
    g.phone AS matched_contact_phone,
    g.source AS matched_contact_source,
    g.last_seen_ts,
    'email_exact' AS match_method,
    1.00 AS match_score,
    1 AS match_priority
  FROM responses r
  JOIN ghl_contacts g
    ON r.respondent_email_norm = g.email_norm
   AND r.respondent_email_norm IS NOT NULL

  UNION ALL

  SELECT
    r.response_id,
    r.form_id,
    r.respondent_id,
    r.respondent_email,
    r.respondent_name,
    r.submitted_at,
    g.location_id,
    g.contact_id,
    g.email AS matched_contact_email,
    g.full_name AS matched_contact_name,
    g.phone AS matched_contact_phone,
    g.source AS matched_contact_source,
    g.last_seen_ts,
    'email_canonical' AS match_method,
    0.95 AS match_score,
    2 AS match_priority
  FROM responses r
  JOIN ghl_contacts g
    ON r.respondent_email_canon = g.email_canon
   AND r.respondent_email_canon IS NOT NULL

  UNION ALL

  SELECT
    r.response_id,
    r.form_id,
    r.respondent_id,
    r.respondent_email,
    r.respondent_name,
    r.submitted_at,
    g.location_id,
    g.contact_id,
    g.email AS matched_contact_email,
    g.full_name AS matched_contact_name,
    g.phone AS matched_contact_phone,
    g.source AS matched_contact_source,
    g.last_seen_ts,
    'phone_last10' AS match_method,
    0.90 AS match_score,
    3 AS match_priority
  FROM responses r
  JOIN ghl_contacts g
    ON r.respondent_phone_last10 = g.phone_last10
   AND r.respondent_phone_last10 IS NOT NULL
),
candidates AS (
  SELECT
    response_id,
    form_id,
    respondent_id,
    respondent_email,
    respondent_name,
    submitted_at,
    location_id,
    contact_id,
    matched_contact_email,
    matched_contact_name,
    matched_contact_phone,
    matched_contact_source,
    last_seen_ts,
    match_method,
    match_score,
    match_priority
  FROM matched_candidates

  UNION ALL

  SELECT
    r.response_id,
    r.form_id,
    r.respondent_id,
    r.respondent_email,
    r.respondent_name,
    r.submitted_at,
    CAST(NULL AS STRING) AS location_id,
    CAST(NULL AS STRING) AS contact_id,
    CAST(NULL AS STRING) AS matched_contact_email,
    CAST(NULL AS STRING) AS matched_contact_name,
    CAST(NULL AS STRING) AS matched_contact_phone,
    CAST(NULL AS STRING) AS matched_contact_source,
    CAST(NULL AS TIMESTAMP) AS last_seen_ts,
    'unmatched' AS match_method,
    0.00 AS match_score,
    99 AS match_priority
  FROM responses r
  WHERE NOT EXISTS (
    SELECT 1
    FROM matched_candidates mc
    WHERE mc.response_id = r.response_id
  )
),
deduped AS (
  SELECT
    response_id,
    form_id,
    respondent_id,
    respondent_email,
    respondent_name,
    submitted_at,
    location_id,
    contact_id,
    matched_contact_email,
    matched_contact_name,
    matched_contact_phone,
    matched_contact_source,
    last_seen_ts,
    match_method,
    match_score,
    ROW_NUMBER() OVER (
      PARTITION BY response_id
      ORDER BY
        match_priority,
        match_score DESC,
        COALESCE(last_seen_ts, TIMESTAMP('1970-01-01 00:00:00 UTC')) DESC,
        contact_id
    ) AS rn
  FROM candidates
)
SELECT
  response_id,
  form_id,
  respondent_id,
  respondent_email,
  respondent_name,
  submitted_at,
  location_id,
  contact_id,
  matched_contact_email,
  matched_contact_name,
  matched_contact_phone,
  matched_contact_source,
  last_seen_ts,
  match_method,
  match_score
FROM deduped
WHERE rn = 1;

-- CORE: one row per answer per response (all question answers exploded for full extraction)
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Core.fct_typeform_answers` AS
SELECT
  r.response_id,
  r.form_id,
  r.submitted_at,
  JSON_VALUE(a, '$.field.id')    AS field_id,
  JSON_VALUE(a, '$.field.ref')   AS field_ref,
  JSON_VALUE(a, '$.field.type')  AS field_type,
  JSON_VALUE(a, '$.field.title') AS field_title,
  JSON_VALUE(a, '$.type')        AS answer_type,
  COALESCE(
    JSON_VALUE(a, '$.text'),
    JSON_VALUE(a, '$.email'),
    JSON_VALUE(a, '$.url'),
    JSON_VALUE(a, '$.file_url'),
    JSON_VALUE(a, '$.date'),
    JSON_VALUE(a, '$.choice.label'),
    SAFE_CAST(JSON_VALUE(a, '$.number') AS STRING),
    SAFE_CAST(JSON_VALUE(a, '$.boolean') AS STRING),
    ARRAY_TO_STRING(
      ARRAY(SELECT JSON_VALUE(lbl, '$') FROM UNNEST(JSON_QUERY_ARRAY(a, '$.choices.labels')) AS lbl),
      ', '
    )
  ) AS answer_value
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses` r
CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r.answers_json)) AS a
WHERE r.answers_json IS NOT NULL;
