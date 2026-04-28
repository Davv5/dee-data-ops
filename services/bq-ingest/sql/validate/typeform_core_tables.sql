-- Typeform form coverage
SELECT
  'typeform_forms_cardinality' AS check_name,
  COUNT(*) AS row_count,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_forms`;

-- Typeform form URL coverage
SELECT
  'typeform_forms_url_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(form_url IS NOT NULL AND TRIM(form_url) != ''), COUNT(*)) AS pct_populated,
  CASE
    WHEN SAFE_DIVIDE(COUNTIF(form_url IS NOT NULL AND TRIM(form_url) != ''), COUNT(*)) >= 0.90 THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_forms`;

-- Typeform responses cardinality
SELECT
  'typeform_responses_cardinality' AS check_name,
  COUNT(*) AS row_count,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response email coverage
SELECT
  'typeform_responses_email_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(respondent_email IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(respondent_email IS NOT NULL), COUNT(*)) >= 0.50 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform demographic extraction coverage: age
SELECT
  'typeform_responses_age_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(respondent_age_bracket IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(respondent_age_bracket IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform demographic extraction coverage: business stage
SELECT
  'typeform_responses_business_stage_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(respondent_business_stage IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(respondent_business_stage IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform demographic extraction coverage: investment range
SELECT
  'typeform_responses_investment_range_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(respondent_investment_range IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(respondent_investment_range IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform demographic extraction coverage: core struggle
SELECT
  'typeform_responses_core_struggle_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(respondent_core_struggle IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(respondent_core_struggle IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response landed_at coverage
SELECT
  'typeform_responses_landed_at_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(landed_at IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(landed_at IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response ending coverage
SELECT
  'typeform_responses_ending_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF((ending_id IS NOT NULL AND TRIM(ending_id) != '') OR (ending_ref IS NOT NULL AND TRIM(ending_ref) != '')), COUNT(*)) AS pct_populated,
  CASE
    WHEN SAFE_DIVIDE(COUNTIF((ending_id IS NOT NULL AND TRIM(ending_id) != '') OR (ending_ref IS NOT NULL AND TRIM(ending_ref) != '')), COUNT(*)) > 0
      THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response hidden field coverage
SELECT
  'typeform_responses_hidden_fields_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(hidden_fields_json IS NOT NULL AND TRIM(TO_JSON_STRING(hidden_fields_json)) NOT IN ('', 'null', '{}')), COUNT(*)) AS pct_populated,
  CASE
    WHEN SAFE_DIVIDE(COUNTIF(hidden_fields_json IS NOT NULL AND TRIM(TO_JSON_STRING(hidden_fields_json)) NOT IN ('', 'null', '{}')), COUNT(*)) > 0
      THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response variables coverage (optional: many forms do not define variables)
SELECT
  'typeform_responses_variables_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(variables_json IS NOT NULL AND TRIM(TO_JSON_STRING(variables_json)) NOT IN ('', 'null', '[]', '{}')), COUNT(*)) AS pct_populated,
  CASE
    WHEN SAFE_DIVIDE(COUNTIF(variables_json IS NOT NULL AND TRIM(TO_JSON_STRING(variables_json)) NOT IN ('', 'null', '[]', '{}')), COUNT(*)) > 0
      THEN 'PASS'
    ELSE 'WARN'
  END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response form score coverage (optional in Typeform config)
SELECT
  'typeform_responses_form_score_coverage' AS check_name,
  SAFE_DIVIDE(COUNTIF(form_score IS NOT NULL), COUNT(*)) AS pct_populated,
  CASE WHEN SAFE_DIVIDE(COUNTIF(form_score IS NOT NULL), COUNT(*)) > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform response freshness
SELECT
  'typeform_responses_data_freshness' AS check_name,
  DATETIME_DIFF(CURRENT_TIMESTAMP(), MAX(submitted_at), HOUR) AS hours_old,
  CASE WHEN DATETIME_DIFF(CURRENT_TIMESTAMP(), MAX(submitted_at), HOUR) <= 168 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses`;

-- Typeform GHL contact match rate
SELECT
  'typeform_ghl_contact_match_rate' AS check_name,
  SAFE_DIVIDE(COUNTIF(contact_id IS NOT NULL), COUNT(*)) AS match_rate,
  CASE WHEN SAFE_DIVIDE(COUNTIF(contact_id IS NOT NULL), COUNT(*)) >= 0.30 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts`;

-- Typeform bridge phone fallback usage
SELECT
  'typeform_ghl_phone_fallback_usage' AS check_name,
  SAFE_DIVIDE(COUNTIF(contact_id IS NOT NULL AND match_method = 'phone_last10'), COUNT(*)) AS pct_rows,
  CASE WHEN COUNTIF(contact_id IS NOT NULL AND match_method = 'phone_last10') > 0 THEN 'PASS' ELSE 'WARN' END AS result
FROM `project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts`;

-- Mart-level Typeform field coverage among leads with Typeform responses.
WITH responder_leads AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  WHERE COALESCE(typeform_responses_count, 0) > 0
)
SELECT
  check_name,
  pct_populated,
  CASE WHEN pct_populated >= 0.80 THEN 'PASS' ELSE 'WARN' END AS result
FROM (
  SELECT
    'mart_typeform_age_bracket_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_age_bracket IS NOT NULL AND TRIM(typeform_age_bracket) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_business_stage_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_business_stage IS NOT NULL AND TRIM(typeform_business_stage) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_investment_range_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_investment_range IS NOT NULL AND TRIM(typeform_investment_range) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads

  UNION ALL

  SELECT
    'mart_typeform_core_struggle_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_core_struggle IS NOT NULL AND TRIM(typeform_core_struggle) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_leads
)
ORDER BY check_name;

-- Typeform stub field coverage among leads with Typeform responses.
WITH responder_contacts AS (
  SELECT *
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
  WHERE COALESCE(typeform_responses_count, 0) > 0
)
SELECT
  check_name,
  pct_populated,
  CASE WHEN pct_populated >= 0.50 THEN 'PASS' ELSE 'WARN' END AS result
FROM (
  SELECT
    'fct_typeform_primary_goal_stub_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_primary_goal_stub IS NOT NULL AND TRIM(typeform_primary_goal_stub) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_contacts

  UNION ALL

  SELECT
    'fct_typeform_primary_obstacle_stub_coverage' AS check_name,
    COALESCE(
      SAFE_DIVIDE(
        COUNTIF(typeform_primary_obstacle_stub IS NOT NULL AND TRIM(typeform_primary_obstacle_stub) != ''),
        COUNT(*)
      ),
      0
    ) AS pct_populated
  FROM responder_contacts
)
ORDER BY check_name;
