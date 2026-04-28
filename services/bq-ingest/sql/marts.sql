-- Marts layer for Looker Studio and GTM reporting.
-- Run AFTER Core models are fresh (ghl_models, calendly_models, models.sql / Fanbasis, optionally Stripe).
-- Example: python3 -c "from mart_models import run_mart_models; run_mart_models()"

-- Golden contact / canonical lead (GHL spine; one row per CRM contact).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` AS
WITH ghl_base AS (
  SELECT
    c.location_id,
    c.contact_id,
    c.email,
    c.first_name,
    c.last_name,
    c.full_name,
    c.phone,
    c.country,
    c.source AS ghl_source,
    cft.source_first AS ghl_source_first,
    c.utm_source,
    c.utm_medium,
    c.utm_campaign,
    c.utm_content,
    cft.utm_source_first,
    cft.utm_medium_first,
    cft.utm_campaign_first,
    cft.utm_term_first,
    cft.utm_content_first,
    c.tags_csv,
    c.first_seen_ts,
    c.last_seen_ts,
    c.assigned_to_user_id,
    cft.assigned_to_user_id_first,
    cft.first_contact_ts,
    LOWER(TRIM(c.email)) AS email_norm,
    CASE
      WHEN c.email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(c.email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(c.email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(c.email))
    END AS email_canon
  FROM `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts` c
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_contacts_first_touch` cft
    ON cft.location_id = c.location_id
   AND cft.contact_id = c.contact_id
),
cal_agg AS (
  SELECT
    b.location_id,
    b.contact_id,
    COUNT(DISTINCT b.invitee_id) AS meetings_booked_cnt,
    COUNTIF(
      LOWER(COALESCE(i.invitee_status, '')) = 'active'
      AND COALESCE(LOWER(i.event_status), '') NOT IN ('canceled', 'cancelled')
      AND i.scheduled_start_time IS NOT NULL
      AND i.scheduled_start_time <= CURRENT_TIMESTAMP()
    ) AS meetings_showed_cnt,
    MIN(i.scheduled_start_time) AS first_meeting_start_ts,
    MAX(i.scheduled_start_time) AS last_meeting_start_ts
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
  GROUP BY 1, 2
),
opp AS (
  SELECT
    o.contact_id,
    o.location_id,
    COUNT(*) AS opportunity_count,
    SUM(SAFE_CAST(o.amount AS NUMERIC)) AS opportunities_total_amount,
    MAX_BY(
      CONCAT(COALESCE(o.pipeline_name, ''), ' / ', COALESCE(o.stage_name, '')),
      COALESCE(o.updated_at_ts, o.event_ts, TIMESTAMP('1970-01-01 00:00:00 UTC'))
    ) AS pipeline_stage_label
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
  GROUP BY 1, 2
),
conv_latest AS (
  SELECT
    location_id,
    contact_id,
    assigned_to_user_id
  FROM (
    SELECT
      location_id,
      contact_id,
      assigned_to_user_id,
      ROW_NUMBER() OVER (
        PARTITION BY location_id, contact_id
        ORDER BY COALESCE(message_created_at, updated_at_ts, event_ts, ingested_at) DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations`
    WHERE contact_id IS NOT NULL
      AND NULLIF(TRIM(assigned_to_user_id), '') IS NOT NULL
  )
  WHERE rn = 1
),
setter_identity AS (
  SELECT
    LOWER(TRIM(ghl_user_id)) AS ghl_user_id_norm,
    canonical_name,
    canonical_team,
    canonical_role
  FROM `project-41542e21-470f-4589-96d.Marts.bridge_setter_identity`
  WHERE NULLIF(TRIM(ghl_user_id), '') IS NOT NULL
),
fb_email AS (
  SELECT
    customer_id,
    email AS customer_email,
    lifetime_net_amount,
    lifetime_transaction_count,
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
    END AS email_canon
  FROM `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers`
),
fb_by_canon AS (
  SELECT
    email_canon,
    SUM(lifetime_net_amount) AS fanbasis_lifetime_net,
    SUM(lifetime_transaction_count) AS fanbasis_transaction_count,
    COUNT(DISTINCT customer_id) AS fanbasis_customer_id_count
  FROM fb_email
  WHERE email_canon IS NOT NULL
  GROUP BY email_canon
),
typeform_agg AS (
  SELECT
    b.location_id,
    b.contact_id,
    COUNT(DISTINCT b.response_id) AS typeform_responses_count,
    MAX_BY(f.form_title, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_form_title,
    MAX(COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_response_ts,
    MAX_BY(
      SAFE_CAST(JSON_VALUE(r.payload_json, '$.landed_at') AS TIMESTAMP),
      COALESCE(r.submitted_at, r.event_ts, r.ingested_at)
    ) AS latest_typeform_landed_at,
    MAX_BY(
      CASE
        WHEN r.submitted_at IS NOT NULL
          AND SAFE_CAST(JSON_VALUE(r.payload_json, '$.landed_at') AS TIMESTAMP) IS NOT NULL
          THEN GREATEST(
            TIMESTAMP_DIFF(
              r.submitted_at,
              SAFE_CAST(JSON_VALUE(r.payload_json, '$.landed_at') AS TIMESTAMP),
              SECOND
            ),
            0
          )
        ELSE NULL
      END,
      COALESCE(r.submitted_at, r.event_ts, r.ingested_at)
    ) AS latest_typeform_completion_seconds,
    MAX_BY(JSON_VALUE(r.payload_json, '$.ending.id'), COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_ending_id,
    MAX_BY(JSON_VALUE(r.payload_json, '$.ending.ref'), COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_ending_ref,
    MAX_BY(SAFE_CAST(JSON_VALUE(r.payload_json, '$.calculated.score') AS NUMERIC), COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_form_score,
    MAX_BY(JSON_QUERY(r.payload_json, '$.hidden'), COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_hidden_fields_json,
    MAX_BY(JSON_QUERY(r.payload_json, '$.variables'), COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_variables_json,
    MAX_BY(r.answers_json, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_answers_json,
    MAX_BY(r.respondent_age_bracket, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_age_bracket,
    MAX_BY(r.respondent_business_stage, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_business_stage,
    MAX_BY(r.respondent_investment_range, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_investment_range,
    MAX_BY(r.respondent_core_struggle, COALESCE(r.submitted_at, r.event_ts, r.ingested_at)) AS latest_typeform_core_struggle
  FROM `project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.dim_typeform_responses` r
    ON r.response_id = b.response_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_typeform_forms` f
    ON f.form_id = r.form_id
  WHERE b.contact_id IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  CONCAT(g.location_id, '|', g.contact_id) AS golden_contact_key,
  g.location_id,
  g.contact_id AS ghl_contact_id,
  g.email,
  g.email_norm,
  g.email_canon,
  g.first_name,
  g.last_name,
  g.full_name,
  g.phone,
  g.country,
  g.ghl_source,
  g.utm_source,
  g.utm_medium,
  g.utm_campaign,
  g.utm_content,
  g.ghl_source_first,
  g.utm_source_first,
  g.utm_medium_first,
  g.utm_campaign_first,
  g.utm_term_first,
  g.utm_content_first,
  g.tags_csv AS lead_magnet_history,
  g.first_seen_ts AS ghl_date_added_ts,
  g.first_seen_ts AS ghl_first_seen_ts,
  g.last_seen_ts AS ghl_last_seen_ts,
  COALESCE(g.assigned_to_user_id, conv.assigned_to_user_id) AS assigned_to_user_id,
  g.assigned_to_user_id_first AS setter_at_first_contact,
  g.first_contact_ts,
  s.canonical_name AS rep_name,
  s.canonical_team AS team_name,
  s.canonical_role AS rep_role,
  CASE
    WHEN COALESCE(NULLIF(TRIM(g.utm_campaign), ''), NULLIF(TRIM(g.utm_source), ''), NULLIF(TRIM(g.ghl_source), '')) IS NULL
      THEN 'Unknown'
    -- IG Blueprint: utm_campaign OR ghl_source contains ig/instagram blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'ig blueprint|instagram blueprint|instagram.*blueprint|ig.*blueprint|ig story blueprint')
      THEN 'IG Blueprint'
    -- TT Blueprint: utm_campaign OR ghl_source contains tiktok/tt blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'tt blueprint|tiktok blueprint|tt.*blueprint|buildsbrands tt roadmap|givesgame tt roadmap')
      THEN 'TT Blueprint'
    -- YT Blueprint: utm_campaign OR ghl_source contains youtube/yt blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'yt blueprint|youtube blueprint|yt.*blueprint')
      THEN 'YT Blueprint'
    -- AI Brand Prompts
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.utm_source, ''), ' ', IFNULL(g.ghl_source, ''))), r'ai brand')
      THEN 'AI Brand Prompts'
    -- Content Guide
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'content guide')
      THEN 'Content Guide'
    -- Google Doc Lead Magnets
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source), '')), r'google doc')
      THEN 'Google Doc Lead Magnets'
    -- Instagram organic: exact platform source names + dbb-ig variants
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) IN ('ig', 'instagram', 'ig-dbb', 'dbb-ig', 'ig manu list', 'ig sms templates', 'ig mod tarriff', 'ig giveaway', 'ig story giveaway', 'dw-ig')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source, '')), r'^ig$|^instagram$')
      THEN 'Instagram Organic'
    -- TikTok organic: exact platform source names + dbb-tt variants
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) IN ('tt', 'tiktok', 'dbb-tt', 'tt-dgg', 'tt giveaway')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source, '')), r'^tt$|^tiktok$')
      THEN 'TikTok Organic'
    -- YouTube organic: exact platform source names
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) IN ('yt', 'youtube', 'yt-live', 'youtube live')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source, '')), r'^yt$|^youtube$')
      THEN 'YouTube Organic'
    -- Skool
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'skool')
      THEN 'Skool'
    -- Free training / free class
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source), '')), r'free.?class|free training|free-class')
      THEN 'Free Training'
    -- Outbound / SMS / Email (direct contact channels)
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) = 'outbound'
      THEN 'Outbound'
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) = 'sms'
      THEN 'SMS'
    WHEN LOWER(TRIM(IFNULL(g.ghl_source, ''))) = 'email'
      THEN 'Email'
    -- Referral
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source), '')), r'friend|referral|referred')
      THEN 'Referral'
    -- Inner Circle
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'inner circle|ic 2\.0|inner circle 2|waitlist')
      THEN 'IC 2.0 Waitlist'
    -- Remaining lower-volume mapped campaigns
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'email training')
      THEN 'Email Training'
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'1-on-1|one on one|mentorship')
      THEN '1-on-1 Mentorship'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign), '')), r'blueprint email|email list')
      THEN 'Blueprint Email List'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign), '')), r'^booking$|book a call|book call')
      THEN 'Booking'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign), '')), r'money management')
      THEN 'Money Management'
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign, ''), ' ', IFNULL(g.ghl_source, ''))), r'free guide|dee free guide')
      THEN 'Free Guide'
    ELSE 'Other'
  END AS campaign_reporting,
  CASE
    WHEN COALESCE(NULLIF(TRIM(g.utm_campaign_first), ''), NULLIF(TRIM(g.utm_source_first), ''), NULLIF(TRIM(g.ghl_source_first), '')) IS NULL
      THEN 'Unknown'
    -- IG Blueprint: utm_campaign OR ghl_source contains ig/instagram blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'ig blueprint|instagram blueprint|instagram.*blueprint|ig.*blueprint|ig story blueprint')
      THEN 'IG Blueprint'
    -- TT Blueprint: utm_campaign OR ghl_source contains tiktok/tt blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'tt blueprint|tiktok blueprint|tt.*blueprint|buildsbrands tt roadmap|givesgame tt roadmap')
      THEN 'TT Blueprint'
    -- YT Blueprint: utm_campaign OR ghl_source contains youtube/yt blueprint signals
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'yt blueprint|youtube blueprint|yt.*blueprint')
      THEN 'YT Blueprint'
    -- AI Brand Prompts
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.utm_source_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'ai brand')
      THEN 'AI Brand Prompts'
    -- Content Guide
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'content guide')
      THEN 'Content Guide'
    -- Google Doc Lead Magnets
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source_first), '')), r'google doc')
      THEN 'Google Doc Lead Magnets'
    -- Instagram organic: exact platform source names + dbb-ig variants
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) IN ('ig', 'instagram', 'ig-dbb', 'dbb-ig', 'ig manu list', 'ig sms templates', 'ig mod tarriff', 'ig giveaway', 'ig story giveaway', 'dw-ig')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source_first, '')), r'^ig$|^instagram$')
      THEN 'Instagram Organic'
    -- TikTok organic: exact platform source names + dbb-tt variants
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) IN ('tt', 'tiktok', 'dbb-tt', 'tt-dgg', 'tt giveaway')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source_first, '')), r'^tt$|^tiktok$')
      THEN 'TikTok Organic'
    -- YouTube organic: exact platform source names
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) IN ('yt', 'youtube', 'yt-live', 'youtube live')
      OR REGEXP_CONTAINS(LOWER(IFNULL(g.utm_source_first, '')), r'^yt$|^youtube$')
      THEN 'YouTube Organic'
    -- Skool
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'skool')
      THEN 'Skool'
    -- Free training / free class
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source_first), '')), r'free.?class|free training|free-class')
      THEN 'Free Training'
    -- Outbound / SMS / Email (direct contact channels)
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) = 'outbound'
      THEN 'Outbound'
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) = 'sms'
      THEN 'SMS'
    WHEN LOWER(TRIM(IFNULL(g.ghl_source_first, ''))) = 'email'
      THEN 'Email'
    -- Referral
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.ghl_source_first), '')), r'friend|referral|referred')
      THEN 'Referral'
    -- Inner Circle
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'inner circle|ic 2\.0|inner circle 2|waitlist')
      THEN 'IC 2.0 Waitlist'
    -- Remaining lower-volume mapped campaigns
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'email training')
      THEN 'Email Training'
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'1-on-1|one on one|mentorship')
      THEN '1-on-1 Mentorship'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign_first), '')), r'blueprint email|email list')
      THEN 'Blueprint Email List'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign_first), '')), r'^booking$|book a call|book call')
      THEN 'Booking'
    WHEN REGEXP_CONTAINS(LOWER(IFNULL(TRIM(g.utm_campaign_first), '')), r'money management')
      THEN 'Money Management'
    WHEN REGEXP_CONTAINS(LOWER(CONCAT(IFNULL(g.utm_campaign_first, ''), ' ', IFNULL(g.ghl_source_first, ''))), r'free guide|dee free guide')
      THEN 'Free Guide'
    ELSE 'Other'
  END AS campaign_reporting_first_touch,
  CASE
    WHEN g.email_canon IS NULL THEN 'no_email_on_contact'
    WHEN COALESCE(NULLIF(TRIM(g.utm_campaign), ''), NULLIF(TRIM(g.utm_source), ''), NULLIF(TRIM(g.ghl_source), '')) IS NULL
      THEN 'no_campaign_or_source'
    ELSE 'attribution_present'
  END AS attribution_gap_reason,
  COALESCE(cal.meetings_booked_cnt, 0) AS meetings_booked_cnt,
  COALESCE(cal.meetings_showed_cnt, 0) AS meetings_showed_cnt,
  cal.first_meeting_start_ts,
  cal.last_meeting_start_ts,
  COALESCE(cal.meetings_booked_cnt, 0) > 0 AS has_calendly_booking,
  COALESCE(opp.opportunity_count, 0) AS opportunity_count,
  opp.opportunities_total_amount,
  opp.pipeline_stage_label,
  COALESCE(fb.fanbasis_lifetime_net, 0) AS fanbasis_lifetime_net,
  COALESCE(fb.fanbasis_transaction_count, 0) AS fanbasis_transaction_count,
  fb.fanbasis_customer_id_count,
  COALESCE(fb.fanbasis_lifetime_net, 0) > 0 AS has_fanbasis_payment,
  COALESCE(tf.typeform_responses_count, 0) AS typeform_responses_count,
  tf.latest_typeform_form_title,
  tf.latest_typeform_response_ts,
  tf.latest_typeform_landed_at,
  tf.latest_typeform_completion_seconds,
  tf.latest_typeform_ending_id,
  tf.latest_typeform_ending_ref,
  tf.latest_typeform_form_score,
  tf.latest_typeform_hidden_fields_json,
  tf.latest_typeform_variables_json,
  tf.latest_typeform_answers_json,
  tf.latest_typeform_age_bracket,
  tf.latest_typeform_business_stage,
  tf.latest_typeform_investment_range,
  tf.latest_typeform_core_struggle,
  CASE
    WHEN tf.latest_typeform_core_struggle IS NULL OR TRIM(tf.latest_typeform_core_struggle) = '' THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'lead|client|customer|book|appointment|call|close|sale|revenue|income|cash')
      THEN 'Increase Leads & Revenue'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'offer|niche|position|messag|brand')
      THEN 'Clarify Offer & Positioning'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'content|social|instagram|tiktok|ads|marketing|audience')
      THEN 'Improve Marketing & Audience Growth'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'system|process|automat|team|operation|workflow|scale|time')
      THEN 'Build Systems & Scale Capacity'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'mindset|confidence|fear|overwhelm|consisten|motivat')
      THEN 'Strengthen Mindset & Consistency'
    ELSE 'Other / Unclassified'
  END AS typeform_primary_goal_stub,
  CASE
    WHEN tf.latest_typeform_core_struggle IS NULL OR TRIM(tf.latest_typeform_core_struggle) = '' THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'lead|client|customer|audience|traffic')
      THEN 'Lead Generation'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'close|sales?|convert|book|appointment|follow.?up')
      THEN 'Sales Conversion'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'time|capacity|overwhelm|burnout|consisten')
      THEN 'Time & Capacity'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'offer|niche|position|messag|clarity')
      THEN 'Offer & Positioning Clarity'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'system|process|automat|workflow|team|operation')
      THEN 'Systems & Operations'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'mindset|confidence|fear|motivat')
      THEN 'Mindset & Confidence'
    WHEN REGEXP_CONTAINS(LOWER(tf.latest_typeform_core_struggle), r'content|social|instagram|tiktok|ads|marketing')
      THEN 'Marketing Execution'
    ELSE 'Other / Unclassified'
  END AS typeform_primary_obstacle_stub,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM ghl_base g
LEFT JOIN conv_latest conv
  ON conv.location_id = g.location_id
 AND conv.contact_id = g.contact_id
LEFT JOIN setter_identity s
  ON s.ghl_user_id_norm = LOWER(TRIM(COALESCE(g.assigned_to_user_id, conv.assigned_to_user_id)))
LEFT JOIN cal_agg cal
  ON cal.location_id = g.location_id
 AND cal.contact_id = g.contact_id
LEFT JOIN opp
  ON opp.location_id = g.location_id
 AND opp.contact_id = g.contact_id
LEFT JOIN fb_by_canon fb
  ON fb.email_canon = g.email_canon
LEFT JOIN typeform_agg tf
  ON tf.location_id = g.location_id
 AND tf.contact_id = g.contact_id
;

-- Rep activity fact (tasks + notes) with rep identity enrichment.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_ghl_rep_activity` AS
WITH bridge_users AS (
  SELECT
    LOWER(TRIM(ghl_user_id)) AS ghl_user_id_norm,
    MAX_BY(canonical_name, source_priority) AS canonical_name,
    MAX_BY(canonical_team, source_priority) AS canonical_team,
    MAX_BY(canonical_role, source_priority) AS canonical_role,
    MAX_BY(identity_source, source_priority) AS identity_source
  FROM (
    SELECT
      ghl_user_id,
      canonical_name,
      canonical_team,
      canonical_role,
      identity_source,
      CASE identity_source
        WHEN 'manual_override' THEN 5
        WHEN 'ghl_users_api' THEN 4
        WHEN 'event_payload' THEN 3
        WHEN 'id_only' THEN 2
        ELSE 1
      END AS source_priority
    FROM `project-41542e21-470f-4589-96d.Marts.bridge_setter_identity`
    WHERE NULLIF(TRIM(ghl_user_id), '') IS NOT NULL
  )
  GROUP BY 1
),
task_events AS (
  SELECT
    'task' AS activity_type,
    location_id,
    task_id AS activity_id,
    contact_id,
    owner_id AS ghl_user_id,
    LOWER(TRIM(task_status)) AS activity_status,
    (completed_at IS NOT NULL OR LOWER(TRIM(task_status)) = 'completed') AS is_completed,
    COALESCE(completed_at, due_at, event_ts, updated_at_ts, ingested_at) AS activity_ts,
    event_ts,
    updated_at_ts,
    ingested_at,
    payload_json
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_tasks`
),
note_events AS (
  SELECT
    'note' AS activity_type,
    location_id,
    note_id AS activity_id,
    contact_id,
    author_user_id AS ghl_user_id,
    'posted' AS activity_status,
    FALSE AS is_completed,
    COALESCE(note_created_at, event_ts, updated_at_ts, ingested_at) AS activity_ts,
    event_ts,
    updated_at_ts,
    ingested_at,
    payload_json
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_notes`
),
events AS (
  SELECT * FROM task_events
  UNION ALL
  SELECT * FROM note_events
)
SELECT
  CONCAT(
    e.location_id,
    '|',
    e.activity_type,
    '|',
    COALESCE(
      e.activity_id,
      CONCAT('ts:', CAST(UNIX_MICROS(COALESCE(e.activity_ts, e.event_ts, e.updated_at_ts, e.ingested_at)) AS STRING))
    )
  ) AS rep_activity_key,
  e.activity_type,
  e.location_id,
  e.activity_id,
  e.contact_id,
  e.ghl_user_id,
  e.activity_status,
  e.is_completed,
  e.activity_ts,
  b.canonical_name AS rep_name,
  b.canonical_team AS team_name,
  b.canonical_role AS rep_role,
  b.identity_source AS rep_identity_source,
  e.event_ts,
  e.updated_at_ts,
  e.ingested_at,
  e.payload_json,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM events e
LEFT JOIN bridge_users b
  ON b.ghl_user_id_norm = LOWER(TRIM(e.ghl_user_id))
;

-- Opportunity attribution helper with setter/closer dual ownership derived from stage snapshots.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_ghl_opportunities_attributed` AS
WITH bridge_users AS (
  SELECT
    LOWER(TRIM(ghl_user_id)) AS ghl_user_id_norm,
    MAX_BY(canonical_name, source_priority) AS canonical_name,
    MAX_BY(canonical_team, source_priority) AS canonical_team,
    MAX_BY(canonical_role, source_priority) AS canonical_role
  FROM (
    SELECT
      ghl_user_id,
      canonical_name,
      canonical_team,
      canonical_role,
      CASE identity_source
        WHEN 'manual_override' THEN 5
        WHEN 'ghl_users_api' THEN 4
        WHEN 'event_payload' THEN 3
        WHEN 'id_only' THEN 2
        ELSE 1
      END AS source_priority
    FROM `project-41542e21-470f-4589-96d.Marts.bridge_setter_identity`
    WHERE NULLIF(TRIM(ghl_user_id), '') IS NOT NULL
  )
  GROUP BY 1
),
assignment_history AS (
  SELECT
    location_id,
    opportunity_id,
    ARRAY_AGG(NULLIF(TRIM(assigned_to_user_id), '') IGNORE NULLS ORDER BY snapshot_date, snapshotted_at)[SAFE_OFFSET(0)] AS first_assigned_user_id,
    ARRAY_AGG(NULLIF(TRIM(assigned_to_user_id), '') IGNORE NULLS ORDER BY snapshot_date DESC, snapshotted_at DESC)[SAFE_OFFSET(0)] AS last_assigned_user_id,
    MIN(snapshot_date) AS first_snapshot_date,
    MAX(snapshot_date) AS last_snapshot_date
  FROM `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`
  GROUP BY 1, 2
)
SELECT
  o.*,
  COALESCE(h.first_assigned_user_id, o.assigned_to_user_id) AS setter_user_id,
  COALESCE(h.last_assigned_user_id, o.assigned_to_user_id) AS closer_user_id,
  s_setter.canonical_name AS setter_name,
  s_setter.canonical_team AS setter_team_name,
  s_setter.canonical_role AS setter_role,
  s_closer.canonical_name AS closer_name,
  s_closer.canonical_team AS closer_team_name,
  s_closer.canonical_role AS closer_role,
  s_owner.canonical_name AS rep_name,
  s_owner.canonical_team AS team_name,
  s_owner.canonical_role AS rep_role,
  CASE
    WHEN h.opportunity_id IS NOT NULL THEN 'pipeline_stage_snapshots'
    ELSE 'current_owner_fallback'
  END AS assignment_history_source,
  h.first_snapshot_date AS assignment_first_snapshot_date,
  h.last_snapshot_date AS assignment_last_snapshot_date
FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
LEFT JOIN assignment_history h
  ON h.location_id = o.location_id
 AND h.opportunity_id = o.opportunity_id
LEFT JOIN bridge_users s_owner
  ON s_owner.ghl_user_id_norm = LOWER(TRIM(o.assigned_to_user_id))
LEFT JOIN bridge_users s_setter
  ON s_setter.ghl_user_id_norm = LOWER(TRIM(COALESCE(h.first_assigned_user_id, o.assigned_to_user_id)))
LEFT JOIN bridge_users s_closer
  ON s_closer.ghl_user_id_norm = LOWER(TRIM(COALESCE(h.last_assigned_user_id, o.assigned_to_user_id)))
;

-- Historical point-in-time opportunity attribution (SCD2-aware team/role assignment).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_ghl_opportunities_historical_attribution` AS
WITH bridge_users AS (
  SELECT
    LOWER(TRIM(ghl_user_id)) AS ghl_user_id_norm,
    MAX_BY(canonical_name, source_priority) AS canonical_name,
    MAX_BY(canonical_team, source_priority) AS canonical_team,
    MAX_BY(canonical_role, source_priority) AS canonical_role
  FROM (
    SELECT
      ghl_user_id,
      canonical_name,
      canonical_team,
      canonical_role,
      CASE identity_source
        WHEN 'manual_override' THEN 5
        WHEN 'ghl_users_api' THEN 4
        WHEN 'event_payload' THEN 3
        WHEN 'id_only' THEN 2
        ELSE 1
      END AS source_priority
    FROM `project-41542e21-470f-4589-96d.Marts.bridge_setter_identity`
    WHERE NULLIF(TRIM(ghl_user_id), '') IS NOT NULL
  )
  GROUP BY 1
),
opportunities AS (
  SELECT
    o.*,
    COALESCE(o.updated_at_ts, o.event_ts, o.ingested_at) AS attribution_event_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
),
historical_owner AS (
  SELECT
    o.location_id,
    o.opportunity_id,
    o.attribution_event_ts,
    u.name AS historical_rep_name,
    u.team AS historical_team_name,
    u.role AS historical_rep_role,
    u.is_active AS historical_rep_is_active,
    u.valid_from AS historical_valid_from,
    u.valid_to AS historical_valid_to,
    ROW_NUMBER() OVER (
      PARTITION BY o.location_id, o.opportunity_id
      ORDER BY u.valid_from DESC, u.updated_at DESC
    ) AS rn
  FROM opportunities o
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_users_scd2` u
    ON LOWER(TRIM(u.ghl_user_id)) = LOWER(TRIM(o.assigned_to_user_id))
   AND o.attribution_event_ts >= u.valid_from
   AND o.attribution_event_ts < u.valid_to
)
SELECT
  o.*,
  COALESCE(h.historical_rep_name, b.canonical_name) AS rep_name,
  COALESCE(h.historical_team_name, b.canonical_team) AS team_name,
  COALESCE(h.historical_rep_role, b.canonical_role) AS rep_role,
  h.historical_rep_is_active,
  h.historical_valid_from,
  h.historical_valid_to,
  CASE
    WHEN h.historical_valid_from IS NOT NULL THEN 'scd2_point_in_time'
    WHEN b.ghl_user_id_norm IS NOT NULL THEN 'current_bridge_fallback'
    ELSE 'unresolved'
  END AS attribution_source,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM opportunities o
LEFT JOIN historical_owner h
  ON h.location_id = o.location_id
 AND h.opportunity_id = o.opportunity_id
 AND h.attribution_event_ts = o.attribution_event_ts
 AND h.rn = 1
LEFT JOIN bridge_users b
  ON b.ghl_user_id_norm = LOWER(TRIM(o.assigned_to_user_id))
;

-- Identity bridge: Fanbasis customer -> golden contact with deterministic matching priority.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment` AS
WITH fb_identity AS (
  SELECT
    customer_id,
    email AS customer_email,
    phone AS customer_phone,
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
    REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', '') AS phone_digits,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers`
),
gc_identity AS (
  SELECT
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    gc.email_norm,
    gc.email_canon,
    REGEXP_REPLACE(IFNULL(gc.phone, ''), r'[^0-9]', '') AS phone_digits,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(gc.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(gc.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
),
candidate_matches AS (
  SELECT
    fb.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'email_exact' AS match_method,
    1 AS method_priority,
    CAST(1.00 AS NUMERIC) AS match_confidence
  FROM fb_identity fb
  JOIN gc_identity gc
    ON gc.email_norm = fb.email_norm
   AND fb.email_norm IS NOT NULL

  UNION ALL

  SELECT
    fb.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'email_canonical' AS match_method,
    2 AS method_priority,
    CAST(0.90 AS NUMERIC) AS match_confidence
  FROM fb_identity fb
  JOIN gc_identity gc
    ON gc.email_canon = fb.email_canon
   AND fb.email_canon IS NOT NULL

  UNION ALL

  SELECT
    fb.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'phone_last10' AS match_method,
    3 AS method_priority,
    CAST(0.70 AS NUMERIC) AS match_confidence
  FROM fb_identity fb
  JOIN gc_identity gc
    ON gc.phone_last10 = fb.phone_last10
   AND fb.phone_last10 IS NOT NULL
),
prioritized AS (
  SELECT
    c.*,
    MIN(method_priority) OVER (PARTITION BY customer_id) AS best_priority
  FROM candidate_matches c
),
best_candidates AS (
  SELECT
    p.*,
    COUNT(*) OVER (PARTITION BY customer_id, method_priority) AS method_candidate_count,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY method_priority, COALESCE(ghl_last_seen_ts, TIMESTAMP('1970-01-01 00:00:00 UTC')) DESC, golden_contact_key
    ) AS rn
  FROM prioritized p
  WHERE method_priority = best_priority
)
SELECT
  fb.customer_id,
  fb.customer_email,
  fb.customer_phone,
  fb.email_norm AS customer_email_norm,
  fb.email_canon AS customer_email_canon,
  fb.phone_digits AS customer_phone_digits,
  fb.phone_last10 AS customer_phone_last10,
  b.golden_contact_key,
  b.location_id AS matched_location_id,
  b.ghl_contact_id AS matched_ghl_contact_id,
  b.campaign_reporting AS attributed_campaign_snapshot,
  b.campaign_reporting_first_touch AS attributed_campaign_snapshot_first_touch,
  b.match_method,
  b.match_confidence,
  b.method_candidate_count AS method_candidate_count,
  CASE
    WHEN b.customer_id IS NULL THEN 'no_candidate'
    WHEN b.method_candidate_count > 1 THEN 'ambiguous'
    ELSE 'matched'
  END AS bridge_status,
  CURRENT_TIMESTAMP() AS bridge_refreshed_at
FROM fb_identity fb
LEFT JOIN best_candidates b
  ON b.customer_id = fb.customer_id
 AND b.rn = 1
;

-- Fanbasis payment lines attributed through identity bridge (email exact/canonical then phone).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line` AS
WITH pay AS (
  SELECT
    t.transaction_id,
    t.event_ts,
    t.amount,
    t.fee_amount,
    t.net_amount,
    t.customer_id,
    t.product_id,
    t.service_id,
    t.payment_type,
    t.ingested_at,
    cu.email AS customer_email,
    cu.phone AS customer_phone,
    CASE
      WHEN cu.email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(cu.email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(cu.email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(cu.email))
    END AS payer_email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(cu.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(cu.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS payer_phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions` t
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers` cu
    ON cu.customer_id = t.customer_id
)
SELECT
  p.transaction_id,
  p.event_ts,
  p.amount,
  p.fee_amount,
  p.net_amount,
  p.customer_id,
  p.product_id,
  p.service_id,
  p.payment_type,
  p.ingested_at,
  p.customer_email,
  p.customer_phone,
  p.payer_email_canon,
  p.payer_phone_last10,
  b.golden_contact_key,
  b.matched_location_id,
  b.matched_ghl_contact_id,
  b.attributed_campaign_snapshot,
  b.attributed_campaign_snapshot_first_touch,
  b.match_method,
  b.match_confidence,
  b.method_candidate_count,
  b.bridge_status,
  CASE
    WHEN b.bridge_status = 'matched' THEN 'matched'
    WHEN b.bridge_status = 'ambiguous' THEN 'ambiguous_multi_candidate'
    WHEN b.golden_contact_key IS NULL
         AND (p.payer_email_canon IS NOT NULL OR p.payer_phone_last10 IS NOT NULL)
      THEN 'direct_sale_no_crm_contact'
    ELSE 'no_ghl_contact_match_on_identity'
  END AS match_status,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM pay p
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment` b
  ON b.customer_id = p.customer_id
;

-- Unified payment lines across APIs (Fanbasis + Stripe) in one reporting fact.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` AS
WITH gc_identity AS (
  SELECT
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    gc.email_norm,
    gc.email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(gc.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(gc.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
),
stripe_identity AS (
  SELECT
    sc.customer_id,
    sc.email AS customer_email,
    sc.phone AS customer_phone,
    LOWER(TRIM(sc.email)) AS email_norm,
    CASE
      WHEN sc.email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(sc.email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(sc.email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(sc.email))
    END AS email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(sc.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(sc.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM `project-41542e21-470f-4589-96d.Core.dim_stripe_customers` sc
),
stripe_candidates AS (
  SELECT
    si.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'email_exact' AS match_method,
    1 AS method_priority,
    CAST(1.00 AS NUMERIC) AS match_confidence
  FROM stripe_identity si
  JOIN gc_identity gc
    ON gc.email_norm = si.email_norm
   AND si.email_norm IS NOT NULL

  UNION ALL

  SELECT
    si.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'email_canonical' AS match_method,
    2 AS method_priority,
    CAST(0.90 AS NUMERIC) AS match_confidence
  FROM stripe_identity si
  JOIN gc_identity gc
    ON gc.email_canon = si.email_canon
   AND si.email_canon IS NOT NULL

  UNION ALL

  SELECT
    si.customer_id,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.campaign_reporting,
    gc.campaign_reporting_first_touch,
    gc.ghl_last_seen_ts,
    'phone_last10' AS match_method,
    3 AS method_priority,
    CAST(0.70 AS NUMERIC) AS match_confidence
  FROM stripe_identity si
  JOIN gc_identity gc
    ON gc.phone_last10 = si.phone_last10
   AND si.phone_last10 IS NOT NULL
),
stripe_prioritized AS (
  SELECT
    c.*,
    MIN(method_priority) OVER (PARTITION BY customer_id) AS best_priority
  FROM stripe_candidates c
),
stripe_best AS (
  SELECT
    p.*,
    COUNT(*) OVER (PARTITION BY customer_id, method_priority) AS method_candidate_count,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY method_priority, COALESCE(ghl_last_seen_ts, TIMESTAMP('1970-01-01 00:00:00 UTC')) DESC, golden_contact_key
    ) AS rn
  FROM stripe_prioritized p
  WHERE method_priority = best_priority
),
stripe_bridge AS (
  SELECT
    si.customer_id,
    si.customer_email,
    si.customer_phone,
    si.email_canon AS payer_email_canon,
    si.phone_last10 AS payer_phone_last10,
    sb.golden_contact_key,
    sb.location_id AS matched_location_id,
    sb.ghl_contact_id AS matched_ghl_contact_id,
    sb.campaign_reporting AS attributed_campaign_snapshot,
    sb.campaign_reporting_first_touch AS attributed_campaign_snapshot_first_touch,
    sb.match_method,
    sb.match_confidence,
    sb.method_candidate_count,
    CASE
      WHEN sb.customer_id IS NULL THEN 'no_candidate'
      WHEN sb.method_candidate_count > 1 THEN 'ambiguous'
      ELSE 'matched'
    END AS bridge_status
  FROM stripe_identity si
  LEFT JOIN stripe_best sb
    ON sb.customer_id = si.customer_id
   AND sb.rn = 1
),
-- Pass 4: billing_details.email from the charge itself, for Stripe Link / guest checkout
-- and as a fallback when the Stripe customer object exists but does not carry usable identity.
billing_email_charges AS (
  WITH stripe_billing AS (
    SELECT
      sp.payment_id,
      COALESCE(
        NULLIF(TRIM(JSON_VALUE(sp.payload_json, '$.billing_details.email')), ''),
        NULLIF(TRIM(JSON_VALUE(sp.payload_json, '$.receipt_email')), '')
      ) AS billing_email
    FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments` sp
  )
  SELECT
    payment_id,
    billing_email,
    CASE
      WHEN billing_email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(billing_email)), '@')[SAFE_OFFSET(1)]
           IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(billing_email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.', ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(billing_email))
    END AS billing_email_canon
  FROM stripe_billing
  WHERE billing_email IS NOT NULL
),
billing_email_bridge AS (
  SELECT
    be.payment_id,
    be.billing_email AS customer_email,
    be.billing_email_canon AS payer_email_canon,
    gc.golden_contact_key,
    gc.location_id AS matched_location_id,
    gc.ghl_contact_id AS matched_ghl_contact_id,
    gc.campaign_reporting AS attributed_campaign_snapshot,
    gc.campaign_reporting_first_touch AS attributed_campaign_snapshot_first_touch,
    gc.ghl_last_seen_ts,
    'billing_email_direct' AS match_method,
    CAST(0.85 AS NUMERIC) AS match_confidence,
    COUNT(*) OVER (PARTITION BY be.payment_id) AS method_candidate_count
  FROM billing_email_charges be
  JOIN gc_identity gc
    ON gc.email_canon = be.billing_email_canon
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY be.payment_id
    ORDER BY COALESCE(gc.ghl_last_seen_ts, TIMESTAMP('1970-01-01 00:00:00 UTC')) DESC, gc.golden_contact_key
  ) = 1
),
fanbasis_rows AS (
  SELECT
    'fanbasis' AS source_system,
    CAST(fb.transaction_id AS STRING) AS payment_id,
    fb.event_ts,
    SAFE_CAST(fb.amount AS NUMERIC) AS amount,
    SAFE_CAST(fb.fee_amount AS NUMERIC) AS fee_amount,
    SAFE_CAST(fb.net_amount AS NUMERIC) AS net_amount,
    CAST(NULL AS STRING) AS currency,
    CAST(NULL AS STRING) AS payment_status,
    CAST(NULL AS STRING) AS payment_description,
    fb.ingested_at,
    CAST(fb.customer_id AS STRING) AS source_customer_id,
    fb.customer_email,
    fb.customer_phone,
    fb.payer_email_canon,
    fb.payer_phone_last10,
    fb.golden_contact_key,
    fb.matched_location_id,
    fb.matched_ghl_contact_id,
    fb.attributed_campaign_snapshot,
    fb.attributed_campaign_snapshot_first_touch,
    fb.match_method,
    fb.match_confidence,
    fb.method_candidate_count,
    fb.bridge_status,
    fb.match_status
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line` fb
),
stripe_rows AS (
  SELECT
    'stripe' AS source_system,
    CAST(sp.payment_id AS STRING) AS payment_id,
    sp.event_ts,
    SAFE_CAST(COALESCE(sp.amount_captured, sp.amount) AS NUMERIC) AS amount,
    CAST(NULL AS NUMERIC) AS fee_amount,
    SAFE_CAST(COALESCE(sp.amount_captured, sp.amount, 0) - COALESCE(sp.amount_refunded, 0) AS NUMERIC) AS net_amount,
    sp.currency,
    sp.status AS payment_status,
    sp.description AS payment_description,
    sp.ingested_at,
    CAST(sp.customer_id AS STRING) AS source_customer_id,
    COALESCE(sb.customer_email, beb.customer_email) AS customer_email,
    sb.customer_phone,
    COALESCE(sb.payer_email_canon, beb.payer_email_canon) AS payer_email_canon,
    sb.payer_phone_last10,
    COALESCE(sb.golden_contact_key,  beb.golden_contact_key)  AS golden_contact_key,
    COALESCE(sb.matched_location_id, beb.matched_location_id) AS matched_location_id,
    COALESCE(sb.matched_ghl_contact_id, beb.matched_ghl_contact_id) AS matched_ghl_contact_id,
    COALESCE(sb.attributed_campaign_snapshot, beb.attributed_campaign_snapshot) AS attributed_campaign_snapshot,
    COALESCE(sb.attributed_campaign_snapshot_first_touch, beb.attributed_campaign_snapshot_first_touch) AS attributed_campaign_snapshot_first_touch,
    COALESCE(sb.match_method, beb.match_method) AS match_method,
    COALESCE(sb.match_confidence, beb.match_confidence) AS match_confidence,
    COALESCE(sb.method_candidate_count, beb.method_candidate_count) AS method_candidate_count,
    CASE
      WHEN sb.bridge_status IN ('matched', 'ambiguous') THEN sb.bridge_status
      WHEN beb.payment_id IS NOT NULL AND beb.method_candidate_count = 1 THEN 'matched'
      WHEN beb.payment_id IS NOT NULL THEN 'ambiguous'
      ELSE COALESCE(sb.bridge_status, 'no_candidate')
    END AS bridge_status,
    CASE
      WHEN sb.bridge_status = 'matched'
           OR ((sb.bridge_status IS NULL OR sb.bridge_status = 'no_candidate')
               AND beb.method_candidate_count = 1) THEN 'matched'
      WHEN sb.bridge_status = 'ambiguous'
           OR ((sb.bridge_status IS NULL OR sb.bridge_status = 'no_candidate')
               AND beb.method_candidate_count > 1) THEN 'ambiguous_multi_candidate'
      WHEN beb.customer_email IS NOT NULL
           AND beb.golden_contact_key IS NULL
           AND (sb.bridge_status IS NULL OR sb.bridge_status = 'no_candidate') THEN 'direct_sale_no_crm_contact'
      ELSE 'no_ghl_contact_match_on_identity'
    END AS match_status
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments` sp
  LEFT JOIN stripe_bridge sb
    ON sb.customer_id = sp.customer_id
  LEFT JOIN billing_email_bridge beb
    ON beb.payment_id = sp.payment_id
  WHERE COALESCE(sp.paid, FALSE)
     OR LOWER(COALESCE(sp.status, '')) IN ('succeeded', 'paid')
)
SELECT
  source_system,
  payment_id,
  event_ts,
  amount,
  fee_amount,
  net_amount,
  currency,
  payment_status,
  payment_description,
  ingested_at,
  source_customer_id,
  customer_email,
  customer_phone,
  payer_email_canon,
  payer_phone_last10,
  golden_contact_key,
  matched_location_id,
  matched_ghl_contact_id,
  attributed_campaign_snapshot,
  attributed_campaign_snapshot_first_touch,
  match_method,
  match_confidence,
  method_candidate_count,
  bridge_status,
  match_status,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM fanbasis_rows
UNION ALL
SELECT
  source_system,
  payment_id,
  event_ts,
  amount,
  fee_amount,
  net_amount,
  currency,
  payment_status,
  payment_description,
  ingested_at,
  source_customer_id,
  customer_email,
  customer_phone,
  payer_email_canon,
  payer_phone_last10,
  golden_contact_key,
  matched_location_id,
  matched_ghl_contact_id,
  attributed_campaign_snapshot,
  attributed_campaign_snapshot_first_touch,
  match_method,
  match_confidence,
  method_candidate_count,
  bridge_status,
  match_status,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM stripe_rows
;

-- Closer dimension: one row per sales rep, seeded entirely from Fathom recorded_by.
-- No GHL API needed — Fathom has real names and emails.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.dim_closers` AS
SELECT
  f.recorded_by_email                                     AS closer_email,
  ANY_VALUE(f.recorded_by_name)                           AS closer_name,
  COUNT(DISTINCT f.call_id)                               AS total_calls,
  COUNTIF(f.is_revenue_relevant)                          AS revenue_relevant_calls,
  COUNT(DISTINCT CASE
    WHEN b.contact_id IS NOT NULL THEN b.contact_id
  END)                                                    AS distinct_contacts_reached,
  MIN(f.event_ts)                                         AS first_call_ts,
  MAX(f.event_ts)                                         AS last_call_ts,
  CURRENT_TIMESTAMP()                                     AS mart_refreshed_at
FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` f
LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts_scored` b
  ON b.call_id = f.call_id
WHERE f.recorded_by_email IS NOT NULL
GROUP BY f.recorded_by_email
;

-- Contact → closer bridge: deterministic attribution of the sales rep who ran the call.
-- Derived from Fathom calendar_invitees (external email) → GHL contact match.
-- Uses the most recent high/medium confidence revenue-relevant call per contact.
-- multi_rep_contact = TRUE when more than one rep appears across all calls for this contact.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.bridge_contact_closer` AS
WITH call_contacts AS (
  SELECT
    s.contact_id,
    s.location_id,
    s.call_id,
    s.call_event_ts,
    s.contact_match_confidence,
    s.contact_match_score,
    f.recorded_by_email                         AS closer_email,
    f.recorded_by_name                          AS closer_name,
    f.share_url                                 AS call_share_url
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts_scored` s
  JOIN `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` f
    ON f.call_id = s.call_id
  WHERE f.is_revenue_relevant = TRUE
    AND s.contact_id IS NOT NULL
    AND s.contact_match_confidence IN ('high', 'medium')
    AND f.recorded_by_email IS NOT NULL
    AND NOT COALESCE(f.is_internal_only, FALSE)
),
contact_rep_counts AS (
  SELECT
    contact_id,
    location_id,
    COUNT(DISTINCT closer_email)    AS distinct_closer_count,
    COUNT(*)                        AS fathom_call_count
  FROM call_contacts
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    cc.*,
    ROW_NUMBER() OVER (
      PARTITION BY cc.contact_id, cc.location_id
      ORDER BY
        CASE cc.contact_match_confidence
          WHEN 'high'   THEN 1
          WHEN 'medium' THEN 2
          ELSE 3
        END,
        cc.call_event_ts DESC
    ) AS rn
  FROM call_contacts cc
)
SELECT
  r.contact_id,
  r.location_id,
  r.closer_email,
  r.closer_name,
  r.call_id                                             AS closer_call_id,
  r.call_event_ts                                       AS closer_call_ts,
  r.call_share_url                                      AS closer_call_url,
  r.contact_match_confidence                            AS closer_confidence,
  COALESCE(crc.distinct_closer_count, 1) > 1            AS multi_rep_contact,
  COALESCE(crc.distinct_closer_count, 1)                AS distinct_closer_count,
  COALESCE(crc.fathom_call_count, 1)                    AS fathom_call_count,
  CURRENT_TIMESTAMP()                                   AS mart_refreshed_at
FROM ranked r
LEFT JOIN contact_rep_counts crc
  ON crc.contact_id = r.contact_id
 AND crc.location_id = r.location_id
WHERE r.rn = 1
;

-- Master Lead wide reporting view (sheet-shaped lead grain with explicit rule metadata).
-- Thin layer on top of dim_golden_contact + event enrichments (does not duplicate base truth tables).
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide` AS
WITH base_contacts AS (
  SELECT
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.email,
    gc.email_norm,
    gc.email_canon,
    gc.first_name,
    gc.last_name,
    gc.full_name,
    gc.phone,
    gc.country,
    gc.ghl_source,
    gc.utm_source,
    gc.utm_medium,
    gc.utm_campaign,
    gc.utm_content,
    gc.lead_magnet_history,
    gc.ghl_date_added_ts,
    gc.ghl_first_seen_ts,
    gc.ghl_last_seen_ts,
    gc.campaign_reporting,
    gc.attribution_gap_reason,
    gc.meetings_booked_cnt,
    gc.meetings_showed_cnt,
    gc.first_meeting_start_ts,
    gc.last_meeting_start_ts,
    gc.has_calendly_booking,
    gc.opportunity_count,
    gc.opportunities_total_amount,
    gc.pipeline_stage_label,
    gc.fanbasis_lifetime_net,
    gc.fanbasis_transaction_count,
    gc.fanbasis_customer_id_count,
    gc.has_fanbasis_payment,
    gc.typeform_responses_count,
    gc.latest_typeform_form_title,
    gc.latest_typeform_response_ts,
    gc.latest_typeform_landed_at,
    gc.latest_typeform_completion_seconds,
    gc.latest_typeform_ending_id,
    gc.latest_typeform_ending_ref,
    gc.latest_typeform_form_score,
    gc.latest_typeform_hidden_fields_json,
    gc.latest_typeform_variables_json,
    gc.latest_typeform_answers_json,
    gc.latest_typeform_age_bracket,
    gc.latest_typeform_business_stage,
    gc.latest_typeform_investment_range,
    gc.latest_typeform_core_struggle,
    gc.typeform_primary_goal_stub,
    gc.typeform_primary_obstacle_stub,
    gc.mart_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
),
touch_events_raw AS (
  SELECT
    a.location_id,
    a.contact_id AS ghl_contact_id,
    COALESCE(a.submission_event_ts, a.submission_updated_at_ts, a.ingested_at) AS event_ts,
    COALESCE(NULLIF(TRIM(a.utm_source), ''), NULLIF(TRIM(a.source), '')) AS touch_source,
    NULLIF(TRIM(a.utm_medium), '') AS touch_medium,
    NULLIF(TRIM(a.utm_campaign), '') AS touch_campaign,
    'ghl_attribution' AS touch_source_system
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_attribution` a
  WHERE a.contact_id IS NOT NULL
    AND COALESCE(NULLIF(TRIM(a.utm_source), ''), NULLIF(TRIM(a.source), ''), NULLIF(TRIM(a.utm_medium), ''), NULLIF(TRIM(a.utm_campaign), '')) IS NOT NULL

  UNION ALL

  SELECT
    b.location_id,
    b.contact_id AS ghl_contact_id,
    COALESCE(i.invitee_created_at, i.scheduled_start_time, i.event_ts, i.ingested_at) AS event_ts,
    NULLIF(TRIM(i.utm_source), '') AS touch_source,
    NULLIF(TRIM(i.utm_medium), '') AS touch_medium,
    NULLIF(TRIM(i.utm_campaign), '') AS touch_campaign,
    'calendly_invitee' AS touch_source_system
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND COALESCE(NULLIF(TRIM(i.utm_source), ''), NULLIF(TRIM(i.utm_medium), ''), NULLIF(TRIM(i.utm_campaign), '')) IS NOT NULL
),
-- UTM hygiene guardrail: reject sentence-like or malformed values before attribution precedence.
touch_events AS (
  SELECT
    location_id,
    ghl_contact_id,
    event_ts,
    touch_source_clean AS touch_source,
    touch_medium_clean AS touch_medium,
    touch_campaign_clean AS touch_campaign,
    touch_source_system
  FROM (
    SELECT
      location_id,
      ghl_contact_id,
      event_ts,
      CASE
        WHEN touch_source IS NULL OR TRIM(touch_source) = '' THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_source), r'^[0-9]+$') THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_source), r'^\+') THEN NULL
        WHEN LENGTH(TRIM(touch_source)) > 50 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_source), r'\?') THEN NULL
        WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(touch_source), r'[a-z0-9]+')) > 3 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_source), r'\b(this|is|my|the|and)\b') THEN NULL
        ELSE TRIM(touch_source)
      END AS touch_source_clean,
      CASE
        WHEN touch_medium IS NULL OR TRIM(touch_medium) = '' THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_medium), r'^[0-9]+$') THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_medium), r'^\+') THEN NULL
        WHEN LENGTH(TRIM(touch_medium)) > 50 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_medium), r'\?') THEN NULL
        WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(touch_medium), r'[a-z0-9]+')) > 3 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_medium), r'\b(this|is|my|the|and)\b') THEN NULL
        ELSE TRIM(touch_medium)
      END AS touch_medium_clean,
      CASE
        WHEN touch_campaign IS NULL OR TRIM(touch_campaign) = '' THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_campaign), r'^[0-9]+$') THEN NULL
        WHEN REGEXP_CONTAINS(TRIM(touch_campaign), r'^\+') THEN NULL
        WHEN LENGTH(TRIM(touch_campaign)) > 50 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_campaign), r'\?') THEN NULL
        WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(touch_campaign), r'[a-z0-9]+')) > 3 THEN NULL
        WHEN REGEXP_CONTAINS(LOWER(touch_campaign), r'\b(this|is|my|the|and)\b') THEN NULL
        ELSE TRIM(touch_campaign)
      END AS touch_campaign_clean,
      touch_source_system
    FROM touch_events_raw
  )
  WHERE event_ts IS NOT NULL
    AND COALESCE(touch_source_clean, touch_medium_clean, touch_campaign_clean) IS NOT NULL
),
first_last_touch AS (
  SELECT
    location_id,
    ghl_contact_id,
    ARRAY_AGG(
      STRUCT(
        event_ts,
        touch_source,
        touch_medium,
        touch_campaign,
        touch_source_system
      )
      ORDER BY event_ts ASC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS first_touch,
    ARRAY_AGG(
      STRUCT(
        event_ts,
        touch_source,
        touch_medium,
        touch_campaign,
        touch_source_system
      )
      ORDER BY event_ts DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS last_touch,
    COUNT(*) AS touch_event_count
  FROM touch_events
  WHERE event_ts IS NOT NULL
  GROUP BY 1, 2
),
snapshot_attribution AS (
  SELECT
    location_id,
    ghl_contact_id,
    CASE
      WHEN COALESCE(NULLIF(TRIM(utm_source), ''), NULLIF(TRIM(ghl_source), '')) IS NULL THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, ''))), r'^[0-9]+$') THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, ''))), r'^\+') THEN NULL
      WHEN LENGTH(TRIM(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, '')))) > 50 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, ''))), r'\?') THEN NULL
      WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, ''))), r'[a-z0-9]+')) > 3 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, ''))), r'\b(this|is|my|the|and)\b') THEN NULL
      ELSE TRIM(COALESCE(NULLIF(utm_source, ''), NULLIF(ghl_source, '')))
    END AS snapshot_source,
    CASE
      WHEN utm_medium IS NULL OR TRIM(utm_medium) = '' THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(utm_medium), r'^[0-9]+$') THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(utm_medium), r'^\+') THEN NULL
      WHEN LENGTH(TRIM(utm_medium)) > 50 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(utm_medium), r'\?') THEN NULL
      WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(utm_medium), r'[a-z0-9]+')) > 3 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(utm_medium), r'\b(this|is|my|the|and)\b') THEN NULL
      ELSE TRIM(utm_medium)
    END AS snapshot_medium,
    CASE
      WHEN utm_campaign IS NULL OR TRIM(utm_campaign) = '' THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(utm_campaign), r'^[0-9]+$') THEN NULL
      WHEN REGEXP_CONTAINS(TRIM(utm_campaign), r'^\+') THEN NULL
      WHEN LENGTH(TRIM(utm_campaign)) > 50 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(utm_campaign), r'\?') THEN NULL
      WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL(LOWER(utm_campaign), r'[a-z0-9]+')) > 3 THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(utm_campaign), r'\b(this|is|my|the|and)\b') THEN NULL
      ELSE TRIM(utm_campaign)
    END AS snapshot_campaign
  FROM base_contacts
),
payment_rollup AS (
  SELECT
    COALESCE(p.golden_contact_key, 'ORPHAN_REVENUE') AS golden_contact_key,
    COUNT(DISTINCT CONCAT(p.source_system, '|', p.payment_id)) AS total_payment_count,
    SUM(SAFE_CAST(p.net_amount AS NUMERIC)) AS total_net_revenue,
    SUM(CASE WHEN p.source_system = 'fanbasis' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS fanbasis_net_revenue,
    SUM(CASE WHEN p.source_system = 'stripe' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS stripe_net_revenue,
    MIN(p.event_ts) AS first_payment_ts,
    MAX(p.event_ts) AS latest_payment_ts,
    MAX_BY(p.payment_status, COALESCE(p.event_ts, TIMESTAMP('1970-01-01 00:00:00 UTC'))) AS latest_payment_status
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` p
  GROUP BY 1
),
application_rollup AS (
  SELECT
    f.location_id,
    f.contact_id AS ghl_contact_id,
    COUNT(*) AS application_count,
    MIN(COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at)) AS first_application_ts,
    MAX(COALESCE(f.updated_at_ts, f.event_ts, f.ingested_at)) AS latest_application_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_form_submissions` f
  WHERE f.contact_id IS NOT NULL
  GROUP BY 1, 2
),
outbound_call_rollup AS (
  SELECT
    c.location_id,
    c.contact_id AS ghl_contact_id,
    COUNT(*) AS outbound_call_count,
    MIN(COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at)) AS first_outbound_call_ts,
    MAX(COALESCE(c.call_started_at, c.updated_at_ts, c.event_ts, c.ingested_at)) AS latest_outbound_call_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  WHERE c.contact_id IS NOT NULL
  GROUP BY 1, 2
),
-- Static GHL userId → name map (same seed as dim_team_members static_ghl_user_seed).
-- Needed here because GHL opportunities payloads carry userId but never userName/email.
ghl_user_name_map AS (
  SELECT user_id, user_name FROM (
    SELECT 'leBv9MtltaKdfSijVEhb' AS user_id, 'Houssam Bentouati'  AS user_name
    UNION ALL SELECT '1D4ZUkV07gGJ25YtUolz', 'Houssam Bentouati'
    UNION ALL SELECT 'c5ujVqeYHGi1WnmlvtWu', 'Marco Branco'
    UNION ALL SELECT '7rCcXXi8tFdihhDvTTM3', 'Mitchell Naude'
    UNION ALL SELECT '9rocXim1JjeIvjSrWLSn', 'Boipelo Mashigo'
    UNION ALL SELECT 'DTtFkB0jtX1ionHhjsGR', 'Blagoj Veleski'
    UNION ALL SELECT 'ZOytPUG1jSWRNBzsJYEp', 'Hammad Ahsan'
    UNION ALL SELECT 'J4eyQWx4oFfPj08qunrS', 'Jordan Evans'
    UNION ALL SELECT 'ILX9jpFp7ycNbWgakiYR', 'Kevin Maya'
    UNION ALL SELECT 'XKcL1lmTZn8LFHiUwtn1', 'Jake Lynch'
    UNION ALL SELECT 'YyBgSVqB1wQoFj8tAe40', 'Stanley Macauley'
    UNION ALL SELECT 'BKc6beDhtuW1GFp0wI',   'Ethan Gerstenberg'
    -- Oct5Tz6ZVUaDkqXC3yHL: 292 older events, not in GHL team list — likely deleted account
  )
),
-- Owner-at-event fallback for closer attribution.
-- Uses the latest opportunity event owner payload when present.
opportunity_owner_at_event AS (
  SELECT
    o.location_id,
    o.contact_id AS ghl_contact_id,
    MAX_BY(
      NULLIF(
        TRIM(
          COALESCE(
            JSON_VALUE(o.payload_json, '$.assignedToName'),
            JSON_VALUE(o.payload_json, '$.assigned_to_name'),
            JSON_VALUE(o.payload_json, '$.ownerName'),
            JSON_VALUE(o.payload_json, '$.owner_name')
          )
        ),
        ''
      ),
      COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at_ts, o.event_ts, o.ingested_at)
    ) AS owner_at_event_name,
    MAX_BY(
      NULLIF(
        TRIM(
          COALESCE(
            JSON_VALUE(o.payload_json, '$.assignedTo'),
            JSON_VALUE(o.payload_json, '$.assigned_to'),
            JSON_VALUE(o.payload_json, '$.ownerId'),
            JSON_VALUE(o.payload_json, '$.owner_id')
          )
        ),
        ''
      ),
      COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at_ts, o.event_ts, o.ingested_at)
    ) AS owner_at_event_id,
    MAX_BY(
      COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at_ts, o.event_ts, o.ingested_at),
      COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at_ts, o.event_ts, o.ingested_at)
    ) AS owner_at_event_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
  WHERE o.contact_id IS NOT NULL
  GROUP BY 1, 2
),
-- Payment Plan / Repeat Buyer / Paid classification.
-- Uses distinct Fanbasis product IDs: same product = Payment Plan, different products = Repeat Buyer.
payment_product_rollup AS (
  SELECT
    COALESCE(p.golden_contact_key, 'ORPHAN_REVENUE') AS golden_contact_key,
    COUNT(DISTINCT CASE WHEN p.source_system = 'fanbasis' THEN t.product_id END) AS distinct_fanbasis_product_count
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` p
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions` t
    ON p.source_system = 'fanbasis'
   AND p.payment_id = CAST(t.transaction_id AS STRING)
  GROUP BY 1
),
-- Most recent Calendly booking's custom question answers (self-reported source + emotional goal value).
calendly_enrichment AS (
  SELECT
    b.location_id,
    b.contact_id AS ghl_contact_id,
    MAX_BY(
      (
        SELECT JSON_VALUE(qa, '$.answer')
        FROM UNNEST(JSON_QUERY_ARRAY(i.payload_json, '$.questions_and_answers')) AS qa
        WHERE REGEXP_CONTAINS(
          LOWER(COALESCE(JSON_VALUE(qa, '$.question'), '')),
          r'where|source|booking link|find|hear about'
        )
        LIMIT 1
      ),
      COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at)
    ) AS self_reported_source,
    MAX_BY(
      (
        SELECT JSON_VALUE(qa, '$.answer')
        FROM UNNEST(JSON_QUERY_ARRAY(i.payload_json, '$.questions_and_answers')) AS qa
        WHERE REGEXP_CONTAINS(
          LOWER(COALESCE(JSON_VALUE(qa, '$.question'), '')),
          r'goal|mean to you|achieve|motivat|important'
        )
        LIMIT 1
      ),
      COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at)
    ) AS emotional_goal_value,
    MAX_BY(i.event_name, COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at)) AS latest_call_type,
    COUNTIF(LOWER(COALESCE(i.invitee_status, i.event_status, JSON_VALUE(i.payload_json, '$.status'), '')) IN ('canceled', 'cancelled')) AS canceled_bookings_count
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
  GROUP BY 1, 2
),
-- Typeform enrichment for master lead mart.
-- Matching precedence:
-- 1) email exact
-- 2) email canonical (gmail dot/plus normalization)
-- 3) phone last10 fallback
typeform_response_identity AS (
  SELECT
    r.response_id,
    r.form_id,
    r.submitted_at,
    r.landed_at,
    r.ending_id,
    r.ending_ref,
    r.form_score,
    r.hidden_fields_json,
    r.variables_json,
    r.event_ts,
    r.ingested_at,
    r.answers_json,
    r.respondent_age_bracket,
    r.respondent_business_stage,
    r.respondent_investment_range,
    r.respondent_core_struggle,
    LOWER(TRIM(r.respondent_email)) AS email_norm,
    CASE
      WHEN r.respondent_email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(r.respondent_email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(r.respondent_email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(r.respondent_email))
    END AS email_canon,
    REGEXP_REPLACE(
      IFNULL(
        (
          SELECT
            COALESCE(
              JSON_VALUE(answer, '$.phone_number'),
              JSON_VALUE(answer, '$.text')
            )
          FROM UNNEST(JSON_QUERY_ARRAY(r.payload_json, '$.answers')) AS answer
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
    ) AS phone_digits,
    f.form_title
  FROM `project-41542e21-470f-4589-96d.Core.dim_typeform_responses` r
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_typeform_forms` f
    ON f.form_id = r.form_id
),
typeform_response_identity_norm AS (
  SELECT
    response_id,
    form_id,
    submitted_at,
    landed_at,
    ending_id,
    ending_ref,
    form_score,
    hidden_fields_json,
    variables_json,
    event_ts,
    ingested_at,
    answers_json,
    respondent_age_bracket,
    respondent_business_stage,
    respondent_investment_range,
    respondent_core_struggle,
    email_norm,
    email_canon,
    CASE
      WHEN LENGTH(phone_digits) >= 10 THEN RIGHT(phone_digits, 10)
      ELSE NULL
    END AS phone_last10,
    form_title
  FROM typeform_response_identity
),
typeform_contact_identity AS (
  SELECT
    b.golden_contact_key,
    b.location_id,
    b.ghl_contact_id,
    b.ghl_last_seen_ts,
    LOWER(TRIM(b.email)) AS email_norm,
    CASE
      WHEN b.email IS NULL THEN NULL
      WHEN SPLIT(LOWER(TRIM(b.email)), '@')[SAFE_OFFSET(1)] IN ('gmail.com', 'googlemail.com') THEN CONCAT(
        REPLACE(
          SPLIT(SPLIT(LOWER(TRIM(b.email)), '@')[SAFE_OFFSET(0)], '+')[SAFE_OFFSET(0)],
          '.',
          ''
        ),
        '@gmail.com'
      )
      ELSE LOWER(TRIM(b.email))
    END AS email_canon,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(IFNULL(b.phone, ''), r'[^0-9]', '')) >= 10
        THEN RIGHT(REGEXP_REPLACE(IFNULL(b.phone, ''), r'[^0-9]', ''), 10)
      ELSE NULL
    END AS phone_last10
  FROM base_contacts b
),
typeform_contact_candidates AS (
  SELECT
    r.response_id,
    r.form_id,
    r.submitted_at,
    r.landed_at,
    r.ending_id,
    r.ending_ref,
    r.form_score,
    r.hidden_fields_json,
    r.variables_json,
    r.event_ts,
    r.ingested_at,
    r.answers_json,
    r.respondent_age_bracket,
    r.respondent_business_stage,
    r.respondent_investment_range,
    r.respondent_core_struggle,
    r.form_title,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.ghl_last_seen_ts,
    1 AS match_priority
  FROM typeform_response_identity_norm r
  JOIN typeform_contact_identity gc
    ON gc.email_norm = r.email_norm
   AND r.email_norm IS NOT NULL

  UNION ALL

  SELECT
    r.response_id,
    r.form_id,
    r.submitted_at,
    r.landed_at,
    r.ending_id,
    r.ending_ref,
    r.form_score,
    r.hidden_fields_json,
    r.variables_json,
    r.event_ts,
    r.ingested_at,
    r.answers_json,
    r.respondent_age_bracket,
    r.respondent_business_stage,
    r.respondent_investment_range,
    r.respondent_core_struggle,
    r.form_title,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.ghl_last_seen_ts,
    2 AS match_priority
  FROM typeform_response_identity_norm r
  JOIN typeform_contact_identity gc
    ON gc.email_canon = r.email_canon
   AND r.email_canon IS NOT NULL

  UNION ALL

  SELECT
    r.response_id,
    r.form_id,
    r.submitted_at,
    r.landed_at,
    r.ending_id,
    r.ending_ref,
    r.form_score,
    r.hidden_fields_json,
    r.variables_json,
    r.event_ts,
    r.ingested_at,
    r.answers_json,
    r.respondent_age_bracket,
    r.respondent_business_stage,
    r.respondent_investment_range,
    r.respondent_core_struggle,
    r.form_title,
    gc.golden_contact_key,
    gc.location_id,
    gc.ghl_contact_id,
    gc.ghl_last_seen_ts,
    3 AS match_priority
  FROM typeform_response_identity_norm r
  JOIN typeform_contact_identity gc
    ON gc.phone_last10 = r.phone_last10
   AND r.phone_last10 IS NOT NULL
),
typeform_contact_ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.response_id
      ORDER BY
        c.match_priority,
        COALESCE(c.ghl_last_seen_ts, TIMESTAMP('1970-01-01 00:00:00 UTC')) DESC,
        c.golden_contact_key
    ) AS rn
  FROM typeform_contact_candidates c
),
typeform_enrichment AS (
  SELECT
    golden_contact_key,
    ANY_VALUE(location_id) AS location_id,
    ANY_VALUE(ghl_contact_id) AS ghl_contact_id,
    COUNT(DISTINCT response_id) AS typeform_responses_count,
    MAX_BY(form_title, ingested_at) AS latest_typeform_title,
    MAX_BY(COALESCE(submitted_at, event_ts, ingested_at), ingested_at) AS latest_typeform_response_ts,
    MAX_BY(landed_at, ingested_at) AS latest_typeform_landed_at,
    MAX_BY(
      CASE
        WHEN submitted_at IS NOT NULL AND landed_at IS NOT NULL
          THEN GREATEST(TIMESTAMP_DIFF(submitted_at, landed_at, SECOND), 0)
        ELSE NULL
      END,
      ingested_at
    ) AS latest_typeform_completion_seconds,
    MAX_BY(ending_id, ingested_at) AS latest_typeform_ending_id,
    MAX_BY(ending_ref, ingested_at) AS latest_typeform_ending_ref,
    MAX_BY(form_score, ingested_at) AS latest_typeform_form_score,
    MAX_BY(hidden_fields_json, ingested_at) AS latest_typeform_hidden_fields_json,
    MAX_BY(variables_json, ingested_at) AS latest_typeform_variables_json,
    MAX_BY(answers_json, ingested_at) AS latest_typeform_answers_json,
    MAX_BY(respondent_age_bracket, ingested_at) AS typeform_age_bracket,
    MAX_BY(respondent_business_stage, ingested_at) AS typeform_business_stage,
    MAX_BY(respondent_investment_range, ingested_at) AS typeform_investment_range,
    MAX_BY(respondent_core_struggle, ingested_at) AS typeform_core_struggle
  FROM typeform_contact_ranked
  WHERE rn = 1
  GROUP BY 1
),
orphan_revenue_base AS (
  SELECT
    'ORPHAN_REVENUE' AS golden_contact_key,
    CAST(NULL AS STRING) AS location_id,
    CAST(NULL AS STRING) AS ghl_contact_id,
    'unattributed@system.internal' AS email,
    'unattributed@system.internal' AS email_norm,
    'unattributed@system.internal' AS email_canon,
    'Unattributed' AS first_name,
    'Revenue' AS last_name,
    'Unattributed Revenue' AS full_name,
    CAST(NULL AS STRING) AS phone,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS ghl_source,
    CAST(NULL AS STRING) AS utm_source,
    CAST(NULL AS STRING) AS utm_medium,
    CAST(NULL AS STRING) AS utm_campaign,
    CAST(NULL AS STRING) AS utm_content,
    CAST(NULL AS STRING) AS lead_magnet_history,
    CAST(NULL AS TIMESTAMP) AS ghl_date_added_ts,
    CAST(NULL AS TIMESTAMP) AS ghl_first_seen_ts,
    CAST(NULL AS TIMESTAMP) AS ghl_last_seen_ts,
    '(Unattributed / Direct Sale)' AS campaign_reporting,
    'orphan_revenue_bucket' AS attribution_gap_reason,
    0 AS meetings_booked_cnt,
    0 AS meetings_showed_cnt,
    CAST(NULL AS TIMESTAMP) AS first_meeting_start_ts,
    CAST(NULL AS TIMESTAMP) AS last_meeting_start_ts,
    FALSE AS has_calendly_booking,
    0 AS opportunity_count,
    CAST(0 AS NUMERIC) AS opportunities_total_amount,
    CAST(NULL AS STRING) AS pipeline_stage_label,
    CAST(0 AS NUMERIC) AS fanbasis_lifetime_net,
    0 AS fanbasis_transaction_count,
    0 AS fanbasis_customer_id_count,
    FALSE AS has_fanbasis_payment,
    0 AS typeform_responses_count,
    CAST(NULL AS STRING) AS latest_typeform_form_title,
    CAST(NULL AS TIMESTAMP) AS latest_typeform_response_ts,
    CAST(NULL AS TIMESTAMP) AS latest_typeform_landed_at,
    CAST(NULL AS INT64) AS latest_typeform_completion_seconds,
    CAST(NULL AS STRING) AS latest_typeform_ending_id,
    CAST(NULL AS STRING) AS latest_typeform_ending_ref,
    CAST(NULL AS FLOAT64) AS latest_typeform_form_score,
    CAST(NULL AS JSON) AS latest_typeform_hidden_fields_json,
    CAST(NULL AS JSON) AS latest_typeform_variables_json,
    CAST(NULL AS JSON) AS latest_typeform_answers_json,
    CAST(NULL AS STRING) AS latest_typeform_age_bracket,
    CAST(NULL AS STRING) AS latest_typeform_business_stage,
    CAST(NULL AS STRING) AS latest_typeform_investment_range,
    CAST(NULL AS STRING) AS latest_typeform_core_struggle,
    CAST(NULL AS STRING) AS typeform_primary_goal_stub,
    CAST(NULL AS STRING) AS typeform_primary_obstacle_stub,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE golden_contact_key IS NULL
  LIMIT 1
),
base_with_orphan AS (
  SELECT * FROM base_contacts
  UNION ALL
  SELECT * FROM orphan_revenue_base
)
SELECT
  b.golden_contact_key,
  b.location_id,
  b.ghl_contact_id,
  COALESCE(
    NULLIF(TRIM(b.email), ''),
    CONCAT('missing-email+', COALESCE(NULLIF(TRIM(b.ghl_contact_id), ''), 'unknown'), '@noemail.local')
  ) AS email,
  CASE
    WHEN b.golden_contact_key = 'ORPHAN_REVENUE' THEN TRUE
    WHEN b.email IS NULL OR TRIM(b.email) = '' THEN TRUE
    ELSE FALSE
  END AS email_is_placeholder,
  b.first_name,
  b.last_name,
  b.full_name,
  b.phone,
  b.country,
  CASE
    WHEN b.golden_contact_key = 'ORPHAN_REVENUE' THEN 'Unattributed Revenue'
    WHEN b.location_id = 'yDDvavWJesa03Cv3wKjt' THEN 'Dee Builds Brands'
    WHEN b.location_id IS NOT NULL THEN CONCAT('Location ', b.location_id)
    ELSE 'Unknown Client'
  END AS client,
  b.ghl_source,
  b.utm_source,
  b.utm_medium,
  b.utm_campaign,
  b.utm_content,
  b.lead_magnet_history,
  b.ghl_date_added_ts,
  b.ghl_first_seen_ts,
  b.ghl_last_seen_ts,
  COALESCE(ft.first_touch.touch_source, sa.snapshot_source) AS first_touch_source,
  COALESCE(ft.first_touch.touch_medium, sa.snapshot_medium) AS first_touch_medium,
  COALESCE(ft.first_touch.touch_campaign, sa.snapshot_campaign) AS first_touch_campaign,
  COALESCE(ft.first_touch.event_ts, b.ghl_date_added_ts, b.ghl_first_seen_ts) AS first_touch_event_ts,
  COALESCE(ft.last_touch.touch_source, sa.snapshot_source) AS last_touch_source,
  COALESCE(ft.last_touch.touch_medium, sa.snapshot_medium) AS last_touch_medium,
  COALESCE(ft.last_touch.touch_campaign, sa.snapshot_campaign) AS last_touch_campaign,
  COALESCE(ft.last_touch.event_ts, b.ghl_last_seen_ts, b.ghl_first_seen_ts) AS last_touch_event_ts,
  CASE
    WHEN ft.first_touch.touch_source IS NOT NULL THEN ft.first_touch.touch_source_system
    WHEN sa.snapshot_source IS NOT NULL THEN 'ghl_snapshot'
    ELSE 'none'
  END AS first_touch_source_used,
  CASE
    WHEN ft.last_touch.touch_source IS NOT NULL THEN ft.last_touch.touch_source_system
    WHEN sa.snapshot_source IS NOT NULL THEN 'ghl_snapshot'
    ELSE 'none'
  END AS last_touch_source_used,
  CASE
    WHEN ft.first_touch.touch_source IS NOT NULL THEN 'high'
    WHEN sa.snapshot_source IS NOT NULL THEN 'medium'
    ELSE 'low'
  END AS first_touch_source_confidence,
  CASE
    WHEN ft.first_touch.touch_source IS NOT NULL THEN 'rule_01_event_touch_precedence'
    WHEN sa.snapshot_source IS NOT NULL THEN 'rule_02_snapshot_fallback'
    ELSE 'rule_99_no_signal'
  END AS first_touch_rule_fired,
  CASE
    WHEN ft.last_touch.touch_source IS NOT NULL THEN 'high'
    WHEN sa.snapshot_source IS NOT NULL THEN 'medium'
    ELSE 'low'
  END AS last_touch_source_confidence,
  CASE
    WHEN ft.last_touch.touch_source IS NOT NULL THEN 'rule_01_event_touch_precedence'
    WHEN sa.snapshot_source IS NOT NULL THEN 'rule_02_snapshot_fallback'
    ELSE 'rule_99_no_signal'
  END AS last_touch_rule_fired,
  COALESCE(ft.touch_event_count, 0) AS touch_event_count,
  CASE
    WHEN b.golden_contact_key = 'ORPHAN_REVENUE' THEN 'Unattributed Revenue'
    WHEN COALESCE(ft.first_touch.event_ts, b.ghl_first_seen_ts) < TIMESTAMP('2025-01-01 00:00:00 UTC') THEN 'Legacy Era'
    WHEN COALESCE(ft.touch_event_count, 0) > 0 THEN 'UTM Era'
    WHEN COALESCE(b.opportunity_count, 0) > 0 OR b.pipeline_stage_label IS NOT NULL THEN 'Pipeline Era'
    ELSE 'Unknown Era'
  END AS tracking_era,
  CASE
    WHEN b.attribution_gap_reason IN ('no_campaign_or_source', 'no_email_on_contact') THEN 'Unknown'
    WHEN b.campaign_reporting IS NULL OR TRIM(b.campaign_reporting) = '' THEN 'Unknown'
    ELSE b.campaign_reporting
  END AS campaign_reporting,
  b.attribution_gap_reason,
  CASE
    WHEN b.attribution_gap_reason IN ('no_campaign_or_source', 'no_email_on_contact')
      AND COALESCE(NULLIF(TRIM(b.campaign_reporting), ''), 'Unknown') <> 'Unknown'
      THEN TRUE
    ELSE FALSE
  END AS campaign_reporting_inconsistent_flag,
  CASE
    WHEN ft.touch_event_count > 0
      AND sa.snapshot_source IS NOT NULL
      AND ft.last_touch.touch_source IS NOT NULL
      AND LOWER(TRIM(ft.last_touch.touch_source)) <> LOWER(TRIM(sa.snapshot_source))
      THEN TRUE
    ELSE FALSE
  END AS attribution_conflict_flag,
  CASE
    WHEN ft.touch_event_count > 0
      AND sa.snapshot_source IS NOT NULL
      AND ft.last_touch.touch_source IS NOT NULL
      AND LOWER(TRIM(ft.last_touch.touch_source)) <> LOWER(TRIM(sa.snapshot_source))
      THEN 'event_snapshot_conflict'
    WHEN ft.touch_event_count > 0 THEN 'event_history_present'
    WHEN COALESCE(sa.snapshot_source, sa.snapshot_medium, sa.snapshot_campaign) IS NOT NULL THEN 'snapshot_fallback_only'
    ELSE 'no_attribution_signal'
  END AS attribution_reason_code,
  CASE
    WHEN ft.touch_event_count > 0 THEN 'rule_01_event_touch_precedence'
    WHEN COALESCE(sa.snapshot_source, sa.snapshot_medium, sa.snapshot_campaign) IS NOT NULL THEN 'rule_02_snapshot_fallback'
    ELSE 'rule_99_no_signal'
  END AS attribution_rule_fired,
  TO_JSON_STRING(STRUCT(
    COALESCE(ft.touch_event_count, 0) AS touch_event_count,
    ft.first_touch.touch_source_system AS first_touch_source_system,
    ft.first_touch.touch_source AS first_touch_source_value,
    ft.first_touch.touch_medium AS first_touch_medium_value,
    ft.first_touch.touch_campaign AS first_touch_campaign_value,
    ft.last_touch.touch_source_system AS last_touch_source_system,
    ft.last_touch.touch_source AS last_touch_source_value,
    ft.last_touch.touch_medium AS last_touch_medium_value,
    ft.last_touch.touch_campaign AS last_touch_campaign_value,
    sa.snapshot_source AS snapshot_source_value,
    sa.snapshot_medium AS snapshot_medium_value,
    sa.snapshot_campaign AS snapshot_campaign_value,
    b.ghl_source AS ghl_source_raw,
    b.utm_source AS utm_source_raw,
    b.utm_medium AS utm_medium_raw,
    b.utm_campaign AS utm_campaign_raw
  )) AS attribution_evidence_json,
  b.meetings_booked_cnt,
  b.meetings_showed_cnt,
  b.first_meeting_start_ts,
  b.last_meeting_start_ts,
  b.has_calendly_booking AS call_booked,
  b.last_meeting_start_ts AS call_booked_date,
  b.meetings_showed_cnt > 0 AS call_taken,
  b.opportunity_count,
  b.opportunities_total_amount,
  b.pipeline_stage_label,
  COALESCE(a.application_count, 0) AS application_count,
  COALESCE(a.application_count, 0) > 0 AS application_submitted,
  a.first_application_ts AS application_date,
  COALESCE(o.outbound_call_count, 0) AS outbound_call_count,
  o.first_outbound_call_ts,
  o.latest_outbound_call_ts,
  COALESCE(p.total_payment_count, 0) AS total_payment_count,
  COALESCE(p.total_net_revenue, 0) AS total_net_revenue,
  COALESCE(p.fanbasis_net_revenue, 0) AS fanbasis_net_revenue,
  COALESCE(p.stripe_net_revenue, 0) AS stripe_net_revenue,
  COALESCE(p.total_payment_count, 0) > 0 AS has_any_payment,
  p.first_payment_ts,
  p.latest_payment_ts,
  CASE
    WHEN COALESCE(p.total_payment_count, 0) > 0 THEN COALESCE(p.latest_payment_status, 'unknown')
    ELSE NULL
  END AS latest_payment_status,
  CASE
    WHEN COALESCE(p.total_payment_count, 0) = 0 THEN NULL
    WHEN b.golden_contact_key = 'ORPHAN_REVENUE' THEN 'Unattributed Revenue'
    WHEN COALESCE(p.total_payment_count, 0) = 1 THEN 'Paid'
    WHEN COALESCE(pp.distinct_fanbasis_product_count, 0) > 1 THEN 'Repeat Buyer'
    WHEN COALESCE(p.total_payment_count, 0) > 1 THEN 'Payment Plan'
    ELSE 'Paid'
  END AS payment_status_label,
  ce.self_reported_source,
  ce.emotional_goal_value,
  ce.latest_call_type,
  COALESCE(ce.canceled_bookings_count, 0) AS canceled_bookings_count,
  COALESCE(te.typeform_responses_count, 0) AS typeform_responses_count,
  te.latest_typeform_title,
  te.latest_typeform_response_ts,
  te.latest_typeform_landed_at,
  te.latest_typeform_completion_seconds,
  te.latest_typeform_ending_id,
  te.latest_typeform_ending_ref,
  te.latest_typeform_form_score,
  te.latest_typeform_hidden_fields_json,
  te.latest_typeform_variables_json,
  te.latest_typeform_answers_json,
  te.typeform_age_bracket,
  te.typeform_business_stage,
  te.typeform_investment_range,
  te.typeform_core_struggle,
  CASE
    WHEN te.typeform_core_struggle IS NULL OR TRIM(te.typeform_core_struggle) = '' THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'lead|client|customer|book|appointment|call|close|sale|revenue|income|cash')
      THEN 'Increase Leads & Revenue'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'offer|niche|position|messag|brand')
      THEN 'Clarify Offer & Positioning'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'content|social|instagram|tiktok|ads|marketing|audience')
      THEN 'Improve Marketing & Audience Growth'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'system|process|automat|team|operation|workflow|scale|time')
      THEN 'Build Systems & Scale Capacity'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'mindset|confidence|fear|overwhelm|consisten|motivat')
      THEN 'Strengthen Mindset & Consistency'
    ELSE 'Other / Unclassified'
  END AS typeform_primary_goal_stub,
  CASE
    WHEN te.typeform_core_struggle IS NULL OR TRIM(te.typeform_core_struggle) = '' THEN NULL
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'lead|client|customer|audience|traffic')
      THEN 'Lead Generation'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'close|sales?|convert|book|appointment|follow.?up')
      THEN 'Sales Conversion'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'time|capacity|overwhelm|burnout|consisten')
      THEN 'Time & Capacity'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'offer|niche|position|messag|clarity')
      THEN 'Offer & Positioning Clarity'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'system|process|automat|workflow|team|operation')
      THEN 'Systems & Operations'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'mindset|confidence|fear|motivat')
      THEN 'Mindset & Confidence'
    WHEN REGEXP_CONTAINS(LOWER(te.typeform_core_struggle), r'content|social|instagram|tiktok|ads|marketing')
      THEN 'Marketing Execution'
    ELSE 'Other / Unclassified'
  END AS typeform_primary_obstacle_stub,
  CASE
    WHEN
      (CASE
        WHEN ft.first_touch.touch_source IS NOT NULL THEN 'high'
        WHEN COALESCE(NULLIF(TRIM(b.utm_source), ''), NULLIF(TRIM(b.ghl_source), '')) IS NOT NULL THEN 'medium'
        ELSE 'low'
      END) = 'low'
      OR
      (CASE
        WHEN ft.last_touch.touch_source IS NOT NULL THEN 'high'
        WHEN COALESCE(NULLIF(TRIM(b.utm_source), ''), NULLIF(TRIM(b.ghl_source), '')) IS NOT NULL THEN 'medium'
        ELSE 'low'
      END) = 'low'
      THEN 'needs_review'
    ELSE 'ok'
  END AS touch_attribution_quality_flag,
  CASE
    WHEN oae.owner_at_event_name IS NOT NULL THEN oae.owner_at_event_name
    WHEN gun.user_name IS NOT NULL THEN gun.user_name
    WHEN oae.owner_at_event_id IS NOT NULL THEN CONCAT('owner_id:', oae.owner_at_event_id)
    WHEN cl.closer_name IS NOT NULL THEN cl.closer_name
    ELSE 'Unassigned'
  END AS closer_name,
  cl.closer_email,
  CASE
    WHEN oae.owner_at_event_id IS NOT NULL THEN 'high'
    WHEN cl.closer_confidence IS NOT NULL THEN cl.closer_confidence
    WHEN cl.closer_email IS NOT NULL THEN 'medium'
    ELSE 'low'
  END AS closer_confidence,
  COALESCE(oae.owner_at_event_ts, cl.closer_call_ts) AS closer_call_ts,
  cl.closer_call_url,
  COALESCE(cl.multi_rep_contact, FALSE) AS multi_rep_contact,
  COALESCE(cl.fathom_call_count, 0) AS fathom_sales_call_count,
  CASE
    WHEN oae.owner_at_event_id IS NOT NULL THEN 'owner_at_event'
    WHEN cl.closer_email IS NOT NULL THEN 'fathom_calendar_invitee'
    ELSE 'unassigned'
  END AS closer_source,
  CASE
    WHEN oae.owner_at_event_id IS NOT NULL THEN 'owner_at_event'
    WHEN cl.closer_email IS NOT NULL THEN 'fathom_call_snapshot'
    ELSE 'none'
  END AS closer_attribution_timing,
  oae.owner_at_event_id AS closer_owner_at_event_id,
  CASE
    WHEN
      (CASE
        WHEN ft.first_touch.touch_source IS NOT NULL THEN 'high'
        WHEN COALESCE(NULLIF(TRIM(b.utm_source), ''), NULLIF(TRIM(b.ghl_source), '')) IS NOT NULL THEN 'medium'
        ELSE 'low'
      END) = 'low'
      OR
      (CASE
        WHEN ft.last_touch.touch_source IS NOT NULL THEN 'high'
        WHEN COALESCE(NULLIF(TRIM(b.utm_source), ''), NULLIF(TRIM(b.ghl_source), '')) IS NOT NULL THEN 'medium'
        ELSE 'low'
      END) = 'low'
      OR (
        COALESCE(p.total_payment_count, 0) > 0
        AND COALESCE(oae.owner_at_event_id, cl.closer_email) IS NULL
      )
      THEN 'needs_review'
    ELSE 'ok'
  END AS attribution_quality_flag,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM base_with_orphan b
LEFT JOIN first_last_touch ft
  ON ft.location_id = b.location_id
 AND ft.ghl_contact_id = b.ghl_contact_id
LEFT JOIN snapshot_attribution sa
  ON sa.location_id = b.location_id
 AND sa.ghl_contact_id = b.ghl_contact_id
LEFT JOIN payment_rollup p
  ON p.golden_contact_key = b.golden_contact_key
LEFT JOIN application_rollup a
  ON a.location_id = b.location_id
 AND a.ghl_contact_id = b.ghl_contact_id
LEFT JOIN outbound_call_rollup o
  ON o.location_id = b.location_id
 AND o.ghl_contact_id = b.ghl_contact_id
LEFT JOIN opportunity_owner_at_event oae
  ON oae.location_id = b.location_id
 AND oae.ghl_contact_id = b.ghl_contact_id
LEFT JOIN ghl_user_name_map gun
  ON gun.user_id = oae.owner_at_event_id
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.bridge_contact_closer` cl
  ON cl.location_id = b.location_id
 AND cl.contact_id = b.ghl_contact_id
LEFT JOIN payment_product_rollup pp
  ON pp.golden_contact_key = b.golden_contact_key
LEFT JOIN calendly_enrichment ce
  ON ce.location_id = b.location_id
 AND ce.ghl_contact_id = b.ghl_contact_id
LEFT JOIN typeform_enrichment te
  ON te.golden_contact_key = b.golden_contact_key
;

-- Lead magnet activity ledger (event grain; one row per deduplicated GHL form submission).
-- Preserves sequence and booking conversion context that the lead-wide mart intentionally compresses.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity` AS
WITH submission_enriched AS (
  SELECT
    gc.golden_contact_key,
    fs.location_id,
    fs.contact_id AS ghl_contact_id,
    COALESCE(NULLIF(TRIM(a.contact_name), ''), gc.full_name) AS contact_name,
    COALESCE(NULLIF(TRIM(a.contact_email), ''), gc.email) AS contact_email,
    fs.submission_id,
    fs.form_id,
    COALESCE(
      NULLIF(TRIM(fs.form_name), ''),
      NULLIF(TRIM(a.form_name), ''),
      CONCAT('form_id:', COALESCE(fs.form_id, 'unknown'))
    ) AS lead_magnet_name,
    gf.slug AS form_slug,
    gf.form_type,
    COALESCE(fs.event_ts, fs.updated_at_ts, fs.ingested_at) AS event_ts,
    fs.event_ts AS submission_event_ts,
    fs.updated_at_ts AS submission_updated_at_ts,
    fs.ingested_at AS submission_ingested_at,
    fs.backfill_run_id,
    fs.is_backfill,
    fs.opportunity_id AS opportunity_id_raw,
    fs.source AS submission_source_raw,
    fs.utm_source AS utm_source_raw,
    fs.utm_medium AS utm_medium_raw,
    fs.utm_campaign AS utm_campaign_raw,
    fs.utm_content AS utm_content_raw,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(fs.payload_json, '$.others.eventData.page.url'),
      JSON_VALUE(fs.payload_json, '$.others.eventData.documentURL')
    )), '') AS landing_page_url_raw,
    NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.referrer')), '') AS referrer_url_raw,
    NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_source')), '') AS utm_source_param_raw,
    NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_medium')), '') AS utm_medium_param_raw,
    NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_campaign')), '') AS utm_campaign_param_raw,
    NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_content')), '') AS utm_content_param_raw,
    COALESCE(
      a.source,
      fs.source,
      NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.source')), '')
    ) AS submission_source,
    COALESCE(
      a.utm_source,
      fs.utm_source,
      NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_source')), ''),
      NULLIF(TRIM(REPLACE(REGEXP_EXTRACT(
        COALESCE(
          JSON_VALUE(fs.payload_json, '$.others.eventData.page.url'),
          JSON_VALUE(fs.payload_json, '$.others.eventData.documentURL')
        ),
        r'(?i)(?:[?&]|^)utm_source=([^&#]+)'
      ), '+', ' ')), '')
    ) AS utm_source,
    COALESCE(
      a.utm_medium,
      fs.utm_medium,
      NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_medium')), ''),
      NULLIF(TRIM(REPLACE(REGEXP_EXTRACT(
        COALESCE(
          JSON_VALUE(fs.payload_json, '$.others.eventData.page.url'),
          JSON_VALUE(fs.payload_json, '$.others.eventData.documentURL')
        ),
        r'(?i)(?:[?&]|^)utm_medium=([^&#]+)'
      ), '+', ' ')), '')
    ) AS utm_medium,
    COALESCE(
      a.utm_campaign,
      fs.utm_campaign,
      NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_campaign')), ''),
      NULLIF(TRIM(REPLACE(REGEXP_EXTRACT(
        COALESCE(
          JSON_VALUE(fs.payload_json, '$.others.eventData.page.url'),
          JSON_VALUE(fs.payload_json, '$.others.eventData.documentURL')
        ),
        r'(?i)(?:[?&]|^)utm_campaign=([^&#]+)'
      ), '+', ' ')), '')
    ) AS utm_campaign,
    COALESCE(
      a.utm_content,
      fs.utm_content,
      NULLIF(TRIM(JSON_VALUE(fs.payload_json, '$.others.eventData.url_params.utm_content')), ''),
      NULLIF(TRIM(REPLACE(REGEXP_EXTRACT(
        COALESCE(
          JSON_VALUE(fs.payload_json, '$.others.eventData.page.url'),
          JSON_VALUE(fs.payload_json, '$.others.eventData.documentURL')
        ),
        r'(?i)(?:[?&]|^)utm_content=([^&#]+)'
      ), '+', ' ')), '')
    ) AS utm_content,
    COALESCE(a.opportunity_id, fs.opportunity_id) AS opportunity_id,
    a.pipeline_id,
    a.pipeline_name,
    a.pipeline_stage_id,
    a.stage_name,
    a.opportunity_status,
    a.opportunity_amount,
    gc.campaign_reporting AS current_campaign_reporting,
    gc.ghl_source AS current_ghl_source,
    fs.payload_json AS submission_payload_json
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_form_submissions` fs
  JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.location_id = fs.location_id
   AND gc.ghl_contact_id = fs.contact_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_attribution` a
    ON a.location_id = fs.location_id
   AND a.submission_id = fs.submission_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_ghl_forms` gf
    ON gf.location_id = fs.location_id
   AND gf.form_id = fs.form_id
  WHERE fs.contact_id IS NOT NULL
    AND COALESCE(fs.event_ts, fs.updated_at_ts, fs.ingested_at) IS NOT NULL
),
submission_deduped AS (
  SELECT
    *
  FROM submission_enriched
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      location_id,
      ghl_contact_id,
      COALESCE(form_id, 'unknown'),
      TIMESTAMP_TRUNC(event_ts, MINUTE),
      COALESCE(NULLIF(TRIM(submission_source), ''), ''),
      COALESCE(NULLIF(TRIM(utm_source), ''), ''),
      COALESCE(NULLIF(TRIM(utm_medium), ''), ''),
      COALESCE(NULLIF(TRIM(utm_campaign), ''), ''),
      COALESCE(NULLIF(TRIM(utm_content), ''), '')
    ORDER BY submission_ingested_at DESC, submission_id DESC
  ) = 1
),
lead_rollup AS (
  SELECT
    golden_contact_key,
    COUNT(*) AS lead_magnet_event_count_for_lead,
    COUNT(DISTINCT lead_magnet_name) AS distinct_lead_magnet_count_for_lead,
    MIN(event_ts) AS first_lead_magnet_ts,
    MAX(event_ts) AS latest_lead_magnet_ts
  FROM submission_deduped
  GROUP BY 1
),
booking_events AS (
  SELECT
    b.location_id,
    b.contact_id AS ghl_contact_id,
    i.invitee_id AS booking_invitee_id,
    COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) AS booking_created_ts,
    i.scheduled_start_time AS booking_scheduled_start_ts,
    COALESCE(NULLIF(TRIM(i.event_name), ''), CONCAT('event_type:', COALESCE(i.event_type_uri, 'unknown'))) AS booking_event_name,
    COALESCE(NULLIF(TRIM(i.event_status), ''), 'unknown') AS booking_event_status,
    COALESCE(NULLIF(TRIM(i.invitee_status), ''), 'unknown') AS booking_invitee_status,
    COALESCE(i.is_canceled, FALSE) AS booking_is_canceled
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) IS NOT NULL
),
next_booking AS (
  SELECT
    e.golden_contact_key,
    e.submission_id,
    ARRAY_AGG(
      STRUCT(
        b.booking_invitee_id,
        b.booking_created_ts,
        b.booking_scheduled_start_ts,
        b.booking_event_name,
        b.booking_event_status,
        b.booking_invitee_status,
        b.booking_is_canceled
      )
      ORDER BY b.booking_created_ts ASC, b.booking_invitee_id
      LIMIT 1
    )[SAFE_OFFSET(0)] AS next_booking
  FROM submission_deduped e
  LEFT JOIN booking_events b
    ON b.location_id = e.location_id
   AND b.ghl_contact_id = e.ghl_contact_id
   AND b.booking_created_ts >= e.event_ts
  GROUP BY 1, 2
),
sequenced AS (
  SELECT
    e.*,
    lr.lead_magnet_event_count_for_lead,
    lr.distinct_lead_magnet_count_for_lead,
    lr.first_lead_magnet_ts,
    lr.latest_lead_magnet_ts,
    ROW_NUMBER() OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts, e.submission_id
    ) AS sequence_number,
    ROW_NUMBER() OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts DESC, e.submission_id DESC
    ) AS reverse_sequence_number,
    LAG(e.lead_magnet_name) OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts, e.submission_id
    ) AS previous_lead_magnet_name,
    LAG(e.event_ts) OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts, e.submission_id
    ) AS previous_event_ts,
    LEAD(e.lead_magnet_name) OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts, e.submission_id
    ) AS next_lead_magnet_name,
    LEAD(e.event_ts) OVER (
      PARTITION BY e.golden_contact_key
      ORDER BY e.event_ts, e.submission_id
    ) AS next_event_ts
  FROM submission_deduped e
  LEFT JOIN lead_rollup lr
    ON lr.golden_contact_key = e.golden_contact_key
)
SELECT
  s.golden_contact_key,
  s.location_id,
  s.ghl_contact_id,
  s.contact_name,
  s.contact_email,
  s.submission_id,
  s.form_id,
  s.lead_magnet_name,
  s.form_slug,
  s.form_type,
  s.event_ts,
  DATE(s.event_ts) AS event_date,
  s.sequence_number,
  s.reverse_sequence_number,
  s.previous_lead_magnet_name,
  s.previous_event_ts,
  s.next_lead_magnet_name,
  s.next_event_ts,
  TIMESTAMP_DIFF(s.event_ts, s.previous_event_ts, MINUTE) AS minutes_since_previous_lead_magnet,
  SAFE_DIVIDE(TIMESTAMP_DIFF(s.event_ts, s.previous_event_ts, MINUTE), 60.0) AS hours_since_previous_lead_magnet,
  SAFE_DIVIDE(TIMESTAMP_DIFF(s.event_ts, s.previous_event_ts, MINUTE), 1440.0) AS days_since_previous_lead_magnet,
  TIMESTAMP_DIFF(s.next_event_ts, s.event_ts, MINUTE) AS minutes_to_next_lead_magnet,
  s.lead_magnet_event_count_for_lead,
  s.distinct_lead_magnet_count_for_lead,
  s.first_lead_magnet_ts,
  s.latest_lead_magnet_ts,
  s.submission_source,
  s.utm_source,
  s.utm_medium,
  s.utm_campaign,
  s.utm_content,
  CASE
    WHEN s.landing_page_url_raw IS NULL THEN NULL
    ELSE NULLIF(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              s.landing_page_url_raw,
              r'([?&])(mcp_token|fbclid|gclid|ttclid|_hsenc|_hsmi|mc_eid|igshid|wbraid|gbraid)=[^&#]*',
              r'\1'
            ),
            r'\?&',
            '?'
          ),
          r'&&+',
          '&'
        ),
        r'[?&]$',
        ''
      ),
      ''
    )
  END AS landing_page_url,
  s.referrer_url_raw AS referrer_url,
  s.submission_source_raw,
  s.utm_source_raw,
  s.utm_medium_raw,
  s.utm_campaign_raw,
  s.utm_content_raw,
  s.landing_page_url_raw,
  s.referrer_url_raw,
  s.utm_source_param_raw,
  s.utm_medium_param_raw,
  s.utm_campaign_param_raw,
  s.utm_content_param_raw,
  s.current_campaign_reporting,
  s.current_ghl_source,
  s.opportunity_id,
  s.pipeline_id,
  s.pipeline_name,
  s.pipeline_stage_id,
  s.stage_name,
  s.opportunity_status,
  s.opportunity_amount,
  nb.next_booking.booking_invitee_id,
  nb.next_booking.booking_created_ts AS next_booking_created_ts,
  nb.next_booking.booking_scheduled_start_ts,
  nb.next_booking.booking_event_name,
  nb.next_booking.booking_event_status,
  nb.next_booking.booking_invitee_status,
  nb.next_booking.booking_is_canceled,
  nb.next_booking.booking_created_ts IS NOT NULL AS has_booked_call_after,
  CASE
    WHEN nb.next_booking.booking_created_ts IS NOT NULL
      THEN TIMESTAMP_DIFF(nb.next_booking.booking_created_ts, s.event_ts, MINUTE)
    ELSE NULL
  END AS minutes_to_booked_call,
  CASE
    WHEN nb.next_booking.booking_created_ts IS NOT NULL
      THEN SAFE_DIVIDE(TIMESTAMP_DIFF(nb.next_booking.booking_created_ts, s.event_ts, MINUTE), 60.0)
    ELSE NULL
  END AS hours_to_booked_call,
  CASE
    WHEN nb.next_booking.booking_created_ts IS NOT NULL
      AND nb.next_booking.booking_created_ts <= TIMESTAMP_ADD(s.event_ts, INTERVAL 48 HOUR)
      THEN TRUE
    ELSE FALSE
  END AS booked_call_within_48h,
  CASE
    WHEN nb.next_booking.booking_created_ts IS NOT NULL
      AND (s.next_event_ts IS NULL OR s.next_event_ts > nb.next_booking.booking_created_ts)
      THEN TRUE
    ELSE FALSE
  END AS is_last_lead_magnet_before_booking,
  s.submission_event_ts,
  s.submission_updated_at_ts,
  s.submission_ingested_at,
  s.backfill_run_id,
  s.is_backfill,
  s.submission_payload_json,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM sequenced s
LEFT JOIN next_booking nb
  ON nb.golden_contact_key = s.golden_contact_key
 AND nb.submission_id = s.submission_id
;

-- Team member dimension for setter attribution.
-- Stable key precedence:
--   1) explicit GHL user id
--   2) email (cross-system friendly)
--   3) fallback display name
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.dim_team_members` AS
WITH ghl_call_users AS (
  SELECT
    COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) AS activity_ts,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedTo'),
      JSON_VALUE(c.payload_json, '$.assigned_to'),
      JSON_VALUE(c.payload_json, '$.userId'),
      JSON_VALUE(c.payload_json, '$.user_id'),
      JSON_VALUE(c.payload_json, '$.ownerId'),
      JSON_VALUE(c.payload_json, '$.owner_id'),
      JSON_VALUE(c.payload_json, '$.agentId'),
      JSON_VALUE(c.payload_json, '$.agent_id')
    )), '') AS user_id_raw,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedToName'),
      JSON_VALUE(c.payload_json, '$.assigned_to_name'),
      JSON_VALUE(c.payload_json, '$.userName'),
      JSON_VALUE(c.payload_json, '$.user_name'),
      JSON_VALUE(c.payload_json, '$.ownerName'),
      JSON_VALUE(c.payload_json, '$.owner_name'),
      JSON_VALUE(c.payload_json, '$.agentName'),
      JSON_VALUE(c.payload_json, '$.agent_name')
    )), '') AS user_name_raw,
    NULLIF(TRIM(LOWER(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedToEmail'),
      JSON_VALUE(c.payload_json, '$.assigned_to_email'),
      JSON_VALUE(c.payload_json, '$.userEmail'),
      JSON_VALUE(c.payload_json, '$.user_email'),
      JSON_VALUE(c.payload_json, '$.ownerEmail'),
      JSON_VALUE(c.payload_json, '$.owner_email'),
      JSON_VALUE(c.payload_json, '$.agentEmail'),
      JSON_VALUE(c.payload_json, '$.agent_email')
    ))), '') AS user_email_norm,
    CAST(NULL AS STRING) AS phone,
    'ghl_call' AS source_event_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  WHERE COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) IS NOT NULL
),
ghl_sms_users AS (
  SELECT
    COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) AS activity_ts,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedTo'),
      JSON_VALUE(m.payload_json, '$.assigned_to'),
      JSON_VALUE(m.payload_json, '$.userId'),
      JSON_VALUE(m.payload_json, '$.user_id'),
      JSON_VALUE(m.payload_json, '$.ownerId'),
      JSON_VALUE(m.payload_json, '$.owner_id'),
      JSON_VALUE(m.payload_json, '$.agentId'),
      JSON_VALUE(m.payload_json, '$.agent_id')
    )), '') AS user_id_raw,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedToName'),
      JSON_VALUE(m.payload_json, '$.assigned_to_name'),
      JSON_VALUE(m.payload_json, '$.userName'),
      JSON_VALUE(m.payload_json, '$.user_name'),
      JSON_VALUE(m.payload_json, '$.ownerName'),
      JSON_VALUE(m.payload_json, '$.owner_name'),
      JSON_VALUE(m.payload_json, '$.agentName'),
      JSON_VALUE(m.payload_json, '$.agent_name')
    )), '') AS user_name_raw,
    NULLIF(TRIM(LOWER(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedToEmail'),
      JSON_VALUE(m.payload_json, '$.assigned_to_email'),
      JSON_VALUE(m.payload_json, '$.userEmail'),
      JSON_VALUE(m.payload_json, '$.user_email'),
      JSON_VALUE(m.payload_json, '$.ownerEmail'),
      JSON_VALUE(m.payload_json, '$.owner_email'),
      JSON_VALUE(m.payload_json, '$.agentEmail'),
      JSON_VALUE(m.payload_json, '$.agent_email')
    ))), '') AS user_email_norm,
    CAST(NULL AS STRING) AS phone,
    'ghl_sms' AS source_event_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
  WHERE COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) IS NOT NULL
    AND (
      LOWER(COALESCE(m.direction_norm, '')) = 'outbound'
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(m.payload_json, '$.lastMessageDirection'), '')), r'outbound')
    )
    AND REGEXP_CONTAINS(
      LOWER(COALESCE(m.message_type_norm, '')),
      r'sms|text|whatsapp|type_sms|type_call|type_phone|phone|call'
    )
),
fathom_users AS (
  SELECT
    COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at) AS activity_ts,
    CAST(NULL AS STRING) AS user_id_raw,
    NULLIF(TRIM(f.recorded_by_name), '') AS user_name_raw,
    NULLIF(TRIM(LOWER(f.recorded_by_email)), '') AS user_email_norm,
    CAST(NULL AS STRING) AS phone,
    'fathom_call' AS source_event_type
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` f
  WHERE COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at) IS NOT NULL
    AND (NULLIF(TRIM(f.recorded_by_name), '') IS NOT NULL OR NULLIF(TRIM(f.recorded_by_email), '') IS NOT NULL)
),
-- Static seed: maps known GHL userIds to name/email so dim_team_members can resolve
-- setter_dim_display_name for ghl_outbound rows.
-- Add rows here whenever a GHL userId is identified (from GHL Settings → Team Members).
-- Confirmed via Fathom cross-reference (18 overlapping calls within ±4h same contact).
static_ghl_user_seed AS (
  SELECT
    activity_ts,
    user_id_raw,
    user_name_raw,
    user_email_norm,
    phone,
    'static_seed' AS source_event_type
  FROM (
    -- Confirmed from GHL Settings → Team Members (screenshots 2026-04-11)
    -- Columns: activity_ts, user_id_raw, user_name_raw, user_email_norm, phone
    SELECT TIMESTAMP('2099-01-01') AS activity_ts, 'leBv9MtltaKdfSijVEhb' AS user_id_raw, 'Houssam Bentouati'  AS user_name_raw, 'houssam@precisionscaling.io'        AS user_email_norm, CAST(NULL AS STRING) AS phone
    UNION ALL SELECT TIMESTAMP('2099-01-01'), '1D4ZUkV07gGJ25YtUolz', 'Houssam Bentouati',  'houssam@precisionscaling.io',        NULL  -- GHL settings ID (may differ from payload ID above)
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'c5ujVqeYHGi1WnmlvtWu', 'Marco Branco',        'marcobranco@precisionscaling.io',    '+27826101942'
    UNION ALL SELECT TIMESTAMP('2099-01-01'), '7rCcXXi8tFdihhDvTTM3', 'Mitchell Naude',      'mitchell@precisionscaling.io',       NULL
    UNION ALL SELECT TIMESTAMP('2099-01-01'), '9rocXim1JjeIvjSrWLSn', 'Boipelo Mashigo',     'boipelo@precisionscaling.io',        '+491723821687'
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'DTtFkB0jtX1ionHhjsGR', 'Blagoj Veleski',      'blagoj@precisionscaling.io',         '+38975439803'
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'ZOytPUG1jSWRNBzsJYEp', 'Hammad Ahsan',        'hammad@precisionscaling.io',         NULL
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'J4eyQWx4oFfPj08qunrS', 'Jordan Evans',         'jordan@precisionscaling.io',        NULL
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'ILX9jpFp7ycNbWgakiYR', 'Kevin Maya',           'kevin@precisionscaling.io',         NULL
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'XKcL1lmTZn8LFHiUwtn1', 'Jake Lynch',           'jake@precisionscaling.io',          NULL
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'YyBgSVqB1wQoFj8tAe40', 'Stanley Macauley',     'stanley@stanleyoperations.com',      '+447586641324'
    UNION ALL SELECT TIMESTAMP('2099-01-01'), 'BKc6beDhtuW1GFp0wI',   'Ethan Gerstenberg',    'ethan@precisionscaling.io',          NULL
    -- Oct5Tz6ZVUaDkqXC3yHL: 292 older events, not in GHL team member list — likely deleted account
  )
),
unioned AS (
  SELECT * FROM ghl_call_users
  UNION ALL
  SELECT * FROM ghl_sms_users
  UNION ALL
  SELECT * FROM fathom_users
  UNION ALL
  SELECT * FROM static_ghl_user_seed
),
normalized AS (
  SELECT
    CASE
      WHEN user_id_raw IS NOT NULL THEN CONCAT('ghl_user:', LOWER(user_id_raw))
      WHEN user_email_norm IS NOT NULL THEN CONCAT('email:', user_email_norm)
      WHEN user_name_raw IS NOT NULL THEN CONCAT('name:', LOWER(user_name_raw))
      ELSE 'unknown_setter'
    END AS team_member_key,
    user_id_raw AS ghl_user_id,
    user_name_raw AS display_name_raw,
    user_email_norm AS email_norm,
    phone,
    source_event_type,
    activity_ts
  FROM unioned
  WHERE user_id_raw IS NOT NULL
     OR user_email_norm IS NOT NULL
     OR user_name_raw IS NOT NULL
),
aggregated AS (
  SELECT
    team_member_key,
    MAX_BY(ghl_user_id, activity_ts) AS ghl_user_id,
    MAX_BY(display_name_raw, activity_ts) AS display_name,
    MAX_BY(email_norm, activity_ts) AS email,
    MAX_BY(phone, activity_ts) AS phone,
    COUNT(*) AS activity_event_count,
    MIN(activity_ts) AS first_seen_ts,
    MAX(activity_ts) AS last_seen_ts,
    COUNTIF(source_event_type = 'ghl_call') > 0 AS seen_in_ghl_call,
    COUNTIF(source_event_type = 'ghl_sms') > 0 AS seen_in_ghl_sms,
    COUNTIF(source_event_type = 'fathom_call') > 0 AS seen_in_fathom_call
  FROM normalized
  GROUP BY 1
)
SELECT
  team_member_key,
  ghl_user_id,
  email,
  phone,
  display_name,
  CASE
    WHEN seen_in_ghl_call OR seen_in_ghl_sms THEN 'ghl'
    WHEN seen_in_fathom_call THEN 'fathom'
    ELSE 'unknown'
  END AS primary_source_system,
  activity_event_count,
  first_seen_ts,
  last_seen_ts,
  seen_in_ghl_call,
  seen_in_ghl_sms,
  seen_in_fathom_call,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM aggregated
;

-- Speed to lead fact (event grain).
-- One row per "hand raise" (first lead magnet touch OR booking event), matched to the
-- first outbound touch that occurs at or after the trigger timestamp.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead` AS
WITH start_events AS (
  SELECT
    CONCAT('lead_magnet|', COALESCE(lma.submission_id, 'unknown')) AS trigger_event_id,
    lma.golden_contact_key,
    lma.location_id,
    lma.ghl_contact_id,
    lma.event_ts AS trigger_ts,
    'lead_magnet' AS trigger_type,
    lma.lead_magnet_name AS source_label,
    lma.submission_id AS source_id,
    lma.utm_source,
    lma.utm_medium,
    lma.utm_campaign,
    lma.utm_content
  FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity` lma
  WHERE lma.sequence_number = 1
    AND lma.event_ts IS NOT NULL

  UNION ALL

  SELECT
    CONCAT('appointment_booking|', i.invitee_id) AS trigger_event_id,
    gc.golden_contact_key,
    b.location_id,
    b.contact_id AS ghl_contact_id,
    COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) AS trigger_ts,
    'appointment_booking' AS trigger_type,
    COALESCE(NULLIF(TRIM(i.event_name), ''), CONCAT('event_type:', COALESCE(i.event_type_uri, 'unknown'))) AS source_label,
    i.invitee_id AS source_id,
    i.utm_source,
    i.utm_medium,
    i.utm_campaign,
    i.utm_content
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.location_id = b.location_id
   AND gc.ghl_contact_id = b.contact_id
  WHERE b.contact_id IS NOT NULL
    AND COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) IS NOT NULL
),
outbound_touch_calls AS (
  SELECT
    gc.golden_contact_key,
    c.location_id,
    c.contact_id AS ghl_contact_id,
    COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) AS touch_ts,
    'call' AS channel,
    COALESCE(NULLIF(TRIM(c.call_status), ''), 'unknown') AS touch_status,
    c.call_log_id AS touch_id,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(c.payload_json, '$.conversationId'),
      JSON_VALUE(c.payload_json, '$.conversation_id')
    )), '') AS call_conversation_id,
    NULLIF(
      REGEXP_REPLACE(
        COALESCE(
          JSON_VALUE(c.payload_json, '$.from'),
          JSON_VALUE(c.payload_json, '$.fromNumber'),
          JSON_VALUE(c.payload_json, '$.from.number')
        ),
        r'[^0-9]',
        ''
      ),
      ''
    ) AS sender_phone_digits,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedTo'),
      JSON_VALUE(c.payload_json, '$.assigned_to'),
      JSON_VALUE(c.payload_json, '$.userId'),
      JSON_VALUE(c.payload_json, '$.user_id'),
      JSON_VALUE(c.payload_json, '$.ownerId'),
      JSON_VALUE(c.payload_json, '$.owner_id'),
      JSON_VALUE(c.payload_json, '$.agentId'),
      JSON_VALUE(c.payload_json, '$.agent_id')
    )), '') AS setter_user_id_raw,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedToName'),
      JSON_VALUE(c.payload_json, '$.assigned_to_name'),
      JSON_VALUE(c.payload_json, '$.userName'),
      JSON_VALUE(c.payload_json, '$.user_name'),
      JSON_VALUE(c.payload_json, '$.ownerName'),
      JSON_VALUE(c.payload_json, '$.owner_name'),
      JSON_VALUE(c.payload_json, '$.agentName'),
      JSON_VALUE(c.payload_json, '$.agent_name')
    )), '') AS setter_user_name_raw,
    NULLIF(TRIM(LOWER(COALESCE(
      JSON_VALUE(c.payload_json, '$.assignedToEmail'),
      JSON_VALUE(c.payload_json, '$.assigned_to_email'),
      JSON_VALUE(c.payload_json, '$.userEmail'),
      JSON_VALUE(c.payload_json, '$.user_email'),
      JSON_VALUE(c.payload_json, '$.ownerEmail'),
      JSON_VALUE(c.payload_json, '$.owner_email'),
      JSON_VALUE(c.payload_json, '$.agentEmail'),
      JSON_VALUE(c.payload_json, '$.agent_email')
    ))), '') AS setter_user_email_raw,
    NULLIF(TRIM(LOWER(JSON_VALUE(c.payload_json, '$.source'))), '') AS touch_source_raw
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.location_id = c.location_id
   AND gc.ghl_contact_id = c.contact_id
  WHERE COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) IS NOT NULL
    AND LOWER(COALESCE(c.direction_norm, '')) = 'outbound'
),
conversation_sender_map AS (
  SELECT
    location_id,
    call_conversation_id AS conversation_id,
    MAX_BY(sender_phone_digits, touch_ts) AS sender_phone_digits
  FROM outbound_touch_calls
  WHERE call_conversation_id IS NOT NULL
    AND sender_phone_digits IS NOT NULL
  GROUP BY 1, 2
),
conversation_message_sender_map AS (
  -- For conversation_phone touches, the conversation entity has no userId.
  -- Look inside message/call records within the conversation to find the sender.
  SELECT
    JSON_VALUE(payload_json, '$.conversationId') AS conversation_id,
    location_id,
    MAX_BY(
      JSON_VALUE(payload_json, '$.userId'),
      COALESCE(TIMESTAMP(JSON_VALUE(payload_json, '$.dateAdded')), event_ts)
    ) AS sender_user_id
  FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
  WHERE entity_type IN ('outbound_call_logs', 'message')
    AND JSON_VALUE(payload_json, '$.direction') = 'outbound'
    AND JSON_VALUE(payload_json, '$.userId') IS NOT NULL
    AND JSON_VALUE(payload_json, '$.conversationId') IS NOT NULL
  GROUP BY 1, 2
),
outbound_touch_sms AS (
  SELECT
    gc.golden_contact_key,
    m.location_id,
    m.contact_id AS ghl_contact_id,
    COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) AS touch_ts,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(m.message_type_norm, '')), r'sms|text|whatsapp|type_sms') THEN 'sms'
      ELSE 'conversation_phone'
    END AS channel,
    COALESCE(NULLIF(TRIM(m.message_status_norm), ''), 'unknown') AS touch_status,
    COALESCE(m.message_id, m.conversation_id, CONCAT('msg_ts:', CAST(UNIX_MICROS(COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at)) AS STRING))) AS touch_id,
    CAST(NULL AS STRING) AS call_conversation_id,
    COALESCE(
      NULLIF(
        REGEXP_REPLACE(
          COALESCE(
            JSON_VALUE(m.payload_json, '$.from'),
            JSON_VALUE(m.payload_json, '$.fromNumber'),
            JSON_VALUE(m.payload_json, '$.from.number')
          ),
          r'[^0-9]',
          ''
        ),
        ''
      ),
      csm.sender_phone_digits
    ) AS sender_phone_digits,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedTo'),
      JSON_VALUE(m.payload_json, '$.assigned_to'),
      JSON_VALUE(m.payload_json, '$.userId'),
      JSON_VALUE(m.payload_json, '$.user_id'),
      JSON_VALUE(m.payload_json, '$.ownerId'),
      JSON_VALUE(m.payload_json, '$.owner_id'),
      JSON_VALUE(m.payload_json, '$.agentId'),
      JSON_VALUE(m.payload_json, '$.agent_id'),
      cmsm.sender_user_id
    )), '') AS setter_user_id_raw,
    NULLIF(TRIM(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedToName'),
      JSON_VALUE(m.payload_json, '$.assigned_to_name'),
      JSON_VALUE(m.payload_json, '$.userName'),
      JSON_VALUE(m.payload_json, '$.user_name'),
      JSON_VALUE(m.payload_json, '$.ownerName'),
      JSON_VALUE(m.payload_json, '$.owner_name'),
      JSON_VALUE(m.payload_json, '$.agentName'),
      JSON_VALUE(m.payload_json, '$.agent_name')
    )), '') AS setter_user_name_raw,
    NULLIF(TRIM(LOWER(COALESCE(
      JSON_VALUE(m.payload_json, '$.assignedToEmail'),
      JSON_VALUE(m.payload_json, '$.assigned_to_email'),
      JSON_VALUE(m.payload_json, '$.userEmail'),
      JSON_VALUE(m.payload_json, '$.user_email'),
      JSON_VALUE(m.payload_json, '$.ownerEmail'),
      JSON_VALUE(m.payload_json, '$.owner_email'),
      JSON_VALUE(m.payload_json, '$.agentEmail'),
      JSON_VALUE(m.payload_json, '$.agent_email')
    ))), '') AS setter_user_email_raw,
    NULLIF(TRIM(LOWER(JSON_VALUE(m.payload_json, '$.source'))), '') AS touch_source_raw
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
  JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.location_id = m.location_id
   AND gc.ghl_contact_id = m.contact_id
  LEFT JOIN conversation_sender_map csm
    ON csm.location_id = m.location_id
   AND csm.conversation_id IN (m.conversation_id, m.message_id)
  LEFT JOIN conversation_message_sender_map cmsm
    ON cmsm.location_id = m.location_id
   AND cmsm.conversation_id = m.conversation_id
  WHERE COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) IS NOT NULL
    AND (
      LOWER(COALESCE(m.direction_norm, '')) = 'outbound'
      OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(m.payload_json, '$.lastMessageDirection'), '')), r'outbound')
    )
    AND REGEXP_CONTAINS(
      LOWER(COALESCE(m.message_type_norm, '')),
      r'sms|text|whatsapp|type_sms|type_call|type_phone|phone|call'
    )
),
outbound_touches AS (
  SELECT
    *
  FROM (
    SELECT * FROM outbound_touch_calls
    UNION ALL
    SELECT * FROM outbound_touch_sms
  )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY golden_contact_key, channel, touch_id, touch_ts
    ORDER BY touch_ts
  ) = 1
),
sender_phone_identity_map AS (
  SELECT
    location_id,
    sender_phone_digits,
    MAX_BY(setter_user_id_raw, touch_ts) AS phone_map_setter_user_id_raw,
    MAX_BY(setter_user_name_raw, touch_ts) AS phone_map_setter_user_name_raw,
    MAX_BY(setter_user_email_raw, touch_ts) AS phone_map_setter_user_email_raw,
    MAX_BY(
      CASE
        WHEN setter_user_id_raw IS NOT NULL THEN CONCAT('ghl_user:', LOWER(setter_user_id_raw))
        WHEN setter_user_email_raw IS NOT NULL THEN CONCAT('email:', LOWER(setter_user_email_raw))
        WHEN setter_user_name_raw IS NOT NULL THEN CONCAT('name:', LOWER(setter_user_name_raw))
        ELSE NULL
      END,
      touch_ts
    ) AS phone_map_setter_team_member_key
  FROM outbound_touches
  WHERE sender_phone_digits IS NOT NULL
    AND (
      setter_user_id_raw IS NOT NULL
      OR setter_user_email_raw IS NOT NULL
      OR setter_user_name_raw IS NOT NULL
    )
  GROUP BY 1, 2
  HAVING COUNT(DISTINCT CASE
    WHEN setter_user_id_raw IS NOT NULL THEN CONCAT('ghl_user:', LOWER(setter_user_id_raw))
    WHEN setter_user_email_raw IS NOT NULL THEN CONCAT('email:', LOWER(setter_user_email_raw))
    WHEN setter_user_name_raw IS NOT NULL THEN CONCAT('name:', LOWER(setter_user_name_raw))
    ELSE NULL
  END) = 1
),
fathom_setter_candidates AS (
  SELECT
    gc.golden_contact_key,
    COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at) AS fathom_call_ts,
    NULLIF(TRIM(LOWER(f.recorded_by_email)), '') AS fathom_recorded_by_email,
    NULLIF(TRIM(f.recorded_by_name), '') AS fathom_recorded_by_name,
    COALESCE(
      CONCAT('email:', NULLIF(TRIM(LOWER(f.recorded_by_email)), '')),
      CONCAT('name:', LOWER(NULLIF(TRIM(f.recorded_by_name), '')))
    ) AS fathom_team_member_key
  FROM `project-41542e21-470f-4589-96d.Core.bridge_fathom_call_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_fathom_calls` f
    ON f.call_id = b.call_id
  JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.location_id = b.location_id
   AND gc.ghl_contact_id = b.contact_id
  WHERE COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at) IS NOT NULL
),
first_touch AS (
  SELECT
    s.trigger_event_id,
    s.golden_contact_key,
    s.location_id,
    s.ghl_contact_id,
    s.trigger_ts,
    s.trigger_type,
    s.source_label,
    s.source_id,
    s.utm_source,
    s.utm_medium,
    s.utm_campaign,
    s.utm_content,
    o.touch_ts AS first_touch_ts,
    o.channel AS first_touch_channel,
    o.touch_status AS first_touch_status,
    o.touch_id AS first_touch_id,
    o.sender_phone_digits,
    o.setter_user_id_raw,
    o.setter_user_name_raw,
    o.setter_user_email_raw,
    o.touch_source_raw AS first_touch_source_raw
  FROM start_events s
  LEFT JOIN outbound_touches o
    ON o.golden_contact_key = s.golden_contact_key
   AND o.touch_ts >= s.trigger_ts
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.trigger_event_id
    ORDER BY o.touch_ts ASC, o.channel ASC, o.touch_id ASC
  ) = 1
),
with_fathom_fallback AS (
  SELECT
    f.*,
    sp.phone_map_setter_user_id_raw,
    sp.phone_map_setter_user_name_raw,
    sp.phone_map_setter_user_email_raw,
    sp.phone_map_setter_team_member_key,
    fs.fathom_recorded_by_email,
    fs.fathom_recorded_by_name,
    fs.fathom_team_member_key
  FROM first_touch f
  LEFT JOIN sender_phone_identity_map sp
    ON sp.location_id = f.location_id
   AND sp.sender_phone_digits = f.sender_phone_digits
  LEFT JOIN fathom_setter_candidates fs
    ON fs.golden_contact_key = f.golden_contact_key
   AND f.first_touch_ts IS NOT NULL
   AND ABS(TIMESTAMP_DIFF(fs.fathom_call_ts, f.first_touch_ts, MINUTE)) <= 240
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY f.trigger_event_id
    ORDER BY
      COALESCE(ABS(TIMESTAMP_DIFF(fs.fathom_call_ts, f.first_touch_ts, SECOND)), 9223372036854775807) ASC,
      fs.fathom_call_ts DESC
  ) = 1
),
resolved AS (
  SELECT
    wf.*,
    COALESCE(wf.setter_user_id_raw, wf.phone_map_setter_user_id_raw) AS setter_user_id_resolved_raw,
    COALESCE(wf.setter_user_name_raw, wf.phone_map_setter_user_name_raw) AS setter_user_name_resolved_raw,
    COALESCE(wf.setter_user_email_raw, wf.phone_map_setter_user_email_raw) AS setter_user_email_resolved_raw,
    CASE
      WHEN COALESCE(wf.setter_user_id_raw, wf.phone_map_setter_user_id_raw) IS NOT NULL THEN CONCAT('ghl_user:', LOWER(COALESCE(wf.setter_user_id_raw, wf.phone_map_setter_user_id_raw)))
      WHEN COALESCE(wf.setter_user_email_raw, wf.phone_map_setter_user_email_raw) IS NOT NULL THEN CONCAT('email:', LOWER(COALESCE(wf.setter_user_email_raw, wf.phone_map_setter_user_email_raw)))
      WHEN COALESCE(wf.setter_user_name_raw, wf.phone_map_setter_user_name_raw) IS NOT NULL THEN CONCAT('name:', LOWER(COALESCE(wf.setter_user_name_raw, wf.phone_map_setter_user_name_raw)))
      WHEN wf.fathom_team_member_key IS NOT NULL THEN wf.fathom_team_member_key
      WHEN wf.first_touch_source_raw = 'workflow' THEN 'automation'
      ELSE 'unknown_setter'
    END AS setter_team_member_key,
    CASE
      WHEN wf.setter_user_id_raw IS NOT NULL OR wf.setter_user_email_raw IS NOT NULL OR wf.setter_user_name_raw IS NOT NULL THEN 'ghl_outbound'
      WHEN wf.phone_map_setter_team_member_key IS NOT NULL THEN 'ghl_sender_phone_map'
      WHEN wf.fathom_team_member_key IS NOT NULL THEN 'fathom_fallback'
      WHEN wf.first_touch_source_raw = 'workflow' THEN 'workflow_automation'
      ELSE 'unknown'
    END AS setter_attribution_method
  FROM with_fathom_fallback wf
)
SELECT
  r.trigger_event_id,
  r.golden_contact_key,
  r.location_id,
  r.ghl_contact_id,
  r.trigger_type,
  CASE
    WHEN r.trigger_type = 'lead_magnet' THEN 'inbound_to_dial'
    WHEN r.trigger_type = 'appointment_booking' THEN 'booking_to_dial'
    ELSE 'trigger_to_dial'
  END AS interval_type,
  r.trigger_ts,
  DATE(r.trigger_ts) AS trigger_date,
  r.source_label AS trigger_source_label,
  r.source_id AS trigger_source_id,
  r.utm_source,
  r.utm_medium,
  r.utm_campaign,
  r.utm_content,
  r.first_touch_ts,
  r.first_touch_channel,
  r.first_touch_status,
  r.first_touch_id,
  CASE
    WHEN r.first_touch_ts IS NOT NULL THEN TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND)
    ELSE NULL
  END AS speed_to_lead_seconds,
  CASE
    WHEN r.first_touch_ts IS NOT NULL THEN ROUND(SAFE_DIVIDE(TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND), 60.0), 2)
    ELSE NULL
  END AS speed_to_lead_minutes,
  CASE
    WHEN r.first_touch_ts IS NULL THEN 'No Outbound Yet'
    WHEN TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND) <= 300 THEN 'Gold (<5m)'
    WHEN TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND) <= 1800 THEN 'Silver (5-30m)'
    WHEN TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND) <= 7200 THEN 'Bronze (30m-2h)'
    ELSE 'SLA Breached (2h+)'
  END AS sla_status,
  CASE
    WHEN r.first_touch_ts IS NOT NULL AND TIMESTAMP_DIFF(r.first_touch_ts, r.trigger_ts, SECOND) <= 300 THEN TRUE
    ELSE FALSE
  END AS is_within_sla,
  r.setter_team_member_key,
  r.setter_attribution_method,
  r.setter_user_id_resolved_raw AS setter_user_id_raw,
  r.setter_user_name_resolved_raw AS setter_user_name_raw,
  r.setter_user_email_resolved_raw AS setter_user_email_raw,
  r.fathom_recorded_by_email AS setter_fathom_email_fallback,
  r.fathom_recorded_by_name AS setter_fathom_name_fallback,
  tm.ghl_user_id AS setter_dim_ghl_user_id,
  tm.email AS setter_dim_email,
  tm.phone AS setter_dim_phone,
  tm.display_name AS setter_dim_display_name,
  tm.primary_source_system AS setter_dim_primary_source_system,
  tm.seen_in_ghl_call AS setter_seen_in_ghl_call,
  tm.seen_in_ghl_sms AS setter_seen_in_ghl_sms,
  tm.seen_in_fathom_call AS setter_seen_in_fathom_call,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM resolved r
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.team_member_key = r.setter_team_member_key
;

-- Speed-to-lead daily KPI rollup for Looker Studio.
-- Compatibility note:
-- Legacy booking KPI column names are retained, but now they are sourced from the
-- event-grain speed fact to keep visibility aligned with lead-magnet + booking triggers.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.mrt_speed_to_lead_daily` AS
SELECT
  trigger_date AS booking_date,
  COUNTIF(trigger_type = 'appointment_booking') AS total_bookings_matched_to_contact,
  COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NOT NULL) AS bookings_with_outbound_call,
  COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NULL) AS bookings_without_outbound_call,
  ROUND(AVG(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL)), 2) AS avg_speed_to_lead_minutes,
  ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(50)], 2) AS median_speed_to_lead_minutes,
  ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(90)], 2) AS p90_speed_to_lead_minutes,
  COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 60) AS sla_within_1m,
  COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 300) AS sla_within_5m,
  COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 900) AS sla_within_15m,
  COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 3600) AS sla_within_60m,
  COUNT(*) AS total_triggers_all,
  COUNTIF(first_touch_ts IS NOT NULL) AS triggers_with_outbound_touch,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)), 2) AS pct_triggers_with_outbound_touch,
  COUNTIF(trigger_type = 'lead_magnet') AS total_lead_magnet_triggers,
  COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL) AS lead_magnet_triggers_with_outbound_touch,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL),
      NULLIF(COUNTIF(trigger_type = 'lead_magnet'), 0)
    ),
    2
  ) AS pct_lead_magnet_triggers_with_outbound_touch
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
GROUP BY booking_date
ORDER BY booking_date DESC
;

-- Speed-to-lead overall KPI snapshot for Looker Studio scorecards.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.mrt_speed_to_lead_overall` AS
SELECT
  CURRENT_TIMESTAMP() AS refreshed_at,
  COUNTIF(trigger_type = 'appointment_booking') AS total_bookings_matched_to_contact,
  COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NOT NULL) AS bookings_with_outbound_call,
  COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NULL) AS bookings_without_outbound_call,
  ROUND(AVG(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL)), 2) AS avg_speed_to_lead_minutes,
  ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(50)], 2) AS median_speed_to_lead_minutes,
  ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(90)], 2) AS p90_speed_to_lead_minutes,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 300),
      NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
    ),
    2
  ) AS pct_within_5m,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 900),
      NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
    ),
    2
  ) AS pct_within_15m,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 3600),
      NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
    ),
    2
  ) AS pct_within_60m,
  COUNT(*) AS total_triggers_all,
  COUNTIF(first_touch_ts IS NOT NULL) AS triggers_with_outbound_touch,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)), 2) AS pct_triggers_with_outbound_touch,
  COUNTIF(trigger_type = 'lead_magnet') AS total_lead_magnet_triggers,
  COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL) AS lead_magnet_triggers_with_outbound_touch,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL),
      NULLIF(COUNTIF(trigger_type = 'lead_magnet'), 0)
    ),
    2
  ) AS pct_lead_magnet_triggers_with_outbound_touch
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
;

-- Monthly funnel by campaign bucket (lead grain from golden contact; revenue from unified payment lines).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_campaign_funnel_month` AS
WITH lead_month_current AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts)), MONTH) AS activity_month,
    'current' AS attribution_method,
    gc.campaign_reporting AS campaign_reporting,
    gc.golden_contact_key,
    gc.has_calendly_booking,
    gc.has_fanbasis_payment,
    gc.meetings_booked_cnt,
    gc.meetings_showed_cnt
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
  WHERE COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts) IS NOT NULL
),
lead_month_first_touch AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts)), MONTH) AS activity_month,
    'first_touch' AS attribution_method,
    gc.campaign_reporting_first_touch AS campaign_reporting,
    gc.golden_contact_key,
    gc.has_calendly_booking,
    gc.has_fanbasis_payment,
    gc.meetings_booked_cnt,
    gc.meetings_showed_cnt
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
  WHERE COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts) IS NOT NULL
),
lead_month AS (
  SELECT * FROM lead_month_current
  UNION ALL
  SELECT * FROM lead_month_first_touch
),
rev_month_current AS (
  SELECT
    DATE_TRUNC(DATE(p.event_ts), MONTH) AS revenue_month,
    'current' AS attribution_method,
    CASE
      WHEN p.match_status = 'matched' THEN COALESCE(NULLIF(TRIM(p.attributed_campaign_snapshot), ''), 'Unknown')
      ELSE '(Unattributed / Direct Sale)'
    END AS campaign_reporting,
    SUM(SAFE_CAST(p.net_amount AS NUMERIC)) AS revenue_net_sum,
    SUM(CASE WHEN p.source_system = 'fanbasis' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS fanbasis_revenue_net_sum,
    SUM(CASE WHEN p.source_system = 'stripe' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS stripe_revenue_net_sum,
    COUNT(DISTINCT CONCAT(p.source_system, '|', p.payment_id)) AS transaction_count,
    COUNT(DISTINCT CASE WHEN p.source_system = 'fanbasis' THEN p.payment_id END) AS fanbasis_transaction_count,
    COUNT(DISTINCT CASE WHEN p.source_system = 'stripe' THEN p.payment_id END) AS stripe_transaction_count,
    COUNT(DISTINCT p.golden_contact_key) AS paying_distinct_golden_contacts
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` p
  WHERE p.event_ts IS NOT NULL
  GROUP BY 1, 2, 3
),
rev_month_first_touch AS (
  SELECT
    DATE_TRUNC(DATE(p.event_ts), MONTH) AS revenue_month,
    'first_touch' AS attribution_method,
    CASE
      WHEN p.match_status = 'matched' THEN COALESCE(NULLIF(TRIM(p.attributed_campaign_snapshot_first_touch), ''), 'Unknown')
      ELSE '(Unattributed / Direct Sale)'
    END AS campaign_reporting,
    SUM(SAFE_CAST(p.net_amount AS NUMERIC)) AS revenue_net_sum,
    SUM(CASE WHEN p.source_system = 'fanbasis' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS fanbasis_revenue_net_sum,
    SUM(CASE WHEN p.source_system = 'stripe' THEN SAFE_CAST(p.net_amount AS NUMERIC) ELSE 0 END) AS stripe_revenue_net_sum,
    COUNT(DISTINCT CONCAT(p.source_system, '|', p.payment_id)) AS transaction_count,
    COUNT(DISTINCT CASE WHEN p.source_system = 'fanbasis' THEN p.payment_id END) AS fanbasis_transaction_count,
    COUNT(DISTINCT CASE WHEN p.source_system = 'stripe' THEN p.payment_id END) AS stripe_transaction_count,
    COUNT(DISTINCT p.golden_contact_key) AS paying_distinct_golden_contacts
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` p
  WHERE p.event_ts IS NOT NULL
  GROUP BY 1, 2, 3
),
rev_month AS (
  SELECT * FROM rev_month_current
  UNION ALL
  SELECT * FROM rev_month_first_touch
),
lead_agg AS (
  SELECT
    activity_month AS report_month,
    attribution_method,
    campaign_reporting,
    COUNT(DISTINCT golden_contact_key) AS new_leads_distinct,
    COUNTIF(has_calendly_booking) AS leads_with_booking,
    COUNTIF(has_fanbasis_payment) AS leads_with_fanbasis_payment_flag,
    SUM(meetings_booked_cnt) AS sum_meeting_slots_booked,
    SUM(meetings_showed_cnt) AS sum_meeting_slots_showed
  FROM lead_month
  GROUP BY 1, 2, 3
)
SELECT
  COALESCE(l.report_month, r.revenue_month) AS report_month,
  COALESCE(l.attribution_method, r.attribution_method) AS attribution_method,
  COALESCE(l.campaign_reporting, r.campaign_reporting) AS campaign_reporting,
  COALESCE(l.new_leads_distinct, 0) AS new_leads_distinct,
  COALESCE(l.leads_with_booking, 0) AS leads_with_booking,
  COALESCE(l.leads_with_fanbasis_payment_flag, 0) AS leads_with_fanbasis_payment_flag,
  COALESCE(l.sum_meeting_slots_booked, 0) AS sum_meeting_slots_booked,
  COALESCE(l.sum_meeting_slots_showed, 0) AS sum_meeting_slots_showed,
  SAFE_DIVIDE(
    COALESCE(l.sum_meeting_slots_showed, 0),
    NULLIF(COALESCE(l.sum_meeting_slots_booked, 0), 0)
  ) AS slot_level_show_rate,
  COALESCE(r.revenue_net_sum, 0) AS revenue_net_sum,
  COALESCE(r.fanbasis_revenue_net_sum, 0) AS fanbasis_revenue_net_sum,
  COALESCE(r.stripe_revenue_net_sum, 0) AS stripe_revenue_net_sum,
  COALESCE(r.transaction_count, 0) AS total_transaction_count,
  COALESCE(r.fanbasis_transaction_count, 0) AS fanbasis_transaction_count,
  COALESCE(r.stripe_transaction_count, 0) AS stripe_transaction_count,
  COALESCE(r.paying_distinct_golden_contacts, 0) AS paying_distinct_golden_contacts,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM lead_agg l
FULL OUTER JOIN rev_month r
  ON r.revenue_month = l.report_month
 AND r.attribution_method = l.attribution_method
 AND r.campaign_reporting = l.campaign_reporting
WHERE COALESCE(l.report_month, r.revenue_month) IS NOT NULL
;

-- Revenue by stage month (surface GHL stage context alongside unified cash).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_revenue_by_stage_month` AS
WITH latest_stage AS (
  SELECT
    o.location_id,
    o.contact_id AS ghl_contact_id,
    COALESCE(NULLIF(TRIM(o.pipeline_name), ''), 'Unknown Pipeline') AS pipeline_name,
    COALESCE(NULLIF(TRIM(o.stage_name), ''), 'Unknown Stage') AS stage_name,
    COALESCE(NULLIF(TRIM(o.status), ''), 'unknown') AS opportunity_status,
    ROW_NUMBER() OVER (
      PARTITION BY o.location_id, o.contact_id
      ORDER BY COALESCE(o.last_stage_change_at, o.last_status_change_at, o.updated_at_ts, o.event_ts, o.ingested_at) DESC
    ) AS rn
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities` o
  WHERE o.contact_id IS NOT NULL
),
payment_with_stage AS (
  SELECT
    DATE_TRUNC(DATE(p.event_ts), MONTH) AS report_month,
    p.source_system,
    p.payment_id,
    p.golden_contact_key,
    SAFE_CAST(p.net_amount AS NUMERIC) AS net_amount,
    CASE
      WHEN p.match_status = 'matched' THEN COALESCE(s.pipeline_name, 'Unknown Pipeline')
      ELSE '(Unattributed / Direct Sale)'
    END AS pipeline_name,
    CASE
      WHEN p.match_status = 'matched' THEN COALESCE(s.stage_name, 'Unknown Stage')
      ELSE '(No CRM Stage)'
    END AS stage_name,
    CASE
      WHEN p.match_status = 'matched' THEN COALESCE(s.opportunity_status, 'unknown')
      ELSE 'unattributed'
    END AS opportunity_status
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified` p
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.golden_contact_key = p.golden_contact_key
  LEFT JOIN latest_stage s
    ON s.location_id = gc.location_id
   AND s.ghl_contact_id = gc.ghl_contact_id
   AND s.rn = 1
  WHERE p.event_ts IS NOT NULL
)
SELECT
  report_month,
  pipeline_name,
  stage_name,
  opportunity_status,
  COUNT(DISTINCT CONCAT(source_system, '|', payment_id)) AS total_transaction_count,
  COUNT(DISTINCT golden_contact_key) AS paying_distinct_golden_contacts,
  SUM(net_amount) AS revenue_net_sum,
  SUM(CASE WHEN source_system = 'fanbasis' THEN net_amount ELSE 0 END) AS fanbasis_revenue_net_sum,
  SUM(CASE WHEN source_system = 'stripe' THEN net_amount ELSE 0 END) AS stripe_revenue_net_sum,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM payment_with_stage
GROUP BY 1, 2, 3, 4
;

-- Applications by month from GHL form submissions (governed "applications submitted" metric).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_applications_month` AS
SELECT
  DATE_TRUNC(DATE(COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at)), MONTH) AS report_month,
  COALESCE(NULLIF(TRIM(f.form_name), ''), CONCAT('form_id:', COALESCE(f.form_id, 'unknown'))) AS form_name,
  COALESCE(NULLIF(TRIM(f.source), ''), 'unknown') AS submission_source,
  COALESCE(NULLIF(TRIM(f.utm_campaign), ''), 'unknown') AS utm_campaign,
  COUNT(*) AS submission_count,
  COUNT(DISTINCT f.submission_id) AS distinct_submission_count,
  COUNT(DISTINCT f.contact_id) AS distinct_contact_count,
  COUNTIF(f.opportunity_id IS NOT NULL) AS linked_opportunity_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_form_submissions` f
WHERE COALESCE(f.event_ts, f.updated_at_ts, f.ingested_at) IS NOT NULL
GROUP BY 1, 2, 3, 4
;

-- Outbound call outcomes by week (source of truth for dials/pickups semantics).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_call_outcome_week` AS
WITH call_base AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at)), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(c.direction_norm), ''), 'unknown') AS direction_norm,
    COALESCE(NULLIF(TRIM(c.channel_norm), ''), 'unknown') AS channel_norm,
    COALESCE(NULLIF(TRIM(c.call_status), ''), 'unknown') AS call_status,
    c.call_log_id,
    c.contact_id,
    c.location_id,
    COALESCE(
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.assignedTo')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.assigned_to')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.userId')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.user_id')), '')
    ) AS owner_id
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  WHERE COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) IS NOT NULL
)
SELECT
  report_week,
  location_id,
  owner_id,
  direction_norm,
  channel_norm,
  call_status,
  CASE
    WHEN REGEXP_CONTAINS(LOWER(call_status), r'answer|connected|complete|completed') THEN 'connected'
    WHEN REGEXP_CONTAINS(LOWER(call_status), r'no.?answer|missed|busy|voicemail|failed|canceled|cancelled') THEN 'not_connected'
    ELSE 'other'
  END AS outcome_bucket,
  COUNT(*) AS dial_count,
  COUNT(DISTINCT call_log_id) AS distinct_call_log_count,
  COUNT(DISTINCT contact_id) AS distinct_contact_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM call_base
GROUP BY 1, 2, 3, 4, 5, 6, 7
;

-- Calendly invitee status detail by week (booked/canceled/showed/no-show proxies).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_calendly_status_week` AS
WITH invitee_base AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at)), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(i.event_status), ''), 'unknown') AS event_status,
    COALESCE(NULLIF(TRIM(i.invitee_status), ''), 'unknown') AS invitee_status,
    i.invitee_id,
    i.scheduled_event_id,
    i.event_name,
    i.scheduled_start_time
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  WHERE COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
)
SELECT
  report_week,
  event_name,
  event_status,
  invitee_status,
  CASE
    WHEN LOWER(event_status) IN ('canceled', 'cancelled') OR LOWER(invitee_status) IN ('canceled', 'cancelled') THEN 'canceled'
    WHEN LOWER(event_status) IN ('rescheduled') THEN 'rescheduled'
    WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
      AND LOWER(invitee_status) = 'active'
      AND LOWER(event_status) NOT IN ('canceled', 'cancelled') THEN 'showed_proxy'
    WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
      AND LOWER(invitee_status) <> 'active'
      AND LOWER(event_status) NOT IN ('canceled', 'cancelled') THEN 'no_show_proxy'
    ELSE 'scheduled_future_or_unknown'
  END AS attendance_bucket,
  COUNT(*) AS invitee_count,
  COUNT(DISTINCT scheduled_event_id) AS distinct_scheduled_event_count,
  COUNT(DISTINCT invitee_id) AS distinct_invitee_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM invitee_base
GROUP BY 1, 2, 3, 4, 5
;

-- Payment reconciliation month (surface Stripe non-charge objects + unified net in one layer).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_payment_reconciliation_month` AS
WITH stripe_payments AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    'stripe' AS source_system,
    SUM(SAFE_CAST(COALESCE(amount_captured, amount, 0) AS NUMERIC)) AS gross_collected_amount,
    COUNT(DISTINCT payment_id) AS payment_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments`
  WHERE event_ts IS NOT NULL
    AND (COALESCE(paid, FALSE) OR LOWER(COALESCE(status, '')) IN ('succeeded', 'paid'))
  GROUP BY 1, 2
),
stripe_refunds AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    'stripe' AS source_system,
    SUM(SAFE_CAST(amount AS NUMERIC)) AS refund_amount,
    COUNT(DISTINCT refund_id) AS refund_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_refunds`
  WHERE event_ts IS NOT NULL
  GROUP BY 1, 2
),
stripe_disputes AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    'stripe' AS source_system,
    SUM(SAFE_CAST(amount AS NUMERIC)) AS dispute_amount,
    COUNT(DISTINCT dispute_id) AS dispute_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_disputes`
  WHERE event_ts IS NOT NULL
  GROUP BY 1, 2
),
fanbasis_payments AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    'fanbasis' AS source_system,
    SUM(SAFE_CAST(amount AS NUMERIC)) AS gross_collected_amount,
    COUNT(DISTINCT transaction_id) AS payment_count
  FROM `project-41542e21-470f-4589-96d.Marts.fct_fanbasis_payment_line`
  WHERE event_ts IS NOT NULL
  GROUP BY 1, 2
),
unified_net AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    source_system,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS unified_net_amount
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE event_ts IS NOT NULL
  GROUP BY 1, 2
),
base AS (
  SELECT * FROM stripe_payments
  UNION ALL
  SELECT * FROM fanbasis_payments
)
SELECT
  b.report_month,
  b.source_system,
  b.gross_collected_amount,
  COALESCE(r.refund_amount, 0) AS refund_amount,
  COALESCE(d.dispute_amount, 0) AS dispute_amount,
  b.gross_collected_amount - COALESCE(r.refund_amount, 0) - COALESCE(d.dispute_amount, 0) AS adjusted_net_amount,
  COALESCE(u.unified_net_amount, 0) AS unified_net_amount,
  b.payment_count,
  COALESCE(r.refund_count, 0) AS refund_count,
  COALESCE(d.dispute_count, 0) AS dispute_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM base b
LEFT JOIN stripe_refunds r
  ON r.report_month = b.report_month
 AND r.source_system = b.source_system
LEFT JOIN stripe_disputes d
  ON d.report_month = b.report_month
 AND d.source_system = b.source_system
LEFT JOIN unified_net u
  ON u.report_month = b.report_month
 AND u.source_system = b.source_system
;

-- Identity quality by day + source (matched/ambiguous/no-match visibility).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_identity_quality_daily` AS
WITH payment_daily AS (
  SELECT
    DATE(COALESCE(event_ts, mart_refreshed_at)) AS report_date,
    source_system,
    match_status,
    COUNT(*) AS payment_line_count,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_amount_sum
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  GROUP BY 1, 2, 3
),
bridge_snapshot AS (
  SELECT
    DATE(MAX(bridge_refreshed_at)) AS report_date,
    bridge_status,
    COUNT(*) AS customer_count
  FROM `project-41542e21-470f-4589-96d.Marts.bridge_identity_contact_payment`
  GROUP BY 2
)
SELECT
  p.report_date,
  p.source_system,
  p.match_status,
  p.payment_line_count,
  p.net_amount_sum,
  SAFE_DIVIDE(
    p.net_amount_sum,
    NULLIF(SUM(p.net_amount_sum) OVER (PARTITION BY p.report_date, p.source_system), 0)
  ) AS net_amount_share_within_source_day,
  b.bridge_status AS latest_bridge_status,
  b.customer_count AS latest_bridge_customer_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM payment_daily p
LEFT JOIN bridge_snapshot b
  ON b.report_date = p.report_date
;

-- GHL activity by week (tasks, notes, conversations, outbound calls).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_ghl_activity_week` AS
WITH task_rollup AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(event_ts, updated_at_ts, ingested_at)), WEEK(MONDAY)) AS report_week,
    location_id,
    COUNT(*) AS task_count,
    COUNTIF(task_status = 'completed' OR completed_at IS NOT NULL) AS task_completed_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_tasks`
  GROUP BY 1, 2
),
note_rollup AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(note_created_at, event_ts, updated_at_ts, ingested_at)), WEEK(MONDAY)) AS report_week,
    location_id,
    COUNT(*) AS note_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_notes`
  GROUP BY 1, 2
),
conversation_rollup AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at)), WEEK(MONDAY)) AS report_week,
    location_id,
    COUNT(*) AS conversation_message_count,
    COUNTIF(direction_norm = 'outbound') AS outbound_message_count,
    COUNTIF(direction_norm = 'inbound') AS inbound_message_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations`
  GROUP BY 1, 2
),
call_rollup AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(call_started_at, event_ts, updated_at_ts, ingested_at)), WEEK(MONDAY)) AS report_week,
    location_id,
    COUNT(*) AS outbound_call_count,
    COUNTIF(REGEXP_CONTAINS(LOWER(COALESCE(call_status, '')), r'answer|connected|complete|completed')) AS connected_call_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls`
  GROUP BY 1, 2
)
SELECT
  COALESCE(t.report_week, n.report_week, c.report_week, o.report_week) AS report_week,
  COALESCE(t.location_id, n.location_id, c.location_id, o.location_id) AS location_id,
  COALESCE(t.task_count, 0) AS task_count,
  COALESCE(t.task_completed_count, 0) AS task_completed_count,
  COALESCE(n.note_count, 0) AS note_count,
  COALESCE(c.conversation_message_count, 0) AS conversation_message_count,
  COALESCE(c.outbound_message_count, 0) AS outbound_message_count,
  COALESCE(c.inbound_message_count, 0) AS inbound_message_count,
  COALESCE(o.outbound_call_count, 0) AS outbound_call_count,
  COALESCE(o.connected_call_count, 0) AS connected_call_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM task_rollup t
FULL OUTER JOIN note_rollup n
  ON n.report_week = t.report_week
 AND n.location_id = t.location_id
FULL OUTER JOIN conversation_rollup c
  ON c.report_week = COALESCE(t.report_week, n.report_week)
 AND c.location_id = COALESCE(t.location_id, n.location_id)
FULL OUTER JOIN call_rollup o
  ON o.report_week = COALESCE(t.report_week, n.report_week, c.report_week)
 AND o.location_id = COALESCE(t.location_id, n.location_id, c.location_id)
WHERE COALESCE(t.report_week, n.report_week, c.report_week, o.report_week) IS NOT NULL
;

-- Calendly routing form submissions by week.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_calendly_routing_week` AS
SELECT
  DATE_TRUNC(DATE(COALESCE(submission_created_at, event_ts, updated_at_ts, ingested_at)), WEEK(MONDAY)) AS report_week,
  COALESCE(NULLIF(TRIM(routing_form_name), ''), 'unknown') AS routing_form_name,
  COALESCE(NULLIF(TRIM(routing_form_status), ''), 'unknown') AS routing_form_status,
  COUNT(*) AS submission_count,
  COUNT(DISTINCT routing_form_submission_id) AS distinct_submission_count,
  COUNT(DISTINCT routing_form_id) AS distinct_routing_form_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_routing_form_submissions`
GROUP BY 1, 2, 3
;

-- Stripe lifecycle by month (invoices + subscriptions + payment collections).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_stripe_lifecycle_month` AS
WITH invoice_rollup AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    COUNT(*) AS invoice_count,
    COUNTIF(LOWER(COALESCE(status, '')) = 'paid') AS invoice_paid_count,
    SUM(SAFE_CAST(amount_due AS NUMERIC)) AS invoice_amount_due_sum,
    SUM(SAFE_CAST(amount_paid AS NUMERIC)) AS invoice_amount_paid_sum
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_invoices`
  WHERE event_ts IS NOT NULL
  GROUP BY 1
),
subscription_rollup AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(event_ts, current_period_start_ts)), MONTH) AS report_month,
    COUNT(*) AS subscription_row_count,
    COUNTIF(LOWER(COALESCE(status, '')) IN ('active', 'trialing', 'past_due')) AS subscription_active_like_count,
    COUNTIF(COALESCE(cancel_at_period_end, FALSE)) AS subscription_cancel_at_period_end_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_subscriptions`
  WHERE COALESCE(event_ts, current_period_start_ts) IS NOT NULL
  GROUP BY 1
),
payment_rollup AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    COUNT(*) AS payment_count,
    SUM(SAFE_CAST(COALESCE(amount_captured, amount, 0) AS NUMERIC)) AS payment_gross_collected_sum,
    SUM(SAFE_CAST(COALESCE(amount_refunded, 0) AS NUMERIC)) AS payment_refunded_sum
  FROM `project-41542e21-470f-4589-96d.Core.fct_stripe_payments`
  WHERE event_ts IS NOT NULL
    AND (COALESCE(paid, FALSE) OR LOWER(COALESCE(status, '')) IN ('succeeded', 'paid'))
  GROUP BY 1
)
SELECT
  COALESCE(i.report_month, s.report_month, p.report_month) AS report_month,
  COALESCE(i.invoice_count, 0) AS invoice_count,
  COALESCE(i.invoice_paid_count, 0) AS invoice_paid_count,
  COALESCE(i.invoice_amount_due_sum, 0) AS invoice_amount_due_sum,
  COALESCE(i.invoice_amount_paid_sum, 0) AS invoice_amount_paid_sum,
  COALESCE(s.subscription_row_count, 0) AS subscription_row_count,
  COALESCE(s.subscription_active_like_count, 0) AS subscription_active_like_count,
  COALESCE(s.subscription_cancel_at_period_end_count, 0) AS subscription_cancel_at_period_end_count,
  COALESCE(p.payment_count, 0) AS payment_count,
  COALESCE(p.payment_gross_collected_sum, 0) AS payment_gross_collected_sum,
  COALESCE(p.payment_refunded_sum, 0) AS payment_refunded_sum,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM invoice_rollup i
FULL OUTER JOIN subscription_rollup s
  ON s.report_month = i.report_month
FULL OUTER JOIN payment_rollup p
  ON p.report_month = COALESCE(i.report_month, s.report_month)
WHERE COALESCE(i.report_month, s.report_month, p.report_month) IS NOT NULL
;

-- Fanbasis customer cohort month (customer profile + value progression).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_fanbasis_customer_month` AS
WITH customer_month AS (
  SELECT
    DATE_TRUNC(DATE(first_seen_ts), MONTH) AS cohort_month,
    COUNT(*) AS new_customer_count,
    COUNTIF(email IS NOT NULL AND TRIM(email) <> '') AS new_customer_with_email_count,
    COUNTIF(phone IS NOT NULL AND TRIM(phone) <> '') AS new_customer_with_phone_count
  FROM `project-41542e21-470f-4589-96d.Core.dim_fanbasis_customers`
  WHERE first_seen_ts IS NOT NULL
  GROUP BY 1
),
txn_month AS (
  SELECT
    DATE_TRUNC(DATE(event_ts), MONTH) AS report_month,
    COUNT(DISTINCT customer_id) AS transacting_customer_count,
    COUNT(*) AS transaction_count,
    SUM(SAFE_CAST(net_amount AS NUMERIC)) AS net_amount_sum
  FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_transactions`
  WHERE event_ts IS NOT NULL
  GROUP BY 1
),
refund_month AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(refund_created_at, transaction_event_ts)), MONTH) AS report_month,
    COUNT(*) AS refund_count,
    SUM(SAFE_CAST(refund_amount AS NUMERIC)) AS refund_amount_sum
  FROM `project-41542e21-470f-4589-96d.Core.fct_fanbasis_refunds`
  WHERE COALESCE(refund_created_at, transaction_event_ts) IS NOT NULL
  GROUP BY 1
)
SELECT
  COALESCE(c.cohort_month, t.report_month, r.report_month) AS report_month,
  COALESCE(c.new_customer_count, 0) AS new_customer_count,
  COALESCE(c.new_customer_with_email_count, 0) AS new_customer_with_email_count,
  COALESCE(c.new_customer_with_phone_count, 0) AS new_customer_with_phone_count,
  COALESCE(t.transacting_customer_count, 0) AS transacting_customer_count,
  COALESCE(t.transaction_count, 0) AS transaction_count,
  COALESCE(t.net_amount_sum, 0) AS net_amount_sum,
  COALESCE(r.refund_count, 0) AS refund_count,
  COALESCE(r.refund_amount_sum, 0) AS refund_amount_sum,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM customer_month c
FULL OUTER JOIN txn_month t
  ON t.report_month = c.cohort_month
FULL OUTER JOIN refund_month r
  ON r.report_month = COALESCE(c.cohort_month, t.report_month)
WHERE COALESCE(c.cohort_month, t.report_month, r.report_month) IS NOT NULL
;

-- Fathom outcomes by week (actionability + close context).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_fathom_outcomes_week` AS
WITH outcome_base AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(o.ended_at_ts, o.event_ts)), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(o.classification_label), ''), 'unknown') AS classification_label,
    o.call_id,
    o.action_item_count,
    o.next_step_count,
    o.question_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_call_outcomes` o
  WHERE COALESCE(o.ended_at_ts, o.event_ts) IS NOT NULL
),
sales_context AS (
  SELECT
    src.call_id,
    CAST(JSON_VALUE(src.sales_json, '$.opportunity_id') AS STRING) AS opportunity_id,
    COALESCE(NULLIF(TRIM(src.stage_name), ''), 'unknown') AS stage_name,
    COALESCE(NULLIF(TRIM(src.opportunity_status), ''), 'unknown') AS opportunity_status,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.classification_confidence') AS FLOAT64) AS classification_confidence,
    COALESCE(
      NULLIF(TRIM(CAST(JSON_VALUE(src.sales_json, '$.resolved_classification_label') AS STRING)), ''),
      NULLIF(TRIM(src.classification_label), ''),
      'unknown'
    ) AS resolved_classification_label,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.resolved_classification_confidence') AS FLOAT64) AS resolved_classification_confidence,
    COALESCE(
      NULLIF(TRIM(CAST(JSON_VALUE(src.sales_json, '$.classification_source') AS STRING)), ''),
      'unknown'
    ) AS classification_source,
    COALESCE(SAFE_CAST(JSON_VALUE(src.sales_json, '$.is_sales_meeting_resolved') AS BOOL), FALSE) AS is_sales_meeting_resolved,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_stage_after_call') AS BOOL) AS moved_stage_after_call,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_stage_within_24h') AS BOOL) AS moved_stage_within_24h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_stage_within_48h') AS BOOL) AS moved_stage_within_48h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_status_after_call') AS BOOL) AS moved_status_after_call,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_status_within_24h') AS BOOL) AS moved_status_within_24h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_status_within_48h') AS BOOL) AS moved_status_within_48h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.pipeline_progressed_after_call') AS BOOL) AS pipeline_progressed_after_call,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.pipeline_progressed_within_24h') AS BOOL) AS pipeline_progressed_within_24h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.pipeline_progressed_within_48h') AS BOOL) AS pipeline_progressed_within_48h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.hours_to_stage_change_from_call') AS FLOAT64) AS hours_to_stage_change_from_call,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.hours_to_status_change_from_call') AS FLOAT64) AS hours_to_status_change_from_call
  FROM (
    SELECT
      f.*,
      TO_JSON_STRING(f) AS sales_json
    FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` f
  ) src
),
aggregated AS (
  SELECT
    o.report_week,
    COALESCE(NULLIF(TRIM(s.resolved_classification_label), ''), o.classification_label, 'unknown') AS classification_label,
    COALESCE(s.stage_name, 'unknown') AS stage_name,
    COALESCE(s.opportunity_status, 'unknown') AS opportunity_status,
    COUNT(*) AS call_count,
    COUNTIF(COALESCE(s.is_sales_meeting_resolved, FALSE)) AS revenue_relevant_call_count,
    COUNTIF(COALESCE(s.is_sales_meeting_resolved, FALSE) AND COALESCE(s.resolved_classification_confidence, s.classification_confidence, 0.0) >= 0.80) AS high_confidence_revenue_call_count,
    COUNTIF(LOWER(COALESCE(s.classification_source, '')) IN ('calendly_event_type', 'calendly_contact_anchor')) AS calls_classified_by_calendly_anchor,
    COUNTIF(LOWER(COALESCE(s.classification_source, '')) = 'behavioral_promotion') AS calls_promoted_behavioral,
    COUNTIF(LOWER(COALESCE(s.classification_source, '')) IN (
      'fathom_fallback',
      'unclassified',
      'non_sales_deterministic',
      'sales_model_high',
      'non_sales_model_high',
      'review_queue',
      'human_review_label_sales',
      'human_review_label_non_sales'
    )) AS calls_classified_by_fallback,
    COUNTIF(s.opportunity_id IS NOT NULL) AS calls_with_linked_opportunity,
    COUNTIF(LOWER(COALESCE(s.opportunity_status, '')) = 'won') AS calls_with_won_opportunity,
    SUM(COALESCE(o.action_item_count, 0)) AS action_item_count,
    SUM(COALESCE(o.next_step_count, 0)) AS next_step_count,
    SUM(COALESCE(o.question_count, 0)) AS question_count,
    COUNTIF(COALESCE(o.action_item_count, 0) > 0) AS calls_with_action_items,
    COUNTIF(COALESCE(s.moved_stage_after_call, FALSE)) AS calls_moved_stage_after_call,
    COUNTIF(COALESCE(s.moved_stage_within_24h, FALSE)) AS calls_moved_stage_within_24h,
    COUNTIF(COALESCE(s.moved_stage_within_48h, FALSE)) AS calls_moved_stage_within_48h,
    COUNTIF(COALESCE(s.moved_status_after_call, FALSE)) AS calls_moved_status_after_call,
    COUNTIF(COALESCE(s.moved_status_within_24h, FALSE)) AS calls_moved_status_within_24h,
    COUNTIF(COALESCE(s.moved_status_within_48h, FALSE)) AS calls_moved_status_within_48h,
    COUNTIF(COALESCE(s.pipeline_progressed_after_call, FALSE)) AS calls_progressed_pipeline_after_call,
    COUNTIF(COALESCE(s.pipeline_progressed_within_24h, FALSE)) AS calls_progressed_pipeline_within_24h,
    COUNTIF(COALESCE(s.pipeline_progressed_within_48h, FALSE)) AS calls_progressed_pipeline_within_48h,
    AVG(IF(COALESCE(s.moved_stage_after_call, FALSE), s.hours_to_stage_change_from_call, NULL)) AS avg_hours_to_stage_change_when_moved,
    AVG(IF(COALESCE(s.moved_status_after_call, FALSE), s.hours_to_status_change_from_call, NULL)) AS avg_hours_to_status_change_when_moved,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
  FROM outcome_base o
  LEFT JOIN sales_context s
    ON s.call_id = o.call_id
  GROUP BY 1, 2, 3, 4
)
SELECT
  a.report_week,
  a.classification_label,
  a.stage_name,
  a.opportunity_status,
  a.call_count,
  a.revenue_relevant_call_count,
  a.high_confidence_revenue_call_count,
  a.calls_classified_by_calendly_anchor,
  a.calls_promoted_behavioral,
  a.calls_classified_by_fallback,
  a.calls_with_linked_opportunity,
  a.calls_with_won_opportunity,
  a.action_item_count,
  a.next_step_count,
  a.question_count,
  a.calls_with_action_items,
  a.calls_moved_stage_after_call,
  a.calls_moved_stage_within_24h,
  a.calls_moved_stage_within_48h,
  a.calls_moved_status_after_call,
  a.calls_moved_status_within_24h,
  a.calls_moved_status_within_48h,
  a.calls_progressed_pipeline_after_call,
  a.calls_progressed_pipeline_within_24h,
  a.calls_progressed_pipeline_within_48h,
  a.avg_hours_to_stage_change_when_moved,
  a.avg_hours_to_status_change_when_moved,
  SAFE_DIVIDE(a.action_item_count, NULLIF(a.call_count, 0)) AS action_items_per_call,
  SAFE_DIVIDE(a.next_step_count, NULLIF(a.call_count, 0)) AS next_steps_per_call,
  SAFE_DIVIDE(a.question_count, NULLIF(a.call_count, 0)) AS questions_per_call,
  SAFE_DIVIDE(a.calls_classified_by_calendly_anchor, NULLIF(a.call_count, 0)) AS pct_calls_classified_by_calendly_anchor,
  SAFE_DIVIDE(a.calls_promoted_behavioral, NULLIF(a.call_count, 0)) AS pct_calls_promoted_behavioral,
  SAFE_DIVIDE(a.calls_with_won_opportunity, NULLIF(a.calls_with_linked_opportunity, 0)) AS linked_call_win_rate,
  SAFE_DIVIDE(a.calls_moved_stage_within_48h, NULLIF(a.call_count, 0)) AS pct_calls_moved_stage_within_48h,
  SAFE_DIVIDE(a.calls_moved_stage_within_48h, NULLIF(a.calls_with_linked_opportunity, 0)) AS pct_linked_calls_moved_stage_within_48h,
  SAFE_DIVIDE(a.calls_progressed_pipeline_within_48h, NULLIF(a.call_count, 0)) AS pct_calls_progressed_pipeline_within_48h,
  a.mart_refreshed_at
FROM aggregated a
;

-- Fathom closer-level weekly effectiveness (actionability + discovery quality + velocity).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_fathom_closer_effectiveness_week` AS
WITH sales_calls AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(src.ended_at_ts, src.event_ts)), WEEK(MONDAY)) AS report_week,
    src.call_id,
    COALESCE(NULLIF(TRIM(src.recorded_by_email), ''), 'unknown') AS closer_email,
    COALESCE(
      NULLIF(TRIM(src.recorded_by_name), ''),
      COALESCE(NULLIF(TRIM(src.recorded_by_email), ''), 'unknown')
    ) AS closer_name,
    COALESCE(NULLIF(TRIM(src.stage_name), ''), 'unknown') AS stage_name,
    COALESCE(NULLIF(TRIM(src.opportunity_status), ''), 'unknown') AS opportunity_status,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.resolved_classification_confidence') AS FLOAT64) AS resolved_classification_confidence,
    COALESCE(SAFE_CAST(JSON_VALUE(src.sales_json, '$.is_sales_meeting_resolved') AS BOOL), FALSE) AS is_sales_meeting_resolved,
    CAST(JSON_VALUE(src.sales_json, '$.opportunity_id') AS STRING) AS opportunity_id,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.moved_stage_within_48h') AS BOOL) AS moved_stage_within_48h,
    SAFE_CAST(JSON_VALUE(src.sales_json, '$.pipeline_progressed_within_48h') AS BOOL) AS pipeline_progressed_within_48h
  FROM (
    SELECT
      s.*,
      TO_JSON_STRING(s) AS sales_json
    FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` s
    WHERE COALESCE(s.ended_at_ts, s.event_ts) IS NOT NULL
  ) src
  WHERE COALESCE(SAFE_CAST(JSON_VALUE(src.sales_json, '$.is_sales_meeting_resolved') AS BOOL), FALSE)
),
call_outcomes AS (
  SELECT
    o.call_id,
    COALESCE(o.action_item_count, 0) AS action_item_count,
    COALESCE(o.next_step_count, 0) AS next_step_count,
    COALESCE(o.question_count, 0) AS question_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_call_outcomes` o
),
aggregated AS (
  SELECT
    s.report_week,
    s.closer_email,
    s.closer_name,
    COUNT(*) AS call_count,
    COUNTIF(COALESCE(s.resolved_classification_confidence, 0.0) >= 0.80) AS high_confidence_call_count,
    COUNTIF(s.opportunity_id IS NOT NULL) AS calls_with_linked_opportunity,
    COUNTIF(LOWER(COALESCE(s.opportunity_status, '')) = 'won') AS won_call_count,
    SUM(COALESCE(o.action_item_count, 0)) AS action_item_count,
    SUM(COALESCE(o.next_step_count, 0)) AS next_step_count,
    SUM(COALESCE(o.question_count, 0)) AS question_count,
    COUNTIF(COALESCE(s.moved_stage_within_48h, FALSE)) AS calls_moved_stage_within_48h,
    COUNTIF(COALESCE(s.pipeline_progressed_within_48h, FALSE)) AS calls_progressed_pipeline_within_48h,
    COUNTIF(LOWER(COALESCE(s.stage_name, '')) = 'discovery') AS discovery_call_count,
    SUM(IF(LOWER(COALESCE(s.stage_name, '')) = 'discovery', COALESCE(o.question_count, 0), 0)) AS discovery_question_count,
    COUNTIF(
      LOWER(COALESCE(s.stage_name, '')) = 'discovery'
      AND LOWER(COALESCE(s.opportunity_status, '')) = 'won'
    ) AS discovery_won_call_count,
    CURRENT_TIMESTAMP() AS mart_refreshed_at
  FROM sales_calls s
  LEFT JOIN call_outcomes o
    ON o.call_id = s.call_id
  GROUP BY 1, 2, 3
)
SELECT
  a.report_week,
  a.closer_email,
  a.closer_name,
  a.call_count,
  a.high_confidence_call_count,
  a.calls_with_linked_opportunity,
  a.won_call_count,
  a.action_item_count,
  a.next_step_count,
  a.question_count,
  a.calls_moved_stage_within_48h,
  a.calls_progressed_pipeline_within_48h,
  a.discovery_call_count,
  a.discovery_question_count,
  a.discovery_won_call_count,
  SAFE_DIVIDE(a.action_item_count, NULLIF(a.call_count, 0)) AS action_items_per_call,
  SAFE_DIVIDE(a.next_step_count, NULLIF(a.call_count, 0)) AS next_steps_per_call,
  SAFE_DIVIDE(a.question_count, NULLIF(a.call_count, 0)) AS questions_per_call,
  SAFE_DIVIDE(a.won_call_count, NULLIF(a.calls_with_linked_opportunity, 0)) AS linked_call_win_rate,
  SAFE_DIVIDE(a.calls_moved_stage_within_48h, NULLIF(a.call_count, 0)) AS pct_calls_moved_stage_within_48h,
  SAFE_DIVIDE(a.calls_progressed_pipeline_within_48h, NULLIF(a.call_count, 0)) AS pct_calls_progressed_pipeline_within_48h,
  SAFE_DIVIDE(a.discovery_question_count, NULLIF(a.discovery_call_count, 0)) AS discovery_questions_per_call,
  SAFE_DIVIDE(a.discovery_won_call_count, NULLIF(a.discovery_call_count, 0)) AS discovery_win_rate,
  a.mart_refreshed_at
FROM aggregated a
;

-- ─────────────────────────────────────────────────────────────────────────────
-- fct_deal_attribution
-- One row per payment, attributed to the most recent confirmed sales call
-- within 60 days before payment for that contact.
-- Surfaces: who set, who closed, which offer, how much.
-- Setter identity: Calendly calendar slug → rep name.
-- Closer identity: Fathom recorded_by_email (precisionscaling.io domain).
-- Offer: Core.dim_offer_resolved at Calendly invitee grain.
-- Set confirmation: invitee_id must exist in fct_speed_to_lead as
--   trigger_type = 'appointment_booking' (P0-1 semantic definition of set).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.fct_deal_attribution` AS
WITH
-- Setter identity: calendar slug → team member profile.
-- Pulls name/email/phone from dim_team_members so updates to the team roster
-- propagate here automatically without touching this SQL.
setter_identity AS (
  SELECT
    slug.calendar_slug,
    tm.display_name  AS setter_name,
    tm.email         AS setter_email,
    tm.phone         AS setter_phone
  FROM (
    SELECT 'houssam-precisionscaling' AS calendar_slug, 'houssam@precisionscaling.io' AS email
    UNION ALL SELECT 'jordan-precisionscaling',  'jordan@precisionscaling.io'
    UNION ALL SELECT 'kevin-precisionscaling',   'kevin@precisionscaling.io'
    UNION ALL SELECT 'jake-precisionscaling',    'jake@precisionscaling.io'
    UNION ALL SELECT 'dee-deesinnercircle',       NULL
    UNION ALL SELECT 'mindofdee',                 NULL
  ) slug
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
    ON tm.team_member_key = CONCAT('email:', slug.email)
),
-- Confirmed sets per P0-1: appointment_booking rows in fct_speed_to_lead
-- trigger_source_id = invitee_id for this trigger type
confirmed_sets AS (
  SELECT
    trigger_source_id AS invitee_id,
    trigger_ts        AS set_ts,
    ghl_contact_id,
    location_id
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
  WHERE trigger_type = 'appointment_booking'
    AND trigger_source_id IS NOT NULL
    AND trigger_ts IS NOT NULL
),
sales_calls AS (
  SELECT
    f.call_id,
    f.event_ts                          AS call_ts,
    f.recorded_by_email                 AS closer_email,
    f.recorded_by_name                  AS closer_name,
    f.matched_contact_id                AS ghl_contact_id,
    f.matched_location_id,
    f.calendly_contact_invitee_id,
    f.pipeline_name,
    f.stage_name,
    f.resolved_classification_confidence,
    f.calendly_contact_match_hour_distance,
    -- Is this call traceable to a confirmed set?
    cs.invitee_id IS NOT NULL           AS is_set_confirmed,
    cs.set_ts
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` f
  LEFT JOIN confirmed_sets cs
    ON cs.invitee_id = f.calendly_contact_invitee_id
  WHERE f.is_sales_meeting_resolved = TRUE
    AND f.matched_contact_id IS NOT NULL
    AND f.event_ts IS NOT NULL
),
booking_setter AS (
  SELECT
    i.invitee_id,
    REGEXP_EXTRACT(et.scheduling_url, r'calendly\.com/([^/]+)/') AS calendar_slug
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON i.event_type_uri = et.event_type_uri
),
payments AS (
  SELECT
    CONCAT(source_system, '|', payment_id) AS payment_key,
    payment_id,
    source_system,
    event_ts                              AS payment_ts,
    SAFE_CAST(net_amount AS NUMERIC)      AS net_amount,
    match_status                          AS payment_match_status,
    -- Flag: ambiguous contact match means attribution chain is unreliable
    match_status = 'ambiguous'            AS payment_match_ambiguous,
    matched_ghl_contact_id                AS ghl_contact_id,
    matched_location_id
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`
  WHERE SAFE_CAST(net_amount AS NUMERIC) > 0
    AND matched_ghl_contact_id IS NOT NULL
    AND event_ts IS NOT NULL
),
-- Step 1: resolve offer for every sales call
calls_with_offer AS (
  SELECT
    sc.*,
    COALESCE(o.canonical_offer, 'unknown_offer') AS canonical_offer,
    o.match_status                               AS offer_match_status,
    bs.calendar_slug,
    REGEXP_EXTRACT(et.scheduling_url, r'calendly\.com/([^/]+)/') AS extracted_slug
  FROM sales_calls sc
  LEFT JOIN booking_setter bs
    ON bs.invitee_id = sc.calendly_contact_invitee_id
  LEFT JOIN (SELECT CAST(NULL AS STRING) AS invitee_id, CAST(NULL AS STRING) AS canonical_offer, CAST(NULL AS STRING) AS match_status LIMIT 0) o
    ON o.invitee_id = sc.calendly_contact_invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = sc.calendly_contact_invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = i.event_type_uri
),
-- Step 2: find the deal-close call per (contact, offer) — earliest confirmed
-- sales call before any payment on that offer
deal_close_call AS (
  SELECT
    sc.ghl_contact_id,
    sc.canonical_offer,
    sc.call_id                                   AS close_call_id,
    sc.call_ts                                   AS close_call_ts,
    sc.closer_email,
    sc.closer_name,
    sc.calendly_contact_invitee_id,
    sc.calendar_slug,
    sc.pipeline_name,
    sc.stage_name,
    sc.resolved_classification_confidence,
    sc.calendly_contact_match_hour_distance,
    sc.is_set_confirmed,
    sc.set_ts,
    sc.offer_match_status,
    ROW_NUMBER() OVER (
      PARTITION BY sc.ghl_contact_id, sc.canonical_offer
      ORDER BY sc.is_set_confirmed DESC, sc.call_ts ASC
    ) AS rn
  FROM calls_with_offer sc
  WHERE EXISTS (
    SELECT 1
    FROM payments p
    WHERE p.ghl_contact_id = sc.ghl_contact_id
      AND p.payment_ts >= sc.call_ts
      AND p.payment_ts <= TIMESTAMP_ADD(sc.call_ts, INTERVAL 60 DAY)
  )
),
close_calls AS (
  SELECT * FROM deal_close_call WHERE rn = 1
),
-- Step 3: attach all payments per (contact, offer) to the deal-close call
-- and number them in order (1 = close payment, 2+ = installments)
attributed AS (
  SELECT
    p.payment_key,
    p.payment_id,
    p.source_system                              AS payment_source,
    p.payment_ts,
    p.net_amount,
    p.payment_match_status,
    p.payment_match_ambiguous,
    cc.close_call_id,
    cc.close_call_ts,
    cc.closer_email,
    cc.closer_name,
    cc.ghl_contact_id,
    cc.calendly_contact_invitee_id,
    cc.calendar_slug,
    cc.pipeline_name,
    cc.stage_name,
    cc.resolved_classification_confidence,
    cc.calendly_contact_match_hour_distance,
    cc.is_set_confirmed,
    cc.set_ts,
    cc.canonical_offer,
    cc.offer_match_status,
    -- Payment sequence within this deal (1 = close, 2+ = installment)
    ROW_NUMBER() OVER (
      PARTITION BY p.ghl_contact_id, cc.canonical_offer
      ORDER BY p.payment_ts ASC
    ) AS payment_sequence_number,
    TIMESTAMP_DIFF(p.payment_ts, cc.close_call_ts, DAY) AS days_since_close_call
  FROM payments p
  JOIN close_calls cc
    ON cc.ghl_contact_id = p.ghl_contact_id
   AND p.payment_ts >= cc.close_call_ts
   AND p.payment_ts <= TIMESTAMP_ADD(cc.close_call_ts, INTERVAL 180 DAY)
)
SELECT
  a.payment_key,
  a.payment_id,
  a.payment_source,
  a.payment_ts,
  DATE_TRUNC(DATE(a.payment_ts), WEEK(MONDAY))  AS report_week,
  a.net_amount,
  a.payment_match_status,

  -- Deal cohort fields
  a.payment_sequence_number,
  a.days_since_close_call,
  a.payment_sequence_number = 1                 AS is_close_payment,

  -- Closer identity (from Fathom recorded_by on close call)
  a.closer_email,
  a.closer_name,

  -- Setter identity (from dim_team_members via calendar slug)
  a.calendar_slug,

  -- Set confirmation (P0-1 semantic gate)
  a.is_set_confirmed,
  a.set_ts,

  -- Offer (from dim_offer_resolved at close-call invitee grain)
  a.canonical_offer,
  a.offer_match_status,

  -- Context
  a.ghl_contact_id,
  a.pipeline_name,
  a.stage_name,
  a.close_call_id                               AS attributed_call_id,
  a.close_call_ts                               AS attributed_call_ts,
  a.resolved_classification_confidence          AS call_classification_confidence,

  -- Setter profile (from dim_team_members via setter_identity)
  si.setter_name,
  si.setter_email,
  si.setter_phone,

  -- Attribution quality flags
  si.setter_name IS NOT NULL                    AS setter_identified,
  a.canonical_offer != 'unknown_offer'          AS offer_identified,
  a.closer_email LIKE '%@precisionscaling.io'   AS closer_is_internal,
  a.payment_match_ambiguous,
  a.calendly_contact_match_hour_distance,
  COALESCE(a.calendly_contact_match_hour_distance <= 24, FALSE) AS setter_match_reliable,

  CURRENT_TIMESTAMP()                           AS mart_refreshed_at
FROM attributed a
LEFT JOIN setter_identity si
  ON si.calendar_slug = a.calendar_slug
;

-- ────────────────────────────────────────────────────────────────────────────
-- METRIC MARTS: Show Rate · Call-to-Booking Rate · Lead Response Time (existing)
--               Cost Per Qualified Appointment
-- ────────────────────────────────────────────────────────────────────────────

-- Show-up rate by week + setter + campaign.
-- Denominator = showed_proxy + no_show_proxy (excludes canceled / future / rescheduled).
-- Numerator  = showed_proxy.
-- Setter resolved via Calendly event_type_uri → dim_calendly_event_types.scheduling_url
-- → calendar_slug → dim_team_members.email → display_name.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_show_rate_week` AS
WITH slug_to_email AS (
  SELECT 'houssam-precisionscaling' AS calendar_slug, 'houssam@precisionscaling.io' AS email
  UNION ALL SELECT 'jordan-precisionscaling',  'jordan@precisionscaling.io'
  UNION ALL SELECT 'kevin-precisionscaling',   'kevin@precisionscaling.io'
  UNION ALL SELECT 'jake-precisionscaling',    'jake@precisionscaling.io'
),
invitee_base AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at)), WEEK(MONDAY)) AS report_week,
    i.invitee_id,
    i.scheduled_event_id,
    i.event_name,
    i.event_type_uri,
    COALESCE(NULLIF(TRIM(i.utm_campaign), ''), '(none)') AS utm_campaign,
    COALESCE(NULLIF(TRIM(i.utm_source), ''), '(none)')   AS utm_source,
    i.invitee_status,
    i.event_status,
    i.scheduled_start_time
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  WHERE COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
),
with_setter AS (
  SELECT
    b.*,
    COALESCE(tm.display_name, 'unknown') AS setter_name
  FROM invitee_base b
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = b.event_type_uri
  LEFT JOIN slug_to_email se
    ON se.calendar_slug = REGEXP_EXTRACT(et.scheduling_url, r'calendly\.com/([^/]+)/')
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
    ON tm.email = se.email
),
with_attendance AS (
  SELECT
    *,
    CASE
      WHEN LOWER(event_status) IN ('canceled', 'cancelled')
        OR LOWER(invitee_status) IN ('canceled', 'cancelled') THEN 'canceled'
      WHEN LOWER(event_status) = 'rescheduled'                THEN 'rescheduled'
      WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
        AND LOWER(invitee_status) = 'active'
        AND LOWER(event_status) NOT IN ('canceled', 'cancelled') THEN 'showed_proxy'
      WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
        AND LOWER(invitee_status) <> 'active'
        AND LOWER(event_status) NOT IN ('canceled', 'cancelled') THEN 'no_show_proxy'
      ELSE 'scheduled_future_or_unknown'
    END AS attendance_bucket
  FROM with_setter
)
SELECT
  report_week,
  event_name,
  setter_name,
  utm_source,
  utm_campaign,
  COUNT(DISTINCT invitee_id)                                       AS total_scheduled,
  COUNTIF(attendance_bucket = 'showed_proxy')                      AS showed_count,
  COUNTIF(attendance_bucket = 'no_show_proxy')                     AS no_show_count,
  COUNTIF(attendance_bucket = 'canceled')                          AS canceled_count,
  COUNTIF(attendance_bucket = 'rescheduled')                       AS rescheduled_count,
  COUNTIF(attendance_bucket IN ('showed_proxy', 'no_show_proxy'))  AS completed_eligible_count,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(attendance_bucket = 'showed_proxy'),
      NULLIF(COUNTIF(attendance_bucket IN ('showed_proxy', 'no_show_proxy')), 0)
    ),
    4
  ) AS show_rate,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(attendance_bucket = 'no_show_proxy'),
      NULLIF(COUNTIF(attendance_bucket IN ('showed_proxy', 'no_show_proxy')), 0)
    ),
    4
  ) AS no_show_rate,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM with_attendance
GROUP BY 1, 2, 3, 4, 5
;

-- Call-to-booking rate by week + setter.
-- Grain: one row per (report_week, setter_name).
-- Voice: contacts who received ≥1 outbound call → what % booked within 14 days.
-- SMS:   of those same contacts, those also touched by outbound SMS → parallel 14-day rate.
-- Setter resolved via owner_id from call log payload → dim_team_members.ghl_user_id.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_call_to_booking_rate_week` AS
WITH outbound_voice AS (
  SELECT
    DATE_TRUNC(DATE(c.call_started_at), WEEK(MONDAY)) AS report_week,
    c.contact_id,
    c.call_started_at,
    COALESCE(
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.assignedTo')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.assigned_to')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.userId')), ''),
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.user_id')), '')
    ) AS owner_id
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  WHERE c.channel_norm = 'call'
    AND c.direction_norm = 'outbound'
    AND c.call_started_at IS NOT NULL
    AND c.contact_id IS NOT NULL
),
outbound_sms AS (
  -- First outbound SMS per contact per week + assigned setter via assigned_to_user_id.
  SELECT
    DATE_TRUNC(DATE(m.message_created_at), WEEK(MONDAY)) AS report_week,
    m.contact_id,
    m.assigned_to_user_id,
    MIN(m.message_created_at) AS first_sms_at,
    COUNT(*) AS sms_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
  WHERE m.message_type_norm = 'sms'
    AND m.direction_norm = 'outbound'
    AND m.message_created_at IS NOT NULL
    AND m.contact_id IS NOT NULL
  GROUP BY 1, 2, 3
),
voice_per_contact_week AS (
  SELECT
    report_week,
    contact_id,
    owner_id,
    MIN(call_started_at) AS first_call_at,
    COUNT(*)             AS dial_count
  FROM outbound_voice
  GROUP BY 1, 2, 3
),
first_booking_per_contact AS (
  SELECT
    b.contact_id,
    MIN(i.invitee_created_at) AS first_booking_at
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND i.invitee_created_at IS NOT NULL
  GROUP BY 1
)
SELECT
  v.report_week,
  COALESCE(tm.display_name, 'unknown')                              AS setter_name,
  COUNT(DISTINCT v.contact_id)                                      AS contacts_called,
  SUM(v.dial_count)                                                 AS total_dials,
  -- All-time: contact ever had a booking
  COUNT(DISTINCT bk.contact_id)                                     AS contacts_with_any_booking,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT bk.contact_id),
      NULLIF(COUNT(DISTINCT v.contact_id), 0)
    ), 4
  )                                                                 AS call_to_booking_rate_all_time,
  -- 14-day forward window: booked within 14 days of first call
  COUNT(DISTINCT CASE
    WHEN bk.first_booking_at
      BETWEEN v.first_call_at AND TIMESTAMP_ADD(v.first_call_at, INTERVAL 14 DAY)
    THEN v.contact_id
  END)                                                              AS contacts_booked_within_14d,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE
        WHEN bk.first_booking_at
          BETWEEN v.first_call_at AND TIMESTAMP_ADD(v.first_call_at, INTERVAL 14 DAY)
        THEN v.contact_id
      END),
      NULLIF(COUNT(DISTINCT v.contact_id), 0)
    ), 4
  )                                                                 AS call_to_booking_rate_14d,
  -- SMS variant: of contacted contacts also touched via SMS that week → 14-day booking rate
  COUNT(DISTINCT s.contact_id)                                      AS contacts_also_sms_touched,
  COUNT(DISTINCT CASE
    WHEN s.contact_id IS NOT NULL
      AND bk.first_booking_at
        BETWEEN s.first_sms_at AND TIMESTAMP_ADD(s.first_sms_at, INTERVAL 14 DAY)
    THEN v.contact_id
  END)                                                              AS sms_touched_booked_within_14d,
  ROUND(
    SAFE_DIVIDE(
      COUNT(DISTINCT CASE
        WHEN s.contact_id IS NOT NULL
          AND bk.first_booking_at
            BETWEEN s.first_sms_at AND TIMESTAMP_ADD(s.first_sms_at, INTERVAL 14 DAY)
        THEN v.contact_id
      END),
      NULLIF(COUNT(DISTINCT s.contact_id), 0)
    ), 4
  )                                                                 AS sms_to_booking_rate_14d,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM voice_per_contact_week v
LEFT JOIN first_booking_per_contact bk
  ON bk.contact_id = v.contact_id
LEFT JOIN outbound_sms s
  ON s.contact_id = v.contact_id AND s.report_week = v.report_week
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.ghl_user_id = v.owner_id
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
;

-- Cost per qualified appointment by month + campaign.
-- "Qualified appointment" = showed_proxy (invitee active + past scheduled time + not canceled).
-- utm_tracked_pct: share of qualified appts with a real utm_campaign value (data quality signal).
-- ad_spend_usd / cost_per_qualified_appt are NULL placeholders until an ad spend
-- source (Meta, Google Ads, or manual CSV seed) is integrated.
-- When ad spend is added: join on (report_month, utm_source, utm_campaign)
-- and compute ad_spend / qualified_appt_count.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_cost_per_qualified_appt_month` AS
WITH qualified_appts AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(i.scheduled_start_time, i.invitee_created_at)), MONTH) AS report_month,
    COALESCE(NULLIF(TRIM(i.utm_campaign), ''), '(none)')                             AS utm_campaign,
    COALESCE(NULLIF(TRIM(i.utm_source), ''), '(none)')                               AS utm_source,
    i.invitee_id,
    b.contact_id,
    i.utm_campaign IS NOT NULL AND TRIM(i.utm_campaign) != ''                        AS has_utm_campaign
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.invitee_id = i.invitee_id
  WHERE i.scheduled_start_time <= CURRENT_TIMESTAMP()
    AND LOWER(i.invitee_status) = 'active'
    AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
    AND COALESCE(i.scheduled_start_time, i.invitee_created_at) IS NOT NULL
)
SELECT
  report_month,
  utm_source,
  utm_campaign,
  COUNT(DISTINCT invitee_id)                                                      AS qualified_appt_count,
  COUNT(DISTINCT contact_id)                                                      AS distinct_contacts,
  ROUND(
    SAFE_DIVIDE(COUNT(DISTINCT invitee_id), COUNT(DISTINCT contact_id)),
    2
  )                                                                               AS appts_per_contact,
  -- Data quality: what % of qualified appts had a real UTM campaign value
  ROUND(
    SAFE_DIVIDE(COUNTIF(has_utm_campaign), COUNT(DISTINCT invitee_id)),
    4
  )                                                                               AS utm_tracked_pct,
  -- Ad spend placeholders — populate when spend source is ingested
  CAST(NULL AS NUMERIC)                                                           AS ad_spend_usd,
  CAST(NULL AS NUMERIC)                                                           AS cost_per_qualified_appt,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM qualified_appts
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- ────────────────────────────────────────────────────────────────────────────
-- CLOSER ATTRIBUTION MARTS
-- ────────────────────────────────────────────────────────────────────────────

-- Close rate by week + closer.
-- Primary source: Fathom-recorded sales calls (is_sales_meeting_resolved = TRUE).
-- Supplement: GHL won opportunities that week by assigned user — catches closes
-- where Fathom was not recording.
-- close_rate = fathom_won_count / sales_calls_taken (Fathom-gated denominator).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_closer_close_rate_week` AS
WITH fathom_sales_calls AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(f.ended_at_ts, f.event_ts)), WEEK(MONDAY)) AS report_week,
    f.call_id,
    f.opportunity_id,
    COALESCE(NULLIF(TRIM(f.recorded_by_email), ''), 'unknown') AS closer_email,
    LOWER(COALESCE(NULLIF(TRIM(f.opportunity_status), ''), 'unknown')) AS opportunity_status,
    f.moved_stage_within_48h,
    f.pipeline_progressed_within_48h,
    f.hours_to_status_change_from_call
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` f
  WHERE f.is_sales_meeting_resolved = TRUE
    AND COALESCE(f.ended_at_ts, f.event_ts) IS NOT NULL
),
-- GHL won opps this week that do NOT have a matching Fathom call (supplement)
latest_opps AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY opportunity_id
        ORDER BY COALESCE(updated_at_ts, ingested_at) DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`
  )
  WHERE rn = 1
),
ghl_won_supplement AS (
  SELECT
    DATE_TRUNC(DATE(o.last_status_change_at), WEEK(MONDAY)) AS report_week,
    COALESCE(
      NULLIF(TRIM(JSON_VALUE(o.payload_json, '$.assignedTo')), ''),
      NULLIF(TRIM(JSON_VALUE(o.payload_json, '$.assignedTo.id')), '')
    ) AS owner_id,
    COUNT(DISTINCT o.opportunity_id) AS ghl_won_count
  FROM latest_opps o
  WHERE LOWER(o.status) = 'won'
    AND o.last_status_change_at IS NOT NULL
    AND o.opportunity_id NOT IN (
      SELECT opportunity_id FROM fathom_sales_calls WHERE opportunity_id IS NOT NULL
    )
  GROUP BY 1, 2
)
SELECT
  f.report_week,
  COALESCE(tm.display_name, f.closer_email)              AS closer_name,
  f.closer_email,
  COUNT(DISTINCT f.call_id)                               AS sales_calls_taken,
  COUNTIF(f.opportunity_id IS NOT NULL)                   AS calls_with_linked_opp,
  COUNTIF(f.opportunity_status = 'won')                   AS fathom_won_count,
  ROUND(
    SAFE_DIVIDE(
      COUNTIF(f.opportunity_status = 'won'),
      NULLIF(COUNT(DISTINCT f.call_id), 0)
    ), 4
  )                                                       AS close_rate,
  COUNTIF(COALESCE(f.moved_stage_within_48h, FALSE))     AS calls_moved_stage_48h,
  COUNTIF(COALESCE(f.pipeline_progressed_within_48h, FALSE)) AS calls_progressed_pipeline_48h,
  ROUND(AVG(
    IF(f.opportunity_status = 'won', f.hours_to_status_change_from_call, NULL)
  ), 2)                                                   AS avg_hours_to_win_on_call,
  -- GHL supplement: won opps this week not covered by Fathom
  MAX(COALESCE(gws.ghl_won_count, 0))                    AS ghl_won_no_fathom_supplement,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM fathom_sales_calls f
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.email = f.closer_email
LEFT JOIN ghl_won_supplement gws
  ON gws.report_week = f.report_week
 AND gws.owner_id = tm.ghl_user_id
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- Revenue attribution by month + closer + offer.
-- Source: fct_deal_attribution (already chains Fathom call → payment via contact match).
-- close_payment_count = first payments only (is_close_payment = TRUE).
-- total_payment_count = all payment sequence numbers (includes installments/continuity).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_closer_revenue_month` AS
SELECT
  DATE_TRUNC(DATE(d.payment_ts), MONTH)                   AS report_month,
  COALESCE(NULLIF(TRIM(d.closer_name), ''), 'unknown')    AS closer_name,
  COALESCE(NULLIF(TRIM(d.closer_email), ''), 'unknown')   AS closer_email,
  COALESCE(NULLIF(TRIM(d.canonical_offer), ''), 'unknown_offer') AS canonical_offer,
  COUNT(DISTINCT CASE WHEN d.is_close_payment THEN d.payment_id END) AS close_payment_count,
  COUNT(DISTINCT d.payment_id)                            AS total_payment_count,
  COUNT(DISTINCT d.ghl_contact_id)                        AS distinct_clients,
  ROUND(SUM(CASE WHEN d.is_close_payment THEN d.net_amount ELSE 0 END), 2) AS close_revenue_net,
  ROUND(SUM(d.net_amount), 2)                             AS total_revenue_net,
  ROUND(
    SAFE_DIVIDE(
      SUM(CASE WHEN d.is_close_payment THEN d.net_amount ELSE 0 END),
      NULLIF(COUNT(DISTINCT CASE WHEN d.is_close_payment THEN d.payment_id END), 0)
    ), 2
  )                                                       AS avg_deal_value,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM `project-41542e21-470f-4589-96d.Marts.fct_deal_attribution` d
WHERE d.payment_ts IS NOT NULL
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 8 DESC
;

-- Speed to close by week + closer.
-- For each won opportunity with a Fathom sales call:
--   hours_to_close = TIMESTAMP_DIFF(won_at, first_sales_call_ts, HOUR)
-- Grain: close_week × closer → distribution (avg / median / p90) + SLA buckets.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_closer_speed_to_close_week` AS
WITH won_opps AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      opportunity_id,
      contact_id,
      last_status_change_at AS won_at,
      COALESCE(
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo')), ''),
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo.id')), '')
      ) AS ghl_owner_id,
      ROW_NUMBER() OVER (
        PARTITION BY opportunity_id
        ORDER BY COALESCE(updated_at_ts, ingested_at) DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`
    WHERE LOWER(status) = 'won'
      AND last_status_change_at IS NOT NULL
  )
  WHERE rn = 1
),
first_fathom_call_per_opp AS (
  SELECT
    f.opportunity_id,
    MIN(COALESCE(f.event_ts, f.ended_at_ts)) AS first_call_ts,
    ANY_VALUE(f.recorded_by_email)            AS closer_email
  FROM `project-41542e21-470f-4589-96d.Core.fct_fathom_sales_calls_enriched` f
  WHERE f.opportunity_id IS NOT NULL
    AND f.is_sales_meeting_resolved = TRUE
    AND COALESCE(f.event_ts, f.ended_at_ts) IS NOT NULL
  GROUP BY 1
),
opp_speed AS (
  SELECT
    DATE_TRUNC(DATE(o.won_at), WEEK(MONDAY))                AS close_week,
    COALESCE(fc.closer_email, 'no_fathom_call')             AS closer_email,
    o.opportunity_id,
    TIMESTAMP_DIFF(o.won_at, fc.first_call_ts, HOUR)        AS hours_to_close,
    fc.first_call_ts IS NOT NULL                            AS has_fathom_call
  FROM won_opps o
  LEFT JOIN first_fathom_call_per_opp fc ON fc.opportunity_id = o.opportunity_id
  WHERE fc.first_call_ts IS NOT NULL  -- only opps with a Fathom call are included in timing
)
SELECT
  os.close_week,
  COALESCE(tm.display_name, os.closer_email)             AS closer_name,
  os.closer_email,
  COUNT(DISTINCT os.opportunity_id)                       AS deals_closed,
  ROUND(AVG(os.hours_to_close), 2)                        AS avg_hours_to_close,
  ROUND(APPROX_QUANTILES(os.hours_to_close, 100)[OFFSET(50)], 2) AS median_hours_to_close,
  ROUND(APPROX_QUANTILES(os.hours_to_close, 100)[OFFSET(90)], 2) AS p90_hours_to_close,
  COUNTIF(os.hours_to_close <= 24)                        AS closed_within_24h,
  COUNTIF(os.hours_to_close <= 48)                        AS closed_within_48h,
  COUNTIF(os.hours_to_close <= 168)                       AS closed_within_7d,
  ROUND(
    SAFE_DIVIDE(COUNTIF(os.hours_to_close <= 48), NULLIF(COUNT(DISTINCT os.opportunity_id), 0)),
    4
  )                                                       AS pct_closed_within_48h,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM opp_speed os
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.email = os.closer_email
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- Pipeline health snapshot by closer + pipeline stage.
-- Always reflects current state (CREATE OR REPLACE on each run = fresh snapshot).
-- snapshot_week anchors it for trend tracking in Looker.
-- Closer resolved via GHL opportunity assignedTo → dim_team_members.ghl_user_id.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_closer_pipeline_health_week` AS
WITH latest_opps AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      opportunity_id,
      contact_id,
      pipeline_name,
      stage_name,
      LOWER(COALESCE(NULLIF(TRIM(status), ''), 'unknown')) AS opp_status,
      COALESCE(amount, 0)                                   AS amount,
      effective_probability,
      last_stage_change_at,
      last_status_change_at,
      updated_at_ts,
      ingested_at,
      COALESCE(
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo')), ''),
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo.id')), '')
      ) AS ghl_owner_id,
      ROW_NUMBER() OVER (
        PARTITION BY opportunity_id
        ORDER BY COALESCE(updated_at_ts, ingested_at) DESC
      ) AS rn
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`
    WHERE COALESCE(updated_at_ts, ingested_at) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))                 AS snapshot_week,
  COALESCE(tm.display_name, lo.ghl_owner_id, 'unassigned') AS closer_name,
  COALESCE(NULLIF(TRIM(lo.pipeline_name), ''), 'unknown')  AS pipeline_name,
  COALESCE(NULLIF(TRIM(lo.stage_name), ''), 'unknown')     AS stage_name,
  lo.opp_status,
  COUNT(DISTINCT lo.opportunity_id)                         AS opportunity_count,
  ROUND(SUM(lo.amount), 2)                                  AS total_pipeline_value,
  ROUND(AVG(lo.amount), 2)                                  AS avg_deal_value,
  ROUND(SUM(lo.amount * COALESCE(lo.effective_probability / 100.0, 0)), 2) AS weighted_pipeline_value,
  COUNTIF(DATE(COALESCE(lo.updated_at_ts, lo.ingested_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS updated_last_7d,
  COUNTIF(lo.opp_status = 'open')                           AS open_count,
  COUNTIF(lo.opp_status = 'won')                            AS won_count,
  COUNTIF(lo.opp_status = 'lost')                           AS lost_count,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM latest_opps lo
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.ghl_user_id = lo.ghl_owner_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC
;

-- ────────────────────────────────────────────────────────────────────────────
-- END-TO-END FUNNEL CONVERSION
-- ────────────────────────────────────────────────────────────────────────────

-- Weekly funnel: New Lead → Contacted → Booked → Showed → Closed.
-- Grain: report_week × campaign_reporting.
-- Each step is event-week based (activity that occurred in that week), not a
-- lead cohort — so step-over-step rates reflect weekly machine velocity rather
-- than true cohort conversion. Use rpt_campaign_funnel_month for cohort view.
-- Campaign attribution: dim_golden_contact.campaign_reporting (bucketed source/UTM)
-- joined through contact_id at every step. Calendly-only bookings/shows fall
-- back to Calendly utm_campaign when no GHL match exists.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_funnel_conversion_week` AS
WITH

-- Step 1: New leads entered GHL this week
new_leads AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts)), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting,
    COUNT(DISTINCT gc.ghl_contact_id) AS new_lead_count
  FROM `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
  WHERE COALESCE(gc.ghl_date_added_ts, gc.ghl_first_seen_ts) IS NOT NULL
  GROUP BY 1, 2
),

-- Step 2a: Outbound voice call stats per (week, campaign)
call_stats AS (
  SELECT
    DATE_TRUNC(DATE(c.call_started_at), WEEK(MONDAY))  AS report_week,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting,
    COUNT(DISTINCT c.contact_id)                        AS contacts_call_attempted,
    COUNT(DISTINCT CASE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(c.call_status, '')), r'answer|connected|complete|completed')
      THEN c.contact_id
    END)                                                AS contacts_call_connected,
    COUNT(*)                                            AS total_dials
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = c.contact_id
  WHERE c.channel_norm = 'call'
    AND c.direction_norm = 'outbound'
    AND c.call_started_at IS NOT NULL
    AND c.contact_id IS NOT NULL
  GROUP BY 1, 2
),

-- Step 2b: Outbound SMS stats per (week, campaign)
-- assigned_to_user_id now available from fct_ghl_conversations ($.assignedTo on conversation).
sms_stats AS (
  SELECT
    DATE_TRUNC(DATE(m.message_created_at), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting,
    COUNT(DISTINCT m.contact_id)                          AS contacts_sms_touched,
    COUNT(*)                                              AS total_sms_sent,
    COUNT(DISTINCT m.assigned_to_user_id)                 AS distinct_sms_senders
  FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = m.contact_id
  WHERE m.message_type_norm = 'sms'
    AND m.direction_norm = 'outbound'
    AND m.message_created_at IS NOT NULL
    AND m.contact_id IS NOT NULL
  GROUP BY 1, 2
),

-- Step 2 combined: any-channel reach, deduped by contact per week.
-- UNION DISTINCT on (week, contact_id, campaign) ensures a contact touched by
-- both call and SMS in the same week is counted once in contacts_reached_any_channel.
leads_contacted AS (
  SELECT
    report_week,
    campaign_reporting,
    COUNT(DISTINCT contact_id) AS contacts_reached_any_channel
  FROM (
    SELECT
      DATE_TRUNC(DATE(c.call_started_at), WEEK(MONDAY))        AS report_week,
      c.contact_id,
      COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
    LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
      ON gc.ghl_contact_id = c.contact_id
    WHERE c.channel_norm = 'call' AND c.direction_norm = 'outbound'
      AND c.call_started_at IS NOT NULL AND c.contact_id IS NOT NULL
    UNION DISTINCT
    SELECT
      DATE_TRUNC(DATE(m.message_created_at), WEEK(MONDAY))      AS report_week,
      m.contact_id,
      COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
    LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
      ON gc.ghl_contact_id = m.contact_id
    WHERE m.message_type_norm = 'sms' AND m.direction_norm = 'outbound'
      AND m.message_created_at IS NOT NULL AND m.contact_id IS NOT NULL
  )
  GROUP BY 1, 2
),

-- Step 3: Calendly bookings created this week (invitee_created_at)
bookings_made AS (
  SELECT
    DATE_TRUNC(DATE(i.invitee_created_at), WEEK(MONDAY)) AS report_week,
    COALESCE(
      NULLIF(TRIM(gc.campaign_reporting), ''),
      NULLIF(TRIM(i.utm_campaign), ''),
      'Other'
    ) AS campaign_reporting,
    COUNT(DISTINCT i.invitee_id)   AS booking_count,
    COUNT(DISTINCT b.contact_id)   AS distinct_contacts_booked
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.invitee_id = i.invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = b.contact_id
  WHERE i.invitee_created_at IS NOT NULL
    AND LOWER(COALESCE(i.invitee_status, '')) != 'canceled'
    AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
  GROUP BY 1, 2
),

-- Step 4: Appointments whose scheduled time fell this week — showed vs. no-show
appointments_held AS (
  SELECT
    DATE_TRUNC(DATE(i.scheduled_start_time), WEEK(MONDAY)) AS report_week,
    COALESCE(
      NULLIF(TRIM(gc.campaign_reporting), ''),
      NULLIF(TRIM(i.utm_campaign), ''),
      'Other'
    ) AS campaign_reporting,
    COUNTIF(
      LOWER(i.invitee_status) = 'active'
      AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
    ) AS showed_count,
    COUNTIF(
      LOWER(i.invitee_status) != 'active'
      AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
    ) AS no_show_count
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.invitee_id = i.invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = b.contact_id
  WHERE i.scheduled_start_time <= CURRENT_TIMESTAMP()
    AND i.scheduled_start_time IS NOT NULL
    AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
    AND LOWER(COALESCE(i.invitee_status, '')) NOT IN ('canceled', 'cancelled')
  GROUP BY 1, 2
),

-- Step 5: Deals won this week (last_status_change_at)
deals_closed AS (
  SELECT
    DATE_TRUNC(DATE(o.last_status_change_at), WEEK(MONDAY)) AS report_week,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other') AS campaign_reporting,
    COUNT(DISTINCT o.opportunity_id) AS won_count
  FROM (
    SELECT * EXCEPT (rn)
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY opportunity_id
          ORDER BY COALESCE(updated_at_ts, ingested_at) DESC
        ) AS rn
      FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_opportunities`
      WHERE LOWER(status) = 'won'
        AND last_status_change_at IS NOT NULL
    )
    WHERE rn = 1
  ) o
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = o.contact_id
  GROUP BY 1, 2
),

-- Spine: every (week, campaign) pair that appears in any step
spine AS (
  SELECT report_week, campaign_reporting FROM new_leads
  UNION DISTINCT
  SELECT report_week, campaign_reporting FROM leads_contacted
  UNION DISTINCT
  SELECT report_week, campaign_reporting FROM bookings_made
  UNION DISTINCT
  SELECT report_week, campaign_reporting FROM appointments_held
  UNION DISTINCT
  SELECT report_week, campaign_reporting FROM deals_closed
)

SELECT
  s.report_week,
  s.campaign_reporting,

  -- ── Volume at each step ──────────────────────────────────────────────────
  COALESCE(nl.new_lead_count, 0)             AS new_leads,

  -- Multi-channel outreach breakdown
  COALESCE(lc.contacts_reached_any_channel, 0) AS contacts_reached_any_channel,
  COALESCE(cs.contacts_call_attempted, 0)    AS contacts_call_attempted,
  COALESCE(cs.contacts_call_connected, 0)    AS contacts_call_connected,
  COALESCE(cs.total_dials, 0)                AS total_dials,
  COALESCE(ss.contacts_sms_touched, 0)       AS contacts_sms_touched,
  COALESCE(ss.total_sms_sent, 0)             AS total_sms_sent,

  -- Call connect rate (answered / dialed)
  ROUND(SAFE_DIVIDE(
    COALESCE(cs.contacts_call_connected, 0),
    NULLIF(COALESCE(cs.contacts_call_attempted, 0), 0)
  ), 4)                                      AS call_connect_rate,

  COALESCE(bm.booking_count, 0)              AS bookings_made,
  COALESCE(bm.distinct_contacts_booked, 0)   AS distinct_contacts_booked,
  COALESCE(ah.showed_count, 0)               AS appointments_showed,
  COALESCE(ah.no_show_count, 0)              AS appointments_no_showed,
  COALESCE(ah.showed_count, 0)
    + COALESCE(ah.no_show_count, 0)          AS appointments_held,
  COALESCE(dc.won_count, 0)                  AS deals_closed,

  -- ── Step-over-step conversion rates ─────────────────────────────────────
  -- Lead → Contact: any-channel reach / new leads (approximate cross-week)
  ROUND(SAFE_DIVIDE(
    COALESCE(lc.contacts_reached_any_channel, 0),
    NULLIF(COALESCE(nl.new_lead_count, 0), 0)
  ), 4)                                      AS lead_to_contact_rate,

  -- Contact → Booking: bookings / any-channel contacts this week
  ROUND(SAFE_DIVIDE(
    COALESCE(bm.booking_count, 0),
    NULLIF(COALESCE(lc.contacts_reached_any_channel, 0), 0)
  ), 4)                                      AS contact_to_booking_rate,

  -- Booking → Show: of appointments whose time fell this week
  ROUND(SAFE_DIVIDE(
    COALESCE(ah.showed_count, 0),
    NULLIF(COALESCE(ah.showed_count, 0) + COALESCE(ah.no_show_count, 0), 0)
  ), 4)                                      AS show_rate,

  -- Show → Close: of shows this week, how many closed
  ROUND(SAFE_DIVIDE(
    COALESCE(dc.won_count, 0),
    NULLIF(COALESCE(ah.showed_count, 0), 0)
  ), 4)                                      AS showed_to_close_rate,

  -- End-to-end: new leads this week → deals closed this week (velocity proxy)
  ROUND(SAFE_DIVIDE(
    COALESCE(dc.won_count, 0),
    NULLIF(COALESCE(nl.new_lead_count, 0), 0)
  ), 4)                                      AS lead_to_close_rate,

  CURRENT_TIMESTAMP() AS mart_refreshed_at

FROM spine s
LEFT JOIN new_leads nl
  ON nl.report_week = s.report_week AND nl.campaign_reporting = s.campaign_reporting
LEFT JOIN leads_contacted lc
  ON lc.report_week = s.report_week AND lc.campaign_reporting = s.campaign_reporting
LEFT JOIN call_stats cs
  ON cs.report_week = s.report_week AND cs.campaign_reporting = s.campaign_reporting
LEFT JOIN sms_stats ss
  ON ss.report_week = s.report_week AND ss.campaign_reporting = s.campaign_reporting
LEFT JOIN bookings_made bm
  ON bm.report_week = s.report_week AND bm.campaign_reporting = s.campaign_reporting
LEFT JOIN appointments_held ah
  ON ah.report_week = s.report_week AND ah.campaign_reporting = s.campaign_reporting
LEFT JOIN deals_closed dc
  ON dc.report_week = s.report_week AND dc.campaign_reporting = s.campaign_reporting
ORDER BY 1 DESC, 3 DESC
;

-- ────────────────────────────────────────────────────────────────────────────
-- SETTER FLOW MARTS
-- Flow 1: Booked leads → pre-appointment outreach → show rate impact
-- Flow 2: Unbooked form leads → outbound conversion to booking
-- ────────────────────────────────────────────────────────────────────────────

-- Flow 1: Pre-appointment outreach effectiveness by week + setter.
-- A lead books via Calendly. The setter's job is to reach them before
-- the appointment to confirm and prep. This mart measures whether they did,
-- how many attempts it took, and — critically — whether pre-appointment
-- contact actually improves show rate (show_rate_when_reached vs not_reached).
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_setter_pre_appt_outreach_week` AS
WITH slug_to_email AS (
  SELECT 'houssam-precisionscaling' AS calendar_slug, 'houssam@precisionscaling.io' AS email
  UNION ALL SELECT 'jordan-precisionscaling',  'jordan@precisionscaling.io'
  UNION ALL SELECT 'kevin-precisionscaling',   'kevin@precisionscaling.io'
  UNION ALL SELECT 'jake-precisionscaling',    'jake@precisionscaling.io'
),
bookings AS (
  SELECT
    i.invitee_id,
    i.event_type_uri,
    i.invitee_created_at                                              AS booked_at,
    i.scheduled_start_time,
    DATE_TRUNC(DATE(i.scheduled_start_time), WEEK(MONDAY))           AS appt_week,
    b.contact_id,
    i.scheduled_start_time <= CURRENT_TIMESTAMP()                    AS is_past,
    CASE
      WHEN i.scheduled_start_time <= CURRENT_TIMESTAMP()
        AND LOWER(i.invitee_status) = 'active'
        AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
      THEN TRUE ELSE FALSE
    END                                                               AS showed_proxy
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.invitee_id = i.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND i.scheduled_start_time IS NOT NULL
    AND i.invitee_created_at IS NOT NULL
    AND LOWER(COALESCE(i.event_status, '')) NOT IN ('canceled', 'cancelled')
    AND LOWER(COALESCE(i.invitee_status, '')) NOT IN ('canceled', 'cancelled')
),
setter_per_booking AS (
  SELECT
    bk.invitee_id,
    COALESCE(tm.display_name, 'unknown') AS setter_name
  FROM bookings bk
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = bk.event_type_uri
  LEFT JOIN slug_to_email se
    ON se.calendar_slug = REGEXP_EXTRACT(et.scheduling_url, r'calendly\.com/([^/]+)/')
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
    ON tm.email = se.email
),
pre_appt_touches AS (
  -- Outbound touches between booking time and appointment time
  SELECT
    b.invitee_id,
    'call'     AS channel,
    c.call_started_at AS touch_at,
    REGEXP_CONTAINS(
      LOWER(COALESCE(c.call_status, '')),
      r'answer|connected|complete|completed'
    )          AS is_connected
  FROM bookings b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
    ON  c.contact_id = b.contact_id
    AND c.call_started_at >= b.booked_at
    AND c.call_started_at <  b.scheduled_start_time
  WHERE c.channel_norm = 'call'
    AND c.direction_norm = 'outbound'

  UNION ALL

  SELECT
    b.invitee_id,
    'sms'      AS channel,
    m.message_created_at AS touch_at,
    TRUE       AS is_connected   -- SMS sent = contact touched
  FROM bookings b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
    ON  m.contact_id = b.contact_id
    AND m.message_created_at >= b.booked_at
    AND m.message_created_at <  b.scheduled_start_time
  WHERE m.message_type_norm = 'sms'
    AND m.direction_norm = 'outbound'
),
booking_summary AS (
  SELECT
    b.invitee_id,
    b.appt_week,
    b.is_past,
    b.showed_proxy,
    COUNT(pt.touch_at)                                    AS total_pre_appt_touches,
    COUNTIF(pt.channel = 'call')                          AS pre_appt_calls,
    COUNTIF(pt.channel = 'sms')                           AS pre_appt_sms,
    COUNTIF(pt.is_connected)                              AS pre_appt_connected_touches,
    COALESCE(MAX(CASE WHEN pt.is_connected THEN 1 ELSE 0 END) = 1, FALSE) AS was_reached
  FROM bookings b
  LEFT JOIN pre_appt_touches pt ON pt.invitee_id = b.invitee_id
  GROUP BY 1, 2, 3, 4
)
SELECT
  bs.appt_week                                                    AS report_week,
  COALESCE(spb.setter_name, 'unknown')                           AS setter_name,
  COUNT(DISTINCT bs.invitee_id)                                   AS total_booked,
  COUNTIF(bs.is_past)                                             AS past_appointments,
  COUNTIF(NOT bs.is_past)                                         AS future_appointments,
  -- Pre-appointment reach
  COUNTIF(bs.was_reached)                                         AS reached_before_appt,
  COUNTIF(NOT bs.was_reached)                                     AS not_reached_before_appt,
  ROUND(SAFE_DIVIDE(
    COUNTIF(bs.was_reached),
    NULLIF(COUNT(DISTINCT bs.invitee_id), 0)
  ), 4)                                                           AS pre_appt_reach_rate,
  -- Touch volume
  ROUND(AVG(bs.total_pre_appt_touches), 2)                       AS avg_touches_per_booking,
  ROUND(AVG(bs.pre_appt_calls), 2)                               AS avg_calls_per_booking,
  ROUND(AVG(bs.pre_appt_sms), 2)                                 AS avg_sms_per_booking,
  -- Show rate: full + split by reached vs not reached (past appts only)
  ROUND(SAFE_DIVIDE(
    COUNTIF(bs.is_past AND bs.showed_proxy),
    NULLIF(COUNTIF(bs.is_past), 0)
  ), 4)                                                           AS show_rate_overall,
  ROUND(SAFE_DIVIDE(
    COUNTIF(bs.is_past AND bs.was_reached AND bs.showed_proxy),
    NULLIF(COUNTIF(bs.is_past AND bs.was_reached), 0)
  ), 4)                                                           AS show_rate_when_reached,
  ROUND(SAFE_DIVIDE(
    COUNTIF(bs.is_past AND NOT bs.was_reached AND bs.showed_proxy),
    NULLIF(COUNTIF(bs.is_past AND NOT bs.was_reached), 0)
  ), 4)                                                           AS show_rate_when_not_reached,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM booking_summary bs
LEFT JOIN setter_per_booking spb ON spb.invitee_id = bs.invitee_id
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
;

-- Flow 2: Unbooked form-submit lead conversion by week + setter.
-- A lead submits a form but does NOT book. Setters do outbound (calls + SMS)
-- to convert them into a booking. This is the true call-to-booking metric.
-- Grain: report_week (week of outbound activity) × setter_name × campaign_reporting.
-- conversion_rate_14d = booked within 14 days of first touch this week / leads worked.
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_setter_unbooked_conversion_week` AS
WITH form_leads AS (
  SELECT
    f.ghl_contact_id                                               AS contact_id,
    MIN(f.event_ts)                                                AS first_submission_at,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other')    AS campaign_reporting
  FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity` f
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = f.ghl_contact_id
  WHERE f.ghl_contact_id IS NOT NULL
    AND f.event_ts IS NOT NULL
  GROUP BY 1, 3
),
first_booking AS (
  SELECT
    b.contact_id,
    MIN(i.invitee_created_at) AS first_booking_at
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE b.contact_id IS NOT NULL
    AND i.invitee_created_at IS NOT NULL
  GROUP BY 1
),
unbooked_leads AS (
  -- Form submitters with no booking at time of submission (or never booked)
  SELECT
    fl.contact_id,
    fl.first_submission_at,
    fl.campaign_reporting,
    fb.first_booking_at,
    fb.first_booking_at IS NOT NULL AS eventually_booked
  FROM form_leads fl
  LEFT JOIN first_booking fb ON fb.contact_id = fl.contact_id
  WHERE fb.first_booking_at IS NULL
    OR fb.first_booking_at > fl.first_submission_at
),
outbound_touches AS (
  -- All outbound touches on unbooked leads made after their form submission
  SELECT
    ul.contact_id,
    ul.first_submission_at,
    ul.campaign_reporting,
    ul.eventually_booked,
    ul.first_booking_at,
    DATE_TRUNC(DATE(touch_at), WEEK(MONDAY))  AS report_week,
    owner_id,
    channel,
    touch_at
  FROM unbooked_leads ul
  JOIN (
    SELECT
      contact_id,
      call_started_at                           AS touch_at,
      'call'                                    AS channel,
      COALESCE(
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.assignedTo')), ''),
        NULLIF(TRIM(JSON_VALUE(payload_json, '$.userId')), '')
      )                                         AS owner_id
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls`
    WHERE channel_norm = 'call'
      AND direction_norm = 'outbound'
      AND call_started_at IS NOT NULL
      AND contact_id IS NOT NULL
    UNION ALL
    SELECT
      contact_id,
      message_created_at                        AS touch_at,
      'sms'                                     AS channel,
      assigned_to_user_id                       AS owner_id
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations`
    WHERE message_type_norm = 'sms'
      AND direction_norm = 'outbound'
      AND message_created_at IS NOT NULL
      AND contact_id IS NOT NULL
  ) t ON t.contact_id = ul.contact_id
      AND t.touch_at >= ul.first_submission_at
),
contact_week AS (
  -- Aggregate to contact × week × setter grain
  SELECT
    report_week,
    contact_id,
    campaign_reporting,
    eventually_booked,
    first_booking_at,
    owner_id,
    MIN(touch_at)              AS first_touch_at,
    COUNTIF(channel = 'call')  AS call_count,
    COUNTIF(channel = 'sms')   AS sms_count
  FROM outbound_touches
  GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
  cw.report_week,
  COALESCE(tm.display_name, cw.owner_id, 'unknown')  AS setter_name,
  cw.campaign_reporting,
  COUNT(DISTINCT cw.contact_id)                       AS unbooked_leads_worked,
  SUM(cw.call_count)                                  AS total_dials,
  SUM(cw.sms_count)                                   AS total_sms,
  COUNT(DISTINCT CASE WHEN cw.call_count > 0 THEN cw.contact_id END) AS contacts_called,
  COUNT(DISTINCT CASE WHEN cw.sms_count  > 0 THEN cw.contact_id END) AS contacts_sms_touched,
  -- Conversion: eventually booked (any time after first touch)
  COUNT(DISTINCT CASE WHEN cw.eventually_booked THEN cw.contact_id END) AS eventually_booked,
  -- Conversion within 7 / 14 days of first touch this week
  COUNT(DISTINCT CASE
    WHEN cw.eventually_booked
      AND cw.first_booking_at
        BETWEEN cw.first_touch_at
            AND TIMESTAMP_ADD(cw.first_touch_at, INTERVAL 7 DAY)
    THEN cw.contact_id
  END)                                                AS booked_within_7d,
  COUNT(DISTINCT CASE
    WHEN cw.eventually_booked
      AND cw.first_booking_at
        BETWEEN cw.first_touch_at
            AND TIMESTAMP_ADD(cw.first_touch_at, INTERVAL 14 DAY)
    THEN cw.contact_id
  END)                                                AS booked_within_14d,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN cw.eventually_booked THEN cw.contact_id END),
    NULLIF(COUNT(DISTINCT cw.contact_id), 0)
  ), 4)                                               AS conversion_rate_all_time,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE
      WHEN cw.eventually_booked
        AND cw.first_booking_at
          BETWEEN cw.first_touch_at
              AND TIMESTAMP_ADD(cw.first_touch_at, INTERVAL 14 DAY)
      THEN cw.contact_id
    END),
    NULLIF(COUNT(DISTINCT cw.contact_id), 0)
  ), 4)                                               AS conversion_rate_14d,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM contact_week cw
LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
  ON tm.ghl_user_id = cw.owner_id
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- ============================================================
-- rpt_unbooked_lead_quality_by_campaign
-- Campaign × lead magnet quality scorecard for unbooked leads.
-- Answers: which campaigns produce leads that eventually book,
-- and how many unbooked leads have never been touched by a setter?
-- ============================================================
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_unbooked_lead_quality_by_campaign` AS
WITH
-- First form submission per contact
form_fills AS (
  SELECT
    lma.ghl_contact_id                                              AS contact_id,
    MIN(lma.event_ts)                                               AS first_submission_at,
    DATE_TRUNC(DATE(MIN(lma.event_ts)), WEEK(MONDAY))               AS submission_week,
    -- Use the lead magnet name from the earliest submission
    ARRAY_AGG(lma.lead_magnet_name ORDER BY lma.event_ts LIMIT 1)[OFFSET(0)] AS lead_magnet_name,
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other')      AS campaign_reporting
  FROM `project-41542e21-470f-4589-96d.Marts.fct_lead_magnet_activity` lma
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = lma.ghl_contact_id
  WHERE lma.event_ts IS NOT NULL
  GROUP BY lma.ghl_contact_id, gc.campaign_reporting
),

-- Earliest Calendly booking per contact
first_booking AS (
  SELECT
    b.contact_id                                                    AS contact_id,
    MIN(i.invitee_created_at)                                       AS first_booking_at
  FROM `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
  JOIN `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
    ON i.invitee_id = b.invitee_id
  WHERE LOWER(COALESCE(i.invitee_status, '')) != 'canceled'
  GROUP BY b.contact_id
),

-- Unbooked at time of submission: no booking, or booked only after submitting
unbooked AS (
  SELECT
    ff.contact_id,
    ff.first_submission_at,
    ff.submission_week,
    ff.lead_magnet_name,
    ff.campaign_reporting,
    fb.first_booking_at,
    -- Booked at some point after submission
    CASE WHEN fb.first_booking_at > ff.first_submission_at THEN TRUE ELSE FALSE END AS eventually_booked,
    CASE
      WHEN fb.first_booking_at > ff.first_submission_at
        AND fb.first_booking_at <= TIMESTAMP_ADD(ff.first_submission_at, INTERVAL 7 DAY)
      THEN TRUE ELSE FALSE
    END AS booked_within_7d,
    CASE
      WHEN fb.first_booking_at > ff.first_submission_at
        AND fb.first_booking_at <= TIMESTAMP_ADD(ff.first_submission_at, INTERVAL 14 DAY)
      THEN TRUE ELSE FALSE
    END AS booked_within_14d,
    CASE
      WHEN fb.first_booking_at > ff.first_submission_at
      THEN DATE_DIFF(DATE(fb.first_booking_at), DATE(ff.first_submission_at), DAY)
      ELSE NULL
    END AS days_to_booking
  FROM form_fills ff
  LEFT JOIN first_booking fb ON fb.contact_id = ff.contact_id
  -- Exclude leads who had already booked BEFORE or AT submission (they're a different population)
  WHERE fb.first_booking_at IS NULL
     OR fb.first_booking_at > ff.first_submission_at
),

-- Any outbound setter touch (call or SMS) after first submission
setter_touches AS (
  SELECT DISTINCT contact_id
  FROM (
    SELECT c.contact_id
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls` c
    JOIN unbooked u ON u.contact_id = c.contact_id
    WHERE c.direction_norm = 'outbound'
      AND c.call_started_at >= u.first_submission_at

    UNION DISTINCT

    SELECT m.contact_id
    FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations` m
    JOIN unbooked u ON u.contact_id = m.contact_id
    WHERE m.direction_norm = 'outbound'
      AND REGEXP_CONTAINS(LOWER(COALESCE(m.message_type_norm, '')), r'sms|text|whatsapp|type_sms')
      AND m.message_created_at >= u.first_submission_at
  )
)

SELECT
  u.submission_week                                                     AS report_week,
  u.campaign_reporting,
  u.lead_magnet_name,

  -- Volume
  COUNT(DISTINCT u.contact_id)                                          AS form_fills,

  -- Booking conversion
  COUNT(DISTINCT CASE WHEN u.eventually_booked THEN u.contact_id END)   AS eventually_booked,
  COUNT(DISTINCT CASE WHEN u.booked_within_7d  THEN u.contact_id END)   AS booked_within_7d,
  COUNT(DISTINCT CASE WHEN u.booked_within_14d THEN u.contact_id END)   AS booked_within_14d,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN u.eventually_booked THEN u.contact_id END),
    COUNT(DISTINCT u.contact_id)
  ), 4)                                                                 AS booking_rate_ever,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN u.booked_within_7d THEN u.contact_id END),
    COUNT(DISTINCT u.contact_id)
  ), 4)                                                                 AS booking_rate_7d,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN u.booked_within_14d THEN u.contact_id END),
    COUNT(DISTINCT u.contact_id)
  ), 4)                                                                 AS booking_rate_14d,

  -- Days to booking (median among those who booked)
  CAST(
    APPROX_QUANTILES(u.days_to_booking, 2)[OFFSET(1)]
  AS INT64)                                                             AS median_days_to_booking,

  -- Setter outreach coverage
  COUNT(DISTINCT CASE WHEN st.contact_id IS NOT NULL THEN u.contact_id END) AS setter_touched,
  COUNT(DISTINCT CASE WHEN st.contact_id IS NULL     THEN u.contact_id END) AS never_touched,
  ROUND(SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN st.contact_id IS NULL THEN u.contact_id END),
    COUNT(DISTINCT u.contact_id)
  ), 4)                                                                 AS never_touched_pct,

  CURRENT_TIMESTAMP()                                                   AS mart_refreshed_at

FROM unbooked u
LEFT JOIN setter_touches st ON st.contact_id = u.contact_id
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- ============================================================
-- rpt_appt_funnel_week
-- Full appointment funnel: bookings → host cancels → invitee
-- cancels → net appointments → shows → no-shows → show rate.
-- Replaces rpt_show_rate_week as the dashboard-facing view.
-- Grain: report_week × setter_name × campaign_reporting
-- ============================================================
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_appt_funnel_week` AS
WITH slug_to_email AS (
  SELECT 'houssam-precisionscaling' AS calendar_slug, 'houssam@precisionscaling.io' AS email
  UNION ALL SELECT 'jordan-precisionscaling',  'jordan@precisionscaling.io'
  UNION ALL SELECT 'kevin-precisionscaling',   'kevin@precisionscaling.io'
  UNION ALL SELECT 'jake-precisionscaling',    'jake@precisionscaling.io'
),

invitee_enriched AS (
  SELECT
    DATE_TRUNC(DATE(COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at)), WEEK(MONDAY)) AS report_week,
    i.invitee_id,
    i.scheduled_event_id,
    i.event_type_uri,
    i.invitee_status,
    i.event_status,
    i.scheduled_start_time,
    i.is_canceled,
    -- Cancel attribution from payload
    NULLIF(TRIM(JSON_VALUE(i.payload_json, '$.cancellation.canceler_type')), '')  AS canceler_type,
    NULLIF(TRIM(JSON_VALUE(i.payload_json, '$.cancellation.canceled_by')), '')    AS canceled_by_name,
    NULLIF(TRIM(JSON_VALUE(i.payload_json, '$.cancellation.reason')), '')         AS cancel_reason,
    -- Setter via event type slug → email → dim_team_members
    COALESCE(tm.display_name, 'unknown')                                           AS setter_name,
    -- Campaign via GHL contact bridge
    COALESCE(NULLIF(TRIM(gc.campaign_reporting), ''), 'Other')                    AS campaign_reporting
  FROM `project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees` i
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.dim_calendly_event_types` et
    ON et.event_type_uri = i.event_type_uri
  LEFT JOIN slug_to_email se
    ON se.calendar_slug = REGEXP_EXTRACT(et.scheduling_url, r'calendly\.com/([^/]+)/')
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_team_members` tm
    ON tm.email = se.email
  LEFT JOIN `project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts` b
    ON b.invitee_id = i.invitee_id
  LEFT JOIN `project-41542e21-470f-4589-96d.Marts.dim_golden_contact` gc
    ON gc.ghl_contact_id = b.contact_id
  WHERE COALESCE(i.scheduled_start_time, i.invitee_created_at, i.event_ts, i.ingested_at) IS NOT NULL
),

classified AS (
  SELECT
    *,
    CASE
      -- Host (triager) canceled — DQ or team-initiated
      WHEN is_canceled AND LOWER(COALESCE(canceler_type, '')) = 'host'    THEN 'host_canceled'
      -- Lead canceled themselves
      WHEN is_canceled AND LOWER(COALESCE(canceler_type, '')) = 'invitee' THEN 'invitee_canceled'
      -- Canceled but no canceler_type recorded
      WHEN is_canceled                                                      THEN 'canceled_unknown'
      -- Past, active invitee = showed
      WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
        AND LOWER(invitee_status) = 'active'
        AND LOWER(COALESCE(event_status, '')) NOT IN ('canceled', 'cancelled') THEN 'showed'
      -- Past, inactive invitee = no-show
      WHEN scheduled_start_time <= CURRENT_TIMESTAMP()
        AND LOWER(invitee_status) != 'active'
        AND LOWER(COALESCE(event_status, '')) NOT IN ('canceled', 'cancelled') THEN 'no_show'
      -- Future appointment
      ELSE 'scheduled_future'
    END AS funnel_bucket
  FROM invitee_enriched
)

SELECT
  report_week,
  setter_name,
  campaign_reporting,

  -- Top of funnel
  COUNT(DISTINCT invitee_id)                                                    AS total_bookings,

  -- Cancellations (split by who canceled)
  COUNTIF(funnel_bucket = 'host_canceled')                                      AS host_canceled,
  COUNTIF(funnel_bucket = 'invitee_canceled')                                   AS invitee_canceled,
  COUNTIF(funnel_bucket = 'canceled_unknown')                                   AS canceled_unknown,
  COUNTIF(funnel_bucket IN ('host_canceled', 'invitee_canceled', 'canceled_unknown')) AS total_canceled,

  -- Net appointments (bookings minus all cancels)
  COUNTIF(funnel_bucket NOT IN ('host_canceled', 'invitee_canceled', 'canceled_unknown', 'scheduled_future')) AS net_appointments,

  -- Outcome of net appointments
  COUNTIF(funnel_bucket = 'showed')                                             AS shows,
  COUNTIF(funnel_bucket = 'no_show')                                            AS no_shows,
  COUNTIF(funnel_bucket = 'scheduled_future')                                   AS scheduled_future,

  -- Show rate: shows / net past appointments (the correct denominator)
  ROUND(SAFE_DIVIDE(
    COUNTIF(funnel_bucket = 'showed'),
    NULLIF(COUNTIF(funnel_bucket IN ('showed', 'no_show')), 0)
  ), 4)                                                                         AS show_rate,

  -- Cancel rate: what % of all bookings got canceled
  ROUND(SAFE_DIVIDE(
    COUNTIF(funnel_bucket IN ('host_canceled', 'invitee_canceled', 'canceled_unknown')),
    NULLIF(COUNT(DISTINCT invitee_id), 0)
  ), 4)                                                                         AS cancel_rate,

  -- Host cancel rate: proxy for triager DQ activity
  ROUND(SAFE_DIVIDE(
    COUNTIF(funnel_bucket = 'host_canceled'),
    NULLIF(COUNT(DISTINCT invitee_id), 0)
  ), 4)                                                                         AS host_cancel_rate,

  CURRENT_TIMESTAMP()                                                           AS mart_refreshed_at

FROM classified
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- ============================================================
-- rpt_speed_to_lead_week
-- Weekly speed-to-lead by setter with SLA tier breakdown.
-- Grain: report_week × setter_name × trigger_type
-- trigger_type: 'lead_magnet' (form submit → first dial)
--               'appointment_booking' (booking → first dial)
-- ============================================================
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_speed_to_lead_week` AS
SELECT
  DATE_TRUNC(DATE(trigger_ts), WEEK(MONDAY))                                AS report_week,
  COALESCE(setter_dim_display_name, 'unattributed')                         AS setter_name,
  trigger_type,
  COUNT(*)                                                                   AS total_triggers,
  COUNTIF(first_touch_ts IS NOT NULL)                                        AS touched,
  COUNTIF(first_touch_ts IS NULL)                                            AS not_yet_touched,
  ROUND(SAFE_DIVIDE(
    COUNTIF(first_touch_ts IS NOT NULL), NULLIF(COUNT(*), 0)
  ), 4)                                                                      AS touch_rate,
  ROUND(AVG(speed_to_lead_minutes), 1)                                       AS avg_speed_minutes,
  CAST(APPROX_QUANTILES(speed_to_lead_minutes, 2)[OFFSET(1)] AS INT64)      AS median_speed_minutes,
  CAST(APPROX_QUANTILES(speed_to_lead_minutes, 10)[OFFSET(9)] AS INT64)     AS p90_speed_minutes,
  -- SLA tiers (denominator = touched leads only)
  COUNTIF(speed_to_lead_seconds <= 300)                                      AS within_5m,
  ROUND(SAFE_DIVIDE(
    COUNTIF(speed_to_lead_seconds <= 300),
    NULLIF(COUNTIF(first_touch_ts IS NOT NULL), 0)
  ), 4)                                                                      AS pct_within_5m,
  COUNTIF(speed_to_lead_seconds <= 900)                                      AS within_15m,
  ROUND(SAFE_DIVIDE(
    COUNTIF(speed_to_lead_seconds <= 900),
    NULLIF(COUNTIF(first_touch_ts IS NOT NULL), 0)
  ), 4)                                                                      AS pct_within_15m,
  COUNTIF(speed_to_lead_seconds <= 3600)                                     AS within_1h,
  ROUND(SAFE_DIVIDE(
    COUNTIF(speed_to_lead_seconds <= 3600),
    NULLIF(COUNTIF(first_touch_ts IS NOT NULL), 0)
  ), 4)                                                                      AS pct_within_1h,
  COUNTIF(first_touch_ts IS NOT NULL AND speed_to_lead_seconds > 7200)      AS sla_breached,
  ROUND(SAFE_DIVIDE(
    COUNTIF(first_touch_ts IS NOT NULL AND speed_to_lead_seconds > 7200),
    NULLIF(COUNTIF(first_touch_ts IS NOT NULL), 0)
  ), 4)                                                                      AS sla_breach_rate,
  CURRENT_TIMESTAMP()                                                         AS mart_refreshed_at
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
WHERE trigger_ts IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
;

-- ============================================================
-- rpt_rep_scorecard_week
-- Unified rep performance scorecard — one row per rep per week.
-- Pulls setter + closer metrics from existing marts.
-- Grain: report_week × rep_name
-- ============================================================
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_rep_scorecard_week` AS
WITH

-- All active rep × week combinations across setter and closer activity
rep_weeks AS (
  SELECT DISTINCT report_week, setter_name AS rep_name
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_appt_funnel_week`
  WHERE setter_name NOT IN ('unknown')
  UNION DISTINCT
  SELECT DISTINCT report_week, closer_name AS rep_name
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_closer_close_rate_week`
  WHERE closer_name NOT IN ('unassigned', 'unknown')
),

-- Setter: booking funnel (sum across campaigns for the week)
appt_funnel AS (
  SELECT
    report_week,
    setter_name,
    SUM(total_bookings)     AS bookings,
    SUM(shows)              AS shows,
    SUM(no_shows)           AS no_shows,
    SUM(host_canceled)      AS host_canceled,
    SUM(invitee_canceled)   AS invitee_canceled,
    ROUND(SAFE_DIVIDE(SUM(shows), NULLIF(SUM(shows) + SUM(no_shows), 0)), 4) AS show_rate
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_appt_funnel_week`
  GROUP BY 1, 2
),

-- Setter: outbound dials + call-to-booking rate
call_booking AS (
  SELECT
    report_week,
    setter_name,
    SUM(total_dials)                      AS total_dials,
    ROUND(AVG(call_to_booking_rate_14d), 4) AS call_to_booking_rate_14d
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_call_to_booking_rate_week`
  GROUP BY 1, 2
),

-- Setter: speed to lead (from fact table, weekly rollup)
speed AS (
  SELECT
    DATE_TRUNC(DATE(trigger_ts), WEEK(MONDAY))        AS report_week,
    COALESCE(setter_dim_display_name, 'unattributed') AS setter_name,
    ROUND(AVG(speed_to_lead_minutes), 1)              AS avg_speed_to_lead_minutes,
    ROUND(SAFE_DIVIDE(
      COUNTIF(is_within_sla),
      NULLIF(COUNTIF(first_touch_ts IS NOT NULL), 0)
    ), 4)                                             AS pct_within_sla
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
  WHERE trigger_ts IS NOT NULL
  GROUP BY 1, 2
),

-- Setter: unbooked conversion (sum across campaigns)
unbooked AS (
  SELECT
    report_week,
    setter_name,
    SUM(unbooked_leads_worked)          AS unbooked_leads_worked,
    SUM(total_dials)                    AS unbooked_total_dials,
    ROUND(AVG(conversion_rate_14d), 4)  AS unbooked_conversion_rate_14d
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_setter_unbooked_conversion_week`
  GROUP BY 1, 2
),

-- Closer: close rate + deal count
closer AS (
  SELECT
    report_week,
    closer_name,
    SUM(sales_calls_taken) AS sales_calls_taken,
    SUM(fathom_won_count)  AS deals_closed,
    ROUND(SAFE_DIVIDE(
      SUM(fathom_won_count), NULLIF(SUM(sales_calls_taken), 0)
    ), 4)                  AS close_rate
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_closer_close_rate_week`
  GROUP BY 1, 2
)

SELECT
  rw.report_week,
  rw.rep_name,

  -- Role: setter, closer, or both
  CASE
    WHEN af.setter_name IS NOT NULL AND cl.closer_name IS NOT NULL THEN 'setter+closer'
    WHEN af.setter_name IS NOT NULL                                 THEN 'setter'
    WHEN cl.closer_name IS NOT NULL                                 THEN 'closer'
    ELSE 'unknown'
  END AS rep_role,

  -- ── Setter: booking funnel ──────────────────────────────
  COALESCE(af.bookings, 0)                AS bookings,
  COALESCE(af.shows, 0)                   AS shows,
  COALESCE(af.no_shows, 0)               AS no_shows,
  COALESCE(af.host_canceled, 0)          AS host_canceled,
  COALESCE(af.invitee_canceled, 0)        AS invitee_canceled,
  af.show_rate,

  -- ── Setter: outbound activity ───────────────────────────
  COALESCE(cb.total_dials, 0)             AS total_dials,
  cb.call_to_booking_rate_14d,

  -- ── Setter: speed to lead ───────────────────────────────
  sp.avg_speed_to_lead_minutes,
  sp.pct_within_sla                       AS speed_to_lead_pct_within_sla,

  -- ── Setter: unbooked conversion ────────────────────────
  COALESCE(ub.unbooked_leads_worked, 0)   AS unbooked_leads_worked,
  COALESCE(ub.unbooked_total_dials, 0)    AS unbooked_total_dials,
  ub.unbooked_conversion_rate_14d,

  -- ── Closer: close rate ─────────────────────────────────
  COALESCE(cl.sales_calls_taken, 0)       AS sales_calls_taken,
  COALESCE(cl.deals_closed, 0)           AS deals_closed,
  cl.close_rate,

  CURRENT_TIMESTAMP()                     AS mart_refreshed_at

FROM rep_weeks rw
LEFT JOIN appt_funnel  af ON af.report_week = rw.report_week AND af.setter_name = rw.rep_name
LEFT JOIN call_booking cb ON cb.report_week = rw.report_week AND cb.setter_name = rw.rep_name
LEFT JOIN speed        sp ON sp.report_week = rw.report_week AND sp.setter_name = rw.rep_name
LEFT JOIN unbooked     ub ON ub.report_week = rw.report_week AND ub.setter_name = rw.rep_name
LEFT JOIN closer       cl ON cl.report_week = rw.report_week AND cl.closer_name = rw.rep_name
ORDER BY 1 DESC, 2
;

-- ============================================================
-- rpt_operations_kpi_panel
-- Dashboard-ready ops health panel used by the Operations dashboard.
-- Grain: one row per (section, period_key, dim_1, dim_2) at refresh time.
-- ============================================================
CREATE OR REPLACE TABLE `project-41542e21-470f-4589-96d.Marts.rpt_operations_kpi_panel` AS
WITH
mart_freshness AS (
  SELECT
    'Marts.mart_master_lead_wide' AS table_name,
    MAX(mart_refreshed_at) AS last_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`

  UNION ALL
  SELECT
    'Marts.fct_payment_line_unified' AS table_name,
    MAX(mart_refreshed_at) AS last_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`

  UNION ALL
  SELECT
    'Marts.fct_speed_to_lead' AS table_name,
    MAX(mart_refreshed_at) AS last_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`

  UNION ALL
  SELECT
    'Marts.rpt_campaign_funnel_month' AS table_name,
    MAX(mart_refreshed_at) AS last_refreshed_at
  FROM `project-41542e21-470f-4589-96d.Marts.rpt_campaign_funnel_month`
),
raw_freshness AS (
  SELECT
    'Raw.ghl_objects_raw' AS table_name,
    MAX(ingested_at) AS last_ingested_at
  FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`

  UNION ALL
  SELECT
    'Raw.calendly_objects_raw' AS table_name,
    MAX(ingested_at) AS last_ingested_at
  FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`

  UNION ALL
  SELECT
    'Raw.stripe_objects_raw' AS table_name,
    MAX(ingested_at) AS last_ingested_at
  FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`

  UNION ALL
  SELECT
    'Raw.fathom_calls_raw' AS table_name,
    MAX(ingested_at) AS last_ingested_at
  FROM `project-41542e21-470f-4589-96d.Raw.fathom_calls_raw`
),
sanity_counts AS (
  SELECT
    'Marts.fct_payment_line_unified' AS table_name,
    COUNTIF(DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS row_count_7d,
    COUNTIF(DATE(event_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS row_count_30d
  FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`

  UNION ALL
  SELECT
    'Marts.mart_master_lead_wide' AS table_name,
    COUNTIF(
      DATE(COALESCE(ghl_date_added_ts, ghl_first_seen_ts)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    ) AS row_count_7d,
    COUNTIF(
      DATE(COALESCE(ghl_date_added_ts, ghl_first_seen_ts)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ) AS row_count_30d
  FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`
),
raw_to_mart_lag AS (
  SELECT
    'Raw.ghl_objects_raw -> Marts.mart_master_lead_wide' AS lag_pair,
    (SELECT MAX(ingested_at) FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`) AS raw_max_ts,
    (SELECT MAX(mart_refreshed_at) FROM `project-41542e21-470f-4589-96d.Marts.mart_master_lead_wide`) AS mart_max_ts

  UNION ALL
  SELECT
    'Raw.calendly_objects_raw -> Marts.fct_speed_to_lead' AS lag_pair,
    (SELECT MAX(ingested_at) FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`) AS raw_max_ts,
    (SELECT MAX(mart_refreshed_at) FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`) AS mart_max_ts

  UNION ALL
  SELECT
    'Raw.stripe_objects_raw -> Marts.fct_payment_line_unified' AS lag_pair,
    (SELECT MAX(ingested_at) FROM `project-41542e21-470f-4589-96d.Raw.stripe_objects_raw`) AS raw_max_ts,
    (SELECT MAX(mart_refreshed_at) FROM `project-41542e21-470f-4589-96d.Marts.fct_payment_line_unified`) AS mart_max_ts
)
SELECT
  CURRENT_DATE() AS report_date,
  'mart_freshness' AS section,
  CAST(CURRENT_DATE() AS STRING) AS period_key,
  CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_refreshed_at, MINUTE) AS FLOAT64) AS metric_1_value,
  'freshness_age_minutes' AS metric_1_name,
  CAST(NULL AS FLOAT64) AS metric_2_value,
  CAST(NULL AS STRING) AS metric_2_name,
  table_name AS dim_1,
  CAST(NULL AS STRING) AS dim_2,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM mart_freshness

UNION ALL

SELECT
  CURRENT_DATE() AS report_date,
  'raw_ingest_freshness' AS section,
  CAST(CURRENT_DATE() AS STRING) AS period_key,
  CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_ingested_at, MINUTE) AS FLOAT64) AS metric_1_value,
  'freshness_age_minutes' AS metric_1_name,
  CAST(NULL AS FLOAT64) AS metric_2_value,
  CAST(NULL AS STRING) AS metric_2_name,
  table_name AS dim_1,
  CAST(NULL AS STRING) AS dim_2,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM raw_freshness

UNION ALL

SELECT
  CURRENT_DATE() AS report_date,
  'row_count_sanity' AS section,
  CAST(CURRENT_DATE() AS STRING) AS period_key,
  CAST(row_count_7d AS FLOAT64) AS metric_1_value,
  'row_count_last_7d' AS metric_1_name,
  CAST(row_count_30d AS FLOAT64) AS metric_2_value,
  'row_count_last_30d' AS metric_2_name,
  table_name AS dim_1,
  CAST(NULL AS STRING) AS dim_2,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM sanity_counts

UNION ALL

SELECT
  CURRENT_DATE() AS report_date,
  'raw_to_mart_lag' AS section,
  CAST(CURRENT_DATE() AS STRING) AS period_key,
  CAST(TIMESTAMP_DIFF(mart_max_ts, raw_max_ts, MINUTE) AS FLOAT64) AS metric_1_value,
  'raw_to_mart_lag_minutes' AS metric_1_name,
  CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), raw_max_ts, MINUTE) AS FLOAT64) AS metric_2_value,
  'raw_freshness_age_minutes' AS metric_2_name,
  lag_pair AS dim_1,
  CAST(NULL AS STRING) AS dim_2,
  CURRENT_TIMESTAMP() AS mart_refreshed_at
FROM raw_to_mart_lag
;

-- ============================================================
-- fct_funnel_stage_transitions
-- Stage transition events derived from daily Core snapshots.
-- Grain: location_id × opportunity_id × snapshot_date
-- ============================================================
CREATE OR REPLACE VIEW `project-41542e21-470f-4589-96d.Marts.fct_funnel_stage_transitions` AS
WITH ranked AS (
  SELECT
    location_id,
    opportunity_id,
    snapshot_date,
    pipeline_id,
    pipeline_name,
    pipeline_stage_id,
    stage_name,
    status,
    amount,
    assigned_to_user_id,
    days_in_current_stage,
    snapshotted_at,
    LAG(pipeline_stage_id) OVER w AS prev_pipeline_stage_id,
    LAG(stage_name) OVER w AS prev_stage_name,
    LAG(snapshot_date) OVER w AS prev_snapshot_date
  FROM `project-41542e21-470f-4589-96d.Core.fct_pipeline_stage_snapshots`
  WINDOW w AS (
    PARTITION BY location_id, opportunity_id
    ORDER BY snapshot_date, snapshotted_at
  )
)
SELECT
  location_id,
  opportunity_id,
  snapshot_date,
  pipeline_id,
  pipeline_name,
  pipeline_stage_id,
  stage_name,
  status,
  amount,
  assigned_to_user_id,
  days_in_current_stage,
  prev_pipeline_stage_id,
  prev_stage_name,
  prev_snapshot_date,
  (pipeline_stage_id IS DISTINCT FROM prev_pipeline_stage_id) AS is_stage_change,
  DATE_DIFF(snapshot_date, prev_snapshot_date, DAY) AS days_since_last_snapshot,
  CASE
    WHEN pipeline_stage_id IS DISTINCT FROM prev_pipeline_stage_id
      THEN DATE_DIFF(snapshot_date, prev_snapshot_date, DAY)
    ELSE NULL
  END AS days_spent_in_prev_stage,
  snapshotted_at
FROM ranked
;
