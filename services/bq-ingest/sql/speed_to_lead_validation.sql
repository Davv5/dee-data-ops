-- A) Raw/CORE touch-source availability and freshness.
SELECT
  'raw_outbound_call_logs' AS source_name,
  COUNT(*) AS row_count,
  MIN(event_ts) AS min_event_ts,
  MAX(event_ts) AS max_event_ts,
  MIN(ingested_at) AS min_ingested_at,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.ghl_objects_raw`
WHERE entity_type = 'outbound_call_logs'

UNION ALL

SELECT
  'core_outbound_calls' AS source_name,
  COUNT(*) AS row_count,
  MIN(COALESCE(call_started_at, event_ts, updated_at_ts, ingested_at)) AS min_event_ts,
  MAX(COALESCE(call_started_at, event_ts, updated_at_ts, ingested_at)) AS max_event_ts,
  MIN(ingested_at) AS min_ingested_at,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls`

UNION ALL

SELECT
  'core_outbound_conversations' AS source_name,
  COUNT(*) AS row_count,
  MIN(COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at)) AS min_event_ts,
  MAX(COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at)) AS max_event_ts,
  MIN(ingested_at) AS min_ingested_at,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_conversations`
WHERE LOWER(COALESCE(direction_norm, '')) = 'outbound'
  AND REGEXP_CONTAINS(
    LOWER(COALESCE(message_type_norm, '')),
    r'sms|text|whatsapp|type_sms|type_call|type_phone|phone|call'
  )
;

-- B) Core + mart row checks.
SELECT 'Core.fct_ghl_outbound_calls' AS table_name, COUNT(*) AS row_count
FROM `project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls`
UNION ALL
SELECT 'Core.fct_speed_to_lead_booking_to_call', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Core.fct_speed_to_lead_booking_to_call`
UNION ALL
SELECT 'Marts.dim_team_members', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.dim_team_members`
UNION ALL
SELECT 'Marts.fct_speed_to_lead', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
UNION ALL
SELECT 'Marts.mrt_speed_to_lead_daily', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.mrt_speed_to_lead_daily`
UNION ALL
SELECT 'Marts.mrt_speed_to_lead_overall', COUNT(*)
FROM `project-41542e21-470f-4589-96d.Marts.mrt_speed_to_lead_overall`
;

-- C) Time-travel check (should be zero).
SELECT
  COUNT(*) AS negative_speed_rows
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
WHERE speed_to_lead_seconds < 0
;

-- D) Ghost-rate audit by trigger type.
SELECT
  trigger_type,
  COUNT(*) AS total_triggers,
  COUNTIF(first_touch_ts IS NULL) AS triggers_without_touch,
  ROUND(100 * SAFE_DIVIDE(COUNTIF(first_touch_ts IS NULL), COUNT(*)), 2) AS ghost_rate_pct
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
GROUP BY 1
ORDER BY 1
;

-- E) First-touch channel mix (visibility check).
SELECT
  COALESCE(first_touch_channel, 'no_touch') AS first_touch_channel,
  COUNT(*) AS trigger_count,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2) AS trigger_pct
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
GROUP BY 1
ORDER BY trigger_count DESC
;

-- F) Setter attribution method mix (visibility check).
SELECT
  setter_attribution_method,
  COUNT(*) AS trigger_count,
  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2) AS trigger_pct
FROM `project-41542e21-470f-4589-96d.Marts.fct_speed_to_lead`
GROUP BY 1
ORDER BY trigger_count DESC
;

-- G) KPI sanity snapshot.
SELECT
  refreshed_at,
  total_bookings_matched_to_contact,
  bookings_with_outbound_call,
  bookings_without_outbound_call,
  avg_speed_to_lead_minutes,
  median_speed_to_lead_minutes,
  p90_speed_to_lead_minutes,
  pct_within_5m,
  pct_within_15m,
  pct_within_60m,
  total_triggers_all,
  triggers_with_outbound_touch,
  pct_triggers_with_outbound_touch,
  total_lead_magnet_triggers,
  lead_magnet_triggers_with_outbound_touch,
  pct_lead_magnet_triggers_with_outbound_touch
FROM `project-41542e21-470f-4589-96d.Marts.mrt_speed_to_lead_overall`
;
