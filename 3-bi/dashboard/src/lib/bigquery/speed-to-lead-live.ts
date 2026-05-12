import { queryContracts, type QueryName } from "@/lib/bigquery/named-queries";
import { runBigQuery } from "@/lib/bigquery/client";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

const tableRef = (queryName: QueryName) => `\`${queryContracts[queryName].table}\``;
const BOOKING_SLA_SECONDS = 45 * 60;

export const SPEED_TO_LEAD_TIME_RANGE_OPTIONS = [
  {
    value: "today",
    label: "Today",
    description: "Lead events from today in Eastern time.",
  },
  {
    value: "7d",
    label: "7D",
    description: "Lead events from the last 7 days.",
  },
  {
    value: "30d",
    label: "30D",
    description: "Lead events from the last 30 days.",
  },
  {
    value: "90d",
    label: "90D",
    description: "Lead events from the last 90 days.",
  },
  {
    value: "all",
    label: "All",
    description: "All lead events in the mart.",
  },
] as const;

export type SpeedToLeadTimeRange = (typeof SPEED_TO_LEAD_TIME_RANGE_OPTIONS)[number]["value"];

type GetSpeedToLeadDataOptions = {
  timeRange?: string | null;
};

const DEFAULT_TIME_RANGE: SpeedToLeadTimeRange = "today";

export function normalizeSpeedToLeadTimeRange(value: string | null | undefined): SpeedToLeadTimeRange {
  const normalized = value?.toLowerCase();
  const option = SPEED_TO_LEAD_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);
  return option?.value ?? DEFAULT_TIME_RANGE;
}

function buildDashboardFilters(timeRange: SpeedToLeadTimeRange): DashboardFilters {
  const activeOption = SPEED_TO_LEAD_TIME_RANGE_OPTIONS.find((option) => option.value === timeRange);

  return {
    timeRange,
    timeRangeLabel: activeOption?.label ?? "30D",
    timeRangeDescription: activeOption?.description ?? "Lead events from the last 30 days.",
    timeRangeOptions: SPEED_TO_LEAD_TIME_RANGE_OPTIONS.map((option) => ({ ...option })),
  };
}

function timestampRangePredicate(timeRange: SpeedToLeadTimeRange, field: string) {
  if (timeRange === "all") return "";
  if (timeRange === "today") {
    return `DATE(${field}, 'America/New_York') = CURRENT_DATE('America/New_York')`;
  }

  const days = timeRange === "7d" ? 6 : timeRange === "90d" ? 89 : 29;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function whereTimeRange(timeRange: SpeedToLeadTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

function andTimeRange(timeRange: SpeedToLeadTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `AND ${predicate}` : "";
}

// Predicate selecting the immediately-preceding window of the same length.
// Used for "vs prior period" deltas. Returns "" for "all" since there is no prior.
function priorTimestampRangePredicate(timeRange: SpeedToLeadTimeRange, field: string) {
  if (timeRange === "all") return "";
  if (timeRange === "today") {
    return `DATE(${field}, 'America/New_York') = DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)`;
  }
  const days = timeRange === "7d" ? 6 : timeRange === "90d" ? 89 : 29;
  // current window = >= days ago. prior window of same length = >= 2*days+1 ago AND < days+1 ago.
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days * 2 + 1} DAY)
    AND DATE(${field}, 'America/New_York') < DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days + 1} DAY)`;
}

function wherePriorTimeRange(timeRange: SpeedToLeadTimeRange, field: string) {
  const predicate = priorTimestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

const buildSpeedToLeadQualityCte = (timeRange: SpeedToLeadTimeRange) => `
  WITH trigger_events AS (
    SELECT
      trigger_event_id,
      golden_contact_key,
      trigger_ts,
      trigger_type,
      trigger_source_label,
      utm_source,
      utm_campaign
    FROM ${tableRef("freshness")}
    WHERE trigger_ts IS NOT NULL
      ${andTimeRange(timeRange, "trigger_ts")}
  ),
  outbound_touches_raw AS (
    SELECT
      gc.golden_contact_key,
      COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) AS touch_ts,
      'call' AS channel_group,
      LOWER(COALESCE(NULLIF(TRIM(c.call_status), ''), 'unknown')) AS touch_status,
      CONCAT('call|', COALESCE(c.call_log_id, CAST(FARM_FINGERPRINT(TO_JSON_STRING(c.payload_json)) AS STRING))) AS touch_id,
      LOWER(NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.source')), '')) AS touch_source_raw,
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.meta.marketplace.appName')), '') AS touch_provider_name,
      NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.from')), '') AS touch_from_number,
      NULLIF(TRIM(COALESCE(
        JSON_VALUE(c.payload_json, '$.assignedTo'),
        JSON_VALUE(c.payload_json, '$.assigned_to'),
        JSON_VALUE(c.payload_json, '$.userId'),
        JSON_VALUE(c.payload_json, '$.user_id'),
        JSON_VALUE(c.payload_json, '$.ownerId'),
        JSON_VALUE(c.payload_json, '$.owner_id'),
        JSON_VALUE(c.payload_json, '$.agentId'),
        JSON_VALUE(c.payload_json, '$.agent_id')
      )), '') AS touch_user_id
    FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls\` c
    JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
      ON gc.location_id = c.location_id
     AND gc.ghl_contact_id = c.contact_id
    WHERE COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) IS NOT NULL
      AND LOWER(COALESCE(c.direction_norm, '')) = 'outbound'

    UNION ALL

    SELECT
      gc.golden_contact_key,
      COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) AS touch_ts,
      CASE
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(m.message_type_norm, '')), r'email') THEN 'email'
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(m.message_type_norm, '')), r'sms|text|whatsapp') THEN 'sms'
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(m.message_type_norm, '')), r'call|phone') THEN 'call'
        ELSE 'conversation_phone'
      END AS channel_group,
      LOWER(COALESCE(NULLIF(TRIM(m.message_status_norm), ''), 'unknown')) AS touch_status,
      CONCAT(
        'message|',
        COALESCE(
          m.message_id,
          m.conversation_id,
          CAST(FARM_FINGERPRINT(TO_JSON_STRING(m.payload_json)) AS STRING)
        )
      ) AS touch_id,
      LOWER(NULLIF(TRIM(JSON_VALUE(m.payload_json, '$.source')), '')) AS touch_source_raw,
      NULLIF(TRIM(JSON_VALUE(m.payload_json, '$.meta.marketplace.appName')), '') AS touch_provider_name,
      NULLIF(TRIM(JSON_VALUE(m.payload_json, '$.from')), '') AS touch_from_number,
      NULLIF(TRIM(COALESCE(
        m.assigned_to_user_id,
        JSON_VALUE(m.payload_json, '$.assignedTo'),
        JSON_VALUE(m.payload_json, '$.assigned_to'),
        JSON_VALUE(m.payload_json, '$.userId'),
        JSON_VALUE(m.payload_json, '$.user_id'),
        JSON_VALUE(m.payload_json, '$.ownerId'),
        JSON_VALUE(m.payload_json, '$.owner_id')
      )), '') AS touch_user_id
    FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_conversations\` m
    JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
      ON gc.location_id = m.location_id
     AND gc.ghl_contact_id = m.contact_id
    WHERE COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) IS NOT NULL
      AND (
        LOWER(COALESCE(m.direction_norm, '')) = 'outbound'
        OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(m.payload_json, '$.lastMessageDirection'), '')), r'outbound')
      )
      AND REGEXP_CONTAINS(
        LOWER(COALESCE(m.message_type_norm, '')),
        r'sms|text|whatsapp|email|type_call|type_phone|phone|call'
      )
  ),
  outbound_touches AS (
    SELECT
      r.*,
      CASE
        WHEN r.touch_source_raw = 'workflow' THEN 'Workflow automation'
        WHEN r.touch_user_id = 'leBv9MtltaKdfSijVEhb' THEN 'Aariz Menon'
        WHEN NULLIF(u.name, '') IS NOT NULL THEN u.name
        WHEN r.touch_user_id = 'Oct5Tz6ZVUaDkqXC3yHL' THEN 'Deleted GHL user Oct5Tz6ZVUaDkqXC3yHL'
        WHEN r.touch_user_id IS NOT NULL THEN CONCAT('Unmapped GHL user ', r.touch_user_id)
        -- Dialer lines (e.g. WAVV) don't carry a userId in the payload. Use the
        -- last 4 digits of the from-number as a short identity until a manual
        -- line→rep mapping is available. Format: "WAVV ·3453".
        WHEN r.touch_provider_name IS NOT NULL AND r.touch_from_number IS NOT NULL THEN CONCAT(
          REPLACE(r.touch_provider_name, ' Dialer', ''),
          ' ·',
          RIGHT(REGEXP_REPLACE(r.touch_from_number, r'[^0-9]', ''), 4)
        )
        WHEN r.touch_provider_name IS NOT NULL THEN r.touch_provider_name
        WHEN r.touch_from_number IS NOT NULL THEN CONCAT(
          'Phone ·',
          RIGHT(REGEXP_REPLACE(r.touch_from_number, r'[^0-9]', ''), 4)
        )
        ELSE 'GHL event with no rep supplied'
      END AS touch_owner_name,
      CASE
        WHEN r.touch_source_raw = 'workflow' THEN 'Automation'
        WHEN NULLIF(u.role, '') IS NOT NULL THEN u.role
        WHEN r.touch_user_id IS NOT NULL THEN 'Unmapped GHL user'
        WHEN r.touch_provider_name IS NOT NULL THEN 'Dialer number'
        WHEN r.touch_from_number IS NOT NULL THEN 'Phone number'
        ELSE 'No rep supplied'
      END AS touch_owner_role,
      CASE
        WHEN r.touch_source_raw = 'workflow' THEN 'Workflow'
        WHEN NULLIF(u.name, '') IS NOT NULL THEN 'GHL user'
        WHEN r.touch_user_id = 'Oct5Tz6ZVUaDkqXC3yHL' THEN 'Deleted GHL user ID'
        WHEN r.touch_user_id IS NOT NULL THEN 'Unmapped GHL user ID'
        WHEN r.touch_provider_name IS NOT NULL AND r.touch_from_number IS NOT NULL THEN 'Dialer number'
        WHEN r.touch_provider_name IS NOT NULL THEN 'Dialer app'
        WHEN r.touch_from_number IS NOT NULL THEN 'Phone number'
        ELSE 'No rep supplied'
      END AS touch_owner_source
    FROM outbound_touches_raw r
    LEFT JOIN \`project-41542e21-470f-4589-96d.Core.dim_users\` u
      ON u.user_id = r.touch_user_id
  ),
  classified_touches AS (
    SELECT
      *,
      touch_source_raw = 'workflow' AS is_automated_workflow_touch,
      CASE
        WHEN channel_group = 'call' AND touch_status IN ('answered', 'completed') THEN 'successful_connection'
        WHEN channel_group = 'call' AND touch_status IN ('no-answer', 'failed', 'canceled', 'busy', 'undelivered', 'voicemail') THEN CONCAT('call_', REPLACE(touch_status, '-', '_'))
        WHEN channel_group = 'call' THEN CONCAT('call_', REPLACE(touch_status, '-', '_'))
        WHEN channel_group IN ('sms', 'email') AND touch_status IN ('delivered', 'sent', 'completed') THEN CONCAT(channel_group, '_delivered')
        WHEN channel_group IN ('sms', 'email') AND touch_status IN ('failed', 'undelivered', 'canceled') THEN CONCAT(channel_group, '_failed')
        ELSE CONCAT(channel_group, '_', REPLACE(touch_status, '-', '_'))
      END AS touch_outcome,
      channel_group = 'call'
        AND touch_status IN ('answered', 'completed') AS is_successful_connection,
      COALESCE(touch_source_raw, '') != 'workflow'
        AND (
          (channel_group = 'call' AND touch_status IN ('answered', 'completed'))
          OR (channel_group IN ('sms', 'email') AND touch_status IN ('delivered', 'sent', 'completed'))
        ) AS is_meaningful_human_response
    FROM outbound_touches
  ),
  trigger_touch_candidates AS (
    SELECT
      t.trigger_event_id,
      t.golden_contact_key,
      t.trigger_ts,
      t.trigger_type,
      t.trigger_source_label,
      t.utm_source,
      t.utm_campaign,
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM DATETIME(t.trigger_ts, 'America/New_York')) BETWEEN 2 AND 6
          AND TIME(DATETIME(t.trigger_ts, 'America/New_York')) >= TIME '09:00:00'
          AND TIME(DATETIME(t.trigger_ts, 'America/New_York')) < TIME '18:00:00'
          THEN 'business_hours'
        ELSE 'after_hours'
      END AS service_window,
      c.touch_ts,
      c.channel_group,
      c.touch_status,
      c.touch_id,
      c.touch_source_raw,
      c.touch_user_id,
      c.touch_owner_name,
      c.touch_owner_role,
      c.touch_owner_source,
      c.is_automated_workflow_touch,
      c.touch_outcome,
      c.is_successful_connection,
      c.is_meaningful_human_response
    FROM trigger_events t
    LEFT JOIN classified_touches c
      ON c.golden_contact_key = t.golden_contact_key
     AND c.touch_ts >= t.trigger_ts
  ),
  trigger_rollup AS (
    SELECT
      trigger_event_id,
      ANY_VALUE(golden_contact_key) AS golden_contact_key,
      ANY_VALUE(trigger_type) AS trigger_type,
      ANY_VALUE(trigger_source_label) AS trigger_source_label,
      ANY_VALUE(utm_source) AS utm_source,
      ANY_VALUE(utm_campaign) AS utm_campaign,
      ANY_VALUE(service_window) AS service_window,
      ANY_VALUE(trigger_ts) AS trigger_ts,
      ARRAY_AGG(
        IF(
          touch_ts IS NULL,
          NULL,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts, touch_user_id, touch_owner_name, touch_owner_role, touch_owner_source)
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_attempt,
      ARRAY_AGG(
        IF(
          is_successful_connection,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts, touch_user_id, touch_owner_name, touch_owner_role, touch_owner_source),
          NULL
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_successful_connection,
      ARRAY_AGG(
        IF(
          is_meaningful_human_response,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts, touch_user_id, touch_owner_name, touch_owner_role, touch_owner_source),
          NULL
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_meaningful_human_response,
      ARRAY_AGG(
        IF(
          is_automated_workflow_touch,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts, touch_user_id, touch_owner_name, touch_owner_role, touch_owner_source),
          NULL
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_automated_workflow_touch
    FROM trigger_touch_candidates
    GROUP BY trigger_event_id
  )
`;

function buildSpeedToLeadQueries(timeRange: SpeedToLeadTimeRange) {
  const qualityCte = buildSpeedToLeadQualityCte(timeRange);
  const triggerWhere = whereTimeRange(timeRange, "trigger_ts");
  const priorTriggerWhere = wherePriorTimeRange(timeRange, "trigger_ts");

  return {
  speed_to_lead_overall: `
    SELECT
      (
        SELECT FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at))
        FROM ${tableRef("freshness")}
      ) AS refreshed_at,
      COUNTIF(trigger_type = 'appointment_booking') AS total_bookings_matched_to_contact,
      COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NOT NULL) AS bookings_with_outbound_call,
      COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NULL) AS bookings_without_outbound_call,
      ROUND(AVG(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL)), 2) AS avg_speed_to_lead_minutes,
      ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(50)], 2) AS median_speed_to_lead_minutes,
      ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(90)], 2) AS p90_speed_to_lead_minutes,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_sla,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 300),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_5m,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 900),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_15m,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 3600),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_60m,
      COUNT(*) AS total_triggers_all,
      COUNTIF(first_touch_ts IS NOT NULL) AS triggers_with_outbound_touch,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)) AS pct_triggers_with_outbound_touch,
      COUNTIF(trigger_type = 'lead_magnet') AS total_lead_magnet_triggers,
      COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL) AS lead_magnet_triggers_with_outbound_touch,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'lead_magnet' AND first_touch_ts IS NOT NULL),
        NULLIF(COUNTIF(trigger_type = 'lead_magnet'), 0)
      ) AS pct_lead_magnet_triggers_with_outbound_touch
    FROM ${tableRef("freshness")}
    ${triggerWhere}
  `,
  speed_to_lead_overall_prior: priorTriggerWhere
    ? `
        SELECT
          SAFE_DIVIDE(
            COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}),
            NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
          ) AS pct_within_sla,
          SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), NULLIF(COUNT(*), 0)) AS pct_triggers_with_outbound_touch,
          ROUND(AVG(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL)), 2) AS avg_speed_to_lead_minutes,
          ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(50)], 2) AS median_speed_to_lead_minutes,
          COUNT(*) AS total_triggers_all,
          COUNTIF(trigger_type = 'appointment_booking') AS total_bookings_matched_to_contact
        FROM ${tableRef("freshness")}
        ${priorTriggerWhere}
      `
    : `SELECT NULL AS pct_within_sla, NULL AS pct_triggers_with_outbound_touch, NULL AS avg_speed_to_lead_minutes, NULL AS median_speed_to_lead_minutes, 0 AS total_triggers_all, 0 AS total_bookings_matched_to_contact FROM (SELECT 1) WHERE FALSE`,
  speed_to_lead_daily: `
    SELECT
      FORMAT_DATE('%Y-%m-%d', trigger_date) AS report_date,
      COUNTIF(trigger_type = 'appointment_booking') AS total_bookings_matched_to_contact,
      COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NOT NULL) AS bookings_with_outbound_call,
      COUNTIF(trigger_type = 'appointment_booking' AND first_touch_ts IS NULL) AS bookings_without_outbound_call,
      ROUND(AVG(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL)), 2) AS avg_speed_to_lead_minutes,
      ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(50)], 2) AS median_speed_to_lead_minutes,
      ROUND(APPROX_QUANTILES(IF(trigger_type = 'appointment_booking', speed_to_lead_minutes, NULL), 100)[OFFSET(90)], 2) AS p90_speed_to_lead_minutes,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_sla,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 300),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_5m,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 900),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_15m,
      SAFE_DIVIDE(
        COUNTIF(trigger_type = 'appointment_booking' AND speed_to_lead_seconds <= 3600),
        NULLIF(COUNTIF(trigger_type = 'appointment_booking'), 0)
      ) AS pct_within_60m,
      COUNT(*) AS total_triggers_all,
      COUNTIF(first_touch_ts IS NOT NULL) AS triggers_with_outbound_touch,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)) AS pct_triggers_with_outbound_touch
    FROM ${tableRef("freshness")}
    ${triggerWhere}
    GROUP BY trigger_date
    ORDER BY trigger_date
  `,
  speed_to_lead_by_rep: `
    -- Computed from trigger_rollup so it (a) respects the dashboard's time range
    -- and (b) credits the actual rep who made the first attempt. The pre-aggregated
    -- mart rpt_rep_scorecard_week has bookings/dials columns wired to literal zeros
    -- for every rep / every week, so we cannot rely on it for activity counts.
    ${qualityCte},
    rep_attempts AS (
      SELECT
        CASE
          WHEN first_attempt.is_automated_workflow_touch THEN 'Workflow automation'
          ELSE COALESCE(NULLIF(first_attempt.touch_owner_name, ''), 'Unknown rep')
        END AS rep_name,
        CASE
          WHEN first_attempt.is_automated_workflow_touch THEN 'automation'
          ELSE COALESCE(NULLIF(first_attempt.touch_owner_role, ''), 'unknown')
        END AS rep_role,
        first_attempt.is_automated_workflow_touch AS is_automation,
        first_attempt.touch_ts AS first_attempt_ts,
        first_successful_connection.touch_ts AS first_connection_ts,
        trigger_ts,
        trigger_type,
        TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) AS seconds_to_first_attempt
      FROM trigger_rollup
      WHERE first_attempt.touch_ts IS NOT NULL
    )
    SELECT
      rep_name,
      ANY_VALUE(rep_role) AS rep_role,
      ANY_VALUE(is_automation) AS is_automation,
      COUNT(*) AS leads_worked,
      COUNTIF(first_connection_ts IS NOT NULL) AS leads_reached,
      COUNTIF(trigger_type = 'appointment_booking') AS bookings_worked,
      AVG(seconds_to_first_attempt) / 60.0 AS avg_speed_to_lead_minutes,
      SAFE_DIVIDE(
        COUNTIF(seconds_to_first_attempt <= ${BOOKING_SLA_SECONDS}),
        COUNT(*)
      ) AS pct_within_sla
    FROM rep_attempts
    GROUP BY rep_name
    HAVING leads_worked > 0
    ORDER BY leads_worked DESC, pct_within_sla DESC NULLS LAST, rep_name
    LIMIT 20
  `,
  speed_to_lead_trigger_summary: `
    SELECT
      trigger_type,
      COUNT(*) AS total_triggers,
      COUNTIF(first_touch_ts IS NOT NULL) AS touched,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)) AS touch_rate,
      COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}) AS within_sla,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS within_sla_rate,
      COUNTIF(speed_to_lead_seconds <= 300) AS within_5m,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= 300), COUNT(*)) AS within_5m_rate,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(50)] AS median_minutes,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(90)] AS p90_minutes
    FROM ${tableRef("freshness")}
    ${triggerWhere}
    GROUP BY trigger_type
    ORDER BY total_triggers DESC
  `,
  speed_to_lead_response_buckets: `
    WITH bucketed AS (
      SELECT
        trigger_type,
        CASE
          WHEN first_touch_ts IS NULL THEN 'no touch'
          WHEN speed_to_lead_minutes <= 1 THEN '<=1m'
          WHEN speed_to_lead_minutes <= 5 THEN '1-5m'
          WHEN speed_to_lead_minutes <= 15 THEN '5-15m'
          WHEN speed_to_lead_minutes <= 60 THEN '15-60m'
          WHEN speed_to_lead_minutes <= 1440 THEN '1-24h'
          ELSE '>24h'
        END AS response_bucket,
        CASE
          WHEN first_touch_ts IS NULL THEN 7
          WHEN speed_to_lead_minutes <= 1 THEN 1
          WHEN speed_to_lead_minutes <= 5 THEN 2
          WHEN speed_to_lead_minutes <= 15 THEN 3
          WHEN speed_to_lead_minutes <= 60 THEN 4
          WHEN speed_to_lead_minutes <= 1440 THEN 5
          ELSE 6
        END AS bucket_order
      FROM ${tableRef("freshness")}
      ${triggerWhere}
    ),
    counted AS (
      SELECT
        trigger_type,
        response_bucket,
        bucket_order,
        COUNT(*) AS triggers
      FROM bucketed
      GROUP BY trigger_type, response_bucket, bucket_order
    )
    SELECT
      trigger_type,
      response_bucket,
      triggers,
      SAFE_DIVIDE(triggers, SUM(triggers) OVER (PARTITION BY trigger_type)) AS share_of_type
    FROM counted
    ORDER BY trigger_type, bucket_order
  `,
  speed_to_lead_source_performance: `
    SELECT
      COALESCE(NULLIF(trigger_source_label, ''), 'Unknown') AS source_label,
      trigger_type,
      COUNT(*) AS total_triggers,
      COUNTIF(first_touch_ts IS NOT NULL) AS touched,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)) AS touch_rate,
      COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}) AS within_sla,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS within_sla_rate,
      COUNTIF(speed_to_lead_seconds <= 300) AS within_5m,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= 300), COUNT(*)) AS within_5m_rate,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(50)] AS median_minutes
    FROM ${tableRef("freshness")}
    ${triggerWhere}
    GROUP BY source_label, trigger_type
    HAVING total_triggers >= 5
    ORDER BY total_triggers DESC
    LIMIT 12
  `,
  speed_to_lead_lane_summary: `
    WITH lane_rollup AS (
      SELECT
        CASE
          WHEN trigger_type = 'appointment_booking'
            AND REGEXP_CONTAINS(
              LOWER(CONCAT(COALESCE(trigger_source_label, ''), ' ', COALESCE(utm_source, ''), ' ', COALESCE(utm_campaign, ''))),
              r'\\bob\\b|outbound|check up'
            )
            THEN 'outbound_existing_bookings'
          WHEN trigger_type = 'appointment_booking' THEN 'inbound_bookings'
          WHEN trigger_type = 'lead_magnet' THEN 'lead_magnets'
          ELSE 'other'
        END AS lane_id,
        CASE
          WHEN first_touch_ts IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24 THEN TRUE
          ELSE FALSE
        END AS is_fresh_no_touch,
        CASE
          WHEN first_touch_ts IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) >= 24 THEN TRUE
          ELSE FALSE
        END AS is_stale_no_touch,
        first_touch_ts,
        speed_to_lead_seconds,
        speed_to_lead_minutes
      FROM ${tableRef("freshness")}
      ${triggerWhere}
    )
    SELECT
      lane_id,
      COUNT(*) AS lead_events,
      COUNTIF(first_touch_ts IS NOT NULL) AS worked,
      COUNTIF(first_touch_ts IS NULL) AS no_touch,
      COUNTIF(is_fresh_no_touch) AS fresh_no_touch,
      COUNTIF(is_stale_no_touch) AS stale_no_touch,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NULL), COUNT(*)) AS no_touch_rate,
      COUNTIF(speed_to_lead_seconds <= 300) AS within_5m,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= 300), COUNT(*)) AS within_5m_rate,
      COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}) AS within_sla,
      SAFE_DIVIDE(COUNTIF(speed_to_lead_seconds <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS within_sla_rate,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(50)] AS median_minutes
    FROM lane_rollup
    GROUP BY lane_id
    ORDER BY CASE lane_id
      WHEN 'inbound_bookings' THEN 1
      WHEN 'lead_magnets' THEN 2
      WHEN 'outbound_existing_bookings' THEN 3
      ELSE 4
    END
  `,
  speed_to_lead_no_touch_examples: `
    SELECT
      f.trigger_event_id,
      FORMAT_TIMESTAMP('%FT%TZ', f.trigger_ts) AS trigger_ts,
      FORMAT_TIMESTAMP('%Y-%m-%d %I:%M %p ET', f.trigger_ts, 'America/New_York') AS trigger_ts_et,
      FORMAT_DATE('%Y-%m-%d', f.trigger_date) AS trigger_date,
      f.trigger_type,
      CASE
        WHEN f.trigger_type = 'appointment_booking'
          AND REGEXP_CONTAINS(
            LOWER(CONCAT(COALESCE(f.trigger_source_label, ''), ' ', COALESCE(f.utm_source, ''), ' ', COALESCE(f.utm_campaign, ''))),
            r'\\bob\\b|outbound|check up'
          )
          THEN 'outbound_existing_bookings'
        WHEN f.trigger_type = 'appointment_booking' THEN 'inbound_bookings'
        WHEN f.trigger_type = 'lead_magnet' THEN 'lead_magnets'
        ELSE 'other'
      END AS lane_id,
      COALESCE(NULLIF(f.trigger_source_label, ''), 'Unknown') AS source_label,
      COALESCE(
        NULLIF(gc.full_name, ''),
        NULLIF(TRIM(CONCAT(COALESCE(gc.first_name, ''), ' ', COALESCE(gc.last_name, ''))), ''),
        'Unknown lead'
      ) AS lead_name,
      COALESCE(NULLIF(gc.email, ''), 'No email') AS lead_email,
      COALESCE(NULLIF(gc.phone, ''), 'No phone') AS lead_phone,
      COALESCE(NULLIF(gc.ghl_contact_id, ''), 'No contact id') AS ghl_contact_id,
      COALESCE(NULLIF(gc.rep_name, ''), NULLIF(gc.setter_at_first_contact, ''), 'Unassigned') AS assigned_rep,
      COALESCE(NULLIF(f.utm_source, ''), 'N/A') AS utm_source,
      COALESCE(NULLIF(f.utm_campaign, ''), 'N/A') AS utm_campaign,
      CASE
        WHEN EXTRACT(DAYOFWEEK FROM DATETIME(f.trigger_ts, 'America/New_York')) BETWEEN 2 AND 6
          AND TIME(DATETIME(f.trigger_ts, 'America/New_York')) >= TIME '09:00:00'
          AND TIME(DATETIME(f.trigger_ts, 'America/New_York')) < TIME '18:00:00'
          THEN 'business_hours'
        ELSE 'after_hours'
      END AS service_window,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), f.trigger_ts, MINUTE) AS age_minutes,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), f.trigger_ts, HOUR) AS age_hours
    FROM ${tableRef("freshness")} f
    LEFT JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
      ON gc.golden_contact_key = f.golden_contact_key
    WHERE f.first_touch_ts IS NULL
      ${andTimeRange(timeRange, "f.trigger_ts")}
    ORDER BY f.trigger_ts DESC
    LIMIT 50
  `,
  speed_to_lead_routing_readiness: `
    ${qualityCte},
    assigned AS (
      SELECT
        COALESCE(NULLIF(gc.rep_name, ''), NULLIF(gc.setter_at_first_contact, ''), 'Unassigned') AS assigned_rep,
        COUNT(*) AS owned_leads,
        COUNTIF(tr.first_attempt.touch_ts IS NULL) AS unworked_leads,
        COUNTIF(tr.first_attempt.touch_ts IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), tr.trigger_ts, HOUR) < 24) AS fresh_unworked_leads,
        COUNTIF(tr.first_attempt.touch_ts IS NULL AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), tr.trigger_ts, HOUR) >= 24) AS stale_unworked_leads,
        COUNTIF(tr.first_attempt.touch_ts IS NULL AND tr.trigger_type = 'appointment_booking') AS unworked_bookings,
        COUNTIF(tr.first_attempt.touch_ts IS NULL AND tr.service_window = 'business_hours') AS business_hours_unworked,
        COUNTIF(NULLIF(gc.phone, '') IS NULL) AS no_phone_leads,
        COUNTIF(TIMESTAMP_DIFF(tr.first_attempt.touch_ts, tr.trigger_ts, SECOND) <= 300) AS first_attempt_within_5m,
        SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(tr.first_attempt.touch_ts, tr.trigger_ts, SECOND) <= 300), COUNT(*)) AS first_attempt_within_5m_rate,
        COUNTIF(TIMESTAMP_DIFF(tr.first_attempt.touch_ts, tr.trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}) AS first_attempt_within_sla,
        SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(tr.first_attempt.touch_ts, tr.trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS first_attempt_within_sla_rate,
        AVG(TIMESTAMP_DIFF(tr.first_attempt.touch_ts, tr.trigger_ts, SECOND) / 60.0) AS avg_minutes_to_first_attempt,
        FORMAT_TIMESTAMP('%Y-%m-%d %I:%M %p ET', MAX(tr.first_attempt.touch_ts), 'America/New_York') AS last_activity_et
      FROM trigger_rollup tr
      LEFT JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
        ON gc.golden_contact_key = tr.golden_contact_key
      GROUP BY assigned_rep
    )
    SELECT *
    FROM assigned
    ORDER BY
      CASE WHEN assigned_rep = 'Unassigned' THEN 0 ELSE 1 END,
      fresh_unworked_leads DESC,
      unworked_bookings DESC,
      first_attempt_within_5m_rate ASC,
      owned_leads DESC
    LIMIT 20
  `,
  speed_to_lead_critical_exceptions: `
    ${qualityCte},
    lead_rows AS (
      SELECT
        tr.trigger_event_id,
        tr.trigger_ts,
        tr.trigger_type,
        tr.trigger_source_label,
        tr.utm_campaign,
        tr.service_window,
        tr.first_attempt.touch_ts AS first_attempt_ts,
        COALESCE(NULLIF(gc.rep_name, ''), NULLIF(gc.setter_at_first_contact, ''), 'Unassigned') AS assigned_rep,
        COALESCE(NULLIF(gc.phone, ''), 'No phone') AS lead_phone,
        CASE
          WHEN tr.trigger_type = 'appointment_booking'
            AND REGEXP_CONTAINS(
              LOWER(CONCAT(COALESCE(tr.trigger_source_label, ''), ' ', COALESCE(tr.utm_source, ''), ' ', COALESCE(tr.utm_campaign, ''))),
              r'\\bob\\b|outbound|check up'
            )
            THEN TRUE
          ELSE FALSE
        END AS is_outbound_existing
      FROM trigger_rollup tr
      LEFT JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
        ON gc.golden_contact_key = tr.golden_contact_key
    ),
    exceptions AS (
      SELECT
        'unassigned' AS exception_key,
        'Unassigned leads' AS exception_label,
        'Assign an owner so SLA can be measured fairly.' AS manager_action,
        assigned_rep,
        trigger_ts,
        first_attempt_ts
      FROM lead_rows
      WHERE assigned_rep = 'Unassigned'

      UNION ALL

      SELECT
        'no_phone' AS exception_key,
        'Missing phone' AS exception_label,
        'Fix the lead-capture form or use non-phone follow-up — these are not dial failures.' AS manager_action,
        assigned_rep,
        trigger_ts,
        first_attempt_ts
      FROM lead_rows
      WHERE lead_phone = 'No phone'

      UNION ALL

      SELECT
        'after_hours' AS exception_key,
        'After-hours trigger' AS exception_label,
        'Queue for the next business block, unless the team is 24/7.' AS manager_action,
        assigned_rep,
        trigger_ts,
        first_attempt_ts
      FROM lead_rows
      WHERE service_window = 'after_hours'

      UNION ALL

      SELECT
        'stale_no_touch' AS exception_key,
        'Stale no-touch' AS exception_label,
        'Use for cleanup or reassignment — these are not live saves.' AS manager_action,
        assigned_rep,
        trigger_ts,
        first_attempt_ts
      FROM lead_rows
      WHERE first_attempt_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) >= 24

      UNION ALL

      SELECT
        'outbound_existing' AS exception_key,
        'Outbound/existing booking' AS exception_label,
        'Review separately so fresh inbound SLA stays clean.' AS manager_action,
        assigned_rep,
        trigger_ts,
        first_attempt_ts
      FROM lead_rows
      WHERE is_outbound_existing
    )
    SELECT
      exception_key,
      exception_label,
      manager_action,
      COUNT(*) AS lead_events,
      COUNTIF(first_attempt_ts IS NULL) AS unworked_leads,
      COUNTIF(
        first_attempt_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24
      ) AS open_unmatched_leads,
      COUNTIF(
        first_attempt_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) >= 24
      ) AS historical_unmatched_events,
      COUNT(DISTINCT assigned_rep) AS affected_reps,
      MAX(IF(
        first_attempt_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR),
        NULL
      )) AS oldest_open_age_hours,
      MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR)) AS oldest_age_hours
    FROM exceptions
    GROUP BY exception_key, exception_label, manager_action
    ORDER BY open_unmatched_leads DESC, historical_unmatched_events DESC, lead_events DESC, exception_label
  `,
  speed_to_lead_quality_summary: `
    ${qualityCte}
    SELECT
      COUNT(*) AS total_triggers,
      COUNTIF(first_attempt.touch_ts IS NOT NULL) AS first_attempts,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NOT NULL), COUNT(*)) AS first_attempt_rate,
      COUNTIF(first_attempt.touch_ts IS NOT NULL) AS worked_leads,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NOT NULL), COUNT(*)) AS worked_lead_rate,
      COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS successful_connections,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS successful_connection_rate,
      COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS reached_leads,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS reached_lead_rate,
      COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL) AS meaningful_human_responses,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS meaningful_human_response_rate,
      COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL) AS human_follow_ups,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS human_follow_up_rate,
      COUNTIF(first_automated_workflow_touch.touch_ts IS NOT NULL) AS automated_workflow_touches,
      SAFE_DIVIDE(COUNTIF(first_automated_workflow_touch.touch_ts IS NOT NULL), COUNT(*)) AS automated_workflow_touch_rate,
      COUNTIF(first_automated_workflow_touch.touch_ts IS NOT NULL) AS automation_touched_leads,
      SAFE_DIVIDE(COUNTIF(first_automated_workflow_touch.touch_ts IS NOT NULL), COUNT(*)) AS automation_touch_rate,
      COUNTIF(first_attempt.touch_ts IS NULL) AS no_attempt,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NULL), COUNT(*)) AS no_attempt_rate,
      COUNTIF(first_attempt.touch_ts IS NULL) AS unworked_leads,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NULL), COUNT(*)) AS unworked_lead_rate,
      COUNTIF(
        first_attempt.touch_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24
      ) AS open_unworked_leads,
      COUNTIF(
        first_attempt.touch_ts IS NULL
        AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) >= 24
      ) AS stale_unworked_leads
    FROM trigger_rollup
  `,
  speed_to_lead_follow_up_counts: `
    ${qualityCte},
    summary AS (
      SELECT
        COUNT(*) AS total_triggers,
        COUNTIF(first_attempt.touch_ts IS NOT NULL) AS worked_leads,
        COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS reached_leads,
        COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL) AS human_follow_ups,
        COUNTIF(first_automated_workflow_touch.touch_ts IS NOT NULL) AS automation_touched_leads,
        COUNTIF(first_attempt.touch_ts IS NULL) AS unworked_leads,
        COUNTIF(
          first_attempt.touch_ts IS NULL
          AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24
        ) AS open_unworked_leads,
        COUNTIF(
          first_attempt.touch_ts IS NULL
          AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) >= 24
        ) AS stale_unworked_leads
      FROM trigger_rollup
    )
    SELECT
      1 AS metric_order,
      'All lead events' AS metric,
      total_triggers AS lead_count,
      total_triggers AS denominator_count,
      1.0 AS share_of_all_leads,
      CAST(NULL AS FLOAT64) AS share_of_worked_leads,
      'Every lead trigger included in this dashboard.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      2 AS metric_order,
      'Post-trigger touch found' AS metric,
      worked_leads AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(worked_leads, total_triggers) AS share_of_all_leads,
      SAFE_DIVIDE(worked_leads, worked_leads) AS share_of_worked_leads,
      'Had a qualifying outbound call, text, email, or logged conversation after this specific Speed-to-Lead trigger.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      3 AS metric_order,
      'Reached by phone' AS metric,
      reached_leads AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(reached_leads, total_triggers) AS share_of_all_leads,
      SAFE_DIVIDE(reached_leads, worked_leads) AS share_of_worked_leads,
      'Had an answered or completed outbound phone call after the lead event.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      4 AS metric_order,
      'Human follow-up' AS metric,
      human_follow_ups AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(human_follow_ups, total_triggers) AS share_of_all_leads,
      SAFE_DIVIDE(human_follow_ups, worked_leads) AS share_of_worked_leads,
      'Had a non-workflow call, text, or email that counts as a real team response.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      5 AS metric_order,
      'Automation touched' AS metric,
      automation_touched_leads AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(automation_touched_leads, total_triggers) AS share_of_all_leads,
      SAFE_DIVIDE(automation_touched_leads, worked_leads) AS share_of_worked_leads,
      'Had at least one workflow-generated follow-up after the lead event.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      6 AS metric_order,
      'Open unmatched post-trigger (<24h)' AS metric,
      open_unworked_leads AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(open_unworked_leads, total_triggers) AS share_of_all_leads,
      CAST(NULL AS FLOAT64) AS share_of_worked_leads,
      'No qualifying post-trigger outbound touch was found and the lead is still within the live operating window.' AS plain_english
    FROM summary

    UNION ALL

    SELECT
      7 AS metric_order,
      'Historical unmatched post-trigger' AS metric,
      stale_unworked_leads AS lead_count,
      total_triggers AS denominator_count,
      SAFE_DIVIDE(stale_unworked_leads, total_triggers) AS share_of_all_leads,
      CAST(NULL AS FLOAT64) AS share_of_worked_leads,
      'No qualifying post-trigger outbound touch was found, but this is historical attribution/backlog signal, not proof the person was never worked.' AS plain_english
    FROM summary
    ORDER BY metric_order
  `,
  speed_to_lead_unmatched_truth_audit: `
    ${qualityCte},
    unmatched AS (
      SELECT
        tr.trigger_event_id,
        tr.golden_contact_key,
        tr.trigger_ts,
        tr.trigger_type,
        CASE
          WHEN tr.trigger_type = 'appointment_booking'
            AND REGEXP_CONTAINS(
              LOWER(CONCAT(COALESCE(tr.trigger_source_label, ''), ' ', COALESCE(tr.utm_source, ''), ' ', COALESCE(tr.utm_campaign, ''))),
              r'\\bob\\b|outbound|check up'
            )
            THEN 'outbound_existing_bookings'
          WHEN tr.trigger_type = 'appointment_booking' THEN 'inbound_bookings'
          WHEN tr.trigger_type = 'lead_magnet' THEN 'lead_magnets'
          ELSE 'other'
        END AS lane_id,
        COALESCE(gc.meetings_booked_cnt, 0) > 0 OR COALESCE(gc.has_calendly_booking, FALSE) AS had_meeting_booked,
        COALESCE(gc.meetings_showed_cnt, 0) > 0 AS had_meeting_showed,
        COALESCE(gc.opportunity_count, 0) > 0 AS had_opportunity,
        COALESCE(gc.has_fanbasis_payment, FALSE) AS had_payment,
        NULLIF(COALESCE(gc.rep_name, gc.setter_at_first_contact), '') IS NOT NULL AS has_owner,
        NULLIF(gc.phone, '') IS NULL AS missing_phone
      FROM trigger_rollup tr
      LEFT JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
        ON gc.golden_contact_key = tr.golden_contact_key
      WHERE tr.first_attempt.touch_ts IS NULL
    ),
    captured_activity AS (
      SELECT
        gc.golden_contact_key,
        COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) AS activity_ts,
        LOWER(COALESCE(c.direction_norm, '')) = 'outbound' AS is_outbound
      FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_outbound_calls\` c
      JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
        ON gc.location_id = c.location_id
       AND gc.ghl_contact_id = c.contact_id
      WHERE COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) IS NOT NULL

      UNION ALL

      SELECT
        gc.golden_contact_key,
        COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) AS activity_ts,
        LOWER(COALESCE(m.direction_norm, '')) = 'outbound'
          OR REGEXP_CONTAINS(LOWER(COALESCE(JSON_VALUE(m.payload_json, '$.lastMessageDirection'), '')), r'outbound') AS is_outbound
      FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_conversations\` m
      JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
        ON gc.location_id = m.location_id
       AND gc.ghl_contact_id = m.contact_id
      WHERE COALESCE(m.message_created_at, m.event_ts, m.updated_at_ts, m.ingested_at) IS NOT NULL
        AND REGEXP_CONTAINS(
          LOWER(COALESCE(m.message_type_norm, '')),
          r'sms|text|whatsapp|email|type_call|type_phone|phone|call'
        )
    ),
    event_activity AS (
      SELECT
        u.trigger_event_id,
        LOGICAL_OR(a.activity_ts IS NOT NULL) AS had_captured_call_or_message_ever,
        COALESCE(LOGICAL_OR(a.is_outbound), FALSE) AS had_captured_outbound_ever,
        COALESCE(LOGICAL_OR(a.activity_ts >= u.trigger_ts), FALSE) AS had_captured_call_or_message_after_trigger,
        COALESCE(LOGICAL_OR(a.is_outbound AND a.activity_ts >= u.trigger_ts), FALSE) AS had_captured_outbound_after_trigger
      FROM unmatched u
      LEFT JOIN captured_activity a
        ON a.golden_contact_key = u.golden_contact_key
      GROUP BY u.trigger_event_id
    ),
    event_truth AS (
      SELECT
        u.*,
        COALESCE(a.had_captured_call_or_message_ever, FALSE) AS had_captured_call_or_message_ever,
        COALESCE(a.had_captured_outbound_ever, FALSE) AS had_captured_outbound_ever,
        COALESCE(a.had_captured_call_or_message_after_trigger, FALSE) AS had_captured_call_or_message_after_trigger,
        COALESCE(a.had_captured_outbound_after_trigger, FALSE) AS had_captured_outbound_after_trigger,
        (u.had_meeting_showed OR u.had_payment OR u.has_owner) AS strong_work_or_progress_evidence,
        (u.had_meeting_booked OR u.had_meeting_showed OR u.had_opportunity OR u.had_payment OR u.has_owner) AS any_crm_progress_evidence
      FROM unmatched u
      LEFT JOIN event_activity a
        ON a.trigger_event_id = u.trigger_event_id
    ),
    rollups AS (
      SELECT
        1 AS sort_order,
        'all' AS lane_id,
        'All unmatched post-trigger' AS truth_slice,
        *
      FROM (
        SELECT
          COUNT(*) AS unmatched_events,
          COUNT(DISTINCT golden_contact_key) AS distinct_contacts,
          COUNTIF(had_captured_call_or_message_ever) AS had_captured_call_or_message_ever,
          COUNTIF(had_captured_outbound_ever) AS had_captured_outbound_ever,
          COUNTIF(had_captured_call_or_message_after_trigger) AS had_captured_call_or_message_after_trigger,
          COUNTIF(had_captured_outbound_after_trigger) AS had_captured_outbound_after_trigger,
          COUNTIF(had_meeting_booked) AS had_meeting_booked,
          COUNTIF(had_meeting_showed) AS had_meeting_showed,
          COUNTIF(had_opportunity) AS had_opportunity,
          COUNTIF(had_payment) AS had_payment,
          COUNTIF(has_owner) AS has_owner,
          COUNTIF(missing_phone) AS missing_phone,
          COUNTIF(strong_work_or_progress_evidence) AS strong_work_or_progress_evidence,
          COUNTIF(any_crm_progress_evidence) AS any_crm_progress_evidence,
          COUNTIF(NOT any_crm_progress_evidence) AS no_external_progress_evidence,
          SAFE_DIVIDE(COUNTIF(any_crm_progress_evidence), COUNT(*)) AS crm_progress_evidence_rate,
          MIN(FORMAT_TIMESTAMP('%Y-%m-%d', trigger_ts, 'America/New_York')) AS first_trigger_date,
          MAX(FORMAT_TIMESTAMP('%Y-%m-%d', trigger_ts, 'America/New_York')) AS last_trigger_date
        FROM event_truth
      )

      UNION ALL

      SELECT
        CASE lane_id
          WHEN 'inbound_bookings' THEN 2
          WHEN 'lead_magnets' THEN 3
          WHEN 'outbound_existing_bookings' THEN 4
          ELSE 5
        END AS sort_order,
        lane_id,
        CASE lane_id
          WHEN 'inbound_bookings' THEN 'Inbound bookings'
          WHEN 'lead_magnets' THEN 'Lead magnets'
          WHEN 'outbound_existing_bookings' THEN 'Outbound / existing'
          ELSE 'Other'
        END AS truth_slice,
        COUNT(*) AS unmatched_events,
        COUNT(DISTINCT golden_contact_key) AS distinct_contacts,
        COUNTIF(had_captured_call_or_message_ever) AS had_captured_call_or_message_ever,
        COUNTIF(had_captured_outbound_ever) AS had_captured_outbound_ever,
        COUNTIF(had_captured_call_or_message_after_trigger) AS had_captured_call_or_message_after_trigger,
        COUNTIF(had_captured_outbound_after_trigger) AS had_captured_outbound_after_trigger,
        COUNTIF(had_meeting_booked) AS had_meeting_booked,
        COUNTIF(had_meeting_showed) AS had_meeting_showed,
        COUNTIF(had_opportunity) AS had_opportunity,
        COUNTIF(had_payment) AS had_payment,
        COUNTIF(has_owner) AS has_owner,
        COUNTIF(missing_phone) AS missing_phone,
        COUNTIF(strong_work_or_progress_evidence) AS strong_work_or_progress_evidence,
        COUNTIF(any_crm_progress_evidence) AS any_crm_progress_evidence,
        COUNTIF(NOT any_crm_progress_evidence) AS no_external_progress_evidence,
        SAFE_DIVIDE(COUNTIF(any_crm_progress_evidence), COUNT(*)) AS crm_progress_evidence_rate,
        MIN(FORMAT_TIMESTAMP('%Y-%m-%d', trigger_ts, 'America/New_York')) AS first_trigger_date,
        MAX(FORMAT_TIMESTAMP('%Y-%m-%d', trigger_ts, 'America/New_York')) AS last_trigger_date
      FROM event_truth
      GROUP BY lane_id
    )
    SELECT *
    FROM rollups
    WHERE unmatched_events > 0
    ORDER BY sort_order
  `,
  speed_to_lead_first_work_by_rep: `
    ${qualityCte},
    summary AS (
      SELECT COUNTIF(first_attempt.touch_ts IS NOT NULL) AS total_worked
      FROM trigger_rollup
    ),
    worked AS (
      SELECT
        CASE
          WHEN first_attempt.is_automated_workflow_touch THEN 'Workflow automation'
          ELSE COALESCE(first_attempt.touch_owner_name, 'Unknown rep')
        END AS worked_by,
        CASE
          WHEN first_attempt.is_automated_workflow_touch THEN 'Automation'
          ELSE COALESCE(first_attempt.touch_owner_role, 'Unknown')
        END AS role,
        COALESCE(first_attempt.touch_owner_source, 'No rep supplied') AS identity_source,
        CASE
          WHEN first_attempt.channel_group = 'call' THEN 'Phone'
          WHEN first_attempt.channel_group = 'sms' THEN 'Text'
          WHEN first_attempt.channel_group = 'email' THEN 'Email'
          ELSE 'Other'
        END AS first_channel_label,
        first_successful_connection.touch_ts IS NOT NULL AS was_reached_by_phone,
        TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) / 60.0 AS minutes_to_first_attempt
      FROM trigger_rollup
      WHERE first_attempt.touch_ts IS NOT NULL
    )
    SELECT
      worked_by,
      role,
      identity_source,
      first_channel_label,
      COUNT(*) AS leads_worked,
      SAFE_DIVIDE(COUNT(*), ANY_VALUE(summary.total_worked)) AS share_of_worked_leads,
      COUNTIF(was_reached_by_phone) AS reached_by_phone,
      AVG(minutes_to_first_attempt) AS avg_minutes_to_first_attempt
    FROM worked
    CROSS JOIN summary
    GROUP BY worked_by, role, identity_source, first_channel_label
    ORDER BY leads_worked DESC, reached_by_phone DESC, worked_by
    LIMIT 20
  `,
  speed_to_lead_phone_reach_by_rep: `
    ${qualityCte},
    summary AS (
      SELECT
        COUNT(*) AS total_triggers,
        COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS total_reached
      FROM trigger_rollup
    ),
    reached AS (
      SELECT
        CASE
          WHEN first_successful_connection.is_automated_workflow_touch THEN 'Workflow automation'
          ELSE COALESCE(first_successful_connection.touch_owner_name, 'Unknown rep')
        END AS reached_by,
        CASE
          WHEN first_successful_connection.is_automated_workflow_touch THEN 'Automation'
          ELSE COALESCE(first_successful_connection.touch_owner_role, 'Unknown')
        END AS role,
        COALESCE(first_successful_connection.touch_owner_source, 'No rep supplied') AS identity_source,
        TIMESTAMP_DIFF(first_successful_connection.touch_ts, trigger_ts, SECOND) / 60.0 AS minutes_to_connection
      FROM trigger_rollup
      WHERE first_successful_connection.touch_ts IS NOT NULL
    )
    SELECT
      reached_by,
      role,
      identity_source,
      COUNT(*) AS leads_reached,
      SAFE_DIVIDE(COUNT(*), ANY_VALUE(summary.total_reached)) AS share_of_reached_leads,
      SAFE_DIVIDE(COUNT(*), ANY_VALUE(summary.total_triggers)) AS share_of_all_leads,
      AVG(minutes_to_connection) AS avg_minutes_to_connection
    FROM reached
    CROSS JOIN summary
    GROUP BY reached_by, role, identity_source
    ORDER BY leads_reached DESC, reached_by
    LIMIT 20
  `,
  speed_to_lead_attribution_confidence: `
    ${qualityCte}
    SELECT
      COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS reached_leads,
      COUNTIF(
        first_successful_connection.touch_ts IS NOT NULL
        AND first_successful_connection.touch_owner_source = 'GHL user'
      ) AS named_rep_reached,
      COUNTIF(
        first_successful_connection.touch_ts IS NOT NULL
        AND COALESCE(first_successful_connection.touch_owner_source, 'No rep supplied') != 'GHL user'
      ) AS needs_mapping,
      COUNTIF(
        first_successful_connection.touch_ts IS NOT NULL
        AND COALESCE(first_successful_connection.touch_owner_source, 'No rep supplied') = 'No rep supplied'
      ) AS no_rep_supplied,
      SAFE_DIVIDE(
        COUNTIF(
          first_successful_connection.touch_ts IS NOT NULL
          AND first_successful_connection.touch_owner_source = 'GHL user'
        ),
        NULLIF(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), 0)
      ) AS named_rep_rate,
      SAFE_DIVIDE(
        COUNTIF(
          first_successful_connection.touch_ts IS NOT NULL
          AND COALESCE(first_successful_connection.touch_owner_source, 'No rep supplied') != 'GHL user'
        ),
        NULLIF(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), 0)
      ) AS needs_mapping_rate
    FROM trigger_rollup
  `,
  speed_to_lead_not_worked_aging: `
    ${qualityCte},
    not_worked AS (
      SELECT
        CASE
          WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 1 THEN 'Under 1h'
          WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24 THEN '1-24h'
          ELSE 'Over 24h'
        END AS age_bucket,
        CASE
          WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 1 THEN 1
          WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) < 24 THEN 2
          ELSE 3
        END AS bucket_order,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) AS age_hours
      FROM trigger_rollup
      WHERE first_attempt.touch_ts IS NULL
    )
    SELECT
      age_bucket,
      bucket_order,
      COUNT(*) AS lead_events,
      SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) AS share_of_not_worked,
      MIN(age_hours) AS youngest_age_hours,
      MAX(age_hours) AS oldest_age_hours
    FROM not_worked
    GROUP BY age_bucket, bucket_order
    ORDER BY bucket_order
  `,
  speed_to_lead_reached_examples: `
    ${qualityCte}
    SELECT
      FORMAT_TIMESTAMP('%Y-%m-%d %I:%M %p ET', tr.first_successful_connection.touch_ts, 'America/New_York') AS reached_at_et,
      COALESCE(
        NULLIF(gc.full_name, ''),
        NULLIF(TRIM(CONCAT(COALESCE(gc.first_name, ''), ' ', COALESCE(gc.last_name, ''))), ''),
        'Unknown lead'
      ) AS lead_name,
      COALESCE(NULLIF(gc.email, ''), 'No email') AS lead_email,
      COALESCE(NULLIF(tr.trigger_source_label, ''), NULLIF(tr.utm_campaign, ''), 'Unknown') AS source_label,
      CASE
        WHEN tr.first_successful_connection.is_automated_workflow_touch THEN 'Workflow automation'
        ELSE COALESCE(tr.first_successful_connection.touch_owner_name, 'Unknown rep')
      END AS reached_by,
      COALESCE(tr.first_successful_connection.touch_owner_source, 'No rep supplied') AS identity_source,
      TIMESTAMP_DIFF(tr.first_successful_connection.touch_ts, tr.trigger_ts, SECOND) / 60.0 AS minutes_to_connect,
      tr.first_successful_connection.touch_status AS phone_status
    FROM trigger_rollup tr
    LEFT JOIN \`project-41542e21-470f-4589-96d.Marts.dim_golden_contact\` gc
      ON gc.golden_contact_key = tr.golden_contact_key
    WHERE tr.first_successful_connection.touch_ts IS NOT NULL
    ORDER BY tr.first_successful_connection.touch_ts DESC
    LIMIT 12
  `,
  speed_to_lead_first_attempt_outcomes: `
    ${qualityCte}
    SELECT
      COALESCE(first_attempt.channel_group, 'no_touch') AS first_attempt_channel,
      COALESCE(first_attempt.touch_status, 'no_touch') AS first_attempt_status,
      COALESCE(first_attempt.touch_outcome, 'no_touch') AS first_attempt_outcome,
      CASE
        WHEN first_attempt.touch_ts IS NULL THEN 'No follow-up yet'
        WHEN first_attempt.touch_outcome = 'successful_connection' THEN 'Reached by phone'
        WHEN first_attempt.touch_outcome = 'call_no_answer' THEN 'Called, no answer'
        WHEN first_attempt.touch_outcome IN ('call_failed', 'call_canceled', 'call_busy', 'call_undelivered', 'call_voicemail') THEN 'Call did not connect'
        WHEN first_attempt.channel_group = 'call' THEN 'Call attempt logged'
        WHEN first_attempt.channel_group = 'sms' THEN 'Text message logged'
        WHEN first_attempt.channel_group = 'email' THEN 'Email logged'
        ELSE 'Other follow-up logged'
      END AS outcome_label,
      CASE
        WHEN first_attempt.touch_ts IS NULL THEN 'No follow-up'
        WHEN first_attempt.channel_group = 'call' THEN 'Phone'
        WHEN first_attempt.channel_group = 'sms' THEN 'Text'
        WHEN first_attempt.channel_group = 'email' THEN 'Email'
        ELSE 'Other'
      END AS channel_label,
      COUNT(*) AS trigger_count,
      SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) AS share_of_triggers,
      COUNTIF(first_attempt.is_automated_workflow_touch) AS workflow_attempts,
      SAFE_DIVIDE(COUNTIF(first_attempt.is_automated_workflow_touch), COUNT(*)) AS workflow_attempt_rate
    FROM trigger_rollup
    GROUP BY first_attempt_channel, first_attempt_status, first_attempt_outcome, outcome_label, channel_label
    ORDER BY trigger_count DESC, first_attempt_channel, first_attempt_status
    LIMIT 16
  `,
  speed_to_lead_business_hours: `
    ${qualityCte}
    SELECT
      service_window,
      CASE
        WHEN service_window = 'business_hours' THEN 'Business hours'
        ELSE 'After hours / weekend'
      END AS service_window_label,
      COUNT(*) AS total_triggers,
      COUNTIF(first_attempt.touch_ts IS NOT NULL) AS first_attempts,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NOT NULL), COUNT(*)) AS first_attempt_rate,
      COUNTIF(first_attempt.touch_ts IS NOT NULL) AS worked_leads,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NOT NULL), COUNT(*)) AS worked_lead_rate,
      COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}) AS first_attempt_within_sla,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS first_attempt_within_sla_rate,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS sla_worked_rate,
      COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS successful_connections,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS successful_connection_rate,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS reached_lead_rate,
      COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL) AS meaningful_human_responses,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS meaningful_human_response_rate,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS human_follow_up_rate,
      COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}) AS meaningful_human_within_sla,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS meaningful_human_within_sla_rate,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= ${BOOKING_SLA_SECONDS}), COUNT(*)) AS sla_human_rate,
      COUNTIF(first_attempt.touch_ts IS NULL) AS no_attempt,
      COUNTIF(first_attempt.touch_ts IS NULL) AS unworked_leads,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NULL), COUNT(*)) AS unworked_lead_rate
    FROM trigger_rollup
    GROUP BY service_window
    ORDER BY CASE service_window WHEN 'business_hours' THEN 1 ELSE 2 END
  `,
  speed_to_lead_typeform_coverage: `
    WITH typeform_responses AS (
      SELECT
        r.response_id,
        COALESCE(r.submitted_at, r.event_ts, r.ingested_at) AS response_ts,
        COALESCE(
          NULLIF(r.form_id, ''),
          REGEXP_EXTRACT(
            COALESCE(r.referer, JSON_VALUE(r.payload_json, '$.metadata.referer')),
            r'(?i)form[.]typeform[.]com/to/([^?/#]+)'
          ),
          REGEXP_EXTRACT(
            COALESCE(r.referer, JSON_VALUE(r.payload_json, '$.metadata.referer')),
            r'(?i)^https?://([^/?#]+)'
          ),
          'unknown_typeform'
        ) AS typeform_source_key,
        b.location_id,
        b.contact_id
      FROM \`project-41542e21-470f-4589-96d.Core.dim_typeform_responses\` r
      LEFT JOIN \`project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts\` b
        ON b.response_id = r.response_id
      WHERE COALESCE(r.submitted_at, r.event_ts, r.ingested_at) IS NOT NULL
        ${andTimeRange(timeRange, "COALESCE(r.submitted_at, r.event_ts, r.ingested_at)")}
    ),
    bookings_after AS (
      SELECT
        tr.response_id,
        COUNT(DISTINCT i.invitee_id) AS bookings_after_typeform
      FROM typeform_responses tr
      JOIN \`project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts\` cb
        ON cb.location_id = tr.location_id
       AND cb.contact_id = tr.contact_id
      JOIN \`project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees\` i
        ON i.invitee_id = cb.invitee_id
      WHERE tr.contact_id IS NOT NULL
        AND COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) >= tr.response_ts
      GROUP BY tr.response_id
    )
    SELECT
      COUNT(*) AS responses,
      COUNTIF(contact_id IS NOT NULL) AS matched_contacts,
      SAFE_DIVIDE(COUNTIF(contact_id IS NOT NULL), COUNT(*)) AS contact_match_rate,
      COUNTIF(contact_id IS NULL) AS unmatched_contacts,
      COUNTIF(COALESCE(ba.bookings_after_typeform, 0) > 0) AS booked_after_typeform,
      COUNTIF(contact_id IS NOT NULL AND COALESCE(ba.bookings_after_typeform, 0) = 0) AS outbound_opportunities,
      SAFE_DIVIDE(
        COUNTIF(contact_id IS NOT NULL AND COALESCE(ba.bookings_after_typeform, 0) = 0),
        NULLIF(COUNTIF(contact_id IS NOT NULL), 0)
      ) AS outbound_opportunity_rate,
      COUNT(DISTINCT typeform_source_key) AS visible_typeform_sources
    FROM typeform_responses tr
    LEFT JOIN bookings_after ba
      ON ba.response_id = tr.response_id
  `,
  speed_to_lead_typeform_outbound_opportunities: `
    WITH typeform_responses AS (
      SELECT
        r.response_id,
        COALESCE(r.submitted_at, r.event_ts, r.ingested_at) AS response_ts,
        COALESCE(
          NULLIF(r.form_id, ''),
          REGEXP_EXTRACT(
            COALESCE(r.referer, JSON_VALUE(r.payload_json, '$.metadata.referer')),
            r'(?i)form[.]typeform[.]com/to/([^?/#]+)'
          ),
          REGEXP_EXTRACT(
            COALESCE(r.referer, JSON_VALUE(r.payload_json, '$.metadata.referer')),
            r'(?i)^https?://([^/?#]+)'
          ),
          'unknown_typeform'
        ) AS typeform_source,
        r.respondent_name,
        r.respondent_email,
        r.respondent_core_struggle,
        b.location_id,
        b.contact_id,
        b.matched_contact_name,
        b.matched_contact_email,
        b.matched_contact_phone,
        b.match_method
      FROM \`project-41542e21-470f-4589-96d.Core.dim_typeform_responses\` r
      LEFT JOIN \`project-41542e21-470f-4589-96d.Core.bridge_typeform_response_contacts\` b
        ON b.response_id = r.response_id
      WHERE COALESCE(r.submitted_at, r.event_ts, r.ingested_at) IS NOT NULL
        ${andTimeRange(timeRange, "COALESCE(r.submitted_at, r.event_ts, r.ingested_at)")}
    ),
    bookings_after AS (
      SELECT
        tr.response_id,
        MIN(COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time)) AS first_booking_after_ts,
        COUNT(DISTINCT i.invitee_id) AS bookings_after_typeform
      FROM typeform_responses tr
      JOIN \`project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts\` cb
        ON cb.location_id = tr.location_id
       AND cb.contact_id = tr.contact_id
      JOIN \`project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees\` i
        ON i.invitee_id = cb.invitee_id
      WHERE tr.contact_id IS NOT NULL
        AND COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) >= tr.response_ts
      GROUP BY tr.response_id
    )
    SELECT
      FORMAT_TIMESTAMP('%Y-%m-%d %I:%M %p ET', tr.response_ts, 'America/New_York') AS submitted_at_et,
      COALESCE(
        NULLIF(tr.matched_contact_name, ''),
        NULLIF(tr.respondent_name, ''),
        'Unknown lead'
      ) AS lead_name,
      COALESCE(NULLIF(tr.matched_contact_email, ''), NULLIF(tr.respondent_email, ''), 'No email') AS lead_email,
      COALESCE(NULLIF(tr.matched_contact_phone, ''), 'No phone') AS lead_phone,
      tr.typeform_source,
      COALESCE(NULLIF(tr.respondent_core_struggle, ''), 'N/A') AS core_struggle,
      COALESCE(NULLIF(tr.match_method, ''), 'unmatched') AS match_method,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), tr.response_ts, HOUR) AS hours_since_submit
    FROM typeform_responses tr
    LEFT JOIN bookings_after ba
      ON ba.response_id = tr.response_id
    WHERE tr.contact_id IS NOT NULL
      AND COALESCE(ba.bookings_after_typeform, 0) = 0
    ORDER BY tr.response_ts DESC
    LIMIT 50
  `,
  speed_to_lead_unmatched_calendly_summary: `
    WITH invitees AS (
      SELECT
        i.invitee_id,
        COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) AS booking_ts,
        b.contact_id,
        b.match_confidence
      FROM \`project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees\` i
      LEFT JOIN \`project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts\` b
        ON b.invitee_id = i.invitee_id
      WHERE COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) IS NOT NULL
        ${andTimeRange(timeRange, "COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time)")}
    )
    SELECT
      COUNT(*) AS invitees,
      COUNTIF(contact_id IS NOT NULL) AS matched_invitees,
      COUNTIF(contact_id IS NULL) AS unmatched_invitees,
      SAFE_DIVIDE(COUNTIF(contact_id IS NULL), COUNT(*)) AS unmatched_rate,
      COUNTIF(match_confidence = 'high') AS high_confidence_matches,
      COUNTIF(match_confidence = 'medium') AS medium_confidence_matches,
      COUNTIF(match_confidence = 'low') AS low_confidence_matches
    FROM invitees
  `,
  speed_to_lead_unmatched_calendly_invitees: `
    SELECT
      FORMAT_TIMESTAMP(
        '%Y-%m-%d %I:%M %p ET',
        COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time),
        'America/New_York'
      ) AS booked_at_et,
      COALESCE(NULLIF(i.invitee_name, ''), 'Unknown invitee') AS invitee_name,
      COALESCE(NULLIF(i.invitee_email, ''), 'No email') AS invitee_email,
      COALESCE(NULLIF(i.event_name, ''), CONCAT('event_type:', COALESCE(i.event_type_uri, 'unknown'))) AS event_name,
      COALESCE(NULLIF(i.utm_source, ''), 'N/A') AS utm_source,
      COALESCE(NULLIF(i.utm_campaign, ''), 'N/A') AS utm_campaign,
      COALESCE(NULLIF(b.match_method, ''), 'no_contact_match') AS match_method,
      COALESCE(NULLIF(b.match_confidence, ''), 'none') AS match_confidence
    FROM \`project-41542e21-470f-4589-96d.Core.fct_calendly_event_invitees\` i
    LEFT JOIN \`project-41542e21-470f-4589-96d.Core.bridge_calendly_invitee_contacts\` b
      ON b.invitee_id = i.invitee_id
    WHERE COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) IS NOT NULL
      AND b.contact_id IS NULL
      ${andTimeRange(timeRange, "COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time)")}
    ORDER BY COALESCE(i.invitee_created_at, i.event_ts, i.ingested_at, i.scheduled_start_time) DESC
    LIMIT 50
  `,
  speed_to_lead_ghl_message_coverage: `
    WITH raw_message_rows AS (
      SELECT
        entity_type,
        COALESCE(event_ts, updated_at_ts, ingested_at) AS row_ts
      FROM \`project-41542e21-470f-4589-96d.Raw.ghl_objects_raw\`
      WHERE entity_type IN ('conversations', 'conversation_messages', 'message', 'messages')
        AND COALESCE(event_ts, updated_at_ts, ingested_at) IS NOT NULL
        ${andTimeRange(timeRange, "COALESCE(event_ts, updated_at_ts, ingested_at)")}
    ),
    core_rows AS (
      SELECT
        direction_norm,
        message_type_norm,
        COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at) AS row_ts
      FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_conversations\`
      WHERE COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at) IS NOT NULL
        ${andTimeRange(timeRange, "COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at)")}
    )
    SELECT
      COUNTIF(entity_type = 'conversations') AS raw_conversation_rows,
      COUNTIF(entity_type IN ('conversation_messages', 'message', 'messages')) AS raw_message_rows,
      (SELECT COUNT(*) FROM core_rows) AS core_conversation_rows,
      (SELECT COUNTIF(direction_norm = 'outbound') FROM core_rows) AS core_outbound_rows,
      (
        SELECT COUNTIF(
          direction_norm = 'outbound'
          AND REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'sms|text|whatsapp')
        )
        FROM core_rows
      ) AS core_outbound_sms_rows,
      (
        SELECT COUNTIF(
          direction_norm = 'outbound'
          AND REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'email')
        )
        FROM core_rows
      ) AS core_outbound_email_rows,
      (
        SELECT COUNTIF(
          direction_norm = 'outbound'
          AND REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'call|phone')
        )
        FROM core_rows
      ) AS core_outbound_call_like_rows
    FROM raw_message_rows
  `,
  speed_to_lead_ghl_outbound_message_breakdown: `
    SELECT
      CASE
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'sms|text|whatsapp') THEN 'SMS/Text'
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'email') THEN 'Email'
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(message_type_norm, '')), r'call|phone') THEN 'Call-like'
        ELSE 'Other'
      END AS channel,
      COALESCE(NULLIF(direction_norm, ''), 'unknown') AS direction,
      COALESCE(NULLIF(message_type_norm, ''), 'unknown') AS message_type,
      COUNT(*) AS row_count
    FROM \`project-41542e21-470f-4589-96d.Core.fct_ghl_conversations\`
    WHERE COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at) IS NOT NULL
      ${andTimeRange(timeRange, "COALESCE(message_created_at, event_ts, updated_at_ts, ingested_at)")}
    GROUP BY channel, direction, message_type
    ORDER BY row_count DESC, channel, direction, message_type
    LIMIT 16
  `,
  } satisfies Record<string, string>;
}

export async function getSpeedToLeadData(options: GetSpeedToLeadDataOptions = {}): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeSpeedToLeadTimeRange(options.timeRange);
  const filters = buildDashboardFilters(timeRange);
  const speedToLeadQueries = buildSpeedToLeadQueries(timeRange);

  try {
    const [
      overall,
      overallPrior,
      daily,
      byRep,
      triggerSummary,
      responseBuckets,
      sourcePerformance,
      laneSummary,
      noTouchExamples,
      routingReadiness,
      criticalExceptions,
      qualitySummary,
      followUpCounts,
      unmatchedTruthAudit,
      firstWorkByRep,
      phoneReachByRep,
      attributionConfidence,
      notWorkedAging,
      reachedExamples,
      firstAttemptOutcomes,
      businessHours,
      typeformCoverage,
      typeformOutboundOpportunities,
      unmatchedCalendlySummary,
      unmatchedCalendlyInvitees,
      ghlMessageCoverage,
      ghlOutboundMessageBreakdown,
    ] = await Promise.all([
      runBigQuery(speedToLeadQueries.speed_to_lead_overall),
      runBigQuery(speedToLeadQueries.speed_to_lead_overall_prior),
      runBigQuery(speedToLeadQueries.speed_to_lead_daily),
      runBigQuery(speedToLeadQueries.speed_to_lead_by_rep),
      runBigQuery(speedToLeadQueries.speed_to_lead_trigger_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_response_buckets),
      runBigQuery(speedToLeadQueries.speed_to_lead_source_performance),
      runBigQuery(speedToLeadQueries.speed_to_lead_lane_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_no_touch_examples),
      runBigQuery(speedToLeadQueries.speed_to_lead_routing_readiness),
      runBigQuery(speedToLeadQueries.speed_to_lead_critical_exceptions),
      runBigQuery(speedToLeadQueries.speed_to_lead_quality_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_follow_up_counts),
      runBigQuery(speedToLeadQueries.speed_to_lead_unmatched_truth_audit),
      runBigQuery(speedToLeadQueries.speed_to_lead_first_work_by_rep),
      runBigQuery(speedToLeadQueries.speed_to_lead_phone_reach_by_rep),
      runBigQuery(speedToLeadQueries.speed_to_lead_attribution_confidence),
      runBigQuery(speedToLeadQueries.speed_to_lead_not_worked_aging),
      runBigQuery(speedToLeadQueries.speed_to_lead_reached_examples),
      runBigQuery(speedToLeadQueries.speed_to_lead_first_attempt_outcomes),
      runBigQuery(speedToLeadQueries.speed_to_lead_business_hours),
      runBigQuery(speedToLeadQueries.speed_to_lead_typeform_coverage),
      runBigQuery(speedToLeadQueries.speed_to_lead_typeform_outbound_opportunities),
      runBigQuery(speedToLeadQueries.speed_to_lead_unmatched_calendly_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_unmatched_calendly_invitees),
      runBigQuery(speedToLeadQueries.speed_to_lead_ghl_message_coverage),
      runBigQuery(speedToLeadQueries.speed_to_lead_ghl_outbound_message_breakdown),
    ]);

    return {
      rows: {
        speed_to_lead_overall: overall,
        speed_to_lead_overall_prior: overallPrior,
        speed_to_lead_daily: daily,
        speed_to_lead_by_rep: byRep,
        speed_to_lead_trigger_summary: triggerSummary,
        speed_to_lead_response_buckets: responseBuckets,
        speed_to_lead_source_performance: sourcePerformance,
        speed_to_lead_lane_summary: laneSummary,
        speed_to_lead_no_touch_examples: noTouchExamples,
        speed_to_lead_routing_readiness: routingReadiness,
        speed_to_lead_critical_exceptions: criticalExceptions,
        speed_to_lead_quality_summary: qualitySummary,
        speed_to_lead_follow_up_counts: followUpCounts,
        speed_to_lead_unmatched_truth_audit: unmatchedTruthAudit,
        speed_to_lead_first_work_by_rep: firstWorkByRep,
        speed_to_lead_phone_reach_by_rep: phoneReachByRep,
        speed_to_lead_attribution_confidence: attributionConfidence,
        speed_to_lead_not_worked_aging: notWorkedAging,
        speed_to_lead_reached_examples: reachedExamples,
        speed_to_lead_first_attempt_outcomes: firstAttemptOutcomes,
        speed_to_lead_business_hours: businessHours,
        speed_to_lead_typeform_coverage: typeformCoverage,
        speed_to_lead_typeform_outbound_opportunities: typeformOutboundOpportunities,
        speed_to_lead_unmatched_calendly_summary: unmatchedCalendlySummary,
        speed_to_lead_unmatched_calendly_invitees: unmatchedCalendlyInvitees,
        speed_to_lead_ghl_message_coverage: ghlMessageCoverage,
        speed_to_lead_ghl_outbound_message_breakdown: ghlOutboundMessageBreakdown,
      },
      freshness: buildFreshness(overall, byRep),
      filters,
      generatedAt,
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Live data unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
    };
  }
}

function buildFreshness(overall: DashboardRow[], byRep: DashboardRow[]): DashboardFreshness {
  const refreshedAt =
    stringValue(overall[0]?.refreshed_at) ?? stringValue(byRep[0]?.mart_refreshed_at);

  if (!refreshedAt) {
    return {
      status: "stale",
      label: "No refresh timestamp",
      detail: "The live query returned rows without a mart refresh timestamp.",
    };
  }

  const ageHours = (Date.now() - new Date(refreshedAt).getTime()) / 36e5;
  const status = ageHours <= 24 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live BigQuery data" : "Data needs review",
    detail: `Marts refreshed ${formatRelativeAge(ageHours)} ago.`,
    refreshedAt,
  };
}

function stringValue(value: DashboardRow[keyof DashboardRow] | undefined) {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function formatRelativeAge(ageHours: number) {
  if (!Number.isFinite(ageHours) || ageHours < 0) {
    return "just now";
  }

  if (ageHours < 1) {
    return `${Math.max(1, Math.round(ageHours * 60))}m`;
  }

  if (ageHours < 48) {
    return `${Math.round(ageHours)}h`;
  }

  return `${Math.round(ageHours / 24)}d`;
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  return "Unknown BigQuery error";
}
