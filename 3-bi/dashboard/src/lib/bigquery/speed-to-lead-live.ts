import { queryContracts, type QueryName } from "@/lib/bigquery/named-queries";
import { runBigQuery } from "@/lib/bigquery/client";
import type { DashboardData, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

const tableRef = (queryName: QueryName) => `\`${queryContracts[queryName].table}\``;

const speedToLeadQualityCte = `
  WITH trigger_events AS (
    SELECT
      trigger_event_id,
      golden_contact_key,
      trigger_ts,
      trigger_type
    FROM ${tableRef("freshness")}
    WHERE trigger_ts IS NOT NULL
  ),
  outbound_touches AS (
    SELECT
      gc.golden_contact_key,
      COALESCE(c.call_started_at, c.event_ts, c.updated_at_ts, c.ingested_at) AS touch_ts,
      'call' AS channel_group,
      LOWER(COALESCE(NULLIF(TRIM(c.call_status), ''), 'unknown')) AS touch_status,
      CONCAT('call|', COALESCE(c.call_log_id, CAST(FARM_FINGERPRINT(TO_JSON_STRING(c.payload_json)) AS STRING))) AS touch_id,
      LOWER(NULLIF(TRIM(JSON_VALUE(c.payload_json, '$.source')), '')) AS touch_source_raw
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
      LOWER(NULLIF(TRIM(JSON_VALUE(m.payload_json, '$.source')), '')) AS touch_source_raw
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
      ANY_VALUE(trigger_type) AS trigger_type,
      ANY_VALUE(service_window) AS service_window,
      ANY_VALUE(trigger_ts) AS trigger_ts,
      ARRAY_AGG(
        IF(
          touch_ts IS NULL,
          NULL,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts)
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_attempt,
      ARRAY_AGG(
        IF(
          is_successful_connection,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts),
          NULL
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_successful_connection,
      ARRAY_AGG(
        IF(
          is_meaningful_human_response,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts),
          NULL
        )
        IGNORE NULLS
        ORDER BY touch_ts ASC, channel_group ASC, touch_id ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS first_meaningful_human_response,
      ARRAY_AGG(
        IF(
          is_automated_workflow_touch,
          STRUCT(channel_group, touch_status, touch_outcome, is_automated_workflow_touch, touch_ts),
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

const speedToLeadQueries = {
  speed_to_lead_overall: `
    SELECT
      FORMAT_TIMESTAMP('%FT%TZ', refreshed_at) AS refreshed_at,
      total_bookings_matched_to_contact,
      bookings_with_outbound_call,
      bookings_without_outbound_call,
      avg_speed_to_lead_minutes,
      median_speed_to_lead_minutes,
      p90_speed_to_lead_minutes,
      SAFE_DIVIDE(pct_within_5m, 100) AS pct_within_5m,
      SAFE_DIVIDE(pct_within_15m, 100) AS pct_within_15m,
      SAFE_DIVIDE(pct_within_60m, 100) AS pct_within_60m,
      total_triggers_all,
      triggers_with_outbound_touch,
      SAFE_DIVIDE(pct_triggers_with_outbound_touch, 100) AS pct_triggers_with_outbound_touch,
      total_lead_magnet_triggers,
      lead_magnet_triggers_with_outbound_touch,
      SAFE_DIVIDE(pct_lead_magnet_triggers_with_outbound_touch, 100) AS pct_lead_magnet_triggers_with_outbound_touch
    FROM ${tableRef("speed_to_lead_overall")}
    LIMIT 1
  `,
  speed_to_lead_daily: `
    SELECT
      FORMAT_DATE('%Y-%m-%d', booking_date) AS report_date,
      total_bookings_matched_to_contact,
      bookings_with_outbound_call,
      bookings_without_outbound_call,
      avg_speed_to_lead_minutes,
      median_speed_to_lead_minutes,
      p90_speed_to_lead_minutes,
      SAFE_DIVIDE(sla_within_5m, total_bookings_matched_to_contact) AS pct_within_5m,
      SAFE_DIVIDE(sla_within_15m, total_bookings_matched_to_contact) AS pct_within_15m,
      SAFE_DIVIDE(sla_within_60m, total_bookings_matched_to_contact) AS pct_within_60m,
      total_triggers_all,
      triggers_with_outbound_touch,
      SAFE_DIVIDE(pct_triggers_with_outbound_touch, 100) AS pct_triggers_with_outbound_touch
    FROM ${tableRef("speed_to_lead_daily")}
    WHERE booking_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ORDER BY booking_date
  `,
  speed_to_lead_by_rep: `
    WITH latest_week AS (
      SELECT MAX(report_week) AS report_week
      FROM ${tableRef("speed_to_lead_by_rep")}
    )
    SELECT
      FORMAT_DATE('%Y-%m-%d', report_week) AS report_week,
      COALESCE(NULLIF(rep_name, ''), 'Unassigned') AS rep_name,
      COALESCE(NULLIF(rep_role, ''), 'unknown') AS rep_role,
      bookings,
      total_dials,
      avg_speed_to_lead_minutes,
      speed_to_lead_pct_within_sla AS pct_within_sla,
      FORMAT_TIMESTAMP('%FT%TZ', mart_refreshed_at) AS mart_refreshed_at
    FROM ${tableRef("speed_to_lead_by_rep")}
    WHERE report_week = (SELECT report_week FROM latest_week)
    ORDER BY IFNULL(speed_to_lead_pct_within_sla, -1) DESC, bookings DESC, rep_name
    LIMIT 12
  `,
  speed_to_lead_trigger_summary: `
    SELECT
      trigger_type,
      COUNT(*) AS total_triggers,
      COUNTIF(first_touch_ts IS NOT NULL) AS touched,
      SAFE_DIVIDE(COUNTIF(first_touch_ts IS NOT NULL), COUNT(*)) AS touch_rate,
      COUNTIF(is_within_sla) AS within_5m,
      SAFE_DIVIDE(COUNTIF(is_within_sla), COUNT(*)) AS within_5m_rate,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(50)] AS median_minutes,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(90)] AS p90_minutes
    FROM ${tableRef("freshness")}
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
      COUNTIF(is_within_sla) AS within_5m,
      SAFE_DIVIDE(COUNTIF(is_within_sla), COUNT(*)) AS within_5m_rate,
      APPROX_QUANTILES(speed_to_lead_minutes, 100)[OFFSET(50)] AS median_minutes
    FROM ${tableRef("freshness")}
    WHERE trigger_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY source_label, trigger_type
    HAVING total_triggers >= 5
    ORDER BY total_triggers DESC
    LIMIT 12
  `,
  speed_to_lead_no_touch_examples: `
    SELECT
      FORMAT_TIMESTAMP('%FT%TZ', trigger_ts) AS trigger_ts,
      FORMAT_DATE('%Y-%m-%d', trigger_date) AS trigger_date,
      trigger_type,
      COALESCE(NULLIF(trigger_source_label, ''), 'Unknown') AS source_label,
      COALESCE(NULLIF(utm_source, ''), 'N/A') AS utm_source,
      COALESCE(NULLIF(utm_campaign, ''), 'N/A') AS utm_campaign,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trigger_ts, HOUR) AS age_hours
    FROM ${tableRef("freshness")}
    WHERE first_touch_ts IS NULL
    ORDER BY trigger_ts DESC
    LIMIT 12
  `,
  speed_to_lead_quality_summary: `
    ${speedToLeadQualityCte}
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
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NULL), COUNT(*)) AS unworked_lead_rate
    FROM trigger_rollup
  `,
  speed_to_lead_first_attempt_outcomes: `
    ${speedToLeadQualityCte}
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
    ${speedToLeadQualityCte}
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
      COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= 300) AS first_attempt_within_5m,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= 300), COUNT(*)) AS first_attempt_within_5m_rate,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_attempt.touch_ts, trigger_ts, SECOND) <= 300), COUNT(*)) AS five_minute_worked_rate,
      COUNTIF(first_successful_connection.touch_ts IS NOT NULL) AS successful_connections,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS successful_connection_rate,
      SAFE_DIVIDE(COUNTIF(first_successful_connection.touch_ts IS NOT NULL), COUNT(*)) AS reached_lead_rate,
      COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL) AS meaningful_human_responses,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS meaningful_human_response_rate,
      SAFE_DIVIDE(COUNTIF(first_meaningful_human_response.touch_ts IS NOT NULL), COUNT(*)) AS human_follow_up_rate,
      COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= 300) AS meaningful_human_within_5m,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= 300), COUNT(*)) AS meaningful_human_within_5m_rate,
      SAFE_DIVIDE(COUNTIF(TIMESTAMP_DIFF(first_meaningful_human_response.touch_ts, trigger_ts, SECOND) <= 300), COUNT(*)) AS five_minute_human_rate,
      COUNTIF(first_attempt.touch_ts IS NULL) AS no_attempt,
      COUNTIF(first_attempt.touch_ts IS NULL) AS unworked_leads,
      SAFE_DIVIDE(COUNTIF(first_attempt.touch_ts IS NULL), COUNT(*)) AS unworked_lead_rate
    FROM trigger_rollup
    GROUP BY service_window
    ORDER BY CASE service_window WHEN 'business_hours' THEN 1 ELSE 2 END
  `,
} satisfies Record<string, string>;

export async function getSpeedToLeadData(): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();

  try {
    const [
      overall,
      daily,
      byRep,
      triggerSummary,
      responseBuckets,
      sourcePerformance,
      noTouchExamples,
      qualitySummary,
      firstAttemptOutcomes,
      businessHours,
    ] = await Promise.all([
      runBigQuery(speedToLeadQueries.speed_to_lead_overall),
      runBigQuery(speedToLeadQueries.speed_to_lead_daily),
      runBigQuery(speedToLeadQueries.speed_to_lead_by_rep),
      runBigQuery(speedToLeadQueries.speed_to_lead_trigger_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_response_buckets),
      runBigQuery(speedToLeadQueries.speed_to_lead_source_performance),
      runBigQuery(speedToLeadQueries.speed_to_lead_no_touch_examples),
      runBigQuery(speedToLeadQueries.speed_to_lead_quality_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_first_attempt_outcomes),
      runBigQuery(speedToLeadQueries.speed_to_lead_business_hours),
    ]);

    return {
      rows: {
        speed_to_lead_overall: overall,
        speed_to_lead_daily: daily,
        speed_to_lead_by_rep: byRep,
        speed_to_lead_trigger_summary: triggerSummary,
        speed_to_lead_response_buckets: responseBuckets,
        speed_to_lead_source_performance: sourcePerformance,
        speed_to_lead_no_touch_examples: noTouchExamples,
        speed_to_lead_quality_summary: qualitySummary,
        speed_to_lead_first_attempt_outcomes: firstAttemptOutcomes,
        speed_to_lead_business_hours: businessHours,
      },
      freshness: buildFreshness(overall, byRep),
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
