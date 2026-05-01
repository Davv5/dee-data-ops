import { queryContracts, type QueryName } from "@/lib/bigquery/named-queries";
import { runBigQuery } from "@/lib/bigquery/client";
import type { DashboardData, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

const tableRef = (queryName: QueryName) => `\`${queryContracts[queryName].table}\``;

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
    ] = await Promise.all([
      runBigQuery(speedToLeadQueries.speed_to_lead_overall),
      runBigQuery(speedToLeadQueries.speed_to_lead_daily),
      runBigQuery(speedToLeadQueries.speed_to_lead_by_rep),
      runBigQuery(speedToLeadQueries.speed_to_lead_trigger_summary),
      runBigQuery(speedToLeadQueries.speed_to_lead_response_buckets),
      runBigQuery(speedToLeadQueries.speed_to_lead_source_performance),
      runBigQuery(speedToLeadQueries.speed_to_lead_no_touch_examples),
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
