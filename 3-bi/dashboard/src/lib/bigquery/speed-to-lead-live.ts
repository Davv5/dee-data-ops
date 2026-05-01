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
} satisfies Record<"speed_to_lead_overall" | "speed_to_lead_daily" | "speed_to_lead_by_rep", string>;

export async function getSpeedToLeadData(): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();

  try {
    const [overall, daily, byRep] = await Promise.all([
      runBigQuery(speedToLeadQueries.speed_to_lead_overall),
      runBigQuery(speedToLeadQueries.speed_to_lead_daily),
      runBigQuery(speedToLeadQueries.speed_to_lead_by_rep),
    ]);

    return {
      rows: {
        speed_to_lead_overall: overall,
        speed_to_lead_daily: daily,
        speed_to_lead_by_rep: byRep,
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
