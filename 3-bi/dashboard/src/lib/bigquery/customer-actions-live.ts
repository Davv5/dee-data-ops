import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

type GetCustomerActionsOptions = {
  area?: string | null;
  includeClosed?: string | null;
  limit?: string | null;
};

const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const ACTION_AREAS = new Set(["all", "revenue", "retention", "contract_terms"]);

function customerActionsDataset() {
  const dataset =
    process.env.BIGQUERY_CUSTOMER_ACTIONS_DATASET ??
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for customer actions: ${dataset}`);
  }

  return dataset;
}

function tableRef() {
  return `\`${deeConfig.bigQuery.projectId}.${customerActionsDataset()}.customer_action_queue\``;
}

function normalizeArea(value: string | null | undefined) {
  const normalized = value?.toLowerCase() ?? "all";
  return ACTION_AREAS.has(normalized) ? normalized : "all";
}

function normalizeIncludeClosed(value: string | null | undefined) {
  return value === "true" || value === "1";
}

function normalizeLimit(value: string | null | undefined) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 50;
  return Math.min(Math.max(Math.trunc(numeric), 1), 100);
}

function areaPredicate(area: string) {
  if (area === "all") return "";
  return "AND action_area = @area";
}

function openPredicate(includeClosed: boolean) {
  return includeClosed ? "" : "AND is_action_open";
}

function buildDashboardFilters(area: string, includeClosed: boolean, limit: number): DashboardFilters {
  return {
    timeRange: "all",
    timeRangeLabel: includeClosed ? "Open + Closed" : "Open",
    timeRangeDescription: includeClosed ? "All customer actions including reviewed rows." : "Open customer actions only.",
    timeRangeOptions: [],
    worklist: area,
    worklistLabel: area === "all" ? "All Areas" : area.replace(/_/g, " "),
    worklistDescription: `Top ${limit} customer actions for ${area === "all" ? "all areas" : area}.`,
    worklistOptions: [
      { value: "all", label: "All", description: "All customer action areas." },
      { value: "revenue", label: "Revenue", description: "Revenue cleanup, attribution, and product actions." },
      { value: "retention", label: "Retention", description: "Recovery, renewal, upsell, and manual collection actions." },
      { value: "contract_terms", label: "Terms", description: "Contract terms review actions." },
    ],
  };
}

function buildQueries(area: string, includeClosed: boolean, limit: number) {
  const actionsTable = tableRef();
  const areaWhere = areaPredicate(area);
  const openWhere = openPredicate(includeClosed);

  return {
    customer_action_summary: `
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at)) AS refreshed_at,
        COUNT(*) AS actions,
        COUNTIF(is_action_open) AS open_actions,
        COUNTIF(NOT is_action_open) AS closed_actions,
        COUNT(DISTINCT contact_sk) AS customers,
        COUNT(DISTINCT IF(is_action_open, contact_sk, NULL)) AS open_customers,
        SUM(IF(is_action_open, money_at_stake, 0)) AS open_money_at_stake,
        COUNTIF(is_action_open AND priority_label = 'high') AS high_priority_open_actions,
        COUNTIF(is_action_open AND recommended_channel IN ('call_text', 'email')) AS contactable_open_actions
      FROM ${actionsTable}
      WHERE TRUE
        ${areaWhere}
    `,
    customer_action_by_area: `
      SELECT
        action_area,
        priority_label,
        MIN(priority_rank) AS priority_rank,
        COUNT(*) AS actions,
        COUNTIF(is_action_open) AS open_actions,
        COUNT(DISTINCT IF(is_action_open, contact_sk, NULL)) AS open_customers,
        SUM(IF(is_action_open, money_at_stake, 0)) AS open_money_at_stake
      FROM ${actionsTable}
      WHERE TRUE
        ${areaWhere}
      GROUP BY action_area, priority_label
      ORDER BY action_area, priority_rank
    `,
    customer_action_by_bucket: `
      SELECT
        action_area,
        action_bucket,
        action_label,
        MIN(priority_rank) AS priority_rank,
        COUNT(*) AS actions,
        COUNTIF(is_action_open) AS open_actions,
        COUNT(DISTINCT IF(is_action_open, contact_sk, NULL)) AS open_customers,
        SUM(IF(is_action_open, money_at_stake, 0)) AS open_money_at_stake
      FROM ${actionsTable}
      WHERE TRUE
        ${areaWhere}
      GROUP BY action_area, action_bucket, action_label
      ORDER BY priority_rank, open_money_at_stake DESC
    `,
    customer_action_queue: `
      SELECT
        customer_action_id,
        contact_sk,
        contact_id,
        customer_display_name,
        email_norm,
        phone,
        has_phone,
        has_email,
        action_area,
        queue_name,
        action_bucket,
        action_label,
        action_reason,
        priority_rank,
        priority_label,
        recommended_channel,
        recommended_channel_label,
        source_table,
        FORMAT_TIMESTAMP('%b %e, %Y', source_event_at, 'America/New_York') AS source_event_label,
        FORMAT_TIMESTAMP('%FT%TZ', source_event_at) AS source_event_at,
        money_at_stake,
        top_product_by_net_revenue,
        top_product_family,
        latest_prior_lead_magnet_name,
        revenue_credit_name,
        revenue_credit_source,
        revenue_credit_confidence,
        credited_setter_name,
        current_owner_name,
        current_owner_source,
        review_status,
        review_note,
        reviewed_by,
        FORMAT_TIMESTAMP('%b %e, %Y', reviewed_at, 'America/New_York') AS reviewed_label,
        is_action_open
      FROM ${actionsTable}
      WHERE TRUE
        ${areaWhere}
        ${openWhere}
      ORDER BY priority_rank, money_at_stake DESC
      LIMIT ${limit}
    `,
  } satisfies Record<string, string>;
}

export async function getCustomerActionsData(options: GetCustomerActionsOptions = {}): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const area = normalizeArea(options.area);
  const includeClosed = normalizeIncludeClosed(options.includeClosed);
  const limit = normalizeLimit(options.limit);
  const filters = buildDashboardFilters(area, includeClosed, limit);
  const dataset = customerActionsDataset();
  const queries = buildQueries(area, includeClosed, limit);
  const params = area === "all" ? undefined : { area };

  try {
    const [summary, byArea, byBucket, queue] = await Promise.all([
      runBigQuery(queries.customer_action_summary, params),
      runBigQuery(queries.customer_action_by_area, params),
      runBigQuery(queries.customer_action_by_bucket, params),
      runBigQuery(queries.customer_action_queue, params),
    ]);

    return {
      rows: {
        customer_action_summary: summary,
        customer_action_by_area: byArea,
        customer_action_by_bucket: byBucket,
        customer_action_queue: queue,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["customer_action_queue", "operator_action_reviews", "contract_terms_reviews"],
        note:
          "Customer Action Queue is one row per customer x action for the separate Actions tab. It joins Revenue, Retention, Contract Terms, and operator review ledgers. Current owner remains Unknown until a real assignment source is modeled.",
      },
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Customer actions unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["customer_action_queue"],
        note:
          "Customer Action Queue is one row per customer x action for the separate Actions tab. It joins Revenue, Retention, Contract Terms, and operator review ledgers. Current owner remains Unknown until a real assignment source is modeled.",
      },
    };
  }
}

function buildFreshness(summary: DashboardRow[]): DashboardFreshness {
  const refreshedAt = stringValue(summary[0]?.refreshed_at);

  if (!refreshedAt) {
    return {
      status: "stale",
      label: "No refresh timestamp",
      detail: "The customer action queue returned rows without a refresh timestamp.",
    };
  }

  const refreshedDate = new Date(refreshedAt);
  const ageMinutes = Math.max(0, Math.round((Date.now() - refreshedDate.getTime()) / 60000));
  const status = ageMinutes <= 180 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live dbt mart data" : "Stale dbt mart data",
    detail: `Customer action queue refreshed ${formatAge(ageMinutes)} ago.`,
    refreshedAt,
  };
}

function formatAge(minutes: number) {
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (remainingMinutes === 0) return `${hours}h`;
  return `${hours}h ${remainingMinutes}m`;
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message;
  return "Unknown Customer Actions error";
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}
