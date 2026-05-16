import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

export const LEAD_MAGNET_TIME_RANGE_OPTIONS = [
  {
    value: "24h",
    label: "24H",
    description: "Buyers whose first purchase landed in the last 24 hours.",
  },
  {
    value: "7d",
    label: "7D",
    description: "Buyers whose first purchase landed in the last 7 days.",
  },
  {
    value: "30d",
    label: "30D",
    description: "Buyers whose first purchase landed in the last 30 days.",
  },
  {
    value: "90d",
    label: "90D",
    description: "Buyers whose first purchase landed in the last 90 days.",
  },
  {
    value: "180d",
    label: "180D",
    description: "Buyers whose first purchase landed in the last 180 days.",
  },
  {
    value: "all",
    label: "All",
    description: "All matched paid buyers in the mart.",
  },
] as const;

export type LeadMagnetTimeRange = (typeof LEAD_MAGNET_TIME_RANGE_OPTIONS)[number]["value"];

/**
 * Window for the "Recently working" operational strip — independent of the
 * date picker. Tune later if observed cadence suggests 14 or 30 is the better
 * operational window. See Spec v1 §5.1.
 */
export const RECENT_BUYER_WINDOW_DAYS = 7;

type GetLeadMagnetDataOptions = {
  timeRange?: string | null;
};

const DEFAULT_TIME_RANGE: LeadMagnetTimeRange = "all";
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function leadMagnetDataset() {
  const dataset =
    process.env.BIGQUERY_LEAD_MAGNET_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for lead magnets: ${dataset}`);
  }

  return dataset;
}

function tableRef(
  tableName:
    | "lead_magnet_buyer_detail"
    | "lead_magnet_detail"
    | "lead_magnet_performance_summary",
) {
  return `\`${deeConfig.bigQuery.projectId}.${leadMagnetDataset()}.${tableName}\``;
}

function coreTableRef(tableName: "dim_ghl_contacts") {
  return `\`${deeConfig.bigQuery.projectId}.Core.${tableName}\``;
}

export function normalizeLeadMagnetTimeRange(value: string | null | undefined): LeadMagnetTimeRange {
  const normalized = value?.toLowerCase();
  const option = LEAD_MAGNET_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);

  return option?.value ?? DEFAULT_TIME_RANGE;
}

function buildDashboardFilters(timeRange: LeadMagnetTimeRange): DashboardFilters {
  const activeOption = LEAD_MAGNET_TIME_RANGE_OPTIONS.find((option) => option.value === timeRange);

  return {
    timeRange,
    timeRangeLabel: activeOption?.label ?? "All",
    timeRangeDescription: activeOption?.description ?? "All matched paid buyers in the mart.",
    timeRangeOptions: LEAD_MAGNET_TIME_RANGE_OPTIONS.map((option) => ({ ...option })),
  };
}

function timestampRangePredicate(timeRange: LeadMagnetTimeRange, field: string) {
  if (timeRange === "all") return "";
  if (timeRange === "24h") {
    return `${field} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)`;
  }

  const days = timeRange === "7d" ? 6 : timeRange === "30d" ? 29 : timeRange === "90d" ? 89 : 179;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function whereTimeRange(timeRange: LeadMagnetTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

export function buildLeadMagnetQueries(timeRange: LeadMagnetTimeRange) {
  const buyerTable = tableRef("lead_magnet_buyer_detail");
  const opportunityTable = tableRef("lead_magnet_detail");
  const performanceTable = tableRef("lead_magnet_performance_summary");
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");
  const buyerWhereOnB = buyerWhere
    ? buyerWhere.replace("first_purchase_at", "b.first_purchase_at")
    : "";

  return {
    lead_magnet_summary: `
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at)) AS refreshed_at,
        COUNT(*) AS buyers,
        SUM(paid_payments_count) AS paid_payments,
        SAFE_DIVIDE(SUM(paid_payments_count), NULLIF(COUNT(*), 0)) AS payments_per_buyer,
        COUNTIF(is_multi_payment_buyer) AS multi_payment_buyers,
        SAFE_DIVIDE(COUNTIF(is_multi_payment_buyer), NULLIF(COUNT(*), 0)) AS multi_payment_buyer_rate,
        SUM(first_purchase_net_revenue) AS first_purchase_net_revenue,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        AVG(first_purchase_net_revenue) AS avg_first_purchase_revenue,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        COUNTIF(has_latest_prior_magnet_before_first_purchase) AS buyers_with_latest_prior_magnet,
        SAFE_DIVIDE(COUNTIF(has_latest_prior_magnet_before_first_purchase), NULLIF(COUNT(*), 0)) AS latest_prior_magnet_buyer_coverage,
        SAFE_DIVIDE(
          SUM(IF(has_latest_prior_magnet_before_first_purchase, total_net_revenue_after_refunds, 0)),
          NULLIF(SUM(total_net_revenue_after_refunds), 0)
        ) AS latest_prior_magnet_revenue_coverage,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        COUNTIF(has_active_booking_before_first_purchase) AS buyers_with_active_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_active_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS active_booking_before_purchase_rate,
        COUNTIF(purchase_magnet_attribution_flag = 'purchase_before_first_magnet') AS buyers_purchase_before_first_magnet,
        COUNTIF(purchase_magnet_attribution_flag = 'no_known_magnet') AS buyers_no_known_magnet
      FROM ${buyerTable}
      ${buyerWhere}
    `,
    lead_magnet_attribution_flags: `
      SELECT
        purchase_magnet_attribution_flag AS attribution_flag,
        CASE purchase_magnet_attribution_flag
          WHEN 'latest_prior_magnet' THEN 'Latest known magnet before purchase'
          WHEN 'purchase_before_first_magnet' THEN 'Purchase came before first known magnet'
          WHEN 'no_known_magnet' THEN 'No known magnet'
          WHEN 'missing_taxonomy' THEN 'Missing taxonomy'
          WHEN 'uncategorized_offer_type' THEN 'Uncategorized offer type'
          ELSE INITCAP(REPLACE(purchase_magnet_attribution_flag, '_', ' '))
        END AS attribution_label,
        COUNT(*) AS buyers,
        SAFE_DIVIDE(COUNT(*), NULLIF(SUM(COUNT(*)) OVER (), 0)) AS buyer_share,
        SUM(first_purchase_net_revenue) AS first_purchase_net_revenue,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer
      FROM ${buyerTable}
      ${buyerWhere}
      GROUP BY attribution_flag, attribution_label
      ORDER BY
        CASE attribution_flag
          WHEN 'latest_prior_magnet' THEN 1
          WHEN 'purchase_before_first_magnet' THEN 2
          WHEN 'no_known_magnet' THEN 3
          WHEN 'uncategorized_offer_type' THEN 4
          ELSE 5
        END
    `,
    lead_magnet_recent_activity: `
      WITH recent AS (
        SELECT
          contact_sk,
          latest_prior_lead_magnet_id
        FROM ${buyerTable}
        WHERE first_purchase_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${RECENT_BUYER_WINDOW_DAYS} DAY)
      )
      SELECT
        COUNT(*) AS new_buyers,
        COUNTIF(latest_prior_lead_magnet_id IS NOT NULL) AS attributed_buyers,
        COUNTIF(latest_prior_lead_magnet_id IS NULL) AS unattributed_buyers,
        COUNT(DISTINCT latest_prior_lead_magnet_id) AS magnets_with_buyers
      FROM recent
    `,
    lead_magnet_revenue_concentration: `
      WITH base AS (
        SELECT
          latest_prior_lead_magnet_id,
          latest_prior_lead_magnet_name,
          total_net_revenue_after_refunds
        FROM ${buyerTable}
        ${buyerWhere}
      ),
      per_magnet AS (
        SELECT
          COALESCE(latest_prior_lead_magnet_name, 'No prior magnet') AS lead_magnet_reporting_name,
          SUM(total_net_revenue_after_refunds) AS revenue
        FROM base
        WHERE latest_prior_lead_magnet_id IS NOT NULL
        GROUP BY lead_magnet_reporting_name
        HAVING SUM(total_net_revenue_after_refunds) > 0
      ),
      ranked AS (
        SELECT
          revenue,
          RANK() OVER (ORDER BY revenue DESC) AS rnk
        FROM per_magnet
      )
      SELECT
        COALESCE(SUM(IF(rnk <= 3, revenue, 0)), 0) AS top_3_revenue,
        COALESCE(SUM(revenue), 0) AS total_attributed_revenue,
        COUNT(*) AS magnets_with_revenue
      FROM ranked
    `,
    lead_magnet_attribution_breakdown: `
      WITH buyer_magnet_counts AS (
        SELECT
          b.contact_sk,
          COUNT(DISTINCT d.lead_magnet_id) AS pre_purchase_magnet_touches
        FROM ${buyerTable} AS b
        LEFT JOIN ${opportunityTable} AS d
          ON d.contact_sk = b.contact_sk
          AND d.opportunity_created_at <= b.first_purchase_at
          AND d.include_in_lead_magnet_dashboard
        ${buyerWhereOnB}
        GROUP BY b.contact_sk
      )
      SELECT
        COUNT(*) AS total_buyers,
        COUNTIF(pre_purchase_magnet_touches = 0) AS no_magnet_buyers,
        COUNTIF(pre_purchase_magnet_touches = 1) AS single_magnet_buyers,
        COUNTIF(pre_purchase_magnet_touches >= 2) AS multi_magnet_buyers
      FROM buyer_magnet_counts
    `,
    lead_magnet_performance_rows: `
      SELECT
        lead_magnet_id,
        lead_magnet_reporting_name,
        lead_magnet_category,
        COALESCE(opt_in_lead_count, 0) AS leads_lifetime,
        COALESCE(net_revenue_after_refunds, 0) AS revenue_lifetime,
        SAFE_DIVIDE(net_revenue_after_refunds, NULLIF(opt_in_lead_count, 0))
          AS revenue_per_lead_lifetime,
        COALESCE(buyer_count, 0) AS buyers_lifetime,
        COALESCE(leads_in_90d, 0) AS leads_in_90d,
        time_to_buyer_days AS time_to_buyer_days,
        FORMAT_TIMESTAMP(
          '%FT%TZ',
          (
            SELECT MAX(ts)
            FROM UNNEST([last_meaningful_engagement_at, latest_payment_at]) AS ts
            WHERE ts IS NOT NULL
          )
        ) AS last_activity_at
      FROM ${performanceTable}
      ORDER BY revenue_lifetime DESC NULLS LAST, leads_lifetime DESC NULLS LAST
    `,
  } satisfies Record<string, string>;
}

export async function getLeadMagnetData(options: GetLeadMagnetDataOptions = {}): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeLeadMagnetTimeRange(options.timeRange);
  const filters = buildDashboardFilters(timeRange);
  const dataset = leadMagnetDataset();
  const leadMagnetQueries = buildLeadMagnetQueries(timeRange);

  try {
    const [
      summary,
      attributionFlags,
      recentActivity,
      revenueConcentration,
      attributionBreakdown,
      performanceRows,
    ] = await Promise.all([
      runBigQuery(leadMagnetQueries.lead_magnet_summary),
      runBigQuery(leadMagnetQueries.lead_magnet_attribution_flags),
      runBigQuery(leadMagnetQueries.lead_magnet_recent_activity),
      runBigQuery(leadMagnetQueries.lead_magnet_revenue_concentration),
      runBigQuery(leadMagnetQueries.lead_magnet_attribution_breakdown),
      runBigQuery(leadMagnetQueries.lead_magnet_performance_rows),
    ]);

    return {
      rows: {
        lead_magnet_summary: summary,
        lead_magnet_attribution_flags: attributionFlags,
        lead_magnet_recent_activity: recentActivity,
        lead_magnet_revenue_concentration: revenueConcentration,
        lead_magnet_attribution_breakdown: attributionBreakdown,
        lead_magnet_performance_rows: performanceRows,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: [
          "lead_magnet_buyer_detail",
          "lead_magnet_detail",
          "lead_magnet_performance_summary",
          "Core.dim_ghl_contacts",
        ],
        note:
          "Last-touch buyer credit on the front page; queue status and revenue_per_lead from lead_magnet_performance_summary. Multi-magnet aggregate counts join lead_magnet_buyer_detail to lead_magnet_detail on contact_sk. The per-magnet drill compares first_known_lead_magnet_id vs latest_prior_lead_magnet_id from lead_magnet_buyer_detail to classify each magnet as introducer or closer.",
      },
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Lead magnet data unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: [
          "lead_magnet_buyer_detail",
          "lead_magnet_detail",
          "lead_magnet_performance_summary",
          "Core.dim_ghl_contacts",
        ],
        note:
          "Last-touch buyer credit on the front page; queue status and revenue_per_lead from lead_magnet_performance_summary. Multi-magnet aggregate counts join lead_magnet_buyer_detail to lead_magnet_detail on contact_sk. The per-magnet drill compares first_known_lead_magnet_id vs latest_prior_lead_magnet_id from lead_magnet_buyer_detail to classify each magnet as introducer or closer.",
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
      detail: "The lead magnet mart returned rows without a refresh timestamp.",
    };
  }

  const ageHours = (Date.now() - new Date(refreshedAt).getTime()) / 36e5;
  const status = ageHours <= 24 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live dbt mart data" : "Mart needs refresh",
    detail: `Lead magnet marts refreshed ${formatRelativeAge(ageHours)} ago.`,
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

// ------------------------------------------------------------------
// Per-magnet drill — Spec v1 §6.
// Lightweight detail fetcher keyed by magnet id, returning the four panels'
// data in one round trip: hero summary, funnel counts, time-to-buyer buckets
// (computed in SQL), and the smoking-gun buyer list.
// ------------------------------------------------------------------

const MAGNET_ID_PATTERN = /^[A-Za-z0-9_\-]{4,64}$/;
const BUYER_TABLE_MAX_ROWS = 100;

function sanitizeMagnetId(value: string | null | undefined): string | null {
  if (!value) return null;
  return MAGNET_ID_PATTERN.test(value) ? value : null;
}

type GetLeadMagnetDetailOptions = {
  magnetId: string;
  timeRange?: string | null;
};

export type LeadMagnetDetailData = {
  magnetId: string;
  rows: {
    magnet_detail_summary: DashboardRow[];
    magnet_detail_buyers: DashboardRow[];
    magnet_detail_first_touch_breakdown: DashboardRow[];
  };
  filters: DashboardFilters;
  generatedAt: string;
  freshness: DashboardFreshness;
  error?: string;
};

export async function getLeadMagnetDetailData(
  options: GetLeadMagnetDetailOptions,
): Promise<LeadMagnetDetailData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeLeadMagnetTimeRange(options.timeRange);
  const filters = buildDashboardFilters(timeRange);
  const magnetId = sanitizeMagnetId(options.magnetId);

  if (!magnetId) {
    return {
      magnetId: options.magnetId,
      rows: { magnet_detail_summary: [], magnet_detail_buyers: [], magnet_detail_first_touch_breakdown: [] },
      filters,
      generatedAt,
      freshness: {
        status: "error",
        label: "Invalid magnet id",
        detail: "The magnet id in the URL did not pass validation.",
      },
      error: "Invalid magnet id",
    };
  }

  const buyerTable = tableRef("lead_magnet_buyer_detail");
  const performanceTable = tableRef("lead_magnet_performance_summary");
  const contactTable = coreTableRef("dim_ghl_contacts");
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");
  const buyerAndPredicate = timestampRangePredicate(timeRange, "first_purchase_at");
  const buyerAnd = buyerAndPredicate ? `AND ${buyerAndPredicate}` : "";

  const summaryQuery = `
    WITH perf AS (
      SELECT
        lead_magnet_id,
        lead_magnet_reporting_name,
        lead_magnet_category,
        lead_magnet_offer_type,
        queue_status,
        opt_in_lead_count,
        booking_lead_count,
        purchase_lead_count,
        gross_revenue,
        net_revenue_after_refunds,
        repeat_buyer_rate,
        FORMAT_TIMESTAMP('%FT%TZ', mart_refreshed_at) AS refreshed_at
      FROM ${performanceTable}
      WHERE lead_magnet_id = '${magnetId}'
    ),
    buyer_aggregates AS (
      SELECT
        COUNT(*) AS buyer_count,
        SUM(total_net_revenue_after_refunds) AS attributed_net_revenue,
        APPROX_QUANTILES(days_latest_prior_magnet_to_first_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_days_opt_in_to_purchase,
        APPROX_QUANTILES(first_purchase_net_revenue, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_first_purchase_revenue
      FROM ${buyerTable}
      WHERE latest_prior_lead_magnet_id = '${magnetId}'
        ${buyerAnd}
    )
    SELECT
      perf.lead_magnet_id,
      perf.lead_magnet_reporting_name,
      perf.lead_magnet_category,
      perf.lead_magnet_offer_type,
      perf.queue_status,
      perf.opt_in_lead_count,
      perf.booking_lead_count,
      perf.purchase_lead_count,
      perf.gross_revenue,
      perf.net_revenue_after_refunds AS lifetime_net_revenue,
      perf.repeat_buyer_rate,
      perf.refreshed_at,
      buyer_aggregates.buyer_count,
      buyer_aggregates.attributed_net_revenue,
      buyer_aggregates.median_days_opt_in_to_purchase,
      buyer_aggregates.median_first_purchase_revenue
    FROM perf
    CROSS JOIN buyer_aggregates
  `;

  const firstTouchBreakdownQuery = `
    WITH attributed AS (
      SELECT
        first_known_lead_magnet_id,
        first_known_lead_magnet_name
      FROM ${buyerTable}
      WHERE latest_prior_lead_magnet_id = '${magnetId}'
        ${buyerAnd}
    ),
    counts AS (
      SELECT
        COUNT(*) AS last_touch_buyers,
        COUNTIF(first_known_lead_magnet_id = '${magnetId}') AS first_and_last_buyers,
        COUNTIF(
          first_known_lead_magnet_id IS NOT NULL
          AND first_known_lead_magnet_id != '${magnetId}'
        ) AS last_only_buyers,
        COUNTIF(first_known_lead_magnet_id IS NULL) AS missing_first_touch_buyers
      FROM attributed
    ),
    other_magnet AS (
      SELECT first_known_lead_magnet_name AS sample_other_magnet
      FROM attributed
      WHERE first_known_lead_magnet_id IS NOT NULL
        AND first_known_lead_magnet_id != '${magnetId}'
      GROUP BY first_known_lead_magnet_name
      ORDER BY COUNT(*) DESC
      LIMIT 1
    )
    SELECT
      counts.last_touch_buyers,
      counts.first_and_last_buyers,
      counts.last_only_buyers,
      counts.missing_first_touch_buyers,
      other_magnet.sample_other_magnet
    FROM counts
    LEFT JOIN other_magnet ON TRUE
  `;

  const buyersQuery = `
    SELECT
      b.contact_sk,
      b.contact_id,
      COALESCE(
        NULLIF(TRIM(CONCAT(COALESCE(c.first_name, ''), ' ', COALESCE(c.last_name, ''))), ''),
        NULLIF(c.email, ''),
        'Unknown buyer'
      ) AS buyer_name,
      c.email AS buyer_email,
      b.first_purchase_at,
      b.days_latest_prior_magnet_to_first_purchase AS days_opt_in_to_purchase,
      b.first_purchase_net_revenue,
      b.total_net_revenue_after_refunds AS lifetime_net_revenue
    FROM ${buyerTable} AS b
    LEFT JOIN ${contactTable} AS c
      ON c.contact_id = b.contact_id
    WHERE b.latest_prior_lead_magnet_id = '${magnetId}'
      ${buyerAnd}
    ORDER BY b.first_purchase_at DESC
    LIMIT ${BUYER_TABLE_MAX_ROWS + 1}
  `;
  void buyerWhere;

  try {
    const [summaryRows, buyerRows, firstTouchBreakdownRows] = await Promise.all([
      runBigQuery(summaryQuery),
      runBigQuery(buyersQuery),
      runBigQuery(firstTouchBreakdownQuery),
    ]);

    return {
      magnetId,
      rows: {
        magnet_detail_summary: summaryRows,
        magnet_detail_buyers: buyerRows,
        magnet_detail_first_touch_breakdown: firstTouchBreakdownRows,
      },
      filters,
      generatedAt,
      freshness: buildFreshness(summaryRows),
    };
  } catch (error) {
    return {
      magnetId,
      rows: { magnet_detail_summary: [], magnet_detail_buyers: [], magnet_detail_first_touch_breakdown: [] },
      filters,
      generatedAt,
      freshness: {
        status: "error",
        label: "Magnet detail unavailable",
        detail: getErrorMessage(error),
      },
      error: getErrorMessage(error),
    };
  }
}
