import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

export const LEAD_MAGNET_TIME_RANGE_OPTIONS = [
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

type GetLeadMagnetDataOptions = {
  timeRange?: string | null;
};

const DEFAULT_TIME_RANGE: LeadMagnetTimeRange = "all";
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function leadMagnetDataset() {
  const dataset =
    process.env.BIGQUERY_LEAD_MAGNET_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    "dev_david";

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for lead magnets: ${dataset}`);
  }

  return dataset;
}

function tableRef(tableName: "lead_magnet_buyer_detail" | "lead_magnet_detail") {
  return `\`${deeConfig.bigQuery.projectId}.${leadMagnetDataset()}.${tableName}\``;
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

  const days = timeRange === "30d" ? 29 : timeRange === "90d" ? 89 : 179;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function whereTimeRange(timeRange: LeadMagnetTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

function offerTypeLabelSql(field: string) {
  return `
    CASE ${field}
      WHEN 'prompt_pack' THEN 'Prompt packs'
      WHEN 'template' THEN 'Templates'
      WHEN 'guide_or_doc' THEN 'Guides / docs'
      WHEN 'resource_list' THEN 'Resource lists'
      WHEN 'training_or_class' THEN 'Trainings / classes'
      WHEN 'video_or_replay' THEN 'Videos / replays'
      WHEN 'giveaway' THEN 'Giveaways'
      WHEN 'community' THEN 'Communities'
      WHEN 'waitlist' THEN 'Waitlists'
      WHEN 'sales_pipeline' THEN 'Sales pipelines'
      WHEN 'launch_event' THEN 'Launch events'
      WHEN 'uncategorized' THEN 'Uncategorized'
      WHEN 'no_prior_magnet' THEN 'No prior magnet'
      ELSE INITCAP(REPLACE(COALESCE(${field}, 'unknown'), '_', ' '))
    END
  `;
}

function buildLeadMagnetQueries(timeRange: LeadMagnetTimeRange) {
  const buyerTable = tableRef("lead_magnet_buyer_detail");
  const opportunityTable = tableRef("lead_magnet_detail");
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");
  const opportunityWhere = whereTimeRange(timeRange, "opportunity_created_at");

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
    lead_magnet_offer_types: `
      WITH typed AS (
        SELECT
          COALESCE(latest_prior_lead_magnet_offer_type, 'no_prior_magnet') AS offer_type,
          *
        FROM ${buyerTable}
        ${buyerWhere}
      )
      SELECT
        offer_type,
        ${offerTypeLabelSql("offer_type")} AS offer_type_label,
        COUNT(*) AS buyers,
        SUM(paid_payments_count) AS paid_payments,
        SAFE_DIVIDE(SUM(paid_payments_count), NULLIF(COUNT(*), 0)) AS payments_per_buyer,
        COUNTIF(is_multi_payment_buyer) AS multi_payment_buyers,
        SAFE_DIVIDE(COUNTIF(is_multi_payment_buyer), NULLIF(COUNT(*), 0)) AS multi_payment_buyer_rate,
        SUM(first_purchase_net_revenue) AS first_purchase_net_revenue,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        AVG(first_purchase_net_revenue) AS avg_first_purchase_revenue,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        APPROX_QUANTILES(days_latest_prior_magnet_to_first_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_days_latest_magnet_to_purchase
      FROM typed
      GROUP BY offer_type, offer_type_label
      ORDER BY total_net_revenue_after_refunds DESC, buyers DESC
      LIMIT 12
    `,
    lead_magnet_top_magnets: `
      WITH magneted AS (
        SELECT
          COALESCE(latest_prior_lead_magnet_name, 'No prior magnet') AS lead_magnet_name,
          COALESCE(latest_prior_lead_magnet_offer_type, 'no_prior_magnet') AS offer_type,
          COALESCE(latest_prior_lead_magnet_category, 'no_prior_magnet') AS lead_magnet_category,
          *
        FROM ${buyerTable}
        ${buyerWhere}
      )
      SELECT
        lead_magnet_name,
        offer_type,
        ${offerTypeLabelSql("offer_type")} AS offer_type_label,
        lead_magnet_category,
        COUNT(*) AS buyers,
        SUM(paid_payments_count) AS paid_payments,
        SAFE_DIVIDE(SUM(paid_payments_count), NULLIF(COUNT(*), 0)) AS payments_per_buyer,
        COUNTIF(is_multi_payment_buyer) AS multi_payment_buyers,
        SAFE_DIVIDE(COUNTIF(is_multi_payment_buyer), NULLIF(COUNT(*), 0)) AS multi_payment_buyer_rate,
        SUM(first_purchase_net_revenue) AS first_purchase_net_revenue,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        APPROX_QUANTILES(days_latest_prior_magnet_to_first_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_days_latest_magnet_to_purchase
      FROM magneted
      GROUP BY lead_magnet_name, offer_type, offer_type_label, lead_magnet_category
      ORDER BY total_net_revenue_after_refunds DESC, buyers DESC
      LIMIT 12
    `,
    lead_magnet_opportunity_offer_types: `
      WITH typed AS (
        SELECT
          lead_magnet_offer_type AS offer_type,
          *
        FROM ${opportunityTable}
        ${opportunityWhere}
      )
      SELECT
        offer_type,
        ${offerTypeLabelSql("offer_type")} AS offer_type_label,
        COUNT(*) AS opportunities,
        COUNT(DISTINCT contact_sk) AS contacts,
        SUM(touches_count) AS touches,
        SUM(call_count) AS calls,
        SUM(sms_count) AS sms,
        SUM(successful_call_count) AS successful_calls,
        SUM(direct_bookings_count) AS direct_bookings,
        SAFE_DIVIDE(SUM(direct_bookings_count), NULLIF(COUNT(*), 0)) AS direct_booking_rate,
        SUM(window_bookings_count) AS window_bookings,
        SUM(payment_count) AS payment_count,
        SUM(net_revenue_after_refunds) AS window_attributed_net_revenue,
        SAFE_DIVIDE(SUM(net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS window_revenue_per_opportunity
      FROM typed
      GROUP BY offer_type, offer_type_label
      ORDER BY window_attributed_net_revenue DESC, direct_bookings DESC, opportunities DESC
      LIMIT 12
    `,
    lead_magnet_pipeline_activity: `
      SELECT
        lead_magnet_reporting_name AS lead_magnet_name,
        lead_magnet_offer_type AS offer_type,
        ${offerTypeLabelSql("lead_magnet_offer_type")} AS offer_type_label,
        lead_magnet_category,
        COUNT(*) AS opportunities,
        COUNT(DISTINCT contact_sk) AS contacts,
        SUM(touches_count) AS touches,
        SUM(successful_call_count) AS successful_calls,
        SUM(direct_bookings_count) AS direct_bookings,
        SAFE_DIVIDE(SUM(direct_bookings_count), NULLIF(COUNT(*), 0)) AS direct_booking_rate,
        SUM(payment_count) AS payment_count,
        SUM(net_revenue_after_refunds) AS window_attributed_net_revenue,
        SAFE_DIVIDE(SUM(net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS window_revenue_per_opportunity
      FROM ${opportunityTable}
      ${opportunityWhere}
      GROUP BY lead_magnet_name, offer_type, offer_type_label, lead_magnet_category
      ORDER BY window_attributed_net_revenue DESC, direct_bookings DESC, opportunities DESC
      LIMIT 12
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
      offerTypes,
      topMagnets,
      opportunityOfferTypes,
      pipelineActivity,
    ] = await Promise.all([
      runBigQuery(leadMagnetQueries.lead_magnet_summary),
      runBigQuery(leadMagnetQueries.lead_magnet_attribution_flags),
      runBigQuery(leadMagnetQueries.lead_magnet_offer_types),
      runBigQuery(leadMagnetQueries.lead_magnet_top_magnets),
      runBigQuery(leadMagnetQueries.lead_magnet_opportunity_offer_types),
      runBigQuery(leadMagnetQueries.lead_magnet_pipeline_activity),
    ]);

    return {
      rows: {
        lead_magnet_summary: summary,
        lead_magnet_attribution_flags: attributionFlags,
        lead_magnet_offer_types: offerTypes,
        lead_magnet_top_magnets: topMagnets,
        lead_magnet_opportunity_offer_types: opportunityOfferTypes,
        lead_magnet_pipeline_activity: pipelineActivity,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["lead_magnet_buyer_detail", "lead_magnet_detail"],
        note:
          "Buyer money comes from lead_magnet_buyer_detail. Opportunity activity and revenue are window-attributed from lead_magnet_detail.",
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
        tables: ["lead_magnet_buyer_detail", "lead_magnet_detail"],
        note:
          "Buyer money comes from lead_magnet_buyer_detail. Opportunity activity and revenue are window-attributed from lead_magnet_detail.",
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
