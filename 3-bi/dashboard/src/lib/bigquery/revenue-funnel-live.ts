import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

export const REVENUE_FUNNEL_TIME_RANGE_OPTIONS = [
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

export type RevenueFunnelTimeRange = (typeof REVENUE_FUNNEL_TIME_RANGE_OPTIONS)[number]["value"];

type GetRevenueFunnelDataOptions = {
  timeRange?: string | null;
};

const DEFAULT_TIME_RANGE: RevenueFunnelTimeRange = "all";
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function revenueFunnelDataset() {
  const dataset =
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for revenue funnel: ${dataset}`);
  }

  return dataset;
}

function tableRef() {
  return `\`${deeConfig.bigQuery.projectId}.${revenueFunnelDataset()}.revenue_funnel_detail\``;
}

export function normalizeRevenueFunnelTimeRange(value: string | null | undefined): RevenueFunnelTimeRange {
  const normalized = value?.toLowerCase();
  const option = REVENUE_FUNNEL_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);

  return option?.value ?? DEFAULT_TIME_RANGE;
}

function buildDashboardFilters(timeRange: RevenueFunnelTimeRange): DashboardFilters {
  const activeOption = REVENUE_FUNNEL_TIME_RANGE_OPTIONS.find((option) => option.value === timeRange);

  return {
    timeRange,
    timeRangeLabel: activeOption?.label ?? "All",
    timeRangeDescription: activeOption?.description ?? "All matched paid buyers in the mart.",
    timeRangeOptions: REVENUE_FUNNEL_TIME_RANGE_OPTIONS.map((option) => ({ ...option })),
  };
}

function timestampRangePredicate(timeRange: RevenueFunnelTimeRange, field: string) {
  if (timeRange === "all") return "";

  const days = timeRange === "30d" ? 29 : timeRange === "90d" ? 89 : 179;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function whereTimeRange(timeRange: RevenueFunnelTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

function buildRevenueFunnelQueries(timeRange: RevenueFunnelTimeRange) {
  const revenueTable = tableRef();
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");

  return {
    revenue_funnel_summary: `
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at)) AS refreshed_at,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SUM(total_gross_revenue) AS total_gross_revenue,
        SUM(total_refunds_amount) AS total_refunds_amount,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        SUM(paid_payments_count) AS paid_payments,
        SAFE_DIVIDE(SUM(paid_payments_count), NULLIF(COUNT(*), 0)) AS payments_per_buyer,
        COUNTIF(is_payment_plan_buyer) AS payment_plan_buyers,
        SAFE_DIVIDE(COUNTIF(is_payment_plan_buyer), NULLIF(COUNT(*), 0)) AS payment_plan_buyer_rate,
        SUM(IF(is_payment_plan_buyer, total_net_revenue_after_refunds, 0)) AS payment_plan_net_revenue,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        COUNTIF(has_latest_prior_magnet_before_first_purchase) AS buyers_with_latest_prior_magnet,
        SAFE_DIVIDE(COUNTIF(has_latest_prior_magnet_before_first_purchase), NULLIF(COUNT(*), 0)) AS latest_prior_magnet_buyer_coverage,
        COUNTIF(purchase_magnet_attribution_flag = 'purchase_before_first_magnet') AS buyers_purchase_before_first_magnet,
        COUNTIF(purchase_magnet_attribution_flag = 'no_known_magnet') AS buyers_no_known_magnet,
        COUNTIF(best_available_operator_source = 'unassigned') AS unassigned_operator_buyers,
        SAFE_DIVIDE(COUNTIF(best_available_operator_source = 'unassigned'), NULLIF(COUNT(*), 0)) AS unassigned_operator_rate,
        APPROX_QUANTILES(hours_latest_booking_to_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_hours_booking_to_purchase
      FROM ${revenueTable}
      ${buyerWhere}
    `,
    revenue_funnel_payment_plans: `
      SELECT
        payment_plan_status,
        CASE payment_plan_status
          WHEN 'single_payment' THEN 'Single payment'
          WHEN 'multi_payment_plan' THEN 'Multi-payment plan'
          WHEN 'auto_renew_plan' THEN 'Auto-renew plan'
          WHEN 'plan_named_single_payment' THEN 'Plan-named single payment'
          ELSE INITCAP(REPLACE(payment_plan_status, '_', ' '))
        END AS payment_plan_label,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        SUM(paid_payments_count) AS paid_payments,
        SAFE_DIVIDE(SUM(paid_payments_count), NULLIF(COUNT(*), 0)) AS payments_per_buyer,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY payment_plan_status, payment_plan_label
      ORDER BY total_net_revenue_after_refunds DESC
    `,
    revenue_funnel_product_families: `
      SELECT
        TRIM(top_product_family) AS top_product_family,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        COUNTIF(is_payment_plan_buyer) AS payment_plan_buyers,
        SAFE_DIVIDE(COUNTIF(is_payment_plan_buyer), NULLIF(COUNT(*), 0)) AS payment_plan_buyer_rate,
        SUM(paid_payments_count) AS paid_payments,
        COUNTIF(first_payment_source_platform = 'stripe') AS first_purchase_stripe_buyers,
        COUNTIF(fanbasis_payments_count > 0) AS has_fanbasis_buyers
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY top_product_family
      ORDER BY total_net_revenue_after_refunds DESC
      LIMIT 12
    `,
    revenue_funnel_paths: `
      SELECT
        pre_purchase_funnel_path,
        CASE pre_purchase_funnel_path
          WHEN 'booked_before_purchase' THEN 'Booked before purchase'
          WHEN 'canceled_booking_before_purchase' THEN 'Canceled booking before purchase'
          WHEN 'reached_by_phone_before_purchase' THEN 'Reached by phone before purchase'
          WHEN 'worked_before_purchase' THEN 'Worked before purchase'
          WHEN 'magnet_before_purchase_no_work_logged' THEN 'Magnet before purchase, no work logged'
          WHEN 'buyer_without_known_pre_purchase_path' THEN 'No known pre-purchase path'
          ELSE INITCAP(REPLACE(pre_purchase_funnel_path, '_', ' '))
        END AS path_label,
        COUNT(*) AS buyers,
        SAFE_DIVIDE(COUNT(*), NULLIF(SUM(COUNT(*)) OVER (), 0)) AS buyer_share,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        APPROX_QUANTILES(hours_latest_booking_to_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_hours_booking_to_purchase
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY pre_purchase_funnel_path, path_label
      ORDER BY buyers DESC
    `,
    revenue_funnel_magnets: `
      SELECT
        COALESCE(latest_prior_lead_magnet_name, 'No prior magnet') AS lead_magnet_name,
        COALESCE(latest_prior_lead_magnet_offer_type, 'no_prior_magnet') AS offer_type,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer,
        COUNTIF(is_payment_plan_buyer) AS payment_plan_buyers,
        SAFE_DIVIDE(COUNTIF(is_payment_plan_buyer), NULLIF(COUNT(*), 0)) AS payment_plan_buyer_rate,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        APPROX_QUANTILES(days_latest_prior_magnet_to_first_purchase, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_days_latest_magnet_to_purchase
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY lead_magnet_name, offer_type
      ORDER BY total_net_revenue_after_refunds DESC, buyers DESC
      LIMIT 12
    `,
    revenue_funnel_operator_diagnostic: `
      SELECT
        best_available_operator_source,
        CASE best_available_operator_source
          WHEN 'first_successful_call_before_purchase' THEN 'First successful call'
          WHEN 'first_touch_before_purchase' THEN 'First touch'
          WHEN 'latest_prior_opportunity_owner' THEN 'Latest prior owner'
          WHEN 'latest_booking_owner' THEN 'Booking owner'
          WHEN 'unassigned' THEN 'Unassigned'
          ELSE INITCAP(REPLACE(best_available_operator_source, '_', ' '))
        END AS operator_source_label,
        CASE
          WHEN LOWER(best_available_operator_name) IN ('aariz menon', 'ayaan menon')
            THEN 'Ayaan Menon'
          ELSE best_available_operator_name
        END AS best_available_operator_name,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY best_available_operator_source, operator_source_label, best_available_operator_name
      ORDER BY buyers DESC, total_net_revenue_after_refunds DESC
      LIMIT 12
    `,
    revenue_funnel_quality: `
      SELECT
        revenue_funnel_quality_flag,
        CASE revenue_funnel_quality_flag
          WHEN 'clean' THEN 'Clean'
          WHEN 'no_known_magnet' THEN 'No known magnet'
          WHEN 'negative_net_revenue' THEN 'Negative net revenue'
          WHEN 'missing_taxonomy' THEN 'Missing taxonomy'
          WHEN 'uncategorized_offer_type' THEN 'Uncategorized offer type'
          WHEN 'contact_not_matched' THEN 'Contact not matched'
          ELSE INITCAP(REPLACE(revenue_funnel_quality_flag, '_', ' '))
        END AS quality_label,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY revenue_funnel_quality_flag, quality_label
      ORDER BY buyers DESC
    `,
  } satisfies Record<string, string>;
}

export async function getRevenueFunnelData(options: GetRevenueFunnelDataOptions = {}): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeRevenueFunnelTimeRange(options.timeRange);
  const filters = buildDashboardFilters(timeRange);
  const dataset = revenueFunnelDataset();
  const revenueFunnelQueries = buildRevenueFunnelQueries(timeRange);

  try {
    const [
      summary,
      paymentPlans,
      productFamilies,
      paths,
      magnets,
      operatorDiagnostic,
      quality,
    ] = await Promise.all([
      runBigQuery(revenueFunnelQueries.revenue_funnel_summary),
      runBigQuery(revenueFunnelQueries.revenue_funnel_payment_plans),
      runBigQuery(revenueFunnelQueries.revenue_funnel_product_families),
      runBigQuery(revenueFunnelQueries.revenue_funnel_paths),
      runBigQuery(revenueFunnelQueries.revenue_funnel_magnets),
      runBigQuery(revenueFunnelQueries.revenue_funnel_operator_diagnostic),
      runBigQuery(revenueFunnelQueries.revenue_funnel_quality),
    ]);

    return {
      rows: {
        revenue_funnel_summary: summary,
        revenue_funnel_payment_plans: paymentPlans,
        revenue_funnel_product_families: productFamilies,
        revenue_funnel_paths: paths,
        revenue_funnel_magnets: magnets,
        revenue_funnel_operator_diagnostic: operatorDiagnostic,
        revenue_funnel_quality: quality,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["revenue_funnel_detail"],
        note:
          "Revenue Funnel is buyer-journey grain. It is useful for money, product, payment-plan, magnet, and booking-path analysis; operator attribution is diagnostic only.",
      },
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Revenue funnel data unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["revenue_funnel_detail"],
        note:
          "Revenue Funnel is buyer-journey grain. It is useful for money, product, payment-plan, magnet, and booking-path analysis; operator attribution is diagnostic only.",
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
      detail: "The revenue funnel mart returned rows without a refresh timestamp.",
    };
  }

  const refreshedDate = new Date(refreshedAt);
  const ageMinutes = Math.max(0, Math.round((Date.now() - refreshedDate.getTime()) / 60000));
  const status = ageMinutes <= 180 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live dbt mart data" : "Stale dbt mart data",
    detail: `Revenue funnel mart refreshed ${formatAge(ageMinutes)} ago.`,
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
  return "Unknown Revenue Funnel error";
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}
