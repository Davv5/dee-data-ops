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

function martTableRef(tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.${revenueFunnelDataset()}.${tableName}\``;
}

function coreTableRef(tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.Core.${tableName}\``;
}

function rawTableRef(tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.Raw.${tableName}\``;
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

function andTimeRange(timeRange: RevenueFunnelTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `AND ${predicate}` : "";
}

function buildRevenueFunnelQueries(timeRange: RevenueFunnelTimeRange) {
  const revenueTable = tableRef();
  const retentionTable = martTableRef("customer_retention_detail");
  const missingContactsTable = martTableRef("fanbasis_missing_ghl_contacts");
  const paymentsTable = coreTableRef("fct_payments");
  const canceledRecoveryTable = martTableRef("canceled_booking_recovery_detail");
  const fanbasisObjectsTable = rawTableRef("fanbasis_objects_raw");
  const contactSyncAuditTable = rawTableRef("fanbasis_ghl_contact_sync_audit");
  const actionReviewsTable = martTableRef("operator_action_reviews");
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");
  const revenueQueueBase = `
    WITH latest_reviews AS (
      SELECT
        contact_sk,
        action_bucket,
        review_status,
        review_note,
        reviewed_by,
        FORMAT_TIMESTAMP('%b %e, %Y', reviewed_at, 'America/New_York') AS reviewed_label,
        FORMAT_TIMESTAMP('%FT%TZ', reviewed_at) AS reviewed_at,
        FORMAT_TIMESTAMP('%FT%TZ', expires_at) AS expires_at
      FROM (
        SELECT
          COALESCE(contact_sk, entity_id) AS contact_sk,
          action_bucket,
          LOWER(review_status) AS review_status,
          review_note,
          reviewed_by,
          reviewed_at,
          expires_at,
          ROW_NUMBER() OVER (
            PARTITION BY COALESCE(contact_sk, entity_id), action_bucket
            ORDER BY reviewed_at DESC
          ) AS review_rank
        FROM ${actionReviewsTable}
        WHERE
          area = 'revenue'
          AND queue_name = 'revenue_action_queue'
          AND entity_type = 'customer'
          AND LOWER(review_status) IN ('open', 'reviewed', 'fixed', 'wont_fix')
      )
      WHERE review_rank = 1
    ),
    classified AS (
      SELECT
        contact_sk,
        contact_id,
        COALESCE(NULLIF(contact_name, ''), NULLIF(email_norm, ''), NULLIF(phone, ''), 'Unknown buyer') AS customer_display_name,
        email_norm,
        phone,
        FORMAT_TIMESTAMP('%b %e, %Y', first_purchase_at, 'America/New_York') AS first_purchase_label,
        FORMAT_TIMESTAMP('%b %e, %Y', latest_purchase_at, 'America/New_York') AS latest_purchase_label,
        total_net_revenue_after_refunds,
        paid_payments_count,
        top_product_by_net_revenue,
        top_product_family,
        payment_plan_status,
        payment_plan_truth_status,
        revenue_funnel_quality_flag,
        purchase_magnet_attribution_flag,
        latest_prior_lead_magnet_name,
        latest_prior_lead_magnet_offer_type,
        credited_closer_name,
        credited_closer_source,
        credited_closer_confidence,
        credited_setter_name,
        pre_purchase_funnel_path,
        CASE
          WHEN revenue_funnel_quality_flag = 'negative_net_revenue'
            THEN 'data_risk'
          WHEN revenue_funnel_quality_flag IN ('missing_taxonomy', 'uncategorized_offer_type', 'contact_not_matched', 'no_known_magnet')
            THEN 'data_risk'
          WHEN top_product_family = 'Unknown / historical Stripe'
            THEN 'product_cleanup'
          WHEN credited_closer_source = 'unassigned'
            OR credited_closer_confidence IN ('low', 'missing')
            THEN 'attribution_gap'
          WHEN payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
            THEN 'payment_plan_review'
          ELSE 'open_customer'
        END AS action_bucket,
        CASE
          WHEN revenue_funnel_quality_flag = 'negative_net_revenue'
            THEN 'Data risk'
          WHEN revenue_funnel_quality_flag IN ('missing_taxonomy', 'uncategorized_offer_type', 'contact_not_matched', 'no_known_magnet')
            THEN 'Data risk'
          WHEN top_product_family = 'Unknown / historical Stripe'
            THEN 'Product cleanup'
          WHEN credited_closer_source = 'unassigned'
            OR credited_closer_confidence IN ('low', 'missing')
            THEN 'Attribution gap'
          WHEN payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
            THEN 'Payment-plan review'
          ELSE 'Open customer'
        END AS action_bucket_label,
        CASE
          WHEN revenue_funnel_quality_flag = 'negative_net_revenue'
            THEN 'Negative net revenue'
          WHEN revenue_funnel_quality_flag = 'contact_not_matched'
            THEN 'Buyer is not matched to a clean GHL contact'
          WHEN revenue_funnel_quality_flag = 'missing_taxonomy'
            THEN 'Lead magnet taxonomy is missing'
          WHEN revenue_funnel_quality_flag = 'uncategorized_offer_type'
            THEN 'Lead magnet offer type is uncategorized'
          WHEN revenue_funnel_quality_flag = 'no_known_magnet'
            THEN 'No known magnet before purchase'
          WHEN top_product_family = 'Unknown / historical Stripe'
            THEN 'Unknown historical Stripe product'
          WHEN credited_closer_source = 'unassigned'
            THEN 'No closer credited'
          WHEN credited_closer_confidence IN ('low', 'missing')
            THEN 'Low-confidence closer attribution'
          WHEN payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
            THEN 'Payment plan truth needs review'
          ELSE 'Open customer'
        END AS action_reason,
        CASE
          WHEN revenue_funnel_quality_flag = 'negative_net_revenue' THEN 1
          WHEN revenue_funnel_quality_flag IN ('missing_taxonomy', 'uncategorized_offer_type', 'contact_not_matched', 'no_known_magnet') THEN 2
          WHEN top_product_family = 'Unknown / historical Stripe' THEN 3
          WHEN credited_closer_source = 'unassigned' THEN 4
          WHEN credited_closer_confidence IN ('low', 'missing') THEN 5
          WHEN payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only') THEN 6
          ELSE 9
        END AS action_priority,
        CASE
          WHEN revenue_funnel_quality_flag = 'negative_net_revenue'
            THEN 'Review refund / chargeback'
          WHEN revenue_funnel_quality_flag IN ('missing_taxonomy', 'uncategorized_offer_type', 'contact_not_matched', 'no_known_magnet')
            THEN 'Repair source data'
          WHEN top_product_family = 'Unknown / historical Stripe'
            THEN 'Repair product'
          WHEN credited_closer_source = 'unassigned'
            THEN 'Find revenue credit'
          WHEN credited_closer_confidence IN ('low', 'missing')
            THEN 'Confirm revenue credit'
          WHEN payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
            THEN 'Review payment plan truth'
          ELSE 'Open customer'
        END AS revenue_next_action
      FROM ${revenueTable}
      WHERE
        (
          revenue_funnel_quality_flag != 'clean'
          OR credited_closer_source = 'unassigned'
          OR credited_closer_confidence IN ('low', 'missing')
          OR top_product_family = 'Unknown / historical Stripe'
          OR payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
        )
        ${andTimeRange(timeRange, "first_purchase_at")}
    ),
    action_queue AS (
      SELECT
        classified.*,
        COALESCE(latest_reviews.review_status, 'open') AS review_status,
        latest_reviews.review_note,
        latest_reviews.reviewed_by,
        latest_reviews.reviewed_label,
        latest_reviews.reviewed_at,
        latest_reviews.expires_at,
        CASE
          WHEN latest_reviews.review_status IN ('fixed', 'wont_fix')
            AND (latest_reviews.expires_at IS NULL OR TIMESTAMP(latest_reviews.expires_at) > CURRENT_TIMESTAMP())
            THEN FALSE
          ELSE TRUE
        END AS is_action_open
      FROM classified
      LEFT JOIN latest_reviews
        ON latest_reviews.contact_sk = classified.contact_sk
        AND latest_reviews.action_bucket = classified.action_bucket
    )
  `;

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
        SUM(fanbasis_auto_renew_payments_count) AS fanbasis_auto_renew_payments,
        COUNTIF(payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')) AS cash_only_plan_signal_buyers,
        COUNTIF(fanbasis_unreleased_payments_count > 0) AS buyers_with_unreleased_fanbasis_payments,
        SUM(fanbasis_unreleased_payments_count) AS fanbasis_unreleased_payments,
        COUNTIF(has_booking_before_first_purchase) AS buyers_with_booking_before_purchase,
        SAFE_DIVIDE(COUNTIF(has_booking_before_first_purchase), NULLIF(COUNT(*), 0)) AS booking_before_purchase_rate,
        COUNTIF(has_latest_prior_magnet_before_first_purchase) AS buyers_with_latest_prior_magnet,
        SAFE_DIVIDE(COUNTIF(has_latest_prior_magnet_before_first_purchase), NULLIF(COUNT(*), 0)) AS latest_prior_magnet_buyer_coverage,
        COUNTIF(purchase_magnet_attribution_flag = 'purchase_before_first_magnet') AS buyers_purchase_before_first_magnet,
        COUNTIF(purchase_magnet_attribution_flag = 'no_known_magnet') AS buyers_no_known_magnet,
        COUNTIF(credited_closer_source != 'unassigned' AND credited_closer_confidence IN ('high', 'medium')) AS credited_closer_buyers,
        SAFE_DIVIDE(COUNTIF(credited_closer_source != 'unassigned' AND credited_closer_confidence IN ('high', 'medium')), NULLIF(COUNT(*), 0)) AS credited_closer_rate,
        COUNTIF(credited_closer_source = 'unassigned' OR credited_closer_confidence IN ('low', 'missing')) AS unassigned_operator_buyers,
        SAFE_DIVIDE(COUNTIF(credited_closer_source = 'unassigned' OR credited_closer_confidence IN ('low', 'missing')), NULLIF(COUNT(*), 0)) AS unassigned_operator_rate,
        COUNTIF(credited_closer_source = 'unassigned') AS true_unassigned_operator_buyers,
        COUNTIF(credited_closer_source != 'unassigned' AND credited_closer_confidence = 'low') AS low_confidence_credit_buyers,
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
    revenue_funnel_payment_truth: `
      SELECT
        payment_plan_truth_status,
        CASE payment_plan_truth_status
          WHEN 'fanbasis_auto_renew_cash_only' THEN 'Fanbasis auto-renew cash'
          WHEN 'name_inferred_plan_cash_only' THEN 'Name-inferred plan cash'
          WHEN 'fanbasis_single_payment_cash' THEN 'Fanbasis single-payment cash'
          WHEN 'historical_stripe_cash_only' THEN 'Historical Stripe cash'
          ELSE INITCAP(REPLACE(payment_plan_truth_status, '_', ' '))
        END AS truth_label,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        SUM(fanbasis_net_revenue_after_refunds) AS fanbasis_net_revenue_after_refunds,
        SUM(fanbasis_auto_renew_payments_count) AS fanbasis_auto_renew_payments,
        SUM(fanbasis_unreleased_payments_count) AS fanbasis_unreleased_payments,
        SAFE_DIVIDE(SUM(total_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS revenue_per_buyer
      FROM ${revenueTable}
      ${buyerWhere}
      GROUP BY payment_plan_truth_status, truth_label
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
      WITH credit_rows AS (
        SELECT
          credited_closer_source AS revenue_credit_source,
          CASE credited_closer_source
            WHEN 'latest_prior_opportunity_closer' THEN 'Latest prior opportunity closer'
            WHEN 'fathom_contact_email_revenue_call_recorder' THEN 'Fathom matched buyer email'
            WHEN 'fathom_revenue_call_recorder' THEN 'Fathom sales call'
            WHEN 'latest_booking_closer' THEN 'Latest booking closer'
            WHEN 'fathom_transcript_self_intro_closer' THEN 'Fathom transcript self-intro'
            WHEN 'latest_booking_event_name_closer' THEN 'Latest booking title'
            WHEN 'latest_booking_calendly_host_closer' THEN 'Latest booking host'
            WHEN 'latest_prior_opportunity_owner' THEN 'Latest prior opportunity owner'
            WHEN 'calendly_host_for_fathom_team_account' THEN 'Calendly host for Fathom team'
            WHEN 'fathom_contact_email_team_account' THEN 'Fathom team account'
            WHEN 'fathom_contact_email_recorder' THEN 'Fathom matched buyer email'
            WHEN 'fathom_call_recorder' THEN 'Fathom call recorder'
            WHEN 'latest_booking_owner' THEN 'Latest booking owner'
            WHEN 'latest_booking_event_name_operator' THEN 'Latest booking title'
            WHEN 'latest_booking_calendly_host' THEN 'Latest booking host account'
            WHEN 'unassigned' THEN 'Unassigned'
            ELSE INITCAP(REPLACE(credited_closer_source, '_', ' '))
          END AS revenue_credit_source_label,
          CASE
            WHEN LOWER(credited_closer_name) IN ('aariz menon', 'ayaan menon')
              THEN 'Ayaan Menon'
            ELSE credited_closer_name
          END AS revenue_credit_name,
          credited_closer_role AS revenue_credit_role,
          credited_closer_confidence AS revenue_credit_confidence,
          latest_booking_scheduled_for,
          total_net_revenue_after_refunds
        FROM ${revenueTable}
        ${buyerWhere}
      )
      SELECT
        revenue_credit_source,
        revenue_credit_source_label,
        revenue_credit_name,
        revenue_credit_role,
        revenue_credit_confidence,
        COUNT(*) AS buyers,
        SUM(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
        COUNTIF(latest_booking_scheduled_for < TIMESTAMP('2026-01-01')) AS historical_booking_buyers,
        COUNTIF(latest_booking_scheduled_for >= TIMESTAMP('2026-01-01')) AS current_booking_buyers,
        CASE
          WHEN revenue_credit_confidence IN ('high', 'medium')
            THEN 'Person-level evidence'
          WHEN revenue_credit_source = 'latest_booking_calendly_host'
            AND LOWER(revenue_credit_name) = 'mind of dee calendar'
            THEN FORMAT(
              '%d historical pre-2026 bookings; %d current bookings need Manny/Mind of Dee human mapping',
              COUNTIF(latest_booking_scheduled_for < TIMESTAMP('2026-01-01')),
              COUNTIF(latest_booking_scheduled_for >= TIMESTAMP('2026-01-01'))
            )
          WHEN revenue_credit_confidence = 'low'
            THEN 'Known account, not a confirmed human closer'
          WHEN revenue_credit_source = 'unassigned'
            THEN 'No usable closer, booking-host, Fathom, or touch identity yet'
          ELSE 'Needs review'
        END AS revenue_credit_review_note
      FROM credit_rows
      GROUP BY revenue_credit_source, revenue_credit_source_label, revenue_credit_name, revenue_credit_role, revenue_credit_confidence
      ORDER BY buyers DESC, total_net_revenue_after_refunds DESC
      LIMIT 20
    `,
    revenue_funnel_customer_worklist: `
      ${revenueQueueBase}
      SELECT *
      FROM action_queue
      WHERE is_action_open
      ORDER BY
        action_priority,
        total_net_revenue_after_refunds DESC
      LIMIT 40
    `,
    revenue_funnel_action_queue_summary: `
      ${revenueQueueBase}
      SELECT
        action_bucket,
        action_bucket_label,
        MIN(action_priority) AS action_priority,
        COUNTIF(is_action_open) AS buyers,
        COUNTIF(review_status = 'reviewed') AS reviewed_buyers,
        COUNTIF(NOT is_action_open) AS closed_buyers,
        SUM(IF(is_action_open, total_net_revenue_after_refunds, 0)) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(
          SUM(IF(is_action_open, total_net_revenue_after_refunds, 0)),
          NULLIF(COUNTIF(is_action_open), 0)
        ) AS net_revenue_per_buyer,
        COUNTIF(is_action_open AND credited_closer_source = 'unassigned') AS unassigned_credit_buyers,
        COUNTIF(is_action_open AND credited_closer_confidence IN ('low', 'missing')) AS low_confidence_credit_buyers
      FROM action_queue
      GROUP BY action_bucket, action_bucket_label
      ORDER BY action_priority, total_net_revenue_after_refunds DESC
    `,
    revenue_funnel_top_value_gaps: `
      ${revenueQueueBase}
      SELECT *
      FROM action_queue
      WHERE is_action_open
      ORDER BY ABS(total_net_revenue_after_refunds) DESC, action_priority
      LIMIT 10
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
    customer_retention_summary: `
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at)) AS refreshed_at,
        COUNT(DISTINCT contact_sk) AS customers,
        COUNT(DISTINCT IF(is_repeat_paid_month, contact_sk, NULL)) AS repeat_paid_customers,
        SAFE_DIVIDE(
          COUNT(DISTINCT IF(is_repeat_paid_month, contact_sk, NULL)),
          NULLIF(COUNT(DISTINCT contact_sk), 0)
        ) AS repeat_paid_customer_rate,
        SUM(IF(is_repeat_paid_month, net_revenue_after_refunds_in_month, 0)) AS repeat_paid_net_revenue,
        SUM(net_revenue_after_refunds_in_month) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(
          SUM(net_revenue_after_refunds_in_month),
          NULLIF(COUNT(DISTINCT contact_sk), 0)
        ) AS customer_ltv,
        COUNT(DISTINCT IF(has_active_fanbasis_subscription_now, contact_sk, NULL)) AS active_fanbasis_subscription_customers,
        COUNT(DISTINCT IF(customer_lifecycle_status = 'fanbasis_transaction_no_subscriber_record', contact_sk, NULL)) AS fanbasis_cash_no_subscriber_customers,
        COUNT(DISTINCT IF(retention_quality_flag != 'clean', contact_sk, NULL)) AS retention_quality_gap_customers
      FROM ${retentionTable}
      ${buyerWhere}
    `,
    customer_retention_states: `
      SELECT
        retention_state,
        CASE retention_state
          WHEN 'new_paid_month' THEN 'First purchase month'
          WHEN 'repeat_paid_month' THEN 'Repeat paid month'
          WHEN 'refund_only_month' THEN 'Refund-only month'
          WHEN 'active_subscriber_current_month_no_payment' THEN 'Active now, no payment yet'
          WHEN 'post_latest_payment_month' THEN 'After latest payment'
          WHEN 'observed_gap_month' THEN 'Observed gap month'
          ELSE INITCAP(REPLACE(retention_state, '_', ' '))
        END AS retention_state_label,
        COUNT(*) AS customer_months,
        COUNT(DISTINCT contact_sk) AS customers,
        SUM(net_revenue_after_refunds_in_month) AS net_revenue_after_refunds
      FROM ${retentionTable}
      ${buyerWhere}
      GROUP BY retention_state, retention_state_label
      ORDER BY customer_months DESC
    `,
    revenue_identity_health: `
      WITH
      missing_contacts AS (
        SELECT
          COUNT(*) AS missing_ghl_contacts,
          COUNTIF(recommended_action = 'create_ghl_contact') AS contacts_to_create,
          COUNTIF(recommended_action = 'repair_identity_bridge') AS contacts_to_repair,
          COUNTIF(recommended_action = 'review_duplicate_ghl_contacts') AS duplicate_contact_reviews
        FROM ${missingContactsTable}
      ),
      payment_bridge AS (
        SELECT
          COUNTIF(is_paid) AS paid_payment_rows,
          COUNTIF(is_paid AND bridge_status NOT IN ('matched', 'payment_identity_only')) AS non_matched_paid_rows,
          COUNTIF(is_paid AND source_platform = 'fanbasis' AND bridge_status NOT IN ('matched', 'payment_identity_only')) AS fanbasis_non_matched_paid_rows,
          COUNTIF(is_paid AND source_platform = 'stripe' AND bridge_status NOT IN ('matched', 'payment_identity_only')) AS stripe_non_matched_paid_rows,
          COUNTIF(
            is_paid
            AND contact_sk IS NULL
            AND NOT (source_platform = 'stripe' AND bridge_status = 'payment_identity_only')
          ) AS paid_contact_null_rows,
          COUNTIF(is_paid AND bridge_status = 'payment_identity_only') AS payment_identity_only_paid_rows,
          COUNTIF(is_paid AND source_platform = 'stripe' AND bridge_status = 'payment_identity_only') AS historical_stripe_payment_identity_only_rows,
          COUNTIF(is_paid AND source_platform = 'fanbasis' AND bridge_status = 'payment_identity_only') AS fanbasis_payment_identity_only_rows,
          SUM(IF(is_paid AND source_platform = 'stripe' AND bridge_status = 'payment_identity_only', net_amount, 0)) AS historical_stripe_payment_identity_only_net_revenue
        FROM ${paymentsTable}
      ),
      buyer_quality AS (
        SELECT
          COUNT(*) AS buyer_rows,
          COUNTIF(revenue_funnel_quality_flag = 'contact_not_matched') AS contact_not_matched_buyers,
          COUNTIF(revenue_funnel_quality_flag != 'clean') AS quality_flag_buyers
        FROM ${revenueTable}
      ),
      fanbasis_depth AS (
        SELECT
          FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)) AS fanbasis_identity_refreshed_at,
          COUNTIF(object_type = 'customers') AS fanbasis_customer_rows,
          COUNTIF(object_type = 'subscribers') AS fanbasis_subscriber_rows
        FROM ${fanbasisObjectsTable}
        WHERE object_type IN ('customers', 'subscribers')
      ),
      latest_repair_run AS (
        SELECT
          run_id AS latest_contact_sync_run_id,
          LOGICAL_OR(dry_run) AS latest_contact_sync_was_dry_run,
          FORMAT_TIMESTAMP('%FT%TZ', MAX(processed_at)) AS latest_contact_sync_at,
          COUNTIF(status = 'success') AS latest_contact_sync_success_rows,
          COUNTIF(status = 'failed') AS latest_contact_sync_failed_rows,
          COUNTIF(status = 'skipped') AS latest_contact_sync_skipped_rows,
          COUNTIF(action_taken = 'created_contact' AND status = 'success') AS latest_created_contacts,
          COUNTIF(action_taken = 'linked_existing_contact' AND status = 'success') AS latest_linked_contacts
        FROM ${contactSyncAuditTable}
        GROUP BY run_id
        ORDER BY MAX(processed_at) DESC
        LIMIT 1
      ),
      latest_repair_safe AS (
        SELECT * FROM latest_repair_run
        UNION ALL
        SELECT
          CAST(NULL AS STRING) AS latest_contact_sync_run_id,
          CAST(NULL AS BOOL) AS latest_contact_sync_was_dry_run,
          CAST(NULL AS STRING) AS latest_contact_sync_at,
          0 AS latest_contact_sync_success_rows,
          0 AS latest_contact_sync_failed_rows,
          0 AS latest_contact_sync_skipped_rows,
          0 AS latest_created_contacts,
          0 AS latest_linked_contacts
        FROM (SELECT 1)
        WHERE NOT EXISTS (SELECT 1 FROM latest_repair_run)
      )
      SELECT
        missing_contacts.*,
        payment_bridge.*,
        buyer_quality.*,
        fanbasis_depth.*,
        latest_repair_safe.*,
        CASE
          WHEN missing_ghl_contacts = 0
            AND non_matched_paid_rows = 0
            AND paid_contact_null_rows = 0
            AND fanbasis_payment_identity_only_rows = 0
            AND contact_not_matched_buyers = 0
            AND latest_contact_sync_failed_rows = 0
            THEN 'clean'
          ELSE 'needs_attention'
        END AS identity_health_status
      FROM missing_contacts
      CROSS JOIN payment_bridge
      CROSS JOIN buyer_quality
      CROSS JOIN fanbasis_depth
      CROSS JOIN latest_repair_safe
    `,
    revenue_canceled_recovery_summary: `
      SELECT
        COUNT(*) AS canceled_bookings,
        COUNTIF(cancelled_by_type = 'host') AS host_canceled_bookings,
        COUNTIF(cancelled_by_type = 'invitee') AS invitee_canceled_bookings,
        COUNTIF(contact_sk IS NULL) AS canceled_without_contact,
        COUNTIF(has_rebooked_after_cancel) AS rebooked_after_cancel,
        SAFE_DIVIDE(COUNTIF(has_rebooked_after_cancel), NULLIF(COUNT(*), 0)) AS rebook_rate_after_cancel,
        COUNTIF(has_likely_show_after_cancel) AS likely_show_after_cancel,
        SAFE_DIVIDE(
          COUNTIF(has_likely_show_after_cancel),
          NULLIF(COUNTIF(has_rebooked_after_cancel), 0)
        ) AS likely_show_rate_after_rebook,
        COUNTIF(has_fathom_show_evidence) AS fathom_show_evidence_after_cancel,
        COUNTIF(next_active_marked_no_show) AS no_show_after_rebook,
        COUNT(DISTINCT IF(had_purchase_after_cancel, contact_sk, NULL)) AS buyers_after_cancel,
        SUM(credited_net_revenue_after_first_cancel) AS net_revenue_after_first_cancel,
        APPROX_QUANTILES(hours_to_rebook, 100 IGNORE NULLS)[SAFE_OFFSET(50)] AS median_hours_to_rebook
      FROM ${canceledRecoveryTable}
    `,
    revenue_canceled_recovery_by_actor: `
      SELECT
        COALESCE(cancelled_by_type, 'unknown') AS cancel_actor_type,
        CASE COALESCE(cancelled_by_type, 'unknown')
          WHEN 'host' THEN 'Host / triager'
          WHEN 'invitee' THEN 'Invitee'
          ELSE 'Unknown'
        END AS cancel_actor_label,
        COUNT(*) AS canceled_bookings,
        COUNTIF(has_rebooked_after_cancel) AS rebooked_after_cancel,
        SAFE_DIVIDE(COUNTIF(has_rebooked_after_cancel), NULLIF(COUNT(*), 0)) AS rebook_rate_after_cancel,
        COUNTIF(has_likely_show_after_cancel) AS likely_show_after_cancel,
        COUNTIF(has_fathom_show_evidence) AS fathom_show_evidence_after_cancel,
        COUNTIF(next_active_marked_no_show) AS no_show_after_rebook,
        COUNT(DISTINCT IF(had_purchase_after_cancel, contact_sk, NULL)) AS buyers_after_cancel,
        SUM(credited_net_revenue_after_first_cancel) AS net_revenue_after_first_cancel
      FROM ${canceledRecoveryTable}
      GROUP BY cancel_actor_type, cancel_actor_label
      ORDER BY canceled_bookings DESC
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
      paymentTruth,
      productFamilies,
      paths,
      magnets,
      operatorDiagnostic,
      customerWorklist,
      actionQueueSummary,
      topValueGaps,
      quality,
      retentionSummary,
      retentionStates,
      identityHealth,
      canceledRecoverySummary,
      canceledRecoveryByActor,
    ] = await Promise.all([
      runBigQuery(revenueFunnelQueries.revenue_funnel_summary),
      runBigQuery(revenueFunnelQueries.revenue_funnel_payment_plans),
      runBigQuery(revenueFunnelQueries.revenue_funnel_payment_truth),
      runBigQuery(revenueFunnelQueries.revenue_funnel_product_families),
      runBigQuery(revenueFunnelQueries.revenue_funnel_paths),
      runBigQuery(revenueFunnelQueries.revenue_funnel_magnets),
      runBigQuery(revenueFunnelQueries.revenue_funnel_operator_diagnostic),
      runBigQuery(revenueFunnelQueries.revenue_funnel_customer_worklist),
      runBigQuery(revenueFunnelQueries.revenue_funnel_action_queue_summary),
      runBigQuery(revenueFunnelQueries.revenue_funnel_top_value_gaps),
      runBigQuery(revenueFunnelQueries.revenue_funnel_quality),
      runBigQuery(revenueFunnelQueries.customer_retention_summary),
      runBigQuery(revenueFunnelQueries.customer_retention_states),
      runBigQuery(revenueFunnelQueries.revenue_identity_health),
      runBigQuery(revenueFunnelQueries.revenue_canceled_recovery_summary),
      runBigQuery(revenueFunnelQueries.revenue_canceled_recovery_by_actor),
    ]);

    return {
      rows: {
        revenue_funnel_summary: summary,
        revenue_funnel_payment_plans: paymentPlans,
        revenue_funnel_payment_truth: paymentTruth,
        revenue_funnel_product_families: productFamilies,
        revenue_funnel_paths: paths,
        revenue_funnel_magnets: magnets,
        revenue_funnel_operator_diagnostic: operatorDiagnostic,
        revenue_funnel_customer_worklist: customerWorklist,
        revenue_funnel_action_queue_summary: actionQueueSummary,
        revenue_funnel_top_value_gaps: topValueGaps,
        revenue_funnel_quality: quality,
        customer_retention_summary: retentionSummary,
        customer_retention_states: retentionStates,
        revenue_identity_health: identityHealth,
        revenue_canceled_recovery_summary: canceledRecoverySummary,
        revenue_canceled_recovery_by_actor: canceledRecoveryByActor,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["revenue_funnel_detail", "customer_retention_detail", "operator_action_reviews"],
        note:
          "Revenue Funnel is buyer-journey grain. Customer Retention is customer-month grain. Together they cover money, product, payment-plan, magnet, booking-path, repeat-payment, and Fanbasis lifecycle evidence; remaining balances are not inferred.",
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
        tables: ["revenue_funnel_detail", "customer_retention_detail", "operator_action_reviews"],
        note:
          "Revenue Funnel is buyer-journey grain. Customer Retention is customer-month grain. Together they cover money, product, payment-plan, magnet, booking-path, repeat-payment, and Fanbasis lifecycle evidence; remaining balances are not inferred.",
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
