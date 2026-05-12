import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

export const RETENTION_TIME_RANGE_OPTIONS = [
  {
    value: "30d",
    label: "30D",
    description: "Customers whose first purchase landed in the last 30 days.",
  },
  {
    value: "90d",
    label: "90D",
    description: "Customers whose first purchase landed in the last 90 days.",
  },
  {
    value: "180d",
    label: "180D",
    description: "Customers whose first purchase landed in the last 180 days.",
  },
  {
    value: "all",
    label: "All",
    description: "All matched paid customers in the retention mart.",
  },
] as const;

export type RetentionTimeRange = (typeof RETENTION_TIME_RANGE_OPTIONS)[number]["value"];

export const RETENTION_WORKLIST_OPTIONS = [
  {
    value: "recovery_queue",
    label: "Recovery",
    description: "Failed plans plus active plans that are due with no observed payment.",
  },
  {
    value: "failed_plan_recovery_needed",
    label: "Failed Plans",
    description: "Fanbasis subscribers currently showing failed subscription evidence.",
  },
  {
    value: "active_plan_due_no_payment_yet",
    label: "Due Now",
    description: "Active Fanbasis plans whose expected next payment date has arrived.",
  },
  {
    value: "one_time_upsell_candidate",
    label: "One-Time Upsell",
    description: "One-time Fanbasis buyers who have not produced repeat paid months yet.",
  },
  {
    value: "completed_plan_paid_off",
    label: "Paid Off",
    description: "Completed Fanbasis plans ready for customer-success or upsell review.",
  },
  {
    value: "historical_stripe_product_review",
    label: "Product Repair",
    description: "Historical Stripe customers whose product family still needs source repair.",
  },
  {
    value: "active_plan_not_yet_due",
    label: "Watchlist",
    description: "Active Fanbasis plans that are not due yet but should stay visible.",
  },
  {
    value: "repeat_payment_observed",
    label: "Repeat Paid",
    description: "Customers with repeat cash already observed; monitor, do not chase.",
  },
  {
    value: "manual_collections",
    label: "Collections",
    description: "Manual collection patterns: stale repeat payments, collection calls without repeat cash, and plan-named cash.",
  },
  {
    value: "manual_collection_stale_review",
    label: "Stale Manual",
    description: "Manual payment-plan collection was observed, but the latest post-first payment is aging.",
  },
  {
    value: "collection_call_no_payment_review",
    label: "Call No Pay",
    description: "A post-first-payment collection/checkup call exists, but no follow-up payment was observed.",
  },
  {
    value: "contract_terms_review",
    label: "Contract Terms",
    description: "Customers with sales-call transcript payment terms that need human confirmation against collected cash.",
  },
] as const;

export type RetentionWorklist = (typeof RETENTION_WORKLIST_OPTIONS)[number]["value"];

type GetRetentionDataOptions = {
  timeRange?: string | null;
  worklist?: string | null;
};

const DEFAULT_TIME_RANGE: RetentionTimeRange = "all";
const DEFAULT_WORKLIST: RetentionWorklist = "recovery_queue";
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function retentionDataset() {
  const dataset =
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for retention: ${dataset}`);
  }

  return dataset;
}

function tableRef() {
  return `\`${deeConfig.bigQuery.projectId}.${retentionDataset()}.customer_retention_detail\``;
}

function martTableRef(tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.${retentionDataset()}.${tableName}\``;
}

export function normalizeRetentionTimeRange(value: string | null | undefined): RetentionTimeRange {
  const normalized = value?.toLowerCase();
  const option = RETENTION_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);

  return option?.value ?? DEFAULT_TIME_RANGE;
}

export function normalizeRetentionWorklist(value: string | null | undefined): RetentionWorklist {
  const normalized = value?.toLowerCase();
  const option = RETENTION_WORKLIST_OPTIONS.find((candidate) => candidate.value === normalized);

  return option?.value ?? DEFAULT_WORKLIST;
}

function buildDashboardFilters(timeRange: RetentionTimeRange, worklist: RetentionWorklist): DashboardFilters {
  const activeOption = RETENTION_TIME_RANGE_OPTIONS.find((option) => option.value === timeRange);
  const activeWorklist = RETENTION_WORKLIST_OPTIONS.find((option) => option.value === worklist);

  return {
    timeRange,
    timeRangeLabel: activeOption?.label ?? "All",
    timeRangeDescription: activeOption?.description ?? "All matched paid customers in the retention mart.",
    timeRangeOptions: RETENTION_TIME_RANGE_OPTIONS.map((option) => ({ ...option })),
    worklist,
    worklistLabel: activeWorklist?.label ?? "Recovery",
    worklistDescription: activeWorklist?.description ?? "Failed plans plus active plans that are due with no observed payment.",
    worklistOptions: RETENTION_WORKLIST_OPTIONS.map((option) => ({ ...option })),
  };
}

function timestampRangePredicate(timeRange: RetentionTimeRange, field: string) {
  if (timeRange === "all") return "";

  const days = timeRange === "30d" ? 29 : timeRange === "90d" ? 89 : 179;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function whereTimeRange(timeRange: RetentionTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `WHERE ${predicate}` : "";
}

function worklistPredicate(worklist: RetentionWorklist) {
  if (worklist === "recovery_queue") {
    return "customer_latest.payment_plan_health_status IN ('failed_plan_recovery_needed', 'active_plan_due_no_payment_yet')";
  }

  if (worklist === "manual_collections") {
    return "customer_latest.collection_health_status IN ('manual_collection_stale_review', 'collection_call_no_payment_review', 'plan_named_collection_review', 'repeat_or_upsell_review')";
  }

  if (
    [
      "manual_collection_stale_review",
      "collection_call_no_payment_review",
    ].includes(worklist)
  ) {
    return `customer_latest.collection_health_status = '${worklist}'`;
  }

  if (worklist === "contract_terms_review") {
    return "contract_evidence_latest.contract_evidence_status = 'transcript_payment_terms_found'";
  }

  return `customer_latest.payment_plan_health_status = '${worklist}'`;
}

function buildRetentionQueries(timeRange: RetentionTimeRange, worklist: RetentionWorklist) {
  const retentionTable = tableRef();
  const contractEvidenceTable = martTableRef("collection_contract_evidence_detail");
  const contractTermsReviewsTable = martTableRef("contract_terms_reviews");
  const operatorActionReviewsTable = martTableRef("operator_action_reviews");
  const cohortWhere = whereTimeRange(timeRange, "first_purchase_at");

  const customerLatest = `
    customer_latest AS (
      SELECT *
      FROM ${retentionTable}
      ${cohortWhere}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY activity_month DESC
      ) = 1
    )
  `;

  const customerRepeat = `
    customer_repeat AS (
      SELECT
        contact_sk,
        COUNTIF(is_repeat_paid_month) AS repeat_paid_months,
        SUM(IF(is_repeat_paid_month, net_revenue_after_refunds_in_month, 0)) AS repeat_paid_net_revenue
      FROM ${retentionTable}
      ${cohortWhere}
      GROUP BY contact_sk
    )
  `;

  const contractEvidenceLatest = `
    contract_evidence_latest AS (
      SELECT *
      FROM ${contractEvidenceTable}
      ${cohortWhere}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY mart_refreshed_at DESC
      ) = 1
    )
  `;

  const contractTermsLatest = `
    contract_terms_latest AS (
      SELECT
        contact_sk,
        contact_id,
        review_status,
        promised_contract_value,
        upfront_agreed_amount,
        balance_expected_amount,
        review_confidence,
        terms_source_note,
        reviewed_by,
        FORMAT_TIMESTAMP('%b %e, %Y', reviewed_at, 'America/New_York') AS reviewed_label,
        FORMAT_TIMESTAMP('%FT%TZ', reviewed_at) AS reviewed_at
      FROM ${contractTermsReviewsTable}
      WHERE review_status = 'confirmed'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY reviewed_at DESC
      ) = 1
    )
  `;

  const latestReviews = `
    latest_reviews AS (
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
        FROM ${operatorActionReviewsTable}
        WHERE
          area = 'retention'
          AND queue_name = 'retention_worklist'
          AND entity_type = 'customer'
          AND LOWER(review_status) IN ('open', 'reviewed', 'fixed', 'wont_fix')
      )
      WHERE review_rank = 1
    )
  `;

  return {
    retention_summary: `
      WITH
      ${customerLatest},
      ${contractTermsLatest}
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', MAX(mart_refreshed_at)) AS refreshed_at,
        COUNT(DISTINCT contact_sk) AS customers,
        COUNT(DISTINCT IF(is_repeat_paid_month, contact_sk, NULL)) AS repeat_paid_customers,
        SAFE_DIVIDE(
          COUNT(DISTINCT IF(is_repeat_paid_month, contact_sk, NULL)),
          NULLIF(COUNT(DISTINCT contact_sk), 0)
        ) AS repeat_paid_customer_rate,
        SUM(IF(is_first_purchase_month, net_revenue_after_refunds_in_month, 0)) AS first_purchase_month_net_revenue,
        SUM(IF(is_repeat_paid_month, net_revenue_after_refunds_in_month, 0)) AS repeat_paid_net_revenue,
        SUM(net_revenue_after_refunds_in_month) AS total_net_revenue_after_refunds,
        SAFE_DIVIDE(
          SUM(IF(is_repeat_paid_month, net_revenue_after_refunds_in_month, 0)),
          NULLIF(SUM(net_revenue_after_refunds_in_month), 0)
        ) AS repeat_paid_net_revenue_share,
        SAFE_DIVIDE(
          SUM(net_revenue_after_refunds_in_month),
          NULLIF(COUNT(DISTINCT contact_sk), 0)
        ) AS customer_ltv,
        COUNT(DISTINCT IF(has_active_fanbasis_subscription_now, contact_sk, NULL)) AS active_fanbasis_subscription_customers,
        COUNT(DISTINCT IF(is_current_month AND is_observed_retained_month, contact_sk, NULL)) AS observed_current_month_retained_customers,
        COUNT(DISTINCT IF(is_repeat_payment_eligible_now, contact_sk, NULL)) AS repeat_payment_eligible_customers,
        COUNT(DISTINCT IF(is_expected_payment_due_now, contact_sk, NULL)) AS expected_payment_due_customers,
        COUNT(DISTINCT IF(is_expected_payment_missed_now, contact_sk, NULL)) AS expected_payment_missed_customers,
        COUNT(DISTINCT IF(payment_plan_health_status = 'failed_plan_recovery_needed', contact_sk, NULL)) AS failed_plan_recovery_customers,
        COUNT(DISTINCT IF(payment_plan_health_status = 'active_plan_due_no_payment_yet', contact_sk, NULL)) AS active_plan_due_customers,
        COUNT(DISTINCT IF(payment_plan_health_status = 'completed_plan_paid_off', contact_sk, NULL)) AS completed_plan_customers,
        COUNT(DISTINCT IF(payment_plan_health_status = 'one_time_upsell_candidate', contact_sk, NULL)) AS one_time_upsell_customers,
        COUNT(DISTINCT IF(customer_lifecycle_status = 'fanbasis_transaction_no_subscriber_record', contact_sk, NULL)) AS fanbasis_cash_no_subscriber_customers,
        COUNT(DISTINCT IF(is_current_month AND collection_motion_type IN ('manual_payment_plan_collected', 'manual_collection_or_upsell'), contact_sk, NULL)) AS manual_collection_customers,
        COUNT(DISTINCT IF(is_current_month AND collection_health_status IN ('manual_collection_stale_review', 'collection_call_no_payment_review', 'plan_named_collection_review', 'repeat_or_upsell_review'), contact_sk, NULL)) AS manual_collection_review_customers,
        COUNT(DISTINCT IF(is_current_month AND collection_health_status = 'collection_call_no_payment_review', contact_sk, NULL)) AS collection_call_no_payment_customers,
        SUM(IF(is_current_month, post_first_collected_net_revenue, 0)) AS post_first_collected_net_revenue,
        (
          SELECT COUNT(*)
          FROM customer_latest
          INNER JOIN contract_terms_latest
            ON contract_terms_latest.contact_sk = customer_latest.contact_sk
        ) AS confirmed_contract_terms_customers,
        (
          SELECT SUM(contract_terms_latest.promised_contract_value)
          FROM customer_latest
          INNER JOIN contract_terms_latest
            ON contract_terms_latest.contact_sk = customer_latest.contact_sk
        ) AS confirmed_promised_contract_value,
        (
          SELECT SUM(contract_terms_latest.balance_expected_amount)
          FROM customer_latest
          INNER JOIN contract_terms_latest
            ON contract_terms_latest.contact_sk = customer_latest.contact_sk
        ) AS confirmed_balance_expected_amount,
        COUNT(DISTINCT IF(retention_quality_flag != 'clean', contact_sk, NULL)) AS retention_quality_gap_customers
      FROM ${retentionTable}
      ${cohortWhere}
    `,
    retention_states: `
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
      ${cohortWhere}
      GROUP BY retention_state, retention_state_label
      ORDER BY customer_months DESC
    `,
    retention_payment_health: `
      WITH
      ${customerLatest},
      ${latestReviews}
      SELECT
        payment_plan_health_status,
        CASE payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 'Failed plan recovery'
          WHEN 'active_plan_due_no_payment_yet' THEN 'Active plan due, no payment'
          WHEN 'active_plan_not_yet_due' THEN 'Active plan not yet due'
          WHEN 'active_plan_paid_current_month' THEN 'Active plan paid this month'
          WHEN 'completed_plan_paid_off' THEN 'Completed / paid off'
          WHEN 'repeat_payment_observed' THEN 'Repeat payment observed'
          WHEN 'one_time_upsell_candidate' THEN 'One-time upsell candidate'
          WHEN 'historical_stripe_product_review' THEN 'Historical Stripe product repair'
          WHEN 'review_negative_value' THEN 'Review negative value'
          WHEN 'no_repeat_expected_yet' THEN 'No repeat expected yet'
          ELSE INITCAP(REPLACE(payment_plan_health_status, '_', ' '))
        END AS health_label,
        CASE payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 'recover_failed_payment'
          WHEN 'active_plan_due_no_payment_yet' THEN 'collect_due_payment'
          WHEN 'active_plan_not_yet_due' THEN 'watch_next_due_date'
          WHEN 'active_plan_paid_current_month' THEN 'monitor_active_plan'
          WHEN 'completed_plan_paid_off' THEN 'upsell_completed_customer'
          WHEN 'one_time_upsell_candidate' THEN 'upsell_one_time_customer'
          WHEN 'historical_stripe_product_review' THEN 'repair_historical_product'
          WHEN 'review_negative_value' THEN 'review_refund_or_chargeback'
          WHEN 'repeat_payment_observed' THEN 'monitor_repeat_customer'
          ELSE 'monitor'
        END AS retention_operator_next_action,
        CASE payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 'Recover failed payment'
          WHEN 'active_plan_due_no_payment_yet' THEN 'Collect due payment'
          WHEN 'active_plan_not_yet_due' THEN 'Watch next due date'
          WHEN 'active_plan_paid_current_month' THEN 'Monitor active plan'
          WHEN 'completed_plan_paid_off' THEN 'Upsell completed customer'
          WHEN 'one_time_upsell_candidate' THEN 'Upsell one-time customer'
          WHEN 'historical_stripe_product_review' THEN 'Repair historical product'
          WHEN 'review_negative_value' THEN 'Review refund / chargeback'
          WHEN 'repeat_payment_observed' THEN 'Monitor repeat customer'
          ELSE 'Monitor'
        END AS next_action_label,
        COUNTIF(
          COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
          OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
        ) AS customers,
        COUNTIF(
          COALESCE(latest_reviews.review_status, 'open') IN ('fixed', 'wont_fix')
          AND (latest_reviews.expires_at IS NULL OR TIMESTAMP(latest_reviews.expires_at) > CURRENT_TIMESTAMP())
        ) AS closed_customers,
        SUM(IF(
          COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
          OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP()),
          lifetime_net_revenue_after_refunds,
          0
        )) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(
          SUM(IF(
            COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
            OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP()),
            lifetime_net_revenue_after_refunds,
            0
          )),
          NULLIF(COUNTIF(
            COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
            OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
          ), 0)
        ) AS customer_ltv,
        COUNTIF(
          is_repeat_payment_eligible_now
          AND (
            COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
            OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
          )
        ) AS repeat_payment_eligible_customers,
        COUNTIF(
          is_expected_payment_due_now
          AND (
            COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
            OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
          )
        ) AS expected_payment_due_customers,
        COUNTIF(
          is_expected_payment_missed_now
          AND (
            COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
            OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
          )
        ) AS expected_payment_missed_customers
      FROM customer_latest
      LEFT JOIN latest_reviews
        ON latest_reviews.contact_sk = customer_latest.contact_sk
        AND latest_reviews.action_bucket = customer_latest.payment_plan_health_status
      GROUP BY payment_plan_health_status, health_label, retention_operator_next_action, next_action_label
      ORDER BY
        CASE payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 1
          WHEN 'active_plan_due_no_payment_yet' THEN 2
          WHEN 'active_plan_not_yet_due' THEN 3
          WHEN 'completed_plan_paid_off' THEN 4
          WHEN 'one_time_upsell_candidate' THEN 5
          WHEN 'historical_stripe_product_review' THEN 6
          WHEN 'repeat_payment_observed' THEN 7
          ELSE 8
        END,
        customers DESC
    `,
    retention_repeat_payment_types: `
      WITH ${customerLatest}
      SELECT
        repeat_payment_type,
        CASE repeat_payment_type
          WHEN 'fanbasis_auto_renew_or_installment' THEN 'Fanbasis auto-renew / installment'
          WHEN 'fanbasis_subscription_installment' THEN 'Fanbasis subscription installment'
          WHEN 'multi_product_repeat_or_upsell' THEN 'Multi-product repeat / upsell'
          WHEN 'same_product_multi_payment' THEN 'Same-product multi-payment'
          WHEN 'active_subscription_no_repeat_paid_yet' THEN 'Active subscription, no repeat yet'
          WHEN 'failed_subscription_no_repeat_paid' THEN 'Failed subscription, no repeat'
          WHEN 'completed_subscription_no_repeat_paid' THEN 'Completed subscription, no repeat'
          WHEN 'historical_stripe_single_payment' THEN 'Historical Stripe single payment'
          WHEN 'single_payment_no_repeat' THEN 'Single payment, no repeat'
          ELSE INITCAP(REPLACE(repeat_payment_type, '_', ' '))
        END AS repeat_type_label,
        COUNT(*) AS customers,
        SUM(lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv,
        COUNTIF(is_repeat_payment_eligible_now) AS repeat_payment_eligible_customers,
        COUNTIF(is_expected_payment_due_now) AS expected_payment_due_customers,
        COUNTIF(is_expected_payment_missed_now) AS expected_payment_missed_customers
      FROM customer_latest
      GROUP BY repeat_payment_type, repeat_type_label
      ORDER BY customers DESC
    `,
    retention_collections: `
      WITH ${customerLatest}
      SELECT
        collection_health_status,
        CASE collection_health_status
          WHEN 'automated_plan_failed_recovery' THEN 'Automated plan failed'
          WHEN 'automated_plan_due_no_payment' THEN 'Automated plan due, no pay'
          WHEN 'automated_plan_monitor' THEN 'Automated plan monitor'
          WHEN 'repeat_or_upsell_review' THEN 'Repeat or upsell review'
          WHEN 'manual_collection_recently_collected' THEN 'Manual collection current'
          WHEN 'manual_collection_stale_review' THEN 'Manual collection stale'
          WHEN 'collection_call_no_payment_review' THEN 'Collection call, no pay'
          WHEN 'plan_named_collection_review' THEN 'Plan-named cash only'
          WHEN 'review_negative_value' THEN 'Review negative value'
          WHEN 'no_collection_signal' THEN 'No collection signal'
          ELSE INITCAP(REPLACE(collection_health_status, '_', ' '))
        END AS collection_health_label,
        collection_motion_type,
        CASE collection_motion_type
          WHEN 'automated_fanbasis_plan' THEN 'Automated Fanbasis plan'
          WHEN 'manual_collection_or_upsell' THEN 'Manual collection or upsell'
          WHEN 'manual_payment_plan_collected' THEN 'Manual payment plan collected'
          WHEN 'collection_call_scheduled_no_repeat_payment' THEN 'Collection call, no repeat payment'
          WHEN 'plan_named_cash_only' THEN 'Plan-named cash only'
          WHEN 'single_payment_no_collection_signal' THEN 'Single payment, no collection signal'
          ELSE INITCAP(REPLACE(collection_motion_type, '_', ' '))
        END AS collection_motion_label,
        COUNT(*) AS customers,
        SUM(upfront_collected_net_revenue) AS upfront_collected_net_revenue,
        SUM(post_first_collected_net_revenue) AS post_first_collected_net_revenue,
        SUM(lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(post_first_collected_net_revenue), NULLIF(SUM(lifetime_net_revenue_after_refunds), 0)) AS post_first_collected_net_revenue_share,
        COUNTIF(post_first_paid_payments_count > 0) AS post_first_paid_customers,
        COUNTIF(post_first_purchase_collection_bookings_count > 0) AS collection_booking_customers
      FROM customer_latest
      GROUP BY collection_health_status, collection_health_label, collection_motion_type, collection_motion_label
      ORDER BY
        CASE collection_health_status
          WHEN 'manual_collection_stale_review' THEN 1
          WHEN 'collection_call_no_payment_review' THEN 2
          WHEN 'plan_named_collection_review' THEN 3
          WHEN 'repeat_or_upsell_review' THEN 4
          WHEN 'automated_plan_failed_recovery' THEN 5
          WHEN 'automated_plan_due_no_payment' THEN 6
          WHEN 'manual_collection_recently_collected' THEN 7
          WHEN 'automated_plan_monitor' THEN 8
          ELSE 9
        END,
        post_first_collected_net_revenue DESC
    `,
    retention_contract_evidence: `
      WITH ${contractTermsLatest}
      SELECT
        COALESCE(evidence.top_product_family, 'Unknown') AS top_product_family,
        COALESCE(evidence.top_product_by_net_revenue, 'Unknown product') AS top_product_by_net_revenue,
        evidence.collection_health_status,
        CASE evidence.collection_health_status
          WHEN 'manual_collection_stale_review' THEN 'Manual collection stale'
          WHEN 'collection_call_no_payment_review' THEN 'Collection call, no pay'
          WHEN 'plan_named_collection_review' THEN 'Plan-named cash only'
          WHEN 'repeat_or_upsell_review' THEN 'Repeat or upsell review'
          WHEN 'manual_collection_recently_collected' THEN 'Manual collection current'
          WHEN 'automated_plan_due_no_payment' THEN 'Automated plan due, no pay'
          WHEN 'automated_plan_failed_recovery' THEN 'Automated plan failed'
          WHEN 'automated_plan_monitor' THEN 'Automated plan monitor'
          WHEN 'no_collection_signal' THEN 'No collection signal'
          ELSE INITCAP(REPLACE(evidence.collection_health_status, '_', ' '))
        END AS collection_health_label,
        COUNT(*) AS customers,
        SUM(evidence.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SUM(evidence.upfront_collected_net_revenue) AS upfront_collected_net_revenue,
        SUM(evidence.post_first_collected_net_revenue) AS post_first_collected_net_revenue,
        SAFE_DIVIDE(
          SUM(evidence.post_first_collected_net_revenue),
          NULLIF(SUM(evidence.lifetime_net_revenue_after_refunds), 0)
        ) AS post_first_collected_net_revenue_share,
        COUNTIF(evidence.post_first_collected_net_revenue > 0) AS post_first_cash_customers,
        COUNTIF(evidence.has_payment_terms_transcript_evidence) AS transcript_evidence_customers,
        COUNTIF(evidence.contract_evidence_status = 'sales_call_found_no_payment_terms') AS sales_call_no_terms_customers,
        COUNTIF(contract_terms_latest.contact_sk IS NOT NULL) AS confirmed_terms_customers,
        SUM(contract_terms_latest.promised_contract_value) AS confirmed_promised_contract_value,
        SUM(contract_terms_latest.balance_expected_amount) AS confirmed_balance_expected_amount,
        MAX(evidence.largest_mentioned_payment_amount) AS largest_mentioned_payment_amount
      FROM ${contractEvidenceTable} AS evidence
      LEFT JOIN contract_terms_latest
        ON contract_terms_latest.contact_sk = evidence.contact_sk
      ${cohortWhere}
      GROUP BY top_product_family, top_product_by_net_revenue, evidence.collection_health_status, collection_health_label
      ORDER BY post_first_collected_net_revenue DESC, lifetime_net_revenue_after_refunds DESC
      LIMIT 10
    `,
    retention_worklist: `
      WITH
      ${customerLatest},
      ${contractEvidenceLatest},
      ${contractTermsLatest},
      ${latestReviews}
      SELECT
        customer_latest.contact_sk,
        customer_latest.contact_id,
        CASE
          WHEN '${worklist}' = 'contract_terms_review'
            THEN 'contract_terms_review'
          WHEN customer_latest.retention_operator_next_action IN ('review_manual_collection', 'confirm_repeat_or_upsell', 'monitor_manual_collection')
            THEN customer_latest.collection_health_status
          ELSE customer_latest.payment_plan_health_status
        END AS action_bucket,
        COALESCE(NULLIF(customer_latest.contact_name, ''), NULLIF(customer_latest.email_norm, ''), NULLIF(customer_latest.phone, ''), 'Unknown customer') AS customer_display_name,
        customer_latest.email_norm,
        customer_latest.phone,
        customer_latest.payment_plan_health_status,
        CASE customer_latest.payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 'Failed plan recovery'
          WHEN 'active_plan_due_no_payment_yet' THEN 'Active plan due, no payment'
          WHEN 'active_plan_not_yet_due' THEN 'Active plan not yet due'
          WHEN 'active_plan_paid_current_month' THEN 'Active plan paid this month'
          WHEN 'completed_plan_paid_off' THEN 'Completed / paid off'
          WHEN 'repeat_payment_observed' THEN 'Repeat payment observed'
          WHEN 'one_time_upsell_candidate' THEN 'One-time upsell candidate'
          WHEN 'historical_stripe_product_review' THEN 'Historical Stripe product repair'
          WHEN 'review_negative_value' THEN 'Review negative value'
          WHEN 'no_repeat_expected_yet' THEN 'No repeat expected yet'
          ELSE INITCAP(REPLACE(customer_latest.payment_plan_health_status, '_', ' '))
        END AS health_label,
        CASE
          WHEN '${worklist}' = 'contract_terms_review'
            THEN 'review_contract_terms'
          ELSE customer_latest.retention_operator_next_action
        END AS retention_operator_next_action,
        CASE
          WHEN '${worklist}' = 'contract_terms_review'
            THEN 'Review contract terms'
          ELSE CASE customer_latest.retention_operator_next_action
          WHEN 'recover_failed_payment' THEN 'Recover failed payment'
          WHEN 'collect_due_payment' THEN 'Collect due payment'
          WHEN 'watch_next_due_date' THEN 'Watch next due date'
          WHEN 'monitor_active_plan' THEN 'Monitor active plan'
          WHEN 'upsell_completed_customer' THEN 'Upsell completed customer'
          WHEN 'upsell_one_time_customer' THEN 'Upsell one-time customer'
          WHEN 'repair_historical_product' THEN 'Repair historical product'
          WHEN 'review_refund_or_chargeback' THEN 'Review refund / chargeback'
          WHEN 'review_manual_collection' THEN 'Review manual collection'
          WHEN 'confirm_repeat_or_upsell' THEN 'Confirm repeat / upsell'
          WHEN 'monitor_manual_collection' THEN 'Monitor manual collection'
          WHEN 'monitor_repeat_customer' THEN 'Monitor repeat customer'
          ELSE 'Monitor'
          END
        END AS next_action_label,
        customer_latest.customer_lifecycle_status,
        CASE customer_latest.customer_lifecycle_status
          WHEN 'active_fanbasis_subscription' THEN 'Active Fanbasis'
          WHEN 'completed_fanbasis_subscription' THEN 'Completed Fanbasis'
          WHEN 'failed_fanbasis_subscription' THEN 'Failed Fanbasis'
          WHEN 'one_time_fanbasis_customer' THEN 'One-time Fanbasis'
          WHEN 'fanbasis_transaction_no_subscriber_record' THEN 'Fanbasis cash, no subscriber'
          WHEN 'historical_stripe_or_no_subscriber_record' THEN 'Historical Stripe / no subscriber'
          ELSE INITCAP(REPLACE(customer_latest.customer_lifecycle_status, '_', ' '))
        END AS lifecycle_label,
        customer_latest.repeat_payment_type,
        FORMAT_TIMESTAMP('%b %e, %Y', customer_latest.first_purchase_at, 'America/New_York') AS first_purchase_label,
        FORMAT_TIMESTAMP('%b %e, %Y', customer_latest.latest_purchase_at, 'America/New_York') AS latest_purchase_label,
        FORMAT_TIMESTAMP('%FT%TZ', customer_latest.first_purchase_at) AS first_purchase_at,
        FORMAT_TIMESTAMP('%FT%TZ', customer_latest.latest_purchase_at) AS latest_purchase_at,
        FORMAT_DATE('%b %e, %Y', customer_latest.expected_next_payment_date) AS expected_next_payment_label,
        customer_latest.days_until_expected_next_payment,
        customer_latest.days_past_expected_payment,
        customer_latest.is_expected_payment_due_now,
        customer_latest.is_expected_payment_missed_now,
        customer_latest.lifetime_net_revenue_after_refunds,
        customer_latest.upfront_collected_net_revenue,
        customer_latest.post_first_collected_net_revenue,
        customer_latest.post_first_collected_net_revenue_share,
        customer_latest.post_first_paid_payments_count,
        FORMAT_TIMESTAMP('%b %e, %Y', customer_latest.latest_post_first_payment_at, 'America/New_York') AS latest_post_first_payment_label,
        customer_latest.days_since_latest_post_first_payment,
        customer_latest.collection_motion_type,
        customer_latest.collection_health_status,
        CASE customer_latest.collection_health_status
          WHEN 'manual_collection_stale_review' THEN 'Manual collection stale'
          WHEN 'collection_call_no_payment_review' THEN 'Collection call, no pay'
          WHEN 'plan_named_collection_review' THEN 'Plan-named cash only'
          WHEN 'repeat_or_upsell_review' THEN 'Repeat or upsell review'
          WHEN 'manual_collection_recently_collected' THEN 'Manual collection current'
          WHEN 'automated_plan_monitor' THEN 'Automated plan monitor'
          ELSE INITCAP(REPLACE(customer_latest.collection_health_status, '_', ' '))
        END AS collection_health_label,
        customer_latest.post_first_purchase_collection_bookings_count,
        FORMAT_TIMESTAMP('%b %e, %Y', customer_latest.latest_collection_booking_at, 'America/New_York') AS latest_collection_booking_label,
        customer_latest.latest_collection_booking_name,
        customer_latest.lifetime_paid_payments_count,
        customer_latest.lifetime_fanbasis_payments_count,
        customer_latest.lifetime_stripe_payments_count,
        customer_latest.lifetime_refunds_amount,
        customer_latest.lifetime_refunds_count,
        customer_latest.latest_purchase_product,
        customer_latest.top_product_by_net_revenue,
        customer_latest.top_product_family,
        customer_latest.latest_prior_lead_magnet_name,
        customer_latest.latest_prior_lead_magnet_offer_type,
        customer_latest.credited_closer_name,
        customer_latest.credited_closer_source,
        customer_latest.credited_setter_name,
        customer_latest.best_available_operator_name,
        customer_latest.best_available_operator_source,
        customer_latest.latest_fanbasis_subscription_status,
        customer_latest.latest_fanbasis_payment_frequency_days,
        customer_latest.fanbasis_customer_ids,
        customer_latest.fanbasis_subscription_ids,
        customer_latest.fanbasis_directory_customer_ids,
        customer_latest.retention_quality_flag,
        customer_latest.revenue_funnel_quality_flag,
        contract_evidence_latest.contract_evidence_status,
        contract_evidence_latest.contract_evidence_truth_note,
        contract_evidence_latest.candidate_sales_calls_count,
        contract_evidence_latest.candidate_sales_call_ids,
        contract_evidence_latest.payment_terms_calls_count,
        contract_evidence_latest.payment_terms_snippets_count,
        contract_evidence_latest.mentioned_payment_amounts_count,
        contract_evidence_latest.mentioned_payment_amounts_text,
        contract_evidence_latest.largest_mentioned_payment_amount,
        contract_evidence_latest.payment_terms_evidence_text,
        contract_evidence_latest.has_payment_terms_transcript_evidence,
        contract_terms_latest.promised_contract_value AS confirmed_promised_contract_value,
        contract_terms_latest.upfront_agreed_amount AS confirmed_upfront_agreed_amount,
        contract_terms_latest.balance_expected_amount AS confirmed_balance_expected_amount,
        contract_terms_latest.review_confidence AS confirmed_review_confidence,
        contract_terms_latest.terms_source_note AS confirmed_terms_source_note,
        contract_terms_latest.reviewed_by AS confirmed_terms_reviewed_by,
        contract_terms_latest.reviewed_label AS confirmed_terms_reviewed_label,
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
      FROM customer_latest
      LEFT JOIN contract_evidence_latest
        ON contract_evidence_latest.contact_sk = customer_latest.contact_sk
      LEFT JOIN contract_terms_latest
        ON contract_terms_latest.contact_sk = customer_latest.contact_sk
      LEFT JOIN latest_reviews
        ON latest_reviews.contact_sk = customer_latest.contact_sk
        AND latest_reviews.action_bucket = CASE
          WHEN '${worklist}' = 'contract_terms_review'
            THEN 'contract_terms_review'
          WHEN customer_latest.retention_operator_next_action IN ('review_manual_collection', 'confirm_repeat_or_upsell', 'monitor_manual_collection')
            THEN customer_latest.collection_health_status
          ELSE customer_latest.payment_plan_health_status
        END
      WHERE ${worklistPredicate(worklist)}
        AND (
          '${worklist}' != 'contract_terms_review'
          OR contract_terms_latest.contact_sk IS NULL
        )
        AND (
          COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
          OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
        )
      ORDER BY
        CASE
          WHEN '${worklist}' = 'contract_terms_review' THEN 1
          ELSE CASE customer_latest.payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 1
          WHEN 'active_plan_due_no_payment_yet' THEN 2
          WHEN 'active_plan_not_yet_due' THEN 3
          WHEN 'completed_plan_paid_off' THEN 4
          WHEN 'one_time_upsell_candidate' THEN 5
          WHEN 'historical_stripe_product_review' THEN 6
          WHEN 'repeat_payment_observed' THEN 7
          ELSE 8
          END
        END,
        customer_latest.is_expected_payment_missed_now DESC,
        COALESCE(customer_latest.days_past_expected_payment, -999) DESC,
        customer_latest.lifetime_net_revenue_after_refunds DESC,
        customer_latest.latest_purchase_at DESC
      LIMIT 75
    `,
    retention_cohorts: `
      WITH
      ${customerLatest},
      ${customerRepeat}
      SELECT
        customer_latest.cohort_month,
        FORMAT_DATE('%b %Y', customer_latest.cohort_month) AS cohort_label,
        COUNT(*) AS customers,
        COUNTIF(customer_repeat.repeat_paid_months > 0) AS repeat_paid_customers,
        SAFE_DIVIDE(COUNTIF(customer_repeat.repeat_paid_months > 0), NULLIF(COUNT(*), 0)) AS repeat_paid_customer_rate,
        SUM(customer_repeat.repeat_paid_net_revenue) AS repeat_paid_net_revenue,
        SUM(customer_latest.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(customer_latest.lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv,
        COUNTIF(customer_latest.has_active_fanbasis_subscription_now) AS active_fanbasis_subscription_customers
      FROM customer_latest
      LEFT JOIN customer_repeat
        ON customer_latest.contact_sk = customer_repeat.contact_sk
      GROUP BY customer_latest.cohort_month, cohort_label
      ORDER BY customer_latest.cohort_month DESC
      LIMIT 12
    `,
    retention_product_families: `
      WITH
      ${customerLatest},
      ${customerRepeat}
      SELECT
        COALESCE(customer_latest.top_product_family, 'Unknown') AS top_product_family,
        COUNT(*) AS customers,
        COUNTIF(customer_repeat.repeat_paid_months > 0) AS repeat_paid_customers,
        SAFE_DIVIDE(COUNTIF(customer_repeat.repeat_paid_months > 0), NULLIF(COUNT(*), 0)) AS repeat_paid_customer_rate,
        SUM(customer_repeat.repeat_paid_net_revenue) AS repeat_paid_net_revenue,
        SUM(customer_latest.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(customer_latest.lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv,
        COUNTIF(customer_latest.has_active_fanbasis_subscription_now) AS active_fanbasis_subscription_customers,
        COUNTIF(customer_latest.is_payment_plan_buyer) AS payment_plan_customers,
        SAFE_DIVIDE(COUNTIF(customer_latest.is_payment_plan_buyer), NULLIF(COUNT(*), 0)) AS payment_plan_customer_rate
      FROM customer_latest
      LEFT JOIN customer_repeat
        ON customer_latest.contact_sk = customer_repeat.contact_sk
      GROUP BY top_product_family
      ORDER BY lifetime_net_revenue_after_refunds DESC
      LIMIT 10
    `,
    retention_magnets: `
      WITH
      ${customerLatest},
      ${customerRepeat}
      SELECT
        COALESCE(customer_latest.latest_prior_lead_magnet_name, 'No prior magnet') AS lead_magnet_name,
        COALESCE(customer_latest.latest_prior_lead_magnet_offer_type, 'no_prior_magnet') AS offer_type,
        COUNT(*) AS customers,
        COUNTIF(customer_repeat.repeat_paid_months > 0) AS repeat_paid_customers,
        SAFE_DIVIDE(COUNTIF(customer_repeat.repeat_paid_months > 0), NULLIF(COUNT(*), 0)) AS repeat_paid_customer_rate,
        SUM(customer_repeat.repeat_paid_net_revenue) AS repeat_paid_net_revenue,
        SUM(customer_latest.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(customer_latest.lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv,
        COUNTIF(customer_latest.has_active_fanbasis_subscription_now) AS active_fanbasis_subscription_customers
      FROM customer_latest
      LEFT JOIN customer_repeat
        ON customer_latest.contact_sk = customer_repeat.contact_sk
      GROUP BY lead_magnet_name, offer_type
      ORDER BY lifetime_net_revenue_after_refunds DESC, customers DESC
      LIMIT 12
    `,
    retention_operators: `
      WITH
      ${customerLatest},
      ${customerRepeat}
      SELECT
        customer_latest.credited_closer_name AS operator_name,
        customer_latest.credited_closer_source AS operator_source,
        customer_latest.credited_closer_confidence AS operator_confidence,
        COUNT(*) AS customers,
        COUNTIF(customer_repeat.repeat_paid_months > 0) AS repeat_paid_customers,
        SAFE_DIVIDE(COUNTIF(customer_repeat.repeat_paid_months > 0), NULLIF(COUNT(*), 0)) AS repeat_paid_customer_rate,
        SUM(customer_repeat.repeat_paid_net_revenue) AS repeat_paid_net_revenue,
        SUM(customer_latest.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(customer_latest.lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv,
        COUNTIF(customer_latest.has_active_fanbasis_subscription_now) AS active_fanbasis_subscription_customers
      FROM customer_latest
      LEFT JOIN customer_repeat
        ON customer_latest.contact_sk = customer_repeat.contact_sk
      GROUP BY operator_name, operator_source, operator_confidence
      ORDER BY lifetime_net_revenue_after_refunds DESC, customers DESC
      LIMIT 15
    `,
    retention_lifecycle: `
      WITH
      ${customerLatest},
      ${customerRepeat}
      SELECT
        customer_latest.customer_lifecycle_status,
        CASE customer_latest.customer_lifecycle_status
          WHEN 'active_fanbasis_subscription' THEN 'Active Fanbasis'
          WHEN 'completed_fanbasis_subscription' THEN 'Completed Fanbasis'
          WHEN 'failed_fanbasis_subscription' THEN 'Failed Fanbasis'
          WHEN 'one_time_fanbasis_customer' THEN 'One-time Fanbasis'
          WHEN 'fanbasis_transaction_no_subscriber_record' THEN 'Fanbasis cash, no subscriber'
          WHEN 'historical_stripe_or_no_subscriber_record' THEN 'Historical Stripe / no subscriber'
          ELSE INITCAP(REPLACE(customer_latest.customer_lifecycle_status, '_', ' '))
        END AS lifecycle_label,
        COUNT(*) AS customers,
        COUNTIF(customer_repeat.repeat_paid_months > 0) AS repeat_paid_customers,
        SAFE_DIVIDE(COUNTIF(customer_repeat.repeat_paid_months > 0), NULLIF(COUNT(*), 0)) AS repeat_paid_customer_rate,
        SUM(customer_latest.lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds,
        SAFE_DIVIDE(SUM(customer_latest.lifetime_net_revenue_after_refunds), NULLIF(COUNT(*), 0)) AS customer_ltv
      FROM customer_latest
      LEFT JOIN customer_repeat
        ON customer_latest.contact_sk = customer_repeat.contact_sk
      GROUP BY customer_lifecycle_status, lifecycle_label
      ORDER BY customers DESC
    `,
    retention_quality: `
      WITH ${customerLatest}
      SELECT
        retention_quality_flag,
        CASE retention_quality_flag
          WHEN 'clean' THEN 'Clean'
          WHEN 'missing_product_family' THEN 'Missing product family'
          WHEN 'no_subscriber_record' THEN 'No subscriber record'
          WHEN 'negative_lifetime_value' THEN 'Negative lifetime value'
          ELSE INITCAP(REPLACE(retention_quality_flag, '_', ' '))
        END AS quality_label,
        COUNT(*) AS customers,
        SUM(lifetime_net_revenue_after_refunds) AS lifetime_net_revenue_after_refunds
      FROM customer_latest
      GROUP BY retention_quality_flag, quality_label
      ORDER BY customers DESC
    `,
  } satisfies Record<string, string>;
}

export async function getRetentionData(options: GetRetentionDataOptions = {}): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeRetentionTimeRange(options.timeRange);
  const worklist = normalizeRetentionWorklist(options.worklist);
  const filters = buildDashboardFilters(timeRange, worklist);
  const dataset = retentionDataset();
  const retentionQueries = buildRetentionQueries(timeRange, worklist);

  try {
    const [
      summary,
      states,
      paymentHealth,
      repeatTypes,
      collections,
      contractEvidence,
      worklistRows,
      cohorts,
      productFamilies,
      magnets,
      operators,
      lifecycle,
      quality,
    ] = await Promise.all([
      runBigQuery(retentionQueries.retention_summary),
      runBigQuery(retentionQueries.retention_states),
      runBigQuery(retentionQueries.retention_payment_health),
      runBigQuery(retentionQueries.retention_repeat_payment_types),
      runBigQuery(retentionQueries.retention_collections),
      runBigQuery(retentionQueries.retention_contract_evidence),
      runBigQuery(retentionQueries.retention_worklist),
      runBigQuery(retentionQueries.retention_cohorts),
      runBigQuery(retentionQueries.retention_product_families),
      runBigQuery(retentionQueries.retention_magnets),
      runBigQuery(retentionQueries.retention_operators),
      runBigQuery(retentionQueries.retention_lifecycle),
      runBigQuery(retentionQueries.retention_quality),
    ]);

    return {
      rows: {
        retention_summary: summary,
        retention_states: states,
        retention_payment_health: paymentHealth,
        retention_repeat_payment_types: repeatTypes,
        retention_collections: collections,
        retention_contract_evidence: contractEvidence,
        retention_worklist: worklistRows,
        retention_cohorts: cohorts,
        retention_product_families: productFamilies,
        retention_magnets: magnets,
        retention_operators: operators,
        retention_lifecycle: lifecycle,
        retention_quality: quality,
      },
      freshness: buildFreshness(summary),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["customer_retention_detail", "collection_contract_evidence_detail", "contract_terms_reviews", "operator_action_reviews"],
        note:
          "Retention is customer-month grain. It uses collected payment and refund activity as cash truth, then layers Fanbasis subscriber/customer rows as lifecycle evidence without inventing future receivables.",
      },
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Retention data unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["customer_retention_detail", "collection_contract_evidence_detail", "contract_terms_reviews", "operator_action_reviews"],
        note:
          "Retention is customer-month grain. It uses collected payment and refund activity as cash truth, then layers Fanbasis subscriber/customer rows as lifecycle evidence without inventing future receivables.",
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
      detail: "The retention mart returned rows without a refresh timestamp.",
    };
  }

  const refreshedDate = new Date(refreshedAt);
  const ageMinutes = Math.max(0, Math.round((Date.now() - refreshedDate.getTime()) / 60000));
  const status = ageMinutes <= 180 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live dbt mart data" : "Stale dbt mart data",
    detail: `Retention mart refreshed ${formatAge(ageMinutes)} ago.`,
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
  return "Unknown Retention error";
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}
