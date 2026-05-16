import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type {
  DashboardData,
  DashboardFilters,
  DashboardFreshness,
  DashboardRow,
} from "@/types/dashboard-data";

export const RECOVERY_TIME_RANGE_OPTIONS = [
  { value: "today", label: "Today", description: "Recovery events from today in Eastern time." },
  { value: "7d", label: "7D", description: "Recovery events from the last 7 days." },
  { value: "30d", label: "30D", description: "Recovery events from the last 30 days." },
  { value: "90d", label: "90D", description: "Recovery events from the last 90 days." },
  { value: "all", label: "All", description: "All open recovery evidence in the warehouse." },
] as const;

export type RecoveryTimeRange = (typeof RECOVERY_TIME_RANGE_OPTIONS)[number]["value"];

const DEFAULT_TIME_RANGE: RecoveryTimeRange = "all";
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

type QueryResult = {
  key: string;
  rows: DashboardRow[];
  error?: string;
};

export function normalizeRecoveryTimeRange(value: string | null | undefined): RecoveryTimeRange {
  const normalized = value?.toLowerCase();
  const option = RECOVERY_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);
  return option?.value ?? DEFAULT_TIME_RANGE;
}

function martDataset() {
  const dataset =
    process.env.BIGQUERY_RECOVERY_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for recovery: ${dataset}`);
  }

  return dataset;
}

function tableRef(tableName: string, dataset = martDataset()) {
  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name: ${dataset}`);
  }

  return `\`${deeConfig.bigQuery.projectId}.${dataset}.${tableName}\``;
}

function buildFilters(timeRange: RecoveryTimeRange): DashboardFilters {
  const active = RECOVERY_TIME_RANGE_OPTIONS.find((option) => option.value === timeRange);
  return {
    timeRange,
    timeRangeLabel: active?.label ?? "All",
    timeRangeDescription: active?.description ?? "All open recovery evidence in the warehouse.",
    timeRangeOptions: RECOVERY_TIME_RANGE_OPTIONS.map((option) => ({ ...option })),
  };
}

function timestampRangePredicate(timeRange: RecoveryTimeRange, field: string) {
  if (timeRange === "all") return "";
  if (timeRange === "today") {
    return `DATE(${field}, 'America/New_York') = CURRENT_DATE('America/New_York')`;
  }

  const days = timeRange === "7d" ? 6 : timeRange === "90d" ? 89 : 29;
  return `DATE(${field}, 'America/New_York') >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL ${days} DAY)`;
}

function andTimeRange(timeRange: RecoveryTimeRange, field: string) {
  const predicate = timestampRangePredicate(timeRange, field);
  return predicate ? `AND ${predicate}` : "";
}

export function buildRecoveryQueries(timeRange: RecoveryTimeRange) {
  const retentionTable = tableRef("customer_retention_detail");
  const contractEvidenceTable = tableRef("collection_contract_evidence_detail");
  const actionQueueTable = tableRef("customer_action_queue");
  const canceledRecoveryTable = tableRef("canceled_booking_recovery_detail");
  const goldenContactTable = tableRef("dim_golden_contact");
  const dimContactsTable = tableRef("dim_contacts", "Core");
  const fanbasisTransactionsTable = tableRef("fanbasis_transactions_txn_raw", "Raw");
  const fanbasisObjectsTable = tableRef("fanbasis_objects_raw", "Raw");
  const ghlObjectsTable = tableRef("ghl_objects_raw", "Raw");
  const calendlyObjectsTable = tableRef("calendly_objects_raw", "Raw");
  const fathomCallsTable = tableRef("fathom_calls_raw", "Raw");
  const typeformObjectsTable = tableRef("typeform_objects_raw", "Raw");
  const stripeObjectsTable = tableRef("stripe_objects_raw", "Raw");
  const eventAt = "COALESCE(cancelled_at, scheduled_for, booked_at)";

  const latestRetention = `
    latest_retention AS (
      SELECT *
      FROM ${retentionTable}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY activity_month DESC
      ) = 1
    )
  `;

  const scopedCanceled = `
    scoped_canceled AS (
      SELECT *
      FROM ${canceledRecoveryTable}
      WHERE 1 = 1
      ${andTimeRange(timeRange, eventAt)}
    )
  `;

  const scopedActions = `
    scoped_actions AS (
      SELECT *
      FROM ${actionQueueTable}
      WHERE is_action_open
      ${andTimeRange(timeRange, "source_event_at")}
    )
  `;

  return {
    recovery_summary: `
      WITH
      ${latestRetention},
      ${scopedCanceled},
      ${scopedActions},
      retention AS (
        SELECT
          COUNT(*) AS retention_customers,
          COUNTIF(payment_plan_health_status IN ('failed_plan_recovery_needed', 'active_plan_due_no_payment_yet')) AS payment_recovery_customers,
          COUNTIF(payment_plan_health_status = 'failed_plan_recovery_needed') AS failed_plan_customers,
          COUNTIF(payment_plan_health_status = 'active_plan_due_no_payment_yet') AS active_due_customers,
          COUNTIF(is_expected_payment_due_now) AS expected_due_customers,
          COUNTIF(is_expected_payment_missed_now) AS expected_missed_customers,
          COUNTIF(payment_plan_health_status = 'one_time_upsell_candidate') AS one_time_upsell_customers,
          COUNTIF(payment_plan_health_status = 'completed_plan_paid_off') AS paid_off_customers,
          COUNTIF(collection_health_status IN (
            'manual_collection_stale_review',
            'collection_call_no_payment_review',
            'plan_named_collection_review',
            'repeat_or_upsell_review'
          )) AS manual_collection_review_customers,
          COUNTIF(retention_quality_flag != 'clean') AS retention_quality_gap_customers,
          COUNTIF(retention_quality_flag = 'missing_product_family') AS missing_product_family_customers,
          SUM(lifetime_net_revenue_after_refunds) AS retention_lifetime_net_revenue,
          SUM(IF(payment_plan_health_status IN ('failed_plan_recovery_needed', 'active_plan_due_no_payment_yet'), lifetime_net_revenue_after_refunds, 0)) AS payment_recovery_lifetime_value,
          SUM(IF(collection_health_status IN (
            'manual_collection_stale_review',
            'collection_call_no_payment_review',
            'plan_named_collection_review',
            'repeat_or_upsell_review'
          ), lifetime_net_revenue_after_refunds, 0)) AS manual_collection_lifetime_value,
          MAX(mart_refreshed_at) AS retention_refreshed_at
        FROM latest_retention
      ),
      canceled AS (
        SELECT
          COUNT(*) AS canceled_bookings,
          COUNT(DISTINCT contact_sk) AS canceled_contacts,
          COUNTIF(recovery_outcome = 'not_recovered_yet') AS canceled_not_recovered,
          COUNTIF(recovery_outcome = 'rebooked_no_show') AS rebooked_no_show,
          COUNTIF(recovery_outcome = 'contact_not_matched') AS canceled_contact_not_matched,
          COUNTIF(recovery_outcome = 'bought_after_cancel') AS bought_after_cancel_bookings,
          COUNTIF(recovery_outcome IN ('bought_after_cancel', 'fathom_show_after_cancel', 'likely_show_after_cancel', 'rebooked_pending')) AS recovered_or_pending_bookings,
          SUM(total_net_revenue_after_cancel) AS net_revenue_after_cancel,
          SUM(credited_net_revenue_after_first_cancel) AS credited_net_revenue_after_first_cancel,
          MAX(mart_refreshed_at) AS canceled_refreshed_at
        FROM scoped_canceled
      ),
      actions AS (
        SELECT
          COUNT(*) AS open_recovery_actions,
          COUNTIF(action_area = 'retention') AS open_retention_actions,
          COUNTIF(action_area = 'contract_terms') AS open_contract_actions,
          COUNTIF(action_area = 'revenue') AS open_revenue_actions,
          SUM(money_at_stake) AS open_money_at_stake,
          MAX(mart_refreshed_at) AS action_queue_refreshed_at
        FROM scoped_actions
      ),
      contract AS (
        SELECT
          COUNT(*) AS contract_evidence_customers,
          COUNTIF(contract_evidence_status = 'transcript_payment_terms_found') AS transcript_terms_customers,
          COUNTIF(contract_evidence_status = 'sales_call_found_no_payment_terms') AS sales_call_no_terms_customers,
          COUNTIF(contract_evidence_status = 'no_sales_call_transcript') AS no_sales_call_transcript_customers,
          SUM(IF(contract_evidence_status = 'transcript_payment_terms_found', lifetime_net_revenue_after_refunds, 0)) AS transcript_terms_lifetime_value,
          MAX(mart_refreshed_at) AS contract_refreshed_at
        FROM ${contractEvidenceTable}
      ),
      missed AS (
        SELECT
          COUNT(*) AS booked_never_attended_leads,
          COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_meeting_start_ts, DAY) <= 7) AS hot_missed_leads,
          COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_meeting_start_ts, DAY) BETWEEN 8 AND 30) AS warm_missed_leads,
          COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_meeting_start_ts, DAY) > 30) AS cold_missed_leads,
          MAX(mart_refreshed_at) AS golden_contact_refreshed_at
        FROM ${goldenContactTable}
        WHERE meetings_booked_cnt > 0
          AND COALESCE(meetings_showed_cnt, 0) = 0
          AND last_meeting_start_ts IS NOT NULL
          AND last_meeting_start_ts < CURRENT_TIMESTAMP()
          AND (NULLIF(phone, '') IS NOT NULL OR NULLIF(email, '') IS NOT NULL)
          ${andTimeRange(timeRange, "last_meeting_start_ts")}
      )
      SELECT
        retention.*,
        canceled.*,
        actions.*,
        contract.*,
        missed.*,
        SAFE_DIVIDE(recovered_or_pending_bookings, NULLIF(canceled_bookings, 0)) AS canceled_recovery_rate,
        payment_recovery_customers
          + manual_collection_review_customers
          + transcript_terms_customers
          + canceled_not_recovered
          + rebooked_no_show AS open_recovery_surface_count,
        missing_product_family_customers
          + canceled_contact_not_matched
          + no_sales_call_transcript_customers AS known_gap_count,
        (
          SELECT MIN(refreshed_at)
          FROM UNNEST([
            retention.retention_refreshed_at,
            canceled.canceled_refreshed_at,
            actions.action_queue_refreshed_at,
            contract.contract_refreshed_at,
            missed.golden_contact_refreshed_at
          ]) AS refreshed_at
          WHERE refreshed_at IS NOT NULL
        ) AS oldest_dependency_refreshed_at,
        (
          SELECT MAX(refreshed_at)
          FROM UNNEST([
            retention.retention_refreshed_at,
            canceled.canceled_refreshed_at,
            actions.action_queue_refreshed_at,
            contract.contract_refreshed_at,
            missed.golden_contact_refreshed_at
          ]) AS refreshed_at
          WHERE refreshed_at IS NOT NULL
        ) AS newest_dependency_refreshed_at
      FROM retention, canceled, actions, contract, missed
    `,
    recovery_action_queue: `
      WITH
      ${scopedActions},
      ${scopedCanceled},
      canceled_actions AS (
        SELECT
          canceled.contact_sk,
          contacts.contact_id,
          COALESCE(NULLIF(contacts.contact_name, ''), NULLIF(contacts.email_norm, ''), NULLIF(contacts.phone, ''), 'Unknown lead') AS customer_display_name,
          contacts.email_norm,
          contacts.phone,
          'booking_recovery' AS action_area,
          'canceled_booking_recovery' AS queue_name,
          canceled.recovery_outcome AS action_bucket,
          CASE canceled.recovery_outcome
            WHEN 'not_recovered_yet' THEN 'Canceled booking not recovered'
            WHEN 'rebooked_no_show' THEN 'Rebooked, no-showed'
            WHEN 'contact_not_matched' THEN 'Canceled booking, no matched contact'
            ELSE INITCAP(REPLACE(canceled.recovery_outcome, '_', ' '))
          END AS action_label,
          CASE canceled.recovery_outcome
            WHEN 'not_recovered_yet' THEN 'No rebook, Fathom show evidence, or purchase after the canceled booking'
            WHEN 'rebooked_no_show' THEN 'Lead rebooked but still has no show evidence'
            WHEN 'contact_not_matched' THEN 'Calendly booking did not bridge to a clean GHL contact'
            ELSE 'Review canceled booking recovery evidence'
          END AS action_reason,
          CASE canceled.recovery_outcome
            WHEN 'not_recovered_yet' THEN 2
            WHEN 'rebooked_no_show' THEN 3
            WHEN 'contact_not_matched' THEN 5
            ELSE 8
          END AS priority_rank,
          CASE
            WHEN contacts.phone IS NOT NULL THEN 'Call + text'
            WHEN contacts.email_norm IS NOT NULL THEN 'Email'
            ELSE 'Repair contact'
          END AS recommended_channel_label,
          'canceled_booking_recovery_detail' AS source_table,
          canceled.canceled_booking_sk AS source_record_id,
          COALESCE(canceled.cancelled_at, canceled.scheduled_for, canceled.booked_at) AS source_event_at,
          canceled.total_net_revenue_after_cancel AS money_at_stake,
          CAST(NULL AS STRING) AS top_product_by_net_revenue,
          CAST(NULL AS STRING) AS top_product_family,
          CAST(NULL AS STRING) AS latest_prior_lead_magnet_name,
          CAST(NULL AS STRING) AS review_status,
          canceled.cancel_reason,
          canceled.cancelled_by_type,
          canceled.next_active_scheduled_for,
          canceled.hours_to_rebook,
          canceled.mart_refreshed_at
        FROM scoped_canceled AS canceled
        LEFT JOIN ${dimContactsTable} AS contacts
          ON contacts.contact_sk = canceled.contact_sk
        WHERE canceled.recovery_outcome IN ('not_recovered_yet', 'rebooked_no_show', 'contact_not_matched')
      ),
      payment_actions AS (
        SELECT
          contact_sk,
          contact_id,
          customer_display_name,
          email_norm,
          phone,
          action_area,
          queue_name,
          action_bucket,
          action_label,
          action_reason,
          priority_rank,
          recommended_channel_label,
          source_table,
          source_record_id,
          source_event_at,
          money_at_stake,
          top_product_by_net_revenue,
          top_product_family,
          latest_prior_lead_magnet_name,
          review_status,
          CAST(NULL AS STRING) AS cancel_reason,
          CAST(NULL AS STRING) AS cancelled_by_type,
          CAST(NULL AS TIMESTAMP) AS next_active_scheduled_for,
          CAST(NULL AS INT64) AS hours_to_rebook,
          mart_refreshed_at
        FROM scoped_actions
        WHERE action_area IN ('retention', 'contract_terms')
          AND action_bucket IN (
            'failed_plan_recovery_needed',
            'active_plan_due_no_payment_yet',
            'manual_collection_stale_review',
            'collection_call_no_payment_review',
            'plan_named_collection_review',
            'repeat_or_upsell_review',
            'one_time_upsell_candidate',
            'completed_plan_paid_off',
            'contract_terms_review'
          )
      )
      SELECT
        *,
        FORMAT_TIMESTAMP('%b %e, %Y', source_event_at, 'America/New_York') AS source_event_label,
        FORMAT_TIMESTAMP('%FT%TZ', source_event_at) AS source_event_iso,
        FORMAT_TIMESTAMP('%FT%TZ', mart_refreshed_at) AS refreshed_at
      FROM (
        SELECT * FROM payment_actions
        UNION ALL
        SELECT * FROM canceled_actions
      )
      ORDER BY priority_rank, money_at_stake DESC, source_event_at DESC
      LIMIT 90
    `,
    recovery_payment_health: `
      WITH ${latestRetention}
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
          ELSE INITCAP(REPLACE(payment_plan_health_status, '_', ' '))
        END AS health_label,
        COUNT(*) AS customers,
        COUNTIF(is_expected_payment_due_now) AS due_now_customers,
        COUNTIF(is_expected_payment_missed_now) AS missed_now_customers,
        SUM(lifetime_net_revenue_after_refunds) AS lifetime_net_revenue,
        SUM(post_first_collected_net_revenue) AS post_first_collected_net_revenue
      FROM latest_retention
      GROUP BY payment_plan_health_status, health_label
      ORDER BY
        CASE payment_plan_health_status
          WHEN 'failed_plan_recovery_needed' THEN 1
          WHEN 'active_plan_due_no_payment_yet' THEN 2
          WHEN 'manual_collection_stale_review' THEN 3
          WHEN 'one_time_upsell_candidate' THEN 4
          WHEN 'completed_plan_paid_off' THEN 5
          WHEN 'historical_stripe_product_review' THEN 6
          ELSE 9
        END,
        customers DESC
    `,
    recovery_collection_health: `
      WITH ${latestRetention}
      SELECT
        collection_health_status,
        collection_motion_type,
        CASE collection_health_status
          WHEN 'manual_collection_stale_review' THEN 'Manual collection stale'
          WHEN 'collection_call_no_payment_review' THEN 'Collection call, no pay'
          WHEN 'plan_named_collection_review' THEN 'Plan-named cash only'
          WHEN 'repeat_or_upsell_review' THEN 'Repeat or upsell review'
          WHEN 'manual_collection_recently_collected' THEN 'Manual collection current'
          WHEN 'automated_plan_due_no_payment' THEN 'Automated plan due, no pay'
          WHEN 'automated_plan_failed_recovery' THEN 'Automated plan failed'
          WHEN 'automated_plan_monitor' THEN 'Automated plan monitor'
          ELSE INITCAP(REPLACE(collection_health_status, '_', ' '))
        END AS collection_health_label,
        COUNT(*) AS customers,
        SUM(lifetime_net_revenue_after_refunds) AS lifetime_net_revenue,
        SUM(post_first_collected_net_revenue) AS post_first_collected_net_revenue,
        COUNTIF(post_first_purchase_collection_bookings_count > 0) AS collection_booking_customers
      FROM latest_retention
      GROUP BY collection_health_status, collection_motion_type, collection_health_label
      ORDER BY
        CASE collection_health_status
          WHEN 'manual_collection_stale_review' THEN 1
          WHEN 'collection_call_no_payment_review' THEN 2
          WHEN 'plan_named_collection_review' THEN 3
          WHEN 'repeat_or_upsell_review' THEN 4
          WHEN 'automated_plan_failed_recovery' THEN 5
          WHEN 'automated_plan_due_no_payment' THEN 6
          ELSE 9
        END,
        customers DESC
    `,
    recovery_canceled_outcomes: `
      WITH ${scopedCanceled}
      SELECT
        recovery_outcome,
        CASE recovery_outcome
          WHEN 'not_recovered_yet' THEN 'Not recovered yet'
          WHEN 'likely_show_after_cancel' THEN 'Likely showed after cancel'
          WHEN 'bought_after_cancel' THEN 'Bought after cancel'
          WHEN 'contact_not_matched' THEN 'Contact not matched'
          WHEN 'fathom_show_after_cancel' THEN 'Fathom show evidence'
          WHEN 'rebooked_no_show' THEN 'Rebooked no-show'
          WHEN 'rebooked_pending' THEN 'Rebooked pending'
          ELSE INITCAP(REPLACE(recovery_outcome, '_', ' '))
        END AS outcome_label,
        COUNT(*) AS canceled_bookings,
        COUNT(DISTINCT contact_sk) AS contacts,
        SUM(total_net_revenue_after_cancel) AS net_revenue_after_cancel,
        SUM(credited_net_revenue_after_first_cancel) AS credited_net_revenue_after_first_cancel,
        AVG(hours_to_rebook) AS avg_hours_to_rebook
      FROM scoped_canceled
      GROUP BY recovery_outcome, outcome_label
      ORDER BY canceled_bookings DESC
    `,
    recovery_source_breakdown: `
      SELECT
        COALESCE(NULLIF(golden.ghl_source, ''), NULLIF(golden.ghl_source_first, ''), contacts.lead_source, 'Unknown') AS source_label,
        COUNT(*) AS recoverable_count,
        COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, DAY) <= 7) AS hot_count,
        COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, DAY) BETWEEN 8 AND 30) AS warm_count,
        COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, DAY) > 30) AS cold_count,
        AVG(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, DAY)) AS avg_age_days
      FROM ${goldenContactTable} AS golden
      LEFT JOIN ${dimContactsTable} AS contacts
        ON contacts.contact_id = golden.ghl_contact_id
      WHERE golden.meetings_booked_cnt > 0
        AND COALESCE(golden.meetings_showed_cnt, 0) = 0
        AND golden.last_meeting_start_ts IS NOT NULL
        AND golden.last_meeting_start_ts < CURRENT_TIMESTAMP()
        AND (NULLIF(golden.phone, '') IS NOT NULL OR NULLIF(golden.email, '') IS NOT NULL)
        ${andTimeRange(timeRange, "golden.last_meeting_start_ts")}
      GROUP BY source_label
      HAVING recoverable_count > 0
      ORDER BY recoverable_count DESC
      LIMIT 10
    `,
    recovery_missed_meetings: `
      SELECT
        golden.golden_contact_key,
        golden.ghl_contact_id,
        COALESCE(
          NULLIF(golden.full_name, ''),
          NULLIF(TRIM(CONCAT(COALESCE(golden.first_name, ''), ' ', COALESCE(golden.last_name, ''))), ''),
          'Unknown lead'
        ) AS lead_name,
        COALESCE(NULLIF(golden.email, ''), 'No email') AS lead_email,
        COALESCE(NULLIF(golden.phone, ''), 'No phone') AS lead_phone,
        COALESCE(NULLIF(golden.rep_name, ''), NULLIF(golden.setter_at_first_contact, ''), 'Unassigned') AS assigned_rep,
        COALESCE(NULLIF(golden.ghl_source, ''), NULLIF(golden.ghl_source_first, ''), 'Unknown') AS source_label,
        COALESCE(NULLIF(golden.utm_source, ''), NULLIF(golden.utm_source_first, ''), 'N/A') AS utm_source,
        COALESCE(NULLIF(golden.utm_campaign, ''), NULLIF(golden.utm_campaign_first, ''), 'N/A') AS utm_campaign,
        golden.meetings_booked_cnt AS meetings_booked,
        golden.meetings_showed_cnt AS meetings_showed,
        FORMAT_TIMESTAMP('%FT%TZ', golden.last_meeting_start_ts) AS last_meeting_ts,
        FORMAT_TIMESTAMP('%Y-%m-%d %I:%M %p ET', golden.last_meeting_start_ts, 'America/New_York') AS last_meeting_et,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, HOUR) AS hours_since_missed,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), golden.last_meeting_start_ts, DAY) AS days_since_missed
      FROM ${goldenContactTable} AS golden
      WHERE golden.meetings_booked_cnt > 0
        AND COALESCE(golden.meetings_showed_cnt, 0) = 0
        AND golden.last_meeting_start_ts IS NOT NULL
        AND golden.last_meeting_start_ts < CURRENT_TIMESTAMP()
        AND (NULLIF(golden.phone, '') IS NOT NULL OR NULLIF(golden.email, '') IS NOT NULL)
        ${andTimeRange(timeRange, "golden.last_meeting_start_ts")}
      ORDER BY golden.last_meeting_start_ts DESC
      LIMIT 25
    `,
    recovery_data_gaps: `
      WITH
      ${latestRetention},
      ${scopedCanceled},
      contract AS (
        SELECT * FROM ${contractEvidenceTable}
      ),
      actions AS (
        SELECT * FROM ${actionQueueTable}
        WHERE is_action_open
      )
      SELECT
        'mart_freshness' AS gap_key,
        'Mart freshness' AS gap_label,
        'Recovery marts are behind the raw feeds' AS gap_detail,
        COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), mart_refreshed_at, HOUR) > 6) AS affected_count,
        CAST(NULL AS NUMERIC) AS money_at_stake,
        'Refresh dbt marts and verify the scheduler' AS recommended_fix,
        MAX(mart_refreshed_at) AS evidence_at
      FROM (
        SELECT mart_refreshed_at FROM ${retentionTable}
        UNION ALL
        SELECT mart_refreshed_at FROM ${canceledRecoveryTable}
        UNION ALL
        SELECT mart_refreshed_at FROM ${contractEvidenceTable}
        UNION ALL
        SELECT mart_refreshed_at FROM ${actionQueueTable}
      )
      UNION ALL
      SELECT
        'missing_product_family',
        'Missing product family',
        'Historical or unmapped products weaken product-specific recovery routing',
        COUNTIF(retention_quality_flag = 'missing_product_family'),
        SUM(IF(retention_quality_flag = 'missing_product_family', lifetime_net_revenue_after_refunds, 0)),
        'Repair historical Stripe/Fanbasis product taxonomy',
        MAX(mart_refreshed_at)
      FROM latest_retention
      UNION ALL
      SELECT
        'contact_not_matched',
        'Canceled booking contact gap',
        'Canceled Calendly bookings that do not bridge to a clean contact cannot be worked safely',
        COUNTIF(recovery_outcome = 'contact_not_matched'),
        CAST(NULL AS NUMERIC),
        'Repair Calendly invitee email to GHL contact identity bridge',
        MAX(mart_refreshed_at)
      FROM scoped_canceled
      UNION ALL
      SELECT
        'no_sales_call_transcript',
        'No sales-call transcript',
        'Customers without sales-call transcript evidence cannot support promised-balance answers',
        COUNTIF(contract_evidence_status = 'no_sales_call_transcript'),
        SUM(IF(contract_evidence_status = 'no_sales_call_transcript', lifetime_net_revenue_after_refunds, 0)),
        'Fix or backfill Fathom transcript extraction before treating contract terms as final',
        MAX(mart_refreshed_at)
      FROM contract
      UNION ALL
      SELECT
        'owner_not_modeled',
        'Current owner unknown',
        'Open actions still avoid guessing a current recovery owner',
        COUNTIF(current_owner_source = 'not_modelled' OR current_owner_source = 'not_modeled_yet'),
        SUM(IF(current_owner_source = 'not_modelled' OR current_owner_source = 'not_modeled_yet', money_at_stake, 0)),
        'Add a sourced owner field or work-assignment ledger',
        MAX(mart_refreshed_at)
      FROM actions
      ORDER BY affected_count DESC
    `,
    recovery_source_freshness: `
      SELECT
        'Fanbasis transactions' AS source_label,
        'Raw.fanbasis_transactions_txn_raw' AS source_table,
        COUNT(*) AS row_count,
        MAX(ingested_at) AS max_ingested_at,
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)) AS max_ingested_iso,
        'payments' AS coverage_note
      FROM ${fanbasisTransactionsTable}
      UNION ALL
      SELECT
        CONCAT('Fanbasis ', object_type),
        CONCAT('Raw.fanbasis_objects_raw:', object_type),
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        'customer/subscriber lifecycle'
      FROM ${fanbasisObjectsTable}
      GROUP BY object_type
      UNION ALL
      SELECT
        CONCAT('GHL ', entity_type),
        CONCAT('Raw.ghl_objects_raw:', entity_type),
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        'contacts, opportunities, follow-up, forms'
      FROM ${ghlObjectsTable}
      WHERE entity_type IN ('contacts', 'opportunities', 'outbound_call_logs', 'conversations', 'users', 'form_submissions')
      GROUP BY entity_type
      UNION ALL
      SELECT
        CONCAT('Calendly ', entity_type),
        CONCAT('Raw.calendly_objects_raw:', entity_type),
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        'booking and cancellation evidence'
      FROM ${calendlyObjectsTable}
      GROUP BY entity_type
      UNION ALL
      SELECT
        'Fathom calls',
        'Raw.fathom_calls_raw',
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        CONCAT(CAST(COUNTIF(ARRAY_LENGTH(JSON_QUERY_ARRAY(payload_json, '$.transcript')) > 0) AS STRING), ' calls with transcript JSON')
      FROM ${fathomCallsTable}
      UNION ALL
      SELECT
        CONCAT('Typeform ', entity_type),
        CONCAT('Raw.typeform_objects_raw:', entity_type),
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        'lead qualification context'
      FROM ${typeformObjectsTable}
      GROUP BY entity_type
      UNION ALL
      SELECT
        CONCAT('Stripe ', object_type),
        CONCAT('Raw.stripe_objects_raw:', object_type),
        COUNT(*),
        MAX(ingested_at),
        FORMAT_TIMESTAMP('%FT%TZ', MAX(ingested_at)),
        'historical revenue context'
      FROM ${stripeObjectsTable}
      WHERE object_type IN ('charges', 'customers', 'subscriptions', 'invoices', 'refunds', 'disputes')
      GROUP BY object_type
      ORDER BY max_ingested_at DESC
    `,
  } satisfies Record<string, string>;
}

async function runNamedQuery(key: string, sql: string): Promise<QueryResult> {
  try {
    return { key, rows: await runBigQuery(sql) };
  } catch (error) {
    return { key, rows: [], error: getErrorMessage(error) };
  }
}

export async function getRecoveryData(
  options: { timeRange?: string | null } = {},
): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const timeRange = normalizeRecoveryTimeRange(options.timeRange);
  const filters = buildFilters(timeRange);
  const dataset = martDataset();
  const queries = buildRecoveryQueries(timeRange);

  const results = await Promise.all(
    Object.entries(queries).map(([key, sql]) => runNamedQuery(key, sql)),
  );

  const rows = Object.fromEntries(results.map((result) => [result.key, result.rows]));
  const errors = results.filter((result) => result.error);
  const summary = rows.recovery_summary ?? [];

  return {
    rows,
    freshness: buildFreshness(summary, errors),
    filters,
    generatedAt,
    error: errors.length
      ? errors.map((error) => `${error.key}: ${error.error}`).join(" | ")
      : undefined,
    dataContract: {
      owner: "dbt-mart",
      projectId: deeConfig.bigQuery.projectId,
      dataset,
      tables: [
        "customer_retention_detail",
        "customer_action_queue",
        "collection_contract_evidence_detail",
        "canceled_booking_recovery_detail",
        "dim_golden_contact",
        "Raw source freshness",
      ],
      note:
        "Recovery combines payment-plan work, manual collection evidence, canceled booking outcomes, missed-meeting rescue, and explicit source gaps. It does not invent receivable balances or owners that are not sourced.",
    },
  };
}

function buildFreshness(summary: DashboardRow[], errors: QueryResult[]): DashboardFreshness {
  if (errors.some((error) => error.key === "recovery_summary")) {
    return {
      status: "error",
      label: "Recovery summary unavailable",
      detail: errors.find((error) => error.key === "recovery_summary")?.error ?? "Unknown recovery query error.",
    };
  }

  const refreshedAt =
    stringValue(summary[0]?.oldest_dependency_refreshed_at) ??
    stringValue(summary[0]?.newest_dependency_refreshed_at);

  if (!refreshedAt) {
    return {
      status: "stale",
      label: "No refresh timestamp",
      detail: "Recovery returned rows without mart refresh timestamps.",
    };
  }

  const refreshedDate = new Date(refreshedAt);
  const ageMinutes = Math.max(0, Math.round((Date.now() - refreshedDate.getTime()) / 60000));
  const status = errors.length > 0 ? "stale" : ageMinutes <= 360 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live recovery data" : "Recovery data needs attention",
    detail:
      errors.length > 0
        ? `Core recovery loaded, but ${errors.length} supporting check${errors.length === 1 ? "" : "s"} failed.`
        : `Oldest recovery dependency refreshed ${formatAge(ageMinutes)} ago.`,
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
  return "Unknown recovery data error";
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  if (
    value &&
    typeof value === "object" &&
    "value" in value &&
    typeof (value as { value?: unknown }).value === "string" &&
    (value as { value: string }).value.trim() !== ""
  ) {
    return (value as { value: string }).value;
  }
  return null;
}
