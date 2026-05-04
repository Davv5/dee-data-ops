import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardData, DashboardFilters, DashboardFreshness, DashboardRow } from "@/types/dashboard-data";

type GetCustomer360DataOptions = {
  contactSk: string;
};

const CONTACT_SK_PATTERN = /^[a-f0-9]{32}$/i;
const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function martDataset() {
  const dataset =
    process.env.BIGQUERY_CUSTOMER_360_DATASET ??
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for Customer 360: ${dataset}`);
  }

  return dataset;
}

function coreDataset() {
  const dataset = process.env.BIGQUERY_CUSTOMER_360_CORE_DATASET ?? "Core";

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery core dataset name for Customer 360: ${dataset}`);
  }

  return dataset;
}

function tableRef(dataset: string, tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.${dataset}.${tableName}\``;
}

function martTableRef(tableName: string) {
  return tableRef(martDataset(), tableName);
}

function coreTableRef(tableName: string) {
  return tableRef(coreDataset(), tableName);
}

function assertContactSk(contactSk: string) {
  if (!CONTACT_SK_PATTERN.test(contactSk)) {
    throw new Error("Invalid customer identifier.");
  }
}

function buildDashboardFilters(): DashboardFilters {
  return {
    timeRange: "all",
    timeRangeLabel: "All",
    timeRangeDescription: "Customer-level drilldown.",
    timeRangeOptions: [],
  };
}

function buildCustomer360Queries(contactSk: string) {
  const retentionTable = martTableRef("customer_retention_detail");
  const contractEvidenceTable = martTableRef("collection_contract_evidence_detail");
  const contractTermsReviewsTable = martTableRef("contract_terms_reviews");
  const revenueTable = martTableRef("revenue_funnel_detail");
  const contactsTable = coreTableRef("dim_contacts");
  const usersTable = coreTableRef("dim_users");
  const paymentsTable = coreTableRef("fct_payments");
  const refundsTable = coreTableRef("fct_refunds");
  const outreachTable = coreTableRef("fct_outreach");
  const bookingsTable = coreTableRef("fct_calls_booked");
  const pipelineStagesTable = coreTableRef("dim_pipeline_stages");
  const leadMagnetTable = martTableRef("lead_magnet_detail");
  const operatorActionReviewsTable = martTableRef("operator_action_reviews");

  const anchor = `anchor AS (SELECT '${contactSk}' AS contact_sk)`;
  const latestRetention = `
    latest_retention AS (
      SELECT *
      FROM ${retentionTable}
      WHERE contact_sk = '${contactSk}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY activity_month DESC
      ) = 1
    )
  `;
  const revenue = `
    revenue AS (
      SELECT *
      FROM ${revenueTable}
      WHERE contact_sk = '${contactSk}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY first_purchase_at DESC
      ) = 1
    )
  `;
  const contractEvidence = `
    contract_evidence AS (
      SELECT *
      FROM ${contractEvidenceTable}
      WHERE contact_sk = '${contactSk}'
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
      WHERE
        contact_sk = '${contactSk}'
        AND review_status = 'confirmed'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY reviewed_at DESC
      ) = 1
    )
  `;
  const contact = `
    contact AS (
      SELECT *
      FROM ${contactsTable}
      WHERE contact_sk = '${contactSk}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY contact_updated_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
      ) = 1
    )
  `;

  return {
    customer_360_profile: `
      WITH
      ${anchor},
      ${latestRetention},
      ${revenue},
      ${contractEvidence},
      ${contractTermsLatest},
      ${contact}
      SELECT
        anchor.contact_sk,
        latest_retention.contact_id,
        COALESCE(
          NULLIF(latest_retention.contact_name, ''),
          NULLIF(contact.contact_name, ''),
          NULLIF(latest_retention.email_norm, ''),
          NULLIF(contact.email_norm, ''),
          NULLIF(latest_retention.phone, ''),
          'Unknown customer'
        ) AS customer_display_name,
        COALESCE(latest_retention.email_norm, contact.email_norm) AS email_norm,
        COALESCE(latest_retention.phone, contact.phone) AS phone,
        contact.city,
        contact.state,
        contact.country,
        contact.timezone,
        contact.lead_source,
        contact.utm_source,
        contact.utm_medium,
        contact.utm_campaign,
        FORMAT_TIMESTAMP('%b %e, %Y', COALESCE(latest_retention.contact_created_at, contact.contact_created_at), 'America/New_York') AS contact_created_label,

        FORMAT_TIMESTAMP('%b %e, %Y', latest_retention.first_purchase_at, 'America/New_York') AS first_purchase_label,
        FORMAT_TIMESTAMP('%b %e, %Y', latest_retention.latest_purchase_at, 'America/New_York') AS latest_purchase_label,
        FORMAT_TIMESTAMP('%FT%TZ', latest_retention.first_purchase_at) AS first_purchase_at,
        FORMAT_TIMESTAMP('%FT%TZ', latest_retention.latest_purchase_at) AS latest_purchase_at,
        latest_retention.customer_lifecycle_status,
        latest_retention.payment_plan_health_status,
        latest_retention.retention_operator_next_action,
        latest_retention.repeat_payment_type,
        latest_retention.payment_plan_status,
        latest_retention.payment_plan_truth_status,
        latest_retention.collection_motion_type,
        latest_retention.collection_health_status,
        latest_retention.upfront_collected_net_revenue,
        latest_retention.post_first_collected_net_revenue,
        latest_retention.post_first_collected_net_revenue_share,
        latest_retention.post_first_paid_payments_count,
        latest_retention.post_first_purchase_collection_bookings_count,
        FORMAT_TIMESTAMP('%b %e, %Y', latest_retention.latest_post_first_payment_at, 'America/New_York') AS latest_post_first_payment_label,
        FORMAT_TIMESTAMP('%b %e, %Y', latest_retention.latest_collection_booking_at, 'America/New_York') AS latest_collection_booking_label,
        latest_retention.latest_collection_booking_name,
        latest_retention.days_since_latest_post_first_payment,
        contract_evidence.contract_evidence_status,
        contract_evidence.contract_evidence_truth_note,
        contract_evidence.candidate_sales_calls_count,
        contract_evidence.candidate_sales_call_ids,
        contract_evidence.payment_terms_calls_count,
        contract_evidence.payment_terms_snippets_count,
        contract_evidence.mentioned_payment_amounts_count,
        contract_evidence.mentioned_payment_amounts_text,
        contract_evidence.largest_mentioned_payment_amount,
        contract_evidence.payment_terms_evidence_text,
        contract_evidence.has_payment_terms_transcript_evidence,
        contract_terms_latest.promised_contract_value AS confirmed_promised_contract_value,
        contract_terms_latest.upfront_agreed_amount AS confirmed_upfront_agreed_amount,
        contract_terms_latest.balance_expected_amount AS confirmed_balance_expected_amount,
        contract_terms_latest.review_confidence AS confirmed_review_confidence,
        contract_terms_latest.terms_source_note AS confirmed_terms_source_note,
        contract_terms_latest.reviewed_by AS confirmed_terms_reviewed_by,
        contract_terms_latest.reviewed_label AS confirmed_terms_reviewed_label,
        contract_terms_latest.reviewed_at AS confirmed_terms_reviewed_at,
        latest_retention.expected_next_payment_date,
        FORMAT_DATE('%b %e, %Y', latest_retention.expected_next_payment_date) AS expected_next_payment_label,
        latest_retention.days_until_expected_next_payment,
        latest_retention.days_past_expected_payment,
        latest_retention.is_expected_payment_due_now,
        latest_retention.is_expected_payment_missed_now,
        latest_retention.has_active_fanbasis_subscription_now,

        latest_retention.lifetime_paid_payments_count,
        latest_retention.lifetime_fanbasis_payments_count,
        latest_retention.lifetime_stripe_payments_count,
        latest_retention.lifetime_fanbasis_auto_renew_payments_count,
        latest_retention.lifetime_gross_revenue,
        latest_retention.lifetime_net_revenue_after_refunds,
        latest_retention.lifetime_refunds_amount,
        latest_retention.lifetime_refunds_count,
        latest_retention.average_net_revenue_per_payment,
        latest_retention.lifetime_purchased_products,
        latest_retention.top_product_by_net_revenue,
        latest_retention.top_product_family,

        latest_retention.latest_prior_lead_magnet_name,
        latest_retention.latest_prior_lead_magnet_category,
        latest_retention.latest_prior_lead_magnet_offer_type,
        FORMAT_TIMESTAMP('%b %e, %Y', latest_retention.latest_prior_opportunity_created_at, 'America/New_York') AS latest_prior_opportunity_label,
        latest_retention.purchase_magnet_attribution_flag,

        latest_retention.credited_closer_name,
        latest_retention.credited_closer_role,
        latest_retention.credited_closer_source,
        latest_retention.credited_closer_confidence,
        latest_retention.credited_setter_name,
        latest_retention.credited_setter_role,
        latest_retention.credited_setter_source,
        latest_retention.best_available_operator_name,
        latest_retention.best_available_operator_source,
        latest_retention.pre_purchase_funnel_path,

        latest_retention.latest_fanbasis_subscription_status,
        latest_retention.latest_fanbasis_service_type,
        latest_retention.latest_fanbasis_payment_frequency_days,
        latest_retention.latest_fanbasis_product_title,
        latest_retention.fanbasis_customer_ids,
        latest_retention.fanbasis_subscription_ids,
        latest_retention.fanbasis_directory_customer_ids,
        latest_retention.fanbasis_directory_total_spent,
        latest_retention.fanbasis_directory_total_transactions,

        latest_retention.retention_quality_flag,
        latest_retention.revenue_funnel_quality_flag,
        revenue.purchase_magnet_attribution_flag AS revenue_purchase_magnet_attribution_flag,
        revenue.latest_booking_before_first_purchase_at AS booking_before_purchase_at,
        FORMAT_TIMESTAMP('%b %e, %Y', revenue.latest_booking_before_first_purchase_at, 'America/New_York') AS booking_before_purchase_label,
        revenue.hours_latest_booking_to_purchase AS hours_booking_to_purchase,
        revenue.pre_purchase_funnel_path AS sales_path_to_purchase,
        FORMAT_TIMESTAMP('%FT%TZ', latest_retention.mart_refreshed_at) AS mart_refreshed_at,
        latest_retention.contact_sk IS NOT NULL OR revenue.contact_sk IS NOT NULL OR contact.contact_sk IS NOT NULL AS has_customer_source
      FROM anchor
      LEFT JOIN latest_retention
        ON anchor.contact_sk = latest_retention.contact_sk
      LEFT JOIN revenue
        ON anchor.contact_sk = revenue.contact_sk
      LEFT JOIN contract_evidence
        ON anchor.contact_sk = contract_evidence.contact_sk
      LEFT JOIN contract_terms_latest
        ON anchor.contact_sk = contract_terms_latest.contact_sk
      LEFT JOIN contact
        ON anchor.contact_sk = contact.contact_sk
    `,
    customer_360_payments: `
      SELECT
        payment_id,
        source_platform,
        FORMAT_TIMESTAMP('%b %e, %Y', transaction_date, 'America/New_York') AS transaction_label,
        FORMAT_TIMESTAMP('%FT%TZ', transaction_date) AS transaction_date,
        gross_amount,
        net_amount,
        currency,
        source_presentment_gross_amount,
        source_presentment_net_amount,
        source_presentment_currency,
        product,
        payment_method,
        source_service_payment_id,
        source_fund_released,
        is_paid,
        is_refunded,
        match_method,
        match_score,
        bridge_status
      FROM ${paymentsTable}
      WHERE contact_sk = '${contactSk}'
      ORDER BY transaction_date DESC
      LIMIT 30
    `,
    customer_360_refunds: `
      SELECT
        refund_id,
        parent_payment_id,
        source_platform,
        FORMAT_TIMESTAMP('%b %e, %Y', refunded_at, 'America/New_York') AS refunded_label,
        FORMAT_TIMESTAMP('%FT%TZ', refunded_at) AS refunded_at,
        refund_amount,
        refund_amount_net,
        refund_fee,
        currency,
        match_method,
        bridge_status
      FROM ${refundsTable}
      WHERE contact_sk = '${contactSk}'
      ORDER BY refunded_at DESC
      LIMIT 20
    `,
    customer_360_outreach: `
      SELECT
        outreach.touch_sk,
        outreach.message_id,
        outreach.conversation_id,
        FORMAT_TIMESTAMP('%b %e, %Y %l:%M %p', outreach.touched_at, 'America/New_York') AS touched_label,
        FORMAT_TIMESTAMP('%FT%TZ', outreach.touched_at) AS touched_at,
        outreach.channel,
        outreach.message_status,
        outreach.message_source,
        users.name AS user_name,
        users.role AS user_role
      FROM ${outreachTable} AS outreach
      LEFT JOIN ${usersTable} AS users
        ON outreach.user_sk = users.user_sk
      WHERE outreach.contact_sk = '${contactSk}'
      ORDER BY outreach.touched_at DESC
      LIMIT 30
    `,
    customer_360_bookings: `
      SELECT
        bookings.calendly_event_id,
        bookings.event_name,
        FORMAT_TIMESTAMP('%b %e, %Y %l:%M %p', bookings.booked_at, 'America/New_York') AS booked_label,
        FORMAT_TIMESTAMP('%b %e, %Y %l:%M %p', bookings.scheduled_for, 'America/New_York') AS scheduled_label,
        FORMAT_TIMESTAMP('%FT%TZ', bookings.booked_at) AS booked_at,
        FORMAT_TIMESTAMP('%FT%TZ', bookings.scheduled_for) AS scheduled_for,
        bookings.event_status,
        FORMAT_TIMESTAMP('%b %e, %Y', bookings.cancelled_at, 'America/New_York') AS cancelled_label,
        bookings.location_type,
        users.name AS assigned_user_name,
        users.role AS assigned_user_role,
        pipeline_stages.pipeline_name,
        pipeline_stages.stage_name
      FROM ${bookingsTable} AS bookings
      LEFT JOIN ${usersTable} AS users
        ON bookings.assigned_user_sk = users.user_sk
      LEFT JOIN ${pipelineStagesTable} AS pipeline_stages
        ON bookings.pipeline_stage_sk = pipeline_stages.pipeline_stage_sk
      WHERE bookings.contact_sk = '${contactSk}'
      ORDER BY bookings.booked_at DESC
      LIMIT 20
    `,
    customer_360_magnet_trail: `
      SELECT
        opportunity_id,
        lead_magnet_name,
        lead_magnet_category,
        lead_magnet_offer_type,
        lead_magnet_stage_name,
        opportunity_status,
        FORMAT_TIMESTAMP('%b %e, %Y', opportunity_created_at, 'America/New_York') AS opportunity_created_label,
        FORMAT_TIMESTAMP('%b %e, %Y', opportunity_updated_at, 'America/New_York') AS opportunity_updated_label,
        assigned_user_name,
        assigned_user_role,
        touches_count,
        call_count,
        sms_count,
        successful_call_count,
        direct_bookings_count,
        window_bookings_count,
        canceled_bookings_count,
        payment_count,
        gross_revenue,
        net_revenue_after_refunds,
        attribution_quality_flag
      FROM ${leadMagnetTable}
      WHERE contact_sk = '${contactSk}'
      ORDER BY opportunity_created_at DESC
      LIMIT 15
    `,
    customer_360_retention_months: `
      SELECT
        FORMAT_DATE('%b %Y', activity_month) AS activity_month_label,
        retention_state,
        paid_payments_in_month,
        fanbasis_payments_in_month,
        stripe_payments_in_month,
        refunds_count_in_month,
        net_revenue_after_refunds_in_month,
        cumulative_net_revenue_after_refunds,
        is_first_purchase_month,
        is_repeat_paid_month,
        is_current_month
      FROM ${retentionTable}
      WHERE contact_sk = '${contactSk}'
      ORDER BY activity_month DESC
      LIMIT 18
    `,
    customer_360_relationship_timeline: `
      WITH
      ${latestRetention},
      ${revenue},
      ${contractEvidence},
      ${contractTermsLatest},
      first_magnet AS (
        SELECT
          opportunity_id,
          lead_magnet_name,
          lead_magnet_category,
          lead_magnet_offer_type,
          lead_magnet_stage_name,
          opportunity_status,
          opportunity_created_at,
          assigned_user_name,
          attribution_quality_flag
        FROM ${leadMagnetTable}
        WHERE contact_sk = '${contactSk}'
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY contact_sk
          ORDER BY opportunity_created_at ASC NULLS LAST, opportunity_id
        ) = 1
      ),
      first_booking AS (
        SELECT
          calendly_event_id,
          event_name,
          booked_at,
          scheduled_for,
          event_status,
          users.name AS assigned_user_name,
          pipeline_stages.stage_name
        FROM ${bookingsTable} AS bookings
        LEFT JOIN ${usersTable} AS users
          ON bookings.assigned_user_sk = users.user_sk
        LEFT JOIN ${pipelineStagesTable} AS pipeline_stages
          ON bookings.pipeline_stage_sk = pipeline_stages.pipeline_stage_sk
        WHERE bookings.contact_sk = '${contactSk}'
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY bookings.contact_sk
          ORDER BY COALESCE(bookings.scheduled_for, bookings.booked_at) ASC NULLS LAST, bookings.calendly_event_id
        ) = 1
      ),
      first_successful_call AS (
        SELECT
          outreach.touch_sk,
          outreach.touched_at,
          outreach.message_status,
          outreach.message_source,
          users.name AS user_name,
          users.role AS user_role
        FROM ${outreachTable} AS outreach
        LEFT JOIN ${usersTable} AS users
          ON outreach.user_sk = users.user_sk
        WHERE
          outreach.contact_sk = '${contactSk}'
          AND LOWER(COALESCE(outreach.channel, '')) = 'call'
          AND LOWER(COALESCE(outreach.message_status, '')) IN ('answered', 'completed', 'connected')
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY outreach.contact_sk
          ORDER BY outreach.touched_at ASC NULLS LAST, outreach.touch_sk
        ) = 1
      ),
      first_purchase AS (
        SELECT
          payment_id,
          source_platform,
          transaction_date,
          net_amount,
          currency,
          product
        FROM ${paymentsTable}
        WHERE
          contact_sk = '${contactSk}'
          AND is_paid
          AND transaction_date IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY contact_sk
          ORDER BY transaction_date ASC, payment_id
        ) = 1
      ),
      later_payments AS (
        SELECT *
        FROM (
          SELECT
            payments.payment_id,
            payments.source_platform,
            payments.transaction_date,
            payments.net_amount,
            payments.currency,
            payments.product,
            ROW_NUMBER() OVER (
              ORDER BY payments.transaction_date ASC, payments.payment_id
            ) AS later_payment_sequence
          FROM ${paymentsTable} AS payments
          CROSS JOIN first_purchase
          WHERE
            payments.contact_sk = '${contactSk}'
            AND payments.is_paid
            AND payments.transaction_date > first_purchase.transaction_date
        )
        WHERE later_payment_sequence <= 8
      ),
      refund_events AS (
        SELECT *
        FROM (
          SELECT
            refund_id,
            parent_payment_id,
            source_platform,
            refunded_at,
            refund_amount_net,
            currency,
            ROW_NUMBER() OVER (
              ORDER BY refunded_at ASC, refund_id
            ) AS refund_sequence
          FROM ${refundsTable}
          WHERE
            contact_sk = '${contactSk}'
            AND refunded_at IS NOT NULL
        )
        WHERE refund_sequence <= 5
      ),
      events AS (
        SELECT
          opportunity_created_at AS event_sort_at,
          10 AS event_order,
          'lead_magnet' AS event_type,
          'Lead magnet' AS event_label,
          COALESCE(lead_magnet_name, 'First known lead magnet') AS event_title,
          CONCAT(
            COALESCE(lead_magnet_category, 'Uncategorized'),
            ' · ',
            COALESCE(lead_magnet_offer_type, 'Unknown offer'),
            ' · ',
            COALESCE(opportunity_status, 'unknown status')
          ) AS event_detail,
          CAST(NULL AS NUMERIC) AS event_amount,
          'lead_magnet_detail' AS event_source,
          COALESCE(attribution_quality_flag, 'unknown') AS event_quality,
          CAST(opportunity_id AS STRING) AS event_id
        FROM first_magnet
        WHERE opportunity_created_at IS NOT NULL

        UNION ALL

        SELECT
          COALESCE(scheduled_for, booked_at) AS event_sort_at,
          20 AS event_order,
          'booking' AS event_type,
          'First booking' AS event_label,
          COALESCE(event_name, 'First Calendly booking') AS event_title,
          CONCAT(
            COALESCE(event_status, 'unknown status'),
            ' · ',
            COALESCE(assigned_user_name, 'No assigned user'),
            ' · ',
            COALESCE(stage_name, 'No stage')
          ) AS event_detail,
          CAST(NULL AS NUMERIC) AS event_amount,
          'fct_calls_booked' AS event_source,
          COALESCE(event_status, 'unknown') AS event_quality,
          CAST(calendly_event_id AS STRING) AS event_id
        FROM first_booking
        WHERE COALESCE(scheduled_for, booked_at) IS NOT NULL

        UNION ALL

        SELECT
          touched_at AS event_sort_at,
          30 AS event_order,
          'successful_call' AS event_type,
          'First successful call' AS event_label,
          CONCAT('Call ', COALESCE(message_status, 'completed')) AS event_title,
          CONCAT(
            COALESCE(user_name, 'Unknown rep'),
            ' · ',
            COALESCE(message_source, 'unknown source')
          ) AS event_detail,
          CAST(NULL AS NUMERIC) AS event_amount,
          'fct_outreach' AS event_source,
          COALESCE(message_status, 'unknown') AS event_quality,
          CAST(touch_sk AS STRING) AS event_id
        FROM first_successful_call
        WHERE touched_at IS NOT NULL

        UNION ALL

        SELECT
          first_purchase.transaction_date AS event_sort_at,
          40 AS event_order,
          'first_purchase' AS event_type,
          'First purchase' AS event_label,
          COALESCE(first_purchase.product, 'First paid payment') AS event_title,
          CONCAT(
            COALESCE(first_purchase.source_platform, 'unknown source'),
            ' · ',
            COALESCE(first_purchase.currency, 'USD')
          ) AS event_detail,
          SAFE_CAST(first_purchase.net_amount AS NUMERIC) AS event_amount,
          'fct_payments' AS event_source,
          'paid' AS event_quality,
          CAST(first_purchase.payment_id AS STRING) AS event_id
        FROM first_purchase

        UNION ALL

        SELECT
          transaction_date AS event_sort_at,
          50 AS event_order,
          'repeat_payment' AS event_type,
          'Later payment' AS event_label,
          COALESCE(product, 'Later paid payment') AS event_title,
          CONCAT(
            'Payment #',
            CAST(later_payment_sequence + 1 AS STRING),
            ' · ',
            COALESCE(source_platform, 'unknown source')
          ) AS event_detail,
          SAFE_CAST(net_amount AS NUMERIC) AS event_amount,
          'fct_payments' AS event_source,
          'paid' AS event_quality,
          CAST(payment_id AS STRING) AS event_id
        FROM later_payments
        WHERE transaction_date IS NOT NULL

        UNION ALL

        SELECT
          refunded_at AS event_sort_at,
          60 AS event_order,
          'refund' AS event_type,
          'Refund' AS event_label,
          CONCAT('Refund from ', COALESCE(source_platform, 'unknown source')) AS event_title,
          CONCAT('Parent payment ', COALESCE(CAST(parent_payment_id AS STRING), 'unknown')) AS event_detail,
          SAFE_CAST(refund_amount_net AS NUMERIC) AS event_amount,
          'fct_refunds' AS event_source,
          'refund' AS event_quality,
          CAST(refund_id AS STRING) AS event_id
        FROM refund_events
        WHERE refunded_at IS NOT NULL

        UNION ALL

        SELECT
          contract_evidence.latest_payment_terms_call_at AS event_sort_at,
          70 AS event_order,
          'contract_terms_evidence' AS event_type,
          'Contract evidence' AS event_label,
          'Payment terms found in transcript' AS event_title,
          CONCAT(
            COALESCE(contract_evidence.mentioned_payment_amounts_text, 'No amount extracted'),
            ' · ',
            COALESCE(CAST(contract_evidence.payment_terms_snippets_count AS STRING), '0'),
            ' snippets'
          ) AS event_detail,
          SAFE_CAST(contract_evidence.largest_mentioned_payment_amount AS NUMERIC) AS event_amount,
          'collection_contract_evidence_detail' AS event_source,
          COALESCE(contract_evidence.contract_evidence_status, 'unknown') AS event_quality,
          CAST(contract_evidence.collection_contract_evidence_sk AS STRING) AS event_id
        FROM contract_evidence
        WHERE contract_evidence.latest_payment_terms_call_at IS NOT NULL
          AND contract_evidence.contract_evidence_status = 'transcript_payment_terms_found'

        UNION ALL

        SELECT
          SAFE_CAST(contract_terms_latest.reviewed_at AS TIMESTAMP) AS event_sort_at,
          80 AS event_order,
          'contract_terms_confirmed' AS event_type,
          'Contract confirmed' AS event_label,
          'Human-confirmed contract terms' AS event_title,
          CONCAT(
            'Promised ',
            COALESCE(CAST(contract_terms_latest.promised_contract_value AS STRING), 'unknown'),
            ' · Balance ',
            COALESCE(CAST(contract_terms_latest.balance_expected_amount AS STRING), 'unknown')
          ) AS event_detail,
          SAFE_CAST(contract_terms_latest.promised_contract_value AS NUMERIC) AS event_amount,
          'contract_terms_reviews' AS event_source,
          COALESCE(contract_terms_latest.review_confidence, 'unknown') AS event_quality,
          CAST(contract_terms_latest.contact_sk AS STRING) AS event_id
        FROM contract_terms_latest
        WHERE contract_terms_latest.reviewed_at IS NOT NULL

        UNION ALL

        SELECT
          latest_retention.mart_refreshed_at AS event_sort_at,
          90 AS event_order,
          'current_status' AS event_type,
          'Current status' AS event_label,
          COALESCE(latest_retention.retention_operator_next_action, 'Monitor') AS event_title,
          CONCAT(
            COALESCE(latest_retention.collection_health_status, latest_retention.payment_plan_health_status, 'unknown health'),
            ' · ',
            COALESCE(latest_retention.customer_lifecycle_status, 'unknown lifecycle')
          ) AS event_detail,
          SAFE_CAST(latest_retention.lifetime_net_revenue_after_refunds AS NUMERIC) AS event_amount,
          'customer_retention_detail' AS event_source,
          COALESCE(latest_retention.retention_quality_flag, 'unknown') AS event_quality,
          CAST(latest_retention.contact_sk AS STRING) AS event_id
        FROM latest_retention
        WHERE latest_retention.mart_refreshed_at IS NOT NULL
      )
      SELECT
        event_type,
        event_label,
        event_title,
        event_detail,
        FORMAT_TIMESTAMP('%b %e, %Y', event_sort_at, 'America/New_York') AS event_date_label,
        FORMAT_TIMESTAMP('%b %e, %Y %l:%M %p', event_sort_at, 'America/New_York') AS event_time_label,
        FORMAT_TIMESTAMP('%FT%TZ', event_sort_at) AS event_at,
        event_amount,
        event_source,
        event_quality,
        event_order,
        event_id
      FROM events
      WHERE event_sort_at IS NOT NULL
      ORDER BY event_sort_at, event_order
      LIMIT 40
    `,
    customer_360_operator_actions: `
      WITH
      ${latestRetention},
      ${revenue},
      ${contractEvidence},
      ${contractTermsLatest},
      latest_reviews AS (
        SELECT
          area,
          queue_name,
          COALESCE(contact_sk, entity_id) AS contact_sk,
          action_bucket,
          review_status,
          review_note,
          reviewed_by,
          FORMAT_TIMESTAMP('%b %e, %Y', reviewed_at, 'America/New_York') AS reviewed_label,
          FORMAT_TIMESTAMP('%FT%TZ', reviewed_at) AS reviewed_at,
          FORMAT_TIMESTAMP('%FT%TZ', expires_at) AS expires_at
        FROM (
          SELECT
            area,
            queue_name,
            contact_sk,
            entity_id,
            action_bucket,
            LOWER(review_status) AS review_status,
            review_note,
            reviewed_by,
            reviewed_at,
            expires_at,
            ROW_NUMBER() OVER (
              PARTITION BY area, queue_name, COALESCE(contact_sk, entity_id), action_bucket
              ORDER BY reviewed_at DESC
            ) AS review_rank
          FROM ${operatorActionReviewsTable}
          WHERE
            entity_type = 'customer'
            AND COALESCE(contact_sk, entity_id) = '${contactSk}'
            AND LOWER(review_status) IN ('open', 'reviewed', 'fixed', 'wont_fix')
        )
        WHERE review_rank = 1
      ),
      revenue_actions AS (
        SELECT
          'revenue' AS area,
          'revenue_action_queue' AS queue_name,
          contact_sk,
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
          END AS operator_next_action,
          FORMAT_TIMESTAMP('%b %e, %Y', first_purchase_at, 'America/New_York') AS source_date_label,
          total_net_revenue_after_refunds AS money_at_stake,
          'revenue_funnel_detail' AS source_table
        FROM revenue
        WHERE
          revenue_funnel_quality_flag != 'clean'
          OR credited_closer_source = 'unassigned'
          OR credited_closer_confidence IN ('low', 'missing')
          OR top_product_family = 'Unknown / historical Stripe'
          OR payment_plan_truth_status IN ('fanbasis_auto_renew_cash_only', 'name_inferred_plan_cash_only')
      ),
      retention_actions AS (
        SELECT
          'retention' AS area,
          'retention_worklist' AS queue_name,
          contact_sk,
          CASE
            WHEN retention_operator_next_action IN ('review_manual_collection', 'confirm_repeat_or_upsell', 'monitor_manual_collection')
              THEN collection_health_status
            ELSE payment_plan_health_status
          END AS action_bucket,
          CASE
            WHEN retention_operator_next_action IN ('review_manual_collection', 'confirm_repeat_or_upsell', 'monitor_manual_collection')
              THEN CASE collection_health_status
                WHEN 'manual_collection_stale_review' THEN 'Manual collection stale'
                WHEN 'collection_call_no_payment_review' THEN 'Collection call, no pay'
                WHEN 'plan_named_collection_review' THEN 'Plan-named cash only'
                WHEN 'repeat_or_upsell_review' THEN 'Repeat or upsell review'
                WHEN 'manual_collection_recently_collected' THEN 'Manual collection current'
                ELSE INITCAP(REPLACE(collection_health_status, '_', ' '))
              END
            WHEN payment_plan_health_status = 'failed_plan_recovery_needed' THEN 'Failed plan recovery'
            WHEN payment_plan_health_status = 'active_plan_due_no_payment_yet' THEN 'Active plan due, no payment'
            WHEN payment_plan_health_status = 'active_plan_not_yet_due' THEN 'Active plan not yet due'
            WHEN payment_plan_health_status = 'active_plan_paid_current_month' THEN 'Active plan paid this month'
            WHEN payment_plan_health_status = 'completed_plan_paid_off' THEN 'Completed / paid off'
            WHEN payment_plan_health_status = 'repeat_payment_observed' THEN 'Repeat payment observed'
            WHEN payment_plan_health_status = 'one_time_upsell_candidate' THEN 'One-time upsell candidate'
            WHEN payment_plan_health_status = 'historical_stripe_product_review' THEN 'Historical Stripe product repair'
            WHEN payment_plan_health_status = 'review_negative_value' THEN 'Review negative value'
            WHEN payment_plan_health_status = 'no_repeat_expected_yet' THEN 'No repeat expected yet'
            ELSE INITCAP(REPLACE(payment_plan_health_status, '_', ' '))
          END AS action_bucket_label,
          CASE retention_operator_next_action
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
          END AS action_reason,
          CASE
            WHEN payment_plan_health_status = 'failed_plan_recovery_needed' THEN 1
            WHEN payment_plan_health_status = 'active_plan_due_no_payment_yet' THEN 2
            WHEN payment_plan_health_status = 'review_negative_value' THEN 3
            WHEN payment_plan_health_status = 'historical_stripe_product_review' THEN 4
            WHEN payment_plan_health_status = 'completed_plan_paid_off' THEN 5
            WHEN payment_plan_health_status = 'one_time_upsell_candidate' THEN 6
            WHEN collection_health_status IN ('manual_collection_stale_review', 'collection_call_no_payment_review', 'plan_named_collection_review', 'repeat_or_upsell_review') THEN 6
            WHEN payment_plan_health_status = 'active_plan_not_yet_due' THEN 7
            WHEN payment_plan_health_status = 'repeat_payment_observed' THEN 8
            ELSE 9
          END AS action_priority,
          CASE retention_operator_next_action
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
          END AS operator_next_action,
          FORMAT_TIMESTAMP('%b %e, %Y', latest_purchase_at, 'America/New_York') AS source_date_label,
          lifetime_net_revenue_after_refunds AS money_at_stake,
          'customer_retention_detail' AS source_table
        FROM latest_retention
        WHERE
          payment_plan_health_status IN (
            'failed_plan_recovery_needed',
            'active_plan_due_no_payment_yet',
            'one_time_upsell_candidate',
            'completed_plan_paid_off',
            'historical_stripe_product_review',
            'active_plan_not_yet_due',
            'repeat_payment_observed',
            'review_negative_value'
          )
          OR collection_health_status IN (
            'manual_collection_stale_review',
            'collection_call_no_payment_review',
            'plan_named_collection_review',
            'repeat_or_upsell_review'
          )
      ),
      contract_terms_actions AS (
        SELECT
          'retention' AS area,
          'retention_worklist' AS queue_name,
          contract_evidence.contact_sk,
          'contract_terms_review' AS action_bucket,
          'Contract terms review' AS action_bucket_label,
          'Confirm promised value, deposit/upfront amount, expected balance, and confidence from transcript evidence' AS action_reason,
          6 AS action_priority,
          'Review contract terms' AS operator_next_action,
          FORMAT_TIMESTAMP('%b %e, %Y', contract_evidence.latest_payment_terms_call_at, 'America/New_York') AS source_date_label,
          contract_evidence.lifetime_net_revenue_after_refunds AS money_at_stake,
          'collection_contract_evidence_detail' AS source_table
        FROM contract_evidence
        LEFT JOIN contract_terms_latest
          ON contract_terms_latest.contact_sk = contract_evidence.contact_sk
        WHERE contract_evidence.contract_evidence_status = 'transcript_payment_terms_found'
          AND contract_terms_latest.contact_sk IS NULL
      ),
      action_candidates AS (
        SELECT * FROM revenue_actions
        UNION ALL
        SELECT * FROM retention_actions
        UNION ALL
        SELECT * FROM contract_terms_actions
      )
      SELECT
        action_candidates.*,
        COALESCE(latest_reviews.review_status, 'open') AS review_status,
        latest_reviews.review_note,
        latest_reviews.reviewed_by,
        latest_reviews.reviewed_label,
        latest_reviews.reviewed_at,
        latest_reviews.expires_at
      FROM action_candidates
      LEFT JOIN latest_reviews
        ON latest_reviews.area = action_candidates.area
        AND latest_reviews.queue_name = action_candidates.queue_name
        AND latest_reviews.contact_sk = action_candidates.contact_sk
        AND latest_reviews.action_bucket = action_candidates.action_bucket
      WHERE
        COALESCE(latest_reviews.review_status, 'open') NOT IN ('fixed', 'wont_fix')
        OR (latest_reviews.expires_at IS NOT NULL AND TIMESTAMP(latest_reviews.expires_at) <= CURRENT_TIMESTAMP())
      ORDER BY action_priority, ABS(money_at_stake) DESC
    `,
  } satisfies Record<string, string>;
}

export async function getCustomer360Data(options: GetCustomer360DataOptions): Promise<DashboardData> {
  const generatedAt = new Date().toISOString();
  const filters = buildDashboardFilters();
  const dataset = martDataset();

  try {
    assertContactSk(options.contactSk);
    const queries = buildCustomer360Queries(options.contactSk);

    const [
      profile,
      payments,
      refunds,
      outreach,
      bookings,
      magnetTrail,
      retentionMonths,
      relationshipTimeline,
      operatorActions,
    ] = await Promise.all([
      runBigQuery(queries.customer_360_profile),
      runBigQuery(queries.customer_360_payments),
      runBigQuery(queries.customer_360_refunds),
      runBigQuery(queries.customer_360_outreach),
      runBigQuery(queries.customer_360_bookings),
      runBigQuery(queries.customer_360_magnet_trail),
      runBigQuery(queries.customer_360_retention_months),
      runBigQuery(queries.customer_360_relationship_timeline),
      runBigQuery(queries.customer_360_operator_actions),
    ]);

    if (!profile[0]?.has_customer_source) {
      throw new Error("Customer was not found in the current mart layer.");
    }

    return {
      rows: {
        customer_360_profile: profile,
        customer_360_payments: payments,
        customer_360_refunds: refunds,
        customer_360_outreach: outreach,
        customer_360_bookings: bookings,
        customer_360_magnet_trail: magnetTrail,
        customer_360_retention_months: retentionMonths,
        customer_360_relationship_timeline: relationshipTimeline,
        customer_360_operator_actions: operatorActions,
      },
      freshness: buildFreshness(profile),
      filters,
      generatedAt,
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: [
          "customer_retention_detail",
          "collection_contract_evidence_detail",
          "contract_terms_reviews",
          "revenue_funnel_detail",
          "fct_payments",
          "fct_refunds",
          "fct_outreach",
          "fct_calls_booked",
          "lead_magnet_detail",
          "operator_action_reviews",
        ],
        note:
          "Customer 360 is a drilldown view over existing marts and facts. It does not create new attribution; it exposes the source evidence already modeled for the customer.",
      },
    };
  } catch (error) {
    return {
      rows: {},
      freshness: {
        status: "error",
        label: "Customer data unavailable",
        detail: getErrorMessage(error),
      },
      filters,
      generatedAt,
      error: getErrorMessage(error),
      dataContract: {
        owner: "dbt-mart",
        projectId: deeConfig.bigQuery.projectId,
        dataset,
        tables: ["customer_retention_detail", "collection_contract_evidence_detail", "contract_terms_reviews", "revenue_funnel_detail"],
        note:
          "Customer 360 is a drilldown view over existing marts and facts. It does not create new attribution; it exposes the source evidence already modeled for the customer.",
      },
    };
  }
}

function buildFreshness(profile: DashboardRow[]): DashboardFreshness {
  const refreshedAt = stringValue(profile[0]?.mart_refreshed_at);

  if (!refreshedAt) {
    return {
      status: "stale",
      label: "No refresh timestamp",
      detail: "The customer profile returned rows without a mart refresh timestamp.",
    };
  }

  const refreshedDate = new Date(refreshedAt);
  const ageMinutes = Math.max(0, Math.round((Date.now() - refreshedDate.getTime()) / 60000));
  const status = ageMinutes <= 180 ? "live" : "stale";

  return {
    status,
    label: status === "live" ? "Live dbt mart data" : "Stale dbt mart data",
    detail: `Customer marts refreshed ${formatAge(ageMinutes)} ago.`,
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
  return "Unknown Customer 360 error";
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}
