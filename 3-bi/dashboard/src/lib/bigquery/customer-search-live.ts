import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardRow } from "@/types/dashboard-data";

const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function martDataset() {
  const dataset =
    process.env.BIGQUERY_CUSTOMER_360_DATASET ??
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for Customer Search: ${dataset}`);
  }

  return dataset;
}

function tableRef(dataset: string, tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.${dataset}.${tableName}\``;
}

function normalizedQuery(value: string | null | undefined) {
  return value?.trim().toLowerCase().slice(0, 120) ?? "";
}

export async function searchCustomers(query: string | null | undefined): Promise<DashboardRow[]> {
  const q = normalizedQuery(query);
  if (q.length < 2) return [];

  const contactsTable = tableRef("Core", "dim_contacts");
  const retentionTable = tableRef(martDataset(), "customer_retention_detail");
  const revenueTable = tableRef(martDataset(), "revenue_funnel_detail");

  const sql = `
    WITH
    latest_retention AS (
      SELECT *
      FROM ${retentionTable}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY activity_month DESC
      ) = 1
    ),
    revenue AS (
      SELECT *
      FROM ${revenueTable}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY first_purchase_at DESC
      ) = 1
    ),
    contacts AS (
      SELECT *
      FROM ${contactsTable}
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY contact_sk
        ORDER BY contact_updated_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
      ) = 1
    ),
    joined AS (
      SELECT
        COALESCE(contacts.contact_sk, latest_retention.contact_sk, revenue.contact_sk) AS contact_sk,
        COALESCE(contacts.contact_id, latest_retention.contact_id, revenue.contact_id) AS contact_id,
        COALESCE(
          NULLIF(latest_retention.contact_name, ''),
          NULLIF(revenue.contact_name, ''),
          NULLIF(contacts.contact_name, ''),
          NULLIF(contacts.email_norm, ''),
          NULLIF(latest_retention.email_norm, ''),
          NULLIF(revenue.email_norm, ''),
          NULLIF(contacts.phone, ''),
          'Unknown customer'
        ) AS customer_display_name,
        COALESCE(contacts.email_norm, latest_retention.email_norm, revenue.email_norm) AS email_norm,
        COALESCE(contacts.phone, latest_retention.phone, revenue.phone) AS phone,
        latest_retention.payment_plan_health_status,
        latest_retention.retention_operator_next_action,
        latest_retention.customer_lifecycle_status,
        COALESCE(
          latest_retention.lifetime_net_revenue_after_refunds,
          revenue.total_net_revenue_after_refunds,
          0
        ) AS lifetime_net_revenue_after_refunds,
        COALESCE(latest_retention.top_product_by_net_revenue, revenue.top_product_by_net_revenue) AS top_product_by_net_revenue,
        COALESCE(latest_retention.latest_prior_lead_magnet_name, revenue.latest_prior_lead_magnet_name) AS latest_prior_lead_magnet_name,
        COALESCE(latest_retention.credited_closer_name, revenue.credited_closer_name) AS credited_closer_name,
        latest_retention.contact_sk IS NOT NULL AS has_retention,
        revenue.contact_sk IS NOT NULL AS has_revenue
      FROM contacts
      FULL OUTER JOIN latest_retention
        ON contacts.contact_sk = latest_retention.contact_sk
      FULL OUTER JOIN revenue
        ON COALESCE(contacts.contact_sk, latest_retention.contact_sk) = revenue.contact_sk
    )
    SELECT *
    FROM joined
    WHERE
      LOWER(COALESCE(customer_display_name, '')) LIKE @like_query
      OR LOWER(COALESCE(email_norm, '')) LIKE @like_query
      OR REGEXP_REPLACE(COALESCE(phone, ''), r'[^0-9]', '') LIKE @digits_like_query
      OR LOWER(COALESCE(contact_id, '')) = @query
      OR LOWER(COALESCE(contact_sk, '')) = @query
    ORDER BY
      LOWER(COALESCE(email_norm, '')) = @query DESC,
      LOWER(COALESCE(customer_display_name, '')) = @query DESC,
      has_revenue DESC,
      has_retention DESC,
      lifetime_net_revenue_after_refunds DESC
    LIMIT 8
  `;

  const digits = q.replace(/\D/g, "");

  return runBigQuery(sql, {
    query: q,
    like_query: `%${q}%`,
    digits_like_query: digits.length >= 2 ? `%${digits}%` : "__NO_DIGIT_MATCH__",
  });
}
