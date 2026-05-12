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
    | "lead_magnet_performance_summary"
    | "fct_lead_magnet_activity"
    | "fct_speed_to_lead",
) {
  return `\`${deeConfig.bigQuery.projectId}.${leadMagnetDataset()}.${tableName}\``;
}

function coreTableRef(
  tableName:
    | "dim_ghl_contacts"
    | "dim_ghl_contacts_first_touch"
    | "bridge_calendly_invitee_contacts"
    | "fct_calendly_event_invitees"
    | "fct_ghl_opportunities",
) {
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

function activityNameToReportingNameSql(field: string) {
  return `
    CASE
      WHEN ${field} = 'AI Brand Prompts' THEN 'AI Brand Building Prompts'
      WHEN ${field} = 'Google Doc Lead Magnets' THEN 'Google Docs'
      WHEN ${field} = 'IG BuildsBrands Content Guide' THEN 'Content Guide'
      WHEN ${field} = 'IG Manu List' THEN 'Manufacturer List'
      WHEN ${field} = 'IG SMS Templates' THEN 'SMS Templates'
      WHEN ${field} = 'IG DBB Manu Comm Templates' THEN 'Manufacturer Communication Templates'
      WHEN ${field} IN ('IG DBB Tarriff', 'IG MOD Tarriff') THEN 'Tariff Manufacturer List'
      WHEN ${field} = 'IG DBB Drop Blueprint' THEN 'Drop Blueprint'
      WHEN ${field} = 'Webinar Replay Opt-In' THEN 'Webinar Replay'
      WHEN ${field} IN ('NEW BB IG DM Free Training', 'NEW BB IG Free Training') THEN 'New Free Training Funnel'
      ELSE ${field}
    END
  `;
}

function contactSourceToReportingNameSql(field: string) {
  return `
    CASE
      WHEN ${field} IN ('ig blueprint case study', 'free case study blueprint ig', 'ig blueprint') THEN 'IG Blueprint Case Study'
      WHEN ${field} = 'tt blueprint case study' THEN 'TT Blueprint Case Study'
      WHEN ${field} = 'yt blueprint case study' THEN 'YT Blueprint Case Study'
      WHEN ${field} IN ('ig buildsbrands content guide', 'content guide') THEN 'Content Guide'
      WHEN ${field} IN ('ai brand prompts', 'ai brand building prompts', 'prompts') THEN 'AI Brand Building Prompts'
      WHEN ${field} IN ('ig sms templates', 'sms') THEN 'SMS Templates'
      WHEN ${field} = 'ig manu list' THEN 'Manufacturer List'
      WHEN ${field} IN ('ig mod tarriff', 'ig dbb tarriff') THEN 'Tariff Manufacturer List'
      WHEN ${field} IN ('google doc lead magnets', 'google docs', 'doc') THEN 'Google Docs'
      WHEN ${field} IN ('ig dbb manu comm templates', 'manu comm templates') THEN 'Manufacturer Communication Templates'
      WHEN ${field} = 'ig dbb drop blueprint' THEN 'Drop Blueprint'
      WHEN ${field} IN ('bb ig free training', 'bb ig dm free training', 'new bb ig dm free training', 'mod ig free training', 'dee free training') THEN 'New Free Training Funnel'
      WHEN ${field} IN ('giveaway + free class', 'giveaway-free-class') THEN 'Giveaway + Free Class'
      WHEN ${field} = 'ig giveaway' THEN 'IG Giveaway'
      WHEN ${field} = 'tt giveaway' THEN 'TT Giveaway'
      WHEN ${field} = 'yt giveaway' THEN 'YT Giveaway'
      WHEN ${field} = 'ig story giveaway' THEN 'IG Story Giveaway'
      WHEN ${field} = 'ig story blueprint case study' THEN 'IG Story Blueprint Case Study'
      WHEN ${field} = 'sms list blueprint case study' THEN 'SMS List Blueprint Case Study'
      WHEN ${field} = 'email list blueprint case study' THEN 'Email List Blueprint Case Study'
      WHEN ${field} = 'dee free guide' THEN 'Dee Free Guide'
      WHEN ${field} IN ('free-skool', 'free skool') THEN 'Free Skool'
      WHEN ${field} = 'ic 2.0 waitlist' THEN 'Inner Circle 2.0 Waitlist'
      WHEN ${field} IN ('lead magnet waitlist', 'dee leadmagnet waitlist') THEN 'Lead Magnet Waitlist'
      WHEN ${field} = 'dee instagram waitlist' THEN 'Dee Instagram Waitlist'
      WHEN ${field} = 'dee youtube waitlist' THEN 'Dee YouTube Waitlist'
      WHEN REGEXP_CONTAINS(${field}, r'/sms-new') THEN 'SMS Templates'
      ELSE NULL
    END
  `;
}

function buildLeadMagnetQueries(timeRange: LeadMagnetTimeRange) {
  const buyerTable = tableRef("lead_magnet_buyer_detail");
  const opportunityTable = tableRef("lead_magnet_detail");
  const performanceTable = tableRef("lead_magnet_performance_summary");
  const activityTable = tableRef("fct_lead_magnet_activity");
  const speedToLeadTable = tableRef("fct_speed_to_lead");
  const contactTable = coreTableRef("dim_ghl_contacts");
  const contactFirstTouchTable = coreTableRef("dim_ghl_contacts_first_touch");
  const calendlyBridgeTable = coreTableRef("bridge_calendly_invitee_contacts");
  const calendlyInviteeTable = coreTableRef("fct_calendly_event_invitees");
  const ghlOpportunityTable = coreTableRef("fct_ghl_opportunities");
  const buyerWhere = whereTimeRange(timeRange, "first_purchase_at");
  const opportunityWhere = whereTimeRange(timeRange, "opportunity_created_at");
  const activityWhere = whereTimeRange(timeRange, "event_ts");
  const buyerAliasPredicate = timestampRangePredicate(timeRange, "b.first_purchase_at");
  const buyerAliasAnd = buyerAliasPredicate ? `AND ${buyerAliasPredicate}` : "";
  const activityMappedPredicate = timestampRangePredicate(timeRange, "activity_mapped.event_ts");
  const activityMappedWhere = activityMappedPredicate ? `WHERE ${activityMappedPredicate}` : "";
  const bookingPredicate = timestampRangePredicate(timeRange, "COALESCE(d.first_booking_at, d.latest_booking_at)");
  const bookingAliasAnd = bookingPredicate ? `AND ${bookingPredicate}` : "";

  // The performance-summary mart is a current-snapshot table; it carries its
  // own internal denominator window inside the dbt model and does not respect
  // the dashboard time-range filter. Queue classification (queue_status,
  // kill_candidate, repair_candidate, retire_recommended_pending_override,
  // queue_exclusion_reason) is precomputed in dbt — we read it as-is and
  // never recompute thresholds in TypeScript.
  const queueStatusOrderSql = `
    CASE queue_status
      WHEN 'retire_recommended_pending_override' THEN 1
      WHEN 'kill_candidate' THEN 2
      WHEN 'repair_candidate' THEN 3
      WHEN 'healthy' THEN 4
      WHEN 'insufficient_sample' THEN 5
      ELSE 99
    END
  `;

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
    lead_magnet_influence_summary: `
      WITH form_activity AS (
        SELECT
          COUNT(*) AS form_events,
          COUNT(DISTINCT COALESCE(NULLIF(ghl_contact_id, ''), LOWER(contact_email))) AS unique_form_leads,
          COUNT(DISTINCT lead_magnet_name) AS distinct_form_magnets,
          MIN(event_ts) AS first_form_event_at,
          MAX(event_ts) AS latest_form_event_at,
          COUNTIF(is_backfill) AS backfilled_form_events,
          MAX(mart_refreshed_at) AS latest_activity_refreshed_at
        FROM ${activityTable}
        ${activityWhere}
      ),
      buyer_scope AS (
        SELECT *
        FROM ${buyerTable} AS b
        WHERE 1 = 1
          ${buyerAliasAnd}
      ),
      form_touch_buyers AS (
        SELECT DISTINCT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category
        FROM buyer_scope AS b
        INNER JOIN ${activityTable} AS a
          ON (
            a.ghl_contact_id = b.contact_id
            OR LOWER(a.contact_email) = b.email_norm
          )
        WHERE a.event_ts <= b.first_purchase_at
      ),
      opportunity_touch_buyers AS (
        SELECT DISTINCT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category
        FROM buyer_scope AS b
        INNER JOIN ${opportunityTable} AS d
          ON d.contact_id = b.contact_id
        WHERE d.lead_magnet_category IN ('true_lead_magnet', 'launch_event', 'waitlist')
          AND d.opportunity_created_at <= b.first_purchase_at
      ),
      contact_source_labels AS (
        SELECT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${contactTable} AS c
          ON c.contact_id = b.contact_id
        LEFT JOIN ${contactFirstTouchTable} AS ft
          ON ft.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          c.source,
          c.utm_source,
          c.utm_campaign,
          c.utm_content,
          ft.source_first,
          ft.utm_source_first,
          ft.utm_campaign_first,
          ft.utm_content_first
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) <= b.first_purchase_at
      ),
      contact_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          first_purchase_at,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM contact_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      speed_to_lead_source_labels AS (
        SELECT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          s.trigger_ts AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${speedToLeadTable} AS s
          ON s.ghl_contact_id = b.contact_id
        CROSS JOIN UNNEST([
          s.trigger_source_label,
          s.utm_source,
          s.utm_campaign,
          s.utm_content
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND s.trigger_type IN ('lead_magnet', 'appointment_booking')
          AND s.trigger_ts <= b.first_purchase_at
      ),
      speed_to_lead_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          first_purchase_at,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM speed_to_lead_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      calendly_source_labels AS (
        SELECT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${calendlyBridgeTable} AS bc
          ON bc.contact_id = b.contact_id
        LEFT JOIN ${calendlyInviteeTable} AS ci
          ON ci.invitee_id = bc.invitee_id
        CROSS JOIN UNNEST([
          bc.source_first,
          bc.utm_source_first,
          bc.utm_campaign_first,
          bc.utm_content_first,
          bc.utm_source,
          bc.utm_campaign,
          bc.utm_content,
          bc.matched_contact_source,
          ci.utm_source,
          ci.utm_campaign,
          ci.utm_content,
          ci.self_reported_source
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) <= b.first_purchase_at
      ),
      calendly_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          first_purchase_at,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM calendly_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      opportunity_first_touch_labels AS (
        SELECT
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${ghlOpportunityTable} AS o
          ON o.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          o.source,
          o.first_touch_session_source,
          o.first_touch_url
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) <= b.first_purchase_at
      ),
      opportunity_first_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          first_purchase_at,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM opportunity_first_touch_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      any_touch_buyers AS (
        SELECT contact_id, total_net_revenue_after_refunds FROM form_touch_buyers
        UNION DISTINCT
        SELECT contact_id, total_net_revenue_after_refunds FROM opportunity_touch_buyers
        UNION DISTINCT
        SELECT contact_id, total_net_revenue_after_refunds FROM contact_source_touch_buyers
        UNION DISTINCT
        SELECT contact_id, total_net_revenue_after_refunds FROM speed_to_lead_source_touch_buyers
        UNION DISTINCT
        SELECT contact_id, total_net_revenue_after_refunds FROM calendly_source_touch_buyers
        UNION DISTINCT
        SELECT contact_id, total_net_revenue_after_refunds FROM opportunity_first_touch_buyers
      ),
      expanded_touch_buyers AS (
        SELECT
          contact_id,
          ANY_VALUE(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
          ANY_VALUE(latest_prior_lead_magnet_category) AS latest_prior_lead_magnet_category,
          COUNTIF(source = 'form') > 0 AS has_form_touch,
          COUNTIF(source = 'opportunity') > 0 AS has_opportunity_touch,
          COUNTIF(source = 'contact_source') > 0 AS has_contact_source_touch,
          COUNTIF(source = 'speed_to_lead_source') > 0 AS has_speed_to_lead_source_touch,
          COUNTIF(source = 'calendly_source') > 0 AS has_calendly_source_touch,
          COUNTIF(source = 'opportunity_first_touch') > 0 AS has_opportunity_first_touch
        FROM (
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'form' AS source
          FROM form_touch_buyers
          UNION ALL
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'opportunity' AS source
          FROM opportunity_touch_buyers
          UNION ALL
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'contact_source' AS source
          FROM contact_source_touch_buyers
          UNION ALL
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'speed_to_lead_source' AS source
          FROM speed_to_lead_source_touch_buyers
          UNION ALL
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'calendly_source' AS source
          FROM calendly_source_touch_buyers
          UNION ALL
          SELECT contact_id, total_net_revenue_after_refunds, latest_prior_lead_magnet_category, 'opportunity_first_touch' AS source
          FROM opportunity_first_touch_buyers
        )
        GROUP BY contact_id
      ),
      form_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS form_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS form_touch_buyer_revenue,
          COUNT(DISTINCT IF(latest_prior_lead_magnet_category = 'sales_operating_pipeline', contact_id, NULL)) AS form_buyers_later_credited_to_sales_pipeline,
          COUNT(DISTINCT IF(latest_prior_lead_magnet_category = 'launch_event', contact_id, NULL)) AS form_buyers_later_credited_to_launch,
          COUNT(DISTINCT IF(latest_prior_lead_magnet_category = 'true_lead_magnet', contact_id, NULL)) AS form_buyers_latest_credited_to_true_magnet
        FROM form_touch_buyers
      ),
      opportunity_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS opportunity_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS opportunity_touch_buyer_revenue,
          COUNT(DISTINCT IF(latest_prior_lead_magnet_category = 'sales_operating_pipeline', contact_id, NULL)) AS opportunity_touch_buyers_later_credited_to_sales_pipeline,
          COUNT(DISTINCT IF(latest_prior_lead_magnet_category = 'launch_event', contact_id, NULL)) AS opportunity_touch_buyers_later_credited_to_launch
        FROM opportunity_touch_buyers
      ),
      contact_source_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS contact_source_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS contact_source_touch_buyer_revenue
        FROM contact_source_touch_buyers
      ),
      speed_to_lead_source_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS speed_to_lead_source_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS speed_to_lead_source_touch_buyer_revenue
        FROM speed_to_lead_source_touch_buyers
      ),
      calendly_source_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS calendly_source_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS calendly_source_touch_buyer_revenue
        FROM calendly_source_touch_buyers
      ),
      opportunity_first_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS opportunity_first_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS opportunity_first_touch_buyer_revenue
        FROM opportunity_first_touch_buyers
      ),
      any_touch_rollup AS (
        SELECT
          COUNT(DISTINCT contact_id) AS any_lead_magnet_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS any_lead_magnet_touch_buyer_revenue
        FROM any_touch_buyers
      ),
      expanded_touch_rollup AS (
        SELECT
          COUNT(*) AS expanded_touch_buyers,
          SUM(total_net_revenue_after_refunds) AS expanded_touch_buyer_revenue,
          COUNTIF(has_form_touch) AS expanded_form_touch_buyers,
          COUNTIF(has_opportunity_touch) AS expanded_opportunity_touch_buyers,
          COUNTIF(has_contact_source_touch) AS expanded_contact_source_touch_buyers,
          COUNTIF(has_speed_to_lead_source_touch) AS expanded_speed_to_lead_source_buyers,
          COUNTIF(has_calendly_source_touch) AS expanded_calendly_source_buyers,
          COUNTIF(has_opportunity_first_touch) AS expanded_opportunity_first_touch_buyers,
          COUNTIF(has_opportunity_touch AND NOT has_form_touch) AS expanded_opportunity_only_buyers,
          COUNTIF(
            (
              has_contact_source_touch
              OR has_speed_to_lead_source_touch
              OR has_calendly_source_touch
              OR has_opportunity_first_touch
            )
            AND NOT has_form_touch
            AND NOT has_opportunity_touch
          ) AS expanded_source_only_buyers,
          COUNTIF(latest_prior_lead_magnet_category = 'sales_operating_pipeline') AS expanded_buyers_later_credited_to_sales_pipeline,
          COUNTIF(latest_prior_lead_magnet_category = 'launch_event') AS expanded_buyers_later_credited_to_launch,
          COUNTIF(latest_prior_lead_magnet_category = 'true_lead_magnet') AS expanded_buyers_latest_credited_to_true_magnet
        FROM expanded_touch_buyers
      ),
      latest_touch_true AS (
        SELECT
          COUNT(*) AS latest_touch_true_buyers,
          SUM(total_net_revenue_after_refunds) AS latest_touch_true_buyer_revenue
        FROM buyer_scope AS b
        WHERE latest_prior_is_true_lead_magnet IS TRUE
      )
      SELECT
        FORMAT_TIMESTAMP('%FT%TZ', form_activity.latest_activity_refreshed_at) AS activity_refreshed_at,
        form_activity.form_events,
        form_activity.unique_form_leads,
        form_activity.distinct_form_magnets,
        FORMAT_TIMESTAMP('%FT%TZ', form_activity.first_form_event_at) AS first_form_event_at,
        FORMAT_TIMESTAMP('%FT%TZ', form_activity.latest_form_event_at) AS latest_form_event_at,
        form_activity.backfilled_form_events,
        form_touch_rollup.form_touch_buyers,
        form_touch_rollup.form_touch_buyer_revenue,
        SAFE_DIVIDE(
          form_touch_rollup.form_touch_buyers,
          NULLIF(form_activity.unique_form_leads, 0)
        ) AS form_touch_buyer_rate,
        opportunity_touch_rollup.opportunity_touch_buyers AS true_pipeline_touch_buyers,
        opportunity_touch_rollup.opportunity_touch_buyer_revenue AS true_pipeline_touch_buyer_revenue,
        opportunity_touch_rollup.opportunity_touch_buyers,
        opportunity_touch_rollup.opportunity_touch_buyer_revenue,
        contact_source_touch_rollup.contact_source_touch_buyers,
        contact_source_touch_rollup.contact_source_touch_buyer_revenue,
        speed_to_lead_source_touch_rollup.speed_to_lead_source_touch_buyers,
        speed_to_lead_source_touch_rollup.speed_to_lead_source_touch_buyer_revenue,
        calendly_source_touch_rollup.calendly_source_touch_buyers,
        calendly_source_touch_rollup.calendly_source_touch_buyer_revenue,
        opportunity_first_touch_rollup.opportunity_first_touch_buyers,
        opportunity_first_touch_rollup.opportunity_first_touch_buyer_revenue,
        any_touch_rollup.any_lead_magnet_touch_buyers,
        any_touch_rollup.any_lead_magnet_touch_buyer_revenue,
        expanded_touch_rollup.expanded_touch_buyers,
        expanded_touch_rollup.expanded_touch_buyer_revenue,
        expanded_touch_rollup.expanded_form_touch_buyers,
        expanded_touch_rollup.expanded_opportunity_touch_buyers,
        expanded_touch_rollup.expanded_contact_source_touch_buyers,
        expanded_touch_rollup.expanded_speed_to_lead_source_buyers,
        expanded_touch_rollup.expanded_calendly_source_buyers,
        expanded_touch_rollup.expanded_opportunity_first_touch_buyers,
        expanded_touch_rollup.expanded_opportunity_only_buyers,
        expanded_touch_rollup.expanded_source_only_buyers,
        latest_touch_true.latest_touch_true_buyers,
        latest_touch_true.latest_touch_true_buyer_revenue,
        form_touch_rollup.form_buyers_later_credited_to_sales_pipeline,
        form_touch_rollup.form_buyers_later_credited_to_launch,
        form_touch_rollup.form_buyers_latest_credited_to_true_magnet,
        expanded_touch_rollup.expanded_buyers_later_credited_to_sales_pipeline,
        expanded_touch_rollup.expanded_buyers_later_credited_to_launch,
        expanded_touch_rollup.expanded_buyers_latest_credited_to_true_magnet,
        opportunity_touch_rollup.opportunity_touch_buyers_later_credited_to_sales_pipeline AS true_touch_buyers_later_credited_to_sales_pipeline,
        opportunity_touch_rollup.opportunity_touch_buyers_later_credited_to_launch AS true_touch_buyers_later_credited_to_launch,
        opportunity_touch_rollup.opportunity_touch_buyers - latest_touch_true.latest_touch_true_buyers AS true_touch_buyers_hidden_by_later_touch
      FROM form_activity
      CROSS JOIN form_touch_rollup
      CROSS JOIN opportunity_touch_rollup
      CROSS JOIN contact_source_touch_rollup
      CROSS JOIN speed_to_lead_source_touch_rollup
      CROSS JOIN calendly_source_touch_rollup
      CROSS JOIN opportunity_first_touch_rollup
      CROSS JOIN any_touch_rollup
      CROSS JOIN expanded_touch_rollup
      CROSS JOIN latest_touch_true
    `,
    lead_magnet_influence_leaderboard: `
      WITH activity_rollup AS (
        SELECT
          lead_magnet_name,
          COUNT(*) AS form_events,
          COUNT(DISTINCT COALESCE(NULLIF(ghl_contact_id, ''), LOWER(contact_email))) AS unique_form_leads,
          MIN(event_ts) AS first_seen_at,
          MAX(event_ts) AS latest_seen_at
        FROM ${activityTable}
        ${activityWhere}
        GROUP BY lead_magnet_name
      ),
      buyer_magnet AS (
        SELECT DISTINCT
          a.lead_magnet_name,
          b.contact_id,
          b.first_purchase_at,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category
        FROM ${activityTable} AS a
        INNER JOIN ${buyerTable} AS b
          ON (
            a.ghl_contact_id = b.contact_id
            OR LOWER(a.contact_email) = b.email_norm
          )
        WHERE a.event_ts <= b.first_purchase_at
          ${buyerAliasAnd}
      )
      SELECT
        activity_rollup.lead_magnet_name,
        activity_rollup.form_events,
        activity_rollup.unique_form_leads,
        COUNT(DISTINCT buyer_magnet.contact_id) AS buyers_before_purchase,
        SAFE_DIVIDE(
          COUNT(DISTINCT buyer_magnet.contact_id),
          NULLIF(activity_rollup.unique_form_leads, 0)
        ) AS buyer_conversion_rate,
        SUM(buyer_magnet.total_net_revenue_after_refunds) AS nonexclusive_buyer_revenue,
        SAFE_DIVIDE(
          SUM(buyer_magnet.total_net_revenue_after_refunds),
          NULLIF(COUNT(DISTINCT buyer_magnet.contact_id), 0)
        ) AS revenue_per_buyer,
        COUNT(DISTINCT IF(buyer_magnet.latest_prior_lead_magnet_category = 'true_lead_magnet', buyer_magnet.contact_id, NULL)) AS latest_true_lead_magnet_buyers,
        COUNT(DISTINCT IF(buyer_magnet.latest_prior_lead_magnet_category = 'sales_operating_pipeline', buyer_magnet.contact_id, NULL)) AS assisted_to_sales_pipeline_buyers,
        COUNT(DISTINCT IF(buyer_magnet.latest_prior_lead_magnet_category = 'launch_event', buyer_magnet.contact_id, NULL)) AS assisted_to_launch_buyers,
        FORMAT_TIMESTAMP('%FT%TZ', activity_rollup.first_seen_at) AS first_seen_at,
        FORMAT_TIMESTAMP('%FT%TZ', activity_rollup.latest_seen_at) AS latest_seen_at
      FROM activity_rollup
      LEFT JOIN buyer_magnet
        ON buyer_magnet.lead_magnet_name = activity_rollup.lead_magnet_name
      GROUP BY
        activity_rollup.lead_magnet_name,
        activity_rollup.form_events,
        activity_rollup.unique_form_leads,
        activity_rollup.first_seen_at,
        activity_rollup.latest_seen_at
      ORDER BY buyers_before_purchase DESC, nonexclusive_buyer_revenue DESC, unique_form_leads DESC
      LIMIT 20
    `,
    lead_magnet_influence_credit_split: `
      WITH buyer_scope AS (
        SELECT *
        FROM ${buyerTable} AS b
        WHERE 1 = 1
          ${buyerAliasAnd}
      ),
      form_touch_buyers AS (
        SELECT DISTINCT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category
        FROM buyer_scope AS b
        INNER JOIN ${activityTable} AS a
          ON (
            a.ghl_contact_id = b.contact_id
            OR LOWER(a.contact_email) = b.email_norm
          )
        WHERE a.event_ts <= b.first_purchase_at
      ),
      opportunity_touch_buyers AS (
        SELECT DISTINCT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category
        FROM buyer_scope AS b
        INNER JOIN ${opportunityTable} AS d
          ON d.contact_id = b.contact_id
        WHERE d.lead_magnet_category IN ('true_lead_magnet', 'launch_event', 'waitlist')
          AND d.opportunity_created_at <= b.first_purchase_at
      ),
      contact_source_labels AS (
        SELECT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${contactTable} AS c
          ON c.contact_id = b.contact_id
        LEFT JOIN ${contactFirstTouchTable} AS ft
          ON ft.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          c.source,
          c.utm_source,
          c.utm_campaign,
          c.utm_content,
          ft.source_first,
          ft.utm_source_first,
          ft.utm_campaign_first,
          ft.utm_content_first
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) <= b.first_purchase_at
      ),
      contact_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM contact_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      speed_to_lead_source_labels AS (
        SELECT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          s.trigger_ts AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${speedToLeadTable} AS s
          ON s.ghl_contact_id = b.contact_id
        CROSS JOIN UNNEST([
          s.trigger_source_label,
          s.utm_source,
          s.utm_campaign,
          s.utm_content
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND s.trigger_type IN ('lead_magnet', 'appointment_booking')
          AND s.trigger_ts <= b.first_purchase_at
      ),
      speed_to_lead_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM speed_to_lead_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      calendly_source_labels AS (
        SELECT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${calendlyBridgeTable} AS bc
          ON bc.contact_id = b.contact_id
        LEFT JOIN ${calendlyInviteeTable} AS ci
          ON ci.invitee_id = bc.invitee_id
        CROSS JOIN UNNEST([
          bc.source_first,
          bc.utm_source_first,
          bc.utm_campaign_first,
          bc.utm_content_first,
          bc.utm_source,
          bc.utm_campaign,
          bc.utm_content,
          bc.matched_contact_source,
          ci.utm_source,
          ci.utm_campaign,
          ci.utm_content,
          ci.self_reported_source
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) <= b.first_purchase_at
      ),
      calendly_source_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM calendly_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      opportunity_first_touch_labels AS (
        SELECT
          b.contact_id,
          b.total_net_revenue_after_refunds,
          COALESCE(b.latest_prior_lead_magnet_category, 'no_latest_prior_credit') AS latest_prior_lead_magnet_category,
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${ghlOpportunityTable} AS o
          ON o.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          o.source,
          o.first_touch_session_source,
          o.first_touch_url
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) <= b.first_purchase_at
      ),
      opportunity_first_touch_buyers AS (
        SELECT DISTINCT
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category
        FROM opportunity_first_touch_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      influenced_buyers AS (
        SELECT
          contact_id,
          ANY_VALUE(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
          ANY_VALUE(latest_prior_lead_magnet_category) AS latest_prior_lead_magnet_category
        FROM (
          SELECT * FROM form_touch_buyers
          UNION ALL
          SELECT * FROM opportunity_touch_buyers
          UNION ALL
          SELECT * FROM contact_source_touch_buyers
          UNION ALL
          SELECT * FROM speed_to_lead_source_touch_buyers
          UNION ALL
          SELECT * FROM calendly_source_touch_buyers
          UNION ALL
          SELECT * FROM opportunity_first_touch_buyers
        )
        GROUP BY contact_id
      )
      SELECT
        latest_prior_lead_magnet_category AS latest_credit_category,
        CASE latest_prior_lead_magnet_category
          WHEN 'sales_operating_pipeline' THEN 'Later credited to sales pipeline'
          WHEN 'launch_event' THEN 'Later credited to launch / event'
          WHEN 'true_lead_magnet' THEN 'Still credited to true lead magnet'
          WHEN 'waitlist' THEN 'Later credited to waitlist'
          WHEN 'no_latest_prior_credit' THEN 'No latest-prior credit'
          ELSE INITCAP(REPLACE(latest_prior_lead_magnet_category, '_', ' '))
        END AS latest_credit_label,
        COUNT(*) AS buyers,
        SAFE_DIVIDE(COUNT(*), NULLIF(SUM(COUNT(*)) OVER (), 0)) AS buyer_share,
        SUM(total_net_revenue_after_refunds) AS buyer_net_revenue,
        SAFE_DIVIDE(
          SUM(total_net_revenue_after_refunds),
          NULLIF(SUM(SUM(total_net_revenue_after_refunds)) OVER (), 0)
        ) AS revenue_share
      FROM influenced_buyers
      GROUP BY latest_credit_category, latest_credit_label
      ORDER BY buyers DESC
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
    lead_magnet_queue_distribution: `
      SELECT
        queue_status,
        COUNT(*) AS magnet_count,
        ${queueStatusOrderSql} AS queue_status_order
      FROM ${performanceTable}
      GROUP BY queue_status
      ORDER BY queue_status_order
    `,
    lead_magnet_review_queue: `
      SELECT
        lead_magnet_id,
        lead_magnet_reporting_name,
        lead_magnet_category,
        lead_magnet_offer_type,
        ${offerTypeLabelSql("lead_magnet_offer_type")} AS offer_type_label,
        is_true_lead_magnet,
        is_launch,
        is_waitlist,
        is_sales_pipeline,
        queue_status,
        kill_candidate,
        repair_candidate,
        retire_recommended_pending_override,
        leads_in_90d,
        revenue_per_lead,
        purchase_rate,
        engagement_rate_week_4,
        engagement_decay_rate_week_0_to_4,
        repeat_buyer_rate,
        intermediate_activation_milestone,
        intermediate_activation_rate,
        booking_rate,
        opt_in_lead_count,
        buyer_count,
        net_revenue_after_refunds,
        ${queueStatusOrderSql} AS queue_status_order
      FROM ${performanceTable}
      WHERE queue_status IN (
        'retire_recommended_pending_override',
        'kill_candidate',
        'repair_candidate'
      )
      ORDER BY queue_status_order, revenue_per_lead ASC NULLS LAST, opt_in_lead_count DESC NULLS LAST
    `,
    lead_magnet_performance_rows: `
      WITH activity_mapped AS (
        SELECT
          ${activityNameToReportingNameSql("lead_magnet_name")} AS mapped_lead_magnet_reporting_name,
          *
        FROM ${activityTable}
      ),
      activity_rollup AS (
        SELECT
          mapped_lead_magnet_reporting_name AS lead_magnet_reporting_name,
          COUNT(*) AS influence_form_events,
          COUNT(DISTINCT COALESCE(NULLIF(ghl_contact_id, ''), LOWER(contact_email))) AS influence_form_leads
        FROM activity_mapped
        ${activityMappedWhere}
        GROUP BY mapped_lead_magnet_reporting_name
      ),
      booked_call_rollup AS (
        SELECT
          d.lead_magnet_reporting_name,
          COUNT(DISTINCT d.contact_id) AS range_booked_call_contacts,
          SUM(GREATEST(COALESCE(d.direct_bookings_count, 0), COALESCE(d.window_bookings_count, 0))) AS range_booked_calls
        FROM ${opportunityTable} AS d
        WHERE d.lead_magnet_category IN ('true_lead_magnet', 'launch_event', 'waitlist')
          AND COALESCE(d.first_booking_at, d.latest_booking_at) IS NOT NULL
          ${bookingAliasAnd}
        GROUP BY d.lead_magnet_reporting_name
      ),
      buyer_scope AS (
        SELECT *
        FROM ${buyerTable} AS b
        WHERE 1 = 1
          ${buyerAliasAnd}
      ),
      form_buyer_magnet AS (
        SELECT DISTINCT
          activity_mapped.mapped_lead_magnet_reporting_name AS lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          'form' AS influence_source
        FROM activity_mapped
        INNER JOIN buyer_scope AS b
          ON (
            activity_mapped.ghl_contact_id = b.contact_id
            OR LOWER(activity_mapped.contact_email) = b.email_norm
          )
        WHERE activity_mapped.event_ts <= b.first_purchase_at
      ),
      opportunity_buyer_magnet AS (
        SELECT DISTINCT
          d.lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          'opportunity' AS influence_source
        FROM ${opportunityTable} AS d
        INNER JOIN buyer_scope AS b
          ON d.contact_id = b.contact_id
        WHERE d.opportunity_created_at <= b.first_purchase_at
          AND d.lead_magnet_category IN ('true_lead_magnet', 'launch_event', 'waitlist')
      ),
      contact_source_labels AS (
        SELECT
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${contactTable} AS c
          ON c.contact_id = b.contact_id
        LEFT JOIN ${contactFirstTouchTable} AS ft
          ON ft.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          c.source,
          c.utm_source,
          c.utm_campaign,
          c.utm_content,
          ft.source_first,
          ft.utm_source_first,
          ft.utm_campaign_first,
          ft.utm_content_first
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ft.first_contact_ts, c.date_added, c.first_seen_ts) <= b.first_purchase_at
      ),
      contact_source_buyer_magnet AS (
        SELECT DISTINCT
          lead_magnet_reporting_name,
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category,
          latest_prior_lead_magnet_reporting_name,
          'contact_source' AS influence_source
        FROM contact_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      speed_to_lead_source_labels AS (
        SELECT
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          s.trigger_ts AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${speedToLeadTable} AS s
          ON s.ghl_contact_id = b.contact_id
        CROSS JOIN UNNEST([
          s.trigger_source_label,
          s.utm_source,
          s.utm_campaign,
          s.utm_content
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND s.trigger_type IN ('lead_magnet', 'appointment_booking')
          AND s.trigger_ts <= b.first_purchase_at
      ),
      speed_to_lead_source_buyer_magnet AS (
        SELECT DISTINCT
          lead_magnet_reporting_name,
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category,
          latest_prior_lead_magnet_reporting_name,
          'speed_to_lead_source' AS influence_source
        FROM speed_to_lead_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      calendly_source_labels AS (
        SELECT
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${calendlyBridgeTable} AS bc
          ON bc.contact_id = b.contact_id
        LEFT JOIN ${calendlyInviteeTable} AS ci
          ON ci.invitee_id = bc.invitee_id
        CROSS JOIN UNNEST([
          bc.source_first,
          bc.utm_source_first,
          bc.utm_campaign_first,
          bc.utm_content_first,
          bc.utm_source,
          bc.utm_campaign,
          bc.utm_content,
          bc.matched_contact_source,
          ci.utm_source,
          ci.utm_campaign,
          ci.utm_content,
          ci.self_reported_source
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(ci.invitee_created_at, ci.event_ts, bc.first_contact_ts, bc.scheduled_start_time) <= b.first_purchase_at
      ),
      calendly_source_buyer_magnet AS (
        SELECT DISTINCT
          lead_magnet_reporting_name,
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category,
          latest_prior_lead_magnet_reporting_name,
          'calendly_source' AS influence_source
        FROM calendly_source_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      opportunity_first_touch_labels AS (
        SELECT
          ${contactSourceToReportingNameSql("LOWER(TRIM(label))")} AS lead_magnet_reporting_name,
          b.contact_id,
          b.total_net_revenue_after_refunds,
          b.latest_prior_lead_magnet_category,
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS latest_prior_lead_magnet_reporting_name,
          COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) AS source_touch_ts
        FROM buyer_scope AS b
        INNER JOIN ${ghlOpportunityTable} AS o
          ON o.contact_id = b.contact_id
        CROSS JOIN UNNEST([
          o.source,
          o.first_touch_session_source,
          o.first_touch_url
        ]) AS label
        WHERE label IS NOT NULL
          AND TRIM(label) != ''
          AND COALESCE(o.event_ts, o.updated_at_ts, o.ingested_at) <= b.first_purchase_at
      ),
      opportunity_first_touch_buyer_magnet AS (
        SELECT DISTINCT
          lead_magnet_reporting_name,
          contact_id,
          total_net_revenue_after_refunds,
          latest_prior_lead_magnet_category,
          latest_prior_lead_magnet_reporting_name,
          'opportunity_first_touch' AS influence_source
        FROM opportunity_first_touch_labels
        WHERE lead_magnet_reporting_name IS NOT NULL
      ),
      buyer_magnet_contact AS (
        SELECT
          lead_magnet_reporting_name,
          contact_id,
          ANY_VALUE(total_net_revenue_after_refunds) AS total_net_revenue_after_refunds,
          ANY_VALUE(latest_prior_lead_magnet_category) AS latest_prior_lead_magnet_category,
          ANY_VALUE(latest_prior_lead_magnet_reporting_name) AS latest_prior_lead_magnet_reporting_name,
          COUNTIF(influence_source = 'form') > 0 AS has_form_influence,
          COUNTIF(influence_source = 'opportunity') > 0 AS has_opportunity_influence,
          COUNTIF(influence_source = 'contact_source') > 0 AS has_contact_source_influence,
          COUNTIF(influence_source = 'speed_to_lead_source') > 0 AS has_speed_to_lead_source_influence,
          COUNTIF(influence_source = 'calendly_source') > 0 AS has_calendly_source_influence,
          COUNTIF(influence_source = 'opportunity_first_touch') > 0 AS has_opportunity_first_touch_influence
        FROM (
          SELECT * FROM form_buyer_magnet
          UNION ALL
          SELECT * FROM opportunity_buyer_magnet
          UNION ALL
          SELECT * FROM contact_source_buyer_magnet
          UNION ALL
          SELECT * FROM speed_to_lead_source_buyer_magnet
          UNION ALL
          SELECT * FROM calendly_source_buyer_magnet
          UNION ALL
          SELECT * FROM opportunity_first_touch_buyer_magnet
        )
        GROUP BY lead_magnet_reporting_name, contact_id
      ),
      influence_by_magnet AS (
        SELECT
          magnet_index.lead_magnet_reporting_name,
          activity_rollup.influence_form_events,
          activity_rollup.influence_form_leads,
          COUNTIF(buyer_magnet_contact.has_form_influence) AS influence_form_touch_buyers,
          COUNTIF(buyer_magnet_contact.has_opportunity_influence) AS influence_opportunity_touch_buyers,
          COUNTIF(buyer_magnet_contact.has_contact_source_influence) AS influence_contact_source_buyers,
          COUNTIF(buyer_magnet_contact.has_speed_to_lead_source_influence) AS influence_speed_to_lead_source_buyers,
          COUNTIF(buyer_magnet_contact.has_calendly_source_influence) AS influence_calendly_source_buyers,
          COUNTIF(buyer_magnet_contact.has_opportunity_first_touch_influence) AS influence_opportunity_first_touch_buyers,
          COUNTIF(
            (
              buyer_magnet_contact.has_contact_source_influence
              OR buyer_magnet_contact.has_speed_to_lead_source_influence
              OR buyer_magnet_contact.has_calendly_source_influence
              OR buyer_magnet_contact.has_opportunity_first_touch_influence
            )
            AND NOT buyer_magnet_contact.has_form_influence
            AND NOT buyer_magnet_contact.has_opportunity_influence
          ) AS influence_source_only_buyers,
          COUNT(buyer_magnet_contact.contact_id) AS influence_buyers_before_purchase,
          SAFE_DIVIDE(
            COUNTIF(buyer_magnet_contact.has_form_influence),
            NULLIF(activity_rollup.influence_form_leads, 0)
          ) AS influence_buyer_conversion_rate,
          SUM(buyer_magnet_contact.total_net_revenue_after_refunds) AS influence_nonexclusive_buyer_revenue,
          COUNTIF(
            buyer_magnet_contact.latest_prior_lead_magnet_reporting_name = magnet_index.lead_magnet_reporting_name
          ) AS influence_direct_credit_buyers,
          COUNTIF(buyer_magnet_contact.latest_prior_lead_magnet_category = 'true_lead_magnet') AS influence_latest_true_lead_magnet_buyers,
          COUNTIF(buyer_magnet_contact.latest_prior_lead_magnet_category = 'sales_operating_pipeline') AS influence_assisted_to_sales_pipeline_buyers,
          COUNTIF(buyer_magnet_contact.latest_prior_lead_magnet_category = 'launch_event') AS influence_assisted_to_launch_buyers
        FROM (
          SELECT lead_magnet_reporting_name FROM activity_rollup
          UNION DISTINCT
          SELECT lead_magnet_reporting_name FROM buyer_magnet_contact
        ) AS magnet_index
        LEFT JOIN activity_rollup
          ON activity_rollup.lead_magnet_reporting_name = magnet_index.lead_magnet_reporting_name
        LEFT JOIN buyer_magnet_contact
          ON buyer_magnet_contact.lead_magnet_reporting_name = magnet_index.lead_magnet_reporting_name
        GROUP BY
          magnet_index.lead_magnet_reporting_name,
          activity_rollup.influence_form_events,
          activity_rollup.influence_form_leads
      ),
      direct_credit_by_magnet AS (
        SELECT
          ${activityNameToReportingNameSql("b.latest_prior_lead_magnet_name")} AS lead_magnet_reporting_name,
          COUNT(DISTINCT b.contact_id) AS direct_credit_buyers,
          SUM(b.total_net_revenue_after_refunds) AS direct_credit_net_revenue
        FROM buyer_scope AS b
        WHERE b.latest_prior_lead_magnet_name IS NOT NULL
        GROUP BY lead_magnet_reporting_name
      )
      SELECT
        performance.lead_magnet_id,
        performance.lead_magnet_reporting_name,
        performance.lead_magnet_category,
        performance.lead_magnet_offer_type,
        ${offerTypeLabelSql("performance.lead_magnet_offer_type")} AS offer_type_label,
        performance.is_true_lead_magnet,
        performance.is_launch,
        performance.is_waitlist,
        performance.is_sales_pipeline,
        performance.queue_status,
        performance.queue_exclusion_reason,
        performance.kill_candidate,
        performance.repair_candidate,
        performance.retire_recommended_pending_override,
        performance.opt_in_lead_count,
        performance.leads_in_90d,
        performance.revenue_per_lead,
        performance.revenue_per_lead_denominator_window_days,
        performance.time_to_buyer_days,
        performance.avg_time_to_buyer_days,
        performance.repeat_buyer_rate,
        performance.repeat_buyer_count,
        performance.buyer_count,
        performance.net_revenue_after_refunds,
        performance.intermediate_activation_milestone,
        performance.intermediate_activation_lead_count,
        performance.intermediate_activation_rate,
        performance.booking_lead_count,
        performance.booking_rate,
        performance.purchase_lead_count,
        performance.purchase_rate,
        performance.engagement_rate_week_0,
        performance.engagement_rate_week_1,
        performance.engagement_rate_week_2,
        performance.engagement_rate_week_4,
        performance.engagement_rate_week_8,
        performance.engagement_decay_rate_week_0_to_4,
        performance.meaningful_engagement_rate,
        FORMAT_TIMESTAMP('%FT%TZ', performance.mart_refreshed_at) AS performance_mart_refreshed_at,
        ${queueStatusOrderSql} AS queue_status_order,
        COALESCE(influence_by_magnet.influence_form_events, 0) AS influence_form_events,
        COALESCE(influence_by_magnet.influence_form_leads, 0) AS influence_form_leads,
        COALESCE(influence_by_magnet.influence_form_touch_buyers, 0) AS influence_form_touch_buyers,
        COALESCE(influence_by_magnet.influence_opportunity_touch_buyers, 0) AS influence_opportunity_touch_buyers,
        COALESCE(influence_by_magnet.influence_contact_source_buyers, 0) AS influence_contact_source_buyers,
        COALESCE(influence_by_magnet.influence_speed_to_lead_source_buyers, 0) AS influence_speed_to_lead_source_buyers,
        COALESCE(influence_by_magnet.influence_calendly_source_buyers, 0) AS influence_calendly_source_buyers,
        COALESCE(influence_by_magnet.influence_opportunity_first_touch_buyers, 0) AS influence_opportunity_first_touch_buyers,
        COALESCE(influence_by_magnet.influence_source_only_buyers, 0) AS influence_source_only_buyers,
        COALESCE(influence_by_magnet.influence_buyers_before_purchase, 0) AS influence_buyers_before_purchase,
        influence_by_magnet.influence_buyer_conversion_rate,
        COALESCE(influence_by_magnet.influence_nonexclusive_buyer_revenue, 0) AS influence_nonexclusive_buyer_revenue,
        COALESCE(influence_by_magnet.influence_direct_credit_buyers, 0) AS influence_direct_credit_buyers,
        COALESCE(influence_by_magnet.influence_latest_true_lead_magnet_buyers, 0) AS influence_latest_true_lead_magnet_buyers,
        COALESCE(influence_by_magnet.influence_assisted_to_sales_pipeline_buyers, 0) AS influence_assisted_to_sales_pipeline_buyers,
        COALESCE(influence_by_magnet.influence_assisted_to_launch_buyers, 0) AS influence_assisted_to_launch_buyers,
        COALESCE(booked_call_rollup.range_booked_call_contacts, 0) AS range_booked_call_contacts,
        COALESCE(booked_call_rollup.range_booked_calls, 0) AS range_booked_calls,
        SAFE_DIVIDE(
          COALESCE(booked_call_rollup.range_booked_call_contacts, 0),
          NULLIF(COALESCE(influence_by_magnet.influence_form_leads, performance.opt_in_lead_count), 0)
        ) AS range_booked_call_rate,
        COALESCE(direct_credit_by_magnet.direct_credit_buyers, 0) AS direct_credit_buyers,
        COALESCE(direct_credit_by_magnet.direct_credit_net_revenue, 0) AS direct_credit_net_revenue
      FROM ${performanceTable} AS performance
      LEFT JOIN influence_by_magnet
        ON influence_by_magnet.lead_magnet_reporting_name = performance.lead_magnet_reporting_name
      LEFT JOIN booked_call_rollup
        ON booked_call_rollup.lead_magnet_reporting_name = performance.lead_magnet_reporting_name
      LEFT JOIN direct_credit_by_magnet
        ON direct_credit_by_magnet.lead_magnet_reporting_name = performance.lead_magnet_reporting_name
      ORDER BY revenue_per_lead DESC NULLS LAST, opt_in_lead_count DESC NULLS LAST
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
      queueDistribution,
      reviewQueue,
      performanceRows,
      influenceSummary,
      influenceLeaderboard,
      influenceCreditSplit,
    ] = await Promise.all([
      runBigQuery(leadMagnetQueries.lead_magnet_summary),
      runBigQuery(leadMagnetQueries.lead_magnet_attribution_flags),
      runBigQuery(leadMagnetQueries.lead_magnet_offer_types),
      runBigQuery(leadMagnetQueries.lead_magnet_top_magnets),
      runBigQuery(leadMagnetQueries.lead_magnet_opportunity_offer_types),
      runBigQuery(leadMagnetQueries.lead_magnet_pipeline_activity),
      runBigQuery(leadMagnetQueries.lead_magnet_queue_distribution),
      runBigQuery(leadMagnetQueries.lead_magnet_review_queue),
      runBigQuery(leadMagnetQueries.lead_magnet_performance_rows),
      runBigQuery(leadMagnetQueries.lead_magnet_influence_summary),
      runBigQuery(leadMagnetQueries.lead_magnet_influence_leaderboard),
      runBigQuery(leadMagnetQueries.lead_magnet_influence_credit_split),
    ]);

    return {
      rows: {
        lead_magnet_summary: summary,
        lead_magnet_attribution_flags: attributionFlags,
        lead_magnet_offer_types: offerTypes,
        lead_magnet_top_magnets: topMagnets,
        lead_magnet_opportunity_offer_types: opportunityOfferTypes,
        lead_magnet_pipeline_activity: pipelineActivity,
        lead_magnet_queue_distribution: queueDistribution,
        lead_magnet_review_queue: reviewQueue,
        lead_magnet_performance_rows: performanceRows,
        lead_magnet_influence_summary: influenceSummary,
        lead_magnet_influence_leaderboard: influenceLeaderboard,
        lead_magnet_influence_credit_split: influenceCreditSplit,
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
          "fct_lead_magnet_activity",
          "fct_speed_to_lead",
          "Core.dim_ghl_contacts",
          "Core.dim_ghl_contacts_first_touch",
          "Core.bridge_calendly_invitee_contacts",
          "Core.fct_calendly_event_invitees",
          "Core.fct_ghl_opportunities",
        ],
        note:
          "Lead magnet influence is source-enriched: GHL form submissions, pre-purchase lead-magnet opportunities, speed-to-lead trigger labels, Calendly source labels, GHL opportunity first-touch fields, and high-confidence GHL first-source/UTM labels are joined to buyer truth. Latest-touch buyer credit remains exclusive and range-scoped from lead_magnet_buyer_detail; the dashboard separates nonexclusive influenced revenue from final credit.",
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
          "fct_lead_magnet_activity",
          "fct_speed_to_lead",
          "Core.dim_ghl_contacts",
          "Core.dim_ghl_contacts_first_touch",
          "Core.bridge_calendly_invitee_contacts",
          "Core.fct_calendly_event_invitees",
          "Core.fct_ghl_opportunities",
        ],
        note:
          "Lead magnet influence is source-enriched: GHL form submissions, pre-purchase lead-magnet opportunities, speed-to-lead trigger labels, Calendly source labels, GHL opportunity first-touch fields, and high-confidence GHL first-source/UTM labels are joined to buyer truth. Latest-touch buyer credit remains exclusive and range-scoped from lead_magnet_buyer_detail; the dashboard separates nonexclusive influenced revenue from final credit.",
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
