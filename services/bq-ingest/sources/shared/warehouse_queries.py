"""Named analytical query catalog for the GTM Lead Warehouse.

Each query runs entirely in BigQuery and returns a compact summary dict.
Claude (or any caller) fires one HTTP call, gets back a small JSON payload.
No raw data ever loads into the caller's context.

Usage:
    from warehouse_queries import run_named_query
    result = run_named_query("closer_summary")
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from google.cloud import bigquery

PROJECT = os.getenv("GCP_PROJECT_ID", "project-41542e21-470f-4589-96d")

# ---------------------------------------------------------------------------
# Query registry: name → SQL that returns a compact, already-aggregated result.
# Every query must return ≤ 50 rows and use only summary-level columns.
# ---------------------------------------------------------------------------

_QUERIES: Dict[str, str] = {

    # Rep leaderboard — revenue, calls, contacts this month and all-time
    "closer_summary": f"""
        WITH monthly AS (
          SELECT
            cl.closer_name,
            cl.closer_email,
            COUNT(DISTINCT lw.golden_contact_key) AS contacts_this_month,
            COUNT(DISTINCT CASE WHEN lw.has_any_payment THEN lw.golden_contact_key END) AS paying_contacts_this_month,
            SUM(lw.total_net_revenue) AS revenue_this_month
          FROM `{PROJECT}.Marts.mart_master_lead_wide` lw
          JOIN `{PROJECT}.Marts.bridge_contact_closer` cl
            ON cl.contact_id = lw.ghl_contact_id
           AND cl.location_id = lw.location_id
          WHERE lw.closer_call_ts >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
          GROUP BY 1, 2
        )
        SELECT
          d.closer_name,
          d.closer_email,
          d.revenue_relevant_calls AS total_revenue_calls_alltime,
          d.distinct_contacts_reached AS total_contacts_reached_alltime,
          COALESCE(m.contacts_this_month, 0) AS contacts_this_month,
          COALESCE(m.paying_contacts_this_month, 0) AS paying_contacts_this_month,
          ROUND(COALESCE(m.revenue_this_month, 0), 2) AS revenue_this_month,
          ROUND(SAFE_DIVIDE(
            COALESCE(m.paying_contacts_this_month, 0),
            NULLIF(COALESCE(m.contacts_this_month, 0), 0)
          ) * 100, 1) AS close_rate_pct_this_month
        FROM `{PROJECT}.Marts.dim_closers` d
        LEFT JOIN monthly m USING (closer_email)
        WHERE d.revenue_relevant_calls > 0
        ORDER BY revenue_this_month DESC
    """,

    # Payment match rate and revenue attribution health
    "payment_match_rate": f"""
        SELECT
          match_status,
          source_system,
          COUNT(*) AS payment_count,
          ROUND(SUM(SAFE_CAST(net_amount AS NUMERIC)), 2) AS total_net_revenue,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_payments
        FROM `{PROJECT}.Marts.fct_payment_line_unified`
        GROUP BY 1, 2
        ORDER BY total_net_revenue DESC
    """,

    # Campaign funnel — current month vs last month
    "funnel_summary": f"""
        SELECT
          FORMAT_DATE('%Y-%m', report_month) AS month,
          campaign_reporting,
          new_leads_distinct,
          leads_with_booking,
          sum_meeting_slots_showed,
          ROUND(slot_level_show_rate * 100, 1) AS show_rate_pct,
          ROUND(revenue_net_sum, 2) AS revenue_net,
          paying_distinct_golden_contacts
        FROM `{PROJECT}.Marts.rpt_campaign_funnel_month`
        WHERE report_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        ORDER BY report_month DESC, revenue_net DESC
        LIMIT 40
    """,

    # Latest DQ run results — status of each test
    "dq_status": f"""
        WITH latest_run AS (
          SELECT MAX(run_id) AS run_id
          FROM `{PROJECT}.Raw.dq_test_results`
        )
        SELECT
          r.test_name,
          r.status,
          r.failing_rows,
          r.message,
          r.checked_at
        FROM `{PROJECT}.Raw.dq_test_results` r
        JOIN latest_run lr USING (run_id)
        ORDER BY
          CASE r.status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
          r.test_name
    """,

    # Pipeline health — opportunities by stage and status
    "pipeline_health": f"""
        SELECT
          COALESCE(NULLIF(TRIM(JSON_VALUE(payload_json, '$.pipelineName')), ''), 'Unknown') AS pipeline_name,
          COALESCE(NULLIF(TRIM(JSON_VALUE(payload_json, '$.stageName')),    ''), 'Unknown') AS stage_name,
          COALESCE(NULLIF(TRIM(JSON_VALUE(payload_json, '$.status')),       ''), 'unknown') AS status,
          COUNT(*) AS opportunity_count,
          ROUND(SUM(SAFE_CAST(JSON_VALUE(payload_json, '$.monetaryValue') AS NUMERIC)), 2) AS pipeline_value
        FROM `{PROJECT}.Raw.ghl_objects_raw`
        WHERE entity_type = 'opportunities'
        GROUP BY 1, 2, 3
        ORDER BY pipeline_value DESC
        LIMIT 30
    """,

    # New lead velocity — leads per week for last 8 weeks
    "lead_velocity": f"""
        SELECT
          DATE_TRUNC(DATE(ghl_date_added_ts), WEEK(MONDAY)) AS week_start,
          COUNT(*) AS new_leads,
          COUNTIF(has_calendly_booking) AS booked,
          COUNTIF(has_any_payment) AS converted,
          ROUND(SUM(total_net_revenue), 2) AS revenue
        FROM `{PROJECT}.Marts.mart_master_lead_wide`
        WHERE ghl_date_added_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 56 DAY)
        GROUP BY 1
        ORDER BY 1 DESC
    """,

    # Closer attribution coverage — how many contacts/payments have a closer assigned
    "closer_coverage": f"""
        SELECT
          COUNTIF(closer_name IS NOT NULL) AS contacts_with_closer,
          COUNTIF(closer_name IS NULL) AS contacts_without_closer,
          COUNT(*) AS total_contacts,
          ROUND(COUNTIF(closer_name IS NOT NULL) * 100.0 / COUNT(*), 1) AS pct_attributed,
          ROUND(SUM(CASE WHEN closer_name IS NOT NULL THEN total_net_revenue ELSE 0 END), 2) AS revenue_attributed,
          ROUND(SUM(CASE WHEN closer_name IS NULL     THEN total_net_revenue ELSE 0 END), 2) AS revenue_unattributed,
          COUNTIF(multi_rep_contact) AS multi_rep_contacts
        FROM `{PROJECT}.Marts.mart_master_lead_wide`
    """,

    # Revenue by stage and pipeline this month
    "revenue_by_stage": f"""
        SELECT
          FORMAT_DATE('%Y-%m', report_month) AS month,
          pipeline_name,
          stage_name,
          opportunity_status,
          total_transaction_count,
          paying_distinct_golden_contacts,
          ROUND(revenue_net_sum, 2) AS revenue_net
        FROM `{PROJECT}.Marts.rpt_revenue_by_stage_month`
        WHERE report_month >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        ORDER BY report_month DESC, revenue_net DESC
        LIMIT 30
    """,

    # ── New GTM marts ────────────────────────────────────────────────────────

    # KPI summary — 5 headline numbers for the current week
    "kpi_summary": f"""
        SELECT
          SUM(bookings)                                                   AS total_bookings,
          SUM(shows)                                                      AS total_shows,
          SUM(no_shows)                                                   AS total_no_shows,
          ROUND(SAFE_DIVIDE(SUM(shows), NULLIF(SUM(shows)+SUM(no_shows),0)) * 100, 1) AS show_rate_pct,
          SUM(deals_closed)                                               AS total_deals_closed,
          ROUND(SAFE_DIVIDE(SUM(deals_closed), NULLIF(SUM(sales_calls_taken),0)) * 100, 1) AS close_rate_pct
        FROM `{PROJECT}.Marts.rpt_rep_scorecard_week`
        WHERE report_week = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
    """,

    # Rep scorecard — current week, all reps
    "rep_scorecard_week": f"""
        SELECT
          rep_name,
          rep_role,
          COALESCE(bookings, 0)                   AS bookings,
          COALESCE(shows, 0)                       AS shows,
          COALESCE(no_shows, 0)                    AS no_shows,
          ROUND(COALESCE(show_rate, 0) * 100, 1)  AS show_rate_pct,
          COALESCE(total_dials, 0)                 AS total_dials,
          ROUND(COALESCE(call_to_booking_rate_14d, 0) * 100, 1) AS booking_rate_14d_pct,
          ROUND(COALESCE(avg_speed_to_lead_minutes, 0), 1)      AS avg_speed_to_lead_minutes,
          COALESCE(deals_closed, 0)                AS deals_closed,
          ROUND(COALESCE(close_rate, 0) * 100, 1) AS close_rate_pct,
          COALESCE(unbooked_leads_worked, 0)       AS unbooked_leads_worked,
          ROUND(COALESCE(unbooked_conversion_rate_14d, 0) * 100, 1) AS unbooked_conversion_14d_pct
        FROM `{PROJECT}.Marts.rpt_rep_scorecard_week`
        WHERE report_week = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
        ORDER BY bookings DESC
    """,

    # Appointment funnel — current week totals across all setters
    "appt_funnel_week": f"""
        SELECT
          SUM(total_bookings)   AS total_bookings,
          SUM(host_canceled)    AS host_canceled,
          SUM(invitee_canceled) AS invitee_canceled,
          SUM(total_canceled)   AS total_canceled,
          SUM(net_appointments) AS net_appointments,
          SUM(shows)            AS shows,
          SUM(no_shows)         AS no_shows,
          ROUND(SAFE_DIVIDE(SUM(shows), NULLIF(SUM(shows)+SUM(no_shows),0)) * 100, 1) AS show_rate_pct,
          ROUND(SAFE_DIVIDE(SUM(total_canceled), NULLIF(SUM(total_bookings),0)) * 100, 1) AS cancel_rate_pct,
          ROUND(SAFE_DIVIDE(SUM(host_canceled), NULLIF(SUM(total_bookings),0)) * 100, 1) AS host_cancel_rate_pct
        FROM `{PROJECT}.Marts.rpt_appt_funnel_week`
        WHERE report_week = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
    """,

    # Appointment funnel by setter — current week
    "appt_funnel_by_setter_week": f"""
        SELECT
          setter_name,
          SUM(total_bookings)   AS bookings,
          SUM(host_canceled)    AS host_canceled,
          SUM(invitee_canceled) AS invitee_canceled,
          SUM(shows)            AS shows,
          SUM(no_shows)         AS no_shows,
          ROUND(SAFE_DIVIDE(SUM(shows), NULLIF(SUM(shows)+SUM(no_shows),0)) * 100, 1) AS show_rate_pct
        FROM `{PROJECT}.Marts.rpt_appt_funnel_week`
        WHERE report_week = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
          AND setter_name != 'unknown'
        GROUP BY 1
        ORDER BY bookings DESC
    """,

    # Speed to lead — current week by setter (lead_magnet trigger)
    "speed_to_lead_week": f"""
        SELECT
          setter_name,
          total_triggers,
          touched,
          not_yet_touched,
          ROUND(touch_rate * 100, 1)       AS touch_rate_pct,
          ROUND(avg_speed_minutes, 1)       AS avg_speed_minutes,
          median_speed_minutes,
          ROUND(pct_within_5m * 100, 1)    AS pct_within_5m,
          ROUND(pct_within_15m * 100, 1)   AS pct_within_15m,
          ROUND(sla_breach_rate * 100, 1)  AS sla_breach_rate_pct
        FROM `{PROJECT}.Marts.rpt_speed_to_lead_week`
        WHERE report_week = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
          AND trigger_type = 'lead_magnet'
          AND setter_name != 'unattributed'
        ORDER BY avg_speed_minutes ASC
    """,

    # Campaign quality — last 4 weeks
    "campaign_quality": f"""
        SELECT
          campaign_reporting,
          SUM(form_fills)                                                        AS form_fills,
          SUM(eventually_booked)                                                 AS eventually_booked,
          ROUND(SAFE_DIVIDE(SUM(eventually_booked), NULLIF(SUM(form_fills),0)) * 100, 1) AS booking_rate_ever_pct,
          ROUND(SAFE_DIVIDE(SUM(booked_within_14d), NULLIF(SUM(form_fills),0)) * 100, 1) AS booking_rate_14d_pct,
          SUM(never_touched)                                                     AS never_touched,
          ROUND(SAFE_DIVIDE(SUM(never_touched), NULLIF(SUM(form_fills),0)) * 100, 1)     AS never_touched_pct
        FROM `{PROJECT}.Marts.rpt_unbooked_lead_quality_by_campaign`
        WHERE report_week >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 28 DAY)
        GROUP BY 1
        ORDER BY form_fills DESC
        LIMIT 20
    """,

    # Funnel conversion trend — last 8 weeks by campaign
    "funnel_trend_week": f"""
        SELECT
          report_week,
          campaign_reporting,
          new_leads,
          bookings_made,
          shows_count,
          deals_closed,
          ROUND(COALESCE(show_rate, 0) * 100, 1)          AS show_rate_pct,
          ROUND(COALESCE(lead_to_close_rate, 0) * 100, 1) AS lead_to_close_rate_pct
        FROM `{PROJECT}.Marts.rpt_funnel_conversion_week`
        WHERE report_week >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 56 DAY)
        ORDER BY report_week DESC, new_leads DESC
        LIMIT 50
    """,

    # Raw source freshness — when did each pipeline last ingest?
    "source_freshness": f"""
        SELECT
          entity_type AS source,
          COUNT(*) AS total_records,
          MAX(ingested_at) AS last_ingested_at,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR) AS hours_since_ingest
        FROM `{PROJECT}.Raw.ghl_objects_raw`
        GROUP BY 1
        UNION ALL
        SELECT 'calendly' AS source,
          COUNT(*), MAX(ingested_at),
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR)
        FROM `{PROJECT}.Raw.calendly_objects_raw`
        UNION ALL
        SELECT 'fanbasis_transactions' AS source,
          COUNT(*), MAX(ingested_at),
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR)
        FROM `{PROJECT}.Raw.fanbasis_transactions_txn_raw`
        UNION ALL
        SELECT 'stripe' AS source,
          COUNT(*), MAX(ingested_at),
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR)
        FROM `{PROJECT}.Raw.stripe_objects_raw`
        UNION ALL
        SELECT 'fathom' AS source,
          COUNT(*), MAX(ingested_at),
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR)
        FROM `{PROJECT}.Raw.fathom_calls_raw`
        ORDER BY hours_since_ingest DESC
    """,
}

AVAILABLE_QUERIES = sorted(_QUERIES.keys())


def run_named_query(name: str) -> Dict[str, Any]:
    """Run a named query in BigQuery and return a compact summary dict.

    Never returns raw row data — every query is pre-aggregated SQL.
    Raises KeyError for unknown query names.
    """
    if name not in _QUERIES:
        raise KeyError(f"Unknown query '{name}'. Available: {AVAILABLE_QUERIES}")

    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(_QUERIES[name]).result())

    return {
        "query": name,
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(rows),
        "results": [dict(r.items()) for r in rows],
    }
