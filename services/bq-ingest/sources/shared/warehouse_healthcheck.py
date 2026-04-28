import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-41542e21-470f-4589-96d")
RAW_DATASET = os.getenv("BQ_DATASET", "Raw")

RAW_TABLES = [
    "ghl_objects_raw",
    "calendly_objects_raw",
    "fanbasis_transactions_txn_raw",
    "stripe_objects_raw",
    "fathom_calls_raw",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _client() -> bigquery.Client:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    return bigquery.Client(project=PROJECT_ID)


def _scalar(client: bigquery.Client, sql: str) -> Any:
    rows = list(client.query(sql).result())
    if not rows:
        return None
    return rows[0][0]


def run_healthcheck() -> Dict[str, Any]:
    client = _client()
    checks: List[Dict[str, Any]] = []

    # 1) Raw freshness for all source lanes.
    freshness_sql = f"""
    WITH latest AS (
      SELECT 'ghl_objects_raw' AS table_name, MAX(ingested_at) AS max_ingested_at FROM `{PROJECT_ID}.{RAW_DATASET}.ghl_objects_raw`
      UNION ALL
      SELECT 'calendly_objects_raw', MAX(ingested_at) FROM `{PROJECT_ID}.{RAW_DATASET}.calendly_objects_raw`
      UNION ALL
      SELECT 'fanbasis_transactions_txn_raw', MAX(ingested_at) FROM `{PROJECT_ID}.{RAW_DATASET}.fanbasis_transactions_txn_raw`
      UNION ALL
      SELECT 'stripe_objects_raw', MAX(ingested_at) FROM `{PROJECT_ID}.{RAW_DATASET}.stripe_objects_raw`
      UNION ALL
      SELECT 'fathom_calls_raw', MAX(ingested_at) FROM `{PROJECT_ID}.{RAW_DATASET}.fathom_calls_raw`
    )
    SELECT
      table_name,
      max_ingested_at,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), max_ingested_at, HOUR) AS hours_stale
    FROM latest
    ORDER BY table_name
    """
    freshness_rows = [dict(r.items()) for r in client.query(freshness_sql).result()]
    freshness_failures = [
        row
        for row in freshness_rows
        if row["max_ingested_at"] is None or (row["hours_stale"] is not None and row["hours_stale"] > 6)
    ]
    checks.append(
        {
            "check": "raw_freshness_6h",
            "status": "PASS" if not freshness_failures else "FAIL",
            "details": freshness_rows,
        }
    )

    # 2) Raw->Core parity failures (strict fail count only).
    parity_fail_count_sql = f"""
    WITH parity AS (
      SELECT
        'ghl_contacts' AS check_name,
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type='contacts') AS raw_distinct,
        (SELECT COUNT(DISTINCT contact_id) FROM `{PROJECT_ID}.Core.dim_ghl_contacts`) AS core_distinct,
        0.95 AS expected_min_ratio
      UNION ALL
      SELECT
        'ghl_opportunities',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type='opportunities'),
        (SELECT COUNT(DISTINCT opportunity_id) FROM `{PROJECT_ID}.Core.fct_ghl_opportunities`),
        0.95
      UNION ALL
      SELECT
        'ghl_form_submissions',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type='form_submissions'),
        (SELECT COUNT(DISTINCT submission_id) FROM `{PROJECT_ID}.Core.fct_ghl_form_submissions`),
        0.95
      UNION ALL
      SELECT
        'calendly_scheduled_events',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.calendly_objects_raw` WHERE entity_type='scheduled_events'),
        (SELECT COUNT(DISTINCT scheduled_event_id) FROM `{PROJECT_ID}.Core.fct_calendly_scheduled_events`),
        0.95
      UNION ALL
      SELECT
        'calendly_event_invitees',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.calendly_objects_raw` WHERE entity_type='event_invitees'),
        (SELECT COUNT(DISTINCT invitee_id) FROM `{PROJECT_ID}.Core.fct_calendly_event_invitees`),
        0.95
      UNION ALL
      SELECT
        'fanbasis_transactions',
        (SELECT COUNT(DISTINCT transaction_id) FROM `{PROJECT_ID}.Raw.fanbasis_transactions_txn_raw`),
        (SELECT COUNT(DISTINCT transaction_id) FROM `{PROJECT_ID}.Core.fct_fanbasis_transactions`),
        0.95
      UNION ALL
      SELECT
        'stripe_charges_to_payments',
        (SELECT COUNT(DISTINCT object_id) FROM `{PROJECT_ID}.Raw.stripe_objects_raw` WHERE object_type='charges'),
        (SELECT COUNT(DISTINCT payment_id) FROM `{PROJECT_ID}.Core.fct_stripe_payments`),
        0.80
      UNION ALL
      SELECT
        'fathom_calls',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.fathom_calls_raw` WHERE entity_type='calls'),
        (SELECT COUNT(DISTINCT call_id) FROM `{PROJECT_ID}.Core.fct_fathom_calls`),
        0.95
      UNION ALL
      SELECT
        'ghl_tasks',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type='tasks'),
        (SELECT COUNT(DISTINCT task_id) FROM `{PROJECT_ID}.Core.fct_ghl_tasks`),
        0.95
      UNION ALL
      SELECT
        'ghl_notes',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type='notes'),
        (SELECT COUNT(DISTINCT note_id) FROM `{PROJECT_ID}.Core.fct_ghl_notes`),
        0.95
      UNION ALL
      SELECT
        'ghl_conversations',
        (SELECT COUNT(DISTINCT entity_id) FROM `{PROJECT_ID}.Raw.ghl_objects_raw` WHERE entity_type IN ('conversations', 'conversation_messages')),
        (SELECT COUNT(DISTINCT message_id) FROM `{PROJECT_ID}.Core.fct_ghl_conversations`),
        0.95
    )
    SELECT COUNT(*)
    FROM parity
    WHERE raw_distinct > 0
      AND SAFE_DIVIDE(core_distinct, NULLIF(raw_distinct, 0)) < expected_min_ratio * 0.8
    """
    parity_fail_count = int(_scalar(client, parity_fail_count_sql) or 0)
    checks.append(
        {
            "check": "raw_to_core_parity_failures",
            "status": "PASS" if parity_fail_count == 0 else "FAIL",
            "details": {"fail_count": parity_fail_count},
        }
    )

    # 3) Marts rows and recency.
    marts_rows_sql = f"""
    SELECT
      (SELECT COUNT(*) FROM `{PROJECT_ID}.Marts.dim_golden_contact`) AS dim_golden_contact_rows,
      (SELECT COUNT(*) FROM `{PROJECT_ID}.Marts.fct_fanbasis_payment_line`) AS payment_line_rows,
      (SELECT COUNT(*) FROM `{PROJECT_ID}.Marts.rpt_campaign_funnel_month`) AS funnel_rows
    """
    marts_rows = dict(list(client.query(marts_rows_sql).result())[0].items())
    marts_row_ok = (
        marts_rows["dim_golden_contact_rows"] > 0
        and marts_rows["payment_line_rows"] > 0
        and marts_rows["funnel_rows"] > 0
    )
    checks.append(
        {
            "check": "marts_non_empty_core_tables",
            "status": "PASS" if marts_row_ok else "FAIL",
            "details": marts_rows,
        }
    )

    marts_recency_sql = f"""
    SELECT TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(),
      MAX(mart_refreshed_at),
      HOUR
    )
    FROM `{PROJECT_ID}.Marts.rpt_campaign_funnel_month`
    """
    marts_hours_stale = _scalar(client, marts_recency_sql)
    marts_stale_ok = marts_hours_stale is not None and int(marts_hours_stale) <= 6
    checks.append(
        {
            "check": "marts_refresh_recency_6h",
            "status": "PASS" if marts_stale_ok else "FAIL",
            "details": {"hours_stale": marts_hours_stale},
        }
    )

    failures = [c for c in checks if c["status"] == "FAIL"]
    return {
        "ok": len(failures) == 0,
        "checked_at": _now_iso(),
        "failure_count": len(failures),
        "checks": checks,
    }
