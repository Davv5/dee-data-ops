"""GHL (GoHighLevel) extractor → raw_ghl.* in BigQuery.

Pulls the four v1 endpoints (contacts, conversations, opportunities, users) on an
incremental cursor, lands each as `WRITE_APPEND` to its own table in the `raw_ghl`
dataset, and stores per-endpoint watermarks in `raw_ghl._sync_state`.

Status: skeleton. The BQ side (client, load, state table, cursor I/O) is fully wired
and exercised by `--dry-run`. The GHL API side (`fetch_endpoint`) is a stub that
returns []; fill in once the Week-0 GHL API credentials and endpoint schemas land.

Corpus guidance applied:
- Land raw, unformatted (no casts, no renames, no joins here). Source:
  "Data Ingestion / Raw Landing Zone", Data Ops notebook.
- One dataset per source (`raw_ghl`, not `raw.ghl_*`). Same source.
- Secrets via env vars only; no credentials in this file or in git.

Reasoned defaults (NOT corpus-prescribed — revisit if the API shape forces it):
- Append-only + `_ingested_at` column; dedupe in staging (Phase 2).
- Watermark per endpoint in a small state table, keyed by endpoint name.
- First run with a NULL cursor does a full pull; subsequent runs are deltas.
"""

from __future__ import annotations

import json
import os
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID_DEV"]
RAW_DATASET = "raw_ghl"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"

ENDPOINTS: list[str] = ["contacts", "conversations", "opportunities", "users"]


def _bootstrap_adc() -> None:
    """Point Google ADC at the dev keyfile if not already set (local runs).

    In CI, google-github-actions/auth@v2 sets GOOGLE_APPLICATION_CREDENTIALS directly.
    """
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        return
    keyfile = os.environ.get("BQ_KEYFILE_PATH")
    if keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile


def get_client() -> bigquery.Client:
    _bootstrap_adc()
    return bigquery.Client(project=PROJECT_ID)


def ensure_state_table(client: bigquery.Client) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
        endpoint STRING NOT NULL,
        last_synced_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """
    client.query(ddl).result()


def read_cursor(client: bigquery.Client, endpoint: str) -> Optional[datetime]:
    query = f"""
    SELECT last_synced_at
    FROM `{STATE_TABLE}`
    WHERE endpoint = @endpoint
    ORDER BY updated_at DESC
    LIMIT 1
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint)],
        ),
    )
    rows = list(job.result())
    return rows[0].last_synced_at if rows else None


def write_cursor(client: bigquery.Client, endpoint: str, synced_at: datetime) -> None:
    query = f"""
    INSERT INTO `{STATE_TABLE}` (endpoint, last_synced_at, updated_at)
    VALUES (@endpoint, @last_synced_at, CURRENT_TIMESTAMP())
    """
    client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint),
                bigquery.ScalarQueryParameter("last_synced_at", "TIMESTAMP", synced_at),
            ],
        ),
    ).result()


def fetch_endpoint(endpoint: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    """Pull rows from GHL for `endpoint`, modified since `since` (or all if None).

    TODO(week-0): implement once GHL API key + endpoint shapes are confirmed. For now
    this is a stub returning []; the rest of the pipeline is exercisable via --dry-run.
    """
    _api_key = os.environ.get("GHL_API_KEY")
    print(f"[stub] fetch_endpoint(endpoint={endpoint!r}, since={since})")
    return []


def load_rows(client: bigquery.Client, endpoint: str, rows: Iterable[dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{endpoint}"
    ingested_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["_ingested_at"] = ingested_at
    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            autodetect=True,
        ),
    )
    job.result()
    return len(rows)


def run(dry_run: bool = False) -> int:
    client = get_client()
    ensure_state_table(client)

    total_loaded = 0
    started_at = datetime.now(timezone.utc)

    for endpoint in ENDPOINTS:
        cursor = read_cursor(client, endpoint)
        rows = fetch_endpoint(endpoint, cursor)

        if dry_run:
            print(json.dumps({"endpoint": endpoint, "cursor": str(cursor), "would_load": len(rows)}))
            continue

        loaded = load_rows(client, endpoint, rows)
        if loaded > 0:
            write_cursor(client, endpoint, started_at)
        total_loaded += loaded
        print(json.dumps({"endpoint": endpoint, "cursor": str(cursor), "loaded": loaded}))

    print(json.dumps({"total_loaded": total_loaded, "dry_run": dry_run}))
    return 0


def main() -> int:
    parser = ArgumentParser(description="GHL → raw_ghl extractor")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Exercise BQ client + state-table path without writing rows",
    )
    args = parser.parse_args()
    return run(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
