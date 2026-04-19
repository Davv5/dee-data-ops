"""Fanbasis extractor → raw_fanbasis.* in BigQuery.

Pulls v1 endpoints (customers, subscriptions, payments) on an incremental cursor,
lands each as WRITE_APPEND to its own table in `raw_fanbasis`, and stores per-endpoint
watermarks in `raw_fanbasis._sync_state`.

Status: skeleton. BQ side is wired and exercisable via --dry-run. The Fanbasis API side
(`fetch_endpoint`) is a stub — fill in once API credentials and endpoint shapes are
confirmed. If the API proves unreliable, fall back to the CSV-export path documented
in the README (scope Risk #5).

Mirrors `ingestion/ghl/extract.py` — see that file for design-choice rationale and
corpus citations.
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
RAW_DATASET = "raw_fanbasis"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"

ENDPOINTS: list[str] = ["customers", "subscriptions", "payments"]


def _bootstrap_adc() -> None:
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        return
    keyfile = os.environ.get("BQ_KEYFILE_PATH")
    if keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile


def get_client() -> bigquery.Client:
    _bootstrap_adc()
    return bigquery.Client(project=PROJECT_ID)


def ensure_state_table(client: bigquery.Client) -> None:
    client.query(f"""
    CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
        endpoint STRING NOT NULL,
        last_synced_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
    )
    """).result()


def read_cursor(client: bigquery.Client, endpoint: str) -> Optional[datetime]:
    job = client.query(
        f"""
        SELECT last_synced_at FROM `{STATE_TABLE}`
        WHERE endpoint = @endpoint
        ORDER BY updated_at DESC LIMIT 1
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint)],
        ),
    )
    rows = list(job.result())
    return rows[0].last_synced_at if rows else None


def write_cursor(client: bigquery.Client, endpoint: str, synced_at: datetime) -> None:
    client.query(
        f"""
        INSERT INTO `{STATE_TABLE}` (endpoint, last_synced_at, updated_at)
        VALUES (@endpoint, @last_synced_at, CURRENT_TIMESTAMP())
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint),
                bigquery.ScalarQueryParameter("last_synced_at", "TIMESTAMP", synced_at),
            ],
        ),
    ).result()


def fetch_endpoint(endpoint: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    """TODO(week-0): implement against Fanbasis API. CSV fallback documented in README."""
    _api_key = os.environ.get("FANBASIS_API_KEY")
    print(f"[stub] fetch_endpoint(endpoint={endpoint!r}, since={since})")
    return []


RAW_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
]


def load_rows(client: bigquery.Client, endpoint: str, rows: Iterable[dict[str, Any]]) -> int:
    """Land rows as (id, _ingested_at, payload-JSON-string) — mirrors ghl/extract.py.

    See that file for the rationale (schema-drift-proofing the raw landing zone).
    """
    rows = list(rows)
    if not rows:
        return 0
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{endpoint}"
    ingested_at = datetime.now(timezone.utc).isoformat()
    wrapped = [
        {
            "id": r.get("id"),
            "_ingested_at": ingested_at,
            "payload": json.dumps(r, default=str),
        }
        for r in rows
    ]
    client.load_table_from_json(
        wrapped,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema=RAW_SCHEMA,
        ),
    ).result()
    return len(rows)


def run(dry_run: bool = False) -> int:
    client = get_client()
    ensure_state_table(client)
    started_at = datetime.now(timezone.utc)
    total_loaded = 0

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
    parser = ArgumentParser(description="Fanbasis → raw_fanbasis extractor")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return run(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
