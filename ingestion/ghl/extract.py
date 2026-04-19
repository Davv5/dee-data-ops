"""GHL (GoHighLevel) extractor → raw_ghl.* in BigQuery.

Pulls four v2 / LeadConnector API endpoints — contacts, conversations,
opportunities, users — and lands each as `WRITE_APPEND` to its own table in
`raw_ghl`, with per-endpoint wall-clock watermarks in `raw_ghl._sync_state`.

Corpus guidance applied:
- Land raw, unformatted (no casts, no renames, no joins here). Source:
  "Data Ingestion / Raw Landing Zone", Data Ops notebook.
- One dataset per source (`raw_ghl`, not `raw.ghl_*`). Same source.
- Secrets via env vars only; no credentials in this file or in git.

Reasoned defaults (NOT corpus-prescribed):
- Append-only + `_ingested_at` column; dedupe in staging (Phase 2).
- Watermark per endpoint stored in BQ, keyed by endpoint name.
- Only `conversations` honors `since` (via `startAfterDate`); other endpoints
  do a full pull and rely on staging dedupe. GHL's GET endpoints don't expose
  a clean updated-since filter; the POST /contacts/search variant has an
  undocumented body schema — revisit if volume forces incremental.
"""

from __future__ import annotations

import json
import os
import sys
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID_DEV"]
RAW_DATASET = "raw_ghl"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"

GHL_API_BASE = "https://services.leadconnectorhq.com"

# Per-endpoint Version header. conversations sits on an older version than the
# other three — confirmed from the published OpenAPI spec.
VERSIONS: dict[str, str] = {
    "contacts": "2021-07-28",
    "conversations": "2021-04-15",
    "opportunities": "2021-07-28",
    "users": "2021-07-28",
}

ENDPOINTS: list[str] = list(VERSIONS.keys())


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


def _headers(endpoint: str) -> dict[str, str]:
    token = os.environ["GHL_API_KEY"]
    return {
        "Authorization": f"Bearer {token}",
        "Version": VERSIONS[endpoint],
        "Accept": "application/json",
    }


def _get(endpoint: str, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    r = requests.get(
        f"{GHL_API_BASE}{path}",
        headers=_headers(endpoint),
        params=params or {},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return int(dt.timestamp() * 1000)
    return None


def _fetch_contacts(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {"locationId": location_id, "limit": 100}
    while True:
        data = _get("contacts", "/contacts/", params)
        batch = data.get("contacts") or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 100:
            break
        last = batch[-1]
        start_after = _to_epoch_ms(last.get("dateAdded") or last.get("dateUpdated"))
        start_after_id = last.get("id")
        if start_after is None or not start_after_id:
            break
        params["startAfter"] = start_after
        params["startAfterId"] = start_after_id
    return rows


def _fetch_conversations(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {
        "locationId": location_id,
        "limit": 100,
        "sort": "asc",
        "sortBy": "last_message_date",
        "lastMessageType": ["TYPE_CALL", "TYPE_SMS"],
    }
    if since is not None:
        params["startAfterDate"] = int(since.timestamp() * 1000)

    while True:
        data = _get("conversations", "/conversations/search", params)
        batch = data.get("conversations") or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 100:
            break
        next_cursor = _to_epoch_ms(batch[-1].get("lastMessageDate"))
        if next_cursor is None:
            break
        params["startAfterDate"] = next_cursor
    return rows


def _fetch_opportunities(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {"location_id": location_id, "limit": 100}
    while True:
        data = _get("opportunities", "/opportunities/search", params)
        batch = data.get("opportunities") or []
        if not batch:
            break
        rows.extend(batch)
        meta = data.get("meta") or {}
        start_after = meta.get("startAfter")
        start_after_id = meta.get("startAfterId")
        if not start_after_id:
            break
        params["startAfter"] = start_after
        params["startAfterId"] = start_after_id
    return rows


def _fetch_users(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    data = _get("users", "/users/", {"locationId": location_id})
    return data.get("users") or []


FETCHERS: dict[str, Callable[[str, Optional[datetime]], list[dict[str, Any]]]] = {
    "contacts": _fetch_contacts,
    "conversations": _fetch_conversations,
    "opportunities": _fetch_opportunities,
    "users": _fetch_users,
}


def fetch_endpoint(endpoint: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    location_id = os.environ.get("GHL_LOCATION_ID")
    if not location_id:
        raise RuntimeError("GHL_LOCATION_ID env var is required but not set")
    return FETCHERS[endpoint](location_id, since)


RAW_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
]


def load_rows(client: bigquery.Client, endpoint: str, rows: Iterable[dict[str, Any]]) -> int:
    """Land rows as (id, _ingested_at, payload-JSON-string).

    Why payload-as-string rather than per-field columns: GHL responses have
    mixed-type nested fields (e.g. contacts.customFields.value is a string in
    some rows and an array in others). BQ autodetect picks one shape from the
    first row and then chokes on the rest. Storing the full source row as a
    JSON string sidesteps all schema-drift issues; staging parses with
    JSON_VALUE / PARSE_JSON into typed columns.
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
    job = client.load_table_from_json(
        wrapped,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema=RAW_SCHEMA,
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
