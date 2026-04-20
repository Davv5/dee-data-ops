"""GHL (GoHighLevel) extractor → raw_ghl.* in BigQuery.

Pulls six v2 / LeadConnector API endpoints — contacts, conversations,
opportunities, users, messages, pipelines — and lands each as `WRITE_APPEND`
to its own table in `raw_ghl`, with per-endpoint wall-clock watermarks in
`raw_ghl._sync_state`.

Corpus guidance applied:
- Land raw, unformatted (no casts, no renames, no joins here). Source:
  "Data Ingestion / Raw Landing Zone", Data Ops notebook.
- One dataset per source (`raw_ghl`, not `raw.ghl_*`). Same source.
- Secrets via env vars only; no credentials in this file or in git.

Reasoned defaults (NOT corpus-prescribed):
- Append-only + `_ingested_at` column; dedupe in staging (Phase 2).
- Watermark per endpoint stored in BQ, keyed by endpoint name.
- Only `conversations` and `messages` honor `since`; other endpoints do full
  pulls and rely on staging dedupe. `messages` fans out from
  `raw_ghl.conversations` (latest-per-id), filtered to conversations whose
  `lastMessageDate` is newer than the cursor.
- Token-bucket throttle keeps request rate under GHL's 100/10s per-location
  limit; 429 responses retry with Retry-After backoff.
"""

from __future__ import annotations

import collections
import json
import os
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID_DEV"]
RAW_DATASET = "raw_ghl"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"

GHL_API_BASE = "https://services.leadconnectorhq.com"

# Per-endpoint Version header. conversations + messages sit on an older
# version than the other three — confirmed from the published OpenAPI spec.
# Ordering matters: messages fans out from raw_ghl.conversations, so
# conversations must run first on the same invocation.
VERSIONS: dict[str, str] = {
    "contacts": "2021-07-28",
    "conversations": "2021-04-15",
    "opportunities": "2021-07-28",
    "pipelines": "2021-07-28",
    "users": "2021-07-28",
    "messages": "2021-04-15",
}

ENDPOINTS: list[str] = list(VERSIONS.keys())

# Token-bucket throttle. GHL v2 rate limit is 100 req / 10s per location;
# target 90 so bursts leave headroom for Retry-After-driven retries.
_RATE_LIMIT_WINDOW_SEC = 10.0
_RATE_LIMIT_MAX = 90
_request_times: collections.deque[float] = collections.deque()


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


def _throttle() -> None:
    now = time.monotonic()
    while _request_times and now - _request_times[0] > _RATE_LIMIT_WINDOW_SEC:
        _request_times.popleft()
    if len(_request_times) >= _RATE_LIMIT_MAX:
        wait = _RATE_LIMIT_WINDOW_SEC - (now - _request_times[0]) + 0.05
        if wait > 0:
            time.sleep(wait)
    _request_times.append(time.monotonic())


def _get(
    endpoint: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    max_retries: int = 3,
) -> dict[str, Any]:
    for attempt in range(max_retries + 1):
        _throttle()
        r = requests.get(
            f"{GHL_API_BASE}{path}",
            headers=_headers(endpoint),
            params=params or {},
            timeout=30,
        )
        if r.status_code == 429 and attempt < max_retries:
            retry_after = int(r.headers.get("Retry-After", "5"))
            time.sleep(max(retry_after, 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("unreachable")


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


def _fetch_pipelines(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    data = _get("pipelines", "/opportunities/pipelines", {"locationId": location_id})
    return data.get("pipelines") or []


def _conversations_to_fetch(since: Optional[datetime]) -> list[str]:
    """Conversation IDs whose lastMessageDate is newer than `since`.

    Dedupes `raw_ghl.conversations` to the latest `_ingested_at` per id, then
    filters on `lastMessageDate` (epoch-ms in the JSON payload). `since=None`
    returns every conversation.
    """
    since_ms = int(since.timestamp() * 1000) if since else None
    client = get_client()
    query = f"""
    SELECT id
    FROM (
      SELECT id, payload,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
      FROM `{PROJECT_ID}.{RAW_DATASET}.conversations`
    )
    WHERE rn = 1
      AND id IS NOT NULL
      AND (@since_ms IS NULL
           OR SAFE_CAST(JSON_VALUE(payload, '$.lastMessageDate') AS INT64) > @since_ms)
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("since_ms", "INT64", since_ms),
            ],
        ),
    )
    return [row.id for row in job.result()]


def _fetch_messages_for_conversation(conv_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    params: dict[str, Any] = {"limit": 100}
    while True:
        data = _get("messages", f"/conversations/{conv_id}/messages", params)
        # GHL returns {"messages": {"lastMessageId": ..., "nextPage": ..., "messages": [...]}}
        # or sometimes flattens it; handle both shapes.
        envelope = data.get("messages", data)
        if isinstance(envelope, list):
            batch = envelope
            next_page = False
            last_id = batch[-1].get("id") if batch else None
        else:
            batch = envelope.get("messages") or []
            next_page = bool(envelope.get("nextPage"))
            last_id = envelope.get("lastMessageId") or (batch[-1].get("id") if batch else None)
        rows.extend(batch)
        if not next_page or not last_id:
            break
        params["lastMessageId"] = last_id
    return rows


def _fetch_messages(location_id: str, since: Optional[datetime]) -> list[dict[str, Any]]:
    conv_ids = _conversations_to_fetch(since)
    sample_n = os.environ.get("GHL_MESSAGES_SAMPLE_N")
    if sample_n:
        conv_ids = conv_ids[: int(sample_n)]
    print(json.dumps({"endpoint": "messages", "conversations_to_fetch": len(conv_ids)}))
    rows: list[dict[str, Any]] = []
    for conv_id in conv_ids:
        rows.extend(_fetch_messages_for_conversation(conv_id))
    return rows


FETCHERS: dict[str, Callable[[str, Optional[datetime]], list[dict[str, Any]]]] = {
    "contacts": _fetch_contacts,
    "conversations": _fetch_conversations,
    "opportunities": _fetch_opportunities,
    "pipelines": _fetch_pipelines,
    "users": _fetch_users,
    "messages": _fetch_messages,
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
