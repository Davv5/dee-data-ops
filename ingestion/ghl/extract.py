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
- Append-only + downstream staging dedupe is the idempotency contract.
  Source: `.claude/rules/ingest.md`, Data Ops notebook.

Reasoned defaults (NOT corpus-prescribed):
- Watermark per endpoint stored in BQ, keyed by endpoint name.
- Only `conversations` and `messages` honor `since`; other endpoints do full
  pulls and rely on staging dedupe. `messages` fans out from
  `raw_ghl.conversations` (latest-per-id), filtered to conversations whose
  `lastMessageDate` is newer than the cursor.
- Token-bucket throttle keeps request rate under GHL's 100/10s per-location
  limit; 429 responses retry with Retry-After backoff.

Track W additions (2026-04-22):
- `--endpoints` CSV flag splits the same image into hot (conversations,messages)
  and cold (contacts,opportunities,users,pipelines) groups.
- BQ advisory lock via `raw_ghl._job_locks` prevents concurrent executions of
  the same endpoint_group from double-ingesting on scheduler overlap.
  Lock expires after 2 minutes; crashes release via try/finally.
  Source for BQ-as-lock-store: reasoned default — already have BQ, no second
  backing store needed (`.claude/rules/ingest.md` decision, Track W).
"""

from __future__ import annotations

import collections
import json
import os
import sys
import time
import uuid
from argparse import ArgumentParser
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional

import requests
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID_DEV"]
RAW_DATASET = "raw_ghl"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"
LOCK_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._job_locks"

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

# Canonical ordering preserves conversations-before-messages dependency.
ALL_ENDPOINTS: list[str] = ["contacts", "conversations", "opportunities", "pipelines", "users", "messages"]

# Endpoint groups for the Cloud Run hot/cold split (Track W).
HOT_ENDPOINTS: list[str] = ["conversations", "messages"]
COLD_ENDPOINTS: list[str] = ["contacts", "opportunities", "users", "pipelines"]

# Token-bucket throttle. GHL v2 rate limit is 100 req / 10s per location;
# target 90 so bursts leave headroom for Retry-After-driven retries.
_RATE_LIMIT_WINDOW_SEC = 10.0
_RATE_LIMIT_MAX = 90
_request_times: collections.deque[float] = collections.deque()


def _bootstrap_adc() -> None:
    """Point Google ADC at the dev keyfile if not already set (local runs).

    In CI, google-github-actions/auth@v2 sets GOOGLE_APPLICATION_CREDENTIALS directly.
    In Cloud Run Jobs, the SA is bound to the job; ADC resolves automatically.
    """
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        return
    keyfile = os.environ.get("BQ_KEYFILE_PATH")
    if keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile


# env-var name → GCP Secret Manager secret ID in GCP_SECRET_MANAGER_PROJECT.
# CI sets GCP_SECRET_MANAGER_PROJECT=dee-data-ops-prod; local dev leaves it
# unset and relies on os.environ (populated from .env).
_SECRET_MANAGER_IDS: dict[str, str] = {
    "GHL_API_KEY": "ghl-api-key",
    "GHL_LOCATION_ID": "ghl-location-id",
}
_secret_cache: dict[str, str] = {}


def _load_secret(env_name: str) -> str:
    """Resolve a secret, preferring Secret Manager when GCP_SECRET_MANAGER_PROJECT is set.

    Priority:
      1. If GCP_SECRET_MANAGER_PROJECT is set and env_name is mapped, fetch
         from Secret Manager (cached per process).
      2. Otherwise, read os.environ[env_name] (local-dev path).
    Raises KeyError if neither source resolves the value.
    """
    if env_name in _secret_cache:
        return _secret_cache[env_name]

    sm_project = os.environ.get("GCP_SECRET_MANAGER_PROJECT")
    secret_id = _SECRET_MANAGER_IDS.get(env_name)
    if sm_project and secret_id:
        _bootstrap_adc()
        from google.cloud import secretmanager  # lazy import: optional for local dev

        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{sm_project}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        value = response.payload.data.decode("utf-8")
        _secret_cache[env_name] = value
        return value

    value = os.environ.get(env_name)
    if not value:
        raise KeyError(
            f"{env_name} not set and GCP_SECRET_MANAGER_PROJECT unset. "
            f"For local dev, set {env_name} in .env; "
            f"for prod/CI, set GCP_SECRET_MANAGER_PROJECT to the project "
            f"hosting secret '{_SECRET_MANAGER_IDS.get(env_name, '<unmapped>')}'."
        )
    _secret_cache[env_name] = value
    return value


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


def ensure_lock_table(client: bigquery.Client) -> None:
    """Create the BQ advisory-lock table if it doesn't exist.

    Used by Cloud Run Jobs to prevent concurrent executions of the same
    endpoint_group from double-ingesting when the 1-min scheduler fires
    before the prior run finishes. Lock rows self-expire at 2 min.
    (Track W, 2026-04-22 — BQ-as-lock-store pattern)
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{LOCK_TABLE}` (
        run_id STRING NOT NULL,
        endpoint_group STRING NOT NULL,
        started_at TIMESTAMP NOT NULL
    )
    """
    client.query(ddl).result()


def _acquire_lock(client: bigquery.Client, run_id: str, endpoint_group: str) -> bool:
    """Attempt to acquire a BQ advisory lock for this endpoint_group.

    Uses a MERGE to atomically insert the lock row only when no row exists
    for the same endpoint_group started within the last 2 minutes. Returns
    True if the lock was acquired (i.e., this run should proceed), False if
    a prior run is still executing (this run should skip / exit 0).

    Lock TTL is 2 minutes — chosen to be longer than the 1-min Cloud Scheduler
    cadence but shorter than a stuck-job timeout, so a crashed prior run
    auto-clears within 2 minutes. (Track W decision, 2026-04-22)
    """
    merge_sql = f"""
    MERGE `{LOCK_TABLE}` AS target
    USING (SELECT @run_id AS run_id, @endpoint_group AS endpoint_group,
                  CURRENT_TIMESTAMP() AS started_at) AS source
    ON target.endpoint_group = source.endpoint_group
       AND target.started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
    WHEN NOT MATCHED THEN
      INSERT (run_id, endpoint_group, started_at)
      VALUES (source.run_id, source.endpoint_group, source.started_at)
    """
    job = client.query(
        merge_sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("endpoint_group", "STRING", endpoint_group),
            ]
        ),
    )
    result = job.result()
    # num_dml_affected_rows == 1 means the INSERT fired; 0 means MATCHED (lock held)
    return (job.num_dml_affected_rows or 0) > 0


def _release_lock(client: bigquery.Client, run_id: str) -> None:
    """Delete the lock row for this run_id. Called in try/finally so crashes clean up."""
    client.query(
        f"DELETE FROM `{LOCK_TABLE}` WHERE run_id = @run_id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
        ),
    ).result()


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
    token = _load_secret("GHL_API_KEY")
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
    location_id = _load_secret("GHL_LOCATION_ID")
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


def _endpoint_group_name(endpoints: list[str]) -> str:
    """Stable name for the lock key — sorted so order doesn't matter."""
    return ",".join(sorted(endpoints))


def run(endpoints: list[str], since_override: Optional[datetime] = None, dry_run: bool = False) -> int:
    """Run the extractor for the given endpoint list.

    Args:
        endpoints: which endpoints to pull (subset of ALL_ENDPOINTS)
        since_override: if set, use as cursor for all endpoints instead of BQ state
        dry_run: exercise BQ client + state table without writing rows
    """
    client = get_client()
    ensure_state_table(client)
    ensure_lock_table(client)

    run_id = str(uuid.uuid4())
    endpoint_group = _endpoint_group_name(endpoints)

    # BQ advisory lock — skip if prior run for this group is still executing.
    # Only applies when running as Cloud Run Job (GCP_SECRET_MANAGER_PROJECT set).
    # For GHA / local runs (no GCP_SECRET_MANAGER_PROJECT), skip the lock so
    # manual workflow_dispatch reruns aren't blocked.
    use_lock = bool(os.environ.get("GCP_SECRET_MANAGER_PROJECT"))
    lock_acquired = False

    if use_lock:
        lock_acquired = _acquire_lock(client, run_id, endpoint_group)
        if not lock_acquired:
            print(json.dumps({
                "status": "skip",
                "reason": "prior run still executing",
                "endpoint_group": endpoint_group,
            }))
            return 0

    # Preserve conversations-before-messages ordering regardless of CLI input order.
    ordered = [e for e in ALL_ENDPOINTS if e in endpoints]

    total_loaded = 0
    started_at = datetime.now(timezone.utc)

    try:
        for endpoint in ordered:
            cursor = since_override if since_override is not None else read_cursor(client, endpoint)
            rows = fetch_endpoint(endpoint, cursor)

            if dry_run:
                print(json.dumps({"endpoint": endpoint, "cursor": str(cursor), "would_load": len(rows)}))
                continue

            loaded = load_rows(client, endpoint, rows)
            if loaded > 0:
                write_cursor(client, endpoint, started_at)
            total_loaded += loaded
            print(json.dumps({"endpoint": endpoint, "cursor": str(cursor), "loaded": loaded}))

        print(json.dumps({"total_loaded": total_loaded, "dry_run": dry_run, "run_id": run_id}))
    finally:
        if use_lock and lock_acquired:
            _release_lock(client, run_id)

    return 0


def main() -> int:
    parser = ArgumentParser(description="GHL → raw_ghl extractor")
    parser.add_argument(
        "--endpoints",
        metavar="ENDPOINT[,ENDPOINT...]",
        default=None,
        help=(
            "Comma-separated list of endpoints to pull. "
            f"Defaults to all: {','.join(ALL_ENDPOINTS)}. "
            "Hot group: conversations,messages. "
            "Cold group: contacts,opportunities,users,pipelines."
        ),
    )
    parser.add_argument(
        "--since",
        metavar="ISO-8601",
        default=None,
        help=(
            "Override the BQ cursor for all endpoints with this timestamp "
            "(e.g. 2026-04-22T00:00:00Z). Useful for backfills."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Exercise BQ client + state-table path without writing rows",
    )
    args = parser.parse_args()

    if args.endpoints:
        requested = [e.strip() for e in args.endpoints.split(",") if e.strip()]
        unknown = set(requested) - set(ALL_ENDPOINTS)
        if unknown:
            print(f"ERROR: unknown endpoints: {unknown}. Valid: {ALL_ENDPOINTS}", file=sys.stderr)
            return 1
        endpoints = requested
    else:
        endpoints = list(ALL_ENDPOINTS)

    since_override: Optional[datetime] = None
    if args.since:
        try:
            since_override = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
        except ValueError:
            print(f"ERROR: --since must be ISO-8601 (e.g. 2026-04-22T00:00:00Z), got: {args.since}", file=sys.stderr)
            return 1

    return run(endpoints=endpoints, since_override=since_override, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
