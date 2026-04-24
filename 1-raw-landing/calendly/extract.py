"""Calendly extractor -> raw_calendly.* in BigQuery.

Pulls three Calendly v2 API endpoints — scheduled_events, invitees, invitee_no_shows —
and lands each as WRITE_APPEND to its own table in `raw_calendly`, with per-endpoint
wall-clock watermarks in `raw_calendly._sync_state`.

Mirrors the GHL extractor pattern (Track W, 2026-04-22) exactly:
- Same BQ advisory-lock table (`raw_calendly._job_locks`)
- Same token-bucket throttle (60 req/min; exponential backoff on 429)
- Same `_ingested_at` timestamp column + JSON payload column schema
- Same `--endpoints` CSV flag and `--since` backfill flag
- Same Secret Manager resolution via GCP_SECRET_MANAGER_PROJECT

Corpus guidance applied:
- Land raw, unformatted (no casts, no renames, no joins here). Source:
  ".claude/rules/ingest.md" ingestion contract, Data Ops notebook.
- Append-only + downstream staging dedupe is the idempotency contract.
  Source: ".claude/rules/ingest.md", Data Ops notebook.
- Both Fivetran and Cloud Run share the same raw_calendly.* dataset;
  staging's ROW_NUMBER() dedup handles overlap transparently during
  the cutover window. Source: "Why Data Migrations Go Wrong (3 reasons)",
  Data Ops notebook; confirmed by ask-corpus query 2026-04-22.

Reasoned defaults (NOT corpus-prescribed):
- Calendly API base: https://api.calendly.com (v2 — Bearer token auth)
- Token: loaded from Secret Manager secret `calendly-api-token`
  (name chosen to match the track decision; David creates + populates
  the secret manually — see docs/runbooks/calendly-cloud-run-extractor.md).
- Endpoint strategy:
  * `scheduled_events` — incremental poll by `min_start_time` (cursor on
    event start time); Calendly v2 supports `count`, `page_token` pagination.
  * `invitees` — fans out from `scheduled_events` (same pull window);
    calls /scheduled_events/{uuid}/invitees for each event returned.
  * `invitee_no_shows` — calls /invitee_no_shows, filtered by created_at
    watermark; lower volume, no fan-out needed.
- Rate limit: Calendly API is not publicly documented with a hard req/min
  limit; token-bucket at 60 req/min with exponential backoff on 429.
  If 429s are observed at 1-min cadence, raise Cloud Scheduler to 2-min
  and note in runbook. (Track X decision, 2026-04-22)
- Single Cloud Run Job (no hot/cold split): Calendly data volume is ~1-2
  orders of magnitude below GHL. One 1-min job covers all endpoints.
  (Track X decision, 2026-04-22)

Track X (2026-04-22):
- Replaces Fivetran Calendly connector (daily sync) with this poller.
- During dual-run overlap window, both Fivetran rows (_fivetran_synced)
  and poller rows (_ingested_at) coexist in raw_calendly.*. Staging dedup
  handles both via coalesce(_ingested_at, cast(_fivetran_synced as timestamp)).
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
from typing import Any, Iterable, Optional

import requests
from google.cloud import bigquery

PROJECT_ID = os.environ["GCP_PROJECT_ID_DEV"]
RAW_DATASET = "raw_calendly"
STATE_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._sync_state"
LOCK_TABLE = f"{PROJECT_ID}.{RAW_DATASET}._job_locks"

CALENDLY_API_BASE = "https://api.calendly.com"

# Canonical endpoint ordering — invitees must run after scheduled_events
# because we fan out from the events returned in the same pull window.
ALL_ENDPOINTS: list[str] = ["scheduled_events", "invitees", "invitee_no_shows"]

# Token-bucket throttle: 60 req/min target (conservative; Calendly does not
# publish a hard per-minute limit; backs off on 429). Track X decision.
_RATE_LIMIT_WINDOW_SEC = 60.0
_RATE_LIMIT_MAX = 60
_request_times: collections.deque[float] = collections.deque()

# Secret Manager mapping: env-var name -> secret ID in GCP_SECRET_MANAGER_PROJECT
_SECRET_MANAGER_IDS: dict[str, str] = {
    "CALENDLY_API_TOKEN": "calendly-api-token",
}
_secret_cache: dict[str, str] = {}


def _bootstrap_adc() -> None:
    """Point Google ADC at the dev keyfile if not already set (local runs).

    In CI, google-github-actions/auth@v2 sets GOOGLE_APPLICATION_CREDENTIALS.
    In Cloud Run Jobs, the SA binding resolves ADC automatically.
    """
    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        return
    keyfile = os.environ.get("BQ_KEYFILE_PATH")
    if keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile


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

    Mirrors GHL pattern (Track W). Prevents concurrent Cloud Run Job
    executions from double-ingesting when the 1-min scheduler fires
    before the prior run completes. Lock rows self-expire at 2 min.
    (Track X, 2026-04-22 — BQ-as-lock-store pattern)
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
    for the same endpoint_group started within the last 2 minutes.
    Returns True if lock acquired (proceed), False if prior run still executing (skip).
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


def _auth_headers() -> dict[str, str]:
    token = _load_secret("CALENDLY_API_TOKEN")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _throttle() -> None:
    """Token-bucket throttle: max 60 req/min. Mirrors GHL throttle pattern."""
    now = time.monotonic()
    while _request_times and now - _request_times[0] > _RATE_LIMIT_WINDOW_SEC:
        _request_times.popleft()
    if len(_request_times) >= _RATE_LIMIT_MAX:
        wait = _RATE_LIMIT_WINDOW_SEC - (now - _request_times[0]) + 0.1
        if wait > 0:
            time.sleep(wait)
    _request_times.append(time.monotonic())


def _get(
    path: str,
    params: Optional[dict[str, Any]] = None,
    max_retries: int = 3,
) -> dict[str, Any]:
    """GET from Calendly v2 API with throttle + exponential backoff on 429."""
    for attempt in range(max_retries + 1):
        _throttle()
        r = requests.get(
            f"{CALENDLY_API_BASE}{path}",
            headers=_auth_headers(),
            params=params or {},
            timeout=30,
        )
        if r.status_code == 429 and attempt < max_retries:
            retry_after = int(r.headers.get("Retry-After", str(2 ** attempt)))
            print(json.dumps({
                "warn": "429 rate limited",
                "path": path,
                "retry_after_sec": retry_after,
                "attempt": attempt,
            }))
            time.sleep(max(retry_after, 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("unreachable")


def _get_organization_uri() -> str:
    """Resolve the current user's organization URI from the Calendly API.

    Required as a filter param for /scheduled_events and /invitee_no_shows.
    Cached per process to avoid repeated calls.
    """
    if not hasattr(_get_organization_uri, "_cached"):
        data = _get("/users/me")
        _get_organization_uri._cached = data["resource"]["current_organization"]  # type: ignore[attr-defined]
    return _get_organization_uri._cached  # type: ignore[attr-defined]


def _fetch_scheduled_events(since: Optional[datetime]) -> list[dict[str, Any]]:
    """Fetch scheduled events using min_start_time as cursor.

    Calendly v2 /scheduled_events supports:
      - organization: filter by org URI
      - min_start_time / max_start_time: ISO-8601 window
      - count: max 100 per page
      - page_token: cursor pagination

    We use min_start_time = cursor (or 30 days ago for first run) to pull
    new/updated events incrementally. We do NOT use max_start_time to avoid
    missing recently-created future events.
    """
    org_uri = _get_organization_uri()
    rows: list[dict[str, Any]] = []

    if since is None:
        # First run: pull last 30 days to bootstrap
        since = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        import datetime as dt_mod
        since = since - dt_mod.timedelta(days=30)

    params: dict[str, Any] = {
        "organization": org_uri,
        "min_start_time": since.strftime("%Y-%m-%dT%H:%M:%S.000000Z"),
        "count": 100,
        "sort": "start_time:asc",
        "status": "active",
    }

    while True:
        data = _get("/scheduled_events", params)
        collection = data.get("collection") or []
        rows.extend(collection)
        next_page = (data.get("pagination") or {}).get("next_page_token")
        if not next_page or not collection:
            break
        params = {"page_token": next_page, "count": 100}

    return rows


def _fetch_invitees(since: Optional[datetime]) -> list[dict[str, Any]]:
    """Fetch invitees by fanning out from the events returned in the same window.

    For each event returned by _fetch_scheduled_events, calls
    /scheduled_events/{uuid}/invitees with count=100 + page_token pagination.
    Mirrors the GHL conversations->messages fan-out pattern.

    Event URIs look like: https://api.calendly.com/scheduled_events/<uuid>
    We extract the uuid from the URI to build the invitee endpoint path.
    """
    events = _fetch_scheduled_events(since)
    rows: list[dict[str, Any]] = []

    for event in events:
        event_uri = event.get("uri", "")
        # Extract UUID from the full URI
        event_uuid = event_uri.split("/")[-1] if event_uri else None
        if not event_uuid:
            continue

        params: dict[str, Any] = {"count": 100}
        while True:
            data = _get(f"/scheduled_events/{event_uuid}/invitees", params)
            collection = data.get("collection") or []
            # Annotate each invitee row with its parent event_uri for traceability
            for invitee in collection:
                invitee["_event_uri"] = event_uri
            rows.extend(collection)
            next_page = (data.get("pagination") or {}).get("next_page_token")
            if not next_page or not collection:
                break
            params = {"page_token": next_page, "count": 100}

    return rows


def _fetch_invitee_no_shows(since: Optional[datetime]) -> list[dict[str, Any]]:
    """Fetch invitee no-shows filtered by created_at watermark.

    Calendly v2 /invitee_no_shows is a flat list endpoint (not paginated by
    event). We filter by organization and use created_at as watermark.
    """
    org_uri = _get_organization_uri()
    rows: list[dict[str, Any]] = []

    params: dict[str, Any] = {
        "organization": org_uri,
        "count": 100,
    }
    if since is not None:
        params["created_at_gt"] = since.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

    while True:
        data = _get("/invitee_no_shows", params)
        collection = data.get("collection") or []
        rows.extend(collection)
        next_page = (data.get("pagination") or {}).get("next_page_token")
        if not next_page or not collection:
            break
        params = {"page_token": next_page, "count": 100}

    return rows


FETCHERS = {
    "scheduled_events": _fetch_scheduled_events,
    "invitees": _fetch_invitees,
    "invitee_no_shows": _fetch_invitee_no_shows,
}

# BQ schema: same shape as GHL raw tables.
# payload-as-string sidesteps Calendly schema drift (nested objects vary
# by event type). Source: GHL extractor design decision, Track W.
RAW_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
]


def load_rows(client: bigquery.Client, endpoint: str, rows: Iterable[dict[str, Any]]) -> int:
    """Land rows as (id, _ingested_at, payload-JSON-string).

    Why payload-as-string: same reasoning as GHL — Calendly's event payloads
    include deeply nested location objects whose shape varies by event type.
    BQ autodetect would fail on schema drift; JSON string avoids it.
    """
    rows = list(rows)
    if not rows:
        return 0

    # Table name matches what dbt staging expects (and what Fivetran was writing to)
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{endpoint}"
    ingested_at = datetime.now(timezone.utc).isoformat()

    def _extract_id(row: dict[str, Any]) -> Optional[str]:
        """Extract a stable row ID from the Calendly payload."""
        uri = row.get("uri", "")
        if uri:
            return uri.split("/")[-1]  # UUID portion of the URI
        return row.get("uuid") or row.get("id")

    wrapped = [
        {
            "id": _extract_id(r),
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
    return ",".join(sorted(endpoints))


def run(
    endpoints: list[str],
    since_override: Optional[datetime] = None,
    dry_run: bool = False,
) -> int:
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
    # Only applies in Cloud Run (GCP_SECRET_MANAGER_PROJECT set).
    # Local/GHA runs skip the lock so manual reruns aren't blocked.
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

    # Preserve canonical ordering regardless of CLI input order.
    ordered = [e for e in ALL_ENDPOINTS if e in endpoints]

    total_loaded = 0
    started_at = datetime.now(timezone.utc)

    try:
        for endpoint in ordered:
            cursor = since_override if since_override is not None else read_cursor(client, endpoint)
            rows = FETCHERS[endpoint](cursor)

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
    parser = ArgumentParser(description="Calendly -> raw_calendly extractor")
    parser.add_argument(
        "--endpoints",
        metavar="ENDPOINT[,ENDPOINT...]",
        default=None,
        help=(
            "Comma-separated list of endpoints to pull. "
            f"Defaults to all: {','.join(ALL_ENDPOINTS)}."
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
