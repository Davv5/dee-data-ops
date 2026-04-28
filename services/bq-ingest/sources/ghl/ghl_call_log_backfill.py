"""Date-windowed backfill for GHL outbound call logs.

The /conversations/messages/export endpoint requires startTime/endTime (epoch ms)
and does not support cursor pagination. This script windows through time from
GHL_CALL_LOG_START_DATE to today in GHL_CALL_LOG_WINDOW_DAYS chunks, fetching
one page per window and upserting into ghl_objects_raw as entity_type='outbound_call_logs'.

Usage:
    python3 ghl_call_log_backfill.py

Env overrides:
    GHL_CALL_LOG_START_DATE   YYYY-MM-DD, default 2024-01-01
    GHL_CALL_LOG_WINDOW_DAYS  days per window, default 30
    GHL_CALL_LOG_RUN_MODELS   true/false, run ghl_models after, default true
    GHL_CALL_LOG_TASK_INDEX   optional shard index override (defaults to CLOUD_RUN_TASK_INDEX)
    GHL_CALL_LOG_TASK_COUNT   optional shard count override (defaults to CLOUD_RUN_TASK_COUNT)
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery

from sources.ghl.ghl_pipeline import (
    GHL_ACCESS_TOKEN,
    GHL_API_BASE,
    GHL_API_VERSION,
    GHL_AUTH_SCHEME,
    GHL_LOCATION_ID,
    GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS,
    GHL_REQUEST_TIMEOUT_SEC,
    PROJECT_ID,
    DATASET,
    GHL_RAW_TABLE,
    GHL_STATE_TABLE,
    SOURCE_NAME,
    build_rows,
    ensure_tables,
    run_models,
    upsert_raw_rows,
    write_state,
    read_state,
    _utc_now,
)

ENTITY_TYPE = "outbound_call_logs"
ENDPOINT = "/conversations/messages/export"

BACKFILL_START_DATE = os.getenv("GHL_CALL_LOG_START_DATE", "2024-01-01")
WINDOW_DAYS = int(os.getenv("GHL_CALL_LOG_WINDOW_DAYS", "30"))
RUN_MODELS_AFTER = os.getenv("GHL_CALL_LOG_RUN_MODELS", "true").lower() == "true"
TASK_INDEX = int(os.getenv("GHL_CALL_LOG_TASK_INDEX", os.getenv("CLOUD_RUN_TASK_INDEX", "0")))
TASK_COUNT = max(1, int(os.getenv("GHL_CALL_LOG_TASK_COUNT", os.getenv("CLOUD_RUN_TASK_COUNT", "1"))))


def _to_epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _fetch_call_logs_window(
    start: datetime,
    end: datetime,
    run_id: str,
) -> List[Dict[str, Any]]:
    """Fetch all call log messages in [start, end) from the messages export endpoint."""
    url = f"{GHL_API_BASE.rstrip('/')}{ENDPOINT}"
    params = {
        "locationId": GHL_LOCATION_ID,
        "startTime": _to_epoch_ms(start),
        "endTime": _to_epoch_ms(end),
        "type": "TYPE_CALL",
    }

    if GHL_AUTH_SCHEME == "bearer":
        auth_values = [f"Bearer {GHL_ACCESS_TOKEN}"]
    elif GHL_AUTH_SCHEME in {"raw", "plain", "token"}:
        auth_values = [GHL_ACCESS_TOKEN]
    else:
        auth_values = [f"Bearer {GHL_ACCESS_TOKEN}", GHL_ACCESS_TOKEN]

    attempts = max(1, GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS)
    last_resp = None

    for auth_value in auth_values:
        headers = {
            "Authorization": auth_value,
            "Version": GHL_API_VERSION,
            "Accept": "application/json",
        }
        backoff = 1.0
        for attempt in range(1, attempts + 1):
            resp = requests.get(url, headers=headers, params=params, timeout=GHL_REQUEST_TIMEOUT_SEC)
            last_resp = resp
            if resp.ok:
                payload = resp.json()
                if isinstance(payload, list):
                    return [x for x in payload if isinstance(x, dict)]
                if isinstance(payload, dict):
                    for key in ("messages", "data", "items", "results", "calls"):
                        val = payload.get(key)
                        if isinstance(val, list):
                            return [x for x in val if isinstance(x, dict)]
                return []
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            break
        if last_resp and not (last_resp.status_code == 401 and "invalid jwt" in (last_resp.text or "").lower()):
            break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body = "" if last_resp is None else (last_resp.text or "")[:500]
    raise RuntimeError(
        f"call_log_backfill: request failed window={start.date()}–{end.date()} "
        f"status={status} body={body}"
    )


def _window_key(window_start: datetime) -> str:
    """Stable state key for a given window — one state row per window date."""
    return f"outbound_call_logs_window_{window_start.strftime('%Y%m%d')}"


def main() -> None:
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")

    ensure_tables()

    run_id = os.getenv(
        "GHL_CALL_LOG_RUN_ID",
        f"ghl-calllogs-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}",
    )

    start_dt = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = _utc_now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    total_windows = 0
    total_rows = 0
    window_start = start_dt

    print(
        f"Starting call log backfill run_id={run_id} "
        f"from={start_dt.date()} to={end_dt.date()} "
        f"window_days={WINDOW_DAYS} location_id={GHL_LOCATION_ID} "
        f"task_index={TASK_INDEX}/{TASK_COUNT}"
    )

    while window_start < end_dt:
        window_end = min(window_start + timedelta(days=WINDOW_DAYS), end_dt)
        window_index = int((window_start - start_dt).days / WINDOW_DAYS)
        owns_window = (window_index % TASK_COUNT) == TASK_INDEX
        if not owns_window:
            window_start = window_end
            continue
        window_entity = _window_key(window_start)

        existing = read_state(run_id=run_id, entity_type=window_entity, location_id=GHL_LOCATION_ID)
        if existing and existing.get("status") == "COMPLETED":
            print(f"Skipping {window_start.date()}–{window_end.date()}: already COMPLETED")
            window_start = window_end
            continue

        started_at = datetime.now(timezone.utc)
        write_state(
            run_id=run_id, entity_type=window_entity, location_id=GHL_LOCATION_ID,
            status="RUNNING", next_cursor=None, pages_processed=0,
            rows_written=0, started_at=started_at,
        )

        try:
            items = _fetch_call_logs_window(start=window_start, end=window_end, run_id=run_id)
            rows = build_rows(entity_type=ENTITY_TYPE, items=items, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)

            write_state(
                run_id=run_id, entity_type=window_entity, location_id=GHL_LOCATION_ID,
                status="COMPLETED", next_cursor=None, pages_processed=1,
                rows_written=len(rows), started_at=started_at,
            )
            total_rows += len(rows)
            total_windows += 1
            print(
                f"Window {window_start.date()}–{window_end.date()}: "
                f"{len(items)} items fetched, {len(rows)} rows written"
            )

        except Exception as exc:
            write_state(
                run_id=run_id, entity_type=window_entity, location_id=GHL_LOCATION_ID,
                status="FAILED", next_cursor=None, pages_processed=0,
                rows_written=0, started_at=started_at, error_text=str(exc)[:2000],
            )
            raise

        window_start = window_end

    print(f"Call log backfill complete: {total_windows} windows, {total_rows} rows written.")

    if RUN_MODELS_AFTER and TASK_COUNT == 1:
        executed = run_models()
        print(f"GHL models refreshed. statements_executed={executed}")
    elif RUN_MODELS_AFTER and TASK_COUNT > 1:
        print(
            "Skipping model refresh inside sharded execution. "
            "Run model.ghl once after all shards complete."
        )


if __name__ == "__main__":
    main()
