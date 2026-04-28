"""Date-windowed backfill for GHL form submissions.

The /forms/submissions endpoint returns only recent data without date filters.
With startAt/endAt it exposes full history back to Sep 2024.

This script windows through time from GHL_FORMS_START_DATE to today in
GHL_FORMS_WINDOW_DAYS chunks, paginating within each window and upserting
into ghl_objects_raw as entity_type='form_submissions'.

Usage:
    python3 ghl_form_submissions_backfill.py

Env overrides:
    GHL_FORMS_START_DATE    YYYY-MM-DD, default 2024-09-01
    GHL_FORMS_WINDOW_DAYS   days per window, default 30
    GHL_FORMS_RUN_MODELS    true/false, run ghl_models after, default true
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from sources.ghl.ghl_pipeline import (
    GHL_ACCESS_TOKEN,
    GHL_API_BASE,
    GHL_API_VERSION,
    GHL_LOCATION_ID,
    GHL_PAGE_LIMIT,
    GHL_REQUEST_TIMEOUT_SEC,
    GHL_MAX_ATTEMPTS_DEFAULT,
    build_rows,
    ensure_tables,
    run_models,
    upsert_raw_rows,
    write_state,
    read_state,
    _utc_now,
)
import time

ENTITY_TYPE = "form_submissions"
ENDPOINT = "/forms/submissions"

BACKFILL_START_DATE = os.getenv("GHL_FORMS_START_DATE", "2024-09-01")
WINDOW_DAYS = int(os.getenv("GHL_FORMS_WINDOW_DAYS", "30"))
RUN_MODELS_AFTER = os.getenv("GHL_FORMS_RUN_MODELS", "true").lower() == "true"


def _fetch_submissions_window(
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    """Fetch all form submissions in [start, end) — paginating through all pages."""
    url = f"{GHL_API_BASE.rstrip('/')}{ENDPOINT}"
    base_params = {
        "locationId": GHL_LOCATION_ID,
        "startAt": start.strftime("%Y-%m-%d"),
        "endAt": end.strftime("%Y-%m-%d"),
        "limit": GHL_PAGE_LIMIT,
    }
    headers = {
        "Authorization": f"Bearer {GHL_ACCESS_TOKEN}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
    }

    all_items: List[Dict[str, Any]] = []
    page = 1
    attempts = max(1, GHL_MAX_ATTEMPTS_DEFAULT)

    while True:
        params = {**base_params, "page": page}
        backoff = 1.0
        resp = None
        for attempt in range(1, attempts + 1):
            resp = requests.get(url, headers=headers, params=params, timeout=GHL_REQUEST_TIMEOUT_SEC)
            if resp.ok:
                break
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            break

        if resp is None or not resp.ok:
            status = "unknown" if resp is None else str(resp.status_code)
            body = "" if resp is None else (resp.text or "")[:500]
            raise RuntimeError(
                f"form_submissions_backfill: request failed window={start.date()}–{end.date()} "
                f"page={page} status={status} body={body}"
            )

        payload = resp.json()
        submissions = payload.get("submissions", [])
        if not isinstance(submissions, list):
            submissions = []

        items = [x for x in submissions if isinstance(x, dict)]
        all_items.extend(items)

        meta = payload.get("meta", {})
        next_page = meta.get("nextPage")
        if not next_page or len(items) == 0:
            break
        page = next_page

    return all_items


def _window_key(window_start: datetime) -> str:
    return f"form_submissions_window_{window_start.strftime('%Y%m%d')}"


def main() -> None:
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")

    ensure_tables()

    run_id = os.getenv(
        "GHL_FORMS_RUN_ID",
        f"ghl-forms-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}",
    )

    start_dt = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = _utc_now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    total_windows = 0
    total_rows = 0
    window_start = start_dt

    print(
        f"Starting form_submissions backfill run_id={run_id} "
        f"from={start_dt.date()} to={end_dt.date()} "
        f"window_days={WINDOW_DAYS} location_id={GHL_LOCATION_ID}"
    )

    while window_start < end_dt:
        window_end = min(window_start + timedelta(days=WINDOW_DAYS), end_dt)
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
            items = _fetch_submissions_window(start=window_start, end=window_end)
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

    print(f"Form submissions backfill complete: {total_windows} windows, {total_rows} rows written.")

    if RUN_MODELS_AFTER:
        executed = run_models()
        print(f"GHL models refreshed. statements_executed={executed}")


if __name__ == "__main__":
    main()
