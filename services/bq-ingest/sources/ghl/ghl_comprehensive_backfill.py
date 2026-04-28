"""Comprehensive GHL historical backfill.

Fills five entity types that were either never ingested or have date gaps:
  - messages        : individual messages within conversations (sender attribution)
  - outbound_call_logs : call records (extends existing backfill to full history)
  - form_submissions   : form submissions (extends existing backfill to full history)
  - notes           : contact-level notes (rep activity, qualification context)
  - tasks           : contact-level tasks (rep workload, follow-up tracking)

Designed to run daily on a schedule and be fully idempotent:
  - Date-windowed entities (call_logs, form_submissions) skip completed windows via state.
  - Per-entity entities (messages, notes, tasks) skip already-processed IDs via state.
  - Default run identity is stable so reruns resume prior progress instead of reprocessing full history.

Env overrides:
    GHL_BACKFILL_ENABLE_MESSAGES           true/false (default true)
    GHL_BACKFILL_ENABLE_CALL_LOGS          true/false (default true)
    GHL_BACKFILL_ENABLE_FORM_SUBMISSIONS   true/false (default true)
    GHL_BACKFILL_ENABLE_NOTES              true/false (default true)
    GHL_BACKFILL_ENABLE_TASKS              true/false (default true)

    GHL_BACKFILL_CALL_LOGS_START_DATE      YYYY-MM-DD (default 2024-01-01)
    GHL_BACKFILL_FORMS_START_DATE          YYYY-MM-DD (default 2024-09-01)
    GHL_BACKFILL_WINDOW_DAYS               days per date window (default 30)
    GHL_BACKFILL_RUN_MODELS                true/false, run ghl_models after (default true)
    GHL_COMPREHENSIVE_BACKFILL_RUN_ID      explicit run identifier override (optional)
    GHL_COMPREHENSIVE_BACKFILL_DEFAULT_RUN_ID stable default run id (default ghl-comprehensive-backfill-v1)
    GHL_BACKFILL_PER_ENTITY_DELAY_SEC      sleep between per-entity API calls (default 0.15)
    GHL_BACKFILL_MAX_CONTACTS_PER_RUN      cap per-entity processing per run (default 5000)
"""

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery

from sources.ghl.ghl_pipeline import (
    GHL_ACCESS_TOKEN,
    GHL_API_BASE,
    GHL_API_VERSION,
    GHL_LOCATION_ID,
    GHL_PAGE_LIMIT,
    GHL_REQUEST_TIMEOUT_SEC,
    GHL_MAX_ATTEMPTS_DEFAULT,
    GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS,
    PROJECT_ID,
    DATASET,
    GHL_RAW_TABLE,
    GHL_STATE_TABLE,
    build_rows,
    ensure_tables,
    run_models,
    upsert_raw_rows,
    write_state,
    read_state,
    _utc_now,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ENABLE_MESSAGES = os.getenv("GHL_BACKFILL_ENABLE_MESSAGES", "true").lower() == "true"
ENABLE_CALL_LOGS = os.getenv("GHL_BACKFILL_ENABLE_CALL_LOGS", "true").lower() == "true"
ENABLE_FORM_SUBMISSIONS = os.getenv("GHL_BACKFILL_ENABLE_FORM_SUBMISSIONS", "true").lower() == "true"
ENABLE_NOTES = os.getenv("GHL_BACKFILL_ENABLE_NOTES", "true").lower() == "true"
ENABLE_TASKS = os.getenv("GHL_BACKFILL_ENABLE_TASKS", "true").lower() == "true"

CALL_LOGS_START_DATE = os.getenv("GHL_BACKFILL_CALL_LOGS_START_DATE", "2024-01-01")
FORMS_START_DATE = os.getenv("GHL_BACKFILL_FORMS_START_DATE", "2024-09-01")
WINDOW_DAYS = int(os.getenv("GHL_BACKFILL_WINDOW_DAYS", "30"))
RUN_MODELS_AFTER = os.getenv("GHL_BACKFILL_RUN_MODELS", "true").lower() == "true"
PER_ENTITY_DELAY = float(os.getenv("GHL_BACKFILL_PER_ENTITY_DELAY_SEC", "0.15"))
MAX_ENTITIES_PER_RUN = int(os.getenv("GHL_BACKFILL_MAX_CONTACTS_PER_RUN", "5000"))
DEFAULT_RUN_ID = os.getenv("GHL_COMPREHENSIVE_BACKFILL_DEFAULT_RUN_ID", "ghl-comprehensive-backfill-v1")
LEGACY_RUN_PREFIX = os.getenv("GHL_COMPREHENSIVE_BACKFILL_LEGACY_PREFIX", "ghl-comprehensive-")

# Task sharding — Cloud Run sets CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT automatically
# when tasks > 1. Each shard processes a slice of contacts/conversations and a slice of
# date windows, so N parallel tasks complete the job N× faster.
TASK_INDEX = int(os.getenv("GHL_BACKFILL_TASK_INDEX", os.getenv("CLOUD_RUN_TASK_INDEX", "0")))
TASK_COUNT = max(1, int(os.getenv("GHL_BACKFILL_TASK_COUNT", os.getenv("CLOUD_RUN_TASK_COUNT", "1"))))


# ---------------------------------------------------------------------------
# Shared HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {GHL_ACCESS_TOKEN}",
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _get(endpoint: str, params: Dict[str, Any], max_attempts: int = GHL_MAX_ATTEMPTS_DEFAULT) -> Dict[str, Any]:
    url = f"{GHL_API_BASE.rstrip('/')}{endpoint}"
    backoff = 1.0
    last_resp = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, headers=_headers(), params=params, timeout=GHL_REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            if attempt < max_attempts:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise RuntimeError(f"GET {endpoint} failed after {max_attempts} attempts: {exc}") from exc
        last_resp = resp
        if resp.ok:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if resp.status_code in (401, 403, 404):
            # Not retryable — return empty rather than crashing the whole run
            return {}
        resp.raise_for_status()
    raise RuntimeError(f"GET {endpoint} failed, last status={getattr(last_resp, 'status_code', 'unknown')}")


def _post(endpoint: str, payload: Dict[str, Any], max_attempts: int = GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS) -> Dict[str, Any]:
    url = f"{GHL_API_BASE.rstrip('/')}{endpoint}"
    backoff = 1.0
    last_resp = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(url, headers=_headers(), json=payload, timeout=GHL_REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            if attempt < max_attempts:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise RuntimeError(f"POST {endpoint} failed after {max_attempts} attempts: {exc}") from exc
        last_resp = resp
        if resp.ok:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        resp.raise_for_status()
    raise RuntimeError(f"POST {endpoint} failed, last status={getattr(last_resp, 'status_code', 'unknown')}")


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def _query(sql: str) -> List[Dict[str, Any]]:
    return [dict(row) for row in _bq().query(sql).result()]


def _state_key(entity_type: str, identifier: str) -> str:
    return f"{entity_type}__{identifier}"


def _is_completed(run_id: str, entity_type: str, identifier: str) -> bool:
    state = read_state(run_id=run_id, entity_type=_state_key(entity_type, identifier), location_id=GHL_LOCATION_ID)
    return bool(state and state.get("status") == "COMPLETED")


def _run_has_any_state(run_id: str) -> bool:
    if not run_id:
        return False
    job = _bq().query(
        f"""
        SELECT COUNT(*) AS row_count
        FROM `{PROJECT_ID}.{DATASET}.{GHL_STATE_TABLE}`
        WHERE run_id = @run_id
          AND location_id = @location_id
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("location_id", "STRING", GHL_LOCATION_ID),
            ]
        ),
    )
    result = list(job.result())
    return bool(result and int(result[0]["row_count"]) > 0)


def _latest_legacy_run_id() -> Optional[str]:
    job = _bq().query(
        f"""
        SELECT run_id
        FROM `{PROJECT_ID}.{DATASET}.{GHL_STATE_TABLE}`
        WHERE location_id = @location_id
          AND run_id LIKE @run_prefix
          AND run_id != @default_run_id
        GROUP BY run_id
        ORDER BY MAX(updated_at) DESC
        LIMIT 1
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("location_id", "STRING", GHL_LOCATION_ID),
                bigquery.ScalarQueryParameter("run_prefix", "STRING", f"{LEGACY_RUN_PREFIX}%"),
                bigquery.ScalarQueryParameter("default_run_id", "STRING", DEFAULT_RUN_ID),
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    return rows[0]["run_id"]


def _resolve_run_id() -> tuple[str, str]:
    explicit_run_id = os.getenv("GHL_COMPREHENSIVE_BACKFILL_RUN_ID")
    if explicit_run_id:
        return explicit_run_id.strip(), "explicit"

    if _run_has_any_state(DEFAULT_RUN_ID):
        return DEFAULT_RUN_ID, "resume_default"

    legacy_run_id = _latest_legacy_run_id()
    if legacy_run_id:
        return legacy_run_id, "resume_legacy"

    return DEFAULT_RUN_ID, "new_default"


def _mark_completed(run_id: str, entity_type: str, identifier: str, rows_written: int, started_at: datetime) -> None:
    write_state(
        run_id=run_id,
        entity_type=_state_key(entity_type, identifier),
        location_id=GHL_LOCATION_ID,
        status="COMPLETED",
        next_cursor=None,
        pages_processed=1,
        rows_written=rows_written,
        started_at=started_at,
    )


def _mark_failed(run_id: str, entity_type: str, identifier: str, error: str, started_at: datetime) -> None:
    write_state(
        run_id=run_id,
        entity_type=_state_key(entity_type, identifier),
        location_id=GHL_LOCATION_ID,
        status="FAILED",
        next_cursor=None,
        pages_processed=0,
        rows_written=0,
        started_at=started_at,
        error_text=error[:2000],
    )


# ---------------------------------------------------------------------------
# 1. Messages (per conversation)
# ---------------------------------------------------------------------------

def _fetch_messages(conversation_id: str) -> List[Dict[str, Any]]:
    """Paginate through all messages for a conversation."""
    all_messages: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = _get(
            f"/conversations/{conversation_id}/messages",
            params={"limit": GHL_PAGE_LIMIT, "page": page},
        )
        messages = data.get("messages", [])
        if not isinstance(messages, list):
            break
        items = [m for m in messages if isinstance(m, dict)]
        all_messages.extend(items)
        # GHL returns nextPage or stops when fewer than limit items returned
        next_page = data.get("nextPage") or (data.get("meta") or {}).get("nextPage")
        if not next_page or len(items) < GHL_PAGE_LIMIT:
            break
        page = next_page if isinstance(next_page, int) else page + 1
    return all_messages


def run_messages(run_id: str) -> int:
    """Backfill individual messages for all known conversations."""
    conversations = _query(f"""
        SELECT DISTINCT entity_id AS conversation_id
        FROM `{PROJECT_ID}.{DATASET}.{GHL_RAW_TABLE}`
        WHERE entity_type = 'conversations'
          AND location_id = '{GHL_LOCATION_ID}'
        ORDER BY entity_id
    """)

    total_rows = 0
    processed = 0

    print(f"  messages: task {TASK_INDEX}/{TASK_COUNT}, total conversations={len(conversations)}")

    for idx, row in enumerate(conversations):
        # Shard: each task processes only its slice
        if idx % TASK_COUNT != TASK_INDEX:
            continue

        if processed >= MAX_ENTITIES_PER_RUN:
            print(f"  messages: reached MAX_ENTITIES_PER_RUN={MAX_ENTITIES_PER_RUN}, stopping")
            break

        conv_id = row["conversation_id"]
        if _is_completed(run_id, "messages", conv_id):
            continue

        started_at = _utc_now()
        try:
            messages = _fetch_messages(conv_id)
            # Inject conversationId into each message so build_rows can use it as location hint
            for m in messages:
                m.setdefault("conversationId", conv_id)
                m.setdefault("locationId", GHL_LOCATION_ID)
            rows = build_rows(entity_type="message", items=messages, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _mark_completed(run_id, "messages", conv_id, len(rows), started_at)
            total_rows += len(rows)
            processed += 1
            if rows:
                print(f"  messages: conversation={conv_id} fetched={len(messages)} rows={len(rows)}")
        except Exception as exc:
            _mark_failed(run_id, "messages", conv_id, str(exc), started_at)
            print(f"  messages: WARN conversation={conv_id} error={exc}")

        time.sleep(PER_ENTITY_DELAY)

    print(f"messages backfill: {processed} conversations processed, {total_rows} rows written")
    return total_rows


# ---------------------------------------------------------------------------
# 2. Outbound call logs (date-windowed)
# ---------------------------------------------------------------------------

def _fetch_call_logs_window(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    data = _post(
        "/conversations/messages/export",
        payload={
            "locationId": GHL_LOCATION_ID,
            "startTime": int(start.timestamp() * 1000),
            "endTime": int(end.timestamp() * 1000),
        },
    )
    messages = data.get("messages", [])
    if not isinstance(messages, list):
        messages = []
    return [m for m in messages if isinstance(m, dict)]


def run_call_logs(run_id: str) -> int:
    start_dt = datetime.strptime(CALL_LOGS_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = _utc_now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    total_rows = 0

    # Pre-compute all windows so we can shard by index
    windows = []
    w = start_dt
    while w < end_dt:
        windows.append(w)
        w = min(w + timedelta(days=WINDOW_DAYS), end_dt)

    print(f"  call_logs: task {TASK_INDEX}/{TASK_COUNT}, total windows={len(windows)}")

    for window_idx, window_start in enumerate(windows):
        # Shard: each task processes only its slice of windows
        if window_idx % TASK_COUNT != TASK_INDEX:
            continue

        window_end = min(window_start + timedelta(days=WINDOW_DAYS), end_dt)
        key = window_start.strftime("%Y%m%d")

        if _is_completed(run_id, "call_logs_window", key):
            continue

        started_at = _utc_now()
        try:
            items = _fetch_call_logs_window(window_start, window_end)
            rows = build_rows(entity_type="outbound_call_logs", items=items, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _mark_completed(run_id, "call_logs_window", key, len(rows), started_at)
            total_rows += len(rows)
            print(f"  call_logs: window={window_start.date()}–{window_end.date()} fetched={len(items)} rows={len(rows)}")
        except Exception as exc:
            _mark_failed(run_id, "call_logs_window", key, str(exc), started_at)
            print(f"  call_logs: WARN window={window_start.date()} error={exc}")

    print(f"call_logs backfill: {total_rows} rows written")
    return total_rows


# ---------------------------------------------------------------------------
# 3. Form submissions (date-windowed)
# ---------------------------------------------------------------------------

def _fetch_form_submissions_window(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = _get(
            "/forms/submissions",
            params={
                "locationId": GHL_LOCATION_ID,
                "startAt": start.strftime("%Y-%m-%d"),
                "endAt": end.strftime("%Y-%m-%d"),
                "limit": GHL_PAGE_LIMIT,
                "page": page,
            },
        )
        submissions = data.get("submissions", [])
        if not isinstance(submissions, list):
            break
        items = [x for x in submissions if isinstance(x, dict)]
        all_items.extend(items)
        next_page = (data.get("meta") or {}).get("nextPage")
        if not next_page or len(items) == 0:
            break
        page = next_page
    return all_items


def run_form_submissions(run_id: str) -> int:
    start_dt = datetime.strptime(FORMS_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = _utc_now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    total_rows = 0

    # Pre-compute all windows so we can shard by index
    windows = []
    w = start_dt
    while w < end_dt:
        windows.append(w)
        w = min(w + timedelta(days=WINDOW_DAYS), end_dt)

    print(f"  form_submissions: task {TASK_INDEX}/{TASK_COUNT}, total windows={len(windows)}")

    for window_idx, window_start in enumerate(windows):
        # Shard: each task processes only its slice of windows
        if window_idx % TASK_COUNT != TASK_INDEX:
            continue

        window_end = min(window_start + timedelta(days=WINDOW_DAYS), end_dt)
        key = window_start.strftime("%Y%m%d")

        if _is_completed(run_id, "form_submissions_window", key):
            continue

        started_at = _utc_now()
        try:
            items = _fetch_form_submissions_window(window_start, window_end)
            rows = build_rows(entity_type="form_submissions", items=items, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _mark_completed(run_id, "form_submissions_window", key, len(rows), started_at)
            total_rows += len(rows)
            print(f"  form_submissions: window={window_start.date()}–{window_end.date()} fetched={len(items)} rows={len(rows)}")
        except Exception as exc:
            _mark_failed(run_id, "form_submissions_window", key, str(exc), started_at)
            print(f"  form_submissions: WARN window={window_start.date()} error={exc}")

    print(f"form_submissions backfill: {total_rows} rows written")
    return total_rows


# ---------------------------------------------------------------------------
# 4. Notes (per contact)
# ---------------------------------------------------------------------------

def _fetch_notes(contact_id: str) -> List[Dict[str, Any]]:
    data = _get(f"/contacts/{contact_id}/notes", params={})
    notes = data.get("notes", [])
    if not isinstance(notes, list):
        return []
    for note in notes:
        if isinstance(note, dict):
            note.setdefault("contactId", contact_id)
            note.setdefault("locationId", GHL_LOCATION_ID)
    return [n for n in notes if isinstance(n, dict)]


def run_notes(run_id: str) -> int:
    contacts = _query(f"""
        SELECT DISTINCT entity_id AS contact_id
        FROM `{PROJECT_ID}.{DATASET}.{GHL_RAW_TABLE}`
        WHERE entity_type = 'contacts'
          AND location_id = '{GHL_LOCATION_ID}'
        ORDER BY entity_id
    """)

    total_rows = 0
    processed = 0

    print(f"  notes: task {TASK_INDEX}/{TASK_COUNT}, total contacts={len(contacts)}")

    for idx, row in enumerate(contacts):
        # Shard: each task processes only its slice
        if idx % TASK_COUNT != TASK_INDEX:
            continue

        if processed >= MAX_ENTITIES_PER_RUN:
            print(f"  notes: reached MAX_ENTITIES_PER_RUN={MAX_ENTITIES_PER_RUN}, stopping")
            break

        contact_id = row["contact_id"]
        if _is_completed(run_id, "notes", contact_id):
            continue

        started_at = _utc_now()
        try:
            notes = _fetch_notes(contact_id)
            rows = build_rows(entity_type="notes", items=notes, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _mark_completed(run_id, "notes", contact_id, len(rows), started_at)
            total_rows += len(rows)
            processed += 1
        except Exception as exc:
            _mark_failed(run_id, "notes", contact_id, str(exc), started_at)
            print(f"  notes: WARN contact={contact_id} error={exc}")

        time.sleep(PER_ENTITY_DELAY)

    print(f"notes backfill: {processed} contacts processed, {total_rows} rows written")
    return total_rows


# ---------------------------------------------------------------------------
# 5. Tasks (per contact)
# ---------------------------------------------------------------------------

def _fetch_tasks(contact_id: str) -> List[Dict[str, Any]]:
    data = _get(f"/contacts/{contact_id}/tasks", params={})
    tasks = data.get("tasks", [])
    if not isinstance(tasks, list):
        return []
    for task in tasks:
        if isinstance(task, dict):
            task.setdefault("contactId", contact_id)
            task.setdefault("locationId", GHL_LOCATION_ID)
    return [t for t in tasks if isinstance(t, dict)]


def run_tasks(run_id: str) -> int:
    contacts = _query(f"""
        SELECT DISTINCT entity_id AS contact_id
        FROM `{PROJECT_ID}.{DATASET}.{GHL_RAW_TABLE}`
        WHERE entity_type = 'contacts'
          AND location_id = '{GHL_LOCATION_ID}'
        ORDER BY entity_id
    """)

    total_rows = 0
    processed = 0

    print(f"  tasks: task {TASK_INDEX}/{TASK_COUNT}, total contacts={len(contacts)}")

    for idx, row in enumerate(contacts):
        # Shard: each task processes only its slice
        if idx % TASK_COUNT != TASK_INDEX:
            continue

        if processed >= MAX_ENTITIES_PER_RUN:
            print(f"  tasks: reached MAX_ENTITIES_PER_RUN={MAX_ENTITIES_PER_RUN}, stopping")
            break

        contact_id = row["contact_id"]
        if _is_completed(run_id, "tasks", contact_id):
            continue

        started_at = _utc_now()
        try:
            tasks = _fetch_tasks(contact_id)
            rows = build_rows(entity_type="tasks", items=tasks, run_id=run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _mark_completed(run_id, "tasks", contact_id, len(rows), started_at)
            total_rows += len(rows)
            processed += 1
        except Exception as exc:
            _mark_failed(run_id, "tasks", contact_id, str(exc), started_at)
            print(f"  tasks: WARN contact={contact_id} error={exc}")

        time.sleep(PER_ENTITY_DELAY)

    print(f"tasks backfill: {processed} contacts processed, {total_rows} rows written")
    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")

    ensure_tables()

    run_id, run_mode = _resolve_run_id()

    print(
        f"Starting GHL comprehensive backfill run_id={run_id} mode={run_mode} "
        f"location_id={GHL_LOCATION_ID} task={TASK_INDEX}/{TASK_COUNT}"
    )
    if run_mode == "new_default":
        print(
            "Run mode is new_default: no prior state found for default or legacy runs; "
            "starting a new resumable run.",
            flush=True,
        )
    elif run_mode.startswith("resume"):
        print("Run mode is resume: continuing from previously recorded state.", flush=True)
    else:
        print("Run mode is explicit: using caller-provided run_id.", flush=True)
    print(f"Enabled: messages={ENABLE_MESSAGES} call_logs={ENABLE_CALL_LOGS} "
          f"form_submissions={ENABLE_FORM_SUBMISSIONS} notes={ENABLE_NOTES} tasks={ENABLE_TASKS}")

    totals: Dict[str, int] = {}

    if ENABLE_CALL_LOGS:
        print("\n--- call_logs ---")
        totals["call_logs"] = run_call_logs(run_id)

    if ENABLE_FORM_SUBMISSIONS:
        print("\n--- form_submissions ---")
        totals["form_submissions"] = run_form_submissions(run_id)

    if ENABLE_MESSAGES:
        print("\n--- messages ---")
        totals["messages"] = run_messages(run_id)

    if ENABLE_NOTES:
        print("\n--- notes ---")
        totals["notes"] = run_notes(run_id)

    if ENABLE_TASKS:
        print("\n--- tasks ---")
        totals["tasks"] = run_tasks(run_id)

    print(f"\nComprehensive backfill complete task={TASK_INDEX}/{TASK_COUNT}: {totals}")

    # Only task 0 runs models — avoids N concurrent model refreshes
    if RUN_MODELS_AFTER and TASK_INDEX == 0:
        executed = run_models()
        print(f"GHL models refreshed. statements_executed={executed}")
    elif RUN_MODELS_AFTER:
        print(f"Skipping model refresh (task {TASK_INDEX} defers to task 0)")


if __name__ == "__main__":
    main()
