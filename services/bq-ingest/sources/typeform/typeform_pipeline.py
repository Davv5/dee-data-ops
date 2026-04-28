import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")

TYPEFORM_API_KEY = (os.getenv("TYPEFORM_API_KEY") or "").strip()
TYPEFORM_API_BASE = os.getenv("TYPEFORM_API_BASE", "https://api.typeform.com")
TYPEFORM_PAGE_LIMIT = int(os.getenv("TYPEFORM_PAGE_LIMIT", "200"))

TYPEFORM_RAW_TABLE = os.getenv("BQ_TYPEFORM_RAW_TABLE", "typeform_objects_raw")
TYPEFORM_STATE_TABLE = os.getenv("BQ_TYPEFORM_STATE_TABLE", "typeform_backfill_state")

TYPEFORM_INCREMENTAL_LOOKBACK_HOURS = int(os.getenv("TYPEFORM_INCREMENTAL_LOOKBACK_HOURS", "2"))
TYPEFORM_REQUEST_TIMEOUT_SEC = int(os.getenv("TYPEFORM_REQUEST_TIMEOUT_SEC", "30"))
TYPEFORM_MAX_ATTEMPTS = int(os.getenv("TYPEFORM_MAX_ATTEMPTS", "4"))

SOURCE_NAME = "typeform"

client = bigquery.Client(project=PROJECT_ID)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    """Parse ISO 8601 timestamp to UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def ensure_tables() -> None:
    """Create Raw and State tables if they don't exist."""
    raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TYPEFORM_RAW_TABLE}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      event_ts TIMESTAMP,
      updated_at_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY DATE(event_ts)
    CLUSTER BY entity_type, entity_id
    """
    client.query(raw_query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TYPEFORM_STATE_TABLE}` (
      run_id STRING NOT NULL,
      entity_type STRING NOT NULL,
      status STRING NOT NULL,
      next_cursor STRING,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    """
    client.query(state_query).result()


def _require_runtime_config() -> None:
    """Validate required API credentials are set."""
    if not TYPEFORM_API_KEY:
        raise RuntimeError("Missing TYPEFORM_API_KEY environment variable")


def fetch_typeform_page(
    endpoint: str,
    page_size: int = TYPEFORM_PAGE_LIMIT,
    page_token: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    max_attempts: int = TYPEFORM_MAX_ATTEMPTS,
    initial_backoff_sec: float = 1.0,
) -> Dict[str, Any]:
    """Fetch a page from Typeform API with exponential backoff."""
    if not TYPEFORM_API_KEY:
        raise RuntimeError("Missing TYPEFORM_API_KEY")

    url = f"{TYPEFORM_API_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {TYPEFORM_API_KEY}",
        "Accept": "application/json",
    }

    query_params: Dict[str, Any] = {"page_size": page_size}
    if params:
        query_params.update(params)
    if page_token:
        query_params["after"] = page_token

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=headers, params=query_params, timeout=TYPEFORM_REQUEST_TIMEOUT_SEC)
        last_resp = resp
        if resp.ok:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:500]
    raise RuntimeError(
        f"Typeform request failed (endpoint={endpoint}, status={status}, body={body_preview})"
    )


def fetch_forms() -> List[Dict[str, Any]]:
    """Fetch all forms from workspace."""
    forms: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        payload = fetch_typeform_page("/forms", page_size=TYPEFORM_PAGE_LIMIT, page_token=page_token)
        items = payload.get("items", [])
        if not items:
            break
        forms.extend(items)
        page_token = payload.get("page_token")
        if not page_token:
            break

    return forms


def fetch_form_responses(form_id: str, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Fetch responses for a specific form, optionally filtered by modified_at."""
    responses: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    params: Dict[str, Any] = {}

    if since:
        # Typeform accepts RFC3339 timestamps like `YYYY-MM-DDTHH:MM:SSZ`.
        params["since"] = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        payload = fetch_typeform_page(
            f"/forms/{form_id}/responses",
            page_size=TYPEFORM_PAGE_LIMIT,
            page_token=page_token,
            params=params,
        )
        items = payload.get("items", [])
        if not items:
            break
        responses.extend(items)
        page_token = payload.get("page_token")
        if not page_token:
            break

    return responses


def build_raw_rows(
    entity_type: str,
    items: List[Dict[str, Any]],
    run_id: str,
    is_backfill: bool = False,
) -> Tuple[List[Dict[str, Any]], int]:
    """Build raw rows from API response items."""
    rows: List[Dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        if entity_type == "responses":
            # Response payloads do not always include `id`; use stable response keys.
            entity_id = item.get("response_id") or item.get("token") or item.get("landing_id")
        else:
            entity_id = item.get("id")
        if not entity_id:
            continue

        # Extract event timestamp
        if entity_type == "forms":
            event_ts = _parse_ts(item.get("created_at"))
        elif entity_type == "responses":
            event_ts = _parse_ts(item.get("submitted_at"))
        else:
            event_ts = _utc_now()

        # Extract updated timestamp
        updated_at_ts = _parse_ts(item.get("updated_at")) or _parse_ts(item.get("modified_at"))

        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": str(entity_id),
                "event_ts": event_ts.isoformat() if event_ts else None,
                "updated_at_ts": updated_at_ts.isoformat() if updated_at_ts else None,
                "ingested_at": _utc_now().isoformat(),
                "source": SOURCE_NAME,
                "payload_json": json.dumps(item),
                "backfill_run_id": run_id,
                "is_backfill": is_backfill,
            }
        )

    return rows, len(rows)


def upsert_raw_rows(rows: List[Dict[str, Any]]) -> int:
    """Upsert rows into raw table using MERGE."""
    if not rows:
        return 0

    stage_table = f"{PROJECT_ID}.{DATASET}.typeform_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      event_ts TIMESTAMP,
      updated_at_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    """
    client.query(create_stage).result()
    try:
        errors = client.insert_rows_json(stage_table, rows)
        if errors:
            raise RuntimeError(f"Typeform stage insert errors: {errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TYPEFORM_RAW_TABLE}` T
        USING `{stage_table}` S
        ON T.entity_type = S.entity_type AND T.entity_id = S.entity_id
        WHEN MATCHED THEN UPDATE SET
          event_ts = S.event_ts,
          updated_at_ts = S.updated_at_ts,
          ingested_at = S.ingested_at,
          source = S.source,
          payload_json = S.payload_json,
          backfill_run_id = S.backfill_run_id,
          is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            entity_type, entity_id, event_ts, updated_at_ts, ingested_at, source,
            payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.entity_type, S.entity_id, S.event_ts, S.updated_at_ts, S.ingested_at, S.source,
            S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
        return len(rows)
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def run_incremental_sync() -> Dict[str, Any]:
    """Run incremental sync: fetch new/updated forms and responses."""
    ensure_tables()
    _require_runtime_config()

    run_id = f"typeform-hourly-{_utc_now().strftime('%Y%m%d-%H%M%S')}"
    updated_after = _utc_now() - timedelta(hours=TYPEFORM_INCREMENTAL_LOOKBACK_HOURS)

    entities_summary: List[Dict[str, Any]] = []
    total_rows_upserted = 0

    # Fetch and sync forms
    try:
        forms = fetch_forms()
        rows, count = build_raw_rows("forms", forms, run_id, is_backfill=False)
        upserted = 0
        if rows:
            upserted = upsert_raw_rows(rows)
            total_rows_upserted += upserted
        entities_summary.append(
            {
                "entity_type": "forms",
                "rows_fetched": len(forms),
                "rows_upserted": upserted,
                "status": "success",
                "error": None,
            }
        )
    except Exception as e:
        entities_summary.append(
            {
                "entity_type": "forms",
                "rows_fetched": 0,
                "rows_upserted": 0,
                "status": "error",
                "error": str(e),
            }
        )

    # Fetch and sync responses for each form
    try:
        forms = fetch_forms()
        for form in forms:
            form_id = form.get("id")
            if not form_id:
                continue
            try:
                responses = fetch_form_responses(form_id, since=updated_after)
                rows, count = build_raw_rows("responses", responses, run_id, is_backfill=False)
                upserted = 0
                if rows:
                    upserted = upsert_raw_rows(rows)
                    total_rows_upserted += upserted
                entities_summary.append(
                    {
                        "entity_type": f"responses (form={form_id})",
                        "rows_fetched": len(responses),
                        "rows_upserted": upserted,
                        "status": "success",
                        "error": None,
                    }
                )
            except Exception as form_err:
                entities_summary.append(
                    {
                        "entity_type": f"responses (form={form_id})",
                        "rows_fetched": 0,
                        "rows_upserted": 0,
                        "status": "error",
                        "error": str(form_err),
                    }
                )
    except Exception as e:
        entities_summary.append(
            {
                "entity_type": "responses",
                "rows_fetched": 0,
                "rows_upserted": 0,
                "status": "error",
                "error": str(e),
            }
        )

    return {
        "entities_fetched": len(entities_summary),
        "records_upserted": total_rows_upserted,
        "summary": entities_summary,
    }


def run_models() -> int:
    """Execute Typeform STG/Core/Marts SQL models."""
    sql_file = "sql/typeform_models.sql"
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"SQL models file not found: {sql_file}")

    with open(sql_file, "r") as f:
        content = f.read()

    lines = []
    for line in content.split("\n"):
        line = line.rstrip()
        if line and not line.lstrip().startswith("--"):
            lines.append(line)

    statements = [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]
    executed = 0
    for stmt in statements:
        client.query(stmt).result()
        executed += 1
    return executed
