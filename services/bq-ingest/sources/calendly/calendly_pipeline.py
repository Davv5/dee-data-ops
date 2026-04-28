import json
import os
import time
import uuid
import hashlib
import hmac
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")

CALENDLY_API_BASE = os.getenv("CALENDLY_API_BASE", "https://api.calendly.com")
CALENDLY_API_KEY = os.getenv("CALENDLY_API_KEY")
CALENDLY_PAGE_LIMIT = int(os.getenv("CALENDLY_PAGE_LIMIT", "100"))
CALENDLY_INVITEE_MAX_PAGES_PER_EVENT = int(os.getenv("CALENDLY_INVITEE_MAX_PAGES_PER_EVENT", "0"))
CALENDLY_BACKFILL_INVITEE_WORKERS = int(os.getenv("CALENDLY_BACKFILL_INVITEE_WORKERS", "6"))
CALENDLY_BACKFILL_INVITEE_BATCH_SIZE = int(os.getenv("CALENDLY_BACKFILL_INVITEE_BATCH_SIZE", "200"))
CALENDLY_INCREMENTAL_INVITEE_WORKERS = int(os.getenv("CALENDLY_INCREMENTAL_INVITEE_WORKERS", "4"))

CALENDLY_ORGANIZATION_URI = os.getenv("CALENDLY_ORGANIZATION_URI")
CALENDLY_USER_URI = os.getenv("CALENDLY_USER_URI")

CALENDLY_RAW_TABLE = os.getenv("BQ_CALENDLY_RAW_TABLE", "calendly_objects_raw")
CALENDLY_STATE_TABLE = os.getenv("BQ_CALENDLY_STATE_TABLE", "calendly_backfill_state")
CALENDLY_INVITEE_STATE_TABLE = os.getenv(
    "BQ_CALENDLY_INVITEE_STATE_TABLE", "calendly_invitee_backfill_state"
)
CALENDLY_WEBHOOK_RAW_TABLE = os.getenv(
    "BQ_CALENDLY_WEBHOOK_RAW_TABLE", "calendly_webhook_events_raw"
)

CALENDLY_OBJECT_TYPES = os.getenv("CALENDLY_OBJECT_TYPES", "event_types,scheduled_events,event_invitees")
# Legacy phase gates kept for backward compatibility with existing jobs/env files.
CALENDLY_ENABLE_PHASE1 = os.getenv("CALENDLY_ENABLE_PHASE1", "true").strip().lower() == "true"
CALENDLY_ENABLE_PHASE2 = os.getenv("CALENDLY_ENABLE_PHASE2", "false").strip().lower() == "true"
CALENDLY_ENABLE_PHASE3 = os.getenv("CALENDLY_ENABLE_PHASE3", "false").strip().lower() == "true"
CALENDLY_INCREMENTAL_LOOKBACK_HOURS = int(os.getenv("CALENDLY_INCREMENTAL_LOOKBACK_HOURS", "48"))
CALENDLY_INCREMENTAL_MAX_PAGES_PER_OBJECT = int(
    os.getenv("CALENDLY_INCREMENTAL_MAX_PAGES_PER_OBJECT", "3")
)
CALENDLY_MAX_PAGES_EVENT_TYPES = int(os.getenv("CALENDLY_MAX_PAGES_EVENT_TYPES", "0"))
CALENDLY_MAX_PAGES_ROUTING_FORMS = int(os.getenv("CALENDLY_MAX_PAGES_ROUTING_FORMS", "0"))
CALENDLY_MAX_PAGES_ROUTING_FORM_SUBMISSIONS = int(
    os.getenv("CALENDLY_MAX_PAGES_ROUTING_FORM_SUBMISSIONS", "0")
)
CALENDLY_MAX_PAGES_SCHEDULED_EVENTS = int(os.getenv("CALENDLY_MAX_PAGES_SCHEDULED_EVENTS", "0"))
CALENDLY_MAX_PAGES_EVENT_INVITEES = int(os.getenv("CALENDLY_MAX_PAGES_EVENT_INVITEES", "0"))
CALENDLY_INVITEE_DRAIN_POLL_SECONDS = int(os.getenv("CALENDLY_INVITEE_DRAIN_POLL_SECONDS", "20"))
CALENDLY_WEBHOOK_SIGNING_KEY = os.getenv("CALENDLY_WEBHOOK_SIGNING_KEY")
CALENDLY_WEBHOOK_REQUIRE_SIGNATURE = (
    os.getenv("CALENDLY_WEBHOOK_REQUIRE_SIGNATURE", "false").strip().lower() == "true"
)

CORE_ENTITY_TYPES: Set[str] = {"event_types", "scheduled_events", "event_invitees"}
ROUTING_ENTITY_TYPES: Set[str] = {"routing_forms", "routing_form_submissions"}
SUPPORTED_ENTITY_TYPES: Set[str] = CORE_ENTITY_TYPES | ROUTING_ENTITY_TYPES

SOURCE_NAME = "calendly"
BACKFILL_MODES: Set[str] = {"combined", "events_only", "invitees_only"}
BACKFILL_FAILURE_ENTITY_TYPES: Dict[str, Tuple[str, ...]] = {
    "combined": (
        "event_types",
        "routing_forms",
        "routing_form_submissions",
        "scheduled_events",
        "event_invitees",
    ),
    "events_only": (
        "event_types",
        "routing_forms",
        "routing_form_submissions",
        "scheduled_events",
    ),
    "invitees_only": ("event_invitees",),
}

client = bigquery.Client(project=PROJECT_ID)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    txt = str(value).strip()
    return txt if txt else None


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.fromtimestamp(value, tz=timezone.utc)
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


def _require_runtime_config() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    if not CALENDLY_API_KEY:
        raise RuntimeError("Missing CALENDLY_API_KEY")


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {CALENDLY_API_KEY}",
        "Accept": "application/json",
    }


def ensure_tables() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")

    raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{CALENDLY_RAW_TABLE}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      parent_id STRING,
      organization_uri STRING,
      user_uri STRING,
      partition_date DATE NOT NULL,
      event_ts TIMESTAMP,
      updated_at_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY partition_date
    CLUSTER BY entity_type, entity_id, parent_id
    """
    client.query(raw_query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}` (
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
    PARTITION BY DATE(updated_at)
    CLUSTER BY run_id, entity_type
    """
    client.query(state_query).result()

    invitee_state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` (
      run_id STRING NOT NULL,
      event_id STRING NOT NULL,
      status STRING NOT NULL,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      last_status INT64,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    PARTITION BY DATE(updated_at)
    CLUSTER BY run_id, status, event_id
    """
    client.query(invitee_state_query).result()

    webhook_raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{CALENDLY_WEBHOOK_RAW_TABLE}` (
      webhook_event_id STRING NOT NULL,
      webhook_event_type STRING NOT NULL,
      scheduled_event_id STRING,
      invitee_id STRING,
      delivery_ts TIMESTAMP,
      event_created_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      headers_json JSON
    )
    PARTITION BY DATE(ingested_at)
    CLUSTER BY webhook_event_type, scheduled_event_id, invitee_id
    """
    client.query(webhook_raw_query).result()


def parse_object_types() -> List[str]:
    requested = [x.strip() for x in CALENDLY_OBJECT_TYPES.split(",") if x.strip()]

    enabled_entities: Set[str] = set()
    if CALENDLY_ENABLE_PHASE1:
        enabled_entities.update(CORE_ENTITY_TYPES)
    if CALENDLY_ENABLE_PHASE2:
        enabled_entities.update(ROUTING_ENTITY_TYPES)
    if CALENDLY_ENABLE_PHASE3:
        # Reserved for future Calendly entities; intentionally no-op today.
        pass

    selected: List[str] = []
    seen: Set[str] = set()
    for entity in requested:
        if entity not in SUPPORTED_ENTITY_TYPES:
            continue
        if entity not in enabled_entities:
            continue
        if entity in seen:
            continue
        selected.append(entity)
        seen.add(entity)
    return selected


def _entity_page_cap(entity_type: str, fallback_max_pages: int) -> int:
    caps = {
        "event_types": CALENDLY_MAX_PAGES_EVENT_TYPES,
        "routing_forms": CALENDLY_MAX_PAGES_ROUTING_FORMS,
        "routing_form_submissions": CALENDLY_MAX_PAGES_ROUTING_FORM_SUBMISSIONS,
        "scheduled_events": CALENDLY_MAX_PAGES_SCHEDULED_EVENTS,
        "event_invitees": CALENDLY_MAX_PAGES_EVENT_INVITEES,
    }
    entity_cap = int(caps.get(entity_type, 0) or 0)
    if entity_cap > 0:
        return entity_cap
    return max(0, int(fallback_max_pages))


def _extract_uuid_from_uri(uri: Optional[str]) -> Optional[str]:
    txt = _safe_str(uri)
    if not txt:
        return None
    return txt.rstrip("/").split("/")[-1]


def _extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    collection = payload.get("collection")
    if isinstance(collection, list):
        return [x for x in collection if isinstance(x, dict)]
    data = payload.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        nested_collection = data.get("collection")
        if isinstance(nested_collection, list):
            return [x for x in nested_collection if isinstance(x, dict)]
    return []


def _extract_next_page(payload: Dict[str, Any]) -> Optional[str]:
    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        next_page = pagination.get("next_page")
        if next_page and str(next_page).strip():
            return str(next_page).strip()
    return None


def _request_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    max_attempts: int = 6,
    initial_backoff_sec: float = 1.0,
) -> Tuple[Dict[str, Any], int]:
    _require_runtime_config()

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=_headers(), params=params, timeout=60)
        last_resp = resp
        if resp.ok:
            payload = resp.json()
            if isinstance(payload, list):
                payload = {"collection": payload}
            if not isinstance(payload, dict):
                payload = {}
            return payload, resp.status_code

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    raise RuntimeError(
        f"Calendly request failed (status={status}, url={url}, body_preview={body_preview})"
    )


def _resolve_scope() -> Tuple[Optional[str], Optional[str]]:
    org = _safe_str(CALENDLY_ORGANIZATION_URI)
    user = _safe_str(CALENDLY_USER_URI)
    if org or user:
        return org, user

    payload, _ = _request_json(f"{CALENDLY_API_BASE.rstrip('/')}/users/me")
    resource = payload.get("resource")
    if not isinstance(resource, dict):
        return None, None

    return _safe_str(resource.get("current_organization")), _safe_str(resource.get("uri"))


def _scheduled_event_id(item: Dict[str, Any]) -> Optional[str]:
    return (
        _extract_uuid_from_uri(_safe_str(item.get("uri")))
        or _safe_str(item.get("uuid"))
        or _safe_str(item.get("id"))
    )


def _invitee_id(item: Dict[str, Any], event_id: str) -> Optional[str]:
    candidate = (
        _extract_uuid_from_uri(_safe_str(item.get("uri")))
        or _safe_str(item.get("uuid"))
        or _safe_str(item.get("id"))
    )
    if candidate:
        return candidate

    email = _safe_str(item.get("email"))
    created = _safe_str(item.get("created_at"))
    if email:
        return f"{event_id}:{email.lower()}:{created or 'na'}"
    return None


def _event_type_id(item: Dict[str, Any]) -> Optional[str]:
    return (
        _extract_uuid_from_uri(_safe_str(item.get("uri")))
        or _safe_str(item.get("uuid"))
        or _safe_str(item.get("id"))
    )


def _routing_form_id(item: Dict[str, Any]) -> Optional[str]:
    return (
        _extract_uuid_from_uri(_safe_str(item.get("uri")))
        or _safe_str(item.get("uuid"))
        or _safe_str(item.get("id"))
    )


def _routing_form_submission_id(item: Dict[str, Any], form_id: str) -> Optional[str]:
    candidate = (
        _extract_uuid_from_uri(_safe_str(item.get("uri")))
        or _safe_str(item.get("uuid"))
        or _safe_str(item.get("id"))
    )
    if candidate:
        return candidate
    created = _safe_str(item.get("created_at")) or "na"
    return f"{form_id}:{created}"


def _event_ts(entity_type: str, item: Dict[str, Any]) -> Optional[datetime]:
    if entity_type == "scheduled_events":
        for key in ("start_time", "event_start_time", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type == "event_invitees":
        for key in ("created_at", "updated_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type == "event_types":
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type in {"routing_forms", "routing_form_submissions"}:
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    return None


def _updated_ts(entity_type: str, item: Dict[str, Any]) -> Optional[datetime]:
    if entity_type == "scheduled_events":
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type == "event_invitees":
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type == "event_types":
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    elif entity_type in {"routing_forms", "routing_form_submissions"}:
        for key in ("updated_at", "created_at"):
            dt = _parse_ts(item.get(key))
            if dt:
                return dt
    return None


def fetch_scheduled_events_page(
    next_page_url: Optional[str],
    min_start_time: Optional[datetime] = None,
    max_start_time: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
    org_uri, user_uri = _resolve_scope()

    if next_page_url:
        payload, status_code = _request_json(next_page_url)
    else:
        url = f"{CALENDLY_API_BASE.rstrip('/')}/scheduled_events"
        params: Dict[str, Any] = {"count": CALENDLY_PAGE_LIMIT}
        if org_uri:
            params["organization"] = org_uri
        elif user_uri:
            params["user"] = user_uri

        if min_start_time:
            params["min_start_time"] = min_start_time.isoformat().replace("+00:00", "Z")
        if max_start_time:
            params["max_start_time"] = max_start_time.isoformat().replace("+00:00", "Z")

        payload, status_code = _request_json(url, params=params)

    items = _extract_items(payload)
    next_cursor = _extract_next_page(payload)
    return items, next_cursor, status_code


def fetch_event_types_page(next_page_url: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
    org_uri, user_uri = _resolve_scope()

    if next_page_url:
        payload, status_code = _request_json(next_page_url)
    else:
        url = f"{CALENDLY_API_BASE.rstrip('/')}/event_types"
        params: Dict[str, Any] = {"count": CALENDLY_PAGE_LIMIT}
        if org_uri:
            params["organization"] = org_uri
        elif user_uri:
            params["user"] = user_uri
        payload, status_code = _request_json(url, params=params)

    items = _extract_items(payload)
    next_cursor = _extract_next_page(payload)
    return items, next_cursor, status_code


def fetch_routing_forms_page(next_page_url: Optional[str]) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
    org_uri, _ = _resolve_scope()
    if next_page_url:
        payload, status_code = _request_json(next_page_url)
    else:
        url = f"{CALENDLY_API_BASE.rstrip('/')}/routing_forms"
        params: Dict[str, Any] = {"count": CALENDLY_PAGE_LIMIT}
        if org_uri:
            params["organization"] = org_uri
        payload, status_code = _request_json(url, params=params)
    items = _extract_items(payload)
    next_cursor = _extract_next_page(payload)
    return items, next_cursor, status_code


def fetch_routing_form_submissions_page(
    form_id: str,
    next_page_url: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str], int]:
    if next_page_url:
        payload, status_code = _request_json(next_page_url)
    else:
        url = f"{CALENDLY_API_BASE.rstrip('/')}/routing_forms/{form_id}/submissions"
        payload, status_code = _request_json(url, params={"count": CALENDLY_PAGE_LIMIT})
    items = _extract_items(payload)
    next_cursor = _extract_next_page(payload)
    return items, next_cursor, status_code


def fetch_event_invitees(event_id: str) -> Tuple[List[Dict[str, Any]], int, int]:
    base_url = f"{CALENDLY_API_BASE.rstrip('/')}/scheduled_events/{event_id}/invitees"
    next_page_url: Optional[str] = None
    pages = 0
    all_items: List[Dict[str, Any]] = []
    last_status = 200

    while True:
        if next_page_url:
            payload, status_code = _request_json(next_page_url)
        else:
            payload, status_code = _request_json(base_url, params={"count": CALENDLY_PAGE_LIMIT})

        last_status = status_code
        pages += 1

        items = _extract_items(payload)
        all_items.extend(items)
        next_page_url = _extract_next_page(payload)

        if not next_page_url:
            break
        if CALENDLY_INVITEE_MAX_PAGES_PER_EVENT > 0 and pages >= CALENDLY_INVITEE_MAX_PAGES_PER_EVENT:
            break

    return all_items, pages, last_status


def build_rows(
    entity_type: str,
    items: List[Dict[str, Any]],
    run_id: str,
    is_backfill: bool,
    parent_id: Optional[str] = None,
    organization_uri: Optional[str] = None,
    user_uri: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ingested_at_dt = _utc_now()
    ingested_at = ingested_at_dt.isoformat()
    rows: List[Dict[str, Any]] = []

    for item in items:
        if entity_type == "event_types":
            entity_id = _event_type_id(item)
            entity_parent_id = None
        elif entity_type == "routing_forms":
            entity_id = _routing_form_id(item)
            entity_parent_id = None
        elif entity_type == "routing_form_submissions":
            if not parent_id:
                continue
            entity_id = _routing_form_submission_id(item=item, form_id=parent_id)
            entity_parent_id = parent_id
        elif entity_type == "scheduled_events":
            entity_id = _scheduled_event_id(item)
            entity_parent_id = None
        elif entity_type == "event_invitees":
            if not parent_id:
                continue
            entity_id = _invitee_id(item=item, event_id=parent_id)
            entity_parent_id = parent_id
        else:
            continue

        if not entity_id:
            continue

        event_ts = _event_ts(entity_type, item)
        updated_ts = _updated_ts(entity_type, item)
        partition_date = (updated_ts or event_ts or ingested_at_dt).date().isoformat()

        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "parent_id": entity_parent_id,
                "organization_uri": organization_uri,
                "user_uri": user_uri,
                "partition_date": partition_date,
                "event_ts": event_ts.isoformat() if event_ts else None,
                "updated_at_ts": updated_ts.isoformat() if updated_ts else None,
                "ingested_at": ingested_at,
                "source": SOURCE_NAME,
                "payload_json": json.dumps(item),
                "backfill_run_id": run_id,
                "is_backfill": is_backfill,
            }
        )

    return rows


def upsert_raw_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.calendly_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      parent_id STRING,
      organization_uri STRING,
      user_uri STRING,
      partition_date DATE NOT NULL,
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
            raise RuntimeError(f"Calendly stage insert errors: {errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{CALENDLY_RAW_TABLE}` T
        USING (
          SELECT * EXCEPT(rn)
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id
                ORDER BY updated_at_ts DESC, event_ts DESC, ingested_at DESC
              ) AS rn
            FROM `{stage_table}`
          )
          WHERE rn = 1
        ) S
        ON T.entity_type = S.entity_type
           AND T.entity_id = S.entity_id
        WHEN MATCHED THEN UPDATE SET
          parent_id = S.parent_id,
          organization_uri = S.organization_uri,
          user_uri = S.user_uri,
          partition_date = S.partition_date,
          event_ts = S.event_ts,
          updated_at_ts = S.updated_at_ts,
          ingested_at = S.ingested_at,
          source = S.source,
          payload_json = S.payload_json,
          backfill_run_id = S.backfill_run_id,
          is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            entity_type, entity_id, parent_id, organization_uri, user_uri,
            partition_date, event_ts, updated_at_ts, ingested_at, source,
            payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.entity_type, S.entity_id, S.parent_id, S.organization_uri, S.user_uri,
            S.partition_date, S.event_ts, S.updated_at_ts, S.ingested_at, S.source,
            S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def write_state(
    run_id: str,
    entity_type: str,
    status: str,
    next_cursor: Optional[str],
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @entity_type AS entity_type,
        @status AS status,
        @next_cursor AS next_cursor,
        @pages_processed AS pages_processed,
        @rows_written AS rows_written,
        @started_at AS started_at,
        CURRENT_TIMESTAMP() AS updated_at,
        @error_text AS error_text
    ) S
    ON T.run_id = S.run_id
       AND T.entity_type = S.entity_type
    WHEN MATCHED THEN UPDATE SET
      status = S.status,
      next_cursor = S.next_cursor,
      pages_processed = S.pages_processed,
      rows_written = S.rows_written,
      started_at = S.started_at,
      updated_at = S.updated_at,
      error_text = S.error_text
    WHEN NOT MATCHED THEN
      INSERT (
        run_id, entity_type, status, next_cursor,
        pages_processed, rows_written, started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.entity_type, S.status, S.next_cursor,
        S.pages_processed, S.rows_written, S.started_at, S.updated_at, S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_cursor", "STRING", next_cursor),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(query, job_config=job_config).result()


def read_state(run_id: str, entity_type: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id,
      entity_type,
      status,
      next_cursor,
      pages_processed,
      rows_written,
      started_at,
      updated_at,
      error_text
    FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}`
    WHERE run_id = @run_id
      AND entity_type = @entity_type
    LIMIT 1
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None

    row = rows[0]
    return {
        "run_id": row["run_id"],
        "entity_type": row["entity_type"],
        "status": row["status"],
        "next_cursor": row["next_cursor"],
        "pages_processed": row["pages_processed"],
        "rows_written": row["rows_written"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "error_text": row["error_text"],
    }


def _seed_invitee_work_items(run_id: str, event_ids: List[str], started_at: datetime) -> None:
    unique_ids = sorted({x.strip() for x in event_ids if x and x.strip()})
    if not unique_ids:
        return

    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` (
      run_id,
      event_id,
      status,
      pages_processed,
      rows_written,
      last_status,
      started_at,
      updated_at,
      error_text
    )
    SELECT
      @run_id AS run_id,
      src_event_id AS event_id,
      'PENDING' AS status,
      0 AS pages_processed,
      0 AS rows_written,
      NULL AS last_status,
      @started_at AS started_at,
      CURRENT_TIMESTAMP() AS updated_at,
      NULL AS error_text
    FROM UNNEST(@event_ids) AS src_event_id
    WHERE NOT EXISTS (
      SELECT 1
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` T
      WHERE T.run_id = @run_id
        AND T.event_id = src_event_id
    )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ArrayQueryParameter("event_ids", "STRING", unique_ids),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _seed_invitee_work_items_from_raw(run_id: str, started_at: datetime) -> None:
    query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` (
      run_id,
      event_id,
      status,
      pages_processed,
      rows_written,
      last_status,
      started_at,
      updated_at,
      error_text
    )
    SELECT
      @run_id AS run_id,
      R.entity_id AS event_id,
      'PENDING' AS status,
      0 AS pages_processed,
      0 AS rows_written,
      NULL AS last_status,
      @started_at AS started_at,
      CURRENT_TIMESTAMP() AS updated_at,
      NULL AS error_text
    FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_RAW_TABLE}` R
    WHERE R.backfill_run_id = @run_id
      AND R.entity_type = 'scheduled_events'
      AND NOT EXISTS (
        SELECT 1
        FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` T
        WHERE T.run_id = @run_id
          AND T.event_id = R.entity_id
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _reset_failed_invitee_work_items(run_id: str) -> None:
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
    SET
      status = 'PENDING',
      updated_at = CURRENT_TIMESTAMP(),
      error_text = NULL
    WHERE run_id = @run_id
      AND status = 'FAILED'
    """
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _list_pending_invitee_events(run_id: str, limit: int) -> List[str]:
    query = f"""
    SELECT event_id
    FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
    WHERE run_id = @run_id
      AND status IN ('PENDING', 'RUNNING')
    ORDER BY updated_at ASC, event_id ASC
    LIMIT @limit
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("limit", "INT64", int(limit)),
    ]
    rows = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return [str(row["event_id"]) for row in rows]


def _mark_invitee_events_running(run_id: str, event_ids: List[str]) -> None:
    unique_ids = sorted({x.strip() for x in event_ids if x and x.strip()})
    if not unique_ids:
        return
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
    SET
      status = 'RUNNING',
      updated_at = CURRENT_TIMESTAMP(),
      error_text = NULL
    WHERE run_id = @run_id
      AND event_id IN UNNEST(@event_ids)
      AND status != 'COMPLETED'
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ArrayQueryParameter("event_ids", "STRING", unique_ids),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def _upsert_invitee_event_states(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.calendly_invitee_state_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      run_id STRING NOT NULL,
      event_id STRING NOT NULL,
      status STRING NOT NULL,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      last_status INT64,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    """
    client.query(create_stage).result()
    try:
        errors = client.insert_rows_json(stage_table, rows)
        if errors:
            raise RuntimeError(f"Calendly invitee state stage insert errors: {errors}")
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}` T
        USING `{stage_table}` S
        ON T.run_id = S.run_id
           AND T.event_id = S.event_id
        WHEN MATCHED THEN UPDATE SET
          status = S.status,
          pages_processed = S.pages_processed,
          rows_written = S.rows_written,
          last_status = S.last_status,
          started_at = S.started_at,
          updated_at = S.updated_at,
          error_text = S.error_text
        WHEN NOT MATCHED THEN INSERT (
          run_id, event_id, status, pages_processed, rows_written,
          last_status, started_at, updated_at, error_text
        )
        VALUES (
          S.run_id, S.event_id, S.status, S.pages_processed, S.rows_written,
          S.last_status, S.started_at, S.updated_at, S.error_text
        )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def _summarize_invitee_work(run_id: str) -> Dict[str, int]:
    query = f"""
    SELECT
      COUNT(*) AS total_events,
      COUNTIF(status = 'COMPLETED') AS completed_events,
      COUNTIF(status = 'FAILED') AS failed_events,
      COUNTIF(status IN ('PENDING', 'RUNNING')) AS pending_events,
      COALESCE(SUM(pages_processed), 0) AS pages_processed,
      COALESCE(SUM(rows_written), 0) AS rows_written
    FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
    WHERE run_id = @run_id
    """
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    row = list(
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    )[0]
    return {
        "total_events": int(row["total_events"] or 0),
        "completed_events": int(row["completed_events"] or 0),
        "failed_events": int(row["failed_events"] or 0),
        "pending_events": int(row["pending_events"] or 0),
        "pages_processed": int(row["pages_processed"] or 0),
        "rows_written": int(row["rows_written"] or 0),
    }


def _fetch_invitee_rows_for_event(
    event_id: str,
    run_id: str,
    is_backfill: bool,
    organization_uri: Optional[str],
    user_uri: Optional[str],
) -> Dict[str, Any]:
    invitee_items, invitee_pages, invitee_status = fetch_event_invitees(event_id)
    invitee_rows = build_rows(
        entity_type="event_invitees",
        items=invitee_items,
        run_id=run_id,
        is_backfill=is_backfill,
        parent_id=event_id,
        organization_uri=organization_uri,
        user_uri=user_uri,
    )
    return {
        "event_id": event_id,
        "rows": invitee_rows,
        "pages_processed": invitee_pages,
        "rows_written": len(invitee_rows),
        "upstream_status": invitee_status,
    }


def _process_invitees_for_event_ids(
    run_id: str,
    event_ids: List[str],
    is_backfill: bool,
    organization_uri: Optional[str],
    user_uri: Optional[str],
    max_workers: int,
) -> Dict[str, Any]:
    unique_ids = sorted({x.strip() for x in event_ids if x and x.strip()})
    if not unique_ids:
        return {
            "status": "COMPLETED",
            "pages_processed": 0,
            "rows_upserted": 0,
            "upstream_status": None,
            "failed_events": 0,
            "total_events": 0,
        }

    worker_count = max(1, int(max_workers))
    last_status: Optional[int] = None
    pages_processed = 0
    rows_upserted = 0
    failed_events = 0
    invitee_rows_buffer: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                _fetch_invitee_rows_for_event,
                event_id,
                run_id,
                is_backfill,
                organization_uri,
                user_uri,
            ): event_id
            for event_id in unique_ids
        }
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception:
                failed_events += 1
                continue
            last_status = result.get("upstream_status")
            pages_processed += int(result.get("pages_processed", 0))
            rows = result.get("rows") or []
            if rows:
                invitee_rows_buffer.extend(rows)
                rows_upserted += len(rows)

    if invitee_rows_buffer:
        upsert_raw_rows(invitee_rows_buffer)

    status = "COMPLETED" if failed_events == 0 else "PARTIAL_FAILED"
    return {
        "status": status,
        "pages_processed": pages_processed,
        "rows_upserted": rows_upserted,
        "upstream_status": last_status,
        "failed_events": failed_events,
        "total_events": len(unique_ids),
    }


def _process_invitees_for_backfill_run(
    run_id: str,
    is_backfill: bool,
    organization_uri: Optional[str],
    user_uri: Optional[str],
    started_at: datetime,
    next_cursor: Optional[str],
    base_pages_processed: int,
    base_rows_written: int,
    max_pages: int = 0,
) -> Dict[str, Any]:
    _seed_invitee_work_items_from_raw(run_id=run_id, started_at=started_at)
    _reset_failed_invitee_work_items(run_id)

    worker_count = max(1, int(CALENDLY_BACKFILL_INVITEE_WORKERS))
    batch_size = max(worker_count, int(CALENDLY_BACKFILL_INVITEE_BATCH_SIZE))
    last_status: Optional[int] = None

    cap_reached = False
    while True:
        current_summary = _summarize_invitee_work(run_id)
        current_total_pages = base_pages_processed + int(current_summary.get("pages_processed", 0))
        if max_pages > 0 and current_total_pages >= max_pages:
            cap_reached = True
            break

        pending_ids = _list_pending_invitee_events(run_id=run_id, limit=batch_size)
        if not pending_ids:
            break

        _mark_invitee_events_running(run_id=run_id, event_ids=pending_ids)
        state_rows: List[Dict[str, Any]] = []
        invitee_rows_buffer: List[Dict[str, Any]] = []
        now_iso = _utc_now().isoformat()
        started_at_iso = started_at.isoformat()

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {
                pool.submit(
                    _fetch_invitee_rows_for_event,
                    event_id,
                    run_id,
                    is_backfill,
                    organization_uri,
                    user_uri,
                ): event_id
                for event_id in pending_ids
            }
            for future in as_completed(futures):
                event_id = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    state_rows.append(
                        {
                            "run_id": run_id,
                            "event_id": event_id,
                            "status": "FAILED",
                            "pages_processed": 0,
                            "rows_written": 0,
                            "last_status": None,
                            "started_at": started_at_iso,
                            "updated_at": now_iso,
                            "error_text": str(exc)[:2000],
                        }
                    )
                    continue

                last_status = result.get("upstream_status")
                rows = result.get("rows") or []
                if rows:
                    invitee_rows_buffer.extend(rows)
                state_rows.append(
                    {
                        "run_id": run_id,
                        "event_id": event_id,
                        "status": "COMPLETED",
                        "pages_processed": int(result.get("pages_processed", 0)),
                        "rows_written": int(result.get("rows_written", 0)),
                        "last_status": result.get("upstream_status"),
                        "started_at": started_at_iso,
                        "updated_at": now_iso,
                        "error_text": None,
                    }
                )

        if invitee_rows_buffer:
            upsert_raw_rows(invitee_rows_buffer)
        if state_rows:
            _upsert_invitee_event_states(state_rows)

        invitee_summary = _summarize_invitee_work(run_id)
        write_state(
            run_id=run_id,
            entity_type="event_invitees",
            status="RUNNING",
            next_cursor=next_cursor,
            pages_processed=base_pages_processed + invitee_summary["pages_processed"],
            rows_written=base_rows_written + invitee_summary["rows_written"],
            started_at=started_at,
            error_text=None,
        )

    final_summary = _summarize_invitee_work(run_id)
    failed_events = final_summary["failed_events"]
    pending_events = final_summary["pending_events"]
    if cap_reached:
        status = "PAUSED_LIMIT_REACHED"
    elif pending_events > 0:
        status = "RUNNING"
    elif failed_events > 0:
        status = "PARTIAL_FAILED"
    else:
        status = "COMPLETED"

    return {
        "status": status,
        "pages_processed": base_pages_processed + final_summary["pages_processed"],
        "rows_upserted": base_rows_written + final_summary["rows_written"],
        "upstream_status": last_status,
        "failed_events": failed_events,
        "total_events": final_summary["total_events"],
    }


def _run_event_types_sync(
    run_id: str,
    is_backfill: bool,
    max_pages: int,
    write_checkpoint: bool,
) -> Dict[str, Any]:
    object_types = set(parse_object_types())
    if "event_types" not in object_types:
        return {
            "status": "SKIPPED",
            "pages_processed": 0,
            "rows_upserted": 0,
            "upstream_status": None,
        }

    entity_max_pages = _entity_page_cap("event_types", max_pages)
    next_cursor: Optional[str] = None
    pages_processed = 0
    rows_written = 0
    started_at = _utc_now()
    status = "RUNNING"
    last_status: Optional[int] = None

    if write_checkpoint:
        existing = read_state(run_id=run_id, entity_type="event_types")
        if existing:
            next_cursor = existing.get("next_cursor")
            pages_processed = int(existing.get("pages_processed", 0))
            rows_written = int(existing.get("rows_written", 0))
            started_at = existing.get("started_at") or started_at
            status = str(existing.get("status") or "RUNNING")
            if status == "COMPLETED":
                return {
                    "status": status,
                    "pages_processed": pages_processed,
                    "rows_upserted": rows_written,
                    "upstream_status": None,
                }
        write_state(
            run_id=run_id,
            entity_type="event_types",
            status="RUNNING",
            next_cursor=next_cursor,
            pages_processed=pages_processed,
            rows_written=rows_written,
            started_at=started_at,
            error_text=None,
        )

    while True:
        if entity_max_pages > 0 and pages_processed >= entity_max_pages:
            status = "PAUSED_LIMIT_REACHED"
            if write_checkpoint:
                write_state(
                    run_id=run_id,
                    entity_type="event_types",
                    status=status,
                    next_cursor=next_cursor,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )
            break

        items, new_cursor, upstream_status = fetch_event_types_page(next_page_url=next_cursor)
        last_status = upstream_status

        rows = build_rows(
            entity_type="event_types",
            items=items,
            run_id=run_id,
            is_backfill=is_backfill,
            parent_id=None,
        )
        if rows:
            upsert_raw_rows(rows)
            rows_written += len(rows)

        pages_processed += 1
        next_cursor = new_cursor
        has_more = bool(next_cursor)
        status = "RUNNING" if has_more else "COMPLETED"

        if write_checkpoint:
            write_state(
                run_id=run_id,
                entity_type="event_types",
                status=status,
                next_cursor=next_cursor,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=None,
            )

        if not has_more:
            break

    return {
        "status": status,
        "pages_processed": pages_processed,
        "rows_upserted": rows_written,
        "upstream_status": last_status,
    }


def _list_routing_form_ids_from_raw(run_id: str) -> List[str]:
    query = f"""
    SELECT DISTINCT entity_id AS form_id
    FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_RAW_TABLE}`
    WHERE entity_type = 'routing_forms'
      AND (
        backfill_run_id = @run_id
        OR @run_id LIKE 'calendly-hourly-%'
      )
    """
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    rows = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return [str(row["form_id"]) for row in rows if row["form_id"]]


def _run_routing_forms_sync(
    run_id: str,
    is_backfill: bool,
    max_pages: int,
    write_checkpoint: bool,
) -> Dict[str, Dict[str, Any]]:
    object_types = set(parse_object_types())
    include_forms = "routing_forms" in object_types
    include_submissions = "routing_form_submissions" in object_types

    forms_result = {
        "status": "SKIPPED",
        "pages_processed": 0,
        "rows_upserted": 0,
        "upstream_status": None,
    }
    submissions_result = {
        "status": "SKIPPED",
        "pages_processed": 0,
        "rows_upserted": 0,
        "upstream_status": None,
    }

    forms_max_pages = _entity_page_cap("routing_forms", max_pages)
    submissions_max_pages = _entity_page_cap("routing_form_submissions", max_pages)

    if include_forms:
        next_cursor: Optional[str] = None
        pages_processed = 0
        rows_written = 0
        started_at = _utc_now()
        status = "RUNNING"
        last_status: Optional[int] = None

        if write_checkpoint:
            existing = read_state(run_id=run_id, entity_type="routing_forms")
            if existing:
                next_cursor = existing.get("next_cursor")
                pages_processed = int(existing.get("pages_processed", 0))
                rows_written = int(existing.get("rows_written", 0))
                started_at = existing.get("started_at") or started_at
                status = str(existing.get("status") or "RUNNING")
            write_state(
                run_id=run_id,
                entity_type="routing_forms",
                status="RUNNING",
                next_cursor=next_cursor,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=None,
            )

        while True:
            if forms_max_pages > 0 and pages_processed >= forms_max_pages:
                status = "PAUSED_LIMIT_REACHED"
                if write_checkpoint:
                    write_state(
                        run_id=run_id,
                        entity_type="routing_forms",
                        status=status,
                        next_cursor=next_cursor,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                break
            items, new_cursor, upstream_status = fetch_routing_forms_page(next_page_url=next_cursor)
            last_status = upstream_status
            rows = build_rows(
                entity_type="routing_forms",
                items=items,
                run_id=run_id,
                is_backfill=is_backfill,
                parent_id=None,
            )
            if rows:
                upsert_raw_rows(rows)
                rows_written += len(rows)
            pages_processed += 1
            next_cursor = new_cursor
            has_more = bool(next_cursor)
            status = "RUNNING" if has_more else "COMPLETED"
            if write_checkpoint:
                write_state(
                    run_id=run_id,
                    entity_type="routing_forms",
                    status=status,
                    next_cursor=next_cursor,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )
            if not has_more:
                break

        forms_result = {
            "status": status,
            "pages_processed": pages_processed,
            "rows_upserted": rows_written,
            "upstream_status": last_status,
        }

    if include_submissions:
        form_ids = _list_routing_form_ids_from_raw(run_id=run_id)
        pages_processed = 0
        rows_written = 0
        started_at = _utc_now()
        status = "RUNNING"
        last_status: Optional[int] = None

        if write_checkpoint:
            existing = read_state(run_id=run_id, entity_type="routing_form_submissions")
            if existing:
                pages_processed = int(existing.get("pages_processed", 0))
                rows_written = int(existing.get("rows_written", 0))
                started_at = existing.get("started_at") or started_at
                status = str(existing.get("status") or "RUNNING")
            write_state(
                run_id=run_id,
                entity_type="routing_form_submissions",
                status="RUNNING",
                next_cursor=None,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=None,
            )

        for form_id in form_ids:
            next_cursor: Optional[str] = None
            while True:
                if submissions_max_pages > 0 and pages_processed >= submissions_max_pages:
                    status = "PAUSED_LIMIT_REACHED"
                    break
                items, new_cursor, upstream_status = fetch_routing_form_submissions_page(
                    form_id=form_id,
                    next_page_url=next_cursor,
                )
                last_status = upstream_status
                rows = build_rows(
                    entity_type="routing_form_submissions",
                    items=items,
                    run_id=run_id,
                    is_backfill=is_backfill,
                    parent_id=form_id,
                )
                if rows:
                    upsert_raw_rows(rows)
                    rows_written += len(rows)
                pages_processed += 1
                next_cursor = new_cursor
                has_more = bool(next_cursor)
                if write_checkpoint:
                    write_state(
                        run_id=run_id,
                        entity_type="routing_form_submissions",
                        status="RUNNING",
                        next_cursor=None,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                if not has_more:
                    break
            if status == "PAUSED_LIMIT_REACHED":
                break

        if status != "PAUSED_LIMIT_REACHED":
            status = "COMPLETED"
        if write_checkpoint:
            write_state(
                run_id=run_id,
                entity_type="routing_form_submissions",
                status=status,
                next_cursor=None,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=None,
            )
        submissions_result = {
            "status": status,
            "pages_processed": pages_processed,
            "rows_upserted": rows_written,
            "upstream_status": last_status,
        }

    return {
        "routing_forms": forms_result,
        "routing_form_submissions": submissions_result,
    }


def _run_events_sync(
    run_id: str,
    is_backfill: bool,
    max_pages: int,
    created_after: Optional[datetime],
    write_checkpoint: bool,
    process_invitees: bool = True,
) -> Dict[str, Any]:
    org_uri, user_uri = _resolve_scope()
    object_types = set(parse_object_types())
    include_invitees = "event_invitees" in object_types
    manage_invitee_state = include_invitees and process_invitees

    scheduled_events_max_pages = _entity_page_cap("scheduled_events", max_pages)
    event_invitees_max_pages = _entity_page_cap("event_invitees", max_pages)

    next_cursor: Optional[str] = None
    pages_processed = 0
    events_rows_written = 0
    invitee_rows_written = 0
    invitee_pages_processed = 0
    started_at = _utc_now()
    events_status = "RUNNING"
    invitees_status = "SKIPPED"
    event_ids_for_incremental: Set[str] = set()

    existing_events: Optional[Dict[str, Any]] = None
    existing_invitees: Optional[Dict[str, Any]] = None
    events_already_completed = False
    invitees_already_completed = not include_invitees

    if write_checkpoint:
        existing_events = read_state(run_id=run_id, entity_type="scheduled_events")
        existing_invitees = (
            read_state(run_id=run_id, entity_type="event_invitees")
            if manage_invitee_state
            else None
        )

        if existing_events:
            next_cursor = existing_events.get("next_cursor")
            pages_processed = int(existing_events.get("pages_processed", 0))
            events_rows_written = int(existing_events.get("rows_written", 0))
            started_at = existing_events.get("started_at") or started_at
            events_status = str(existing_events.get("status") or "RUNNING")

        if existing_invitees:
            invitee_pages_processed = int(existing_invitees.get("pages_processed", 0))
            invitee_rows_written = int(existing_invitees.get("rows_written", 0))
            started_at = existing_invitees.get("started_at") or started_at
            invitees_status = str(existing_invitees.get("status") or "RUNNING")

        events_already_completed = bool(existing_events and existing_events.get("status") == "COMPLETED")
        invitees_already_completed = bool(
            (not manage_invitee_state)
            or (existing_invitees and existing_invitees.get("status") == "COMPLETED")
        )
        if events_already_completed and invitees_already_completed:
            return {
                "scheduled_events": {
                    "status": str(existing_events.get("status")),
                    "pages_processed": int(existing_events.get("pages_processed", 0)),
                    "rows_upserted": int(existing_events.get("rows_written", 0)),
                    "upstream_status": None,
                },
                "event_invitees": {
                    "status": str((existing_invitees or {}).get("status", "SKIPPED")),
                    "pages_processed": int((existing_invitees or {}).get("pages_processed", 0)),
                    "rows_upserted": int((existing_invitees or {}).get("rows_written", 0)),
                    "upstream_status": None,
                },
            }

        if not events_already_completed:
            write_state(
                run_id=run_id,
                entity_type="scheduled_events",
                status="RUNNING",
                next_cursor=next_cursor,
                pages_processed=pages_processed,
                rows_written=events_rows_written,
                started_at=started_at,
                error_text=None,
            )
        if manage_invitee_state and not invitees_already_completed:
            write_state(
                run_id=run_id,
                entity_type="event_invitees",
                status="RUNNING",
                next_cursor=next_cursor,
                pages_processed=invitee_pages_processed,
                rows_written=invitee_rows_written,
                started_at=started_at,
                error_text=None,
            )

    last_events_status: Optional[int] = None
    last_invitee_status: Optional[int] = None

    if not events_already_completed:
        while True:
            if scheduled_events_max_pages > 0 and pages_processed >= scheduled_events_max_pages:
                events_status = "PAUSED_LIMIT_REACHED"
                if write_checkpoint:
                    write_state(
                        run_id=run_id,
                        entity_type="scheduled_events",
                        status=events_status,
                        next_cursor=next_cursor,
                        pages_processed=pages_processed,
                        rows_written=events_rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                break

            try:
                items, new_cursor, upstream_status = fetch_scheduled_events_page(
                    next_page_url=next_cursor,
                    min_start_time=created_after,
                )
            except RuntimeError as exc:
                error_text = str(exc)
                invalid_page_token = (
                    bool(next_cursor)
                    and "page_token" in error_text
                    and "invalid" in error_text.lower()
                )
                if invalid_page_token:
                    next_cursor = None
                    if write_checkpoint:
                        write_state(
                            run_id=run_id,
                            entity_type="scheduled_events",
                            status="RUNNING",
                            next_cursor=next_cursor,
                            pages_processed=pages_processed,
                            rows_written=events_rows_written,
                            started_at=started_at,
                            error_text="Recovered invalid page_token; reset pagination cursor.",
                        )
                    continue
                raise
            last_events_status = upstream_status

            event_rows = build_rows(
                entity_type="scheduled_events",
                items=items,
                run_id=run_id,
                is_backfill=is_backfill,
                parent_id=None,
                organization_uri=org_uri,
                user_uri=user_uri,
            )
            if event_rows:
                upsert_raw_rows(event_rows)
                events_rows_written += len(event_rows)

            event_ids = [eid for eid in (_scheduled_event_id(x) for x in items) if eid]
            if include_invitees:
                if write_checkpoint:
                    _seed_invitee_work_items(run_id=run_id, event_ids=event_ids, started_at=started_at)
                else:
                    event_ids_for_incremental.update(event_ids)

            pages_processed += 1
            next_cursor = new_cursor
            has_more = bool(next_cursor)
            events_status = "RUNNING" if has_more else "COMPLETED"

            if write_checkpoint:
                write_state(
                    run_id=run_id,
                    entity_type="scheduled_events",
                    status=events_status,
                    next_cursor=next_cursor,
                    pages_processed=pages_processed,
                    rows_written=events_rows_written,
                    started_at=started_at,
                    error_text=None,
                )

            if not has_more:
                break
    else:
        events_status = "COMPLETED"

    if include_invitees:
        if not process_invitees:
            # In split mode (`events_only`), this producer is only responsible for
            # event extraction + invitee queue seeding. The invitee consumer owns
            # `event_invitees` checkpoint writes to avoid stale overwrite races.
            invitees_status = "DEFERRED"
        elif write_checkpoint:
            if events_status == "PAUSED_LIMIT_REACHED":
                invitees_status = "PAUSED_LIMIT_REACHED"
                write_state(
                    run_id=run_id,
                    entity_type="event_invitees",
                    status=invitees_status,
                    next_cursor=next_cursor,
                    pages_processed=invitee_pages_processed,
                    rows_written=invitee_rows_written,
                    started_at=started_at,
                    error_text=None,
                )
            else:
                invitee_result = _process_invitees_for_backfill_run(
                    run_id=run_id,
                    is_backfill=is_backfill,
                    organization_uri=org_uri,
                    user_uri=user_uri,
                    started_at=started_at,
                    next_cursor=next_cursor,
                    base_pages_processed=invitee_pages_processed,
                    base_rows_written=invitee_rows_written,
                    max_pages=event_invitees_max_pages,
                )
                invitees_status = str(invitee_result.get("status", "RUNNING"))
                invitee_pages_processed = max(
                    invitee_pages_processed, int(invitee_result.get("pages_processed", 0))
                )
                invitee_rows_written = max(
                    invitee_rows_written, int(invitee_result.get("rows_upserted", 0))
                )
                last_invitee_status = invitee_result.get("upstream_status")
                write_state(
                    run_id=run_id,
                    entity_type="event_invitees",
                    status=invitees_status,
                    next_cursor=next_cursor,
                    pages_processed=invitee_pages_processed,
                    rows_written=invitee_rows_written,
                    started_at=started_at,
                    error_text=None,
                )
        else:
            invitee_result = _process_invitees_for_event_ids(
                run_id=run_id,
                event_ids=sorted(event_ids_for_incremental),
                is_backfill=is_backfill,
                organization_uri=org_uri,
                user_uri=user_uri,
                max_workers=CALENDLY_INCREMENTAL_INVITEE_WORKERS,
            )
            invitees_status = str(invitee_result.get("status", "COMPLETED"))
            invitee_pages_processed = int(invitee_result.get("pages_processed", 0))
            invitee_rows_written = int(invitee_result.get("rows_upserted", 0))
            last_invitee_status = invitee_result.get("upstream_status")
    else:
        invitees_status = "SKIPPED"

    return {
        "scheduled_events": {
            "status": events_status,
            "pages_processed": pages_processed,
            "rows_upserted": events_rows_written,
            "upstream_status": last_events_status,
        },
        "event_invitees": {
            "status": invitees_status,
            "pages_processed": invitee_pages_processed,
            "rows_upserted": invitee_rows_written,
            "upstream_status": last_invitee_status,
        },
    }


def _validate_calendly_webhook_signature(
    body_bytes: bytes,
    headers: Dict[str, Any],
) -> bool:
    signing_key = _safe_str(CALENDLY_WEBHOOK_SIGNING_KEY)
    if not signing_key:
        return not CALENDLY_WEBHOOK_REQUIRE_SIGNATURE

    header_candidates = [
        "Calendly-Webhook-Signature",
        "X-Calendly-Webhook-Signature",
        "calendly-webhook-signature",
        "x-calendly-webhook-signature",
    ]
    signature_header: Optional[str] = None
    for name in header_candidates:
        value = _safe_str(headers.get(name))
        if value:
            signature_header = value
            break
    if not signature_header:
        return False

    sig_txt = signature_header.strip()
    if sig_txt.lower().startswith("sha256="):
        sig_txt = sig_txt.split("=", 1)[1].strip()

    expected_simple = hmac.new(
        signing_key.encode("utf-8"),
        body_bytes,
        hashlib.sha256,
    ).hexdigest()
    if hmac.compare_digest(sig_txt, expected_simple):
        return True

    parts: Dict[str, str] = {}
    for token in signature_header.split(","):
        token = token.strip()
        if "=" not in token:
            continue
        k, v = token.split("=", 1)
        parts[k.strip()] = v.strip()
    t_value = parts.get("t")
    v1_value = parts.get("v1")
    if t_value and v1_value:
        signed_payload = f"{t_value}.{body_bytes.decode('utf-8')}".encode("utf-8")
        expected_v1 = hmac.new(
            signing_key.encode("utf-8"),
            signed_payload,
            hashlib.sha256,
        ).hexdigest()
        if hmac.compare_digest(v1_value, expected_v1):
            return True

    return False


def _extract_webhook_resource_ids(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    body_payload = payload.get("payload")
    if not isinstance(body_payload, dict):
        body_payload = {}

    scheduled_event_id = _extract_uuid_from_uri(_safe_str(body_payload.get("event")))
    invitee_id = _extract_uuid_from_uri(_safe_str(body_payload.get("uri")))

    if not scheduled_event_id:
        event_obj = body_payload.get("event")
        if isinstance(event_obj, dict):
            scheduled_event_id = _extract_uuid_from_uri(_safe_str(event_obj.get("uri")))
    if not invitee_id:
        invitee_obj = body_payload.get("invitee")
        if isinstance(invitee_obj, dict):
            invitee_id = _extract_uuid_from_uri(_safe_str(invitee_obj.get("uri")))

    return scheduled_event_id, invitee_id


def _upsert_webhook_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.calendly_webhook_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      webhook_event_id STRING NOT NULL,
      webhook_event_type STRING NOT NULL,
      scheduled_event_id STRING,
      invitee_id STRING,
      delivery_ts TIMESTAMP,
      event_created_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      headers_json JSON
    )
    """
    client.query(create_stage).result()
    try:
        errors = client.insert_rows_json(stage_table, rows)
        if errors:
            raise RuntimeError(f"Calendly webhook stage insert errors: {errors}")
        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{CALENDLY_WEBHOOK_RAW_TABLE}` T
        USING `{stage_table}` S
        ON T.webhook_event_id = S.webhook_event_id
        WHEN MATCHED THEN UPDATE SET
          webhook_event_type = S.webhook_event_type,
          scheduled_event_id = S.scheduled_event_id,
          invitee_id = S.invitee_id,
          delivery_ts = S.delivery_ts,
          event_created_ts = S.event_created_ts,
          ingested_at = S.ingested_at,
          source = S.source,
          payload_json = S.payload_json,
          headers_json = S.headers_json
        WHEN NOT MATCHED THEN INSERT (
          webhook_event_id, webhook_event_type, scheduled_event_id, invitee_id,
          delivery_ts, event_created_ts, ingested_at, source, payload_json, headers_json
        )
        VALUES (
          S.webhook_event_id, S.webhook_event_type, S.scheduled_event_id, S.invitee_id,
          S.delivery_ts, S.event_created_ts, S.ingested_at, S.source, S.payload_json, S.headers_json
        )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def ingest_webhook_event(payload: Dict[str, Any], headers: Dict[str, Any], body_bytes: bytes) -> Dict[str, Any]:
    ensure_tables()
    _require_runtime_config()

    if not isinstance(payload, dict):
        raise ValueError("Webhook payload must be a JSON object")

    webhook_event_type = _safe_str(payload.get("event"))
    if webhook_event_type not in {"invitee.created", "invitee.canceled"}:
        return {"ignored": True, "reason": "unsupported_event", "event": webhook_event_type}

    signature_ok = _validate_calendly_webhook_signature(body_bytes=body_bytes, headers=headers)
    if not signature_ok:
        raise PermissionError("Invalid Calendly webhook signature")

    canonical_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    webhook_event_id = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    scheduled_event_id, invitee_id = _extract_webhook_resource_ids(payload=payload)
    event_created_ts = _parse_ts(payload.get("created_at"))
    delivery_ts = _utc_now()

    normalized_headers = {
        str(k): str(v)
        for k, v in headers.items()
        if k and v and str(k).lower().startswith("calendly")
    }
    row = {
        "webhook_event_id": webhook_event_id,
        "webhook_event_type": webhook_event_type,
        "scheduled_event_id": scheduled_event_id,
        "invitee_id": invitee_id,
        "delivery_ts": delivery_ts.isoformat(),
        "event_created_ts": event_created_ts.isoformat() if event_created_ts else None,
        "ingested_at": delivery_ts.isoformat(),
        "source": SOURCE_NAME,
        "payload_json": canonical_payload,
        "headers_json": json.dumps(normalized_headers),
    }
    _upsert_webhook_rows([row])
    return {
        "ignored": False,
        "webhook_event_id": webhook_event_id,
        "event": webhook_event_type,
        "scheduled_event_id": scheduled_event_id,
        "invitee_id": invitee_id,
    }


def run_models(sql_file_path: Optional[str] = None) -> int:
    if sql_file_path is None:
        sql_file_path = str(Path(__file__).resolve().parent / "sql" / "calendly_models.sql")

    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    lines: List[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)

    statements = [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]
    executed = 0
    for stmt in statements:
        client.query(stmt).result()
        executed += 1
    return executed


def run_incremental_sync() -> Dict[str, Any]:
    ensure_tables()
    _require_runtime_config()

    run_id = f"calendly-hourly-{_utc_now().strftime('%Y%m%d-%H%M%S')}"
    created_after = _utc_now() - timedelta(hours=CALENDLY_INCREMENTAL_LOOKBACK_HOURS)

    event_types_result = _run_event_types_sync(
        run_id=run_id,
        is_backfill=False,
        max_pages=CALENDLY_INCREMENTAL_MAX_PAGES_PER_OBJECT,
        write_checkpoint=False,
    )
    routing_results = _run_routing_forms_sync(
        run_id=run_id,
        is_backfill=False,
        max_pages=CALENDLY_INCREMENTAL_MAX_PAGES_PER_OBJECT,
        write_checkpoint=False,
    )

    results = _run_events_sync(
        run_id=run_id,
        is_backfill=False,
        max_pages=CALENDLY_INCREMENTAL_MAX_PAGES_PER_OBJECT,
        created_after=created_after,
        write_checkpoint=False,
        process_invitees=True,
    )

    run_models_after = os.getenv("CALENDLY_RUN_MODELS_AFTER_INCREMENTAL", "false").lower() == "true"
    statements_executed = 0
    if run_models_after:
        statements_executed = run_models()

    entity_results = []
    event_types_row = dict(event_types_result)
    event_types_row["entity_type"] = "event_types"
    entity_results.append(event_types_row)
    for key in ("routing_forms", "routing_form_submissions"):
        row = dict(routing_results[key])
        row["entity_type"] = key
        entity_results.append(row)
    for key in ("scheduled_events", "event_invitees"):
        row = dict(results[key])
        row["entity_type"] = key
        entity_results.append(row)

    return {
        "run_id": run_id,
        "lookback_hours": CALENDLY_INCREMENTAL_LOOKBACK_HOURS,
        "entity_results": entity_results,
        "models_refreshed": run_models_after,
        "statements_executed": statements_executed,
    }


def _mark_backfill_entities_failed(
    run_id: str,
    entity_types: Tuple[str, ...],
    error_text: str,
) -> None:
    for entity_type in entity_types:
        existing = read_state(run_id=run_id, entity_type=entity_type)
        if not existing:
            continue
        write_state(
            run_id=run_id,
            entity_type=entity_type,
            status="FAILED",
            next_cursor=existing.get("next_cursor"),
            pages_processed=int(existing.get("pages_processed", 0)),
            rows_written=int(existing.get("rows_written", 0)),
            started_at=existing.get("started_at") or _utc_now(),
            error_text=error_text,
        )


def run_backfill(
    run_id: str,
    max_pages_per_object: int,
    run_models_after: bool,
    mode: str = "combined",
) -> Dict[str, Any]:
    ensure_tables()
    _require_runtime_config()
    mode_value = (mode or "combined").strip().lower()
    if mode_value not in BACKFILL_MODES:
        raise ValueError("mode must be one of: combined, events_only, invitees_only")

    if mode_value == "invitees_only":
        return run_invitee_drain(
            run_id=run_id,
            run_models_after=run_models_after,
        )

    try:
        event_types_result = _run_event_types_sync(
            run_id=run_id,
            is_backfill=True,
            max_pages=max_pages_per_object,
            write_checkpoint=True,
        )
        routing_results = _run_routing_forms_sync(
            run_id=run_id,
            is_backfill=True,
            max_pages=max_pages_per_object,
            write_checkpoint=True,
        )
        results = _run_events_sync(
            run_id=run_id,
            is_backfill=True,
            max_pages=max_pages_per_object,
            created_after=None,
            write_checkpoint=True,
            process_invitees=(mode_value == "combined"),
        )
    except Exception as exc:
        _mark_backfill_entities_failed(
            run_id=run_id,
            entity_types=BACKFILL_FAILURE_ENTITY_TYPES[mode_value],
            error_text=str(exc)[:2000],
        )
        raise

    statements_executed = 0
    if run_models_after:
        statements_executed = run_models()

    merged_results = {
        "event_types": event_types_result,
        **routing_results,
        **results,
    }
    return {
        "run_id": run_id,
        "mode": mode_value,
        "results": merged_results,
        "models_refreshed": run_models_after,
        "statements_executed": statements_executed,
    }


def run_invitee_drain(
    run_id: str,
    run_models_after: bool,
) -> Dict[str, Any]:
    ensure_tables()
    _require_runtime_config()

    scheduled_state = read_state(run_id=run_id, entity_type="scheduled_events")
    invitee_state = read_state(run_id=run_id, entity_type="event_invitees")
    started_at = (invitee_state or {}).get("started_at") or (scheduled_state or {}).get("started_at") or _utc_now()
    next_cursor = (scheduled_state or {}).get("next_cursor")
    base_pages = int((invitee_state or {}).get("pages_processed", 0))
    base_rows = int((invitee_state or {}).get("rows_written", 0))
    scheduled_status = str((scheduled_state or {}).get("status", "RUNNING"))

    write_state(
        run_id=run_id,
        entity_type="event_invitees",
        status="RUNNING",
        next_cursor=next_cursor,
        pages_processed=base_pages,
        rows_written=base_rows,
        started_at=started_at,
        error_text=None,
    )

    org_uri, user_uri = _resolve_scope()
    result: Dict[str, Any] = {
        "status": "RUNNING",
        "pages_processed": base_pages,
        "rows_upserted": base_rows,
        "upstream_status": None,
        "failed_events": 0,
        "total_events": 0,
    }
    terminal_scheduled_statuses = {"COMPLETED", "PAUSED_LIMIT_REACHED", "FAILED", "PARTIAL_FAILED"}
    poll_seconds = max(5, CALENDLY_INVITEE_DRAIN_POLL_SECONDS)
    invitees_max_pages = _entity_page_cap("event_invitees", 0)

    while True:
        scheduled_state = read_state(run_id=run_id, entity_type="scheduled_events")
        scheduled_status = str((scheduled_state or {}).get("status", scheduled_status))
        next_cursor = (scheduled_state or {}).get("next_cursor", next_cursor)

        try:
            result = _process_invitees_for_backfill_run(
                run_id=run_id,
                is_backfill=True,
                organization_uri=org_uri,
                user_uri=user_uri,
                started_at=started_at,
                next_cursor=next_cursor,
                base_pages_processed=base_pages,
                base_rows_written=base_rows,
                max_pages=invitees_max_pages,
            )
        except Exception as exc:
            existing = read_state(run_id=run_id, entity_type="event_invitees")
            if existing:
                write_state(
                    run_id=run_id,
                    entity_type="event_invitees",
                    status="FAILED",
                    next_cursor=existing.get("next_cursor"),
                    pages_processed=int(existing.get("pages_processed", 0)),
                    rows_written=int(existing.get("rows_written", 0)),
                    started_at=existing.get("started_at") or _utc_now(),
                    error_text=str(exc)[:2000],
                )
            raise

        invitee_summary = _summarize_invitee_work(run_id)
        pending_events = int(invitee_summary.get("pending_events", 0))
        invitee_pages = int(result.get("pages_processed", base_pages))
        invitee_rows = int(result.get("rows_upserted", base_rows))
        invitee_failed = int(result.get("failed_events", 0))

        if pending_events > 0:
            invitee_status = "RUNNING"
        elif str(result.get("status")) == "PAUSED_LIMIT_REACHED":
            invitee_status = "PAUSED_LIMIT_REACHED"
        elif invitee_failed > 0:
            invitee_status = "PARTIAL_FAILED"
        elif scheduled_status in terminal_scheduled_statuses:
            invitee_status = "COMPLETED"
        else:
            invitee_status = "RUNNING"

        write_state(
            run_id=run_id,
            entity_type="event_invitees",
            status=invitee_status,
            next_cursor=next_cursor,
            pages_processed=invitee_pages,
            rows_written=invitee_rows,
            started_at=started_at,
            error_text=None,
        )

        if scheduled_status in terminal_scheduled_statuses and pending_events == 0:
            break
        if pending_events == 0:
            time.sleep(poll_seconds)

    final_invitee_state = read_state(run_id=run_id, entity_type="event_invitees") or {}
    invitee_status = str(final_invitee_state.get("status", "RUNNING"))
    invitee_pages = int(final_invitee_state.get("pages_processed", result.get("pages_processed", base_pages)))
    invitee_rows = int(final_invitee_state.get("rows_written", result.get("rows_upserted", base_rows)))

    statements_executed = 0
    should_run_models = run_models_after and scheduled_status == "COMPLETED" and invitee_status == "COMPLETED"
    if should_run_models:
        statements_executed = run_models()

    return {
        "run_id": run_id,
        "mode": "invitees_only",
        "scheduled_status": scheduled_status,
        "results": {
            "event_invitees": {
                "status": invitee_status,
                "pages_processed": invitee_pages,
                "rows_upserted": invitee_rows,
                "upstream_status": result.get("upstream_status"),
                "failed_events": result.get("failed_events", 0),
                "total_events": result.get("total_events", 0),
            }
        },
        "models_refreshed": should_run_models,
        "statements_executed": statements_executed,
    }
