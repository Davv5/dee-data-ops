import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")

GHL_API_BASE = os.getenv("GHL_API_BASE", "https://services.leadconnectorhq.com")
GHL_API_VERSION = os.getenv("GHL_API_VERSION", "2021-07-28")
GHL_ACCESS_TOKEN = os.getenv("GHL_ACCESS_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
GHL_LOCATION_PARAM = os.getenv("GHL_LOCATION_PARAM", "locationId")
GHL_AUTH_SCHEME = os.getenv("GHL_AUTH_SCHEME", "auto").strip().lower()

GHL_PAGE_LIMIT = int(os.getenv("GHL_PAGE_LIMIT", "100"))
GHL_PAGE_PARAM = os.getenv("GHL_PAGE_PARAM", "page")
GHL_CURSOR_PARAM = os.getenv("GHL_CURSOR_PARAM", "startAfterId")
GHL_UPDATED_AFTER_PARAM = os.getenv("GHL_UPDATED_AFTER_PARAM", "updatedAfter")

GHL_RAW_TABLE = os.getenv("BQ_GHL_RAW_TABLE", "ghl_objects_raw")
GHL_STATE_TABLE = os.getenv("BQ_GHL_STATE_TABLE", "ghl_backfill_state")

DEFAULT_OBJECT_TYPES = "contacts,opportunities,forms,form_submissions,pipelines,pipeline_stages,conversations"
GHL_OBJECT_TYPES = os.getenv("GHL_OBJECT_TYPES", DEFAULT_OBJECT_TYPES)
GHL_ENABLE_OUTBOUND_CALL_LOGS = (
    os.getenv("GHL_ENABLE_OUTBOUND_CALL_LOGS", "false").strip().lower() == "true"
)

GHL_INCREMENTAL_LOOKBACK_HOURS = int(os.getenv("GHL_INCREMENTAL_LOOKBACK_HOURS", "2"))
GHL_INCREMENTAL_MAX_PAGES_PER_OBJECT = int(os.getenv("GHL_INCREMENTAL_MAX_PAGES_PER_OBJECT", "3"))
GHL_REQUEST_TIMEOUT_SEC = int(os.getenv("GHL_REQUEST_TIMEOUT_SEC", "25"))
GHL_MAX_ATTEMPTS_DEFAULT = int(os.getenv("GHL_MAX_ATTEMPTS_DEFAULT", "4"))
GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS = int(os.getenv("GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS", "2"))

SOURCE_NAME = "gohighlevel"

ENTITY_DEFAULT_ENDPOINTS = {
    "contacts": "/contacts/",
    "opportunities": "/opportunities/search",
    "forms": "/forms/",
    "form_submissions": "/forms/submissions",
    "pipelines": "/opportunities/pipelines",
    "pipeline_stages": "/opportunities/pipelines",
    # Conversations: full conversation thread index (cursor-paginated).
    # Downstream STG/Core models read entity_type='conversations'.
    "conversations": "/conversations/search",
    # Optional speed-to-lead source. Override endpoint/method/pagination via env.
    # Uses location-level messages export; downstream models filter outbound CALL rows.
    "outbound_call_logs": "/conversations/messages/export",
    # Team members roster — supports updatedAfter for incremental, page pagination.
    "users": "/users/search",
    # Location tag catalog — endpoint path contains locationId, set GHL_ENDPOINT_TAGS
    # in env if a different location is needed. No pagination (full list in one response).
    "tags": "",  # Must be overridden via GHL_ENDPOINT_TAGS env var
    # Custom field schema definitions for the location.  {location_id} is resolved at
    # runtime by _endpoint_for_entity.  No pagination — full list in one response.
    "custom_field_definitions": "/locations/{location_id}/customFields",
}

ENTITY_TYPE_ALIASES = {
    # Common config/runtime aliases we normalize to canonical entity names.
    "conversation": "conversations",
    "conversation_threads": "conversations",
    "conversation_message": "outbound_call_logs",
    "conversation_messages": "outbound_call_logs",
    "messages": "outbound_call_logs",
    "outbound_calls": "outbound_call_logs",
    "outbound_call_log": "outbound_call_logs",
}

ENTITY_DEFAULT_METHODS = {
    "contacts": "GET",
    "opportunities": "POST",
    "forms": "GET",
    "form_submissions": "GET",
    "pipelines": "GET",
    "pipeline_stages": "GET",
    "conversations": "GET",
    "outbound_call_logs": "GET",
    "users": "GET",
    "tags": "GET",
    "custom_field_definitions": "GET",
}

ENTITY_DEFAULT_PAGINATION = {
    "contacts": "page",
    "opportunities": "page",
    "forms": "skip",
    "form_submissions": "page",
    "pipelines": "none",
    "pipeline_stages": "none",
    "conversations": "cursor",
    "users": "page",
    "tags": "none",
    "outbound_call_logs": "none",
    # Single-response fetch; locationId is in the URL path, not a query param.
    "custom_field_definitions": "none",
}

# Entity types where the locationId appears in the URL path instead of as a query param.
# fetch_entity_page skips adding locationId to the request payload for these types.
ENTITY_LOCATION_IN_PATH: frozenset = frozenset({"custom_field_definitions"})

GHL_SCROLL_SORT_OPPORTUNITIES = os.getenv(
    "GHL_SCROLL_SORT_OPPORTUNITIES",
    '[{"field":"date_added","direction":"asc"}]',
)

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
        # GoHighLevel occasionally uses milliseconds.
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


def _get_first(item: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[Any]:
    for key in keys:
        if key in item and item.get(key) is not None:
            return item.get(key)
    return None


def _entity_env_suffix(entity_type: str) -> str:
    return entity_type.upper().replace("-", "_")


def _normalize_entity_type(entity_type: str) -> str:
    normalized = entity_type.strip()
    return ENTITY_TYPE_ALIASES.get(normalized, normalized)


def _endpoint_for_entity(entity_type: str) -> str:
    canonical_entity_type = _normalize_entity_type(entity_type)
    env_key = f"GHL_ENDPOINT_{_entity_env_suffix(canonical_entity_type)}"
    endpoint = os.getenv(env_key, ENTITY_DEFAULT_ENDPOINTS.get(canonical_entity_type))
    if not endpoint:
        supported = ", ".join(sorted(ENTITY_DEFAULT_ENDPOINTS.keys()))
        raise ValueError(
            f"Unsupported GoHighLevel entity type: {entity_type} "
            f"(normalized={canonical_entity_type}; supported={supported}). "
            f"If this type requires a dynamic path (e.g. tags), set {env_key} in the environment."
        )
    # Resolve path-level {location_id} placeholder (e.g. custom_field_definitions).
    if "{location_id}" in endpoint:
        endpoint = endpoint.replace("{location_id}", GHL_LOCATION_ID or "")
    return endpoint if endpoint.startswith("/") else f"/{endpoint}"


def _method_for_entity(entity_type: str) -> str:
    canonical_entity_type = _normalize_entity_type(entity_type)
    env_key = f"GHL_METHOD_{_entity_env_suffix(canonical_entity_type)}"
    method = os.getenv(env_key, ENTITY_DEFAULT_METHODS.get(canonical_entity_type, "GET"))
    method = method.upper().strip()
    if method not in {"GET", "POST"}:
        raise ValueError(f"Unsupported HTTP method for {canonical_entity_type}: {method}")
    return method


def _pagination_for_entity(entity_type: str) -> str:
    canonical_entity_type = _normalize_entity_type(entity_type)
    env_key = f"GHL_PAGINATION_{_entity_env_suffix(canonical_entity_type)}"
    mode = os.getenv(env_key, ENTITY_DEFAULT_PAGINATION.get(canonical_entity_type, "page"))
    mode = mode.strip().lower()
    if mode not in {"page", "skip", "cursor", "scroll", "none"}:
        raise ValueError(f"Unsupported pagination mode for {canonical_entity_type}: {mode}")
    return mode


def _max_attempts_for_entity(entity_type: str) -> int:
    if _normalize_entity_type(entity_type) == "outbound_call_logs":
        return max(1, GHL_MAX_ATTEMPTS_OUTBOUND_CALL_LOGS)
    return max(1, GHL_MAX_ATTEMPTS_DEFAULT)


def _require_runtime_config() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")


def ensure_tables() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")

    raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{GHL_RAW_TABLE}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      location_id STRING NOT NULL,
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
    CLUSTER BY entity_type, location_id, entity_id
    """
    client.query(raw_query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{GHL_STATE_TABLE}` (
      run_id STRING NOT NULL,
      entity_type STRING NOT NULL,
      location_id STRING NOT NULL,
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


def parse_object_types() -> List[str]:
    requested = [_normalize_entity_type(x) for x in GHL_OBJECT_TYPES.split(",") if x.strip()]
    # Preserve user-provided ordering while de-duping aliases.
    requested = list(dict.fromkeys(requested))
    if GHL_ENABLE_OUTBOUND_CALL_LOGS:
        return requested
    return [entity for entity in requested if entity != "outbound_call_logs"]


def _extract_list_candidates(payload: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
    candidates: List[List[Dict[str, Any]]] = []
    for key in (
        "data",
        "items",
        "results",
        "contacts",
        "opportunities",
        "forms",
        "submissions",
        "formSubmissions",
        "pipelines",
        "stages",
        "conversations",
        "messages",
        "conversationMessages",
        "conversation_messages",
        "callLogs",
        "call_logs",
        "calls",
        "users",
        "tags",
        "customFields",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.append([x for x in value if isinstance(x, dict)])

    data_block = payload.get("data")
    if isinstance(data_block, dict):
        for key in (
            "items",
            "results",
            "contacts",
            "opportunities",
            "forms",
            "submissions",
            "pipelines",
            "stages",
            "conversations",
            "messages",
            "conversationMessages",
            "conversation_messages",
            "callLogs",
            "call_logs",
            "calls",
        ):
            value = data_block.get(key)
            if isinstance(value, list):
                candidates.append([x for x in value if isinstance(x, dict)])
    return candidates


def _extract_conversation_like_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for key in (
        "message",
        "conversation",
        "call",
        "callLog",
        "call_log",
    ):
        value = payload.get(key)
        if isinstance(value, dict):
            out.append(value)
    data_block = payload.get("data")
    if isinstance(data_block, dict):
        for key in (
            "message",
            "conversation",
            "call",
            "callLog",
            "call_log",
        ):
            value = data_block.get(key)
            if isinstance(value, dict):
                out.append(value)
    return out


def _looks_like_entity_record(entity_type: str, payload: Dict[str, Any]) -> bool:
    entity_id = _extract_entity_id(entity_type=entity_type, item=payload)
    if entity_id:
        return True
    if entity_type in {"conversations", "outbound_call_logs"}:
        return _safe_str(
            _get_first(
                payload,
                (
                    "contactId",
                    "contact_id",
                    "conversationId",
                    "conversation_id",
                    "messageId",
                    "message_id",
                    "callId",
                    "call_id",
                ),
            )
        ) is not None
    return False


def _extract_pipeline_stages_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    pipelines: List[Dict[str, Any]] = []

    for candidate in _extract_list_candidates(payload):
        if candidate and any("stages" in p or "pipelineStages" in p for p in candidate):
            pipelines.extend(candidate)

    if not pipelines:
        return []

    out: List[Dict[str, Any]] = []
    for pipeline in pipelines:
        pipeline_id = _safe_str(_get_first(pipeline, ("id", "_id", "pipelineId", "pipeline_id")))
        pipeline_name = _safe_str(_get_first(pipeline, ("name", "title")))
        stages = _get_first(pipeline, ("stages", "pipelineStages"))
        if not isinstance(stages, list):
            continue
        for stage in stages:
            if not isinstance(stage, dict):
                continue
            stage_row = dict(stage)
            if pipeline_id:
                stage_row["pipelineId"] = pipeline_id
            if pipeline_name:
                stage_row["pipelineName"] = pipeline_name
            out.append(stage_row)
    return out


def _extract_items(payload: Dict[str, Any], entity_type: str) -> List[Dict[str, Any]]:
    canonical_entity_type = _normalize_entity_type(entity_type)
    if canonical_entity_type == "pipeline_stages":
        return _extract_pipeline_stages_from_payload(payload)

    if canonical_entity_type in {"conversations", "outbound_call_logs"}:
        conversation_like_items = _extract_conversation_like_items(payload)
        if conversation_like_items:
            return conversation_like_items

    candidates = _extract_list_candidates(payload)
    if candidates:
        # Prefer the largest list in case payload exposes several list views.
        return max(candidates, key=len)
    if _looks_like_entity_record(entity_type=canonical_entity_type, payload=payload):
        return [payload]
    data_block = payload.get("data")
    if isinstance(data_block, dict) and _looks_like_entity_record(
        entity_type=canonical_entity_type, payload=data_block
    ):
        return [data_block]
    return []


def _extract_entity_id(entity_type: str, item: Dict[str, Any]) -> Optional[str]:
    canonical_entity_type = _normalize_entity_type(entity_type)
    if canonical_entity_type == "pipeline_stages":
        pipeline_id = _safe_str(_get_first(item, ("pipelineId", "pipeline_id")))
        stage_id = _safe_str(_get_first(item, ("id", "_id", "stageId", "stage_id")))
        if pipeline_id and stage_id:
            return f"{pipeline_id}:{stage_id}"
        return stage_id or pipeline_id

    key_map: Dict[str, Tuple[str, ...]] = {
        "contacts": ("id", "_id", "contactId", "contact_id"),
        "opportunities": ("id", "_id", "opportunityId", "opportunity_id"),
        "forms": ("id", "_id", "formId", "form_id"),
        "form_submissions": ("id", "_id", "submissionId", "submission_id"),
        "pipelines": ("id", "_id", "pipelineId", "pipeline_id"),
        "conversations": ("id", "_id", "conversationId", "conversation_id", "messageId", "message_id"),
        "outbound_call_logs": ("id", "_id", "messageId", "message_id", "callId", "call_id"),
        "custom_field_definitions": ("id", "_id"),
    }

    keys = key_map.get(canonical_entity_type, ("id", "_id"))
    return _safe_str(_get_first(item, keys))


def _extract_location_id(item: Dict[str, Any]) -> Optional[str]:
    return _safe_str(
        _get_first(
            item,
            (
                "locationId",
                "location_id",
                "subAccountId",
                "sub_account_id",
            ),
        )
    ) or _safe_str(GHL_LOCATION_ID)


def _extract_event_ts(item: Dict[str, Any]) -> Optional[datetime]:
    return _parse_ts(
        _get_first(
            item,
            (
                "createdAt",
                "created_at",
                "dateAdded",
                "date_added",
                "submittedAt",
                "submitted_at",
                "created",
                "date",
            ),
        )
    )


def _extract_updated_ts(item: Dict[str, Any]) -> Optional[datetime]:
    return _parse_ts(
        _get_first(
            item,
            (
                "updatedAt",
                "updated_at",
                "lastUpdated",
                "last_updated",
                "dateUpdated",
                "date_updated",
                "modifiedAt",
                "modified_at",
            ),
        )
    )


def _extract_bool(payload: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[bool]:
    for key in keys:
        if key in payload:
            value = payload.get(key)
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes"}:
                    return True
                if lowered in {"false", "0", "no"}:
                    return False
    return None


def _extract_next_cursor(payload: Dict[str, Any]) -> Optional[str]:
    for key in (
        "nextCursor",
        "next_cursor",
        "cursor",
        "nextPageToken",
        "next_page_token",
        "nextPageUrl",
        "next_page_url",
    ):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()

    meta = payload.get("meta")
    if isinstance(meta, dict):
        for key in (
            "nextCursor",
            "next_cursor",
            "nextPageToken",
            "next_page_token",
            "nextPageUrl",
            "next_page_url",
            "startAfterId",    # POST /contacts/search pagination
            "start_after_id",
        ):
            value = meta.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()

    return None


def _derive_pagination(
    payload: Dict[str, Any],
    items: List[Dict[str, Any]],
    current_cursor: Optional[str],
    pagination_mode: str,
) -> Tuple[Optional[str], bool]:
    if pagination_mode == "none":
        return None, False

    if pagination_mode == "scroll":
        if not items:
            return None, False
        last_sort = items[-1].get("sort")
        if not isinstance(last_sort, list) or not last_sort:
            return None, False
        next_cursor = json.dumps(last_sort, separators=(",", ":"))
        has_more = len(items) >= GHL_PAGE_LIMIT
        return (next_cursor if has_more else None), has_more

    if pagination_mode == "skip":
        current_skip = int(current_cursor) if current_cursor and current_cursor.isdigit() else 0
        next_skip = current_skip + len(items)
        total_val = payload.get("total")
        if isinstance(total_val, str) and total_val.isdigit():
            total = int(total_val)
        elif isinstance(total_val, (int, float)):
            total = int(total_val)
        else:
            total = None
        has_more = next_skip < total if total is not None else len(items) >= GHL_PAGE_LIMIT
        return (str(next_skip) if has_more else None), has_more

    has_more = _extract_bool(payload, ("hasMore", "has_more", "more"))
    next_cursor = _extract_next_cursor(payload)

    if has_more is None:
        if next_cursor:
            has_more = True
        elif len(items) >= GHL_PAGE_LIMIT:
            has_more = True
        else:
            has_more = False

    if has_more and not next_cursor:
        if pagination_mode == "cursor":
            last_item = items[-1] if items else None
            if isinstance(last_item, dict):
                next_cursor = _safe_str(_get_first(last_item, ("id", "_id")))
        else:
            if current_cursor and current_cursor.isdigit():
                next_cursor = str(int(current_cursor) + 1)
            elif current_cursor is None:
                next_cursor = "2"
            else:
                last_item = items[-1] if items else None
                if isinstance(last_item, dict):
                    next_cursor = _safe_str(_get_first(last_item, ("id", "_id")))

    if next_cursor == current_cursor:
        return None, False

    if not items and not next_cursor:
        return None, False

    return next_cursor, bool(has_more)


def fetch_entity_page(
    entity_type: str,
    next_cursor: Optional[str],
    updated_after: Optional[datetime] = None,
    max_attempts: Optional[int] = None,
    initial_backoff_sec: float = 1.0,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str], bool, int]:
    _require_runtime_config()

    endpoint = _endpoint_for_entity(entity_type)
    method = _method_for_entity(entity_type)
    pagination_mode = _pagination_for_entity(entity_type)
    attempts = _max_attempts_for_entity(entity_type) if max_attempts is None else max(1, max_attempts)
    url = f"{GHL_API_BASE.rstrip('/')}{endpoint}"
    request_url = url

    # Contacts incremental: switch to POST /contacts/search with a structured filters body
    # instead of the GET /contacts/?updatedAfter= pattern (which has a known 422 failure mode).
    canonical_entity_type = _normalize_entity_type(entity_type)
    contacts_incremental = canonical_entity_type == "contacts" and updated_after is not None
    if contacts_incremental:
        method = "POST"
        request_url = f"{GHL_API_BASE.rstrip('/')}/contacts/search"

    # For entities whose locationId lives in the URL path (e.g. custom_field_definitions),
    # do not add it as a query/body param — the path resolution already included it.
    payload_params_base: Dict[str, Any] = {}
    if canonical_entity_type not in ENTITY_LOCATION_IN_PATH:
        payload_params_base[GHL_LOCATION_PARAM] = GHL_LOCATION_ID
    cursor_from_url = False

    # GHL often returns nextPageUrl containing page/startAfter/startAfterId tokens.
    # Preserve those query params instead of treating the whole URL as one cursor.
    if pagination_mode == "page" and next_cursor and next_cursor.startswith(("http://", "https://")):
        parsed = urlparse(next_cursor)
        if parsed.scheme and parsed.netloc and parsed.path:
            request_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            parsed_params = {
                key: values[-1]
                for key, values in parse_qs(parsed.query, keep_blank_values=False).items()
                if values
            }
            payload_params_base.update(parsed_params)
            payload_params_base.setdefault(GHL_LOCATION_PARAM, GHL_LOCATION_ID)
            # GHL rejects requests that include both page and startAfter/startAfterId.
            if "startAfter" in payload_params_base and GHL_CURSOR_PARAM in payload_params_base:
                payload_params_base.pop(GHL_PAGE_PARAM, None)
            cursor_from_url = True
    if pagination_mode != "none":
        payload_params_base["limit"] = GHL_PAGE_LIMIT
    if pagination_mode == "page":
        if cursor_from_url:
            pass
        elif next_cursor and next_cursor.isdigit():
            payload_params_base[GHL_PAGE_PARAM] = int(next_cursor)
        elif next_cursor:
            payload_params_base[GHL_CURSOR_PARAM] = next_cursor
        else:
            payload_params_base[GHL_PAGE_PARAM] = 1
    elif pagination_mode == "skip":
        if next_cursor and next_cursor.isdigit():
            payload_params_base["skip"] = int(next_cursor)
        elif next_cursor:
            payload_params_base[GHL_CURSOR_PARAM] = next_cursor
        else:
            payload_params_base["skip"] = 0
    elif pagination_mode == "cursor":
        if next_cursor:
            payload_params_base[GHL_CURSOR_PARAM] = next_cursor
    elif pagination_mode == "scroll":
        try:
            sort_spec = json.loads(GHL_SCROLL_SORT_OPPORTUNITIES)
            if not isinstance(sort_spec, list):
                raise ValueError("GHL_SCROLL_SORT_OPPORTUNITIES must decode to an array")
        except Exception as exc:
            raise RuntimeError(
                "Invalid GHL_SCROLL_SORT_OPPORTUNITIES; expected JSON array"
            ) from exc
        payload_params_base["sort"] = sort_spec
        if next_cursor:
            try:
                sa = json.loads(next_cursor)
                if not isinstance(sa, list):
                    raise ValueError("searchAfter cursor must be a JSON array")
            except Exception as exc:
                raise RuntimeError("Invalid scroll next_cursor format") from exc
            payload_params_base["searchAfter"] = sa
        else:
            payload_params_base["searchAfter"] = []
    elif pagination_mode == "none":
        pass

    if contacts_incremental:
        # Override payload: POST /contacts/search expects a filters body, not query params.
        # Operator must be "gt" (not "gte") and value must be epoch milliseconds (int),
        # not an ISO-8601 string — LeadConnector returns 422 for the documented-but-rejected
        # "gte" form and for ISO string values on date fields. Pattern matches the working
        # production usage in accounting-qs/compete-iq's GHL client.
        payload_params_base = {
            GHL_LOCATION_PARAM: GHL_LOCATION_ID,
            "pageLimit": GHL_PAGE_LIMIT,
            "filters": [
                {
                    "field": "dateUpdated",
                    "operator": "gt",
                    "value": int(updated_after.astimezone(timezone.utc).timestamp() * 1000),
                }
            ],
            "sort": [{"field": "dateUpdated", "direction": "asc"}],
        }
        if next_cursor:
            payload_params_base["startAfterId"] = next_cursor

    if GHL_AUTH_SCHEME == "bearer":
        auth_values = [f"Bearer {GHL_ACCESS_TOKEN}"]
    elif GHL_AUTH_SCHEME in {"raw", "plain", "token"}:
        auth_values = [GHL_ACCESS_TOKEN]
    else:
        # Auto mode: try bearer first, then plain token for private integration keys.
        auth_values = [f"Bearer {GHL_ACCESS_TOKEN}", GHL_ACCESS_TOKEN]

    last_resp: Optional[requests.Response] = None

    for auth_value in auth_values:
        headers = {
            "Authorization": auth_value,
            "Version": GHL_API_VERSION,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        backoff = initial_backoff_sec
        for attempt in range(1, attempts + 1):
            payload_params = dict(payload_params_base)
            # contacts_incremental already embeds the watermark in the filters body above;
            # skip the legacy updatedAfter query/body param for that path.
            if updated_after and not contacts_incremental:
                payload_params[GHL_UPDATED_AFTER_PARAM] = updated_after.isoformat()

            if method == "POST":
                resp = requests.post(
                    request_url, headers=headers, json=payload_params, timeout=GHL_REQUEST_TIMEOUT_SEC
                )
            else:
                resp = requests.get(
                    request_url, headers=headers, params=payload_params, timeout=GHL_REQUEST_TIMEOUT_SEC
                )

            last_resp = resp
            if (
                resp.status_code == 422
                and updated_after is not None
                and not contacts_incremental
                and "updatedAfter" in (resp.text or "")
            ):
                payload_params = dict(payload_params_base)
                if method == "POST":
                    resp = requests.post(
                        request_url, headers=headers, json=payload_params, timeout=GHL_REQUEST_TIMEOUT_SEC
                    )
                else:
                    resp = requests.get(
                        request_url, headers=headers, params=payload_params, timeout=GHL_REQUEST_TIMEOUT_SEC
                    )
                last_resp = resp

            if resp.ok:
                try:
                    payload = resp.json()
                except ValueError as exc:
                    raise RuntimeError(
                        f"GoHighLevel response was not valid JSON for entity_type={entity_type}"
                    ) from exc

                if isinstance(payload, list):
                    payload = {"data": payload}
                if not isinstance(payload, dict):
                    payload = {"data": []}

                items = _extract_items(payload=payload, entity_type=entity_type)
                new_cursor, has_more = _derive_pagination(
                    payload=payload,
                    items=items,
                    current_cursor=next_cursor,
                    pagination_mode=pagination_mode,
                )
                return payload, items, new_cursor, has_more, resp.status_code

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            break

        # Retry with next auth mode only for auth failures.
        if last_resp is None:
            continue
        body_preview = (last_resp.text or "")[:1000]
        if not (
            last_resp.status_code == 401
            and "invalid jwt" in body_preview.lower()
        ):
            break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    raise RuntimeError(
        f"GoHighLevel request failed (entity_type={entity_type}, status={status}, "
        f"url={url}, body_preview={body_preview})"
    )


def build_rows(
    entity_type: str,
    items: List[Dict[str, Any]],
    run_id: str,
    is_backfill: bool,
) -> List[Dict[str, Any]]:
    ingested_at_dt = _utc_now()
    ingested_at = ingested_at_dt.isoformat()
    rows: List[Dict[str, Any]] = []

    for item in items:
        entity_id = _extract_entity_id(entity_type=entity_type, item=item)
        location_id = _extract_location_id(item)
        if not entity_id or not location_id:
            continue

        event_ts = _extract_event_ts(item)
        updated_ts = _extract_updated_ts(item)
        partition_date = (updated_ts or event_ts or ingested_at_dt).date().isoformat()

        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "location_id": location_id,
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

    stage_table = f"{PROJECT_ID}.{DATASET}.ghl_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      location_id STRING NOT NULL,
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
        insert_errors = client.insert_rows_json(stage_table, rows)
        if insert_errors:
            raise RuntimeError(f"GoHighLevel stage insert errors: {insert_errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{GHL_RAW_TABLE}` T
        USING (
          SELECT * EXCEPT(rn)
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY entity_type, entity_id, location_id
                ORDER BY updated_at_ts DESC, event_ts DESC, ingested_at DESC
              ) AS rn
            FROM `{stage_table}`
          )
          WHERE rn = 1
        ) S
        ON T.entity_type = S.entity_type
           AND T.entity_id = S.entity_id
           AND T.location_id = S.location_id
        WHEN MATCHED THEN UPDATE SET
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
            entity_type, entity_id, location_id, partition_date, event_ts, updated_at_ts,
            ingested_at, source, payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.entity_type, S.entity_id, S.location_id, S.partition_date, S.event_ts, S.updated_at_ts,
            S.ingested_at, S.source, S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def write_state(
    run_id: str,
    entity_type: str,
    location_id: str,
    status: str,
    next_cursor: Optional[str],
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{GHL_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @entity_type AS entity_type,
        @location_id AS location_id,
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
       AND T.location_id = S.location_id
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
        run_id, entity_type, location_id, status, next_cursor,
        pages_processed, rows_written, started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.entity_type, S.location_id, S.status, S.next_cursor,
        S.pages_processed, S.rows_written, S.started_at, S.updated_at, S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_cursor", "STRING", next_cursor),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(query, job_config=job_config).result()


def read_state(run_id: str, entity_type: str, location_id: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id,
      entity_type,
      location_id,
      status,
      next_cursor,
      pages_processed,
      rows_written,
      started_at,
      updated_at,
      error_text
    FROM `{PROJECT_ID}.{DATASET}.{GHL_STATE_TABLE}`
    WHERE run_id = @run_id
      AND entity_type = @entity_type
      AND location_id = @location_id
    LIMIT 1
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
        bigquery.ScalarQueryParameter("location_id", "STRING", location_id),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None

    row = rows[0]
    return {
        "run_id": row["run_id"],
        "entity_type": row["entity_type"],
        "location_id": row["location_id"],
        "status": row["status"],
        "next_cursor": row["next_cursor"],
        "pages_processed": row["pages_processed"],
        "rows_written": row["rows_written"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "error_text": row["error_text"],
    }


def run_models(sql_file_path: Optional[str] = None) -> int:
    if sql_file_path is None:
        candidate_path = Path(__file__).resolve().parent / "sql" / "ghl_models.sql"
        if candidate_path.exists():
            sql_file_path = str(candidate_path)
        else:
            sql_file_path = str(Path(__file__).resolve().parents[2] / "sql" / "ghl_models.sql")

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


def ensure_pipeline_stage_snapshots_table() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")

    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.Core.fct_pipeline_stage_snapshots`
    (
      snapshot_date DATE NOT NULL,
      location_id STRING,
      opportunity_id STRING NOT NULL,
      contact_id STRING,
      pipeline_id STRING,
      pipeline_name STRING,
      pipeline_stage_id STRING,
      stage_name STRING,
      status STRING,
      opportunity_name STRING,
      amount FLOAT64,
      assigned_to_user_id STRING,
      effective_probability FLOAT64,
      last_stage_change_at TIMESTAMP,
      days_in_current_stage INT64,
      snapshotted_at TIMESTAMP NOT NULL
    )
    PARTITION BY snapshot_date
    CLUSTER BY pipeline_id, status
    OPTIONS (
      require_partition_filter = FALSE,
      partition_expiration_days = NULL
    )
    """
    client.query(create_query).result()


def snapshot_pipeline_stages_daily() -> Dict[str, Any]:
    """
    Append today's opportunity states into Core.fct_pipeline_stage_snapshots.

    Captures every opportunity — including terminal (won/lost/abandoned) — so
    downstream transition analysis can detect the move into a terminal state.
    Filtering to active-only here would silently drop the most analytically
    valuable transitions (the close events).

    Idempotent per day: delete current day's partition before insert.
    """
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")

    ensure_pipeline_stage_snapshots_table()
    snapshot_day = _utc_now().date()
    params = [bigquery.ScalarQueryParameter("snapshot_date", "DATE", snapshot_day)]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    delete_query = f"""
    DELETE FROM `{PROJECT_ID}.Core.fct_pipeline_stage_snapshots`
    WHERE snapshot_date = @snapshot_date
    """
    client.query(delete_query, job_config=job_config).result()

    insert_query = f"""
    INSERT INTO `{PROJECT_ID}.Core.fct_pipeline_stage_snapshots`
    SELECT
      @snapshot_date AS snapshot_date,
      location_id,
      opportunity_id,
      contact_id,
      pipeline_id,
      pipeline_name,
      pipeline_stage_id,
      stage_name,
      status,
      opportunity_name,
      SAFE_CAST(amount AS FLOAT64) AS amount,
      -- NULL until Core.fct_ghl_opportunities surfaces $.assignedTo (the
      -- staging model already extracts it as `assigned_user_id`; the warehouse
      -- model does not pass it through). Tracked as a follow-up.
      CAST(NULL AS STRING) AS assigned_to_user_id,
      SAFE_CAST(effective_probability AS FLOAT64) AS effective_probability,
      last_stage_change_at,
      CASE
        WHEN last_stage_change_at IS NULL THEN NULL
        WHEN DATE(last_stage_change_at) > @snapshot_date THEN 0
        ELSE DATE_DIFF(@snapshot_date, DATE(last_stage_change_at), DAY)
      END AS days_in_current_stage,
      CURRENT_TIMESTAMP() AS snapshotted_at
    FROM `{PROJECT_ID}.Core.fct_ghl_opportunities`
    WHERE opportunity_id IS NOT NULL
    """
    job = client.query(insert_query, job_config=job_config)
    job.result()
    rows_inserted = int(job.num_dml_affected_rows or 0)

    print(f"[snapshot_pipeline_stages_daily] snapshot_date={snapshot_day} rows_inserted={rows_inserted}", flush=True)
    return {
        "snapshot_date": snapshot_day.isoformat(),
        "rows_inserted": rows_inserted,
    }


def run_incremental_sync() -> Dict[str, Any]:
    ensure_tables()
    _require_runtime_config()

    run_id = f"ghl-hourly-{_utc_now().strftime('%Y%m%d-%H%M%S')}"
    object_types = parse_object_types()
    updated_after = _utc_now() - timedelta(hours=GHL_INCREMENTAL_LOOKBACK_HOURS)

    entities_summary: List[Dict[str, Any]] = []

    for entity_type in object_types:
        next_cursor: Optional[str] = None
        pages_processed = 0
        rows_written = 0
        last_upstream_status: Optional[int] = None
        status = "RUNNING"
        error_text: Optional[str] = None

        try:
            while True:
                if (
                    GHL_INCREMENTAL_MAX_PAGES_PER_OBJECT > 0
                    and pages_processed >= GHL_INCREMENTAL_MAX_PAGES_PER_OBJECT
                ):
                    status = "PAUSED_LIMIT_REACHED"
                    break

                _, items, new_cursor, has_more, upstream_status = fetch_entity_page(
                    entity_type=entity_type,
                    next_cursor=next_cursor,
                    updated_after=updated_after,
                )
                last_upstream_status = upstream_status
                rows = build_rows(
                    entity_type=entity_type,
                    items=items,
                    run_id=run_id,
                    is_backfill=False,
                )
                if rows:
                    upsert_raw_rows(rows)
                    rows_written += len(rows)

                pages_processed += 1
                next_cursor = new_cursor

                if not has_more:
                    status = "COMPLETED"
                    break
        except Exception as exc:
            status = "FAILED"
            error_text = str(exc)[:2000]

        entities_summary.append(
            {
                "entity_type": entity_type,
                "status": status,
                "pages_processed": pages_processed,
                "rows_upserted": rows_written,
                "upstream_status": last_upstream_status,
                "error_text": error_text,
            }
        )

    run_models_after = os.getenv("GHL_RUN_MODELS_AFTER_INCREMENTAL", "false").lower() == "true"
    statements_executed = 0
    if run_models_after:
        statements_executed = run_models()

    return {
        "run_id": run_id,
        "location_id": GHL_LOCATION_ID,
        "lookback_hours": GHL_INCREMENTAL_LOOKBACK_HOURS,
        "entity_results": entities_summary,
        "models_refreshed": run_models_after,
        "statements_executed": statements_executed,
    }
