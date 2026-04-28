from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("BQ_DATASET", "Raw")
CORE_DATASET = os.getenv("BQ_CORE_DATASET", "Core")
MARTS_DATASET = os.getenv("BQ_MARTS_DATASET", "Marts")
OPS_DATASET = os.getenv("BQ_OPS_DATASET", "Ops")

GHL_API_BASE = os.getenv("GHL_API_BASE", "https://services.leadconnectorhq.com")
GHL_API_VERSION = os.getenv("GHL_API_VERSION", "2021-07-28")
GHL_ACCESS_TOKEN = os.getenv("GHL_ACCESS_TOKEN")
GHL_AUTH_SCHEME = os.getenv("GHL_AUTH_SCHEME", "auto").strip().lower()
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID")
GHL_LOCATION_PARAM = os.getenv("GHL_LOCATION_PARAM", "locationId")

GHL_USERS_METHOD = os.getenv("GHL_USERS_METHOD", "GET").strip().upper()
GHL_USERS_ENDPOINTS = os.getenv("GHL_USERS_ENDPOINTS", "/users/,/users/search")
GHL_USERS_PAGE_LIMIT = int(os.getenv("GHL_USERS_PAGE_LIMIT", "100"))
GHL_USERS_MAX_PAGES = int(os.getenv("GHL_USERS_MAX_PAGES", "200"))
GHL_USERS_TIMEOUT_SEC = int(os.getenv("GHL_USERS_TIMEOUT_SEC", "30"))
GHL_USERS_MAX_ATTEMPTS = int(os.getenv("GHL_USERS_MAX_ATTEMPTS", "5"))
GHL_USERS_INCLUDE_LIMIT = os.getenv("GHL_USERS_INCLUDE_LIMIT", "false").strip().lower() == "true"
GHL_USERS_PAGE_PARAM = os.getenv("GHL_USERS_PAGE_PARAM", "page")
GHL_USERS_CURSOR_PARAM = os.getenv("GHL_USERS_CURSOR_PARAM", "startAfterId")

IDENTITY_PIPELINE_NAME = os.getenv("IDENTITY_PIPELINE_NAME", "ghl_identity_sync")
IDENTITY_DQ_LOOKBACK_DAYS = int(os.getenv("IDENTITY_DQ_LOOKBACK_DAYS", "7"))
IDENTITY_DQ_MIN_TOUCHED_COVERAGE = float(os.getenv("IDENTITY_DQ_MIN_TOUCHED_COVERAGE", "0.95"))
UNKNOWN_SETTER_LABEL = os.getenv("UNKNOWN_SETTER_LABEL", "Unknown Setter")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL") or os.getenv("SLACK_WEBHOOK_URL")

RAW_USERS_STAGING_TABLE = os.getenv("BQ_GHL_USERS_STAGING_TABLE", "stg_ghl_users_current")
RAW_USERS_HISTORY_TABLE = os.getenv("BQ_GHL_USERS_HISTORY_TABLE", "ghl_users_raw")
CORE_USERS_SCD2_TABLE = os.getenv("BQ_GHL_USERS_SCD2_TABLE", "dim_ghl_users_scd2")
MART_SETTER_BRIDGE_TABLE = os.getenv("BQ_SETTER_BRIDGE_TABLE", "bridge_setter_identity")
MART_SPEED_TO_LEAD_ENRICHED_VIEW = os.getenv("BQ_SPEED_TO_LEAD_ENRICHED_VIEW", "v_fct_speed_to_lead_enriched")
MART_SETTER_UNKNOWN_QUEUE_TABLE = os.getenv("BQ_SETTER_UNKNOWN_QUEUE_TABLE", "rpt_setter_identity_unknown_queue")
MART_SETTER_COVERAGE_DAILY_TABLE = os.getenv("BQ_SETTER_COVERAGE_DAILY_TABLE", "rpt_setter_identity_coverage_daily")
MART_UNIFIED_DASHBOARD_SCHEMA_VIEW = os.getenv("BQ_UNIFIED_DASHBOARD_SCHEMA_VIEW", "v_unified_dashboard_schema")
OPS_SYNC_STATE_TABLE = os.getenv("BQ_SYNC_STATE_TABLE", "sync_state")
OPS_SETTER_OVERRIDES_TABLE = os.getenv("BQ_SETTER_OVERRIDES_TABLE", "setter_id_overrides")

client = bigquery.Client(project=PROJECT_ID)


class RetryableRequestError(RuntimeError):
    pass


class ApiResponseError(RuntimeError):
    def __init__(self, status_code: int, message: str, response_text: str = "") -> None:
        super().__init__(f"{message} (status={status_code})")
        self.status_code = status_code
        self.response_text = response_text


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    txt = str(value).strip()
    return txt if txt else None


def _normalize_email(value: Any) -> Optional[str]:
    txt = _safe_str(value)
    return txt.lower() if txt else None


def _table_ref(dataset: str, table: str) -> str:
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def _require_runtime_config() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")
    if GHL_USERS_METHOD not in {"GET", "POST"}:
        raise RuntimeError(f"Unsupported GHL_USERS_METHOD: {GHL_USERS_METHOD}")
    if GHL_USERS_PAGE_LIMIT < 1:
        raise RuntimeError("GHL_USERS_PAGE_LIMIT must be >= 1")
    if GHL_USERS_MAX_PAGES < 1:
        raise RuntimeError("GHL_USERS_MAX_PAGES must be >= 1")


def _auth_values() -> List[str]:
    if not GHL_ACCESS_TOKEN:
        return []
    if GHL_AUTH_SCHEME == "bearer":
        return [f"Bearer {GHL_ACCESS_TOKEN}"]
    if GHL_AUTH_SCHEME in {"raw", "plain", "token"}:
        return [GHL_ACCESS_TOKEN]
    return [f"Bearer {GHL_ACCESS_TOKEN}", GHL_ACCESS_TOKEN]


def _candidate_endpoints() -> List[str]:
    endpoints = []
    for raw in GHL_USERS_ENDPOINTS.split(","):
        txt = raw.strip()
        if not txt:
            continue
        endpoints.append(txt if txt.startswith("/") else f"/{txt}")
    if not endpoints:
        endpoints = ["/users/"]
    return list(dict.fromkeys(endpoints))


@retry(
    reraise=True,
    stop=stop_after_attempt(max(1, GHL_USERS_MAX_ATTEMPTS)),
    wait=wait_exponential_jitter(initial=1, max=30),
    retry=retry_if_exception_type(RetryableRequestError),
)
def _request_once(
    request_url: str,
    method: str,
    headers: Dict[str, str],
    payload_params: Dict[str, Any],
) -> requests.Response:
    try:
        if method == "POST":
            resp = requests.post(
                request_url,
                headers=headers,
                json=payload_params,
                timeout=GHL_USERS_TIMEOUT_SEC,
            )
        else:
            resp = requests.get(
                request_url,
                headers=headers,
                params=payload_params,
                timeout=GHL_USERS_TIMEOUT_SEC,
            )
    except requests.RequestException as exc:  # pragma: no cover
        raise RetryableRequestError(f"GHL users request failed: {exc}") from exc

    if resp.status_code in {429, 500, 502, 503, 504}:
        text = (resp.text or "")[:400]
        raise RetryableRequestError(f"GHL users retryable status={resp.status_code} body={text}")
    return resp


def _request_json(endpoint: str, payload_params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{GHL_API_BASE.rstrip('/')}{endpoint}"
    last_error: Optional[ApiResponseError] = None

    for auth_value in _auth_values():
        headers = {
            "Authorization": auth_value,
            "Version": GHL_API_VERSION,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        resp = _request_once(
            request_url=url,
            method=GHL_USERS_METHOD,
            headers=headers,
            payload_params=payload_params,
        )
        if resp.status_code in {401, 403}:
            last_error = ApiResponseError(resp.status_code, "GHL users unauthorized", (resp.text or "")[:400])
            continue
        if resp.status_code >= 400:
            raise ApiResponseError(resp.status_code, "GHL users request failed", (resp.text or "")[:400])
        try:
            payload = resp.json()
        except ValueError as exc:
            raise ApiResponseError(resp.status_code, "GHL users response was not valid JSON", (resp.text or "")[:400]) from exc
        if isinstance(payload, list):
            return {"items": payload}
        if isinstance(payload, dict):
            return payload
        raise ApiResponseError(resp.status_code, "GHL users response was not an object", (resp.text or "")[:400])

    if last_error is not None:
        raise last_error
    raise RuntimeError("No valid GHL authorization values configured.")


def _looks_like_user_record(item: Dict[str, Any]) -> bool:
    return any(item.get(key) for key in ("id", "_id", "userId", "user_id"))


def _extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[List[Dict[str, Any]]] = []
    for key in ("users", "items", "results", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.append([x for x in value if isinstance(x, dict)])
        elif isinstance(value, dict):
            for nested_key in ("users", "items", "results", "data"):
                nested_value = value.get(nested_key)
                if isinstance(nested_value, list):
                    candidates.append([x for x in nested_value if isinstance(x, dict)])
    if _looks_like_user_record(payload):
        candidates.append([payload])
    if not candidates:
        return []
    # Prefer the largest candidate set in case payload includes multiple wrappers.
    return max(candidates, key=len)


def _extract_next_cursor(payload: Dict[str, Any], items: List[Dict[str, Any]]) -> Optional[str]:
    keys = (
        "nextCursor",
        "next_cursor",
        "nextPageToken",
        "next_page_token",
        "startAfterId",
        "nextStartAfterId",
    )
    for key in keys:
        value = payload.get(key)
        txt = _safe_str(value)
        if txt:
            return txt
    for container_key in ("meta", "metadata", "paging", "pagination"):
        container = payload.get(container_key)
        if not isinstance(container, dict):
            continue
        for key in keys:
            value = container.get(key)
            txt = _safe_str(value)
            if txt:
                return txt
    if items:
        maybe_id = _safe_str(items[-1].get("id") or items[-1].get("_id") or items[-1].get("userId"))
        if maybe_id and GHL_USERS_METHOD == "GET":
            return maybe_id
    return None


def _pick_role(raw_role: Any) -> Optional[str]:
    if isinstance(raw_role, dict):
        for key in ("name", "label", "role", "value"):
            txt = _safe_str(raw_role.get(key))
            if txt:
                return txt
        return None
    return _safe_str(raw_role)


def _pick_team(raw_team: Any) -> Optional[str]:
    if isinstance(raw_team, dict):
        for key in ("name", "label", "team", "value"):
            txt = _safe_str(raw_team.get(key))
            if txt:
                return txt
        return None
    return _safe_str(raw_team)


def _pick_active(item: Dict[str, Any]) -> bool:
    for key in ("isActive", "is_active", "active"):
        value = item.get(key)
        if isinstance(value, bool):
            return value
        txt = _safe_str(value)
        if txt is None:
            continue
        lowered = txt.lower()
        if lowered in {"true", "1", "yes", "active"}:
            return True
        if lowered in {"false", "0", "no", "inactive", "disabled"}:
            return False
    status = _safe_str(item.get("status"))
    if status and status.lower() in {"inactive", "disabled", "archived"}:
        return False
    return True


def _normalize_user(item: Dict[str, Any], sync_run_id: str, ingested_at: datetime) -> Optional[Dict[str, Any]]:
    user_id = _safe_str(
        item.get("id")
        or item.get("_id")
        or item.get("userId")
        or item.get("user_id")
        or (item.get("user") or {}).get("id")
    )
    if not user_id:
        return None

    full_name = _safe_str(item.get("name") or item.get("fullName") or item.get("full_name"))
    if not full_name:
        first_name = _safe_str(item.get("firstName") or item.get("first_name"))
        last_name = _safe_str(item.get("lastName") or item.get("last_name"))
        if first_name or last_name:
            full_name = " ".join([x for x in [first_name, last_name] if x])

    email = _normalize_email(item.get("email") or item.get("userEmail") or item.get("user_email"))
    role = _pick_role(item.get("role") or item.get("userRole") or item.get("user_role"))
    team = _pick_team(item.get("team") or item.get("teamName") or item.get("team_name"))
    is_active = _pick_active(item)

    return {
        "user_id": user_id,
        "name": full_name,
        "email": email,
        "role": role,
        "team": team,
        "is_active": is_active,
        "payload_json": item,
        "source": "gohighlevel.users",
        "sync_run_id": sync_run_id,
        "ingested_at": ingested_at.isoformat(),
    }


def fetch_users_snapshot(sync_run_id: str, ingested_at: datetime) -> List[Dict[str, Any]]:
    last_error: Optional[Exception] = None
    for endpoint in _candidate_endpoints():
        try:
            by_user_id: Dict[str, Dict[str, Any]] = {}
            page = 1
            cursor: Optional[str] = None
            seen_cursors: set[str] = set()

            for _ in range(GHL_USERS_MAX_PAGES):
                params: Dict[str, Any] = {
                    GHL_LOCATION_PARAM: GHL_LOCATION_ID,
                }
                if GHL_USERS_INCLUDE_LIMIT:
                    params["limit"] = GHL_USERS_PAGE_LIMIT
                if cursor:
                    params[GHL_USERS_CURSOR_PARAM] = cursor
                elif page > 1:
                    params[GHL_USERS_PAGE_PARAM] = page

                try:
                    payload = _request_json(endpoint=endpoint, payload_params=params)
                except ApiResponseError as exc:
                    # Some GHL /users variants reject pagination query params even when
                    # the first page succeeds. Preserve harvested users and stop paging.
                    if exc.status_code == 422 and by_user_id:
                        break
                    raise
                items = _extract_items(payload)

                for item in items:
                    normalized = _normalize_user(item=item, sync_run_id=sync_run_id, ingested_at=ingested_at)
                    if normalized is None:
                        continue
                    by_user_id[normalized["user_id"]] = normalized

                if not items:
                    break

                next_cursor = _extract_next_cursor(payload=payload, items=items)
                if next_cursor and next_cursor not in seen_cursors:
                    seen_cursors.add(next_cursor)
                    cursor = next_cursor
                    continue

                if len(items) >= GHL_USERS_PAGE_LIMIT and not cursor:
                    page += 1
                    continue

                break

            users = list(by_user_id.values())
            if users:
                return users
        except ApiResponseError as exc:
            last_error = exc
            if exc.status_code in {404, 405}:
                continue
            raise
        except Exception as exc:  # pragma: no cover
            last_error = exc
            continue

    if last_error is not None:
        raise RuntimeError(f"Failed to fetch GHL users snapshot: {last_error}") from last_error
    raise RuntimeError("Failed to fetch GHL users snapshot: no usable endpoint.")


def _query(
    sql: str,
    params: Optional[List[bigquery.ScalarQueryParameter]] = None,
) -> bigquery.table.RowIterator:
    config = bigquery.QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=config).result()


def ensure_identity_tables() -> None:
    _query(f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{OPS_DATASET}`")

    _query(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_ref(RAW_DATASET, RAW_USERS_STAGING_TABLE)} (
          user_id STRING NOT NULL,
          name STRING,
          email STRING,
          role STRING,
          team STRING,
          is_active BOOL,
          payload_json JSON NOT NULL,
          source STRING NOT NULL,
          sync_run_id STRING NOT NULL,
          ingested_at TIMESTAMP NOT NULL
        )
        """
    )

    _query(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_ref(RAW_DATASET, RAW_USERS_HISTORY_TABLE)} (
          user_id STRING NOT NULL,
          name STRING,
          email STRING,
          role STRING,
          team STRING,
          is_active BOOL,
          payload_json JSON NOT NULL,
          source STRING NOT NULL,
          sync_run_id STRING NOT NULL,
          ingested_at TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(ingested_at)
        CLUSTER BY user_id
        """
    )

    _query(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} (
          ghl_user_id STRING NOT NULL,
          name STRING,
          email STRING,
          role STRING,
          team STRING,
          is_active BOOL NOT NULL,
          row_hash INT64 NOT NULL,
          valid_from TIMESTAMP NOT NULL,
          valid_to TIMESTAMP NOT NULL,
          is_current BOOL NOT NULL,
          first_seen_at TIMESTAMP NOT NULL,
          last_seen_at TIMESTAMP NOT NULL,
          source_sync_run_id STRING NOT NULL,
          updated_at TIMESTAMP NOT NULL
        )
        CLUSTER BY ghl_user_id, is_current
        """
    )

    _query(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_ref(OPS_DATASET, OPS_SYNC_STATE_TABLE)} (
          pipeline_name STRING NOT NULL,
          sync_run_id STRING NOT NULL,
          status STRING NOT NULL,
          started_at TIMESTAMP NOT NULL,
          finished_at TIMESTAMP,
          records_processed INT64,
          detail STRING,
          updated_at TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(started_at)
        CLUSTER BY pipeline_name, status
        """
    )

    _query(
        f"""
        CREATE TABLE IF NOT EXISTS {_table_ref(OPS_DATASET, OPS_SETTER_OVERRIDES_TABLE)} (
          team_member_key STRING,
          ghl_user_id STRING,
          override_name STRING,
          override_email STRING,
          is_active BOOL,
          note STRING,
          updated_at TIMESTAMP
        )
        """
    )


def upsert_sync_state(
    sync_run_id: str,
    status: str,
    started_at: datetime,
    finished_at: Optional[datetime] = None,
    records_processed: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    _query(
        f"""
        MERGE {_table_ref(OPS_DATASET, OPS_SYNC_STATE_TABLE)} T
        USING (
          SELECT
            @pipeline_name AS pipeline_name,
            @sync_run_id AS sync_run_id
        ) S
        ON T.pipeline_name = S.pipeline_name
        AND T.sync_run_id = S.sync_run_id
        WHEN MATCHED THEN
          UPDATE SET
            status = @status,
            started_at = @started_at,
            finished_at = @finished_at,
            records_processed = @records_processed,
            detail = @detail,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (
            pipeline_name,
            sync_run_id,
            status,
            started_at,
            finished_at,
            records_processed,
            detail,
            updated_at
          )
          VALUES (
            @pipeline_name,
            @sync_run_id,
            @status,
            @started_at,
            @finished_at,
            @records_processed,
            @detail,
            CURRENT_TIMESTAMP()
          )
        """,
        params=[
            bigquery.ScalarQueryParameter("pipeline_name", "STRING", IDENTITY_PIPELINE_NAME),
            bigquery.ScalarQueryParameter("sync_run_id", "STRING", sync_run_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
            bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", finished_at),
            bigquery.ScalarQueryParameter("records_processed", "INT64", records_processed),
            bigquery.ScalarQueryParameter("detail", "STRING", detail),
        ],
    )


def load_users_snapshot(users: List[Dict[str, Any]]) -> None:
    staging_table = f"{PROJECT_ID}.{RAW_DATASET}.{RAW_USERS_STAGING_TABLE}"
    schema = [
        bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("role", "STRING"),
        bigquery.SchemaField("team", "STRING"),
        bigquery.SchemaField("is_active", "BOOL"),
        bigquery.SchemaField("payload_json", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sync_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_json(users, staging_table, job_config=config)
    load_job.result()


def append_history_from_staging(sync_run_id: str) -> None:
    _query(
        f"""
        MERGE {_table_ref(RAW_DATASET, RAW_USERS_HISTORY_TABLE)} T
        USING (
          SELECT
            user_id,
            name,
            email,
            role,
            team,
            is_active,
            payload_json,
            source,
            sync_run_id,
            ingested_at
          FROM {_table_ref(RAW_DATASET, RAW_USERS_STAGING_TABLE)}
          WHERE sync_run_id = @sync_run_id
        ) S
        ON T.sync_run_id = S.sync_run_id
        AND T.user_id = S.user_id
        WHEN MATCHED THEN
          UPDATE SET
            name = S.name,
            email = S.email,
            role = S.role,
            team = S.team,
            is_active = S.is_active,
            payload_json = S.payload_json,
            source = S.source,
            ingested_at = S.ingested_at
        WHEN NOT MATCHED THEN
          INSERT (
            user_id,
            name,
            email,
            role,
            team,
            is_active,
            payload_json,
            source,
            sync_run_id,
            ingested_at
          )
          VALUES (
            S.user_id,
            S.name,
            S.email,
            S.role,
            S.team,
            S.is_active,
            S.payload_json,
            S.source,
            S.sync_run_id,
            S.ingested_at
          )
        """,
        params=[bigquery.ScalarQueryParameter("sync_run_id", "STRING", sync_run_id)],
    )


def merge_users_scd2(sync_run_id: str, run_ts: datetime) -> None:
    source_select = f"""
        SELECT
          s.user_id AS ghl_user_id,
          NULLIF(TRIM(s.name), '') AS name,
          NULLIF(TRIM(LOWER(s.email)), '') AS email,
          NULLIF(TRIM(s.role), '') AS role,
          NULLIF(TRIM(s.team), '') AS team,
          COALESCE(s.is_active, TRUE) AS is_active,
          FARM_FINGERPRINT(
            CONCAT(
              IFNULL(NULLIF(TRIM(s.name), ''), ''),
              '|',
              IFNULL(NULLIF(TRIM(LOWER(s.email)), ''), ''),
              '|',
              IFNULL(NULLIF(TRIM(s.role), ''), ''),
              '|',
              IFNULL(NULLIF(TRIM(s.team), ''), ''),
              '|',
              CAST(COALESCE(s.is_active, TRUE) AS STRING)
            )
          ) AS row_hash
        FROM {_table_ref(RAW_DATASET, RAW_USERS_STAGING_TABLE)} s
        WHERE s.sync_run_id = @sync_run_id
        AND s.user_id IS NOT NULL
    """

    _query(
        f"""
        UPDATE {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} t
        SET
          valid_to = @run_ts,
          is_current = FALSE,
          last_seen_at = @run_ts,
          source_sync_run_id = @sync_run_id,
          updated_at = CURRENT_TIMESTAMP()
        FROM ({source_select}) src
        WHERE t.ghl_user_id = src.ghl_user_id
          AND t.is_current = TRUE
          AND t.row_hash != src.row_hash
        """,
        params=[
            bigquery.ScalarQueryParameter("sync_run_id", "STRING", sync_run_id),
            bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
        ],
    )

    _query(
        f"""
        INSERT INTO {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} (
          ghl_user_id,
          name,
          email,
          role,
          team,
          is_active,
          row_hash,
          valid_from,
          valid_to,
          is_current,
          first_seen_at,
          last_seen_at,
          source_sync_run_id,
          updated_at
        )
        SELECT
          src.ghl_user_id,
          src.name,
          src.email,
          src.role,
          src.team,
          src.is_active,
          src.row_hash,
          @run_ts AS valid_from,
          TIMESTAMP('9999-12-31 00:00:00+00') AS valid_to,
          TRUE AS is_current,
          @run_ts AS first_seen_at,
          @run_ts AS last_seen_at,
          @sync_run_id AS source_sync_run_id,
          CURRENT_TIMESTAMP() AS updated_at
        FROM ({source_select}) src
        LEFT JOIN {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} t
          ON t.ghl_user_id = src.ghl_user_id
         AND t.is_current = TRUE
        WHERE t.ghl_user_id IS NULL
           OR t.row_hash != src.row_hash
        """,
        params=[
            bigquery.ScalarQueryParameter("sync_run_id", "STRING", sync_run_id),
            bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
        ],
    )

    _query(
        f"""
        UPDATE {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} t
        SET
          last_seen_at = @run_ts,
          source_sync_run_id = @sync_run_id,
          updated_at = CURRENT_TIMESTAMP()
        FROM ({source_select}) src
        WHERE t.ghl_user_id = src.ghl_user_id
          AND t.is_current = TRUE
          AND t.row_hash = src.row_hash
        """,
        params=[
            bigquery.ScalarQueryParameter("sync_run_id", "STRING", sync_run_id),
            bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
        ],
    )


def refresh_setter_identity_bridge() -> None:
    _query(
        f"""
        CREATE OR REPLACE TABLE {_table_ref(MARTS_DATASET, MART_SETTER_BRIDGE_TABLE)} AS
        WITH speed_keys AS (
          SELECT DISTINCT
            setter_team_member_key AS team_member_key
          FROM {_table_ref(MARTS_DATASET, 'fct_speed_to_lead')}
          WHERE NULLIF(TRIM(setter_team_member_key), '') IS NOT NULL
        ),
        attribution_user_ids AS (
          SELECT DISTINCT LOWER(TRIM(assigned_to_user_id)) AS ghl_user_id
          FROM {_table_ref(CORE_DATASET, 'fct_ghl_conversations')}
          WHERE NULLIF(TRIM(assigned_to_user_id), '') IS NOT NULL

          UNION DISTINCT

          SELECT DISTINCT LOWER(TRIM(assigned_to_user_id)) AS ghl_user_id
          FROM {_table_ref(CORE_DATASET, 'fct_ghl_opportunities')}
          WHERE NULLIF(TRIM(assigned_to_user_id), '') IS NOT NULL

          UNION DISTINCT

          SELECT DISTINCT LOWER(TRIM(owner_id)) AS ghl_user_id
          FROM {_table_ref(CORE_DATASET, 'fct_ghl_tasks')}
          WHERE NULLIF(TRIM(owner_id), '') IS NOT NULL

          UNION DISTINCT

          SELECT DISTINCT LOWER(TRIM(author_user_id)) AS ghl_user_id
          FROM {_table_ref(CORE_DATASET, 'fct_ghl_notes')}
          WHERE NULLIF(TRIM(author_user_id), '') IS NOT NULL

          UNION DISTINCT

          SELECT DISTINCT LOWER(TRIM(assigned_to_user_id)) AS ghl_user_id
          FROM {_table_ref(CORE_DATASET, 'dim_ghl_contacts')}
          WHERE NULLIF(TRIM(assigned_to_user_id), '') IS NOT NULL
        ),
        direct_user_keys AS (
          SELECT DISTINCT
            CONCAT('ghl_user:', ghl_user_id) AS team_member_key
          FROM attribution_user_ids
        ),
        seed_keys AS (
          SELECT team_member_key FROM speed_keys
          UNION DISTINCT
          SELECT team_member_key FROM direct_user_keys
        ),
        team_members AS (
          SELECT
            tm.team_member_key,
            tm.ghl_user_id,
            tm.display_name,
            tm.email,
            tm.seen_in_ghl_call,
            tm.seen_in_ghl_sms,
            tm.seen_in_fathom_call
          FROM {_table_ref(MARTS_DATASET, 'dim_team_members')} tm
        ),
        base AS (
          SELECT DISTINCT
            k.team_member_key,
            COALESCE(
              tm.ghl_user_id,
              REGEXP_EXTRACT(k.team_member_key, r'^ghl_user:(.+)$')
            ) AS ghl_user_id,
            tm.display_name AS payload_display_name,
            tm.email AS payload_email,
            tm.seen_in_ghl_call,
            tm.seen_in_ghl_sms,
            tm.seen_in_fathom_call
          FROM seed_keys k
          LEFT JOIN team_members tm
            ON tm.team_member_key = k.team_member_key
        ),
        overrides AS (
          SELECT
            COALESCE(
              NULLIF(TRIM(team_member_key), ''),
              CONCAT('ghl_user:', LOWER(NULLIF(TRIM(ghl_user_id), '')))
            ) AS team_member_key,
            NULLIF(TRIM(ghl_user_id), '') AS override_ghl_user_id,
            NULLIF(TRIM(override_name), '') AS override_name,
            NULLIF(TRIM(LOWER(override_email)), '') AS override_email
          FROM {_table_ref(OPS_DATASET, OPS_SETTER_OVERRIDES_TABLE)}
          WHERE COALESCE(is_active, TRUE)
        ),
        users_current AS (
          SELECT
            u.ghl_user_id,
            u.name,
            u.email,
            u.role,
            u.team,
            u.is_active
          FROM {_table_ref(CORE_DATASET, CORE_USERS_SCD2_TABLE)} u
          WHERE u.is_current = TRUE
        ),
        resolved AS (
          SELECT
            b.team_member_key,
            COALESCE(o.override_ghl_user_id, b.ghl_user_id) AS ghl_user_id,
            o.override_name,
            o.override_email,
            u.name AS api_name,
            u.email AS api_email,
            u.role AS api_role,
            u.team AS api_team,
            u.is_active AS api_is_active,
            b.payload_display_name,
            b.payload_email,
            b.seen_in_ghl_call,
            b.seen_in_ghl_sms,
            b.seen_in_fathom_call
          FROM base b
          LEFT JOIN overrides o
            ON LOWER(o.team_member_key) = LOWER(b.team_member_key)
          LEFT JOIN users_current u
            ON LOWER(u.ghl_user_id) = LOWER(COALESCE(o.override_ghl_user_id, b.ghl_user_id))
        )
        SELECT
          r.team_member_key,
          r.ghl_user_id,
          COALESCE(r.override_name, r.api_name, r.payload_display_name) AS canonical_name,
          COALESCE(r.override_email, r.api_email, r.payload_email) AS canonical_email,
          COALESCE(
            NULLIF(COALESCE(r.override_name, r.api_name, r.payload_display_name), ''),
            NULLIF(COALESCE(r.override_email, r.api_email, r.payload_email), ''),
            @unknown_setter_label
          ) AS setter_display_label,
          (
            NULLIF(COALESCE(r.override_name, r.api_name, r.payload_display_name), '') IS NOT NULL
            OR NULLIF(COALESCE(r.override_email, r.api_email, r.payload_email), '') IS NOT NULL
          ) AS is_identity_resolved,
          r.api_role AS canonical_role,
          r.api_team AS canonical_team,
          r.api_is_active AS canonical_is_active,
          CASE
            WHEN r.override_name IS NOT NULL OR r.override_email IS NOT NULL THEN 'manual_override'
            WHEN r.api_name IS NOT NULL OR r.api_email IS NOT NULL THEN 'ghl_users_api'
            WHEN r.payload_display_name IS NOT NULL OR r.payload_email IS NOT NULL THEN 'event_payload'
            WHEN r.ghl_user_id IS NOT NULL THEN 'id_only'
            ELSE 'unknown'
          END AS identity_source,
          COALESCE(r.seen_in_ghl_call, FALSE) AS seen_in_ghl_call,
          COALESCE(r.seen_in_ghl_sms, FALSE) AS seen_in_ghl_sms,
          COALESCE(r.seen_in_fathom_call, FALSE) AS seen_in_fathom_call,
          CURRENT_TIMESTAMP() AS bridge_refreshed_at
        FROM resolved r
        """,
        params=[
            bigquery.ScalarQueryParameter("unknown_setter_label", "STRING", UNKNOWN_SETTER_LABEL),
        ],
    )


def refresh_speed_to_lead_enriched_view() -> None:
    unknown_label_literal = UNKNOWN_SETTER_LABEL.replace("\\", "\\\\").replace("'", "\\'")
    _query(
        f"""
        CREATE OR REPLACE VIEW {_table_ref(MARTS_DATASET, MART_SPEED_TO_LEAD_ENRICHED_VIEW)} AS
        SELECT
          f.*,
          COALESCE(
            NULLIF(TRIM(b.canonical_name), ''),
            NULLIF(TRIM(f.setter_user_name_raw), ''),
            NULLIF(TRIM(f.setter_dim_display_name), '')
          ) AS setter_name_enriched,
          COALESCE(
            NULLIF(TRIM(LOWER(b.canonical_email)), ''),
            NULLIF(TRIM(LOWER(f.setter_user_email_raw)), ''),
            NULLIF(TRIM(LOWER(f.setter_dim_email)), '')
          ) AS setter_email_enriched,
          CASE
            WHEN COALESCE(
              NULLIF(TRIM(b.canonical_name), ''),
              NULLIF(TRIM(f.setter_user_name_raw), ''),
              NULLIF(TRIM(f.setter_dim_display_name), '')
            ) IS NOT NULL THEN COALESCE(
              NULLIF(TRIM(b.canonical_name), ''),
              NULLIF(TRIM(f.setter_user_name_raw), ''),
              NULLIF(TRIM(f.setter_dim_display_name), '')
            )
            WHEN COALESCE(
              NULLIF(TRIM(LOWER(b.canonical_email)), ''),
              NULLIF(TRIM(LOWER(f.setter_user_email_raw)), ''),
              NULLIF(TRIM(LOWER(f.setter_dim_email)), '')
            ) IS NOT NULL THEN COALESCE(
              NULLIF(TRIM(LOWER(b.canonical_email)), ''),
              NULLIF(TRIM(LOWER(f.setter_user_email_raw)), ''),
              NULLIF(TRIM(LOWER(f.setter_dim_email)), '')
            )
            ELSE '{unknown_label_literal}'
          END AS setter_display_label_enriched,
          COALESCE(b.identity_source, 'unknown') AS setter_identity_source,
          COALESCE(b.is_identity_resolved, FALSE) AS setter_identity_resolved
        FROM {_table_ref(MARTS_DATASET, 'fct_speed_to_lead')} f
        LEFT JOIN {_table_ref(MARTS_DATASET, MART_SETTER_BRIDGE_TABLE)} b
          ON LOWER(b.team_member_key) = LOWER(f.setter_team_member_key)
        """
    )


def refresh_setter_identity_observability_tables() -> None:
    _query(
        f"""
        CREATE OR REPLACE TABLE {_table_ref(MARTS_DATASET, MART_SETTER_UNKNOWN_QUEUE_TABLE)} AS
        WITH unresolved_touched AS (
          SELECT
            f.setter_team_member_key,
            NULLIF(TRIM(REGEXP_EXTRACT(f.setter_team_member_key, r'^ghl_user:(.+)$')), '') AS team_key_user_id_hint,
            NULLIF(TRIM(f.setter_user_id_raw), '') AS setter_user_id_raw,
            NULLIF(TRIM(f.setter_user_name_raw), '') AS setter_user_name_raw,
            NULLIF(TRIM(LOWER(f.setter_user_email_raw)), '') AS setter_user_email_raw,
            NULLIF(TRIM(f.setter_dim_display_name), '') AS setter_dim_display_name,
            NULLIF(TRIM(LOWER(f.setter_dim_email)), '') AS setter_dim_email,
            f.trigger_type,
            f.first_touch_channel,
            f.trigger_ts
          FROM {_table_ref(MARTS_DATASET, 'fct_speed_to_lead')} f
          LEFT JOIN {_table_ref(MARTS_DATASET, MART_SETTER_BRIDGE_TABLE)} b
            ON LOWER(b.team_member_key) = LOWER(f.setter_team_member_key)
          WHERE f.first_touch_ts IS NOT NULL
            AND COALESCE(b.is_identity_resolved, FALSE) = FALSE
            AND f.setter_team_member_key IS NOT NULL
        )
        SELECT
          setter_team_member_key,
          COALESCE(team_key_user_id_hint, setter_user_id_raw) AS unresolved_ghl_user_id,
          COUNT(*) AS touched_trigger_count,
          COUNTIF(trigger_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS touched_last_7d_count,
          COUNTIF(trigger_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)) AS touched_last_30d_count,
          MIN(trigger_ts) AS first_seen_trigger_ts,
          MAX(trigger_ts) AS last_seen_trigger_ts,
          ARRAY_AGG(DISTINCT trigger_type IGNORE NULLS LIMIT 5) AS trigger_types_seen,
          ARRAY_AGG(DISTINCT first_touch_channel IGNORE NULLS LIMIT 5) AS channels_seen,
          COALESCE(
            MAX_BY(setter_user_name_raw, trigger_ts),
            MAX_BY(setter_dim_display_name, trigger_ts)
          ) AS best_name_hint,
          COALESCE(
            MAX_BY(setter_user_email_raw, trigger_ts),
            MAX_BY(setter_dim_email, trigger_ts)
          ) AS best_email_hint,
          CURRENT_TIMESTAMP() AS queue_refreshed_at
        FROM unresolved_touched
        GROUP BY 1, 2
        ORDER BY touched_last_30d_count DESC, touched_trigger_count DESC, last_seen_trigger_ts DESC
        """
    )

    _query(
        f"""
        CREATE OR REPLACE TABLE {_table_ref(MARTS_DATASET, MART_SETTER_COVERAGE_DAILY_TABLE)} AS
        WITH base AS (
          SELECT
            DATE(f.trigger_ts) AS trigger_date,
            f.trigger_type,
            f.first_touch_ts IS NOT NULL AS is_touched,
            COALESCE(b.is_identity_resolved, FALSE) AS is_identity_resolved
          FROM {_table_ref(MARTS_DATASET, 'fct_speed_to_lead')} f
          LEFT JOIN {_table_ref(MARTS_DATASET, MART_SETTER_BRIDGE_TABLE)} b
            ON LOWER(b.team_member_key) = LOWER(f.setter_team_member_key)
          WHERE f.trigger_ts IS NOT NULL
        ),
        by_type AS (
          SELECT
            trigger_date,
            trigger_type AS coverage_scope,
            COUNT(*) AS total_trigger_count,
            COUNTIF(is_touched) AS touched_trigger_count,
            COUNTIF(is_touched AND is_identity_resolved) AS touched_resolved_count,
            COUNTIF(is_touched AND NOT is_identity_resolved) AS touched_unresolved_count
          FROM base
          GROUP BY 1, 2
        ),
        overall AS (
          SELECT
            trigger_date,
            'all' AS coverage_scope,
            COUNT(*) AS total_trigger_count,
            COUNTIF(is_touched) AS touched_trigger_count,
            COUNTIF(is_touched AND is_identity_resolved) AS touched_resolved_count,
            COUNTIF(is_touched AND NOT is_identity_resolved) AS touched_unresolved_count
          FROM base
          GROUP BY 1
        ),
        combined AS (
          SELECT * FROM by_type
          UNION ALL
          SELECT * FROM overall
        )
        SELECT
          trigger_date,
          coverage_scope,
          total_trigger_count,
          touched_trigger_count,
          touched_resolved_count,
          touched_unresolved_count,
          ROUND(
            100 * SAFE_DIVIDE(touched_resolved_count, NULLIF(touched_trigger_count, 0)),
            2
          ) AS touched_resolved_pct,
          CURRENT_TIMESTAMP() AS coverage_refreshed_at
        FROM combined
        ORDER BY trigger_date DESC, coverage_scope
        """
    )


def refresh_unified_dashboard_schema_view() -> None:
    _query(
        f"""
        CREATE OR REPLACE VIEW {_table_ref(MARTS_DATASET, MART_UNIFIED_DASHBOARD_SCHEMA_VIEW)} AS
        SELECT
          'setter_identity_unknown_queue' AS schema_source,
          'unknown_queue' AS schema_record_type,
          COALESCE(DATE(last_seen_trigger_ts), DATE(queue_refreshed_at)) AS report_date,
          queue_refreshed_at AS refreshed_at,
          'unresolved_queue' AS coverage_scope,
          setter_team_member_key,
          unresolved_ghl_user_id,
          best_name_hint AS setter_name_hint,
          best_email_hint AS setter_email_hint,
          ARRAY_TO_STRING(trigger_types_seen, ', ') AS trigger_types_seen,
          ARRAY_TO_STRING(channels_seen, ', ') AS channels_seen,
          CAST(NULL AS INT64) AS total_trigger_count,
          touched_trigger_count,
          CAST(0 AS INT64) AS touched_resolved_count,
          touched_trigger_count AS touched_unresolved_count,
          CAST(0 AS FLOAT64) AS touched_resolved_pct,
          touched_last_7d_count,
          touched_last_30d_count,
          first_seen_trigger_ts,
          last_seen_trigger_ts
        FROM {_table_ref(MARTS_DATASET, MART_SETTER_UNKNOWN_QUEUE_TABLE)}

        UNION ALL

        SELECT
          'setter_identity_coverage_daily' AS schema_source,
          'coverage_daily' AS schema_record_type,
          trigger_date AS report_date,
          coverage_refreshed_at AS refreshed_at,
          coverage_scope,
          CAST(NULL AS STRING) AS setter_team_member_key,
          CAST(NULL AS STRING) AS unresolved_ghl_user_id,
          CAST(NULL AS STRING) AS setter_name_hint,
          CAST(NULL AS STRING) AS setter_email_hint,
          CAST(NULL AS STRING) AS trigger_types_seen,
          CAST(NULL AS STRING) AS channels_seen,
          total_trigger_count,
          touched_trigger_count,
          touched_resolved_count,
          touched_unresolved_count,
          SAFE_CAST(touched_resolved_pct AS FLOAT64) AS touched_resolved_pct,
          CAST(NULL AS INT64) AS touched_last_7d_count,
          CAST(NULL AS INT64) AS touched_last_30d_count,
          CAST(NULL AS TIMESTAMP) AS first_seen_trigger_ts,
          CAST(NULL AS TIMESTAMP) AS last_seen_trigger_ts
        FROM {_table_ref(MARTS_DATASET, MART_SETTER_COVERAGE_DAILY_TABLE)}
        """
    )


def run_identity_dq_gate() -> Dict[str, Any]:
    rows = list(
        _query(
            f"""
            WITH touched AS (
              SELECT
                f.trigger_event_id,
                COALESCE(b.is_identity_resolved, FALSE) AS is_identity_resolved
              FROM {_table_ref(MARTS_DATASET, 'fct_speed_to_lead')} f
              LEFT JOIN {_table_ref(MARTS_DATASET, MART_SETTER_BRIDGE_TABLE)} b
                ON b.team_member_key = f.setter_team_member_key
              WHERE f.first_touch_ts IS NOT NULL
                AND f.trigger_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
            )
            SELECT
              COUNT(*) AS touched_trigger_count,
              COUNTIF(NOT is_identity_resolved) AS unknown_touched_count,
              SAFE_DIVIDE(
                COUNTIF(is_identity_resolved),
                NULLIF(COUNT(*), 0)
              ) AS touched_coverage_ratio
            FROM touched
            """,
            params=[
                bigquery.ScalarQueryParameter("lookback_days", "INT64", IDENTITY_DQ_LOOKBACK_DAYS),
            ],
        )
    )
    row = rows[0]
    touched_count = int(row["touched_trigger_count"] or 0)
    unknown_count = int(row["unknown_touched_count"] or 0)
    coverage = float(row["touched_coverage_ratio"] or 0.0) if touched_count > 0 else 1.0
    return {
        "touched_trigger_count": touched_count,
        "unknown_touched_count": unknown_count,
        "touched_coverage_ratio": coverage,
        "min_required_coverage_ratio": IDENTITY_DQ_MIN_TOUCHED_COVERAGE,
        "pass": coverage >= IDENTITY_DQ_MIN_TOUCHED_COVERAGE,
    }


def _post_alert(text: str, payload: Dict[str, Any]) -> None:
    if not ALERT_WEBHOOK_URL:
        return
    try:
        requests.post(
            ALERT_WEBHOOK_URL,
            json={
                "text": text,
                "pipeline": IDENTITY_PIPELINE_NAME,
                "payload": payload,
            },
            timeout=15,
        )
    except requests.RequestException:
        # Alerting should not shadow the root pipeline error.
        pass


def run_identity_resolution_pipeline() -> Dict[str, Any]:
    _require_runtime_config()
    ensure_identity_tables()

    sync_run_id = f"ghl-identity-{_utc_now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    started_at = _utc_now()
    upsert_sync_state(
        sync_run_id=sync_run_id,
        status="RUNNING",
        started_at=started_at,
        detail="starting",
    )

    try:
        users = fetch_users_snapshot(sync_run_id=sync_run_id, ingested_at=started_at)
        load_users_snapshot(users=users)
        append_history_from_staging(sync_run_id=sync_run_id)
        merge_users_scd2(sync_run_id=sync_run_id, run_ts=started_at)
        refresh_setter_identity_bridge()
        refresh_speed_to_lead_enriched_view()
        refresh_setter_identity_observability_tables()
        refresh_unified_dashboard_schema_view()
        dq = run_identity_dq_gate()

        if not dq["pass"]:
            raise RuntimeError(
                "Identity DQ gate failed: "
                f"touched_coverage_ratio={dq['touched_coverage_ratio']:.4f} "
                f"threshold={dq['min_required_coverage_ratio']:.4f}"
            )

        finished_at = _utc_now()
        summary = {
            "sync_run_id": sync_run_id,
            "users_fetched": len(users),
            "dq": dq,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": int((finished_at - started_at).total_seconds()),
        }
        upsert_sync_state(
            sync_run_id=sync_run_id,
            status="SUCCESS",
            started_at=started_at,
            finished_at=finished_at,
            records_processed=len(users),
            detail=json.dumps(summary, ensure_ascii=True),
        )
        return summary
    except Exception as exc:
        finished_at = _utc_now()
        detail = str(exc)
        upsert_sync_state(
            sync_run_id=sync_run_id,
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            records_processed=None,
            detail=detail[:1000],
        )
        _post_alert(
            text=f"[{IDENTITY_PIPELINE_NAME}] FAILED: {detail}",
            payload={
                "sync_run_id": sync_run_id,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "error": detail,
            },
        )
        raise


def lookup_ghl_phone_numbers(
    location_id: str = "yDDvavWJesa03Cv3wKjt",
    company_id: str = "oUXHwWLAMHoOyX6xpVOS",
    target_user_id: str = "Oct5Tz6ZVUaDkqXC3yHL",
) -> Dict[str, Any]:
    """Find deleted GHL user via company-scoped search, printing all users with deleted status."""
    endpoint = f"/users/search?companyId={company_id}&locationId={location_id}&limit=100"
    auth_value = _auth_values()[0] if _auth_values() else ""
    headers = {
        "Authorization": auth_value,
        "Version": GHL_API_VERSION,
        "Accept": "application/json",
    }
    resp = requests.get(
        f"{GHL_API_BASE.rstrip('/')}{endpoint}",
        headers=headers,
        timeout=GHL_USERS_TIMEOUT_SEC,
    )
    print(f"Status: {resp.status_code}, total body length: {len(resp.text)}")
    if resp.status_code >= 400:
        print(f"Error: {resp.text[:1000]}")
        return {}
    data = resp.json()
    users = data.get("users", [])
    print(f"Total users returned: {len(users)}")
    # Print all users with id, name, email, deleted status
    for u in users:
        uid = u.get("id", "")
        name = u.get("name", "")
        email = u.get("email", "")
        deleted = u.get("deleted", False)
        print(f"  id={uid} name={name!r} email={email!r} deleted={deleted}")
    # Check if target is in the list
    match = next((u for u in users if u.get("id", "").lower() == target_user_id.lower()), None)
    if match:
        print(f"\nTARGET FOUND: {json.dumps(match, indent=2)}")
    else:
        print(f"\nTarget user {target_user_id} NOT found in this response.")
    return data


def lookup_ghl_user_by_id(user_id: str = "Oct5Tz6ZVUaDkqXC3yHL") -> Dict[str, Any]:
    """Direct GHL user lookup by ID. Prints and returns the API response."""
    endpoint = f"/users/{user_id}"
    for auth_value in _auth_values():
        headers = {
            "Authorization": auth_value,
            "Version": GHL_API_VERSION,
            "Accept": "application/json",
        }
        resp = requests.get(
            f"{GHL_API_BASE.rstrip('/')}{endpoint}",
            headers=headers,
            timeout=GHL_USERS_TIMEOUT_SEC,
        )
        print(f"Status: {resp.status_code}")
        print(f"Body: {resp.text[:2000]}")
        if resp.status_code < 400:
            return resp.json()
    return {}
