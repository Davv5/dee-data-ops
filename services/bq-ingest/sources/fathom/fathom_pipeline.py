import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")

FATHOM_API_BASE = os.getenv("FATHOM_API_BASE", "https://api.fathom.ai/external/v1")
FATHOM_API_KEY = os.getenv("FATHOM_API_KEY")
FATHOM_API_KEY_HEADER = os.getenv("FATHOM_API_KEY_HEADER", "X-Api-Key")
FATHOM_WORKSPACE_ID = os.getenv("FATHOM_WORKSPACE_ID", "default")

FATHOM_PAGE_LIMIT = int(os.getenv("FATHOM_PAGE_LIMIT", "100"))
FATHOM_CURSOR_PARAM = os.getenv("FATHOM_CURSOR_PARAM", "cursor")

FATHOM_RAW_TABLE = os.getenv("BQ_FATHOM_RAW_TABLE", "fathom_calls_raw")
FATHOM_STATE_TABLE = os.getenv("BQ_FATHOM_STATE_TABLE", "fathom_backfill_state")

FATHOM_OBJECT_TYPES = os.getenv("FATHOM_OBJECT_TYPES", "calls")
FATHOM_INCREMENTAL_LOOKBACK_HOURS = int(os.getenv("FATHOM_INCREMENTAL_LOOKBACK_HOURS", "2"))
FATHOM_INCREMENTAL_MAX_PAGES_PER_OBJECT = int(os.getenv("FATHOM_INCREMENTAL_MAX_PAGES_PER_OBJECT", "3"))

FATHOM_INCLUDE_SUMMARY = os.getenv("FATHOM_INCLUDE_SUMMARY", "false").lower() == "true"
FATHOM_INCLUDE_ACTION_ITEMS = os.getenv("FATHOM_INCLUDE_ACTION_ITEMS", "false").lower() == "true"
FATHOM_INCLUDE_CRM_MATCHES = os.getenv("FATHOM_INCLUDE_CRM_MATCHES", "false").lower() == "true"
FATHOM_INCLUDE_TRANSCRIPT = os.getenv("FATHOM_INCLUDE_TRANSCRIPT", "false").lower() == "true"

FATHOM_INTERNAL_EMAIL_DOMAINS = {
    d.strip().lower()
    for d in os.getenv("FATHOM_INTERNAL_EMAIL_DOMAINS", "fanbasis.com").split(",")
    if d.strip()
}
INTERNAL_TITLE_KEYWORDS = [
    token.strip().lower()
    for token in os.getenv(
        "FATHOM_INTERNAL_TITLE_KEYWORDS",
        "team sync,standup,1:1,one on one,all hands,retro,sprint planning,internal",
    ).split(",")
    if token.strip()
]
REVENUE_TITLE_KEYWORDS = [
    token.strip().lower()
    for token in os.getenv(
        "FATHOM_REVENUE_TITLE_KEYWORDS",
        "sales,demo,discovery,proposal,close,renewal,customer,client,prospect,onboarding",
    ).split(",")
    if token.strip()
]

SOURCE_NAME = "fathom"
ENTITY_DEFAULT_ENDPOINTS = {
    "calls": "/meetings",
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
        # Prefer ms if obviously too large for unix seconds.
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
    if not FATHOM_API_KEY:
        raise RuntimeError("Missing FATHOM_API_KEY")


def ensure_tables() -> None:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")

    raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{FATHOM_RAW_TABLE}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      workspace_id STRING NOT NULL,
      team_id STRING,
      title STRING,
      partition_date DATE NOT NULL,
      event_ts TIMESTAMP,
      ended_at_ts TIMESTAMP,
      updated_at_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      external_participant_count INT64,
      is_internal_only BOOL,
      is_revenue_relevant BOOL,
      classification_label STRING,
      classification_confidence FLOAT64,
      classification_reason STRING,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY partition_date
    CLUSTER BY entity_type, workspace_id, entity_id
    """
    client.query(raw_query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{FATHOM_STATE_TABLE}` (
      run_id STRING NOT NULL,
      entity_type STRING NOT NULL,
      workspace_id STRING NOT NULL,
      status STRING NOT NULL,
      next_cursor STRING,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    PARTITION BY DATE(updated_at)
    CLUSTER BY run_id, entity_type, workspace_id
    """
    client.query(state_query).result()


def parse_object_types() -> List[str]:
    return [x.strip() for x in FATHOM_OBJECT_TYPES.split(",") if x.strip()]


def _endpoint_for_entity(entity_type: str) -> str:
    endpoint = os.getenv(
        f"FATHOM_ENDPOINT_{entity_type.upper().replace('-', '_')}",
        ENTITY_DEFAULT_ENDPOINTS.get(entity_type),
    )
    if not endpoint:
        raise ValueError(f"Unsupported Fathom entity type: {entity_type}")
    return endpoint if endpoint.startswith("/") else f"/{endpoint}"


def _build_headers() -> Dict[str, str]:
    return {
        FATHOM_API_KEY_HEADER: FATHOM_API_KEY,
        "Accept": "application/json",
    }


def _extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    value = payload.get("items")
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    value = payload.get("data")
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        nested_items = value.get("items")
        if isinstance(nested_items, list):
            return [x for x in nested_items if isinstance(x, dict)]
    return []


def _extract_next_cursor(payload: Dict[str, Any]) -> Optional[str]:
    for key in ("next_cursor", "nextCursor", "cursor", "nextPageToken"):
        val = payload.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _extract_entity_id(item: Dict[str, Any]) -> Optional[str]:
    for key in ("recording_id", "id", "meeting_id"):
        val = item.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _extract_event_ts(item: Dict[str, Any]) -> Optional[datetime]:
    for key in (
        "recording_start_time",
        "scheduled_start_time",
        "created_at",
        "meeting_start_time",
    ):
        dt = _parse_ts(item.get(key))
        if dt:
            return dt
    return None


def _extract_ended_ts(item: Dict[str, Any]) -> Optional[datetime]:
    for key in ("recording_end_time", "scheduled_end_time", "meeting_end_time"):
        dt = _parse_ts(item.get(key))
        if dt:
            return dt
    return None


def _extract_updated_ts(item: Dict[str, Any]) -> Optional[datetime]:
    for key in ("updated_at", "modified_at", "last_updated", "created_at"):
        dt = _parse_ts(item.get(key))
        if dt:
            return dt
    return None


def _extract_team_id(item: Dict[str, Any]) -> Optional[str]:
    recorded_by = item.get("recorded_by")
    if isinstance(recorded_by, dict):
        for key in ("team", "team_name", "team_id"):
            val = recorded_by.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
    return None


def _extract_title(item: Dict[str, Any]) -> Optional[str]:
    return _safe_str(item.get("meeting_title")) or _safe_str(item.get("title"))


def _domain_from_email(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[1].strip().lower() or None


def _iter_invitees(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    invitees = item.get("calendar_invitees")
    if isinstance(invitees, list):
        return [x for x in invitees if isinstance(x, dict)]
    return []


def _is_external_invitee(invitee: Dict[str, Any]) -> bool:
    if isinstance(invitee.get("is_external"), bool):
        return bool(invitee.get("is_external"))
    domain = _safe_str(invitee.get("email_domain")) or _domain_from_email(_safe_str(invitee.get("email")))
    if not domain:
        return False
    return domain.lower() not in FATHOM_INTERNAL_EMAIL_DOMAINS


def classify_call(item: Dict[str, Any]) -> Dict[str, Any]:
    title = (_extract_title(item) or "").lower()
    invitees = _iter_invitees(item)
    external_count = sum(1 for inv in invitees if _is_external_invitee(inv))

    domains_type = _safe_str(item.get("calendar_invitees_domains_type"))
    meeting_type = _safe_str(item.get("meeting_type"))

    has_internal_keyword = any(token in title for token in INTERNAL_TITLE_KEYWORDS)
    has_revenue_keyword = any(token in title for token in REVENUE_TITLE_KEYWORDS)

    is_internal_only = False
    if domains_type == "only_internal":
        is_internal_only = True
    elif meeting_type and meeting_type.lower() == "internal":
        is_internal_only = True
    elif invitees and external_count == 0:
        is_internal_only = True

    if is_internal_only or has_internal_keyword:
        label = "internal"
        confidence = 0.95 if is_internal_only else 0.75
        reason = "internal_signal"
        is_revenue_relevant = False
    elif has_revenue_keyword:
        label = "sales"
        confidence = 0.85
        reason = "title_keyword"
        is_revenue_relevant = True
    elif external_count > 0:
        label = "external"
        confidence = 0.7
        reason = "external_participants"
        is_revenue_relevant = True
    else:
        label = "other"
        confidence = 0.55
        reason = "fallback"
        is_revenue_relevant = False

    return {
        "external_participant_count": external_count,
        "is_internal_only": is_internal_only,
        "is_revenue_relevant": is_revenue_relevant,
        "classification_label": label,
        "classification_confidence": confidence,
        "classification_reason": reason,
    }


def fetch_entity_page(
    entity_type: str,
    next_cursor: Optional[str],
    created_after: Optional[datetime] = None,
    max_attempts: int = 6,
    initial_backoff_sec: float = 1.0,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str], bool, int]:
    _require_runtime_config()

    endpoint = _endpoint_for_entity(entity_type)
    url = f"{FATHOM_API_BASE.rstrip('/')}{endpoint}"

    params: Dict[str, Any] = {
        "limit": FATHOM_PAGE_LIMIT,
        "include_summary": str(FATHOM_INCLUDE_SUMMARY).lower(),
        "include_action_items": str(FATHOM_INCLUDE_ACTION_ITEMS).lower(),
        "include_crm_matches": str(FATHOM_INCLUDE_CRM_MATCHES).lower(),
        "include_transcript": str(FATHOM_INCLUDE_TRANSCRIPT).lower(),
    }
    if next_cursor:
        params[FATHOM_CURSOR_PARAM] = next_cursor
    if created_after:
        params["created_after"] = created_after.isoformat().replace("+00:00", "Z")

    headers = _build_headers()

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    allow_limit = True

    for attempt in range(1, max_attempts + 1):
        req_params = dict(params)
        if not allow_limit:
            req_params.pop("limit", None)

        resp = requests.get(url, headers=headers, params=req_params, timeout=60)
        last_resp = resp

        if resp.ok:
            payload = resp.json()
            if isinstance(payload, list):
                payload = {"items": payload}
            if not isinstance(payload, dict):
                payload = {"items": []}

            items = _extract_items(payload)
            new_cursor = _extract_next_cursor(payload)
            has_more = bool(new_cursor)
            return payload, items, new_cursor, has_more, resp.status_code

        body_preview = (resp.text or "")[:1000].lower()
        if resp.status_code == 422 and "limit" in body_preview and allow_limit:
            allow_limit = False
            continue

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    raise RuntimeError(
        f"Fathom request failed (entity_type={entity_type}, status={status}, "
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
        entity_id = _extract_entity_id(item)
        if not entity_id:
            continue

        event_ts = _extract_event_ts(item)
        ended_at_ts = _extract_ended_ts(item)
        updated_at_ts = _extract_updated_ts(item)
        partition_date = (updated_at_ts or event_ts or ingested_at_dt).date().isoformat()
        classification = classify_call(item)

        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "workspace_id": FATHOM_WORKSPACE_ID,
                "team_id": _extract_team_id(item),
                "title": _extract_title(item),
                "partition_date": partition_date,
                "event_ts": event_ts.isoformat() if event_ts else None,
                "ended_at_ts": ended_at_ts.isoformat() if ended_at_ts else None,
                "updated_at_ts": updated_at_ts.isoformat() if updated_at_ts else None,
                "ingested_at": ingested_at,
                "source": SOURCE_NAME,
                "payload_json": json.dumps(item),
                "external_participant_count": classification["external_participant_count"],
                "is_internal_only": classification["is_internal_only"],
                "is_revenue_relevant": classification["is_revenue_relevant"],
                "classification_label": classification["classification_label"],
                "classification_confidence": classification["classification_confidence"],
                "classification_reason": classification["classification_reason"],
                "backfill_run_id": run_id,
                "is_backfill": is_backfill,
            }
        )

    return rows


def upsert_raw_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.fathom_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      entity_type STRING NOT NULL,
      entity_id STRING NOT NULL,
      workspace_id STRING NOT NULL,
      team_id STRING,
      title STRING,
      partition_date DATE NOT NULL,
      event_ts TIMESTAMP,
      ended_at_ts TIMESTAMP,
      updated_at_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      external_participant_count INT64,
      is_internal_only BOOL,
      is_revenue_relevant BOOL,
      classification_label STRING,
      classification_confidence FLOAT64,
      classification_reason STRING,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    """
    client.query(create_stage).result()

    try:
        errors = client.insert_rows_json(stage_table, rows)
        if errors:
            raise RuntimeError(f"Fathom stage insert errors: {errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{FATHOM_RAW_TABLE}` T
        USING (
          SELECT * EXCEPT(rn)
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY entity_type, workspace_id, entity_id
                ORDER BY updated_at_ts DESC, event_ts DESC, ingested_at DESC
              ) AS rn
            FROM `{stage_table}`
          )
          WHERE rn = 1
        ) S
        ON T.entity_type = S.entity_type
           AND T.workspace_id = S.workspace_id
           AND T.entity_id = S.entity_id
        WHEN MATCHED THEN UPDATE SET
          team_id = S.team_id,
          title = S.title,
          partition_date = S.partition_date,
          event_ts = S.event_ts,
          ended_at_ts = S.ended_at_ts,
          updated_at_ts = S.updated_at_ts,
          ingested_at = S.ingested_at,
          source = S.source,
          payload_json = S.payload_json,
          external_participant_count = S.external_participant_count,
          is_internal_only = S.is_internal_only,
          is_revenue_relevant = S.is_revenue_relevant,
          classification_label = S.classification_label,
          classification_confidence = S.classification_confidence,
          classification_reason = S.classification_reason,
          backfill_run_id = S.backfill_run_id,
          is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            entity_type, entity_id, workspace_id, team_id, title, partition_date,
            event_ts, ended_at_ts, updated_at_ts, ingested_at, source, payload_json,
            external_participant_count, is_internal_only, is_revenue_relevant,
            classification_label, classification_confidence, classification_reason,
            backfill_run_id, is_backfill
          )
          VALUES (
            S.entity_type, S.entity_id, S.workspace_id, S.team_id, S.title, S.partition_date,
            S.event_ts, S.ended_at_ts, S.updated_at_ts, S.ingested_at, S.source, S.payload_json,
            S.external_participant_count, S.is_internal_only, S.is_revenue_relevant,
            S.classification_label, S.classification_confidence, S.classification_reason,
            S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def write_state(
    run_id: str,
    entity_type: str,
    workspace_id: str,
    status: str,
    next_cursor: Optional[str],
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{FATHOM_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @entity_type AS entity_type,
        @workspace_id AS workspace_id,
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
       AND T.workspace_id = S.workspace_id
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
        run_id, entity_type, workspace_id, status, next_cursor,
        pages_processed, rows_written, started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.entity_type, S.workspace_id, S.status, S.next_cursor,
        S.pages_processed, S.rows_written, S.started_at, S.updated_at, S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
        bigquery.ScalarQueryParameter("workspace_id", "STRING", workspace_id),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_cursor", "STRING", next_cursor),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(query, job_config=job_config).result()


def read_state(run_id: str, entity_type: str, workspace_id: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id,
      entity_type,
      workspace_id,
      status,
      next_cursor,
      pages_processed,
      rows_written,
      started_at,
      updated_at,
      error_text
    FROM `{PROJECT_ID}.{DATASET}.{FATHOM_STATE_TABLE}`
    WHERE run_id = @run_id
      AND entity_type = @entity_type
      AND workspace_id = @workspace_id
    LIMIT 1
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
        bigquery.ScalarQueryParameter("workspace_id", "STRING", workspace_id),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None

    row = rows[0]
    return {
        "run_id": row["run_id"],
        "entity_type": row["entity_type"],
        "workspace_id": row["workspace_id"],
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
        sql_file_path = str(Path(__file__).resolve().parent / "sql" / "fathom_models.sql")

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

    run_id = f"fathom-hourly-{_utc_now().strftime('%Y%m%d-%H%M%S')}"
    object_types = parse_object_types()
    created_after = _utc_now() - timedelta(hours=FATHOM_INCREMENTAL_LOOKBACK_HOURS)

    entities_summary: List[Dict[str, Any]] = []

    for entity_type in object_types:
        next_cursor: Optional[str] = None
        pages_processed = 0
        rows_written = 0
        last_upstream_status: Optional[int] = None

        while True:
            if (
                FATHOM_INCREMENTAL_MAX_PAGES_PER_OBJECT > 0
                and pages_processed >= FATHOM_INCREMENTAL_MAX_PAGES_PER_OBJECT
            ):
                status = "PAUSED_LIMIT_REACHED"
                break

            _, items, new_cursor, has_more, upstream_status = fetch_entity_page(
                entity_type=entity_type,
                next_cursor=next_cursor,
                created_after=created_after,
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

        entities_summary.append(
            {
                "entity_type": entity_type,
                "status": status,
                "pages_processed": pages_processed,
                "rows_upserted": rows_written,
                "upstream_status": last_upstream_status,
            }
        )

    run_models_after = os.getenv("FATHOM_RUN_MODELS_AFTER_INCREMENTAL", "false").lower() == "true"
    statements_executed = 0
    if run_models_after:
        statements_executed = run_models()

    return {
        "run_id": run_id,
        "workspace_id": FATHOM_WORKSPACE_ID,
        "lookback_hours": FATHOM_INCREMENTAL_LOOKBACK_HOURS,
        "entity_results": entities_summary,
        "models_refreshed": run_models_after,
        "statements_executed": statements_executed,
    }
