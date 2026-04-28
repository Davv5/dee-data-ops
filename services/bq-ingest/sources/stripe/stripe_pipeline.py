import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")
STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
STRIPE_API_BASE = os.getenv("STRIPE_API_BASE", "https://api.stripe.com")
STRIPE_PAGE_LIMIT = int(os.getenv("STRIPE_PAGE_LIMIT", "100"))
STRIPE_CUTOFF_TS = os.getenv("STRIPE_CUTOFF_TS")  # Optional ISO 8601 UTC timestamp

STRIPE_RAW_TABLE = os.getenv("BQ_STRIPE_RAW_TABLE", "stripe_objects_raw")
STRIPE_STATE_TABLE = os.getenv("BQ_STRIPE_STATE_TABLE", "stripe_backfill_state")

DEFAULT_OBJECT_TYPES = (
    "charges,refunds,disputes,customers,products,prices,subscriptions,invoices,balance_transactions"
)
STRIPE_OBJECT_TYPES = os.getenv("STRIPE_OBJECT_TYPES", DEFAULT_OBJECT_TYPES)

client = bigquery.Client(project=PROJECT_ID)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    txt = value.strip()
    if not txt:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _cutoff_unix_ts() -> Optional[int]:
    dt = _parse_iso_ts(STRIPE_CUTOFF_TS)
    if not dt:
        return None
    return int(dt.timestamp())


def _event_ts_from_object(item: Dict[str, Any]) -> Optional[datetime]:
    created = item.get("created")
    if isinstance(created, (int, float)):
        return datetime.fromtimestamp(created, tz=timezone.utc)
    if isinstance(created, str):
        maybe_iso = _parse_iso_ts(created)
        if maybe_iso:
            return maybe_iso
    return None


def ensure_tables() -> None:
    raw_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{STRIPE_RAW_TABLE}` (
      object_type STRING NOT NULL,
      object_id STRING NOT NULL,
      event_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY DATE(event_ts)
    CLUSTER BY object_type, object_id
    """
    client.query(raw_query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{STRIPE_STATE_TABLE}` (
      run_id STRING NOT NULL,
      object_type STRING NOT NULL,
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


def _stripe_list_endpoint(object_type: str) -> str:
    mapping = {
        "charges": "/v1/charges",
        "refunds": "/v1/refunds",
        "disputes": "/v1/disputes",
        "customers": "/v1/customers",
        "products": "/v1/products",
        "prices": "/v1/prices",
        "subscriptions": "/v1/subscriptions",
        "invoices": "/v1/invoices",
        "balance_transactions": "/v1/balance_transactions",
    }
    if object_type not in mapping:
        raise ValueError(f"Unsupported Stripe object type: {object_type}")
    return f"{STRIPE_API_BASE}{mapping[object_type]}"


def fetch_stripe_page(
    object_type: str,
    starting_after: Optional[str] = None,
    max_attempts: int = 6,
    initial_backoff_sec: float = 1.0,
) -> Dict[str, Any]:
    if not STRIPE_API_KEY:
        raise RuntimeError("Missing STRIPE_API_KEY")

    url = _stripe_list_endpoint(object_type)
    params: Dict[str, Any] = {"limit": STRIPE_PAGE_LIMIT}
    if starting_after:
        params["starting_after"] = starting_after
    cutoff = _cutoff_unix_ts()
    if cutoff is not None:
        params["created[lte]"] = cutoff
    if object_type == "subscriptions":
        params["status"] = "all"

    headers = {
        "Authorization": f"Bearer {STRIPE_API_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        last_resp = resp
        if resp.ok:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    raise RuntimeError(
        f"Stripe request failed (object_type={object_type}, status={status}, "
        f"url={url}, body_preview={body_preview})"
    )


def build_raw_rows(
    object_type: str,
    payload: Dict[str, Any],
    run_id: str,
    is_backfill: bool = True,
) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
    data = payload.get("data")
    if not isinstance(data, list):
        data = []

    rows: List[Dict[str, Any]] = []
    last_id: Optional[str] = None
    for item in data:
        if not isinstance(item, dict):
            continue
        object_id = item.get("id")
        if not object_id:
            continue
        last_id = str(object_id)
        event_ts = _event_ts_from_object(item)
        rows.append(
            {
                "object_type": object_type,
                "object_id": str(object_id),
                "event_ts": event_ts.isoformat() if event_ts else None,
                "ingested_at": _utc_now().isoformat(),
                "source": "stripe",
                "payload_json": json.dumps(item),
                "backfill_run_id": run_id,
                "is_backfill": is_backfill,
            }
        )

    has_more = bool(payload.get("has_more"))
    next_cursor = last_id if has_more and last_id else None
    return rows, next_cursor, has_more


def upsert_raw_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.stripe_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      object_type STRING NOT NULL,
      object_id STRING NOT NULL,
      event_ts TIMESTAMP,
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
            raise RuntimeError(f"Stripe stage insert errors: {errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{STRIPE_RAW_TABLE}` T
        USING `{stage_table}` S
        ON T.object_type = S.object_type AND T.object_id = S.object_id
        WHEN MATCHED THEN UPDATE SET
          event_ts = S.event_ts,
          ingested_at = S.ingested_at,
          source = S.source,
          payload_json = S.payload_json,
          backfill_run_id = S.backfill_run_id,
          is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            object_type, object_id, event_ts, ingested_at, source,
            payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.object_type, S.object_id, S.event_ts, S.ingested_at, S.source,
            S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def write_state(
    run_id: str,
    object_type: str,
    status: str,
    next_cursor: Optional[str],
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{STRIPE_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @object_type AS object_type,
        @status AS status,
        @next_cursor AS next_cursor,
        @pages_processed AS pages_processed,
        @rows_written AS rows_written,
        @started_at AS started_at,
        CURRENT_TIMESTAMP() AS updated_at,
        @error_text AS error_text
    ) S
    ON T.run_id = S.run_id AND T.object_type = S.object_type
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
        run_id, object_type, status, next_cursor, pages_processed, rows_written,
        started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.object_type, S.status, S.next_cursor, S.pages_processed, S.rows_written,
        S.started_at, S.updated_at, S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_cursor", "STRING", next_cursor),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def read_state(run_id: str, object_type: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id, object_type, status, next_cursor, pages_processed, rows_written, started_at, updated_at, error_text
    FROM `{PROJECT_ID}.{DATASET}.{STRIPE_STATE_TABLE}`
    WHERE run_id = @run_id AND object_type = @object_type
    LIMIT 1
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
    ]
    rows = list(client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    if not rows:
        return None
    row = rows[0]
    return {
        "run_id": row["run_id"],
        "object_type": row["object_type"],
        "status": row["status"],
        "next_cursor": row["next_cursor"],
        "pages_processed": row["pages_processed"],
        "rows_written": row["rows_written"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "error_text": row["error_text"],
    }


def parse_object_types() -> List[str]:
    return [x.strip() for x in STRIPE_OBJECT_TYPES.split(",") if x.strip()]


def run_models(sql_file_path: Optional[str] = None) -> int:
    if sql_file_path is None:
        sql_file_path = str(Path(__file__).resolve().parent / "sql" / "stripe_models.sql")
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    lines: List[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)
    statements = [x.strip() for x in "\n".join(lines).split(";") if x.strip()]
    executed = 0
    for stmt in statements:
        client.query(stmt).result()
        executed += 1
    return executed
