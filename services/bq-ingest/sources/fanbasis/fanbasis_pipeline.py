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
FANBASIS_API_KEY = os.getenv("FANBASIS_API_KEY")
FANBASIS_ENDPOINT = os.getenv(
    "FANBASIS_ENDPOINT",
    "https://www.fanbasis.com/public-api/checkout-sessions/transactions",
)
FANBASIS_API_BASE = os.getenv("FANBASIS_API_BASE", "https://www.fanbasis.com")
FANBASIS_PER_PAGE = int(os.getenv("FANBASIS_PER_PAGE", "100"))
FANBASIS_OBJECT_TYPES = os.getenv(
    "FANBASIS_OBJECT_TYPES",
    "customers,subscribers",
)

TXN_TABLE = os.getenv("BQ_TXN_TABLE", "fanbasis_transactions_txn_raw")
OBJECTS_TABLE = os.getenv("BQ_FANBASIS_OBJECTS_TABLE", "fanbasis_objects_raw")
BACKFILL_STATE_TABLE = os.getenv("BQ_BACKFILL_STATE_TABLE", "fanbasis_backfill_state")
OBJECT_BACKFILL_STATE_TABLE = os.getenv(
    "BQ_FANBASIS_OBJECT_STATE_TABLE",
    "fanbasis_object_backfill_state",
)

SOURCE_NAME = "fanbasis"
ENDPOINT_NAME = "transactions"

OBJECT_ENDPOINTS = {
    "customers": "/public-api/customers",
    "subscribers": "/public-api/subscribers",
    "products": "/public-api/products",
}

client = bigquery.Client(project=PROJECT_ID)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
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


def _extract_transactions(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    # Primary FanBasis shape: {"data": {"transactions": [...]}}
    nested_data = payload.get("data")
    if isinstance(nested_data, dict):
        nested_txns = nested_data.get("transactions")
        if isinstance(nested_txns, list):
            return [x for x in nested_txns if isinstance(x, dict)]
    for key in ("transactions", "data", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _transaction_id(txn: Dict[str, Any]) -> Optional[str]:
    for key in ("transaction_id", "id", "payment_id", "txn_id"):
        value = txn.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _transaction_event_ts(txn: Dict[str, Any]) -> Optional[datetime]:
    for key in ("transaction_date", "created_at", "event_ts", "timestamp", "created", "updated_at"):
        dt = _parse_ts(txn.get(key))
        if dt:
            return dt
    return None


def _object_types() -> List[str]:
    return [
        object_type.strip()
        for object_type in FANBASIS_OBJECT_TYPES.split(",")
        if object_type.strip()
    ]


def _object_endpoint(object_type: str) -> str:
    if object_type not in OBJECT_ENDPOINTS:
        supported = ", ".join(sorted(OBJECT_ENDPOINTS))
        raise ValueError(
            f"Unsupported Fanbasis object_type={object_type}. "
            f"Supported values: {supported}"
        )
    return f"{FANBASIS_API_BASE}{OBJECT_ENDPOINTS[object_type]}"


def _extract_objects(payload: Any, object_type: str) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, dict):
        keyed = data.get(object_type)
        if isinstance(keyed, list):
            return [x for x in keyed if isinstance(x, dict)]
        laravel_rows = data.get("data")
        if isinstance(laravel_rows, list):
            return [x for x in laravel_rows if isinstance(x, dict)]

    keyed = payload.get(object_type)
    if isinstance(keyed, list):
        return [x for x in keyed if isinstance(x, dict)]
    for key in ("data", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _object_id(item: Dict[str, Any], object_type: str) -> Optional[str]:
    for key in ("id", f"{object_type[:-1]}_id", "object_id"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    if object_type == "subscribers":
        subscription = item.get("subscription")
        if isinstance(subscription, dict):
            value = subscription.get("id")
            if value is not None and str(value).strip():
                return str(value).strip()
    return None


def _object_event_ts(item: Dict[str, Any], object_type: str) -> Optional[datetime]:
    for key in ("updated_at", "created_at", "last_transaction_date"):
        dt = _parse_ts(item.get(key))
        if dt:
            return dt
    if object_type == "subscribers":
        subscription = item.get("subscription")
        if isinstance(subscription, dict):
            for key in ("updated_at", "created_at", "completion_date", "cancelled_at"):
                dt = _parse_ts(subscription.get(key))
                if dt:
                    return dt
    return None


def _pagination_state(payload: Dict[str, Any], page: int) -> Tuple[bool, int]:
    data = payload.get("data")
    if isinstance(data, dict):
        pagination = data.get("pagination")
        if isinstance(pagination, dict):
            has_more = bool(pagination.get("has_more"))
            total_pages = int(pagination.get("total_pages") or page)
            return has_more or page < total_pages, total_pages

        if "last_page" in data:
            total_pages = int(data.get("last_page") or page)
            return page < total_pages, total_pages

    return False, page


def ensure_tables() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{TXN_TABLE}` (
      transaction_id STRING NOT NULL,
      event_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      endpoint STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY DATE(event_ts)
    CLUSTER BY transaction_id
    """
    client.query(query).result()

    state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{BACKFILL_STATE_TABLE}` (
      run_id STRING NOT NULL,
      status STRING NOT NULL,
      next_page INT64 NOT NULL,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    """
    client.query(state_query).result()

    object_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{OBJECTS_TABLE}` (
      object_type STRING NOT NULL,
      object_id STRING NOT NULL,
      event_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      endpoint STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    PARTITION BY DATE(event_ts)
    CLUSTER BY object_type, object_id
    """
    client.query(object_query).result()

    object_state_query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{OBJECT_BACKFILL_STATE_TABLE}` (
      run_id STRING NOT NULL,
      object_type STRING NOT NULL,
      status STRING NOT NULL,
      next_page INT64 NOT NULL,
      pages_processed INT64 NOT NULL,
      rows_written INT64 NOT NULL,
      started_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      error_text STRING
    )
    """
    client.query(object_state_query).result()


def fetch_transactions_page(
    page: int,
    per_page: int,
    max_attempts: int = 5,
    initial_backoff_sec: float = 1.0,
) -> Tuple[Dict[str, Any], requests.Response]:
    if not FANBASIS_API_KEY:
        raise RuntimeError("Missing FANBASIS_API_KEY")

    headers = {
        "x-api-key": FANBASIS_API_KEY,
        "Content-Type": "application/json",
    }
    params = {"page": page, "per_page": per_page}

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(FANBASIS_ENDPOINT, headers=headers, params=params, timeout=60)
        last_resp = resp
        if resp.ok:
            return resp.json(), resp
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    status = "unknown" if last_resp is None else str(last_resp.status_code)
    raise RuntimeError(
        f"FanBasis request failed after retries (status={status}, page={page}, "
        f"url={FANBASIS_ENDPOINT}, body_preview={body_preview})"
    )


def fetch_object_page(
    object_type: str,
    page: int,
    per_page: int,
    max_attempts: int = 5,
    initial_backoff_sec: float = 1.0,
) -> Tuple[Dict[str, Any], requests.Response]:
    if not FANBASIS_API_KEY:
        raise RuntimeError("Missing FANBASIS_API_KEY")

    url = _object_endpoint(object_type)
    headers = {
        "x-api-key": FANBASIS_API_KEY,
        "Content-Type": "application/json",
    }
    params = {"page": page, "per_page": per_page}

    backoff = initial_backoff_sec
    last_resp: Optional[requests.Response] = None
    for attempt in range(1, max_attempts + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        last_resp = resp
        if resp.ok:
            return resp.json(), resp
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
            time.sleep(backoff)
            backoff *= 2
            continue
        break

    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    status = "unknown" if last_resp is None else str(last_resp.status_code)
    raise RuntimeError(
        f"FanBasis object request failed after retries "
        f"(object_type={object_type}, status={status}, page={page}, "
        f"url={url}, body_preview={body_preview})"
    )


def build_txn_rows(
    payload: Dict[str, Any],
    is_backfill: bool,
    backfill_run_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ingested_at = _utc_now().isoformat()
    rows: List[Dict[str, Any]] = []
    for txn in _extract_transactions(payload):
        txn_id = _transaction_id(txn)
        if not txn_id:
            continue
        event_ts = _transaction_event_ts(txn)
        rows.append(
            {
                "transaction_id": txn_id,
                "event_ts": event_ts.isoformat() if event_ts else None,
                "ingested_at": ingested_at,
                "source": SOURCE_NAME,
                "endpoint": ENDPOINT_NAME,
                "payload_json": json.dumps(txn),
                "backfill_run_id": backfill_run_id,
                "is_backfill": is_backfill,
            }
        )
    return rows


def build_object_rows(
    object_type: str,
    payload: Dict[str, Any],
    is_backfill: bool,
    backfill_run_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ingested_at = _utc_now().isoformat()
    endpoint = OBJECT_ENDPOINTS[object_type]
    rows: List[Dict[str, Any]] = []
    for item in _extract_objects(payload, object_type):
        item_id = _object_id(item, object_type)
        if not item_id:
            continue
        event_ts = _object_event_ts(item, object_type)
        rows.append(
            {
                "object_type": object_type,
                "object_id": item_id,
                "event_ts": event_ts.isoformat() if event_ts else None,
                "ingested_at": ingested_at,
                "source": SOURCE_NAME,
                "endpoint": endpoint,
                "payload_json": json.dumps(item),
                "backfill_run_id": backfill_run_id,
                "is_backfill": is_backfill,
            }
        )
    return rows


def upsert_transactions(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.fanbasis_txn_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      transaction_id STRING NOT NULL,
      event_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      endpoint STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    """
    client.query(create_stage).result()
    try:
        insert_errors = client.insert_rows_json(stage_table, rows)
        if insert_errors:
            raise RuntimeError(f"Stage insert errors: {insert_errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TXN_TABLE}` T
        USING `{stage_table}` S
        ON T.transaction_id = S.transaction_id
        WHEN MATCHED THEN
          UPDATE SET
            event_ts = S.event_ts,
            ingested_at = S.ingested_at,
            source = S.source,
            endpoint = S.endpoint,
            payload_json = S.payload_json,
            backfill_run_id = S.backfill_run_id,
            is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            transaction_id, event_ts, ingested_at, source, endpoint,
            payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.transaction_id, S.event_ts, S.ingested_at, S.source, S.endpoint,
            S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def upsert_objects(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    stage_table = f"{PROJECT_ID}.{DATASET}.fanbasis_objects_stage_{uuid.uuid4().hex[:8]}"
    create_stage = f"""
    CREATE TABLE `{stage_table}` (
      object_type STRING NOT NULL,
      object_id STRING NOT NULL,
      event_ts TIMESTAMP,
      ingested_at TIMESTAMP NOT NULL,
      source STRING NOT NULL,
      endpoint STRING NOT NULL,
      payload_json JSON NOT NULL,
      backfill_run_id STRING,
      is_backfill BOOL NOT NULL
    )
    """
    client.query(create_stage).result()
    try:
        insert_errors = client.insert_rows_json(stage_table, rows)
        if insert_errors:
            raise RuntimeError(f"Object stage insert errors: {insert_errors}")

        merge_query = f"""
        MERGE `{PROJECT_ID}.{DATASET}.{OBJECTS_TABLE}` T
        USING `{stage_table}` S
        ON T.object_type = S.object_type
       AND T.object_id = S.object_id
        WHEN MATCHED THEN
          UPDATE SET
            event_ts = S.event_ts,
            ingested_at = S.ingested_at,
            source = S.source,
            endpoint = S.endpoint,
            payload_json = S.payload_json,
            backfill_run_id = S.backfill_run_id,
            is_backfill = S.is_backfill
        WHEN NOT MATCHED THEN
          INSERT (
            object_type, object_id, event_ts, ingested_at, source,
            endpoint, payload_json, backfill_run_id, is_backfill
          )
          VALUES (
            S.object_type, S.object_id, S.event_ts, S.ingested_at, S.source,
            S.endpoint, S.payload_json, S.backfill_run_id, S.is_backfill
          )
        """
        client.query(merge_query).result()
    finally:
        client.delete_table(stage_table, not_found_ok=True)


def write_backfill_state(
    run_id: str,
    status: str,
    next_page: int,
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{BACKFILL_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @status AS status,
        @next_page AS next_page,
        @pages_processed AS pages_processed,
        @rows_written AS rows_written,
        @started_at AS started_at,
        CURRENT_TIMESTAMP() AS updated_at,
        @error_text AS error_text
    ) S
    ON T.run_id = S.run_id
    WHEN MATCHED THEN
      UPDATE SET
        status = S.status,
        next_page = S.next_page,
        pages_processed = S.pages_processed,
        rows_written = S.rows_written,
        started_at = S.started_at,
        updated_at = S.updated_at,
        error_text = S.error_text
    WHEN NOT MATCHED THEN
      INSERT (
        run_id, status, next_page, pages_processed, rows_written,
        started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.status, S.next_page, S.pages_processed, S.rows_written,
        S.started_at, S.updated_at, S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_page", "INT64", next_page),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def read_backfill_state(run_id: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id, status, next_page, pages_processed, rows_written, started_at, updated_at, error_text
    FROM `{PROJECT_ID}.{DATASET}.{BACKFILL_STATE_TABLE}`
    WHERE run_id = @run_id
    LIMIT 1
    """
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    rows = list(
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "next_page": row["next_page"],
        "pages_processed": row["pages_processed"],
        "rows_written": row["rows_written"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "error_text": row["error_text"],
    }


def write_object_backfill_state(
    run_id: str,
    object_type: str,
    status: str,
    next_page: int,
    pages_processed: int,
    rows_written: int,
    started_at: datetime,
    error_text: Optional[str] = None,
) -> None:
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET}.{OBJECT_BACKFILL_STATE_TABLE}` T
    USING (
      SELECT
        @run_id AS run_id,
        @object_type AS object_type,
        @status AS status,
        @next_page AS next_page,
        @pages_processed AS pages_processed,
        @rows_written AS rows_written,
        @started_at AS started_at,
        CURRENT_TIMESTAMP() AS updated_at,
        @error_text AS error_text
    ) S
    ON T.run_id = S.run_id
   AND T.object_type = S.object_type
    WHEN MATCHED THEN
      UPDATE SET
        status = S.status,
        next_page = S.next_page,
        pages_processed = S.pages_processed,
        rows_written = S.rows_written,
        started_at = S.started_at,
        updated_at = S.updated_at,
        error_text = S.error_text
    WHEN NOT MATCHED THEN
      INSERT (
        run_id, object_type, status, next_page, pages_processed,
        rows_written, started_at, updated_at, error_text
      )
      VALUES (
        S.run_id, S.object_type, S.status, S.next_page,
        S.pages_processed, S.rows_written, S.started_at, S.updated_at,
        S.error_text
      )
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("next_page", "INT64", next_page),
        bigquery.ScalarQueryParameter("pages_processed", "INT64", pages_processed),
        bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
        bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at),
        bigquery.ScalarQueryParameter("error_text", "STRING", error_text),
    ]
    client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def read_object_backfill_state(
    run_id: str,
    object_type: str,
) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT
      run_id, object_type, status, next_page, pages_processed,
      rows_written, started_at, updated_at, error_text
    FROM `{PROJECT_ID}.{DATASET}.{OBJECT_BACKFILL_STATE_TABLE}`
    WHERE run_id = @run_id
      AND object_type = @object_type
    LIMIT 1
    """
    params = [
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        bigquery.ScalarQueryParameter("object_type", "STRING", object_type),
    ]
    rows = list(
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "run_id": row["run_id"],
        "object_type": row["object_type"],
        "status": row["status"],
        "next_page": row["next_page"],
        "pages_processed": row["pages_processed"],
        "rows_written": row["rows_written"],
        "started_at": row["started_at"],
        "updated_at": row["updated_at"],
        "error_text": row["error_text"],
    }


def run_identity_backfill() -> Dict[str, Any]:
    ensure_tables()

    default_run_id = (
        f"identity-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    run_id = os.getenv("FANBASIS_OBJECT_BACKFILL_RUN_ID", default_run_id)
    max_pages = int(os.getenv("FANBASIS_OBJECT_MAX_PAGES", "0"))
    start_page = int(os.getenv("FANBASIS_OBJECT_START_PAGE", "1"))

    results: Dict[str, Any] = {}
    for object_type in _object_types():
        existing = read_object_backfill_state(run_id, object_type)
        if existing:
            next_page = int(existing["next_page"])
            pages_processed = int(existing["pages_processed"])
            rows_written = int(existing["rows_written"])
            started_at = existing["started_at"]
        else:
            next_page = start_page
            pages_processed = 0
            rows_written = 0
            started_at = datetime.now(timezone.utc)

        write_object_backfill_state(
            run_id=run_id,
            object_type=object_type,
            status="RUNNING",
            next_page=next_page,
            pages_processed=pages_processed,
            rows_written=rows_written,
            started_at=started_at,
            error_text=None,
        )

        pages_this_object = 0
        try:
            while True:
                if max_pages > 0 and pages_this_object >= max_pages:
                    write_object_backfill_state(
                        run_id=run_id,
                        object_type=object_type,
                        status="PAUSED_LIMIT_REACHED",
                        next_page=next_page,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                    break

                payload, _ = fetch_object_page(
                    object_type=object_type,
                    page=next_page,
                    per_page=FANBASIS_PER_PAGE,
                )
                rows = build_object_rows(
                    object_type=object_type,
                    payload=payload,
                    is_backfill=True,
                    backfill_run_id=run_id,
                )
                if rows:
                    upsert_objects(rows)

                pages_processed += 1
                rows_written += len(rows)
                pages_this_object += 1

                has_more, _ = _pagination_state(payload, next_page)
                next_page += 1
                if not rows or not has_more:
                    write_object_backfill_state(
                        run_id=run_id,
                        object_type=object_type,
                        status="COMPLETED",
                        next_page=next_page,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                    break

                write_object_backfill_state(
                    run_id=run_id,
                    object_type=object_type,
                    status="RUNNING",
                    next_page=next_page,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )
        except Exception as exc:
            write_object_backfill_state(
                run_id=run_id,
                object_type=object_type,
                status="FAILED",
                next_page=next_page,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=str(exc)[:2000],
            )
            raise

        results[object_type] = {
            "pages_processed": pages_processed,
            "rows_written": rows_written,
            "next_page": next_page,
        }

    return {"run_id": run_id, "object_types": results}


def _split_sql_statements(sql_text: str) -> List[str]:
    # Keep parser simple: drop comment lines and split on semicolons.
    lines: List[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)
    joined = "\n".join(lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


def refresh_models_from_file(sql_file_path: Optional[str] = None) -> int:
    if sql_file_path is None:
        candidate_path = Path(__file__).resolve().parent / "sql" / "models.sql"
        if candidate_path.exists():
            sql_file_path = str(candidate_path)
        else:
            sql_file_path = str(Path(__file__).resolve().parents[2] / "sql" / "models.sql")
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    for stmt in statements:
        client.query(stmt).result()
    return len(statements)
