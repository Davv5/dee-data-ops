"""Create or repair GHL contacts from the Fanbasis missing-contact mart.

This is intentionally small-batch and auditable. The source of truth is the
dbt mart `Marts.fanbasis_missing_ghl_contacts`; every attempted CRM write gets
recorded in BigQuery before the repaired contact is re-landed into GHL raw.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
from google.cloud import bigquery

from sources.ghl.ghl_pipeline import (
    GHL_ACCESS_TOKEN,
    GHL_API_BASE,
    GHL_API_VERSION,
    GHL_AUTH_SCHEME,
    GHL_LOCATION_ID,
    GHL_REQUEST_TIMEOUT_SEC,
    build_rows,
    ensure_tables as ensure_ghl_tables,
    upsert_raw_rows,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
AUDIT_DATASET = os.getenv("FANBASIS_GHL_SYNC_AUDIT_DATASET", "Raw")
AUDIT_TABLE = os.getenv("FANBASIS_GHL_SYNC_AUDIT_TABLE", "fanbasis_ghl_contact_sync_audit")
MART_FQN = os.getenv(
    "FANBASIS_MISSING_GHL_CONTACTS_FQN",
    f"{PROJECT_ID}.Marts.fanbasis_missing_ghl_contacts",
)
DEFAULT_TAGS = [
    tag.strip()
    for tag in os.getenv(
        "FANBASIS_GHL_CONTACT_TAGS",
        "fanbasis paid buyer,data ops source depth,fanbasis ghl repair",
    ).split(",")
    if tag.strip()
]

client = bigquery.Client(project=PROJECT_ID)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    txt = str(value).strip()
    return txt if txt else None


def _email_norm(value: Any) -> Optional[str]:
    txt = _safe_str(value)
    return txt.lower() if txt else None


def _phone_digits(value: Any) -> Optional[str]:
    txt = _safe_str(value)
    if not txt:
        return None
    digits = "".join(ch for ch in txt if ch.isdigit())
    return digits or None


def _split_name(name: Optional[str]) -> Dict[str, str]:
    if not name:
        return {}
    parts = name.split()
    if not parts:
        return {}
    if len(parts) == 1:
        return {"firstName": parts[0]}
    return {"firstName": parts[0], "lastName": " ".join(parts[1:])}


def _auth_values() -> List[str]:
    if not GHL_ACCESS_TOKEN:
        raise RuntimeError("Missing GHL_ACCESS_TOKEN")
    if GHL_AUTH_SCHEME == "bearer":
        return [f"Bearer {GHL_ACCESS_TOKEN}"]
    if GHL_AUTH_SCHEME in {"raw", "plain", "token"}:
        return [GHL_ACCESS_TOKEN]
    return [f"Bearer {GHL_ACCESS_TOKEN}", GHL_ACCESS_TOKEN]


def _request(
    method: str,
    endpoint: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    url = f"{GHL_API_BASE.rstrip('/')}{endpoint}"
    last_resp: Optional[requests.Response] = None

    for auth_value in _auth_values():
        headers = {
            "Authorization": auth_value,
            "Version": GHL_API_VERSION,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        backoff = 1.0
        for attempt in range(1, max(1, max_attempts) + 1):
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=payload,
                timeout=GHL_REQUEST_TIMEOUT_SEC,
            )
            last_resp = resp
            if resp.ok:
                if not resp.text:
                    return {}
                parsed = resp.json()
                return parsed if isinstance(parsed, dict) else {"data": parsed}

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            body_preview = (resp.text or "")[:1000]
            if resp.status_code == 401 and "invalid jwt" in body_preview.lower():
                break
            raise RuntimeError(
                f"GHL {method} {endpoint} failed: status={resp.status_code}, body={body_preview}"
            )

    status = "unknown" if last_resp is None else str(last_resp.status_code)
    body_preview = "" if last_resp is None else (last_resp.text or "")[:1000]
    raise RuntimeError(f"GHL {method} {endpoint} failed: status={status}, body={body_preview}")


def _extract_contacts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    contacts: List[Dict[str, Any]] = []
    for key in ("contacts", "contact", "data", "items", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            contacts.extend(x for x in value if isinstance(x, dict))
        elif isinstance(value, dict):
            contacts.append(value)
    if not contacts and "id" in payload:
        contacts.append(payload)
    return contacts


def _contact_id(contact: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "_id", "contactId", "contact_id"):
        value = _safe_str(contact.get(key))
        if value:
            return value
    return None


def _query_contacts(email: Optional[str], phone: Optional[str]) -> List[Dict[str, Any]]:
    query_terms = [value for value in (email, phone) if value]
    candidates: Dict[str, Dict[str, Any]] = {}

    for query in query_terms:
        payload = _request(
            "GET",
            "/contacts/",
            params={"locationId": GHL_LOCATION_ID, "query": query, "limit": 20},
        )
        for contact in _extract_contacts(payload):
            contact_id = _contact_id(contact)
            if contact_id:
                candidates[contact_id] = contact

    if email or phone:
        params = {"locationId": GHL_LOCATION_ID}
        if email:
            params["email"] = email
        if phone:
            params["number"] = phone
        try:
            payload = _request("GET", "/contacts/search/duplicate", params=params, max_attempts=1)
            for contact in _extract_contacts(payload):
                contact_id = _contact_id(contact)
                if contact_id:
                    candidates[contact_id] = contact
        except Exception:
            # Duplicate-search is only a guardrail; the query search above is
            # enough to continue, and failures are captured on the write audit.
            pass

    expected_email = _email_norm(email)
    expected_phone = _phone_digits(phone)
    exact_matches: Dict[str, Dict[str, Any]] = {}
    for contact_id, contact in candidates.items():
        contact_email = _email_norm(contact.get("email"))
        contact_phone = _phone_digits(contact.get("phone"))
        email_match = expected_email and contact_email == expected_email
        phone_match = (
            expected_phone
            and contact_phone
            and contact_phone[-10:] == expected_phone[-10:]
        )
        if email_match or phone_match:
            exact_matches[contact_id] = contact

    return list(exact_matches.values())


def _contact_payload(row: bigquery.table.Row) -> Dict[str, Any]:
    name = _safe_str(row.get("buyer_name"))
    payload: Dict[str, Any] = {
        "locationId": GHL_LOCATION_ID,
        "source": "Fanbasis",
    }
    if name:
        payload["name"] = name
        payload.update(_split_name(name))
    if _safe_str(row.get("buyer_email")):
        payload["email"] = _safe_str(row.get("buyer_email"))
    if _safe_str(row.get("buyer_phone")):
        payload["phone"] = _safe_str(row.get("buyer_phone"))
    return payload


def _add_tags(contact_id: str) -> Dict[str, Any]:
    if not DEFAULT_TAGS:
        return {}
    return _request(
        "POST",
        f"/contacts/{contact_id}/tags",
        payload={"tags": DEFAULT_TAGS},
        max_attempts=3,
    )


def _read_contact(contact_id: str) -> Optional[Dict[str, Any]]:
    payload = _request("GET", f"/contacts/{contact_id}", max_attempts=3)
    contacts = _extract_contacts(payload)
    if not contacts:
        return None
    contact = contacts[0]
    contact.setdefault("locationId", GHL_LOCATION_ID)
    return contact


def _ensure_audit_table() -> None:
    query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}` (
      run_id STRING NOT NULL,
      processed_at TIMESTAMP NOT NULL,
      dry_run BOOL NOT NULL,
      missing_ghl_contact_sk STRING NOT NULL,
      fanbasis_customer_id STRING,
      buyer_name STRING,
      buyer_email STRING,
      buyer_phone STRING,
      recommended_action STRING,
      action_taken STRING NOT NULL,
      status STRING NOT NULL,
      ghl_contact_id STRING,
      candidate_count INT64,
      paid_payments_count INT64,
      total_net_revenue NUMERIC,
      request_payload_json JSON,
      response_json JSON,
      error_text STRING
    )
    PARTITION BY DATE(processed_at)
    CLUSTER BY run_id, status, action_taken
    """
    client.query(query).result()


def _write_audit(rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        return
    insert_errors = client.insert_rows_json(f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}", rows)
    if insert_errors:
        raise RuntimeError(f"fanbasis_ghl_contact_sync_audit insert errors: {insert_errors}")


def _pending_rows(limit: int) -> List[bigquery.table.Row]:
    query = f"""
    SELECT
      missing_ghl_contact_sk,
      fanbasis_customer_id,
      buyer_name,
      buyer_email,
      buyer_phone,
      recommended_action,
      paid_payments_count,
      total_net_revenue
    FROM `{MART_FQN}`
    WHERE recommended_action IN ('create_ghl_contact', 'repair_identity_bridge')
      AND buyer_email IS NOT NULL
    ORDER BY total_net_revenue DESC, buyer_email
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return list(client.query(query, job_config=job_config).result())


def _audit_row(
    *,
    run_id: str,
    dry_run: bool,
    row: bigquery.table.Row,
    action_taken: str,
    status: str,
    candidate_count: int,
    ghl_contact_id: Optional[str] = None,
    request_payload: Optional[Dict[str, Any]] = None,
    response: Optional[Dict[str, Any]] = None,
    error_text: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "processed_at": _utc_now().isoformat(),
        "dry_run": dry_run,
        "missing_ghl_contact_sk": row.get("missing_ghl_contact_sk"),
        "fanbasis_customer_id": row.get("fanbasis_customer_id"),
        "buyer_name": row.get("buyer_name"),
        "buyer_email": row.get("buyer_email"),
        "buyer_phone": row.get("buyer_phone"),
        "recommended_action": row.get("recommended_action"),
        "action_taken": action_taken,
        "status": status,
        "ghl_contact_id": ghl_contact_id,
        "candidate_count": candidate_count,
        "paid_payments_count": row.get("paid_payments_count"),
        "total_net_revenue": str(row.get("total_net_revenue") or "0"),
        "request_payload_json": json.dumps(request_payload or {}),
        "response_json": json.dumps(response or {}),
        "error_text": error_text,
    }


def sync_missing_fanbasis_contacts() -> Dict[str, Any]:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    if not GHL_LOCATION_ID:
        raise RuntimeError("Missing GHL_LOCATION_ID")

    dry_run = _truthy(os.getenv("FANBASIS_GHL_CONTACT_SYNC_DRY_RUN", "true"))
    limit = int(os.getenv("FANBASIS_GHL_CONTACT_SYNC_LIMIT", "100"))
    run_id = os.getenv(
        "FANBASIS_GHL_CONTACT_SYNC_RUN_ID",
        f"fanbasis-ghl-contact-sync-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}",
    )

    _ensure_audit_table()
    ensure_ghl_tables()

    rows = _pending_rows(limit)
    audit_rows: List[Dict[str, Any]] = []
    summary = {
        "run_id": run_id,
        "dry_run": dry_run,
        "queued_rows": len(rows),
        "created": 0,
        "linked_existing": 0,
        "skipped_duplicates": 0,
        "failed": 0,
        "raw_contacts_upserted": 0,
    }

    for row in rows:
        request_payload = _contact_payload(row)
        try:
            candidates = _query_contacts(
                email=_safe_str(row.get("buyer_email")),
                phone=_safe_str(row.get("buyer_phone")),
            )
            if len(candidates) > 1:
                summary["skipped_duplicates"] += 1
                audit_rows.append(
                    _audit_row(
                        run_id=run_id,
                        dry_run=dry_run,
                        row=row,
                        action_taken="skipped_duplicate_candidates",
                        status="skipped",
                        candidate_count=len(candidates),
                        request_payload=request_payload,
                        response={"candidates": candidates},
                    )
                )
                continue

            if len(candidates) == 1:
                contact_id = _contact_id(candidates[0])
                response: Dict[str, Any] = {"matched_contact": candidates[0]}
                if not dry_run and contact_id:
                    tag_response = _add_tags(contact_id)
                    fresh_contact = _read_contact(contact_id)
                    response["tag_response"] = tag_response
                    response["fresh_contact"] = fresh_contact
                    if fresh_contact:
                        raw_rows = build_rows(
                            entity_type="contacts",
                            items=[fresh_contact],
                            run_id=run_id,
                            is_backfill=False,
                        )
                        upsert_raw_rows(raw_rows)
                        summary["raw_contacts_upserted"] += len(raw_rows)
                summary["linked_existing"] += 1
                audit_rows.append(
                    _audit_row(
                        run_id=run_id,
                        dry_run=dry_run,
                        row=row,
                        action_taken="linked_existing_contact",
                        status="success",
                        ghl_contact_id=contact_id,
                        candidate_count=1,
                        request_payload=request_payload,
                        response=response,
                    )
                )
                continue

            response = {"would_create": request_payload}
            contact_id: Optional[str] = None
            if not dry_run:
                response = _request(
                    "POST",
                    "/contacts/upsert",
                    payload=request_payload,
                    max_attempts=1,
                )
                contacts = _extract_contacts(response)
                contact_id = _contact_id(contacts[0]) if contacts else None
                if not contact_id:
                    raise RuntimeError(f"GHL upsert returned no contact id: {response}")
                tag_response = _add_tags(contact_id)
                fresh_contact = _read_contact(contact_id)
                response = {
                    "upsert_response": response,
                    "tag_response": tag_response,
                    "fresh_contact": fresh_contact,
                }
                if fresh_contact:
                    raw_rows = build_rows(
                        entity_type="contacts",
                        items=[fresh_contact],
                        run_id=run_id,
                        is_backfill=False,
                    )
                    upsert_raw_rows(raw_rows)
                    summary["raw_contacts_upserted"] += len(raw_rows)

            summary["created"] += 1
            audit_rows.append(
                _audit_row(
                    run_id=run_id,
                    dry_run=dry_run,
                    row=row,
                    action_taken="created_contact",
                    status="success",
                    ghl_contact_id=contact_id,
                    candidate_count=0,
                    request_payload=request_payload,
                    response=response,
                )
            )
        except Exception as exc:
            summary["failed"] += 1
            audit_rows.append(
                _audit_row(
                    run_id=run_id,
                    dry_run=dry_run,
                    row=row,
                    action_taken="failed",
                    status="failed",
                    candidate_count=0,
                    request_payload=request_payload,
                    error_text=str(exc)[:2000],
                )
            )

        _write_audit(audit_rows)
        audit_rows = []

    _write_audit(audit_rows)
    return summary


def main() -> None:
    print(json.dumps(sync_missing_fanbasis_contacts(), ensure_ascii=True), flush=True)


if __name__ == "__main__":
    main()
