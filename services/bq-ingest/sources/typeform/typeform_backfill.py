"""Typeform historical backfill.

Fetches all historical responses for every form — no `since` filter.
State-tracked per form_id so the job is safe to re-run and resumes where
it left off if interrupted.

New forms added after the initial run are automatically picked up on the
next daily execution because they will have no state entry yet.

Env overrides:
    TYPEFORM_BACKFILL_RUN_ID      stable run identifier (default: typeform-backfill-v1)
    TYPEFORM_BACKFILL_RUN_MODELS  true/false — run Typeform models after (default true)
"""

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

from sources.typeform.typeform_pipeline import (
    PROJECT_ID,
    DATASET,
    TYPEFORM_STATE_TABLE,
    build_raw_rows,
    ensure_tables,
    fetch_form_responses,
    fetch_forms,
    run_models,
    upsert_raw_rows,
    _utc_now,
)

RUN_MODELS_AFTER = os.getenv("TYPEFORM_BACKFILL_RUN_MODELS", "true").lower() == "true"

# Stable run_id: once a form is COMPLETED under this ID it won't be re-fetched.
# Change the value to force a full re-backfill.
DEFAULT_RUN_ID = "typeform-backfill-v1"


# ---------------------------------------------------------------------------
# State helpers (the typeform_pipeline doesn't export these)
# ---------------------------------------------------------------------------

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def _read_state(run_id: str, form_id: str) -> Optional[Dict[str, Any]]:
    entity_type = f"responses__{form_id}"
    rows = list(
        _bq().query(
            f"""
            SELECT status, rows_written
            FROM `{PROJECT_ID}.{DATASET}.{TYPEFORM_STATE_TABLE}`
            WHERE run_id = @run_id AND entity_type = @entity_type
            LIMIT 1
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                    bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
                ]
            ),
        ).result()
    )
    return dict(rows[0]) if rows else None


def _write_state(run_id: str, form_id: str, status: str, rows_written: int) -> None:
    entity_type = f"responses__{form_id}"
    now = _utc_now().isoformat()
    _bq().query(
        f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TYPEFORM_STATE_TABLE}` T
        USING (SELECT @run_id AS run_id, @entity_type AS entity_type) S
        ON T.run_id = S.run_id AND T.entity_type = S.entity_type
        WHEN MATCHED THEN UPDATE SET
            status = @status,
            rows_written = @rows_written,
            updated_at = @now
        WHEN NOT MATCHED THEN INSERT (
            run_id, entity_type, status, next_cursor,
            pages_processed, rows_written, started_at, updated_at, error_text
        ) VALUES (
            @run_id, @entity_type, @status, NULL,
            1, @rows_written, @now, @now, NULL
        )
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("entity_type", "STRING", entity_type),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("rows_written", "INT64", rows_written),
                bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            ]
        ),
    ).result()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ensure_tables()

    run_id = os.getenv("TYPEFORM_BACKFILL_RUN_ID", DEFAULT_RUN_ID)
    print(f"Starting Typeform backfill run_id={run_id}")

    forms = fetch_forms()
    print(f"Found {len(forms)} form(s)")

    total_rows = 0
    completed = 0
    skipped = 0

    for form in forms:
        form_id = form.get("id")
        form_title = form.get("title", form_id)
        if not form_id:
            continue

        existing = _read_state(run_id, form_id)
        if existing and existing.get("status") == "COMPLETED":
            skipped += 1
            continue

        _write_state(run_id, form_id, "RUNNING", 0)
        try:
            responses = fetch_form_responses(form_id, since=None)
            rows, _ = build_raw_rows("responses", responses, run_id, is_backfill=True)
            if rows:
                upsert_raw_rows(rows)
            _write_state(run_id, form_id, "COMPLETED", len(rows))
            total_rows += len(rows)
            completed += 1
            print(f"  form={form_id} ({form_title}): fetched={len(responses)} rows={len(rows)}")
        except Exception as exc:
            _write_state(run_id, form_id, "FAILED", 0)
            print(f"  form={form_id} ({form_title}): WARN error={exc}")

    print(
        f"\nTypeform backfill complete: {completed} forms processed, "
        f"{skipped} skipped (already done), {total_rows} total rows written"
    )

    if RUN_MODELS_AFTER:
        executed = run_models()
        print(f"Typeform models refreshed. statements_executed={executed}")


if __name__ == "__main__":
    main()
