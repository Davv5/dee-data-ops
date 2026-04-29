import os
import sys
from typing import Optional

from google.cloud import bigquery

from sources.calendly.calendly_pipeline import (
    CALENDLY_INVITEE_STATE_TABLE,
    CALENDLY_STATE_TABLE,
    DATASET,
    PROJECT_ID,
    client,
    ensure_tables,
    run_invitee_drain,
)

# Quiescence window: a scheduled_events row that JUST flipped to COMPLETED may
# still belong to a parent process winding down post-completion (e.g. emitting
# its final invitee writes). Excluding rows whose updated_at is within this
# window prevents the drain from racing the live parent. Also excludes the
# hourly /ingest-calendly path: at the end of an hourly cycle scheduled_events
# is COMPLETED but invitee fan-out may still be in progress in the same
# request — the drain should let it finish naturally.
_QUIESCENCE_INTERVAL_MINUTES = 30


def _find_drainable_run_id() -> Optional[str]:
    """Find the oldest run_id whose scheduled_events backfill COMPLETED at
    least 30 minutes ago, but whose invitee work is still PENDING/RUNNING.

    Returns None when there is nothing to drain. The drain Job is meant to
    clean up stuck invitee work from a *prior* run, so it is a no-op when no
    such work exists. This avoids the 3-hour Cloud-Run-timeout that occurs
    when run_invitee_drain() is invoked with a fresh run_id whose
    scheduled_state default ('RUNNING') never advances.

    Filters to status='COMPLETED' only — PAUSED_LIMIT_REACHED, FAILED, and
    PARTIAL_FAILED are excluded because (a) PAUSED_LIMIT_REACHED is a
    transient cooldown the parent orchestrator resumes from itself, so the
    drain would race the live parent, and (b) FAILED/PARTIAL_FAILED runs
    typically need operator attention, not auto-drain.

    Quiescence guard: scheduled_events.updated_at must be older than 30 min.
    Closes the race with the hourly /ingest-calendly path which writes
    scheduled_events=COMPLETED at end-of-cycle while invitee fan-out may
    still be running in the same request.

    Orders ASC by scheduled_done_at so the oldest backlog drains first. Daily
    cadence + LIMIT 1 means each invocation drains one run_id; FIFO order
    guarantees no run_id ages indefinitely.

    Concurrency note: this module does NOT hold an advisory lock. Two
    overlapping invocations (e.g. operator manual `gcloud run jobs execute`
    racing a scheduled trigger) can pick the same run_id and double-fetch
    invitees. Cloud Run Jobs `parallelism=1, tasks=1` only constrains within
    a single execution — it does not serialize executions. Until a state-
    table claim mechanism lands, treat manual triggers as exclusive: don't
    fire one while a scheduled run is in flight.
    """
    query = f"""
    WITH scheduled_completed AS (
      SELECT
        run_id,
        MAX(updated_at) AS scheduled_done_at
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}`
      WHERE entity_type = 'scheduled_events'
        AND status = 'COMPLETED'
        AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {_QUIESCENCE_INTERVAL_MINUTES} MINUTE)
      GROUP BY run_id
    ),
    invitee_pending AS (
      SELECT run_id
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
      WHERE status IN ('PENDING', 'RUNNING')
      GROUP BY run_id
    )
    SELECT s.run_id
    FROM scheduled_completed s
    INNER JOIN invitee_pending p USING (run_id)
    ORDER BY s.scheduled_done_at ASC
    LIMIT 1
    """
    rows = list(client.query(query).result())
    if not rows:
        return None
    return rows[0]["run_id"]


def _validate_explicit_run_id(run_id: str) -> None:
    """Verify an operator-supplied run_id has terminal scheduled_events AND
    pending invitees before handing off to run_invitee_drain.

    Without this check, a typo'd or non-existent run_id causes a 3-hour
    Cloud-Run-timeout (the very failure mode the discovery path was designed
    to prevent — see _find_drainable_run_id docstring). Worse, run_invitee_drain
    unconditionally writes RUNNING state at calendly_pipeline.py:2148 before
    checking scheduled state, so the next discovery query then re-finds the
    bogus run_id.
    """
    query = f"""
    WITH scheduled AS (
      SELECT status, updated_at
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}`
      WHERE run_id = @run_id AND entity_type = 'scheduled_events'
    ),
    invitees AS (
      SELECT
        COUNTIF(status IN ('PENDING', 'RUNNING')) AS pending_count,
        COUNT(*) AS total_count
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_INVITEE_STATE_TABLE}`
      WHERE run_id = @run_id
    )
    SELECT
      (SELECT status FROM scheduled) AS scheduled_status,
      (SELECT pending_count FROM invitees) AS pending_count,
      (SELECT total_count FROM invitees) AS total_count
    """
    params = [bigquery.ScalarQueryParameter("run_id", "STRING", run_id)]
    rows = list(
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    )
    row = rows[0] if rows else None
    scheduled_status = (row["scheduled_status"] if row else None)
    pending_count = int(row["pending_count"] or 0) if row else 0
    total_count = int(row["total_count"] or 0) if row else 0

    if scheduled_status is None and total_count == 0:
        raise RuntimeError(
            f"explicit run_id {run_id!r} has no scheduled_events or invitee state — "
            "refusing to drain (would hang for 3 hours on RUNNING-default exit condition)"
        )
    if scheduled_status != "COMPLETED":
        raise RuntimeError(
            f"explicit run_id {run_id!r} has scheduled_events.status="
            f"{scheduled_status!r} (expected 'COMPLETED') — refusing to drain"
        )
    if pending_count == 0:
        raise RuntimeError(
            f"explicit run_id {run_id!r} has no PENDING/RUNNING invitees — nothing to drain"
        )


def main() -> int:
    ensure_tables()

    explicit_run_id = os.getenv("CALENDLY_INVITEE_DRAIN_RUN_ID")
    run_models_after = os.getenv("CALENDLY_RUN_MODELS_AFTER", "false").lower() == "true"

    if explicit_run_id:
        _validate_explicit_run_id(explicit_run_id)
        run_id = explicit_run_id
        print(f"calendly-invitee-drain: outcome=using_explicit run_id={run_id}")
    else:
        discovered = _find_drainable_run_id()
        if discovered is None:
            print(
                "calendly-invitee-drain: outcome=no_op "
                "reason=no_quiescent_completed_run_with_pending_invitees"
            )
            return 0
        run_id = discovered
        print(f"calendly-invitee-drain: outcome=discovered run_id={run_id}")

    print(
        f"calendly-invitee-drain: starting run_id={run_id} "
        f"run_models_after={run_models_after}"
    )

    result = run_invitee_drain(
        run_id=run_id,
        run_models_after=run_models_after,
    )

    final_status = result.get("status", "UNKNOWN")
    print(
        "calendly-invitee-drain: outcome=drained "
        f"run_id={run_id} "
        f"status={final_status} "
        f"pages_processed={result.get('pages_processed')} "
        f"rows_upserted={result.get('rows_upserted')} "
        f"failed_events={result.get('failed_events')}"
    )

    # Cloud Run Jobs alerting keys off exit code, not stdout. Non-COMPLETED
    # outcomes (PARTIAL_FAILED, PAUSED_LIMIT_REACHED) signal stuck or
    # incomplete work that needs operator attention; surface them as exit 1
    # so monitoring can fire instead of treating the Job as green.
    if final_status != "COMPLETED":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
