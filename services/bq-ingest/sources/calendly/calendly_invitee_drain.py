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
# still belong to a parent process winding down post-completion (the hourly
# /ingest-calendly path writes scheduled_events=COMPLETED while invitee
# fan-out is still in progress in the same request). 60 min covers Cloud
# Run's max HTTP request timeout, closing the hourly race.
#
# Backfill Cloud Run Jobs (timeout_seconds=10800 / 3hr) can still race the
# drain after invitee fan-out exceeds 60 min post-COMPLETED. Mitigation for
# that path is the `mode` column on calendly_backfill_state — tracked in
# project-state Open threads as a follow-up; the current 60-min guard is
# the right tradeoff for daily-cadence drain operation.
_QUIESCENCE_INTERVAL_MINUTES = 60


def _find_drainable_run_id() -> Optional[str]:
    """Find the oldest run_id whose scheduled_events backfill COMPLETED at
    least _QUIESCENCE_INTERVAL_MINUTES ago, but whose invitee work is still
    PENDING/RUNNING.

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

    Quiescence guard: scheduled_events.updated_at must be older than
    _QUIESCENCE_INTERVAL_MINUTES. Closes the race with the hourly
    /ingest-calendly path which writes scheduled_events=COMPLETED at
    end-of-cycle while invitee fan-out may still be running in the same
    request.

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


def _validate_explicit_run_id(run_id: str) -> bool:
    """Verify an operator-supplied run_id is safe to drain.

    Returns True when there is work to drain (caller should proceed),
    False when the run_id is COMPLETED with no pending invitees (caller
    should no-op cleanly — same exit as the discovery path).

    Raises RuntimeError when the run_id config is invalid (no state, or
    scheduled_events not COMPLETED). Without this check, a typo'd or
    non-existent run_id causes a 3-hour Cloud-Run-timeout — the very
    failure mode the discovery path was designed to prevent.

    Operator-recovery note: returning False (rather than raising) when
    invitee state is empty preserves the manual-state-clear escape hatch.
    Operators may DELETE FROM ... WHERE run_id=? to clear corrupt state
    and re-trigger; this validator should not punish that recovery path
    with an error when the cleanup left valid (COMPLETED, no-pending) state.
    """
    query = f"""
    WITH scheduled AS (
      SELECT status
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}`
      WHERE run_id = @run_id AND entity_type = 'scheduled_events'
      LIMIT 1
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
    return pending_count > 0


def main() -> None:
    ensure_tables()

    explicit_run_id = os.getenv("CALENDLY_INVITEE_DRAIN_RUN_ID")
    run_models_after = os.getenv("CALENDLY_RUN_MODELS_AFTER", "false").lower() == "true"

    if explicit_run_id:
        has_work = _validate_explicit_run_id(explicit_run_id)
        if not has_work:
            print(
                "calendly-invitee-drain: outcome=no_op "
                f"reason=explicit_run_id_completed_no_pending run_id={explicit_run_id}"
            )
            return
        run_id = explicit_run_id
        print(f"calendly-invitee-drain: outcome=using_explicit run_id={run_id}")
    else:
        discovered = _find_drainable_run_id()
        if discovered is None:
            print(
                "calendly-invitee-drain: outcome=no_op "
                "reason=no_quiescent_completed_run_with_pending_invitees"
            )
            return
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

    # run_invitee_drain returns nested status — there is no top-level
    # 'status' key. The terminal invitee status sits at
    # result['results']['event_invitees']['status']; the parent backfill's
    # status sits at result['scheduled_status']. Both must be COMPLETED for
    # the run to be considered fully successful.
    invitee_result = result.get("results", {}).get("event_invitees", {}) or {}
    invitee_status = invitee_result.get("status", "UNKNOWN")
    scheduled_status = result.get("scheduled_status", "UNKNOWN")

    print(
        "calendly-invitee-drain: outcome=drained "
        f"run_id={run_id} "
        f"scheduled_status={scheduled_status} "
        f"invitee_status={invitee_status} "
        f"pages_processed={invitee_result.get('pages_processed')} "
        f"rows_upserted={invitee_result.get('rows_upserted')} "
        f"failed_events={invitee_result.get('failed_events')}"
    )

    # Cloud Run Jobs alerting via the runner CLI dispatch path keys off
    # whether `run_task_safe` caught an exception (ok=False), NOT off main()'s
    # return value (the int return is wrapped into payload['result'] and
    # discarded). So non-COMPLETED outcomes must RAISE, not return non-zero,
    # to surface as Job failures. The script-direct path
    # (`if __name__ == '__main__': sys.exit(main())`) also propagates the
    # exception → sys.exit traceback → exit 1.
    if invitee_status != "COMPLETED" or scheduled_status != "COMPLETED":
        raise RuntimeError(
            "calendly-invitee-drain finished with non-COMPLETED status — "
            f"scheduled_status={scheduled_status!r} "
            f"invitee_status={invitee_status!r}. "
            "Operator action required (PARTIAL_FAILED or PAUSED_LIMIT_REACHED)."
        )


if __name__ == "__main__":
    main()
