import os
import sys
from typing import Optional

from sources.calendly.calendly_pipeline import (
    CALENDLY_INVITEE_STATE_TABLE,
    CALENDLY_STATE_TABLE,
    DATASET,
    PROJECT_ID,
    client,
    ensure_tables,
    run_invitee_drain,
)


def _find_drainable_run_id() -> Optional[str]:
    """Find the oldest run_id whose scheduled_events backfill COMPLETED but
    whose invitee work is still PENDING/RUNNING.

    Returns None when there is nothing to drain. The drain Job is meant to
    clean up stuck invitee work from a *prior* run, so it is a no-op when no
    such work exists. This avoids the 3-hour timeout that occurs when
    run_invitee_drain() is invoked with a fresh run_id whose scheduled_state
    default ('RUNNING') never advances to a terminal status.

    Filters to status='COMPLETED' only — PAUSED_LIMIT_REACHED, FAILED, and
    PARTIAL_FAILED are excluded because (a) PAUSED_LIMIT_REACHED is a
    transient cooldown the parent orchestrator resumes from itself, so the
    drain would race the live parent, and (b) FAILED/PARTIAL_FAILED runs
    typically need operator attention, not auto-drain.

    Orders ASC by scheduled_done_at so the oldest backlog drains first. Daily
    cadence + LIMIT 1 means each invocation drains one run_id; FIFO order
    guarantees no run_id ages indefinitely.
    """
    query = f"""
    WITH scheduled_completed AS (
      SELECT
        run_id,
        MAX(updated_at) AS scheduled_done_at
      FROM `{PROJECT_ID}.{DATASET}.{CALENDLY_STATE_TABLE}`
      WHERE entity_type = 'scheduled_events'
        AND status = 'COMPLETED'
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


def main() -> None:
    ensure_tables()

    explicit_run_id = os.getenv("CALENDLY_INVITEE_DRAIN_RUN_ID")
    run_models_after = os.getenv("CALENDLY_RUN_MODELS_AFTER", "false").lower() == "true"

    if explicit_run_id:
        run_id = explicit_run_id
        print(f"calendly-invitee-drain: outcome=using_explicit run_id={run_id}")
    else:
        run_id = _find_drainable_run_id()
        if run_id is None:
            print(
                "calendly-invitee-drain: outcome=no_op "
                "reason=no_completed_run_with_pending_invitees"
            )
            return
        print(f"calendly-invitee-drain: outcome=discovered run_id={run_id}")

    print(
        f"calendly-invitee-drain: starting run_id={run_id} "
        f"run_models_after={run_models_after}"
    )

    result = run_invitee_drain(
        run_id=run_id,
        run_models_after=run_models_after,
    )

    print(
        "calendly-invitee-drain: outcome=drained "
        f"run_id={run_id} "
        f"status={result.get('status')} "
        f"pages_processed={result.get('pages_processed')} "
        f"rows_upserted={result.get('rows_upserted')} "
        f"failed_events={result.get('failed_events')}"
    )


if __name__ == "__main__":
    sys.exit(main())
