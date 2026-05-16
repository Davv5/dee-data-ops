import os
import uuid
from datetime import datetime, timezone

from sources.fanbasis.fanbasis_pipeline import (
    FANBASIS_PER_PAGE,
    build_txn_rows,
    ensure_tables,
    fetch_transactions_page,
    read_backfill_state,
    run_identity_backfill,
    upsert_transactions,
    write_backfill_state,
)


def main() -> None:
    ensure_tables()

    mode = os.getenv("FANBASIS_BACKFILL_MODE", "transactions").strip().lower()
    if mode in {"identity", "objects", "customers_subscribers"}:
        result = run_identity_backfill()
        print(f"Fanbasis identity backfill completed: {result}")
        return

    default_run_id = f"backfill-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_id = os.getenv("BACKFILL_RUN_ID", default_run_id)
    max_pages = int(os.getenv("BACKFILL_MAX_PAGES", "0"))
    start_page = int(os.getenv("BACKFILL_START_PAGE", "1"))

    existing = read_backfill_state(run_id)
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

    write_backfill_state(
        run_id=run_id,
        status="RUNNING",
        next_page=next_page,
        pages_processed=pages_processed,
        rows_written=rows_written,
        started_at=started_at,
        error_text=None,
    )

    pages_this_execution = 0
    try:
        while True:
            if max_pages > 0 and pages_this_execution >= max_pages:
                write_backfill_state(
                    run_id=run_id,
                    status="PAUSED_LIMIT_REACHED",
                    next_page=next_page,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )
                print(
                    f"Run {run_id}: page limit reached (max_pages={max_pages}). "
                    f"Resume with same BACKFILL_RUN_ID."
                )
                return

            payload, _ = fetch_transactions_page(page=next_page, per_page=FANBASIS_PER_PAGE)
            rows = build_txn_rows(payload=payload, is_backfill=True, backfill_run_id=run_id)
            if not rows:
                write_backfill_state(
                    run_id=run_id,
                    status="COMPLETED",
                    next_page=next_page,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )
                print(
                    f"Run {run_id} completed. pages_processed={pages_processed}, "
                    f"rows_written={rows_written}"
                )
                return

            upsert_transactions(rows)
            pages_processed += 1
            rows_written += len(rows)
            next_page += 1
            pages_this_execution += 1

            write_backfill_state(
                run_id=run_id,
                status="RUNNING",
                next_page=next_page,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=None,
            )
            print(
                f"Run {run_id}: processed page={next_page - 1}, "
                f"rows_upserted={len(rows)}, total_rows_written={rows_written}"
            )
    except Exception as exc:
        write_backfill_state(
            run_id=run_id,
            status="FAILED",
            next_page=next_page,
            pages_processed=pages_processed,
            rows_written=rows_written,
            started_at=started_at,
            error_text=str(exc)[:2000],
        )
        raise


if __name__ == "__main__":
    main()
