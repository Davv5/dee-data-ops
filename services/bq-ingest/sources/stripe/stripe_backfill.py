import os
import uuid
from datetime import datetime, timezone

from sources.stripe.stripe_pipeline import (
    STRIPE_PAGE_LIMIT,
    build_raw_rows,
    ensure_tables,
    fetch_stripe_page,
    parse_object_types,
    read_state,
    run_models,
    upsert_raw_rows,
    write_state,
)


def main() -> None:
    ensure_tables()

    default_run_id = f"stripe-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_id = os.getenv("STRIPE_BACKFILL_RUN_ID", default_run_id)
    max_pages_per_object = int(os.getenv("STRIPE_MAX_PAGES_PER_OBJECT", "0"))
    run_models_after = os.getenv("STRIPE_RUN_MODELS_AFTER", "true").lower() == "true"
    object_types = parse_object_types()

    print(f"Starting Stripe backfill run_id={run_id} object_types={','.join(object_types)}")

    for object_type in object_types:
        existing = read_state(run_id=run_id, object_type=object_type)
        if existing and existing.get("status") == "COMPLETED":
            print(f"Skipping {object_type}: already COMPLETED for run_id={run_id}")
            continue

        next_cursor = existing["next_cursor"] if existing else None
        pages_processed = int(existing["pages_processed"]) if existing else 0
        rows_written = int(existing["rows_written"]) if existing else 0
        started_at = existing["started_at"] if existing else datetime.now(timezone.utc)
        pages_this_execution = 0

        write_state(
            run_id=run_id,
            object_type=object_type,
            status="RUNNING",
            next_cursor=next_cursor,
            pages_processed=pages_processed,
            rows_written=rows_written,
            started_at=started_at,
            error_text=None,
        )

        try:
            while True:
                if max_pages_per_object > 0 and pages_this_execution >= max_pages_per_object:
                    write_state(
                        run_id=run_id,
                        object_type=object_type,
                        status="PAUSED_LIMIT_REACHED",
                        next_cursor=next_cursor,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                    print(
                        f"Paused {object_type} after {pages_this_execution} pages "
                        f"(limit={max_pages_per_object})."
                    )
                    break

                payload = fetch_stripe_page(object_type=object_type, starting_after=next_cursor)
                rows, new_cursor, has_more = build_raw_rows(
                    object_type=object_type,
                    payload=payload,
                    run_id=run_id,
                    is_backfill=True,
                )

                if rows:
                    upsert_raw_rows(rows)
                    rows_written += len(rows)
                pages_processed += 1
                pages_this_execution += 1
                next_cursor = new_cursor

                write_state(
                    run_id=run_id,
                    object_type=object_type,
                    status="RUNNING" if has_more else "COMPLETED",
                    next_cursor=next_cursor,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )

                print(
                    f"{object_type}: page={pages_processed}, "
                    f"rows_this_page={len(rows)}, rows_written_total={rows_written}, "
                    f"has_more={has_more}"
                )

                if not has_more:
                    break
        except Exception as exc:
            write_state(
                run_id=run_id,
                object_type=object_type,
                status="FAILED",
                next_cursor=next_cursor,
                pages_processed=pages_processed,
                rows_written=rows_written,
                started_at=started_at,
                error_text=str(exc)[:2000],
            )
            raise

    if run_models_after:
        executed = run_models()
        print(f"Stripe models refreshed successfully. statements_executed={executed}")

    print(f"Stripe backfill completed for run_id={run_id}")


if __name__ == "__main__":
    main()
