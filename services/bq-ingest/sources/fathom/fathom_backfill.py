import os
import uuid
from datetime import datetime, timezone

from sources.fathom.fathom_pipeline import (
    FATHOM_WORKSPACE_ID,
    build_rows,
    ensure_tables,
    fetch_entity_page,
    parse_object_types,
    read_state,
    run_models,
    upsert_raw_rows,
    write_state,
)


def main() -> None:
    ensure_tables()

    default_run_id = f"fathom-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_id = os.getenv("FATHOM_BACKFILL_RUN_ID", default_run_id)
    max_pages_per_object = int(os.getenv("FATHOM_MAX_PAGES_PER_OBJECT", "0"))
    run_models_after = os.getenv("FATHOM_RUN_MODELS_AFTER", "true").lower() == "true"
    object_types = parse_object_types()

    print(
        f"Starting Fathom backfill run_id={run_id} "
        f"workspace_id={FATHOM_WORKSPACE_ID} object_types={','.join(object_types)}"
    )

    for entity_type in object_types:
        existing = read_state(
            run_id=run_id,
            entity_type=entity_type,
            workspace_id=FATHOM_WORKSPACE_ID,
        )
        if existing and existing.get("status") == "COMPLETED":
            print(f"Skipping {entity_type}: already COMPLETED for run_id={run_id}")
            continue

        next_cursor = existing["next_cursor"] if existing else None
        pages_processed = int(existing["pages_processed"]) if existing else 0
        rows_written = int(existing["rows_written"]) if existing else 0
        started_at = existing["started_at"] if existing else datetime.now(timezone.utc)
        pages_this_execution = 0

        write_state(
            run_id=run_id,
            entity_type=entity_type,
            workspace_id=FATHOM_WORKSPACE_ID,
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
                        entity_type=entity_type,
                        workspace_id=FATHOM_WORKSPACE_ID,
                        status="PAUSED_LIMIT_REACHED",
                        next_cursor=next_cursor,
                        pages_processed=pages_processed,
                        rows_written=rows_written,
                        started_at=started_at,
                        error_text=None,
                    )
                    print(
                        f"Paused {entity_type} after {pages_this_execution} pages "
                        f"(limit={max_pages_per_object})."
                    )
                    break

                _, items, new_cursor, has_more, upstream_status = fetch_entity_page(
                    entity_type=entity_type,
                    next_cursor=next_cursor,
                    created_after=None,
                )

                rows = build_rows(
                    entity_type=entity_type,
                    items=items,
                    run_id=run_id,
                    is_backfill=True,
                )
                if rows:
                    upsert_raw_rows(rows)
                    rows_written += len(rows)

                pages_processed += 1
                pages_this_execution += 1
                next_cursor = new_cursor

                status = "RUNNING" if has_more else "COMPLETED"
                write_state(
                    run_id=run_id,
                    entity_type=entity_type,
                    workspace_id=FATHOM_WORKSPACE_ID,
                    status=status,
                    next_cursor=next_cursor,
                    pages_processed=pages_processed,
                    rows_written=rows_written,
                    started_at=started_at,
                    error_text=None,
                )

                print(
                    f"{entity_type}: page={pages_processed}, rows_this_page={len(rows)}, "
                    f"rows_written_total={rows_written}, has_more={has_more}, "
                    f"upstream_status={upstream_status}"
                )

                if not has_more:
                    break
        except Exception as exc:
            write_state(
                run_id=run_id,
                entity_type=entity_type,
                workspace_id=FATHOM_WORKSPACE_ID,
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
        print(f"Fathom models refreshed successfully. statements_executed={executed}")

    print(f"Fathom backfill completed for run_id={run_id}")


if __name__ == "__main__":
    main()
