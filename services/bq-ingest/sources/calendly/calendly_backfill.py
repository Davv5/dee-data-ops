import os
import uuid
from datetime import datetime, timezone

from sources.calendly.calendly_pipeline import run_backfill


def main() -> None:
    default_run_id = (
        f"calendly-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    )
    run_id = os.getenv("CALENDLY_BACKFILL_RUN_ID", default_run_id)
    max_pages_per_object = int(os.getenv("CALENDLY_MAX_PAGES_PER_OBJECT", "0"))
    run_models_after = os.getenv("CALENDLY_RUN_MODELS_AFTER", "true").lower() == "true"
    mode = os.getenv("CALENDLY_BACKFILL_MODE", "combined").strip().lower()

    print(
        f"Starting Calendly backfill run_id={run_id} "
        f"max_pages_per_object={max_pages_per_object} run_models_after={run_models_after} mode={mode}"
    )

    result = run_backfill(
        run_id=run_id,
        max_pages_per_object=max_pages_per_object,
        run_models_after=run_models_after,
        mode=mode,
    )

    print(
        "Calendly backfill completed "
        f"run_id={result['run_id']} models_refreshed={result['models_refreshed']} "
        f"statements_executed={result['statements_executed']}"
    )


if __name__ == "__main__":
    main()
