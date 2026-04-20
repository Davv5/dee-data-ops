---
paths: ["ingestion/**", "ingest/**", "sources/**"]
---

# Ingestion conventions

Load when working on any file under `ingestion/` (current target shape for custom Python extractors). `ingest/` and `sources/` are accepted aliases for portability across sibling projects.

## The ingestion contract

Every custom-source extractor is:

1. **A Python script running in GitHub Actions.** One workflow per source. Triggered by `workflow_dispatch` + a `schedule:` cron. Never runs from a local shell against production BigQuery.
2. **Writes to `raw_<source>.<source>__<object>_raw`** with two required columns:
   - `_ingested_at TIMESTAMP` (UTC, set at write time)
   - Raw JSON payload column (typed as `JSON` or `STRING`-of-JSON depending on source shape)
3. **Idempotent.** Upsert on the source primary key, or append-only with downstream deduplication in staging.
4. **State-tracked.** Reads/writes `ops.ingest_checkpoints` (columns: `source`, `object`, `last_cursor`, `last_success_at`).
5. **Logged.** Every run inserts into `ops.ingest_runs` with `run_id`, `source`, `object`, `started_at`, `ended_at`, `row_count`, `status`.
6. **Backfill flag.** Accepts `--since <ISO-8601 timestamp>` for replay; when set, bypasses the checkpoint and uses the arg as lower bound. Surface the flag as a `workflow_dispatch` input so backfills run through the same path as scheduled runs.

## v1 source inventory

D-DEE v1 has exactly five sources — two ingested via custom Python, three via Fivetran:

| Source     | Pipeline                          | Raw dataset      | Notes                                                 |
|------------|-----------------------------------|------------------|-------------------------------------------------------|
| GHL        | Python extractor + GitHub Actions | `raw_ghl`        | Primary CRM; the identity-spine anchor                |
| Fanbasis   | Python extractor + GitHub Actions | `raw_fanbasis`   | **Week-0 deferred** — scoped but not wired in v1 cut  |
| Typeform   | Fivetran managed connector        | `raw_typeform`   | No repo-local extractor; managed in Fivetran UI       |
| Calendly   | Fivetran managed connector        | `raw_calendly`   | No repo-local extractor; managed in Fivetran UI       |
| Stripe     | Fivetran managed connector        | `raw_stripe`     | No repo-local extractor; managed in Fivetran UI       |

Fivetran-managed sources follow the **same raw-dataset contract** (`raw_<source>.*`) so staging views remain uniform, but their freshness/schedule/SLAs are governed by Fivetran config, not this rule.

## Directory shape (Python extractors)

```
ingestion/
├── common/
│   ├── README.md               # the contract above
│   ├── bq.py                   # shared BigQuery client, retry logic, insert helpers
│   ├── checkpoint.py           # ops.ingest_checkpoints read/write
│   └── runs.py                 # ops.ingest_runs logging
├── ghl/
│   ├── extract.py              # main incremental extractor
│   ├── backfill.py             # state-tracked full backfill (optional, as needed)
│   ├── README.md               # per-source doc index
│   └── requirements.txt        # pinned Python deps
└── fanbasis/
    ├── extract.py
    ├── README.md
    └── requirements.txt
```

One source = one `ingestion/<source>/` directory. Do not split a single source across multiple packages.

## Per-source extractor skeleton

```python
# ingestion/<source>/extract.py
from ingestion.common.checkpoint import read_checkpoint, write_checkpoint
from ingestion.common.runs import log_run_start, log_run_end
from ingestion.common.bq import insert_raw

def main(since: str | None = None) -> None:
    run_id = log_run_start(source="<source>", object="<object>")
    try:
        cursor = since or read_checkpoint(source="<source>", object="<object>")
        rows = fetch_incremental(since=cursor)
        inserted = insert_raw(
            table="raw_<source>.<source>__<object>_raw",
            rows=rows,
            ingested_at_col="_ingested_at",
        )
        write_checkpoint(source="<source>", object="<object>", cursor=max_cursor_of(rows))
        log_run_end(run_id=run_id, status="success", row_count=inserted)
    except Exception as e:
        log_run_end(run_id=run_id, status="failure", error=str(e))
        raise
```

## Security

- **Never read credentials from a hardcoded path or a committed file.** Secrets live in **GitHub Actions secrets** (repository or environment scope). The workflow binds them as env vars at job runtime.
- **Every source uses its own dedicated GCP service account** for writes to `raw_<source>.*` and `ops.*`. No shared "ingest-sa-allpowerful."
- **`.env.example` files carry empty placeholders only.** Never commit live values.
- **Key rotation:** rotate the source API key + the GCP service-account key on a scheduled cadence (TBD) or immediately on suspected exposure. Update the corresponding GitHub Actions secret; no code change required.

## Orchestration

- **GitHub Actions `workflow_dispatch` + `schedule:` cron is the only production trigger path** for custom-source ingest. Not Airflow, not Dagster, not a local cron.
- **Workflow manifest:** each source gets its own workflow at `.github/workflows/ingest-<source>.yml` with a daily (or source-appropriate) cron and a manual `workflow_dispatch` input for `--since` backfills.
- **On-demand retry / backfill:** `gh workflow run ingest-<source>.yml -f since=<ISO-8601>` (requires the `gh` CLI authenticated with repo scope).
- **State inspection:** `select * from ops.ingest_checkpoints where source='<source>'` and `select * from ops.ingest_runs where source='<source>' order by started_at desc limit 10`.
- **Fivetran-managed sources** do not have a workflow file — orchestration is configured in the Fivetran dashboard. Only their `raw_<source>` datasets show up in this repo (via dbt sources declarations).

## Do not

- Run `python ingestion/<source>/extract.py` from your local shell against production BigQuery. Production ingest runs via GitHub Actions only. Local runs MUST target a dev BigQuery project.
- Mutate `raw_*` tables by hand — if a correction is needed, write a targeted backfill and run it through the workflow.
- Join across sources at the ingest layer. That belongs in warehouse bridges.
- Split a single source across multiple directories. One source = one `ingestion/<source>/` dir.
- Add credentials or service-account JSON to the repo. If a secret leaks into a commit, treat it as compromised and rotate immediately.

## Lessons learned

*(Populate as ingest issues arise.)*
