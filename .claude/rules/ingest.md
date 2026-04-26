---
paths: ["1-raw-landing/**", "ingest/**", "sources/**"]
---

# Ingestion conventions

Load when working on any file under `1-raw-landing/` (current target shape for custom Python extractors). `ingest/` and `sources/` are accepted aliases for portability across sibling projects.

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

| Source     | Pipeline                                          | Raw dataset      | Notes                                                                              |
|------------|---------------------------------------------------|------------------|------------------------------------------------------------------------------------|
| GHL        | Python extractor + Cloud Run Jobs                 | `raw_ghl`        | Primary CRM; identity-spine anchor. Hot (1-min) + cold (15-min). Track W.         |
| Calendly   | Python extractor + Cloud Run Job (1-min cadence)  | `raw_calendly`   | Replaced Fivetran connector (Track X, 2026-04-22). Fivetran paused, not deleted.  |
| Fanbasis   | Python extractor + GitHub Actions                 | `raw_fanbasis`   | **Week-0 deferred** — scoped but not wired in v1 cut                               |
| Typeform   | Fivetran managed connector                        | `raw_typeform`   | No repo-local extractor; managed in Fivetran UI                                    |
| Stripe     | Fivetran managed connector                        | `raw_stripe`     | No repo-local extractor; managed in Fivetran UI                                    |

Fivetran-managed sources follow the **same raw-dataset contract** (`raw_<source>.*`) so staging views remain uniform, but their freshness/schedule/SLAs are governed by Fivetran config, not this rule.

**Calendly migration note (Track X, 2026-04-22):** The Fivetran Calendly connector was replaced with a custom Cloud Run Job poller. Fivetran connector is PAUSED (not deleted) as a 30-day rollback path. During the 24h dual-run overlap window, both sources wrote to `raw_calendly.*`; staging handled dedup via `coalesce(_ingested_at, _fivetran_synced)`. After Fivetran was paused, staging was simplified to `_ingested_at` only. See `docs/runbooks/calendly-cloud-run-extractor.md` for ops and rollback procedures.

## Directory shape (Python extractors)

```
1-raw-landing/
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

One source = one `1-raw-landing/<source>/` directory. Do not split a single source across multiple packages.

## Per-source extractor skeleton

```python
# 1-raw-landing/<source>/extract.py
# Run from the 1-raw-landing/ directory so `common` resolves as a local package
# (invalid parent-dir name "1-raw-landing" isn't a Python identifier, so the
# imports below are relative to the package root, not the repo root).
from common.checkpoint import read_checkpoint, write_checkpoint
from common.runs import log_run_start, log_run_end
from common.bq import insert_raw

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

- **Default trigger path:** GitHub Actions `workflow_dispatch` + `schedule:` cron for custom-source ingest. Not Airflow, not Dagster, not a local cron.
- **Workflow manifest:** each source gets its own workflow at `.github/workflows/ingest-<source>.yml` with a daily (or source-appropriate) cron and a manual `workflow_dispatch` input for `--since` backfills.
- **On-demand retry / backfill:** `gh workflow run ingest-<source>.yml -f since=<ISO-8601>` (requires the `gh` CLI authenticated with repo scope).
- **State inspection:** `select * from ops.ingest_checkpoints where source='<source>'` and `select * from ops.ingest_runs where source='<source>' order by started_at desc limit 10`.
- **Fivetran-managed sources** do not have a workflow file — orchestration is configured in the Fivetran dashboard. Only their `raw_<source>` datasets show up in this repo (via dbt sources declarations).

### Near-real-time exception (Cloud Run Jobs)

Custom extractors whose freshness SLA is **sub-5-minute AND dashboard-load-bearing**
may run on Cloud Run Jobs + Cloud Scheduler instead of GitHub Actions cron.
As of 2026-04-22 this applies ONLY to `1-raw-landing/ghl/` hot endpoints
(conversations, messages) at 1-min cadence, with cold endpoints
(contacts, opportunities, users, pipelines) at 15-min cadence.

All other custom extractors stay on the GHA path. The GHA workflow is
retained as a manual backstop via `workflow_dispatch` — flip to it for
emergency backfill if Cloud Scheduler is paused.

Exception criteria (ALL must hold):
- Dashboard tile SLA measures a sub-5-min event (here: 5-min Speed-to-Lead SLA)
- Source API supports polling at the target cadence without exceeding rate limits
  (GHL: 100 req/10s; 1-min hot pull is well within budget)
- Concurrency guard exists (BQ advisory lock in `raw_ghl._job_locks`, no file locks —
  Cloud Run Jobs are stateless; file locks don't survive container boundaries)
- Terraform-managed (not clicked in the GCP console); state lives under
  `1-raw-landing/deploy/<source>/terraform/`

**Why we deviate:** the headline STL metric is a 5-minute SLA measurement. A dashboard
that shows this metric on 5-minute-stale data is structurally incapable of catching a
live 5-minute-SLA miss. 1-min freshness is a product requirement, not an engineering
preference. David has signed off. (Track W, 2026-04-22)

**Corpus grounding:** the corpus confirms that double-ingestion risk during scheduler
migration is handled by the pipeline's native idempotency (append-only +
downstream staging dedupe) and that a sequenced atomic cutover (disable old scheduler,
then switch new scheduler to prod) eliminates cursor-state corruption risk.
(source: `.claude/rules/ingest.md` ingestion contract + "Why Data Migrations Go Wrong
(3 reasons)", Data Ops notebook)

## Do not

- Run `python 1-raw-landing/<source>/extract.py` from your local shell against production BigQuery. Production ingest runs via GitHub Actions only. Local runs MUST target a dev BigQuery project.
- Mutate `raw_*` tables by hand — if a correction is needed, write a targeted backfill and run it through the workflow.
- Join across sources at the ingest layer. That belongs in warehouse bridges.
- Split a single source across multiple directories. One source = one `1-raw-landing/<source>/` dir.
- Add credentials or service-account JSON to the repo. If a secret leaks into a commit, treat it as compromised and rotate immediately.

## Lessons learned

*(Populate as ingest issues arise.)*
