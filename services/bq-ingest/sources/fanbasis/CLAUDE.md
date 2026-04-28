# Fanbasis — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Fanbasis Index | `docs/sources/fanbasis/INDEX.md` | Any Fanbasis work |
| On trigger | SQL Models | `sql/models.sql` | schema, query, SQL, model, raw_, core_, fanbasis_ |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run, trigger |

## Files

| File | Purpose |
|------|---------|
| `sources/fanbasis/fanbasis_pipeline.py` | Incremental ingest (transactions) + model runner |
| `sources/fanbasis/fanbasis_backfill.py` | Historical transaction backfill |
