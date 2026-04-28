# Typeform — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Typeform Index | `docs/sources/typeform/INDEX.md` | Any Typeform work |
| On trigger | SQL Models | `sql/typeform_models.sql` | schema, query, SQL, model, raw_, core_, typeform_ |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run, trigger |

## Files

| File | Purpose |
|------|---------|
| `sources/typeform/typeform_pipeline.py` | Incremental ingest + model runner |
| `sources/typeform/typeform_backfill.py` | Full historical backfill per form (state-tracked, resumable) |
