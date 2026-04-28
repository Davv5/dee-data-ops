# GHL — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | GHL Index | `docs/sources/ghl/INDEX.md` | Any GHL work |
| On trigger | SQL Models | `sql/ghl_models.sql` | schema, query, SQL, model, raw_, core_, ghl_objects |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume, state |
| On trigger | Comprehensive Backfill | `docs/runbooks/GHL_COMPREHENSIVE_BACKFILL.md` | comprehensive, messages, notes, tasks, call logs, form submissions |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run, trigger |

## Files

| File | Purpose |
|------|---------|
| `sources/ghl/ghl_pipeline.py` | Incremental ingest + model runner |
| `sources/ghl/ghl_backfill.py` | Historical backfill (contacts, opportunities, pipelines) |
| `sources/ghl/ghl_call_log_backfill.py` | Date-windowed call log backfill |
| `sources/ghl/ghl_form_submissions_backfill.py` | Date-windowed form submissions backfill |
| `sources/ghl/ghl_comprehensive_backfill.py` | Multi-entity comprehensive backfill (messages, notes, tasks, call logs, form submissions) |
