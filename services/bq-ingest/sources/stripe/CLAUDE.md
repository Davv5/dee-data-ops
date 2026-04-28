# Stripe — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Stripe Index | `docs/sources/stripe/INDEX.md` | Any Stripe work |
| On trigger | SQL Models | `sql/stripe_models.sql` | schema, query, SQL, model, raw_, core_, stripe_ |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run, trigger |

## Files

| File | Purpose |
|------|---------|
| `sources/stripe/stripe_pipeline.py` | Incremental ingest + model runner |
| `sources/stripe/stripe_backfill.py` | Historical backfill for all Stripe object types |
