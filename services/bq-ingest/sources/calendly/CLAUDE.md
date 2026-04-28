# Calendly — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Calendly Index | `docs/sources/calendly/INDEX.md` | Any Calendly work |
| On trigger | SQL Models | `sql/calendly_models.sql` | schema, query, SQL, model, raw_, core_, calendly_ |
| On trigger | Calendly Guardrails | `docs/runbooks/CALENDLY_GUARDRAILS.md` | webhook, invitee, booking, guardrail, dedup |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run, trigger |

## Files

| File | Purpose |
|------|---------|
| `sources/calendly/calendly_pipeline.py` | Incremental ingest + webhook handler + model runner |
| `sources/calendly/calendly_backfill.py` | Historical backfill for events and invitees |
| `sources/calendly/calendly_invitee_drain.py` | Invitee-only drain (legacy entrypoint, delegates to backfill) |
