---
paths: ["**"]
---

# GCP Project Routing

Load this rule before any command, doc, or code change that touches GCP, BigQuery, Cloud Run, Cloud Scheduler, dbt profiles, dashboard data access, or ingestion.

## Current Labels

- `project-41542e21-470f-4589-96d` is the **current consolidated D-DEE project**.
- `dee-data-ops-prod` is **legacy prod / rollback reference**.
- `dee-data-ops` is **legacy dev/raw reference**.
- `nice-karma-314717` is **unclassified / unrelated unless proven otherwise**.

Canonical map: `docs/discovery/cloud-project-provenance-map.md`.

## Hard Rule

Never rely on the local gcloud default for D-DEE work. The local default may still point at `dee-data-ops-prod`.

Always pass the project explicitly:

```bash
gcloud ... --project=project-41542e21-470f-4589-96d
bq ... --project_id=project-41542e21-470f-4589-96d
```

For SQL, fully qualify current-project objects:

```sql
`project-41542e21-470f-4589-96d.<dataset>.<table>`
```

## Legacy Project Handling

Use `dee-data-ops-prod` and `dee-data-ops` only for:

- read-only audit
- parity checks
- rollback/reference investigation
- written migration or decommission work

Do not create new dashboard, dbt, ingestion, or scheduler paths against legacy projects.

Do not delete, pause, or retarget legacy runtime unless a specific decommission checklist exists and David has asked for that action.

## Dashboard Routing

New dashboard product work belongs under `3-bi/dashboard/` and should read from the current consolidated project.

Metabase files and legacy Metabase docs are historical reference only unless David explicitly asks for Metabase recovery or comparison.
