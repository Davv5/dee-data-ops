# Cloud Project Provenance Map

Last updated: 2026-05-01.

Purpose: label the D-DEE cloud/project sprawl before any more mart, dashboard, or ingestion work. This is a truth-labeling document, not a deletion plan.

## Executive Read

D-DEE currently has one intended home and two legacy reference projects:

| Project | Label | Current role |
|---|---|---|
| `project-41542e21-470f-4589-96d` | **Current consolidated project** | Active BigQuery target plus active Cloud Run/Scheduler runtime for the consolidated ingestion and marts path. |
| `dee-data-ops-prod` | **Legacy prod / rollback reference** | Older clean-rebuild prod BigQuery layer and legacy minute-level Cloud Run jobs. Do not use as the default for new work. |
| `dee-data-ops` | **Legacy dev/raw reference** | Older raw/dev BigQuery project. Large raw datasets remain here, but Cloud Run and Scheduler APIs are disabled. |
| `nice-karma-314717` | **Unrelated / unclassified** | Accessible project named "My First Project"; no BigQuery datasets and Cloud Run API disabled in this audit. |

The local `gcloud` default still points at `dee-data-ops-prod`, so any command that omits `--project` or `--project_id` is suspect.

## Hard Operating Rule

All new D-DEE work must pass the project explicitly:

```bash
--project=project-41542e21-470f-4589-96d
--project_id=project-41542e21-470f-4589-96d
```

Do not rely on the local default project until the machine is intentionally reconfigured.

## BigQuery Footprint

Read-only audit source:

- `bq ls --project_id=<project>`
- per-dataset `__TABLES__` summaries queried on 2026-05-01

### Current Consolidated Project

`project-41542e21-470f-4589-96d`

| Dataset | Table count | Row total | Latest modified UTC | Interpretation |
|---|---:|---:|---|---|
| `Raw` | 33 | 359,461 | 2026-05-01 17:29:45 | Active raw landing for consolidated pipeline. |
| `STG` | 53 | 201 | 2026-05-01 17:29:49 | Active uppercase staging dataset used by inline SQL/dbt-adjacent paths. Low row total is expected for views/metadata-style tables. |
| `Core` | 73 | 405,331 | 2026-05-01 17:30:07 | Active modeled core. |
| `Marts` | 52 | 138,540 | 2026-05-01 17:00:35 | Active dashboard/report serving dataset. Includes bq-ingest report tables. |
| `raw_ghl` | 15 | 61,720 | 2026-04-24 04:04:25 | Legacy-shaped raw copy retained in current project. |
| `raw_calendly` | 6 | 0 | 2026-04-19 15:43:44 | Legacy-shaped raw copy retained in current project. |
| `staging` | 17 | 29 | 2026-04-29 19:48:42 | Lowercase dbt-era residue; do not treat as authoritative without checking ownership. |
| `warehouse` | 14 | 35,808 | 2026-05-01 13:04:20 | Lowercase dbt-era residue; likely transition/parity artifact. |

### Legacy Prod / Rollback Reference

`dee-data-ops-prod`

| Dataset | Table count | Row total | Latest modified UTC | Interpretation |
|---|---:|---:|---|---|
| `marts` | 5 | 40,085 | 2026-04-23 11:56:38 | Old clean-rebuild mart outputs; useful for Speed-to-Lead parity, not new work. |
| `warehouse` | 13 | 71,358 | 2026-04-23 11:56:24 | Old clean-rebuild warehouse outputs. |
| `staging` | 13 | 29 | 2026-04-23 11:55:37 | Old clean-rebuild staging views/tables. |
| `snapshots` | 1 | 16 | 2026-04-23 11:55:59 | Old snapshot residue. |
| `validation` | 6 | 172 | 2026-04-23 11:55:23 | Old validation artifacts. |

### Legacy Dev / Raw Reference

`dee-data-ops`

| Dataset | Table count | Row total | Latest modified UTC | Interpretation |
|---|---:|---:|---|---|
| `raw_ghl` | 8 | 35,204,444 | 2026-05-01 17:32:15 | Very large legacy raw GHL estate. Must be mapped before deletion or migration claims. |
| `raw_calendly` | 20 | 108,595 | 2026-05-01 17:32:10 | Legacy raw Calendly estate. |
| `raw_stripe` | 97 | 718,621 | 2026-05-01 14:10:49 | Legacy raw Stripe estate. Stripe is historical-only for business direction, but data may still be useful for history. |
| `raw_typeform` | 19 | 152,333 | 2026-05-01 14:11:17 | Legacy raw Typeform estate. |
| `dev_david` | 48 | 112,550 | 2026-04-28 14:37:28 | Personal/dev dbt output residue. |
| `raw_fanbasis` | 1 | 0 | 2026-04-19 19:59:11 | Empty legacy Fanbasis raw dataset. |
| `ci` | 0 | null | null | Empty CI residue. |

## Runtime Footprint

Read-only audit source:

- `gcloud run services list --region=us-central1`
- `gcloud run jobs list --region=us-central1`
- `gcloud scheduler jobs list --location=us-central1`

### Current Consolidated Project Runtime

`project-41542e21-470f-4589-96d`

Cloud Run services:

- `bq-ingest`
- `gtm-warehouse-mcp-phase0`

Cloud Run jobs include current ingestion/modeling jobs such as:

- `ghl-*`
- `calendly-*`
- `fanbasis-*`
- `fathom-*`
- `stripe-backfill`
- `typeform-backfill`
- `pipeline-run`
- `pipeline-marts-hourly`
- `dq-tests`

Cloud Scheduler has 19 enabled jobs, including hourly ingestion, marts refresh, daily backfills, and warehouse health checks.

Interpretation: this is the active runtime estate for the consolidated project.

### Legacy Prod Runtime

`dee-data-ops-prod`

Cloud Run services:

- none listed

Cloud Run jobs:

- `calendly-poll`
- `ghl-cold`
- `ghl-hot`

Cloud Scheduler:

- `calendly-poll` every minute
- `ghl-hot` every minute
- `ghl-cold` every 15 minutes

Interpretation: legacy runtime is still alive. Do not delete it casually, but do not build new product paths on it.

### Legacy Dev Runtime

`dee-data-ops`

Cloud Run and Cloud Scheduler APIs are disabled. This project appears to be BigQuery data/reference, not active Cloud Run runtime.

## Local Clone Footprint

Read-only audit source:

- `find /Users/david/Documents -maxdepth 2 -name .git -type d`
- `git remote -v`

Relevant local repos:

| Local path | Remote | Label |
|---|---|---|
| `/Users/david/Documents/data ops` | `Davv5/dee-data-ops` | Canonical active repo. |
| `/Users/david/Documents/gtm-lead-warehouse` | `heidyforero1/gtm-lead-warehouse` | Legacy/original ingestion repo. |
| `/Users/david/Documents/fanbasis-ingest` | `heidyforero1/gtm-lead-warehouse` | Duplicate clone of the legacy/original ingestion repo. |
| `/Users/david/Documents/cabinet` | no remote shown | Dashboard app starting-point reference. |

Interpretation: stale local clones are a real risk. Future agents should treat `/Users/david/Documents/data ops` as the canonical repo unless David explicitly asks to inspect another clone.

## Cleanup Doctrine

Do not start over blindly. The current consolidated project has active data and runtime, and the legacy projects still contain evidence and rollback value.

Clean up in this order:

1. **Label:** keep this provenance map current and cite it in future plans.
2. **Route:** require explicit project IDs in commands and docs.
3. **Compare:** map duplicate raw/report tables before deleting anything.
4. **Migrate:** move only the pieces that are still useful into the current project/repo.
5. **Quarantine:** pause or archive legacy runtimes after a replacement path is verified.
6. **Delete:** only after a written decommission checklist and rollback window.

## Immediate Product Direction

For the dashboard product:

- New app work lives under `3-bi/dashboard/`.
- First dashboard chapter is Speed-to-Lead.
- First live read path should use current-project `Marts.*` Speed-to-Lead report tables as a pragmatic v1.
- The durable modeling direction remains dbt under `2-dbt/`.
- Metabase is a historical parity/reference surface, not the product surface.

## Open Questions

- Which `dee-data-ops.raw_*` tables still contain data not represented in `project-41542e21-470f-4589-96d.Raw`?
- Are `dee-data-ops-prod` minute-level jobs still writing useful data, or are they duplicating consolidated jobs?
- Which lowercase datasets in the current project are still read by anything?
- Should the local `gcloud` default be switched from `dee-data-ops-prod` to `project-41542e21-470f-4589-96d` after the next PR lands?

## Follow-up Audits

- `docs/discovery/duplicate-data-audit-2026-05-01.md` — read-only duplicate/overlap audit across current and legacy BigQuery projects.
- `docs/discovery/legacy-runtime-audit-2026-05-01.md` — read-only classification of the three active legacy jobs in `dee-data-ops-prod`.
