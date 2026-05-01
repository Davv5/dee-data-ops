# Pause Legacy `calendly-poll`

Last updated: 2026-05-01.

Purpose: safely pause the legacy every-minute `calendly-poll` Scheduler trigger in `dee-data-ops-prod` without deleting the Cloud Run Job, image, secrets, or legacy BigQuery tables.

This runbook is based on:

- `docs/discovery/cloud-project-provenance-map.md`
- `docs/discovery/duplicate-data-audit-2026-05-01.md`
- `docs/discovery/legacy-runtime-audit-2026-05-01.md`
- `docs/discovery/source-id-comparison-audit-2026-05-01.md`

## Decision

`calendly-poll` is the first legacy pause candidate.

Why:

- It runs every minute in legacy project `dee-data-ops-prod`.
- It writes to legacy dataset `dee-data-ops.raw_calendly`.
- It has recent quota/timeout strain.
- Current project `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw` has complete core Calendly coverage for the checked dashboard-critical entities:
  - `scheduled_events`: 5,488 current IDs; matched 100% against legacy `raw_calendly.event`.
  - `event_invitees`: 5,488 current IDs; matched 100% against legacy `raw_calendly.event_invitee`.
  - current invitee payloads contain `questions_and_answers` on all 5,488 invitee rows.

Do not delete anything in this runbook. Pause only the Scheduler trigger.

## Current State Snapshot

Captured 2026-05-01 around 18:47 UTC.

Scheduler:

```text
project:     dee-data-ops-prod
location:    us-central1
job:         calendly-poll
state:       ENABLED
schedule:    * * * * *
time zone:   UTC
target:      https://us-central1-run.googleapis.com/v2/projects/dee-data-ops-prod/locations/us-central1/jobs/calendly-poll:run
SA:          cloud-scheduler@dee-data-ops-prod.iam.gserviceaccount.com
last attempt: 2026-05-01T18:47:04Z
```

Cloud Run Job:

```text
project: dee-data-ops-prod
region:  us-central1
job:     calendly-poll
image:   us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/calendly-extractor:latest
SA:      ingest-prod@dee-data-ops-prod.iam.gserviceaccount.com
timeout: 120s
env:
  GCP_PROJECT_ID_DEV=dee-data-ops
  GCP_SECRET_MANAGER_PROJECT=dee-data-ops-prod
```

Recent execution baseline:

| Execution | Created UTC | Completion UTC | Status |
|---|---|---|---|
| `calendly-poll-nkm8s` | 2026-05-01 18:47:04 | pending at capture | Unknown |
| `calendly-poll-ch6fp` | 2026-05-01 18:46:05 | pending at capture | Unknown |
| `calendly-poll-vzbp2` | 2026-05-01 18:45:05 | 2026-05-01 18:46:18 | True |
| `calendly-poll-cgdvz` | 2026-05-01 18:36:05 | 2026-05-01 18:42:46 | False |

Current consolidated project baseline:

| Surface | Entity/table | Rows | Freshness |
|---|---|---:|---|
| `Raw.calendly_objects_raw` | `scheduled_events` | 5,488 | max `ingested_at` 2026-05-01 18:33:17 |
| `Raw.calendly_objects_raw` | `event_invitees` | 5,488 | max `ingested_at` 2026-05-01 18:33:23 |
| `Raw.calendly_objects_raw` | `event_types` | 13 | max `ingested_at` 2026-05-01 18:33:10 |
| `Marts.fct_speed_to_lead` | table | 17,751 | modified 2026-05-01 17:57:48 |
| `Marts.mrt_speed_to_lead_daily` | table | 521 | modified 2026-05-01 17:57:50 |
| `Marts.mrt_speed_to_lead_overall` | table | 1 | modified 2026-05-01 17:57:52 |
| `Marts.rpt_speed_to_lead_week` | table | 386 | modified 2026-05-01 17:59:04 |

Legacy baseline:

| Surface | Entity/table | Rows/freshness |
|---|---|---|
| `dee-data-ops.raw_calendly._sync_state` | `scheduled_events` | 5,345 rows, max `updated_at` 2026-05-01 18:45:32 |

## Pre-Pause Checks

Run these read-only checks immediately before pausing.

Confirm Scheduler is still enabled:

```bash
gcloud scheduler jobs describe calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod \
  --format='value(state,schedule,timeZone,lastAttemptTime)'
```

Confirm current raw Calendly freshness in the consolidated project:

```bash
bq query --use_legacy_sql=false --project_id=project-41542e21-470f-4589-96d '
SELECT
  entity_type,
  COUNT(*) AS row_count,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type IN ("scheduled_events", "event_invitees", "event_types")
GROUP BY entity_type
ORDER BY entity_type
'
```

Confirm current Speed-to-Lead report tables are fresh:

```bash
bq query --use_legacy_sql=false --project_id=project-41542e21-470f-4589-96d '
SELECT
  table_id,
  row_count,
  TIMESTAMP_MILLIS(last_modified_time) AS latest_modified
FROM `project-41542e21-470f-4589-96d.Marts.__TABLES__`
WHERE table_id IN (
  "mrt_speed_to_lead_overall",
  "mrt_speed_to_lead_daily",
  "fct_speed_to_lead",
  "rpt_speed_to_lead_week"
)
ORDER BY table_id
'
```

## Pause

Pause only the Scheduler trigger:

```bash
gcloud scheduler jobs pause calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod
```

Do not delete:

- Cloud Scheduler job
- Cloud Run Job
- Artifact Registry image
- Secret Manager secret
- BigQuery legacy raw tables

## Immediate Verification

Confirm Scheduler state:

```bash
gcloud scheduler jobs describe calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod \
  --format='value(state,schedule,timeZone,lastAttemptTime)'
```

Expected:

```text
PAUSED    * * * * *    UTC    <last attempt before pause>
```

Confirm no new legacy executions are being created:

```bash
gcloud run jobs executions list \
  --job=calendly-poll \
  --region=us-central1 \
  --project=dee-data-ops-prod \
  --limit=5 \
  --format='table(name,metadata.creationTimestamp,status.completionTime,status.succeededCount,status.failedCount,status.conditions[0].status)'
```

## One-Hour Watch

For one hour after pausing, check these at least twice.

Current raw Calendly freshness:

```bash
bq query --use_legacy_sql=false --project_id=project-41542e21-470f-4589-96d '
SELECT
  entity_type,
  COUNT(*) AS row_count,
  MAX(ingested_at) AS max_ingested_at
FROM `project-41542e21-470f-4589-96d.Raw.calendly_objects_raw`
WHERE entity_type IN ("scheduled_events", "event_invitees", "event_types")
GROUP BY entity_type
ORDER BY entity_type
'
```

Current Speed-to-Lead report freshness:

```bash
bq query --use_legacy_sql=false --project_id=project-41542e21-470f-4589-96d '
SELECT
  table_id,
  row_count,
  TIMESTAMP_MILLIS(last_modified_time) AS latest_modified
FROM `project-41542e21-470f-4589-96d.Marts.__TABLES__`
WHERE table_id IN (
  "mrt_speed_to_lead_overall",
  "mrt_speed_to_lead_daily",
  "fct_speed_to_lead",
  "rpt_speed_to_lead_week"
)
ORDER BY table_id
'
```

Dashboard/API smoke target once the dashboard has live wiring:

```bash
curl -s http://localhost:3000/api/health
```

## Rollback

Resume the Scheduler trigger:

```bash
gcloud scheduler jobs resume calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod
```

Confirm state:

```bash
gcloud scheduler jobs describe calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod \
  --format='value(state,schedule,timeZone,lastAttemptTime)'
```

Optionally trigger one manual run only if needed:

```bash
gcloud run jobs execute calendly-poll \
  --region=us-central1 \
  --project=dee-data-ops-prod
```

## Rollback Triggers

Resume `calendly-poll` if any of these are observed after pause:

- current `Raw.calendly_objects_raw` stops refreshing while current ingestion should be active
- Speed-to-Lead marts stop refreshing and the failure is traced to missing Calendly raw
- dashboard/API read path unexpectedly references `dee-data-ops.raw_calendly`
- a required Calendly Q&A field is missing from current invitee payloads despite the audit finding

Do not use this runbook to pause `ghl-hot` or `ghl-cold`.
