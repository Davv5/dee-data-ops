# Legacy Runtime Audit — `dee-data-ops-prod`

Date: 2026-05-01.

Purpose: classify the three remaining legacy Cloud Run Jobs in `dee-data-ops-prod` before pausing, deleting, migrating, or wiring dashboard assumptions around them.

This audit is read-only. No Scheduler jobs or Cloud Run jobs were changed.

Companion docs:

- `docs/discovery/cloud-project-provenance-map.md`
- `docs/discovery/duplicate-data-audit-2026-05-01.md`
- `docs/discovery/current-data-layer-truth-map.md`
- `docs/discovery/source-id-comparison-audit-2026-05-01.md`
- `docs/runbooks/pause-legacy-calendly-poll.md`

## Short Answer

The three legacy jobs are still active and still writing to the old raw project:

```text
runtime project: dee-data-ops-prod
write target:    dee-data-ops.raw_ghl / dee-data-ops.raw_calendly
secret project:  dee-data-ops-prod
```

They are not the current dashboard path, but they are not harmless historical residue either.

Do not delete them yet. Do not let new dashboard work depend on them. Treat them as legacy raw-history writers to classify source-by-source.

## Jobs Audited

| Job | Scheduler cadence | Runtime image | Args | Writes |
|---|---|---|---|---|
| `ghl-hot` | every minute | `us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/ghl-extractor:latest` | `--endpoints conversations,messages` | `dee-data-ops.raw_ghl.*` |
| `ghl-cold` | every 15 minutes | `us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/ghl-extractor:latest` | `--endpoints contacts,opportunities,users,pipelines` | `dee-data-ops.raw_ghl.*` |
| `calendly-poll` | every minute | `us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/calendly-extractor:latest` | default/all endpoints | `dee-data-ops.raw_calendly.*` |

All three use:

```text
service account: ingest-prod@dee-data-ops-prod.iam.gserviceaccount.com
secret project:  GCP_SECRET_MANAGER_PROJECT=dee-data-ops-prod
write project:   GCP_PROJECT_ID_DEV=dee-data-ops
```

Schedulers use:

```text
service account: cloud-scheduler@dee-data-ops-prod.iam.gserviceaccount.com
target: https://us-central1-run.googleapis.com/v2/projects/dee-data-ops-prod/locations/us-central1/jobs/<job>:run
```

## Current Status

### `ghl-hot`

Status:

- Scheduler enabled every minute.
- Job had 12,000+ executions at audit time.
- Recent executions mostly completed successfully.
- Latest observed executions were still running/completing around 2026-05-01 18:15 UTC.

Sync-state summary:

| Endpoint | State rows | Max last synced UTC | Max updated UTC |
|---|---:|---|---|
| `conversations` | 1,386 | 2026-05-01 16:21:05 | 2026-05-01 16:21:10 |
| `messages` | 1,376 | 2026-05-01 16:21:05 | 2026-05-01 16:21:16 |

Target table freshness:

| Table | Rows | Latest modified UTC |
|---|---:|---|
| `dee-data-ops.raw_ghl.conversations` | 18,371 | 2026-05-01 16:21:08 |
| `dee-data-ops.raw_ghl.messages` | 76,922 | 2026-05-01 16:21:15 |

Classification:

```text
keep temporarily; migrate/compare before pausing
```

Reason: this job appears to be the active writer for the legacy messages/conversations estate. Current consolidated `Raw.ghl_objects_raw` does not obviously contain the same message-level shape. Pausing this before a message/conversation comparison risks losing a raw-history stream that may explain old Speed-to-Lead behavior.

### `ghl-cold`

Status:

- Scheduler enabled every 15 minutes.
- Recent executions completed successfully.
- Latest observed successful execution completed around 2026-05-01 18:04 UTC.

Sync-state summary:

| Endpoint | State rows | Max last synced UTC | Max updated UTC |
|---|---:|---|---|
| `contacts` | 832 | 2026-05-01 18:01:31 | 2026-05-01 18:02:20 |
| `opportunities` | 820 | 2026-05-01 18:01:31 | 2026-05-01 18:03:50 |
| `pipelines` | 816 | 2026-05-01 18:01:31 | 2026-05-01 18:03:56 |
| `users` | 818 | 2026-05-01 18:01:31 | 2026-05-01 18:04:04 |

Target table freshness:

| Table | Rows | Latest modified UTC |
|---|---:|---|
| `dee-data-ops.raw_ghl.opportunities` | 21,797,405 | 2026-05-01 18:03:50 |
| `dee-data-ops.raw_ghl.contacts` | 13,374,881 | 2026-05-01 18:02:18 |
| `dee-data-ops.raw_ghl.pipelines` | 29,376 | 2026-05-01 18:03:55 |
| `dee-data-ops.raw_ghl.users` | 13,104 | 2026-05-01 18:04:02 |

Classification:

```text
pause candidate after source-id comparison
```

Reason: current consolidated `Raw.ghl_objects_raw` already contains current contacts, opportunities, users, pipelines, stages, and forms in the new target project. The legacy cold job is likely duplicating active-source ingestion at a much higher append/version grain. Do not pause it until a short ID-level comparison confirms the current project has the business-current contact/opportunity/user/pipeline coverage needed by dbt and dashboard work.

### `calendly-poll`

Status:

- Scheduler enabled every minute.
- Recent executions are mixed: several completed successfully, but recent failures include timeout and BigQuery quota errors.
- The job has a 120-second timeout and can overlap its one-minute schedule.

Recent error cluster:

```text
Terminating task because it has reached the maximum timeout of 120 seconds.
Quota exceeded: Your table exceeded quota for imports or query appends per table.
```

Sync-state summary:

| Endpoint | State rows | Max last synced UTC | Max updated UTC |
|---|---:|---|---|
| `scheduled_events` | 5,330 | 2026-05-01 18:14:23 | 2026-05-01 18:14:27 |

Target table freshness:

| Table | Rows | Latest modified UTC |
|---|---:|---|
| `dee-data-ops.raw_calendly.scheduled_events` | 58,805 | 2026-05-01 18:14:26 |
| `dee-data-ops.raw_calendly.question_and_answer` | 22,274 | 2026-05-01 14:15:41 |
| `dee-data-ops.raw_calendly.event` | 5,488 | 2026-05-01 14:15:36 |
| `dee-data-ops.raw_calendly.event_invitee` | 5,488 | 2026-05-01 14:15:36 |

Classification:

```text
highest-priority pause/migration candidate
```

Reason: this job is actively touching legacy raw tables every minute and is showing quota/timeout strain. Current consolidated `Raw.calendly_objects_raw` has current `scheduled_events`, `event_invitees`, and `event_types`, but the legacy raw estate also contains richer per-object child tables such as `question_and_answer`. Pause only after confirming dashboard/current dbt does not require the legacy child-table detail.

## Risk Summary

| Risk | Severity | Why |
|---|---|---|
| Old jobs keep writing to old raw datasets | High | Future agents can mistake fresh legacy data for current truth. |
| `calendly-poll` quota/timeouts | High | Every-minute schedule plus append/query limits are already failing intermittently. |
| GHL message/conversation loss if paused too early | Medium | Legacy `raw_ghl.messages` may preserve detail not present in current unified raw. |
| Cost/noise from duplicated GHL cold ingestion | Medium | Millions of legacy rows continue accumulating outside the current project. |

## Recommended Next Actions

The first source-ID comparison pass is now captured in `docs/discovery/source-id-comparison-audit-2026-05-01.md`.

1. **Do not build the dashboard on these legacy jobs.** New dashboard work should read `project-41542e21-470f-4589-96d.Marts.*`.
2. **Run ID-level comparison for GHL cold objects.** Compare current `Raw.ghl_objects_raw` contacts/opportunities/users/pipelines against `dee-data-ops.raw_ghl.*` latest IDs.
3. **Run detail comparison for GHL hot objects.** Determine whether current project has the message/conversation fields needed for old Speed-to-Lead and sales activity analysis.
4. **Run Calendly detail comparison.** Decide whether legacy `question_and_answer` or other child tables still matter.
5. **Then pause `calendly-poll` first if current coverage is sufficient.** It is the noisiest/riskiest legacy job because it is both frequent and erroring.
6. **Create a decommission checklist before pausing GHL jobs.** The legacy GHL raw estate is large enough to deserve a deliberate checkpoint.

## Decision

Current classification:

| Job | Classification | Immediate action |
|---|---|---|
| `ghl-hot` | keep temporarily; migrate/compare before pausing | No mutation. Compare messages/conversations first. |
| `ghl-cold` | pause candidate after source-id comparison | No mutation. Compare contacts/opportunities/users/pipelines first. |
| `calendly-poll` | highest-priority pause/migration candidate | No mutation. Compare Calendly child-detail needs first. |

The next cleanup PR should focus on source-id comparisons, not dashboard UI.
