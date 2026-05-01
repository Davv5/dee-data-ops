# Pause Execution Log — Legacy `calendly-poll`

Date: 2026-05-01.

Runbook executed: `docs/runbooks/pause-legacy-calendly-poll.md`.

## Action Taken

Paused only the legacy Cloud Scheduler trigger:

```bash
gcloud scheduler jobs pause calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod
```

No Cloud Run Job, BigQuery table, image, secret, or service account was changed.

## Pause Verification

Immediate post-pause Scheduler state:

```text
PAUSED    * * * * *    UTC
```

Recent legacy executions immediately after pause:

| Execution | Created UTC | Completion UTC | Status |
|---|---|---|---|
| `calendly-poll-jsj6m` | 2026-05-01 18:53:05 | 2026-05-01 18:54:49 | False |
| `calendly-poll-jcvgf` | 2026-05-01 18:52:04 | 2026-05-01 18:54:47 | False |
| `calendly-poll-nd449` | 2026-05-01 18:51:05 | 2026-05-01 18:53:21 | False |
| `calendly-poll-hljrx` | 2026-05-01 18:50:06 | 2026-05-01 18:51:43 | True |

Interpretation: the latest visible executions were created before the pause boundary and finished out afterward. No new post-pause execution appeared in the immediate verification window.

## Current Project Baseline

Current consolidated Calendly raw:

| Entity | Rows | Max ingested UTC |
|---|---:|---|
| `event_invitees` | 5,488 | 2026-05-01 18:33:23 |
| `event_types` | 13 | 2026-05-01 18:33:10 |
| `scheduled_events` | 5,488 | 2026-05-01 18:33:17 |

Current Speed-to-Lead marts:

| Table | Rows | Latest modified UTC |
|---|---:|---|
| `fct_speed_to_lead` | 17,751 | 2026-05-01 18:58:20 |
| `mrt_speed_to_lead_daily` | 521 | 2026-05-01 18:58:22 |
| `mrt_speed_to_lead_overall` | 1 | 2026-05-01 18:58:23 |
| `rpt_speed_to_lead_week` | 386 | 2026-05-01 18:59:35 |

## Current-State Check

Checked at 2026-05-01 19:04 UTC at David's request instead of waiting for a full one-hour soak.

Scheduler state:

```text
PAUSED    * * * * *    UTC
```

Recent executions:

| Execution | Created UTC | Completion UTC | Status |
|---|---|---|---|
| `calendly-poll-jsj6m` | 2026-05-01 18:53:05 | 2026-05-01 18:54:49 | False |
| `calendly-poll-jcvgf` | 2026-05-01 18:52:04 | 2026-05-01 18:54:47 | False |
| `calendly-poll-nd449` | 2026-05-01 18:51:05 | 2026-05-01 18:53:21 | False |
| `calendly-poll-hljrx` | 2026-05-01 18:50:06 | 2026-05-01 18:51:43 | True |

Interpretation: no execution was created after the pause boundary. Current project Calendly raw remains present, and Speed-to-Lead mart tables refreshed after the pause.

## Watch Status

Current-state check: healthy.

The full one-hour soak was not required for this decision; David asked to check current state instead. Keep the rollback command below available until the next normal pipeline refresh confirms continued health.

## Rollback

If a later check finds broken freshness or an unexpected dashboard/dbt dependency on `dee-data-ops.raw_calendly`, resume the Scheduler:

```bash
gcloud scheduler jobs resume calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod
```
