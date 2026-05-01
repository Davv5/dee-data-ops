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
| `fct_speed_to_lead` | 17,751 | 2026-05-01 17:57:48 |
| `mrt_speed_to_lead_daily` | 521 | 2026-05-01 17:57:50 |
| `mrt_speed_to_lead_overall` | 1 | 2026-05-01 17:57:52 |
| `rpt_speed_to_lead_week` | 386 | 2026-05-01 17:59:04 |

## Watch Status

Immediate checkpoint: healthy.

One-hour watch: pending. A thread heartbeat was scheduled to complete the one-hour post-pause watch and document the final outcome.

## Rollback

If the one-hour watch finds broken freshness or an unexpected dashboard/dbt dependency on `dee-data-ops.raw_calendly`, resume the Scheduler:

```bash
gcloud scheduler jobs resume calendly-poll \
  --location=us-central1 \
  --project=dee-data-ops-prod
```
