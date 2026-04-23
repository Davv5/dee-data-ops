# Calendly Cloud Run Extractor — Ops Runbook

Introduced in Track X (2026-04-22) to replace the Fivetran Calendly connector
with a custom Cloud Run Job at 1-min cadence.

Mirrors the GHL runbook (`docs/runbooks/ghl-cloud-run-extractor.md`).

## Infrastructure summary

| Resource                 | Name                | Project            |
|--------------------------|---------------------|--------------------|
| Cloud Run Job            | `calendly-poll`     | `dee-data-ops-prod` |
| Cloud Scheduler          | `calendly-poll`     | `dee-data-ops-prod` |
| Artifact Registry image  | `ingest/calendly-extractor` | `dee-data-ops-prod` |
| BQ state table           | `dee-data-ops.raw_calendly._sync_state` | — |
| BQ lock table            | `dee-data-ops.raw_calendly._job_locks`  | — |
| Secret Manager secret    | `calendly-api-token` | `dee-data-ops-prod` |

**Terraform:** `ops/cloud-run/calendly-extractor/terraform/`

## Pause / Resume the scheduler

```bash
# Pause (stops new executions; existing in-flight jobs finish normally)
gcloud scheduler jobs pause calendly-poll \
  --location=us-central1 --project=dee-data-ops-prod

# Resume
gcloud scheduler jobs resume calendly-poll \
  --location=us-central1 --project=dee-data-ops-prod
```

Use pause/resume (not destroy) for temporary stops — it preserves Terraform state.

## Manual trigger (backfill or spot-check)

```bash
# Trigger all endpoints since a specific date
gcloud run jobs execute calendly-poll \
  --region=us-central1 --project=dee-data-ops-prod \
  --args="--endpoints=scheduled_events,invitees,--since=2026-04-22T00:00:00Z"

# Dry run — exercises BQ client without writing rows
gcloud run jobs execute calendly-poll \
  --region=us-central1 --project=dee-data-ops-prod \
  --args="--dry-run,--since=2026-04-22T00:00:00Z"
```

## Inspect recent executions

```bash
# List last 20 executions
gcloud run jobs executions list \
  --job=calendly-poll --region=us-central1 \
  --project=dee-data-ops-prod --limit=20

# Stream live logs for the most recent execution
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="calendly-poll"' \
  --project=dee-data-ops-prod --limit=100 --format=json | jq '.[] | .jsonPayload'
```

## BQ lock debug (if a run appears stuck)

If Cloud Run logs show `"status": "skip", "reason": "prior run still executing"` and
you believe no run is actually in progress:

```sql
-- Check current lock rows
SELECT * FROM `dee-data-ops.raw_calendly._job_locks`
ORDER BY started_at DESC;
```

Stale locks (> 2 minutes old) expire automatically via the MERGE TTL. If a lock is
genuinely stuck (e.g., container was killed mid-run before the finally block):

```sql
-- Manual lock release (only if you are certain no job is running)
DELETE FROM `dee-data-ops.raw_calendly._job_locks`
WHERE endpoint_group = 'invitee_no_shows,invitees,scheduled_events'
  AND started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
```

## Freshness check SQL

```sql
-- Confirm rows are landing at 1-min cadence
SELECT
  'scheduled_events' AS table_name,
  MAX(_ingested_at) AS latest_ingested_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), MINUTE) AS minutes_since_last_ingest
FROM `dee-data-ops.raw_calendly.scheduled_events`

UNION ALL

SELECT
  'invitees',
  MAX(_ingested_at),
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), MINUTE)
FROM `dee-data-ops.raw_calendly.invitees`

UNION ALL

SELECT
  'invitee_no_shows',
  MAX(_ingested_at),
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_ingested_at), MINUTE)
FROM `dee-data-ops.raw_calendly.invitee_no_shows`
ORDER BY 1;
```

Expected: minutes_since_last_ingest < 2 for all tables during business hours.

## Watermark / state inspection

```sql
-- Check per-endpoint cursors
SELECT endpoint, last_synced_at, updated_at
FROM `dee-data-ops.raw_calendly._sync_state`
ORDER BY updated_at DESC;
```

If a cursor is stuck at an old date, the extractor may be failing silently.
Check Cloud Run logs and consider a manual backfill with `--since`.

## 24h dual-run reconciliation query

Run this after at least 24h of dual-run overlap to confirm Cloud Run has parity
with Fivetran before pausing the connector:

```sql
-- Run against the stg_calendly__events dbt model
SELECT
  _source_path,
  COUNT(*) AS row_count,
  COUNT(DISTINCT event_id) AS unique_events,
  MIN(_ingested_at) AS earliest_event,
  MAX(_ingested_at) AS latest_event
FROM {{ ref('stg_calendly__events') }}
WHERE booked_at >= '2026-04-22'
GROUP BY 1
ORDER BY 1;
```

Expected:
- `cloud_run` row count >= `fivetran` row count (Cloud Run is 1-min vs Fivetran daily).
- Unique event counts should be close or equal.
- If `cloud_run` is missing events `fivetran` caught, debug before pausing.

## Cost estimate

Cloud Run Jobs billing is per request/second of vCPU + memory:
- 1 vCPU, 512 MiB, ~15-30 sec per execution at 1-min cadence.
- ~1440 executions/day.
- Estimated cost: $1-3/month (well within Free Tier for modest job sizes).

Cloud Scheduler: $0.10/job/month = $0.10/month.

Total: <$5/month vs $120/month Fivetran Standard tier for 15-min cadence.

## Fivetran connector retirement

**Current state (Track X cutover):** Fivetran connector PAUSED (not deleted).
Fivetran connector ID: _(David — record the connector ID from Fivetran UI here)_

**30-day observation window:** Do not delete the Fivetran connector for 30 days after
pausing it (until ~2026-05-22). Keep it as an emergency rollback path.

**Steps to retire after 30 days (David action in Fivetran UI):**

1. Confirm `stg_calendly__events._source_path` shows only `cloud_run` rows for
   the past 7 days — no Fivetran-sourced rows remain in the active dedup window.
2. Verify `raw_calendly.event` table is no longer receiving new rows (max `_fivetran_synced`
   is >= 30 days old).
3. In the Fivetran UI: navigate to the Calendly connector and click **Delete**.
4. After deletion, clean up the staging models:
   - Remove the `fivetran_source` CTE and `union all` from `stg_calendly__events.sql`.
   - Remove the `fivetran_source` CTE and `union all` from `stg_calendly__event_invitees.sql`.
   - Remove the `_source_path` column from both models (or keep it as `'cloud_run'` literal).
   - Simplify dedup to `ORDER BY _ingested_at DESC`.
   - Update `_calendly__sources.yml` to remove `event` and `event_invitee` from the tables list.
5. Update `.claude/rules/ingest.md` Calendly note: change "PAUSED" to "DELETED".
6. Append a WORKLOG entry.

**Emergency rollback to Fivetran (within 30-day window):**
1. Resume the Cloud Scheduler (pauses poller, doesn't destroy):
   ```bash
   gcloud scheduler jobs pause calendly-poll --location=us-central1 --project=dee-data-ops-prod
   ```
2. Re-enable the Fivetran connector in the Fivetran UI (connector is paused, not deleted).
3. Fivetran resumes from its last checkpoint automatically.

## Token rotation

See `ingestion/calendly/README.md#token-rotation` for the step-by-step procedure.
No code change or Terraform apply required — the extractor resolves `latest` on each cold start.

## Rate limits

Calendly's API does not publish a hard req/min limit. The extractor uses a
token-bucket throttle at 60 req/min with exponential backoff on 429 responses.

If 429 errors appear in Cloud Run logs:
1. Raise cadence to 2 min by editing `main.tf`:
   - Change `schedule = "* * * * *"` to `schedule = "*/2 * * * *"`.
   - Run `terraform apply`.
2. Note the change here: _(fill in date + reason)_.
