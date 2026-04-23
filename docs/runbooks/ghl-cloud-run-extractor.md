# Runbook: GHL Cloud Run Extractor

Operational reference for the Cloud Run Jobs + Cloud Scheduler setup that
runs the GHL extractor at 1-min (hot) and 15-min (cold) cadence.

Shipped: Track W, 2026-04-22.

## Quick-reference

| Job | Endpoints | Cadence | Timeout | Scheduler name |
|-----|-----------|---------|---------|----------------|
| `ghl-hot` | conversations, messages | every 1 min | 120s | `ghl-hot` |
| `ghl-cold` | contacts, opportunities, users, pipelines | every 15 min | 300s | `ghl-cold` |

Project: `dee-data-ops-prod` | Region: `us-central1` | SA: `ingest@dee-data-ops.iam.gserviceaccount.com`

---

## Pause the scheduler (without stopping the job)

Use this when you need to stop live pulls temporarily (e.g., API key rotation,
GHL maintenance window, debugging a spike):

```bash
gcloud scheduler jobs pause ghl-hot  --location=us-central1 --project=dee-data-ops-prod
gcloud scheduler jobs pause ghl-cold --location=us-central1 --project=dee-data-ops-prod
```

Resume when ready:

```bash
gcloud scheduler jobs resume ghl-hot  --location=us-central1 --project=dee-data-ops-prod
gcloud scheduler jobs resume ghl-cold --location=us-central1 --project=dee-data-ops-prod
```

## Manual one-off run (backfill or on-demand pull)

```bash
# Hot endpoints, standard (reads BQ cursor)
gcloud run jobs execute ghl-hot \
  --region=us-central1 --project=dee-data-ops-prod

# Hot endpoints with a specific since-date (backfill)
gcloud run jobs execute ghl-hot \
  --region=us-central1 --project=dee-data-ops-prod \
  --args="--endpoints=conversations,messages" \
  --args="--since=2026-04-22T00:00:00Z"

# Cold endpoints, standard
gcloud run jobs execute ghl-cold \
  --region=us-central1 --project=dee-data-ops-prod
```

Alternatively, the GHA manual backstop (runs all endpoints via the old path):

```bash
gh workflow run ingest.yml --field source=ghl
```

## Read logs

```bash
# List last 20 executions of ghl-hot
gcloud run jobs executions list \
  --job=ghl-hot --region=us-central1 --project=dee-data-ops-prod --limit=20

# Get details (status, log URI) for a specific execution
gcloud run jobs executions describe <execution-name> \
  --region=us-central1 --project=dee-data-ops-prod

# Stream recent logs from Cloud Logging
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="ghl-hot"' \
  --project=dee-data-ops-prod --limit=50 --order=desc

# Same for cold
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="ghl-cold"' \
  --project=dee-data-ops-prod --limit=50 --order=desc
```

## Inspect the BQ advisory lock (concurrency guard)

The lock table prevents scheduler-overlap double-ingestion. A lock row older
than 2 minutes means a prior run crashed without cleanup (safe to ignore — the
MERGE TTL filter ignores stale rows automatically).

```sql
SELECT * FROM `dee-data-ops.raw_ghl._job_locks`
ORDER BY started_at DESC
LIMIT 10;
```

If a stale lock is stuck (started_at < CURRENT_TIMESTAMP - 2 min), the next
run will self-heal. To clear manually:

```sql
DELETE FROM `dee-data-ops.raw_ghl._job_locks`
WHERE started_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
```

## Verify freshness

Confirm 1-min cadence is working (run after ghl-hot has been running >15 min):

```sql
SELECT
  EXTRACT(MINUTE FROM _ingested_at) AS minute,
  COUNT(*) AS row_count
FROM `dee-data-ops.raw_ghl.conversations`
WHERE _ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY 1
ORDER BY 1;
```

Expect: 10-15 distinct minute values, each with at least 1 row.

## Roll back to GHA cron (full rollback)

1. Pause Cloud Scheduler jobs (above).
2. In `.github/workflows/ingest.yml`, remove the `if:` guard from the
   "Run extractor" step:
   ```yaml
   # Remove this line:
   if: matrix.source != 'ghl' || github.event_name == 'workflow_dispatch'
   ```
3. Restore the 06:00 UTC cron comment (it was always there; the Fanbasis
   cron is still active — GHL will now also run on it).
4. Merge to main. The next 06:00 UTC run pulls GHL via GHA.
5. Verify no gap in `raw_ghl._sync_state` by checking `last_synced_at`.

The Cloud Run Jobs and Terraform state are NOT destroyed in this rollback
— just the scheduler is paused. Re-enable when ready.

## Projected cost

- Cloud Run Jobs: ~43,200 executions/month (hot) + ~2,880 (cold) at
  Cloud Run Jobs pricing. Cold start + ~60s run = negligible cost. Estimate:
  < $5/month total. No standing cost between invocations.
- Artifact Registry: one image repo, a few tags. Negligible.
- Cloud Scheduler: first 3 jobs/month free; $0.10/job/month after. Two jobs
  = ~$0.20/month.

Total projected: < $6/month.
