# Calendly Extractor

Custom Python extractor for Calendly, replacing the Fivetran Calendly connector.
Introduced in Track X (2026-04-22) to achieve 1-min freshness vs Fivetran's daily cadence.

Mirrors the GHL extractor pattern (Track W, 2026-04-22):
- Cloud Run Jobs + Cloud Scheduler
- BQ advisory lock (`raw_calendly._job_locks`) prevents scheduler-overlap double-ingest
- Token-bucket throttle at 60 req/min with exponential backoff on 429
- Append-only landing in `raw_calendly.*` with `_ingested_at TIMESTAMP`
- Per-endpoint watermarks in `raw_calendly._sync_state`

## Endpoints

| Endpoint              | BQ Table                            | Cadence | Cursor            |
|-----------------------|-------------------------------------|---------|-------------------|
| `scheduled_events`    | `raw_calendly.scheduled_events`     | 1 min   | `min_start_time`  |
| `invitees`            | `raw_calendly.invitees`             | 1 min   | fans out from events |
| `invitee_no_shows`    | `raw_calendly.invitee_no_shows`     | 1 min   | `created_at_gt`   |

## Auth model

Uses Calendly v2 API with a **personal access token** (PAT) or OAuth2 Bearer token.

Token is currently documented in GCP Secret Manager as `calendly-api-token` in
the legacy `dee-data-ops-prod` project. During the one-GCP transition, confirm
the active secret project from the cutover runbook before rotating or granting
access.

**Manual steps (David):**
1. Generate a PAT in the Calendly dashboard: Settings → Integrations → API & Webhooks → Personal Access Tokens
2. Create the secret shell (if not already done — check first):
   ```bash
   gcloud secrets create calendly-api-token \
     --project=dee-data-ops-prod \
     --replication-policy=automatic
   ```
3. Populate the secret:
   ```bash
   echo -n "YOUR_TOKEN_HERE" | gcloud secrets versions add calendly-api-token \
     --project=dee-data-ops-prod \
     --data-file=-
   ```
4. Grant the ingest SA access:
   ```bash
   gcloud secrets add-iam-policy-binding calendly-api-token \
     --project=dee-data-ops-prod \
     --member="serviceAccount:ingest@dee-data-ops.iam.gserviceaccount.com" \
     --role="roles/secretmanager.secretAccessor"
   ```

## Token rotation

When rotating the Calendly PAT:
1. Generate a new PAT in the Calendly dashboard.
2. Add a new version to the secret:
   ```bash
   echo -n "NEW_TOKEN_HERE" | gcloud secrets versions add calendly-api-token \
     --project=dee-data-ops-prod \
     --data-file=-
   ```
3. The extractor resolves `latest` on each cold start — no Terraform or code change needed.
4. Disable the old version:
   ```bash
   gcloud secrets versions disable <old-version-number> \
     --secret=calendly-api-token --project=dee-data-ops-prod
   ```

## Local dev

For local testing, set `CALENDLY_API_TOKEN` and `GCP_PROJECT_ID_DEV` directly.
Do NOT set `GCP_SECRET_MANAGER_PROJECT` — that triggers the Secret Manager path
and requires prod credentials.

```bash
export CALENDLY_API_TOKEN="your-token-here"
export GCP_PROJECT_ID_DEV="project-41542e21-470f-4589-96d"
export BQ_KEYFILE_PATH="/path/to/dev-sa-keyfile.json"

# Dry-run (no writes):
python 1-raw-landing/calendly/extract.py --dry-run --since 2026-04-22T00:00:00Z

# Full pull since a date:
python 1-raw-landing/calendly/extract.py --since 2026-04-22T00:00:00Z

# Specific endpoint:
python 1-raw-landing/calendly/extract.py --endpoints scheduled_events --since 2026-04-22T00:00:00Z
```

## Rate limits

Calendly's API does not publish a hard req/min limit. The extractor uses a
token-bucket throttle at 60 req/min with exponential backoff on 429.

If 429 responses appear in Cloud Run logs at 1-min cadence:
1. Raise the Cloud Scheduler cadence to 2 min (edit `1-raw-landing/deploy/calendly-extractor/terraform/main.tf`
   and re-apply).
2. Note the change in the runbook.

See `docs/runbooks/calendly-cloud-run-extractor.md` for full ops guidance.
