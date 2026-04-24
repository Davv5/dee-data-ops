# Calendly Extractor — Terraform (Cloud Run Job + Cloud Scheduler)

Provisions the Cloud Run Job and Cloud Scheduler job that replace the Fivetran
Calendly connector for near-real-time ingest. Part of Track X
(Live-by-Default — Calendly Path, 2026-04-22).

Mirrors Track W's GHL extractor Terraform exactly. One job (no hot/cold split)
at 1-min cadence, using the shared `ingest` Artifact Registry repo.

## Prerequisites

1. `gcloud auth application-default login` (or set `GOOGLE_APPLICATION_CREDENTIALS`)
2. The `ingest@dee-data-ops.iam.gserviceaccount.com` SA must already have:
   - `roles/bigquery.dataEditor` on `dee-data-ops` (from Track J)
   - `roles/secretmanager.secretAccessor` on `calendly-api-token`
     in `dee-data-ops-prod` (Manual checkpoint #1 — David creates the secret)
3. The `cloud-scheduler@dee-data-ops-prod.iam.gserviceaccount.com` SA must exist.
   Create if absent:
   ```bash
   gcloud iam service-accounts create cloud-scheduler \
     --project=dee-data-ops-prod \
     --display-name="Cloud Scheduler invoker"
   ```
4. The `calendly-api-token` secret must be created and populated by David
   (see `1-raw-landing/calendly/README.md` for the exact commands).
5. The extractor image must be pushed to Artifact Registry before `apply`:
   ```bash
   bash 1-raw-landing/deploy/calendly-extractor/build-and-push.sh
   ```

## First-time setup

```bash
cd 1-raw-landing/deploy/calendly-extractor/terraform

# Initialize providers
terraform init

# If the Artifact Registry repo already exists (Track W created it):
terraform import google_artifact_registry_repository.ingest \
  projects/dee-data-ops-prod/locations/us-central1/repositories/ingest

# Review the plan — STOP here and share with David before applying
terraform plan

# Apply (only after David confirms the plan)
terraform apply
```

## Updating the image (normal CD path)

On merge to main, `.github/workflows/cloud-run-deploy-calendly.yml` builds and
pushes the image, then runs:

```bash
gcloud run jobs update calendly-poll \
  --image=us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/calendly-extractor:<sha> \
  --region=us-central1 --project=dee-data-ops-prod
```

Manual hotfix:

```bash
SHA=$(git rev-parse --short HEAD)
IMAGE="us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/calendly-extractor:${SHA}"
gcloud run jobs update calendly-poll --image="${IMAGE}" --region=us-central1 --project=dee-data-ops-prod
```

## Rollback / pause

**Pause the scheduler without tearing down the job** (cleanest rollback):

```bash
gcloud scheduler jobs pause calendly-poll --location=us-central1 --project=dee-data-ops-prod
```

Resume when ready:

```bash
gcloud scheduler jobs resume calendly-poll --location=us-central1 --project=dee-data-ops-prod
```

**Full rollback to Fivetran**: pause the scheduler above, then re-enable the Fivetran
connector in the Fivetran UI. The raw_calendly tables remain intact; Fivetran resumes
writing from its last checkpoint.

**Terraform destroy** (nuclear — removes Cloud Run Job + Scheduler job):

```bash
terraform destroy
```

This does NOT delete the Artifact Registry repo or any BigQuery tables.

## Inspect logs

```bash
# List last 20 executions
gcloud run jobs executions list \
  --job=calendly-poll --region=us-central1 --project=dee-data-ops-prod --limit=20

# Stream live logs
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="calendly-poll"' \
  --project=dee-data-ops-prod --limit=50 --format=json
```

## Manual one-off trigger

```bash
# Backfill from a specific date
gcloud run jobs execute calendly-poll \
  --region=us-central1 --project=dee-data-ops-prod \
  --args="--endpoints=scheduled_events,invitees,--since=2026-04-22T00:00:00Z"
```
