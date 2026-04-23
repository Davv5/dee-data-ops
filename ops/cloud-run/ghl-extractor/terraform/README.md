# GHL Extractor — Terraform (Cloud Run Jobs + Cloud Scheduler)

Provisions the Cloud Run Jobs and Cloud Scheduler jobs that replace the GHA
cron for GHL ingest. Part of Track W (Live-by-Default Scheduler Migration).

## Prerequisites

1. `gcloud auth application-default login` (or set `GOOGLE_APPLICATION_CREDENTIALS`)
2. The `ingest@dee-data-ops.iam.gserviceaccount.com` SA must already have:
   - `roles/bigquery.dataEditor` on `dee-data-ops`
   - `roles/secretmanager.secretAccessor` on `ghl-api-key` + `ghl-location-id`
     in `dee-data-ops-prod` (Track J + David's manual fix 2026-04-22)
3. The `cloud-scheduler@dee-data-ops-prod.iam.gserviceaccount.com` SA must exist.
   Create if absent:
   ```bash
   gcloud iam service-accounts create cloud-scheduler \
     --project=dee-data-ops-prod \
     --display-name="Cloud Scheduler invoker"
   ```
4. The extractor image must be pushed to Artifact Registry before `apply`:
   ```bash
   bash ops/cloud-run/ghl-extractor/build-and-push.sh
   ```

## First-time setup

```bash
cd ops/cloud-run/ghl-extractor/terraform

# Initialize providers
terraform init

# If the Artifact Registry repo already exists (Track J may have created it):
terraform import google_artifact_registry_repository.ingest \
  projects/dee-data-ops-prod/locations/us-central1/repositories/ingest

# Review the plan — STOP here and share with David before applying
terraform plan

# Apply (only after David confirms the plan)
terraform apply
```

## Updating the image (normal CD path)

On merge to main, `.github/workflows/cloud-run-deploy-ghl.yml` builds and
pushes the image, then runs:

```bash
gcloud run jobs update ghl-hot \
  --image=us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/ghl-extractor:<sha> \
  --region=us-central1 --project=dee-data-ops-prod

gcloud run jobs update ghl-cold \
  --image=us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/ghl-extractor:<sha> \
  --region=us-central1 --project=dee-data-ops-prod
```

You can also do this manually after a hotfix:

```bash
SHA=$(git rev-parse --short HEAD)
IMAGE="us-central1-docker.pkg.dev/dee-data-ops-prod/ingest/ghl-extractor:${SHA}"
gcloud run jobs update ghl-hot  --image="${IMAGE}" --region=us-central1 --project=dee-data-ops-prod
gcloud run jobs update ghl-cold --image="${IMAGE}" --region=us-central1 --project=dee-data-ops-prod
```

## Rollback / pause

**Pause the scheduler without tearing down the job** (cleanest rollback):

```bash
gcloud scheduler jobs pause ghl-hot  --location=us-central1 --project=dee-data-ops-prod
gcloud scheduler jobs pause ghl-cold --location=us-central1 --project=dee-data-ops-prod
```

Resume when ready:

```bash
gcloud scheduler jobs resume ghl-hot  --location=us-central1 --project=dee-data-ops-prod
gcloud scheduler jobs resume ghl-cold --location=us-central1 --project=dee-data-ops-prod
```

**Full rollback to GHA cron**: pause both scheduler jobs above, then re-enable the
`schedule:` block in `.github/workflows/ingest.yml` (revert the Track W commit).
The GHA path (via `workflow_dispatch`) was never deleted, so this is a one-commit
revert + merge.

**Terraform destroy** (nuclear — removes all Cloud Run Jobs + Scheduler jobs):

```bash
terraform destroy
```

This does NOT delete the Artifact Registry repo or any BigQuery tables.

## Inspect logs

```bash
# List last 20 executions of the hot job
gcloud run jobs executions list \
  --job=ghl-hot --region=us-central1 --project=dee-data-ops-prod --limit=20

# Tail logs for a specific execution
gcloud run jobs executions describe <execution-name> \
  --region=us-central1 --project=dee-data-ops-prod

# Stream live logs (Cloud Logging)
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="ghl-hot"' \
  --project=dee-data-ops-prod --limit=50 --format=json
```

## Manual one-off trigger

```bash
# Trigger hot job with a specific since-date (backfill)
gcloud run jobs execute ghl-hot \
  --region=us-central1 --project=dee-data-ops-prod \
  --args="--endpoints=conversations,messages,--since=2026-04-22T00:00:00Z"
```
