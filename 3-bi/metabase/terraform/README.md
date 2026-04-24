# Terraform — Metabase self-host on GCP

Provisions the full Metabase stack on `dee-data-ops-prod`:

- GCE VM (`metabase`) with a static external IP
- Cloud SQL Postgres 15 (`metabase-appdb`) — Metabase's app-DB, private IP only
- VPC + service-networking peering so the VM reaches Cloud SQL without a public DB endpoint
- GCS bucket (`dee-data-ops-prod-metabase-ops`) for startup script + nightly backups
- Two service accounts (VM runtime, BQ reader) with minimum-scope IAM
- Secret Manager entries for the DB password + BQ SA key
- Firewall rules — 443 + 80 (for ACME) from any IP, 22 from IAP netblock only

TLS + ACME happen at the application layer (Caddy on the VM), not at Cloud LB — keeps v1 simple and keeps cost on GCE's free-tier-adjacent e2-small rather than adding a $20/mo LB.

## One-time prerequisites (NOT Terraform-managed)

These must exist before `terraform apply`:

1. **State bucket.** Already assumed to exist:
   ```bash
   gcloud storage buckets create gs://dee-data-ops-prod-tfstate \
     --project=dee-data-ops-prod \
     --location=us-central1 \
     --uniform-bucket-level-access
   ```
   Skip if it already exists.

2. **Required APIs enabled on dee-data-ops-prod:**
   ```bash
   gcloud services enable \
     compute.googleapis.com \
     sqladmin.googleapis.com \
     secretmanager.googleapis.com \
     servicenetworking.googleapis.com \
     storage.googleapis.com \
     --project=dee-data-ops-prod
   ```

3. **Operator auth.** You (David) run Terraform locally, authenticated as yourself:
   ```bash
   gcloud auth application-default login
   ```

## Apply

```bash
cd 3-bi/metabase/terraform
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

Expect ~5 min. Cloud SQL is the slowest resource to create (~3 min).

## Post-apply — 3 manual steps

Terraform creates the BQ reader SA but does **not** generate or upload its key — that's an explicit human action to keep key material out of state files.

1. **Upload the startup script** so the VM can fetch it on boot:
   ```bash
   gcloud storage cp ../runtime/startup-script.sh gs://$(terraform output -raw ops_bucket)/startup-script.sh
   ```

2. **Generate the BQ reader SA key + upload to Secret Manager:**
   ```bash
   SA_EMAIL=$(terraform output -raw bq_reader_sa_email)
   SECRET_ID=$(terraform output -raw bq_reader_key_secret_name)

   gcloud iam service-accounts keys create /tmp/metabase-bq-reader.json \
     --iam-account="$SA_EMAIL"

   gcloud secrets versions add "$SECRET_ID" \
     --data-file=/tmp/metabase-bq-reader.json

   rm /tmp/metabase-bq-reader.json
   ```

3. **Reboot the VM** (so the startup script re-runs with the freshly-uploaded assets):
   ```bash
   gcloud compute instances reset metabase --zone=us-central1-a
   ```

Wait ~2 min, then `terraform output metabase_url` and open it in a browser. First-load initializes Metabase's schema in the app-DB (~30 sec wait on the loading spinner).

## Tear down

```bash
# Cloud SQL has deletion_protection = true by default; flip it first
gcloud sql instances patch metabase-appdb --no-deletion-protection
terraform destroy
```

## Cost envelope

Order of magnitude against the $300 GCP promotion credit:

| Resource | ~ Monthly |
|---|---|
| GCE e2-small (24x7) | $13 |
| Static external IP (attached) | $0 (free while attached) |
| Cloud SQL db-f1-micro | $10 |
| Cloud SQL disk (10 GB HDD) | $1 |
| GCS ops bucket (small) | $0.10 |
| Egress (light dashboard traffic) | $1–2 |
| **Total** | **~$25/mo** |

$300 credit ≈ 12 months runway.
