# Metabase — self-host on GCP

D-DEE's dashboard layer. Self-hosted Metabase OSS on `dee-data-ops-prod`,
reading `marts.*`, owned by Precision Scaling, templated for future clients.

Conventions live in `.claude/rules/metabase.md` — read that first.

## Directory layout

```
ops/metabase/
├── README.md              ← you are here
├── RECOVERY.md            ← restore-from-backup drill (populate post-v1)
├── terraform/             ← GCP infra as code
├── runtime/               ← what lives on the VM (compose + Caddy + startup)
└── authoring/             ← dashboards as code (REST-API scripts)
    ├── client.py
    ├── sync.py
    ├── infrastructure/
    │   └── bigquery_connection.py
    └── dashboards/
        └── speed_to_lead.py
```

## From zero to URL

**Prerequisite:** Terraform ≥ 1.5 installed locally; `gcloud` authenticated
as David.

### 1. Provision infra

```bash
cd ops/metabase/terraform
terraform init
terraform plan -out=tfplan   # REVIEW before applying
terraform apply tfplan
```

~5 min. See `terraform/README.md` for the full prerequisite + apply
sequence (state bucket creation, API enablement, post-apply manual steps).

### 2. Upload runtime assets to the ops bucket

Terraform created a GCS bucket `dee-data-ops-prod-metabase-ops`. The VM
reads its `docker-compose.yml`, `Caddyfile`, and `startup-script.sh` from
that bucket at boot.

```bash
OPS_BUCKET=$(cd terraform && terraform output -raw ops_bucket)
gcloud storage cp runtime/docker-compose.yml gs://$OPS_BUCKET/
gcloud storage cp runtime/Caddyfile          gs://$OPS_BUCKET/
gcloud storage cp runtime/startup-script.sh  gs://$OPS_BUCKET/
```

### 3. Generate + upload the BigQuery reader key

```bash
SA=$(cd terraform && terraform output -raw bq_reader_sa_email)
SECRET=$(cd terraform && terraform output -raw bq_reader_key_secret_name)

gcloud iam service-accounts keys create /tmp/mb-bq-reader.json \
  --iam-account="$SA"

gcloud secrets versions add "$SECRET" --data-file=/tmp/mb-bq-reader.json
rm /tmp/mb-bq-reader.json
```

### 4. Reboot the VM so the startup script re-runs with the uploaded assets

```bash
gcloud compute instances reset metabase --zone=us-central1-a
```

Wait ~2 min. Then:

```bash
cd terraform
terraform output metabase_url
```

Open that URL. First-load takes ~30 sec while Metabase initializes its
Postgres schema. You'll see Metabase's setup wizard.

### 5. Complete the setup wizard (one-time)

- Create an admin user (your email + a strong password — stash both in 1Password)
- Skip the BigQuery connection step (the authoring script wires it)
- Generate a session token for the authoring scripts:
  1. Click the gear icon → Admin settings → Authentication → API Keys
  2. Create a new key called `authoring`, scope "Admin", copy the value
  3. Save as `MB_SESSION` in your local `.env.metabase`

### 6. Connect BigQuery via the authoring script

```bash
cp .env.metabase.example .env.metabase
# fill in MB_URL + MB_SESSION

source .venv/bin/activate
set -a && source .env.metabase && set +a
python -m ops.metabase.authoring.infrastructure.bigquery_connection
```

Metabase now sees `dee-data-ops-prod.marts.*` as a data source named
`dee-data-ops-prod`.

### 7. Ship the first dashboard

```bash
python -m ops.metabase.authoring.dashboards.speed_to_lead
```

Prints the dashboard URL. Open it.

## Making changes

Every dashboard change is a PR editing the authoring script.

```bash
git checkout -b feat/metabase-speed-to-lead-card-update
# edit ops/metabase/authoring/dashboards/speed_to_lead.py
git add ...
git commit -m "..."
gh pr create ...
```

After merge, re-run the script on prod:

```bash
python -m ops.metabase.authoring.dashboards.speed_to_lead
```

## Connecting Claude Code's MCP client

Metabase v60+ ships an official MCP server at `<MB_URL>/api/mcp`. To wire it
into Claude Code:

```json
// ~/.claude/mcp.json or .claude/mcp.json (project-scoped)
{
  "mcpServers": {
    "metabase": {
      "type": "http",
      "url": "https://<nip-io-host>/api/mcp",
      "headers": {
        "X-Metabase-Session": "${MB_SESSION}"
      }
    }
  }
}
```

(Exact config shape depends on your Claude Code MCP config format — check
`/docs/latest/ai/mcp` on your Metabase instance after it's up.)

With the MCP connected, Claude Code sessions can introspect and author
Metabase objects conversationally. Prototype there; commit via authoring
script.

## Recovery

The Postgres app-DB is backed up nightly to `gs://dee-data-ops-prod-metabase-ops/backups/`.
See `RECOVERY.md` (to be written after first successful backup run) for the
restore drill.

If you lose the app-DB entirely, the dashboards can be rebuilt from scratch
by running the authoring scripts against a fresh Metabase instance — that's
the whole point of Option-1 authoring.

## Cost envelope

~$25/mo (e2-small VM + Cloud SQL db-f1-micro + backup storage). Fits
comfortably within a $300 GCP promotion credit for 12 months.

## Template-ability

For PS's next client, fork this directory, then:

1. Edit `terraform/variables.tf` — change `project_id` + `bq_marts_dataset`
2. Edit `authoring/infrastructure/bigquery_connection.py` — change `PROJECT_ID`
3. Delete `authoring/dashboards/speed_to_lead.py` + write client-specific dashboards
4. Update `.claude/rules/metabase.md` client-specific sections if any
5. Apply

Full onboarding SOP lives at `NEW_CLIENT_METABASE_SOP.md` (post-v1 deliverable).
