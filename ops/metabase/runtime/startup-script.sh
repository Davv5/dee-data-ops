#!/bin/bash
# startup-script.sh — runs on every GCE boot.
#
# Responsibilities:
#   1. Read VM metadata → env var shape
#   2. Pull secrets from Secret Manager → env vars + mounted files
#   3. Pull docker-compose.yml + Caddyfile from the ops bucket
#   4. Bring up the metabase + caddy + cloud-sql-proxy stack
#
# This script is uploaded to gs://<ops-bucket>/startup-script.sh by the
# operator after `terraform apply` (see ops/metabase/terraform/README.md).
# COS reads `startup-script-url` from VM metadata and runs it as root.

set -euo pipefail
exec > >(tee /var/log/metabase-startup.log) 2>&1
echo "[$(date -u +%FT%TZ)] metabase startup begin"

# ──────────────────────────────────────────────────────────────────────────
# Read VM metadata
# ──────────────────────────────────────────────────────────────────────────
META_URL="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
HEADER="Metadata-Flavor: Google"

CLOUD_SQL_CONNECTION_NAME=$(curl -s -H "$HEADER" "$META_URL/METABASE_DB_INSTANCE")
METABASE_DB_NAME=$(curl -s -H "$HEADER" "$META_URL/METABASE_DB_NAME")
METABASE_DB_USER=$(curl -s -H "$HEADER" "$META_URL/METABASE_DB_USER")
METABASE_DB_PASSWORD_SECRET=$(curl -s -H "$HEADER" "$META_URL/METABASE_DB_PASSWORD_SECRET")
METABASE_BQ_KEY_SECRET=$(curl -s -H "$HEADER" "$META_URL/METABASE_BQ_KEY_SECRET")
METABASE_HOSTNAME=$(curl -s -H "$HEADER" "$META_URL/METABASE_HOSTNAME")
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/project/project-id")
OPS_BUCKET="${PROJECT_ID}-metabase-ops"

# ──────────────────────────────────────────────────────────────────────────
# Pull secrets
# ──────────────────────────────────────────────────────────────────────────
TOKEN=$(curl -s -H "$HEADER" "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" | jq -r .access_token)

fetch_secret() {
  local secret_id=$1
  curl -s -H "Authorization: Bearer $TOKEN" \
    "https://secretmanager.googleapis.com/v1/projects/${PROJECT_ID}/secrets/${secret_id}/versions/latest:access" \
    | jq -r .payload.data | base64 -d
}

METABASE_DB_PASSWORD=$(fetch_secret "$METABASE_DB_PASSWORD_SECRET")
mkdir -p /var/lib/metabase
fetch_secret "$METABASE_BQ_KEY_SECRET" > /var/lib/metabase/bq-reader.json
chmod 600 /var/lib/metabase/bq-reader.json

# ──────────────────────────────────────────────────────────────────────────
# Pull compose + Caddyfile
# ──────────────────────────────────────────────────────────────────────────
mkdir -p /opt/metabase
cd /opt/metabase
gsutil -q cp "gs://${OPS_BUCKET}/docker-compose.yml" ./docker-compose.yml
gsutil -q cp "gs://${OPS_BUCKET}/Caddyfile" ./Caddyfile

# ──────────────────────────────────────────────────────────────────────────
# Render env file (read by docker compose)
# ──────────────────────────────────────────────────────────────────────────
cat > /opt/metabase/.env <<EOF
METABASE_DB_NAME=${METABASE_DB_NAME}
METABASE_DB_USER=${METABASE_DB_USER}
METABASE_DB_PASSWORD=${METABASE_DB_PASSWORD}
METABASE_HOSTNAME=${METABASE_HOSTNAME}
CLOUD_SQL_CONNECTION_NAME=${CLOUD_SQL_CONNECTION_NAME}
EOF
chmod 600 /opt/metabase/.env

# ──────────────────────────────────────────────────────────────────────────
# Bring up stack
# ──────────────────────────────────────────────────────────────────────────
# Container-Optimized OS ships with docker + docker compose plugin.
docker compose -f /opt/metabase/docker-compose.yml --env-file /opt/metabase/.env pull
docker compose -f /opt/metabase/docker-compose.yml --env-file /opt/metabase/.env up -d

echo "[$(date -u +%FT%TZ)] metabase startup end — https://${METABASE_HOSTNAME}"
