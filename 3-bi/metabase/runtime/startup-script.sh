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
# NOTE: /opt is read-only on Container-Optimized OS. Use /var/lib which is
# writable + persistent. Also, gsutil isn't installed on COS; fetch via
# curl against the JSON Storage API using the metadata-server access token
# (same pattern as the Secret Manager fetches above).
COMPOSE_DIR=/var/lib/metabase/compose
mkdir -p "$COMPOSE_DIR"
cd "$COMPOSE_DIR"

fetch_ops_object() {
  local name=$1
  curl -sS -H "Authorization: Bearer $TOKEN" \
    "https://storage.googleapis.com/storage/v1/b/${OPS_BUCKET}/o/${name}?alt=media" \
    -o "$COMPOSE_DIR/$name"
}

fetch_ops_object docker-compose.yml
fetch_ops_object Caddyfile

# ──────────────────────────────────────────────────────────────────────────
# Render env file (read by docker compose)
# ──────────────────────────────────────────────────────────────────────────
cat > "$COMPOSE_DIR/.env" <<EOF
METABASE_DB_NAME=${METABASE_DB_NAME}
METABASE_DB_USER=${METABASE_DB_USER}
METABASE_DB_PASSWORD=${METABASE_DB_PASSWORD}
METABASE_HOSTNAME=${METABASE_HOSTNAME}
CLOUD_SQL_CONNECTION_NAME=${CLOUD_SQL_CONNECTION_NAME}
EOF
chmod 600 "$COMPOSE_DIR/.env"

# ──────────────────────────────────────────────────────────────────────────
# Bring up stack
# ──────────────────────────────────────────────────────────────────────────
# COS ships Docker but NOT the docker-compose V2 plugin. Use explicit
# `docker run` instead — one invocation per container. docker-compose.yml
# stays in place as documentation/source-of-truth for the topology.

# Shared network
docker network inspect mbnet >/dev/null 2>&1 || docker network create mbnet

# Cloud SQL Proxy — sidecar that exposes Postgres on port 5432 within mbnet
docker rm -f cloud-sql-proxy 2>/dev/null || true
docker run -d --name cloud-sql-proxy \
  --restart=unless-stopped \
  --network mbnet \
  gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.11.0 \
  --address=0.0.0.0 --port=5432 --private-ip "$CLOUD_SQL_CONNECTION_NAME"

# Metabase
docker rm -f metabase 2>/dev/null || true
docker run -d --name metabase \
  --restart=unless-stopped \
  --network mbnet \
  -e MB_DB_TYPE=postgres \
  -e MB_DB_DBNAME="$METABASE_DB_NAME" \
  -e MB_DB_PORT=5432 \
  -e MB_DB_USER="$METABASE_DB_USER" \
  -e MB_DB_PASS="$METABASE_DB_PASSWORD" \
  -e MB_DB_HOST=cloud-sql-proxy \
  -e MB_SITE_URL="https://$METABASE_HOSTNAME" \
  -e MB_CHECK_FOR_UPDATES=false \
  -e MB_ANON_TRACKING_ENABLED=false \
  -e JAVA_OPTS="-Xmx1g" \
  metabase/metabase:v0.60.1

# Caddy — TLS termination + reverse proxy to metabase:3000
mkdir -p /var/lib/metabase/caddy-data /var/lib/metabase/caddy-config
docker rm -f caddy 2>/dev/null || true
docker run -d --name caddy \
  --restart=unless-stopped \
  --network mbnet \
  -p 80:80 -p 443:443 \
  -e METABASE_HOSTNAME="$METABASE_HOSTNAME" \
  -v "$COMPOSE_DIR/Caddyfile:/etc/caddy/Caddyfile:ro" \
  -v /var/lib/metabase/caddy-data:/data \
  -v /var/lib/metabase/caddy-config:/config \
  caddy:2.8-alpine

echo "[$(date -u +%FT%TZ)] metabase startup end — https://${METABASE_HOSTNAME}"
