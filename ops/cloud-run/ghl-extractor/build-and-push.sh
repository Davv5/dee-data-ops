#!/usr/bin/env bash
# build-and-push.sh — Build the GHL extractor image and push to Artifact Registry.
#
# Usage:
#   bash ops/cloud-run/ghl-extractor/build-and-push.sh
#
# Run from the repo root. Requires:
#   - docker (local) or Cloud Build (CI)
#   - gcloud authenticated to dee-data-ops-prod
#   - git (for short SHA tag)
#
# Called by .github/workflows/cloud-run-deploy-ghl.yml on every merge to main
# that touches ingestion/ghl/**. Can also be run manually for hotfixes.

set -euo pipefail

PROJECT="dee-data-ops-prod"
REGION="us-central1"
REPO="ingest"
IMAGE_NAME="ghl-extractor"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE_NAME}"
SHA=$(git rev-parse --short HEAD)

echo "[build-and-push] Building image..."
docker build \
  --platform linux/amd64 \
  -t "${REGISTRY}:${SHA}" \
  -t "${REGISTRY}:latest" \
  -f ingestion/ghl/Dockerfile \
  .

echo "[build-and-push] Configuring Docker auth for Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "[build-and-push] Pushing ${REGISTRY}:${SHA} ..."
docker push "${REGISTRY}:${SHA}"

echo "[build-and-push] Pushing ${REGISTRY}:latest ..."
docker push "${REGISTRY}:latest"

echo "[build-and-push] Done. Image: ${REGISTRY}:${SHA}"
