#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-project-41542e21-470f-4589-96d}"
REGION="${GCP_REGION:-us-central1}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/fanbasis-python-runner:latest"
MANIFEST="${ROOT_DIR}/ops/cloud/jobs.yaml"

CANARY_ONLY="false"
SUFFIX=""
SKIP_BUILD="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --canary-only)
      CANARY_ONLY="true"
      shift
      ;;
    --suffix)
      SUFFIX="$2"
      shift 2
      ;;
    --suffix=*)
      SUFFIX="${1#*=}"
      shift
      ;;
    --skip-build)
      SKIP_BUILD="true"
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

cd "${ROOT_DIR}"

if [[ "${SKIP_BUILD}" != "true" ]]; then
  echo "Building shared runtime image: ${IMAGE_URI}"
  gcloud builds submit "${ROOT_DIR}" \
    --project="${PROJECT_ID}" \
    --config="${ROOT_DIR}/ops/cloud/pipeline-runner/cloudbuild.yaml" \
    --substitutions="_IMAGE_URI=${IMAGE_URI}"
else
  echo "Skipping image build"
fi

DEPLOY_CMD=(ops/scripts/deploy_jobs_from_manifest.sh --manifest "${MANIFEST}" --project "${PROJECT_ID}" --region "${REGION}")
if [[ -n "${SUFFIX}" ]]; then
  DEPLOY_CMD+=("--suffix=${SUFFIX}")
fi
if [[ "${CANARY_ONLY}" == "true" ]]; then
  DEPLOY_CMD+=(--canary-only)
fi

echo "Deploying jobs from manifest..."
"${DEPLOY_CMD[@]}"
