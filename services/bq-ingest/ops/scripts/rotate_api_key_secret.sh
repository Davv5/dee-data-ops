#!/bin/bash

set -euo pipefail

# Usage:
#   ./rotate_api_key_secret.sh <secret_name> <env_var_name> <cloud_run_job_name> [region] [project_id]
#
# Example:
#   ./rotate_api_key_secret.sh FathomApiKeyLegacySales FATHOM_API_KEY fathom-backfill us-central1 project-41542e21-470f-4589-96d
#
# Behavior:
# 1) Prompts securely for API key input (no echo)
# 2) Creates secret if missing, otherwise adds a new version
# 3) Updates Cloud Run Job secret mapping (env var -> secret:latest)
# 4) Prints final verification details

SECRET_NAME="${1:-}"
ENV_VAR_NAME="${2:-}"
JOB_NAME="${3:-}"
REGION="${4:-us-central1}"
PROJECT_ID="${5:-project-41542e21-470f-4589-96d}"

if [[ -z "${SECRET_NAME}" || -z "${ENV_VAR_NAME}" || -z "${JOB_NAME}" ]]; then
  echo "Usage: $0 <secret_name> <env_var_name> <cloud_run_job_name> [region] [project_id]"
  exit 1
fi

echo "=== API Key Secret Rotation Workflow ==="
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Secret: ${SECRET_NAME}"
echo "Job: ${JOB_NAME}"
echo "Env Var: ${ENV_VAR_NAME}"
echo ""

read -r -s -p "Paste API key value (input hidden): " API_KEY_VALUE
echo ""

if [[ -z "${API_KEY_VALUE}" ]]; then
  echo "Error: empty API key input."
  exit 1
fi

if gcloud secrets describe "${SECRET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  printf '%s' "${API_KEY_VALUE}" | gcloud secrets versions add "${SECRET_NAME}" \
    --data-file=- \
    --project="${PROJECT_ID}" >/dev/null
  echo "✓ Added new version to existing secret ${SECRET_NAME}"
else
  printf '%s' "${API_KEY_VALUE}" | gcloud secrets create "${SECRET_NAME}" \
    --replication-policy=automatic \
    --data-file=- \
    --project="${PROJECT_ID}" >/dev/null
  echo "✓ Created secret ${SECRET_NAME}"
fi

unset API_KEY_VALUE

echo "Updating Cloud Run job secret binding..."
gcloud run jobs update "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --set-secrets="${ENV_VAR_NAME}=${SECRET_NAME}:latest" >/dev/null
echo "✓ Updated ${JOB_NAME}: ${ENV_VAR_NAME} -> ${SECRET_NAME}:latest"

echo ""
echo "Verification:"
gcloud run jobs describe "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="yaml(spec.template.spec.template.spec.containers[0].env)"

echo ""
echo "Done."
