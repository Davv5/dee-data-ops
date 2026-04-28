#!/bin/bash
# Execute any Python callable in Cloud Run via python-runner job.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-project-41542e21-470f-4589-96d}"
REGION="${GCP_REGION:-us-central1}"
JOB_NAME="${CLOUD_PYTHON_JOB_NAME:-python-runner}"
WAIT_FLAG="--async"
ARGS_JSON="[]"
KWARGS_JSON="{}"

usage() {
  cat <<'EOF'
Usage:
  ops/scripts/run_cloud_python.sh <module:function> [options]

Options:
  --args-json '<json_list>'       Positional args as JSON list (default: [])
  --kwargs-json '<json_object>'   Keyword args as JSON object (default: {})
  --project <project_id>          GCP project override
  --region <region>               Region override
  --job <job_name>                Cloud Run job name override
  --wait                          Wait for completion (only when explicitly requested)
  --async                         Return immediately (default)

Examples:
  ops/scripts/run_cloud_python.sh fathom_pipeline:run_models --async
  ops/scripts/run_cloud_python.sh data_quality:run_tests --kwargs-json '{"source":"fathom"}'
EOF
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

TARGET="$1"
shift

while [ $# -gt 0 ]; do
  case "$1" in
    --args-json)
      ARGS_JSON="$2"
      shift 2
      ;;
    --kwargs-json)
      KWARGS_JSON="$2"
      shift 2
      ;;
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --job)
      JOB_NAME="$2"
      shift 2
      ;;
    --wait)
      WAIT_FLAG="--wait"
      shift
      ;;
    --async)
      WAIT_FLAG="--async"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

python3 -c 'import json,sys; x=json.loads(sys.argv[1]); assert isinstance(x,list)' "$ARGS_JSON"
python3 -c 'import json,sys; x=json.loads(sys.argv[1]); assert isinstance(x,dict)' "$KWARGS_JSON"

ARGS_JSON_B64="$(printf '%s' "$ARGS_JSON" | base64 | tr -d '\n')"
KWARGS_JSON_B64="$(printf '%s' "$KWARGS_JSON" | base64 | tr -d '\n')"

ENV_OVERRIDES="PYTHON_TARGET=${TARGET},PYTHON_ARGS_JSON_B64=${ARGS_JSON_B64},PYTHON_KWARGS_JSON_B64=${KWARGS_JSON_B64}"

echo "Executing ${JOB_NAME} in ${PROJECT_ID}/${REGION}..."
gcloud run jobs execute "${JOB_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --update-env-vars="${ENV_OVERRIDES}" \
  "${WAIT_FLAG}"
