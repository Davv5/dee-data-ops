#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-project-41542e21-470f-4589-96d}"
REGION="${GCP_REGION:-us-central1}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TS="$(date -u +%Y%m%d-%H%M%S)"
OUT_FILE="${ROOT_DIR}/ops/cloud/baseline/canary_parity_${TS}.md"

poll_execution() {
  local execution_name="$1"
  local max_attempts="${2:-60}"
  local attempt=0
  while true; do
    local payload
    payload="$(gcloud run jobs executions describe "${execution_name}" \
      --region="${REGION}" \
      --project="${PROJECT_ID}" \
      --format='json(status.conditions,status.completionTime,status.succeededCount,status.failedCount)')"
    local line
    line="$(python3 -c '
import json, sys
data = json.loads(sys.stdin.read() or "{}")
status = data.get("status", {}) or {}
conditions = status.get("conditions", []) or []
completed = None
for cond in conditions:
    if cond.get("type") == "Completed":
        completed = cond
        break
if completed and completed.get("status") in ("True", "False"):
    print(
        "{}\t{}\t{}\t{}".format(
            completed.get("status", ""),
            completed.get("message", ""),
            status.get("completionTime", ""),
            status.get("succeededCount", status.get("failedCount", "")),
        )
    )
' <<<"${payload}")"
    if [[ -n "${line}" ]]; then
      printf '%s' "${line}"
      return 0
    fi
    attempt=$((attempt + 1))
    if [[ "${attempt}" -ge "${max_attempts}" ]]; then
      echo "TIMEOUT polling execution ${execution_name} after ${max_attempts} attempts" >&2
      return 1
    fi
    sleep 20
  done
}

cd "${ROOT_DIR}"

echo "Running canary parity checks..."

PIPE_BASE="$(gcloud run jobs execute pipeline-run --region="${REGION}" --project="${PROJECT_ID}" --async --format='value(metadata.name)')"
PIPE_V2="$(gcloud run jobs execute pipeline-run-v2 --region="${REGION}" --project="${PROJECT_ID}" --async --format='value(metadata.name)')"

PIPE_BASE_STATUS="$(poll_execution "${PIPE_BASE}")"
PIPE_V2_STATUS="$(poll_execution "${PIPE_V2}")"

PY_BASE="$(gcloud run jobs execute python-runner --region="${REGION}" --project="${PROJECT_ID}" --wait --update-env-vars='PYTHON_TARGET=math:sqrt,PYTHON_ARGS_JSON_B64=Wzld,PYTHON_KWARGS_JSON_B64=e30=' --format='value(metadata.name)')"
PY_V2="$(gcloud run jobs execute python-runner-v2 --region="${REGION}" --project="${PROJECT_ID}" --wait --update-env-vars='PYTHON_TARGET=math:sqrt,PYTHON_ARGS_JSON_B64=Wzld,PYTHON_KWARGS_JSON_B64=e30=' --format='value(metadata.name)')"

PY_BASE_JSON="$(gcloud logging read "resource.type=\"cloud_run_job\" AND labels.\"run.googleapis.com/execution_name\"=\"${PY_BASE}\" AND logName=\"projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstdout\"" --project="${PROJECT_ID}" --limit=1 --order=desc --format=json)"
PY_V2_JSON="$(gcloud logging read "resource.type=\"cloud_run_job\" AND labels.\"run.googleapis.com/execution_name\"=\"${PY_V2}\" AND logName=\"projects/${PROJECT_ID}/logs/run.googleapis.com%2Fstdout\"" --project="${PROJECT_ID}" --limit=1 --order=desc --format=json)"

cat > "${OUT_FILE}" <<EOF
# Canary Parity Report (${TS} UTC)

## Pipeline Runs
- baseline execution: \`${PIPE_BASE}\`
- canary execution: \`${PIPE_V2}\`
- baseline status: \`${PIPE_BASE_STATUS}\`
- canary status: \`${PIPE_V2_STATUS}\`

## Python Runner Smoke
- baseline execution: \`${PY_BASE}\`
- canary execution: \`${PY_V2}\`
- baseline stdout:
\`\`\`json
${PY_BASE_JSON}
\`\`\`
- canary stdout:
\`\`\`json
${PY_V2_JSON}
\`\`\`
EOF

echo "Parity report written: ${OUT_FILE}"
