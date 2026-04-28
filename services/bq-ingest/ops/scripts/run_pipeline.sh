#!/bin/bash
# Full ETL pipeline runner delegated to the dependency-aware task registry.
# Deploy to Cloud Run Job for production use.

set -e

PROJECT_ID="${GCP_PROJECT_ID:-project-41542e21-470f-4589-96d}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AUTO_TRIAGE_ON_FAIL="${AUTO_TRIAGE_ON_FAIL:-true}"
AUTO_TRIAGE_SMOKE_ON_FAIL="${AUTO_TRIAGE_SMOKE_ON_FAIL:-false}"
TRIAGE_ALREADY_RAN=0

run_auto_triage_on_failure() {
  if [ "$AUTO_TRIAGE_ON_FAIL" != "true" ]; then
    return 0
  fi
  if [ "$TRIAGE_ALREADY_RAN" -eq 1 ]; then
    return 0
  fi
  TRIAGE_ALREADY_RAN=1

  if [ -x "$ROOT_DIR/ops/scripts/run_pipeline_triage_all.sh" ]; then
    echo ""
    echo "Running automatic pipeline triage due to pipeline failure..."
    TRIAGE_CMD=("$ROOT_DIR/ops/scripts/run_pipeline_triage_all.sh")
    if [ "$AUTO_TRIAGE_SMOKE_ON_FAIL" = "true" ]; then
      TRIAGE_CMD+=("--smoke")
    fi
    "${TRIAGE_CMD[@]}" || true
  else
    echo "Auto-triage script not found at $ROOT_DIR/ops/scripts/run_pipeline_triage_all.sh"
  fi
}

on_error() {
  local exit_code=$?
  echo ""
  echo "❌ Pipeline script failed (exit_code=${exit_code})"
  run_auto_triage_on_failure
  exit "$exit_code"
}

trap on_error ERR

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Starting full pipeline run"
python3 -m ops.runner.cli run pipeline.full

echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') — Pipeline completed successfully"
exit 0
