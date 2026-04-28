#!/usr/bin/env bash
# 2-dbt/scripts/local-ci.sh — run the equivalent of GH Actions dbt-ci.yml locally.
#
# Usage:
#   bash 2-dbt/scripts/local-ci.sh <pr-number> [extra dbt args...]
#
# Examples:
#   bash 2-dbt/scripts/local-ci.sh 92
#   bash 2-dbt/scripts/local-ci.sh 92 --select revenue_detail+
#   bash 2-dbt/scripts/local-ci.sh 92 --fail-fast
#
# What it does:
#   1. Source repo `.env` (provides DBT_SCHEMA, BQ_KEYFILE_PATH, etc.).
#   2. Pick auth: SA keyfile if BQ_KEYFILE_PATH is set, else ADC.
#   3. Provision per-PR BigQuery dataset `ci_pr_<num>` (idempotent, mirrors the
#      `provision` job in `.github/workflows/dbt-ci.yml`).
#   4. Run `dbt build --target <ci|ci_local>` against that schema (mirrors
#      the `build` job).
#
# When to use:
#   - Pre-push smoke check before opening / updating a PR
#   - When GH Actions is in `degraded_performance` and a PR needs to merge
#   - When a parity-test result needs eyeballing before opening the PR
#
# When NOT to use:
#   - As a substitute for the GH Actions merge gate. Public CI history is
#     part of the engagement deliverable; this script is for the developer
#     loop, not the merge gate. See `feedback_local_ci_bypass.md` memory.

set -euo pipefail

PR="${1:-}"
if [[ -z "$PR" ]]; then
  echo "usage: $0 <pr-number> [extra dbt args...]" >&2
  exit 2
fi
shift

PROJECT="project-41542e21-470f-4589-96d"
SCHEMA="ci_pr_${PR}"

# Resolve repo paths whether script is invoked from repo root, 2-dbt/, or scripts/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DBT_DIR/.." && pwd)"

# Source repo .env if present
if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

# Pick target by auth context
if [[ -n "${BQ_KEYFILE_PATH:-}" && -f "${BQ_KEYFILE_PATH}" ]]; then
  TARGET="ci"
  echo "==> auth: SA keyfile $BQ_KEYFILE_PATH"
else
  if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
    echo "ERROR: no usable BQ_KEYFILE_PATH and gcloud ADC not configured." >&2
    echo "       run 'gcloud auth application-default login' first." >&2
    exit 1
  fi
  TARGET="ci_local"
  echo "==> auth: ADC ($(gcloud config get-value account 2>/dev/null))"
fi

echo "==> provision: bq mk $PROJECT:$SCHEMA"
bq --location=US mk --force=true --dataset \
  --description "Local CI schema for PR #$PR (run by ${USER:-unknown})" \
  "$PROJECT:$SCHEMA" >/dev/null

echo "==> build: dbt build --target $TARGET (DBT_CI_SCHEMA=$SCHEMA)"
cd "$DBT_DIR"
DBT_CI_SCHEMA="$SCHEMA" GCP_PROJECT_ID_DEV="$PROJECT" \
  "$REPO_ROOT/.venv/bin/dbt" build --target "$TARGET" "$@"

echo ""
echo "==> done — schema $PROJECT:$SCHEMA"
echo "    drop with: bq rm -r -f -d $PROJECT:$SCHEMA"
