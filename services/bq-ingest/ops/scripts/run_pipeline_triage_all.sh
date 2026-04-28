#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
TRIAGE_SCRIPT="$ROOT_DIR/ops/scripts/run_pipeline_triage.sh"
CONFIG_DIR="${TRIAGE_CONFIG_DIR:-$ROOT_DIR/ops/env/triage}"

RUN_SMOKE="false"

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") [--smoke]

Examples:
  $(basename "$0")
  $(basename "$0") --smoke

Notes:
  - Runs triage for every source config in ops/env/triage/*.env (except _template.env).
  - Exits non-zero if any source triage fails.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke)
      RUN_SMOKE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ ! -x "$TRIAGE_SCRIPT" ]]; then
  echo "Missing or non-executable triage script: $TRIAGE_SCRIPT"
  exit 1
fi

if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "Missing triage config directory: $CONFIG_DIR"
  exit 1
fi

CONFIG_FILES=()
while IFS= read -r line; do
  CONFIG_FILES+=("$line")
done < <(find "$CONFIG_DIR" -maxdepth 1 -type f -name "*.env" ! -name "_template.env" | sort)

if [[ ${#CONFIG_FILES[@]} -eq 0 ]]; then
  echo "No triage source configs found in: $CONFIG_DIR"
  exit 1
fi

echo "== Pipeline Triage (All Sources) =="
echo "time_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "config_dir=$CONFIG_DIR"
echo "smoke=$RUN_SMOKE"
echo ""

PASS_COUNT=0
FAIL_COUNT=0

for config_file in "${CONFIG_FILES[@]}"; do
  source_name="$(basename "$config_file" .env)"
  echo "---- source=$source_name ----"

  cmd=("$TRIAGE_SCRIPT" "--config" "$config_file")
  if [[ "$RUN_SMOKE" == "true" ]]; then
    cmd+=("--smoke")
  fi

  if "${cmd[@]}"; then
    echo "[OK] source=$source_name"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] source=$source_name"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
  echo ""
done

echo "== Combined Summary =="
echo "passed_sources=$PASS_COUNT"
echo "failed_sources=$FAIL_COUNT"

if [[ $FAIL_COUNT -gt 0 ]]; then
  exit 1
fi

exit 0
