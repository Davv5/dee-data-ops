#!/bin/bash
set -e

# Mart Validation Runner
# Executes all validation queries and generates .claude/VALIDATION.md report
# Exit code: 0 = all pass, 1 = any fail

PROJECT_ID="project-41542e21-470f-4589-96d"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VALIDATION_DIR="$ROOT_DIR/sql/validate"
REPORT_FILE="$ROOT_DIR/.claude/VALIDATION.md"
AUTO_TRIAGE_ON_FAIL="${AUTO_TRIAGE_ON_FAIL:-true}"
AUTO_TRIAGE_SMOKE_ON_FAIL="${AUTO_TRIAGE_SMOKE_ON_FAIL:-false}"
TIMESTAMP=$(date -u '+%Y-%m-%d %H:%M:%S UTC')

# Create .claude directory if it doesn't exist
mkdir -p "$(dirname "$REPORT_FILE")"

# Start report
{
  echo "# Mart Validation Report"
  echo "**Generated:** $TIMESTAMP"
  echo "**Status:** Running..."
  echo ""
} > "$REPORT_FILE"

OVERALL_STATUS="PASS"
VALIDATION_COUNT=0

# Run each validation query
for validation_file in "$VALIDATION_DIR"/*.sql; do
  if [ ! -f "$validation_file" ]; then
    continue
  fi

  TABLE_NAME=$(basename "$validation_file" .sql)
  echo "Validating $TABLE_NAME..."

  # Execute query and capture results
  RESULTS=$(bq query \
    --project_id="$PROJECT_ID" \
    --format=json \
    --use_legacy_sql=false \
    < "$validation_file" 2>&1 || true)

  # Check for errors
  if echo "$RESULTS" | grep -q "error\|Error\|ERROR"; then
    if [ "$TABLE_NAME" = "typeform_core_tables" ] && echo "$RESULTS" | grep -q "Not found: Table .*Core\\.dim_typeform_"; then
      echo "  ⚠ WARN (Typeform core tables not initialized)"
      {
        echo "## $TABLE_NAME"
        echo "**Status:** ⚠ WARN"
        echo '```'
        echo "$RESULTS"
        echo '```'
        echo ""
      } >> "$REPORT_FILE"
    else
      echo "  ✗ FAILED"
      OVERALL_STATUS="FAIL"
      {
        echo "## $TABLE_NAME"
        echo "**Status:** ✗ FAILED"
        echo '```'
        echo "$RESULTS"
        echo '```'
        echo ""
      } >> "$REPORT_FILE"
    fi
  else
    if echo "$RESULTS" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"FAIL"'; then
      echo "  ✗ FAILED (assertion status)"
      OVERALL_STATUS="FAIL"
      {
        echo "## $TABLE_NAME"
        echo "**Status:** ✗ FAILED"
        echo '```json'
        echo "$RESULTS"
        echo '```'
        echo ""
      } >> "$REPORT_FILE"
    else
      echo "  ✓ PASSED"
      {
        echo "## $TABLE_NAME"
        echo "**Status:** ✓ PASSED"
        echo '```json'
        echo "$RESULTS"
        echo '```'
        echo ""
      } >> "$REPORT_FILE"
    fi
  fi

  VALIDATION_COUNT=$((VALIDATION_COUNT + 1))
done

# Finalize report
{
  echo "---"
  echo "**Validations run:** $VALIDATION_COUNT"
  echo "**Final Status:** $([ "$OVERALL_STATUS" = "PASS" ] && echo "✓ PASS" || echo "✗ FAIL")"
  echo "**Generated:** $TIMESTAMP"
} >> "$REPORT_FILE"

# Exit with status
if [ "$OVERALL_STATUS" = "FAIL" ]; then
  echo ""
  echo "❌ VALIDATION FAILED"
  echo "See .claude/VALIDATION.md for details"

  if [ "$AUTO_TRIAGE_ON_FAIL" = "true" ] && [ -x "$ROOT_DIR/ops/scripts/run_pipeline_triage_all.sh" ]; then
    echo ""
    echo "Running automatic pipeline triage because validation failed..."
    TRIAGE_CMD=("$ROOT_DIR/ops/scripts/run_pipeline_triage_all.sh")
    if [ "$AUTO_TRIAGE_SMOKE_ON_FAIL" = "true" ]; then
      TRIAGE_CMD+=("--smoke")
    fi
    "${TRIAGE_CMD[@]}" || true
  fi

  exit 1
else
  echo ""
  echo "✅ ALL VALIDATIONS PASSED"
  exit 0
fi
