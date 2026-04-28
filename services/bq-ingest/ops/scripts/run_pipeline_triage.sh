#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
DEFAULT_CONFIG_DIR="$ROOT_DIR/ops/env/triage"

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

declare -a FAIL_REASONS

timestamp_utc() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

log() {
  printf '%s\n' "$*"
}

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  log "[PASS] $*"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  log "[WARN] $*"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAIL_REASONS+=("$*")
  log "[FAIL] $*"
}

usage() {
  cat <<USAGE
Usage:
  $(basename "$0") --source <name> [--config <path>] [--smoke]

Examples:
  $(basename "$0") --source typeform --smoke
  $(basename "$0") --config /abs/path/custom.env --smoke

Notes:
  - Config files default to: ops/env/triage/<source>.env
  - --smoke runs the ingest endpoint (can write data)
USAGE
}

SOURCE=""
CONFIG_FILE=""
RUN_SMOKE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --source)
      SOURCE="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG_FILE="${2:-}"
      shift 2
      ;;
    --smoke)
      RUN_SMOKE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG_FILE" ]]; then
  if [[ -z "$SOURCE" ]]; then
    usage
    exit 1
  fi
  CONFIG_FILE="$DEFAULT_CONFIG_DIR/${SOURCE}.env"
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  log "Config file not found: $CONFIG_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$CONFIG_FILE"
set +a

PROJECT_ID="${TRIAGE_PROJECT_ID:-${GCP_PROJECT_ID:-}}"
REGION="${TRIAGE_REGION:-us-central1}"
SERVICE="${TRIAGE_SERVICE:-bq-ingest}"
INGEST_PATH="${TRIAGE_INGEST_PATH:-}"
SCHEDULER_JOB="${TRIAGE_SCHEDULER_JOB:-}"
RAW_TABLES_CSV="${TRIAGE_RAW_TABLES_CSV:-}"
CORE_TABLES_CSV="${TRIAGE_CORE_TABLES_CSV:-}"
SECRET_NAME="${TRIAGE_SECRET_NAME:-}"
SECRET_ENV_VAR="${TRIAGE_SECRET_ENV_VAR:-}"
SMOKE_TIMEOUT_SEC="${TRIAGE_SMOKE_TIMEOUT_SEC:-90}"

if [[ -z "$PROJECT_ID" ]]; then
  log "Missing TRIAGE_PROJECT_ID (or GCP_PROJECT_ID) in config: $CONFIG_FILE"
  exit 1
fi

log "== Pipeline Triage =="
log "time_utc=$(timestamp_utc)"
log "config=$CONFIG_FILE"
log "project=$PROJECT_ID region=$REGION service=$SERVICE"

DESC_JSON="$(gcloud run services describe "$SERVICE" --region "$REGION" --project "$PROJECT_ID" --format=json 2>/tmp/pipeline_triage_describe.err || true)"

if [[ -z "$DESC_JSON" || "$DESC_JSON" == "null" ]]; then
  fail "Could not describe Cloud Run service $SERVICE ($(cat /tmp/pipeline_triage_describe.err 2>/dev/null || true))"
else
  SERVICE_URL="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("status",{}).get("url", ""))')"
  LATEST_CREATED="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("status",{}).get("latestCreatedRevisionName", ""))')"
  LATEST_READY="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("status",{}).get("latestReadyRevisionName", ""))')"
  TRAFFIC_REV="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); t=(d.get("status",{}).get("traffic") or d.get("spec",{}).get("traffic") or [{}])[0]; print(t.get("revisionName") or ("LATEST" if t.get("latestRevision") else ""))')"
  TRAFFIC_PCT="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); t=(d.get("status",{}).get("traffic") or d.get("spec",{}).get("traffic") or [{}])[0]; print(str(t.get("percent", "")))')"
  START_COMMAND="$(printf '%s' "$DESC_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); c=((d.get("spec",{}).get("template",{}).get("spec",{}).get("containers") or [{}])[0]); cmd=c.get("command") or []; print(cmd[0] if cmd else "")')"

  if [[ -n "$SERVICE_URL" && -n "$LATEST_READY" ]]; then
    pass "Cloud Run service reachable: url=$SERVICE_URL latest_ready=$LATEST_READY traffic=${TRAFFIC_PCT:-?}%->$TRAFFIC_REV command=${START_COMMAND:-<default>}"
  else
    fail "Cloud Run metadata incomplete (url=$SERVICE_URL latest_ready=$LATEST_READY)"
  fi

  if [[ -n "$LATEST_CREATED" && -n "$LATEST_READY" && "$LATEST_CREATED" != "$LATEST_READY" ]]; then
    warn "Newest revision is not ready yet (created=$LATEST_CREATED ready=$LATEST_READY)."
  fi

  REV_TO_CHECK="$LATEST_READY"
  if [[ -n "$LATEST_CREATED" && "$LATEST_CREATED" != "$LATEST_READY" ]]; then
    REV_TO_CHECK="$LATEST_CREATED"
  fi

  if [[ -n "$REV_TO_CHECK" ]]; then
    STARTUP_ERRORS="$(gcloud logging read "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"$SERVICE\" AND resource.labels.revision_name=\"$REV_TO_CHECK\" AND severity>=ERROR" --project="$PROJECT_ID" --limit=20 --order=desc --format='value(textPayload)' 2>/dev/null || true)"
    if printf '%s' "$STARTUP_ERRORS" | rg -qi 'failed to resolve binary path|failed to start and listen on the port|Application failed to start'; then
      fail "Cloud Run revision $REV_TO_CHECK has startup errors (entrypoint/boot issue)."
    else
      pass "No obvious startup errors in recent logs for revision $REV_TO_CHECK."
    fi
  fi
fi

if [[ -n "$SECRET_NAME" ]]; then
  SECRET_HEX="$(gcloud secrets versions access latest --secret="$SECRET_NAME" --project="$PROJECT_ID" 2>/dev/null | od -An -tx1 | tr -d ' \n' || true)"
  if [[ -z "$SECRET_HEX" ]]; then
    fail "Secret $SECRET_NAME is empty or inaccessible."
  else
    pass "Secret $SECRET_NAME is accessible and non-empty."
    if [[ "$SECRET_HEX" == *0a ]]; then
      warn "Secret $SECRET_NAME appears to end with newline byte (0a)."
    else
      pass "Secret $SECRET_NAME does not end with newline byte."
    fi
  fi
fi

if [[ -n "$SECRET_ENV_VAR" && -n "$DESC_JSON" ]]; then
  ENV_CHECK="$(DESC_JSON_PAYLOAD="$DESC_JSON" python3 - "$SECRET_ENV_VAR" "$SECRET_NAME" <<'PY'
import json
import os
import sys

d = json.loads(os.environ.get("DESC_JSON_PAYLOAD", "{}"))
want_env = sys.argv[1]
want_secret = sys.argv[2]
env = ((d.get("spec", {}).get("template", {}).get("spec", {}).get("containers") or [{}])[0]).get("env") or []
match = [e for e in env if e.get("name") == want_env]
if not match:
    print("MISSING")
else:
    e = match[0]
    ref = ((e.get("valueFrom") or {}).get("secretKeyRef") or {})
    if want_secret and ref.get("name") != want_secret:
        print("MISMATCH")
    else:
        print("OK")
PY
)"
  if [[ "$ENV_CHECK" == "OK" ]]; then
    pass "Service env wiring includes $SECRET_ENV_VAR -> $SECRET_NAME."
  elif [[ "$ENV_CHECK" == "MISMATCH" ]]; then
    warn "Service env var $SECRET_ENV_VAR is present but points to a different secret."
  else
    fail "Service env var $SECRET_ENV_VAR is missing."
  fi
fi

SMOKE_BODY=""
SMOKE_OK="false"

if [[ "$RUN_SMOKE" == "true" ]]; then
  if [[ -z "${SERVICE_URL:-}" || -z "$INGEST_PATH" ]]; then
    fail "Cannot run smoke test (service URL or TRIAGE_INGEST_PATH missing)."
  else
    SMOKE_URL="${SERVICE_URL}${INGEST_PATH}"
    log "Running ingest smoke: $SMOKE_URL"
    SMOKE_BODY="$(curl -sS --max-time "$SMOKE_TIMEOUT_SEC" "$SMOKE_URL" 2>/tmp/pipeline_triage_smoke.err || true)"
    SMOKE_ERR="$(cat /tmp/pipeline_triage_smoke.err 2>/dev/null || true)"

    if [[ -n "$SMOKE_ERR" && -z "$SMOKE_BODY" ]]; then
      fail "Smoke request failed: $SMOKE_ERR"
    elif printf '%s' "$SMOKE_BODY" | rg -q '"ok"\s*:\s*true'; then
      pass "Ingest endpoint reports ok=true."
      SMOKE_OK="true"
      if printf '%s' "$SMOKE_BODY" | rg -q 'Invalid header value'; then
        fail "Smoke payload contains header/auth errors."
      fi
    else
      fail "Ingest endpoint did not return ok=true."
    fi

    if [[ -n "$SMOKE_BODY" ]]; then
      COMPACT="$(printf '%s' "$SMOKE_BODY" | python3 -c 'import json,sys
raw=sys.stdin.read().strip()
try:
 d=json.loads(raw)
 out={k:d.get(k) for k in ("ok","records_upserted","entities_fetched","error") if k in d}
 print(out)
except Exception:
 print(raw[:280])' 2>/dev/null || true)"
      log "Smoke summary: ${COMPACT:-<unparsed>}"
    fi
  fi
else
  warn "Smoke test skipped (run with --smoke to exercise ingest endpoint)."
fi

count_table() {
  local full_table="$1"
  local count
  local err

  count="$(bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --format=csv "SELECT COUNT(*) AS row_count FROM \`${full_table}\`" 2>/tmp/pipeline_triage_bq.err | tail -n 1 | tr -d '\r' || true)"
  err="$(cat /tmp/pipeline_triage_bq.err 2>/dev/null || true)"

  if [[ -n "$err" && -z "$count" ]]; then
    fail "Table check failed for $full_table: $err"
    echo "-1"
    return
  fi

  if [[ "$count" =~ ^[0-9]+$ ]]; then
    echo "$count"
  else
    fail "Could not parse row count for $full_table."
    echo "-1"
  fi
}

RAW_NONZERO=0
CORE_NONZERO=0
RAW_TOTAL=0
CORE_TOTAL=0

if [[ -n "$RAW_TABLES_CSV" ]]; then
  IFS=',' read -r -a RAW_TABLES <<< "$RAW_TABLES_CSV"
  for t in "${RAW_TABLES[@]}"; do
    table_trimmed="$(printf '%s' "$t" | xargs)"
    [[ -z "$table_trimmed" ]] && continue
    full_table="$PROJECT_ID.$table_trimmed"
    count="$(count_table "$full_table")"
    if [[ "$count" -ge 0 ]]; then
      RAW_TOTAL=$((RAW_TOTAL + count))
      if [[ "$count" -gt 0 ]]; then
        RAW_NONZERO=1
        pass "Raw table $table_trimmed has $count rows."
      else
        warn "Raw table $table_trimmed has 0 rows."
      fi
    fi
  done
fi

if [[ -n "$CORE_TABLES_CSV" ]]; then
  IFS=',' read -r -a CORE_TABLES <<< "$CORE_TABLES_CSV"
  for t in "${CORE_TABLES[@]}"; do
    table_trimmed="$(printf '%s' "$t" | xargs)"
    [[ -z "$table_trimmed" ]] && continue
    full_table="$PROJECT_ID.$table_trimmed"
    count="$(count_table "$full_table")"
    if [[ "$count" -ge 0 ]]; then
      CORE_TOTAL=$((CORE_TOTAL + count))
      if [[ "$count" -gt 0 ]]; then
        CORE_NONZERO=1
        pass "Core table $table_trimmed has $count rows."
      else
        warn "Core table $table_trimmed has 0 rows."
      fi
    fi
  done
fi

if [[ -n "$SCHEDULER_JOB" ]]; then
  JOB_JSON="$(gcloud scheduler jobs describe "$SCHEDULER_JOB" --location="$REGION" --project="$PROJECT_ID" --format=json 2>/tmp/pipeline_triage_sched.err || true)"
  if [[ -z "$JOB_JSON" ]]; then
    fail "Could not describe scheduler job $SCHEDULER_JOB ($(cat /tmp/pipeline_triage_sched.err 2>/dev/null || true))"
  else
    JOB_URI="$(printf '%s' "$JOB_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("httpTarget") or {}).get("uri", ""))')"
    JOB_STATUS_CODE="$(printf '%s' "$JOB_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("status") or {}).get("code", ""))')"
    JOB_LAST_ATTEMPT="$(printf '%s' "$JOB_JSON" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("lastAttemptTime", ""))')"

    if [[ -n "$JOB_STATUS_CODE" && "$JOB_STATUS_CODE" != "0" ]]; then
      fail "Scheduler job $SCHEDULER_JOB status code is $JOB_STATUS_CODE (last_attempt=$JOB_LAST_ATTEMPT uri=$JOB_URI)."
    else
      pass "Scheduler job $SCHEDULER_JOB status is clear (last_attempt=$JOB_LAST_ATTEMPT)."
    fi

    FINISHED_ROW="$(gcloud logging read "resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"$SCHEDULER_JOB\" AND jsonPayload.\"@type\"=\"type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished\"" --project="$PROJECT_ID" --limit=1 --order=desc --format='value(jsonPayload.status,jsonPayload.debugInfo,timestamp)' 2>/dev/null || true)"

    if [[ -n "$FINISHED_ROW" ]]; then
      if printf '%s' "$FINISHED_ROW" | rg -q 'NOT_FOUND|URL_ERROR|PERMISSION_DENIED'; then
        fail "Latest scheduler attempt failed: $FINISHED_ROW"
      else
        pass "Latest scheduler attempt: $FINISHED_ROW"
      fi
    else
      warn "No scheduler AttemptFinished log found for $SCHEDULER_JOB."
    fi
  fi
fi

ROOT_CAUSE=""

if [[ $FAIL_COUNT -gt 0 ]]; then
  if [[ -n "${LATEST_CREATED:-}" && -n "${LATEST_READY:-}" && "$LATEST_CREATED" != "$LATEST_READY" ]]; then
    ROOT_CAUSE="Cloud Run rollout/runtime issue: newest revision is not becoming ready and traffic may be pinned to an older revision."
  elif printf '%s' "${FAIL_REASONS[*]:-}" | rg -qi 'header/auth|Secret .*newline'; then
    ROOT_CAUSE="Auth/secret issue: API key formatting or secret wiring is breaking upstream requests."
  elif [[ "$RAW_TOTAL" -eq 0 ]]; then
    ROOT_CAUSE="Ingestion issue: endpoint/scheduler may run, but no raw data is landing."
  elif [[ "$RAW_TOTAL" -gt 0 && "$CORE_TOTAL" -eq 0 ]]; then
    ROOT_CAUSE="Modeling issue: raw data exists but core transforms are not materializing output."
  elif printf '%s' "${FAIL_REASONS[*]:-}" | rg -qi 'Scheduler job .*status code|Latest scheduler attempt failed'; then
    ROOT_CAUSE="Scheduling issue: Cloud Scheduler target or auth/config is failing."
  else
    ROOT_CAUSE="Mixed failures: check failed steps below in order (runtime -> auth -> ingest -> raw/core -> scheduler)."
  fi
else
  if [[ "$RUN_SMOKE" != "true" ]]; then
    ROOT_CAUSE="No hard failures in passive checks. Run again with --smoke for active ingest verification."
  elif [[ "$SMOKE_OK" == "true" && "$RAW_TOTAL" -gt 0 && "$CORE_TOTAL" -gt 0 ]]; then
    ROOT_CAUSE="No blocking issue detected. Pipeline appears healthy for configured checks."
  else
    ROOT_CAUSE="No blocking issue detected in checks, but confirm with source-specific business validations."
  fi
fi

log ""
log "== Triage Summary =="
log "passes=$PASS_COUNT warnings=$WARN_COUNT failures=$FAIL_COUNT"
log "likely_root_cause=$ROOT_CAUSE"

if [[ $FAIL_COUNT -gt 0 ]]; then
  log "failed_checks:"
  for r in "${FAIL_REASONS[@]}"; do
    log "- $r"
  done
  exit 1
fi

exit 0
