#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# snapshot_gtm_raw.sh — U4a plumbing-parity snapshot.
#
# Copies GTM's live raw datasets in `project-41542e21-470f-4589-96d` into a
# frozen snapshot dataset `raw_snapshot_u4a_<YYYYMMDD>` inside the same
# project so that dbt can replay staging/warehouse/marts against an
# unchanging raw input.
#
# Datasets copied:
#   Raw.*           — legacy single-blob landing (stripe/typeform/fathom/
#                     calendly/fanbasis/ghl + BQML + support tables)
#   raw_ghl.*       — Phase-2 per-object GHL (ghl__<obj>_raw)
#   raw_calendly.*  — Phase-2 per-object Calendly (empty today but copied
#                     for completeness)
#
# Idempotent: drops + recreates the snapshot dataset on every run.
#
# Usage:
#   ops/bq/snapshot_gtm_raw.sh                       # YYYYMMDD defaults to today (UTC)
#   ops/bq/snapshot_gtm_raw.sh 20260423              # explicit date suffix
#
# Output:
#   - Snapshot dataset:   project-41542e21-470f-4589-96d:raw_snapshot_u4a_<date>
#   - Timestamp sidecar:  ops/bq/.last_snapshot_ts   (wall-clock of last run)
#   - Full log:           ops/bq/.last_snapshot.log
#
# Environment:
#   GCP_PROJECT (default: project-41542e21-470f-4589-96d)
#   BQ_LOCATION (default: US)
# ---------------------------------------------------------------------------

set -euo pipefail

PROJECT="${GCP_PROJECT:-project-41542e21-470f-4589-96d}"
LOCATION="${BQ_LOCATION:-US}"
DATE_SUFFIX="${1:-$(date -u +%Y%m%d)}"
SNAPSHOT_DATASET="raw_snapshot_u4a_${DATE_SUFFIX}"
SOURCE_DATASETS=("Raw" "raw_ghl" "raw_calendly")

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TS_FILE="${SCRIPT_DIR}/.last_snapshot_ts"
LOG_FILE="${SCRIPT_DIR}/.last_snapshot.log"

T_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG_FILE"; }

: > "$LOG_FILE"
log "Snapshot target: ${PROJECT}:${SNAPSHOT_DATASET}"
log "Source datasets: ${SOURCE_DATASETS[*]}"
log "T_start (UTC):   ${T_START}"

# 1. Drop + recreate the snapshot dataset.
if bq --project_id="${PROJECT}" --location="${LOCATION}" show --format=prettyjson \
    "${PROJECT}:${SNAPSHOT_DATASET}" >/dev/null 2>&1; then
  log "Dataset exists; removing contents."
  bq --project_id="${PROJECT}" --location="${LOCATION}" rm -r -f \
    "${PROJECT}:${SNAPSHOT_DATASET}" >>"$LOG_FILE" 2>&1
fi

log "Creating snapshot dataset."
bq --project_id="${PROJECT}" --location="${LOCATION}" mk \
  --dataset \
  --description="U4a plumbing-parity frozen snapshot of GTM raw, captured ${T_START}. Safe to drop after U4a signs off." \
  "${PROJECT}:${SNAPSHOT_DATASET}" >>"$LOG_FILE" 2>&1

# 2. Copy every table in each source dataset into the snapshot.
total=0
for src in "${SOURCE_DATASETS[@]}"; do
  log "Listing tables in ${src}."
  # Use --format=prettyjson instead of --format=csv because bq csv emits
  # a stray header line; JSON is unambiguous and tableId is always present.
  tables_json="$(bq --project_id="${PROJECT}" --location="${LOCATION}" \
    ls --max_results=10000 --format=prettyjson "${PROJECT}:${src}")"
  # bash 3.2 (macOS default) lacks mapfile — use a while-read loop against
  # process substitution instead. Python filters to actual TABLE objects
  # (skipping VIEW / MATERIALIZED_VIEW / EXTERNAL).
  while IFS= read -r tbl; do
    [[ -z "${tbl}" ]] && continue
    # Skip merge-statement scratch tables (created + dropped mid-MERGE by the
    # backfill extractor). They carry no durable data and routinely disappear
    # between `bq ls` and `bq cp` when ingestion is running concurrently.
    if [[ "${tbl}" == _merge_stage_* || "${tbl}" == *_stage_* ]]; then
      log "  skip ${src}.${tbl} (ephemeral merge-stage)"
      continue
    fi
    src_fqn="${PROJECT}:${src}.${tbl}"
    dst_fqn="${PROJECT}:${SNAPSHOT_DATASET}.${tbl}"
    log "  cp ${src_fqn} -> ${dst_fqn}"
    # Tolerate cp failures: a canonical table disappearing is unexpected, but
    # scratch tables that survive the filter above can still race us. Log and
    # continue so one transient miss doesn't kill the whole snapshot.
    if ! bq --project_id="${PROJECT}" --location="${LOCATION}" cp \
        --quiet -f --no_clobber=false \
        "${src_fqn}" "${dst_fqn}" >>"$LOG_FILE" 2>&1; then
      log "  WARN cp failed: ${src_fqn} (likely ephemeral / raced with concurrent writer)"
      continue
    fi
    total=$((total + 1))
  done < <(echo "${tables_json}" | python3 -c 'import json,sys
rows = json.load(sys.stdin)
for r in rows:
    if r.get("type") == "TABLE":
        print(r["tableReference"]["tableId"])')
done

T_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "T_end   (UTC):   ${T_END}"
log "Copied ${total} tables into ${SNAPSHOT_DATASET}."
log "Remember: dee-data-ops-prod time-travel window is 7 days — finish U4a by 7 days after ${T_END}."

# Write the canonical timestamp sidecar. This is what the parity SQL and
# dbt invocations consume. Fields are bash-sourceable (KEY=value).
cat > "$TS_FILE" <<EOF
# Generated by ops/bq/snapshot_gtm_raw.sh — DO NOT EDIT
SNAPSHOT_DATASET=${SNAPSHOT_DATASET}
SNAPSHOT_PROJECT=${PROJECT}
SNAPSHOT_T_START=${T_START}
SNAPSHOT_T_END=${T_END}
# Use SNAPSHOT_T_END as the FOR SYSTEM_TIME AS OF reference on dee-data-ops-prod
# so that any write that landed during the copy window is also reflected in
# the prod baseline.
PARITY_BASELINE_TS=${T_END}
EOF
log "Wrote ${TS_FILE}"
cat "$TS_FILE"
