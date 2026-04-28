#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-project-41542e21-470f-4589-96d}"
REGION="${GCP_REGION:-us-central1}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${ROOT_DIR}/ops/cloud/baseline"
TS="$(date -u +%Y%m%d-%H%M%S)"

mkdir -p "${OUT_DIR}"

echo "Capturing cloud baseline at ${TS}..."
gcloud run jobs list --region="${REGION}" --project="${PROJECT_ID}" --format=json > "${OUT_DIR}/run_jobs_${TS}.json"
gcloud scheduler jobs list --location="${REGION}" --project="${PROJECT_ID}" --format=json > "${OUT_DIR}/scheduler_jobs_${TS}.json"
gcloud iam service-accounts list --project="${PROJECT_ID}" --format=json > "${OUT_DIR}/service_accounts_${TS}.json"
gcloud secrets list --project="${PROJECT_ID}" --format=json > "${OUT_DIR}/secrets_${TS}.json"
gcloud projects get-iam-policy "${PROJECT_ID}" --format=json > "${OUT_DIR}/project_iam_policy_${TS}.json"

python3 - <<PY
import json
import pathlib
import subprocess

root = pathlib.Path("${OUT_DIR}")
ts = "${TS}"
project = "${PROJECT_ID}"
region = "${REGION}"

jobs = json.loads((root / f"run_jobs_{ts}.json").read_text())
schedulers = json.loads((root / f"scheduler_jobs_{ts}.json").read_text())
secrets = json.loads((root / f"secrets_{ts}.json").read_text())

job_dir = root / f"run_job_describes_{ts}"
sch_dir = root / f"scheduler_describes_{ts}"
secret_dir = root / f"secret_iam_policies_{ts}"
job_dir.mkdir(exist_ok=True)
sch_dir.mkdir(exist_ok=True)
secret_dir.mkdir(exist_ok=True)

for job in jobs:
    name = job.get("metadata", {}).get("name")
    if not name:
        continue
    out = subprocess.check_output(
        ["gcloud", "run", "jobs", "describe", name, f"--region={region}", f"--project={project}", "--format=json"],
        text=True,
    )
    (job_dir / f"{name}.json").write_text(out)

for scheduler in schedulers:
    name = scheduler.get("name", "").split("/")[-1]
    if not name:
        continue
    out = subprocess.check_output(
        ["gcloud", "scheduler", "jobs", "describe", name, f"--location={region}", f"--project={project}", "--format=json"],
        text=True,
    )
    (sch_dir / f"{name}.json").write_text(out)

for secret in secrets:
    full_name = secret.get("name", "")
    short_name = full_name.split("/")[-1] if "/" in full_name else full_name
    if not short_name:
        continue
    out = subprocess.check_output(
        ["gcloud", "secrets", "get-iam-policy", short_name, f"--project={project}", "--format=json"],
        text=True,
    )
    (secret_dir / f"{short_name}.json").write_text(out)

print(f"jobs={len(jobs)} schedulers={len(schedulers)} secrets={len(secrets)}")
PY

shasum -a 256 \
  "${ROOT_DIR}/ops/scripts/run_pipeline.sh" \
  "${ROOT_DIR}/ops/scripts/deploy_jobs_from_manifest.sh" \
  "${ROOT_DIR}/ops/scripts/manage_schedulers_from_manifest.py" \
  "${ROOT_DIR}/ops/cloud/jobs.yaml" \
  > "${OUT_DIR}/file_hashes_${TS}.txt"

echo "Baseline captured: ${TS}"
