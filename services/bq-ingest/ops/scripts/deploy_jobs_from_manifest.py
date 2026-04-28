#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def load_manifest(path: Path) -> Dict[str, Any]:
    raw = path.read_text()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        try:
            import yaml  # type: ignore

            return yaml.safe_load(raw)
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"Failed to parse manifest {path}: {exc}") from exc


def _matches_job_filters(job: Dict[str, Any], selected: set[str], canary_only: bool, include_unmanaged: bool) -> bool:
    if selected and job["name"] not in selected:
        return False
    if canary_only and not job.get("canary", False):
        return False
    if not include_unmanaged and not job.get("managed", False):
        return False
    return True


def _write_env_file(env_map: Dict[str, Any], temp_dir: Path, job_name: str) -> Path:
    env_file = temp_dir / f"{job_name}.env.yaml"
    lines = []
    for key, value in sorted(env_map.items()):
        value_str = str(value).replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'{key}: "{value_str}"')
    env_file.write_text("\n".join(lines) + "\n")
    return env_file


def _secrets_flag(secrets: Dict[str, str]) -> str:
    return ",".join(f"{key}={value}" for key, value in sorted(secrets.items()))


def _csv(values: Iterable[Any]) -> str:
    return ",".join(str(v) for v in values)


def build_deploy_cmd(
    job: Dict[str, Any],
    suffix: str,
    project: str,
    region: str,
    env_file: Path | None,
) -> List[str]:
    deploy_name = f"{job['name']}{suffix}"
    target = job.get("target", {})
    command = target.get("command", [])
    args = target.get("args", [])
    secrets = job.get("secrets", {})

    cmd: List[str] = [
        "gcloud",
        "run",
        "jobs",
        "deploy",
        deploy_name,
        f"--project={project}",
        f"--region={region}",
        f"--image={target['image']}",
        f"--service-account={job['service_account']}",
        f"--cpu={job['cpu']}",
        f"--memory={job['memory']}",
        f"--task-timeout={int(job['timeout_seconds'])}",
        f"--max-retries={int(job['max_retries'])}",
        f"--tasks={int(job['tasks'])}",
    ]

    parallelism = job.get("parallelism")
    if parallelism is not None:
        cmd.append(f"--parallelism={int(parallelism)}")
    if command:
        cmd.append(f"--command={_csv(command)}")
    if args:
        cmd.append(f"--args={_csv(args)}")
    if env_file is not None:
        cmd.append(f"--env-vars-file={env_file}")
    if secrets:
        cmd.append(f"--set-secrets={_secrets_flag(secrets)}")

    return cmd


def _scheduler_exists(name: str, location: str, project: str) -> bool:
    result = subprocess.run(
        ["gcloud", "scheduler", "jobs", "describe", name,
         f"--location={location}", f"--project={project}"],
        capture_output=True, text=True,
    )
    return result.returncode == 0


def _scheduler_state(name: str, location: str, project: str) -> Optional[str]:
    result = subprocess.run(
        [
            "gcloud",
            "scheduler",
            "jobs",
            "describe",
            name,
            f"--location={location}",
            f"--project={project}",
            "--format=value(state)",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    state = (result.stdout or "").strip().upper()
    return state or None


def build_scheduler_cmd(action: str, sched: Dict[str, Any], project: str) -> List[str]:
    """Build gcloud scheduler jobs create/update http command."""
    name = sched["name"]
    location = sched.get("location", "us-central1")
    http = sched["http"]
    auth = sched.get("auth", {})

    cmd: List[str] = [
        "gcloud", "scheduler", "jobs", action, "http", name,
        f"--location={location}",
        f"--project={project}",
        f"--schedule={sched['schedule']}",
        f"--time-zone={sched.get('time_zone', 'UTC')}",
        f"--uri={http['uri']}",
        f"--http-method={http.get('method', 'POST')}",
    ]

    headers = http.get("headers") or {}
    if headers:
        headers_str = ",".join(f"{k}={v}" for k, v in headers.items())
        # create uses --headers; update uses --update-headers
        flag = "--headers" if action == "create" else "--update-headers"
        cmd.append(f"{flag}={headers_str}")

    body_b64 = http.get("body_base64")
    if body_b64:
        body = base64.b64decode(body_b64).decode("utf-8")
        cmd.append(f"--message-body={body}")

    auth_type = auth.get("type", "")
    if auth_type == "oauth":
        cmd.append(f"--oauth-service-account-email={auth['service_account_email']}")
        if "scope" in auth:
            cmd.append(f"--oauth-token-scope={auth['scope']}")
    elif auth_type == "oidc":
        cmd.append(f"--oidc-service-account-email={auth['service_account_email']}")
        if "audience" in auth:
            cmd.append(f"--oidc-token-audience={auth['audience']}")

    return cmd


def deploy_schedulers(manifest: Dict[str, Any], project: str, dry_run: bool = False) -> List[str]:
    schedulers: List[Dict[str, Any]] = manifest.get("schedulers", [])
    if not schedulers:
        print("No schedulers defined in manifest.", flush=True)
        return []

    print(f"Deploying {len(schedulers)} scheduler(s)...", flush=True)
    deployed: List[str] = []

    for sched in schedulers:
        name = sched["name"]
        location = sched.get("location", "us-central1")
        exists = _scheduler_exists(name, location, project)
        action = "update" if exists else "create"
        cmd = build_scheduler_cmd(action, sched, project)

        if dry_run:
            print(f"DRY-RUN scheduler {action}: {' '.join(cmd)}", flush=True)
            deployed.append(name)
            continue

        print(f"  {'Updating' if exists else 'Creating'} scheduler {name}...", flush=True)
        subprocess.run(cmd, check=True)

        # Ensure state matches manifest (ENABLED/PAUSED)
        desired_state = sched.get("state", "ENABLED").upper()
        if desired_state == "PAUSED":
            subprocess.run(
                ["gcloud", "scheduler", "jobs", "pause", name,
                 f"--location={location}", f"--project={project}"],
                check=True,
            )
        elif desired_state == "ENABLED":
            # Explicitly unpause so manifest deploys are idempotent and can recover paused jobs.
            current_state = _scheduler_state(name, location, project)
            if current_state == "PAUSED":
                subprocess.run(
                    ["gcloud", "scheduler", "jobs", "resume", name,
                     f"--location={location}", f"--project={project}"],
                    check=True,
                )
        deployed.append(name)

    return deployed


def run() -> int:
    parser = argparse.ArgumentParser(description="Deploy Cloud Run jobs from ops/cloud/jobs.yaml manifest.")
    parser.add_argument("--manifest", default="ops/cloud/jobs.yaml", help="Path to jobs manifest.")
    parser.add_argument("--project", default=None, help="GCP project override.")
    parser.add_argument("--region", default=None, help="Region override.")
    parser.add_argument("--suffix", default="", help="Append suffix to deployed job names (e.g. -v2).")
    parser.add_argument("--job", action="append", default=[], help="Deploy only selected job names.")
    parser.add_argument("--canary-only", action="store_true", help="Deploy only jobs with canary=true.")
    parser.add_argument("--include-unmanaged", action="store_true", help="Include jobs marked managed=false.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing.")
    parser.add_argument("--skip-schedulers", action="store_true", help="Skip Cloud Scheduler deployment.")
    parser.add_argument("--schedulers-only", action="store_true", help="Deploy only schedulers, skip Cloud Run jobs.")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    manifest = load_manifest(manifest_path)
    project = args.project or manifest["project_id"]
    region = args.region or manifest["region"]
    selected = set(args.job)

    deployed_jobs: List[str] = []
    deployed_schedulers: List[str] = []

    if not args.schedulers_only:
        jobs: List[Dict[str, Any]] = manifest.get("jobs", [])
        chosen = [
            job
            for job in jobs
            if _matches_job_filters(job, selected, args.canary_only, args.include_unmanaged)
        ]
        if not chosen:
            print("No jobs matched filters.", flush=True)
        else:
            print(f"Deploying {len(chosen)} job(s) from {manifest_path} in {project}/{region}", flush=True)
            with tempfile.TemporaryDirectory(prefix="manifest-env-") as temp_root:
                temp_dir = Path(temp_root)
                for job in chosen:
                    env_map = job.get("env", {})
                    env_file = _write_env_file(env_map, temp_dir, job["name"]) if env_map else None
                    cmd = build_deploy_cmd(job, args.suffix, project, region, env_file)
                    deploy_name = f"{job['name']}{args.suffix}"
                    if args.dry_run:
                        print("DRY-RUN:", " ".join(cmd), flush=True)
                        deployed_jobs.append(deploy_name)
                        continue
                    print(f"Deploying {deploy_name}...", flush=True)
                    subprocess.run(cmd, check=True)
                    deployed_jobs.append(deploy_name)

    if not args.skip_schedulers:
        deployed_schedulers = deploy_schedulers(manifest, project, dry_run=args.dry_run)

    print(json.dumps({"ok": True, "deployed_jobs": deployed_jobs, "deployed_schedulers": deployed_schedulers}, ensure_ascii=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except subprocess.CalledProcessError as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(exc.returncode)
