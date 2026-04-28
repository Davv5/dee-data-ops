#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


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


def run_cmd(cmd: List[str], dry_run: bool) -> None:
    if dry_run:
        print("DRY-RUN:", " ".join(cmd), flush=True)
        return
    subprocess.run(cmd, check=True)


def _headers_flag(headers: Dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(headers.items()))


def _decode_body(body_base64: str | None) -> str | None:
    if not body_base64:
        return None
    try:
        return base64.b64decode(body_base64.encode("utf-8")).decode("utf-8")
    except Exception:
        return None


def _cloud_run_job_uri(project: str, region: str, job_name: str, api_version: str) -> str:
    if api_version == "v1":
        return f"https://{region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/{project}/jobs/{job_name}:run"
    return f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job_name}:run"


def _scheduler_update_cmd(scheduler: Dict[str, Any], uri: str) -> List[str]:
    http = scheduler["http"]
    auth = scheduler.get("auth", {})
    cmd = [
        "gcloud",
        "scheduler",
        "jobs",
        "update",
        "http",
        scheduler["name"],
        f"--location={scheduler.get('location', 'us-central1')}",
        f"--schedule={scheduler['schedule']}",
        f"--time-zone={scheduler['time_zone']}",
        f"--uri={uri}",
        f"--http-method={http.get('method', 'POST')}",
    ]

    headers = http.get("headers", {})
    if headers:
        # update commands require --update-headers (not --headers).
        cmd.append(f"--update-headers={_headers_flag(headers)}")

    body = _decode_body(http.get("body_base64"))
    if body is not None:
        cmd.append(f"--message-body={body}")

    auth_type = auth.get("type")
    if auth_type == "oidc":
        cmd.append(f"--oidc-service-account-email={auth['service_account_email']}")
        audience = auth.get("audience")
        if audience:
            cmd.append(f"--oidc-token-audience={audience}")
    elif auth_type == "oauth":
        cmd.append(f"--oauth-service-account-email={auth['service_account_email']}")
        scope = auth.get("scope")
        if scope:
            cmd.append(f"--oauth-token-scope={scope}")

    return cmd


def _snapshot_targets(
    schedulers: List[Dict[str, Any]],
    project: str,
    region: str,
    output_dir: Path,
    dry_run: bool,
) -> Path:
    now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snapshot_path = output_dir / f"scheduler_cutover_snapshot_{now}.json"
    payload: Dict[str, Any] = {
        "created_at_utc": now,
        "project_id": project,
        "region": region,
        "schedulers": [],
    }
    for scheduler in schedulers:
        cmd = [
            "gcloud",
            "scheduler",
            "jobs",
            "describe",
            scheduler["name"],
            f"--location={scheduler.get('location', 'us-central1')}",
            f"--project={project}",
            "--format=json",
        ]
        if dry_run:
            print("DRY-RUN:", " ".join(cmd), flush=True)
            continue
        described = json.loads(subprocess.check_output(cmd, text=True))
        payload["schedulers"].append(described)
    if not dry_run:
        snapshot_path.write_text(json.dumps(payload, indent=2) + "\n")
    return snapshot_path


def apply_or_cutover(args: argparse.Namespace) -> int:
    manifest = load_manifest(Path(args.manifest))
    project = args.project or manifest["project_id"]
    region = args.region or manifest["region"]
    schedulers = manifest.get("schedulers", [])

    if args.action == "cutover":
        target_schedulers = [s for s in schedulers if s.get("canary_cutover") and s.get("cloud_run_job_target")]
    else:
        target_schedulers = schedulers

    if not target_schedulers:
        print("No schedulers matched action.", flush=True)
        return 0

    snapshot_dir = Path("ops/cloud/baseline")
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = _snapshot_targets(target_schedulers, project, region, snapshot_dir, args.dry_run)
    if not args.dry_run:
        print(f"Saved scheduler snapshot: {snapshot_path}", flush=True)

    for scheduler in target_schedulers:
        uri = scheduler["http"]["uri"]
        target = scheduler.get("cloud_run_job_target")
        if args.action == "cutover" and target:
            job_name = f"{target['job_name']}{args.suffix}"
            uri = _cloud_run_job_uri(project, region, job_name, target.get("api_version", "v2"))
        cmd = _scheduler_update_cmd(scheduler, uri)
        cmd.append(f"--project={project}")
        run_cmd(cmd, args.dry_run)

        state = scheduler.get("state", "ENABLED")
        if state == "PAUSED":
            pause_cmd = [
                "gcloud",
                "scheduler",
                "jobs",
                "pause",
                scheduler["name"],
                f"--location={scheduler.get('location', 'us-central1')}",
                f"--project={project}",
            ]
            run_cmd(pause_cmd, args.dry_run)
        elif state == "ENABLED":
            resume_cmd = [
                "gcloud",
                "scheduler",
                "jobs",
                "resume",
                scheduler["name"],
                f"--location={scheduler.get('location', 'us-central1')}",
                f"--project={project}",
            ]
            run_cmd(resume_cmd, args.dry_run)

    print(json.dumps({"ok": True, "action": args.action, "count": len(target_schedulers)}, ensure_ascii=True), flush=True)
    return 0


def rollback(args: argparse.Namespace) -> int:
    snapshot = json.loads(Path(args.snapshot).read_text())
    schedulers = snapshot.get("schedulers", [])
    if not schedulers:
        print("No scheduler entries in snapshot.", flush=True)
        return 0
    for scheduler in schedulers:
        name = scheduler["name"].split("/")[-1]
        location = scheduler["name"].split("/locations/")[1].split("/")[0]
        current = {
            "name": name,
            "location": location,
            "schedule": scheduler["schedule"],
            "time_zone": scheduler["timeZone"],
            "http": {
                "uri": scheduler["httpTarget"]["uri"],
                "method": scheduler["httpTarget"].get("httpMethod", "POST"),
                "headers": scheduler["httpTarget"].get("headers", {}),
                "body_base64": scheduler["httpTarget"].get("body"),
            },
            "auth": {},
        }
        if "oidcToken" in scheduler["httpTarget"]:
            t = scheduler["httpTarget"]["oidcToken"]
            current["auth"] = {
                "type": "oidc",
                "service_account_email": t.get("serviceAccountEmail"),
                "audience": t.get("audience"),
            }
        elif "oauthToken" in scheduler["httpTarget"]:
            t = scheduler["httpTarget"]["oauthToken"]
            current["auth"] = {
                "type": "oauth",
                "service_account_email": t.get("serviceAccountEmail"),
                "scope": t.get("scope"),
            }
        cmd = _scheduler_update_cmd(current, current["http"]["uri"])
        cmd.append(f"--project={args.project}")
        run_cmd(cmd, args.dry_run)
        state = scheduler.get("state", "ENABLED")
        state_cmd = "pause" if state == "PAUSED" else "resume"
        run_cmd(
            [
                "gcloud",
                "scheduler",
                "jobs",
                state_cmd,
                name,
                f"--location={location}",
                f"--project={args.project}",
            ],
            args.dry_run,
        )

    print(json.dumps({"ok": True, "action": "rollback", "count": len(schedulers)}, ensure_ascii=True), flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage Scheduler jobs from jobs manifest.")
    parser.add_argument("--manifest", default="ops/cloud/jobs.yaml", help="Path to jobs manifest.")
    parser.add_argument("--project", default="project-41542e21-470f-4589-96d", help="Project override.")
    parser.add_argument("--region", default="us-central1", help="Region override.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing.")

    sub = parser.add_subparsers(dest="mode", required=True)

    apply_parser = sub.add_parser("apply", help="Apply scheduler definitions from manifest.")
    apply_parser.add_argument("--action", choices=["apply", "cutover"], default="apply")
    apply_parser.add_argument("--suffix", default="-v2", help="Suffix for cutover target jobs.")

    rollback_parser = sub.add_parser("rollback", help="Rollback scheduler changes from snapshot.")
    rollback_parser.add_argument("--snapshot", required=True, help="Snapshot file path.")

    args = parser.parse_args()
    if args.mode == "rollback":
        return rollback(args)
    return apply_or_cutover(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        print(f"Scheduler operation failed: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(exc.returncode)
