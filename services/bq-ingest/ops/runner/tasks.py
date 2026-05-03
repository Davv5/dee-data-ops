from __future__ import annotations

import base64
import importlib
import json
import os
import subprocess
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]


def _resolve_callable(target: str) -> Callable[..., Any]:
    module_name, callable_path = target.split(":", 1)
    module = importlib.import_module(module_name)
    resolved: Any = module
    for attribute in callable_path.split("."):
        resolved = getattr(resolved, attribute)
    if not callable(resolved):
        raise TypeError(f"Resolved target is not callable: {target}")
    return resolved


def _run_target(target: str, args: List[Any] | None = None, kwargs: Dict[str, Any] | None = None) -> Any:
    callable_obj = _resolve_callable(target)
    return callable_obj(*(args or []), **(kwargs or {}))


def _decode_json_env(direct_key: str, b64_key: str, default: Any) -> Any:
    direct_value = os.getenv(direct_key)
    if direct_value:
        return json.loads(direct_value)
    b64_value = os.getenv(b64_key)
    if b64_value:
        return json.loads(base64.b64decode(b64_value.encode("utf-8")).decode("utf-8"))
    return default


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def run_callable(target: str, args: List[Any] | None = None, kwargs: Dict[str, Any] | None = None) -> Any:
    return _run_target(target, args=args, kwargs=kwargs)


def run_callable_from_env() -> Dict[str, Any]:
    target = (os.getenv("PYTHON_TARGET") or "").strip()
    if not target:
        raise ValueError("PYTHON_TARGET is required.")
    args = _decode_json_env("PYTHON_ARGS_JSON", "PYTHON_ARGS_JSON_B64", [])
    kwargs = _decode_json_env("PYTHON_KWARGS_JSON", "PYTHON_KWARGS_JSON_B64", {})
    result = run_callable(target, args=args, kwargs=kwargs)
    return {"target": target, "result": _json_safe(result)}


TASK_TARGETS: Dict[str, str] = {
    "model.ghl": "sources.ghl.ghl_pipeline:run_models",
    "model.calendly": "sources.calendly.calendly_pipeline:run_models",
    "model.typeform": "sources.typeform.typeform_pipeline:run_models",
    "model.stripe": "sources.stripe.stripe_pipeline:run_models",
    "model.marts": "sources.marts.mart_models:run_mart_models",
    "model.fathom": "sources.fathom.fathom_pipeline:run_models",
    "backfill.calendly": "sources.calendly.calendly_backfill:main",
    "backfill.calendly_invitee_drain": "sources.calendly.calendly_invitee_drain:main",
    "backfill.fanbasis": "sources.fanbasis.fanbasis_backfill:main",
    "backfill.fanbasis_identity": "sources.fanbasis.fanbasis_pipeline:run_identity_backfill",
    "backfill.fathom": "sources.fathom.fathom_backfill:main",
    "backfill.ghl": "sources.ghl.ghl_backfill:main",
    "backfill.ghl_call_logs": "sources.ghl.ghl_call_log_backfill:main",
    "backfill.ghl_form_submissions": "sources.ghl.ghl_form_submissions_backfill:main",
    "sync.fanbasis_missing_ghl_contacts": "sources.ghl.fanbasis_contact_sync:sync_missing_fanbasis_contacts",
    "snapshot.ghl_pipeline_stages": "sources.ghl.ghl_pipeline:snapshot_pipeline_stages_daily",
    "backfill.stripe": "sources.stripe.stripe_backfill:main",
    "identity.ghl_users_sync": "sources.identity.identity_pipeline:run_identity_resolution_pipeline",
    "quality.dq": "sources.shared.data_quality:main",
    "quality.phase1_release_gate": "sources.shared.phase1_release_gate:run_phase1_release_gate",
}

PIPELINE_MODEL_TASKS: List[Tuple[str, str]] = [
    ("GHL", "model.ghl"),
    ("Calendly", "model.calendly"),
    ("Fathom", "model.fathom"),
    ("Typeform", "model.typeform"),
    ("Stripe", "model.stripe"),
    ("Marts", "model.marts"),
]


def run_pipeline_models() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    for label, task_name in PIPELINE_MODEL_TASKS:
        print(f"Running {label} models...", flush=True)
        result = run_task(task_name)
        results[task_name] = result
        print(f"✓ {label}: {_json_safe(result)}", flush=True)
    print("All model pipelines completed successfully!", flush=True)
    return results


def run_marts_with_dependencies() -> Dict[str, Any]:
    """
    Closes the freshness gap by refreshing high-frequency upstream models
    immediately before rebuilding marts.

    Order matters: Calendly first (independent), Fathom second (independent
    Core layer; the LLM-classifier-derived tables stay outside this hourly
    path), GHL third (its `mrt_speed_to_lead_daily` mart at the end of
    `ghl_models.sql` joins fresh `Core.fct_calendly_*` so Calendly must be
    fresh first), then Marts (consumes all of the above via marts.sql).

    GHL was added 2026-04-29 to support removing
    `GHL_RUN_MODELS_AFTER_INCREMENTAL=true` from the bq-ingest service
    without dropping speed-to-lead refresh cadence — see
    `.claude/rules/bq-ingest.md` §"Hourly HTTP path skips heavy model
    refresh" for the architectural rule.
    """
    print("Pre-refreshing Mart dependencies (Calendly/Fathom/GHL)...", flush=True)
    calendly_result = run_task("model.calendly")
    fathom_result = run_task("model.fathom")
    ghl_result = run_task("model.ghl")

    print("Dependencies refreshed. Rebuilding Marts...", flush=True)
    marts_result = run_task("model.marts")
    return {
        "dependencies": {
            "model.calendly": calendly_result,
            "model.fathom": fathom_result,
            "model.ghl": ghl_result,
        },
        "model.marts": marts_result,
    }


def run_validation() -> Dict[str, Any]:
    validation_script = ROOT_DIR / "ops" / "scripts" / "validate_marts.sh"
    env = os.environ.copy()
    env["AUTO_TRIAGE_ON_FAIL"] = "false"
    completed = subprocess.run(
        ["bash", str(validation_script)],
        cwd=str(ROOT_DIR),
        env=env,
        check=True,
        capture_output=False,
        text=True,
    )
    return {"returncode": completed.returncode}


def run_pipeline_full() -> Dict[str, Any]:
    model_results = run_pipeline_models()
    print("Running validation...", flush=True)
    validation_result = run_validation()
    print("Running Phase 1 release gate...", flush=True)
    release_gate_result = run_task("quality.phase1_release_gate")
    if not isinstance(release_gate_result, dict):
        raise RuntimeError("Phase 1 release gate returned non-dict result.")
    if not release_gate_result.get("ok", False):
        hard_failures = release_gate_result.get("hard_failures") or []
        failure_summary = ", ".join(
            f"{failure.get('gate_name', 'unknown_gate')}: {failure.get('message', 'failed')}"
            for failure in hard_failures
        ) or "unknown hard failure"
        raise RuntimeError(f"Phase 1 release gate failed: {failure_summary}")
    print("✓ Phase 1 release gate passed", flush=True)
    return {
        "models": model_results,
        "validation": validation_result,
        "phase1_release_gate": release_gate_result,
    }


def run_task(task_name: str) -> Any:
    if task_name == "pipeline.models":
        return run_pipeline_models()
    if task_name == "pipeline.full":
        return run_pipeline_full()
    if task_name == "pipeline.marts_refresh_hourly":
        return run_marts_with_dependencies()
    if task_name == "callable.env":
        return run_callable_from_env()
    target = TASK_TARGETS.get(task_name)
    if target is None:
        raise KeyError(f"Unknown task: {task_name}")
    return _run_target(target)


def list_tasks() -> List[str]:
    return sorted(["pipeline.models", "pipeline.full", "pipeline.marts_refresh_hourly", "callable.env", *TASK_TARGETS.keys()])


def run_task_safe(task_name: str) -> Dict[str, Any]:
    try:
        result = run_task(task_name)
        return {"ok": True, "task": task_name, "result": _json_safe(result)}
    except Exception as exc:  # pragma: no cover
        return {
            "ok": False,
            "task": task_name,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
