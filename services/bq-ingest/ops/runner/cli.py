#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List

from ops.runner.tasks import list_tasks, run_callable, run_task_safe


def _parse_json(value: str, expected_type: type) -> Any:
    parsed = json.loads(value)
    if not isinstance(parsed, expected_type):
        raise ValueError(f"Expected JSON {expected_type.__name__}: {value}")
    return parsed


def _print_output(payload: Dict[str, Any], json_output: bool) -> None:
    if json_output:
        print(json.dumps(payload, ensure_ascii=True), flush=True)
    else:
        if payload.get("ok"):
            print(f"Task succeeded: {payload.get('task')}", flush=True)
            if "result" in payload:
                print(json.dumps(payload["result"], ensure_ascii=True), flush=True)
        else:
            print(f"Task failed: {payload.get('task')}", file=sys.stderr, flush=True)
            print(payload.get("error", "Unknown error"), file=sys.stderr, flush=True)


def _run_task_command(args: argparse.Namespace) -> int:
    if args.task == "callable":
        if not args.target:
            print("--target is required for task 'callable'.", file=sys.stderr, flush=True)
            return 2
        call_args = _parse_json(args.args_json, list) if args.args_json else []
        call_kwargs = _parse_json(args.kwargs_json, dict) if args.kwargs_json else {}
        try:
            result = run_callable(args.target, args=call_args, kwargs=call_kwargs)
            payload = {"ok": True, "task": "callable", "target": args.target, "result": result}
        except Exception as exc:
            payload = {"ok": False, "task": "callable", "target": args.target, "error": str(exc)}
    else:
        payload = run_task_safe(args.task)

    _print_output(payload, args.json)
    return 0 if payload.get("ok") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cloud runtime task runner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List available tasks.")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    run_parser = subparsers.add_parser("run", help="Run a named task.")
    run_parser.add_argument("task", help="Task name (or 'callable').")
    run_parser.add_argument("--target", help="Callable target module:function for callable task.")
    run_parser.add_argument("--args-json", help="JSON array of positional args for callable task.")
    run_parser.add_argument("--kwargs-json", help="JSON object of keyword args for callable task.")
    run_parser.add_argument("--json", action="store_true", help="Emit JSON.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        tasks: List[str] = [*list_tasks(), "callable"]
        if args.json:
            print(json.dumps({"tasks": tasks}, ensure_ascii=True), flush=True)
        else:
            for task in tasks:
                print(task, flush=True)
        return 0

    if args.command == "run":
        return _run_task_command(args)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

