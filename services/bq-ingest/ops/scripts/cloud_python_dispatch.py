#!/usr/bin/env python3
import json
from pathlib import Path
import sys
import traceback

# Ensure repository root is importable when invoked as a script path in Cloud Run.
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from ops.runner.tasks import run_callable_from_env


def main() -> int:
    try:
        output = run_callable_from_env()

        print(
            json.dumps(
                {
                    "ok": True,
                    "target": output["target"],
                    "result": output["result"],
                },
                ensure_ascii=True,
            ),
            flush=True,
        )
        return 0
    except Exception as exc:  # pragma: no cover
        print(
            json.dumps(
                {
                    "ok": False,
                    "target": "unknown",
                    "error": str(exc),
                },
                ensure_ascii=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
