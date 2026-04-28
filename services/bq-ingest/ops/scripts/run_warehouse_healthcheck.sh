#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

python3 <<'PY'
import json
import sys

from sources.shared.warehouse_healthcheck import run_healthcheck

result = run_healthcheck()
print(json.dumps(result, indent=2, default=str))
if not result.get("ok"):
    sys.exit(1)
PY

