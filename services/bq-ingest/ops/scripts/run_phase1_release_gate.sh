#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

python3 <<'PY'
import json
import sys

from sources.shared.phase1_release_gate import run_phase1_release_gate

result = run_phase1_release_gate()
print(json.dumps(result, indent=2, default=str))
if not result.get("ok"):
    sys.exit(1)
PY
