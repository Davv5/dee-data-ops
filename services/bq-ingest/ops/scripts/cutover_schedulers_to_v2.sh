#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
exec python3 ops/scripts/manage_schedulers_from_manifest.py apply --action cutover --suffix="${1:--v2}"

