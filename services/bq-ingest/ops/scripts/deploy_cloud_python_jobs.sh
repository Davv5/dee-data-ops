#!/usr/bin/env bash
# Backward-compatible wrapper.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
exec ops/scripts/deploy_runtime_stack.sh "$@"

