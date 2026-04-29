#!/usr/bin/env bash
# Run data quality tests against BigQuery Marts and Core tables.
# Exits 1 if any test FAILs.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
exec python3 -m sources.shared.data_quality
