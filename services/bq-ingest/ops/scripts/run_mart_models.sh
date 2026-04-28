#!/usr/bin/env bash
# Refresh BigQuery Marts (dim_golden_contact, payment lines, campaign funnel rpt).
# Prerequisites: Core datasets built (GHL, Calendly, Fanbasis at minimum).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
exec python3 -m ops.runner.cli run model.marts
