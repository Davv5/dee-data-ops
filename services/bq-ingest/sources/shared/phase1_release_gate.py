from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from google.cloud import bigquery

from sources.shared.warehouse_healthcheck import run_healthcheck

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "project-41542e21-470f-4589-96d")
OPS_DATASET = os.getenv("BQ_OPS_DATASET", "Ops")
RESULTS_TABLE = f"{PROJECT_ID}.{OPS_DATASET}.phase1_release_gate_results"
SQL_FILE = os.getenv(
    "PHASE1_RELEASE_GATE_SQL_FILE",
    str(Path(__file__).resolve().parents[2] / "sql" / "phase1_release_gate.sql"),
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _client() -> bigquery.Client:
    if not PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID")
    return bigquery.Client(project=PROJECT_ID)


def ensure_results_table(client: bigquery.Client) -> None:
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{RESULTS_TABLE}` (
          run_id STRING NOT NULL,
          checked_at TIMESTAMP NOT NULL,
          gate_name STRING NOT NULL,
          severity STRING NOT NULL,
          comparison STRING NOT NULL,
          status STRING NOT NULL,
          metric_value FLOAT64,
          pass_threshold FLOAT64,
          warn_threshold FLOAT64,
          message STRING,
          details_json STRING
        )
        PARTITION BY DATE(checked_at)
        CLUSTER BY severity, status, gate_name
        """
    ).result()


def parse_gate_statements(sql_text: str) -> List[str]:
    blocks = sql_text.split("-- GATE")
    statements: List[str] = []
    for block in blocks:
        raw_lines = [line for line in block.splitlines() if line.strip()]
        if raw_lines:
            raw_lines = raw_lines[1:]
        lines = [line for line in raw_lines if not line.strip().startswith("--")]
        statement = "\n".join(lines).strip().rstrip(";")
        if statement:
            statements.append(statement)
    return statements


def _normalize_gate_row(row: Dict[str, Any], run_id: str, checked_at: str) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "checked_at": checked_at,
        "gate_name": row.get("gate_name", "unknown_gate"),
        "severity": row.get("severity", "hard"),
        "comparison": row.get("comparison", "<="),
        "status": row.get("status", "FAIL"),
        "metric_value": None if row.get("metric_value") is None else float(row["metric_value"]),
        "pass_threshold": None if row.get("pass_threshold") is None else float(row["pass_threshold"]),
        "warn_threshold": None if row.get("warn_threshold") is None else float(row["warn_threshold"]),
        "message": row.get("message"),
        "details_json": row.get("details_json"),
    }


def run_sql_gates(client: bigquery.Client, run_id: str) -> List[Dict[str, Any]]:
    sql_text = Path(SQL_FILE).read_text(encoding="utf-8")
    statements = parse_gate_statements(sql_text)
    checked_at = _now_iso()
    results: List[Dict[str, Any]] = []

    for statement in statements:
        try:
            rows = list(client.query(statement).result())
            if not rows:
                results.append(
                    {
                        "run_id": run_id,
                        "checked_at": checked_at,
                        "gate_name": "unknown_gate",
                        "severity": "hard",
                        "comparison": "<=",
                        "status": "FAIL",
                        "metric_value": None,
                        "pass_threshold": None,
                        "warn_threshold": None,
                        "message": "release gate query returned no rows",
                        "details_json": None,
                    }
                )
                continue
            row_dict = dict(rows[0].items())
            results.append(_normalize_gate_row(row_dict, run_id=run_id, checked_at=checked_at))
        except Exception as exc:  # pragma: no cover
            results.append(
                {
                    "run_id": run_id,
                    "checked_at": checked_at,
                    "gate_name": "sql_execution_error",
                    "severity": "hard",
                    "comparison": "<=",
                    "status": "FAIL",
                    "metric_value": None,
                    "pass_threshold": None,
                    "warn_threshold": None,
                    "message": str(exc)[:500],
                    "details_json": statement[:1000],
                }
            )

    return results


def run_healthcheck_gate(run_id: str) -> Dict[str, Any]:
    checked_at = _now_iso()
    healthcheck = run_healthcheck()
    failure_count = int(healthcheck.get("failure_count", 0) or 0)
    return {
        "run_id": run_id,
        "checked_at": checked_at,
        "gate_name": "hard_warehouse_healthcheck",
        "severity": "hard",
        "comparison": "<=",
        "status": "PASS" if healthcheck.get("ok") else "FAIL",
        "metric_value": float(failure_count),
        "pass_threshold": 0.0,
        "warn_threshold": 0.0,
        "message": f"warehouse healthcheck failure_count = {failure_count}",
        "details_json": json.dumps(healthcheck, default=str),
    }


def write_results(client: bigquery.Client, results: List[Dict[str, Any]]) -> None:
    if not results:
        return
    errors = client.insert_rows_json(RESULTS_TABLE, results)
    if errors:
        raise RuntimeError(f"Failed to write phase1 release gate results: {errors}")


def run_phase1_release_gate() -> Dict[str, Any]:
    client = _client()
    ensure_results_table(client)

    run_id = f"phase1-release-gate-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    results = run_sql_gates(client, run_id=run_id)
    results.append(run_healthcheck_gate(run_id=run_id))
    write_results(client, results)

    hard_failures = [r for r in results if r["severity"] == "hard" and r["status"] == "FAIL"]
    soft_failures = [r for r in results if r["severity"] == "soft" and r["status"] == "FAIL"]
    warnings = [r for r in results if r["status"] == "WARN"]

    return {
        "ok": len(hard_failures) == 0,
        "run_id": run_id,
        "checked_at": _now_iso(),
        "hard_fail_count": len(hard_failures),
        "soft_fail_count": len(soft_failures),
        "warn_count": len(warnings),
        "hard_failures": [
            {"gate_name": r["gate_name"], "message": r["message"], "metric_value": r["metric_value"]}
            for r in hard_failures
        ],
        "soft_failures": [
            {"gate_name": r["gate_name"], "message": r["message"], "metric_value": r["metric_value"]}
            for r in soft_failures
        ],
        "warnings": [
            {"gate_name": r["gate_name"], "message": r["message"], "metric_value": r["metric_value"]}
            for r in warnings
        ],
        "all_results": results,
    }


def main() -> None:
    result = run_phase1_release_gate()
    print(json.dumps(result, indent=2, default=str))
    if not result.get("ok"):
        sys.exit(1)


if __name__ == "__main__":
    main()
