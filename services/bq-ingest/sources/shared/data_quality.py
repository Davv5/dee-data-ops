"""Data quality test runner for the GTM Lead Warehouse.

Executes each test in sql/data_quality_tests.sql, writes results to
Raw.dq_test_results, prints a summary, and exits 1 if any test FAILs.

Usage:
    python3 data_quality.py

Env:
    GCP_PROJECT_ID   BigQuery project
    BQ_DATASET       Raw dataset, default 'Raw'
    DQ_SQL_FILE      Path to test SQL, default sql/data_quality_tests.sql
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = os.getenv("BQ_DATASET", "Raw")
RESULTS_TABLE = f"{PROJECT_ID}.{DATASET}.dq_test_results"
SQL_FILE = os.getenv("DQ_SQL_FILE", str(Path(__file__).resolve().parent / "sql" / "data_quality_tests.sql"))

client = bigquery.Client(project=PROJECT_ID)


def ensure_results_table() -> None:
    client.query(f"""
    CREATE TABLE IF NOT EXISTS `{RESULTS_TABLE}` (
      run_id        STRING NOT NULL,
      checked_at    TIMESTAMP NOT NULL,
      test_name     STRING NOT NULL,
      status        STRING NOT NULL,
      failing_rows  INT64,
      message       STRING
    )
    PARTITION BY DATE(checked_at)
    CLUSTER BY status, test_name
    """).result()


def parse_tests(sql_text: str) -> List[str]:
    blocks = sql_text.split("-- TEST")
    tests = []
    for block in blocks:
        stmt = block.strip()
        # Drop comment-only lines
        lines = [l for l in stmt.splitlines() if l.strip() and not l.strip().startswith("--")]
        if lines:
            tests.append("\n".join(lines).strip().rstrip(";"))
    return tests


def run_tests(run_id: str) -> List[Dict[str, Any]]:
    sql_text = Path(SQL_FILE).read_text(encoding="utf-8")
    tests = parse_tests(sql_text)
    checked_at = datetime.now(timezone.utc).isoformat()
    results = []

    for stmt in tests:
        try:
            rows = list(client.query(stmt).result())
            if not rows:
                # Query returned no rows — treat as PASS with unknown test name
                results.append({
                    "run_id": run_id,
                    "checked_at": checked_at,
                    "test_name": "unknown",
                    "status": "PASS",
                    "failing_rows": 0,
                    "message": "no rows returned",
                })
                continue
            row = rows[0]
            results.append({
                "run_id": run_id,
                "checked_at": checked_at,
                "test_name": row["test_name"],
                "status": row["status"],
                "failing_rows": row["failing_rows"],
                "message": row["message"],
            })
        except Exception as exc:
            # Extract test name from first line of SQL if possible
            first_line = next(
                (l.strip() for l in stmt.splitlines() if "test_name" in l.lower()),
                stmt[:80]
            )
            results.append({
                "run_id": run_id,
                "checked_at": checked_at,
                "test_name": first_line,
                "status": "FAIL",
                "failing_rows": -1,
                "message": str(exc)[:500],
            })

    return results


def write_results(results: List[Dict[str, Any]]) -> None:
    if not results:
        return
    errors = client.insert_rows_json(RESULTS_TABLE, results)
    if errors:
        print(f"WARNING: failed to write some DQ results: {errors}", file=sys.stderr)


def run_dq() -> Dict[str, Any]:
    """Run all DQ tests and return a structured result dict.

    Returns a dict with keys: ok, run_id, pass_count, warn_count, fail_count,
    warnings (list), failures (list), all_results (list).
    Writes results to Raw.dq_test_results but does NOT call sys.exit.
    """
    ensure_results_table()
    run_id = f"dq-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    results = run_tests(run_id)
    write_results(results)

    passed = [r for r in results if r["status"] == "PASS"]
    warned = [r for r in results if r["status"] == "WARN"]
    failed = [r for r in results if r["status"] == "FAIL"]

    return {
        "ok": len(failed) == 0,
        "run_id": run_id,
        "pass_count": len(passed),
        "warn_count": len(warned),
        "fail_count": len(failed),
        "warnings": [{"test_name": r["test_name"], "message": r["message"]} for r in warned],
        "failures": [{"test_name": r["test_name"], "message": r["message"]} for r in failed],
        "all_results": results,
    }


def main() -> None:
    result = run_dq()
    print(f"Running data quality tests run_id={result['run_id']}")
    print(f"\nResults: {result['pass_count']} PASS  {result['warn_count']} WARN  {result['fail_count']} FAIL")

    if result["warnings"]:
        print("\nWARNINGS:")
        for w in result["warnings"]:
            print(f"  WARN  {w['test_name']}: {w['message']}")

    if result["failures"]:
        print("\nFAILURES:")
        for f in result["failures"]:
            print(f"  FAIL  {f['test_name']}: {f['message']}")
        sys.exit(1)

    print("\nAll tests passed.")


if __name__ == "__main__":
    main()
