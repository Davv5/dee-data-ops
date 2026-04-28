"""Refresh BigQuery Marts (Looker Studio reporting layer) from Core tables.

Always runs upstream dependency models first to keep mart inputs fresh.
Current dependency order:
1. ghl_models
2. typeform_models
3. marts.sql
4. sql/dims/*.sql (all files, sorted — runs after marts so Calendly/Core tables are fresh)
"""

import os
from pathlib import Path
from typing import List, Optional

from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID")


def _split_sql_statements(sql_text: str) -> List[str]:
    lines: List[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def run_mart_models(sql_file_path: Optional[str] = None) -> int:
    """Run dependency models then execute sql/marts.sql and sql/dims/*.sql in order."""
    from sources.ghl.ghl_pipeline import run_models as run_ghl_models
    from sources.typeform.typeform_pipeline import run_models as run_typeform_models

    run_ghl_models()
    run_typeform_models()

    if sql_file_path is None:
        sql_file_path = str(Path(__file__).resolve().parent / "sql" / "marts.sql")
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)
    client = bigquery.Client(project=PROJECT_ID)
    executed = 0
    for i, stmt in enumerate(statements, start=1):
        try:
            client.query(stmt).result()
        except Exception as exc:
            raise RuntimeError(f"mart_models failed at statement {i}/{len(statements)}: {exc}") from exc
        executed += 1

    dims_dir = Path(__file__).resolve().parent / "sql" / "dims"
    for dims_file in sorted(dims_dir.glob("*.sql")):
        dims_text = dims_file.read_text(encoding="utf-8")
        dims_statements = _split_sql_statements(dims_text)
        for i, stmt in enumerate(dims_statements, start=1):
            try:
                client.query(stmt).result()
            except Exception as exc:
                raise RuntimeError(f"dim_models ({dims_file.name}) failed at statement {i}/{len(dims_statements)}: {exc}") from exc
            executed += 1

    return executed
