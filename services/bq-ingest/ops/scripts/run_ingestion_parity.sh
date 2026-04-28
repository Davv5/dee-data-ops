#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

python3 <<'PY'
from pathlib import Path
from google.cloud import bigquery

PROJECT = 'project-41542e21-470f-4589-96d'
SQL_PATH = Path('sql/ingestion_parity_validation.sql')

sql_text = SQL_PATH.read_text(encoding='utf-8')

chunks = []
acc = []
for line in sql_text.splitlines():
    stripped = line.strip()
    if stripped.startswith('--'):
        continue
    acc.append(line)
    if ';' in line:
        stmt = '\n'.join(acc).strip()
        if stmt:
            chunks.append(stmt.rstrip(';').strip())
        acc = []

if acc:
    stmt = '\n'.join(acc).strip()
    if stmt:
        chunks.append(stmt.rstrip(';').strip())

client = bigquery.Client(project=PROJECT)
for i, stmt in enumerate(chunks, start=1):
    print(f"\n===== ingestion_parity statement {i}/{len(chunks)} =====")
    rows = list(client.query(stmt).result())
    if not rows:
        print('(no rows)')
        continue
    for row in rows:
        print(dict(row.items()))
PY
