# Shared Utilities — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, ops, healthcheck, broken, incident |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, cloud run |
| On trigger | Warehouse Technical Brief | `docs/guides/WAREHOUSE_TECHNICAL_BRIEF.md` | architecture, schema, bigquery, dataset, raw, core |

## Files

| File | Purpose |
|------|---------|
| `sources/shared/warehouse_healthcheck.py` | Cross-source freshness + row count health checks |
| `sources/shared/warehouse_queries.py` | Named analytical query catalog (used by `/query` endpoint in app.py) |
| `sources/shared/data_quality.py` | DQ test suite across all sources |
| `sources/shared/phase1_release_gate.py` | Revenue match gate — hard blocks release if data diverges |
| `sources/shared/analyst.py` | Gemini-backed natural language → BigQuery SQL analyst |
