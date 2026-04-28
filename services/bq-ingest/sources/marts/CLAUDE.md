# Marts — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Marts Index | `docs/sources/marts/INDEX.md` | Any Marts or Looker work |
| On trigger | SQL | `sql/marts.sql` | schema, query, SQL, mart, fct_, rpt_, golden_contact |
| On trigger | Mart Metric Dictionary | `docs/guides/MART_METRIC_DICTIONARY.md` | metric, looker, report, golden_contact, fct_, rpt_, dashboard |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, stale data, healthcheck, ops |
| On trigger | Execution Rules | `docs/runbooks/AGENT_EXECUTION_RULES.md` | run, execute, deploy, refresh, cloud run |

## Files

| File | Purpose |
|------|---------|
| `sources/marts/mart_models.py` | Runs upstream dependency models (GHL, Typeform), then executes `sql/marts.sql` and `sql/dims/*.sql` |

**Safe change order:** Marts always run last — Raw → Core → Marts. Never skip steps.
