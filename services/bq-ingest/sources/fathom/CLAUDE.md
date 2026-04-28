# Fathom — Doc Index

| Load | Doc | Path | Triggers |
|------|-----|------|----------|
| Always | Fathom Index | `docs/sources/fathom/INDEX.md` | Any Fathom work |
| On trigger | SQL Models (core) | `sql/fathom_models.sql` | schema, query, SQL, model, raw_, core_, fathom_ |
| On trigger | SQL Models (LLM enrichment) | `enrichment/fathom/sql/fathom_llm_analysis.sql` | enrichment, LLM, analysis, transcript, sentiment |
| On trigger | Cloud Run Setup | `docs/runbooks/CLOUD_RUN_SETUP.md` | fathom enrichment, LLM, Gemini, cloud run setup, deploy enrichment |
| On trigger | Backfill Runbook | `docs/runbooks/BACKFILL_RUNBOOK.md` | backfill, historical, retry, resume |
| On trigger | Engineer Playbook | `docs/guides/GTM_ENGINEER_PLAYBOOK.md` | debug, broken, incident, healthcheck, ops |

## Files

| File | Purpose |
|------|---------|
| `sources/fathom/fathom_pipeline.py` | Transcript ingest + model runner (core lane) |
| `sources/fathom/fathom_backfill.py` | Historical transcript backfill |
| `enrichment/fathom/` | LLM enrichment lane (separate container — Gemini SDK, high memory) |

**Note:** Fathom has two SQL trees. Core SQL lives in `sql/fathom_models.sql`. LLM enrichment SQL lives in `enrichment/fathom/sql/`. These are intentionally separate — different runtime profiles.
