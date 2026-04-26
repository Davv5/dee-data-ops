---
paths:
  - "2-dbt/models/marts/rollups/**"
---

# Live-by-default conventions for rollup marts

Rollup marts under `2-dbt/models/marts/rollups/**` are intended to support near-real-time BI consumers. The defaults below keep them refreshable on a sub-minute cadence without per-model configuration.

## Materialization defaults

Rollup models default to:

- `materialized: incremental`
- `incremental_strategy: insert_overwrite`
- `on_schema_change: append_new_columns`

Configure at the directory level in `dbt_project.yml` rather than per-model so new rollups inherit automatically.

(source: "How to Create a Data Modeling Pipeline (3 Layer Approach)", Data Ops notebook — incremental strategy for real-time marts; "3-environment design" MDS starter guide, Data Ops notebook — dev/prod separation)

## Refresh cadence

Sub-minute or near-real-time rollups should be rebuilt by an external scheduler (e.g., Cloud Run Job + Cloud Scheduler), not the regular dbt build cadence. A nightly full-refresh reconciles any incremental drift.

Upstream NRT ingestion conventions (1-min hot endpoints, 15-min cold endpoints, Cloud Run Jobs vs GHA cron) live in `.claude/rules/ingest.md` — the rollup defaults here pair with that ingestion shape.

## Related rules

- `.claude/rules/ingest.md` — NRT extractor cadence (`1-raw-landing/**`)
- `.claude/rules/mart-naming.md` — naming conventions for marts

## Historical

This rule originally encoded a Metabase-coupled live-by-default pattern (cache_ttl, URL-fragment auto-refresh, freshness tile). Metabase was retired 2026-04; that content is preserved at `docs/_archive/metabase.md` and `docs/_archive/metabase-live-dashboard-setup.md`. When the dabi BI surface ships (parked plans at `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md` and `…002…`), expand this rule with the dabi-era cadence pattern.
