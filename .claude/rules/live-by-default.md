---
paths:
  - "2-dbt/models/marts/rollups/**"
  - "3-bi/metabase/authoring/dashboards/**"
  - "3-bi/metabase/authoring/sync.py"
  - "3-bi/metabase/authoring/infrastructure/caching_config.py"
---

# Live-by-default conventions

As of 2026-04 the Speed-to-Lead panel ships with ~60s end-to-end
freshness (raw landing → dashboard render). The defaults below are
encoded so NEW rollups and dashboards inherit that behavior without
per-model / per-tile configuration.

## dbt (see `dbt-marts-rollups.md` for detail)

- Rollups under `marts/rollups/**` default to `materialized: incremental`,
  `incremental_strategy: insert_overwrite`, `on_schema_change: append_new_columns`
  via `dbt_project.yml`.
- 2-minute Cloud Run Job rebuilds `stl_* sales_activity_detail` on prod.
- Nightly full-refresh (08:00 UTC) reconciles any drift.

(source: "How to Create a Data Modeling Pipeline (3 Layer Approach)",
Data Ops notebook — incremental strategy for real-time marts; "3-environment
design" MDS starter guide, Data Ops notebook — dev/prod separation)

## Custom-source ingestion (see `ingest.md` for detail)

- Near-real-time sources (sub-5-min SLA, dashboard-load-bearing) run on
  Cloud Run Jobs + Cloud Scheduler.
- Hot endpoints: 1-min cadence. Cold endpoints: 15-min cadence.
- GHA cron remains the default for daily-or-coarser sources + as manual
  backstop for the NRT sources.

## Metabase

- `upsert_card` defaults `cache_ttl=0` (no per-question cache). A question
  returns the latest query every render.
  (source: "Caching query results", Metabase Learn notebook, source d6a8e3ae —
  cache_ttl=0 = explicit bypass; cache_ttl=null = inherit server default)

- Dashboard auto-refresh is a **frontend-only / URL-fragment feature** on
  Metabase OSS. There is no REST API payload key (`refresh_period`,
  `auto_refresh_seconds`, etc.) — the `/api/dashboard` PUT endpoint does not
  accept or persist a refresh interval.
  To enable 60-second auto-refresh, append `#refresh=60` to the dashboard URL
  (public share link or iframe src). Embedded dashboards inherit this by
  including the fragment in the embedding URL.
  (source: "Dashboards" overview, Metabase Learn notebook, source 04cf5679 —
  "Set up automatic refresh intervals" is listed as a dashboard capability but
  is implemented client-side via URL fragment; cross-confirmed by Metabase
  Craft notebook query 2026-04-22 — no API key found in REST docs)

- `upsert_dashboard` defaults `cache_ttl=0` (no dashboard-level cache).
  Server-level caching stays ON (`MB_ENABLE_QUERY_CACHING=true`) so
  per-dashboard overrides work — live dashboards set `cache_ttl=0`;
  daily-cadence dashboards can set `cache_ttl=21600`.

- Every live-by-default dashboard carries a top-of-page "Data freshness"
  tile sourced from `raw_<source>._ingested_at` so ingest-pipeline
  regressions are immediately visible. On the Speed-to-Lead dashboard this
  tile reads `timestamp_diff(current_timestamp(), max(_ingested_at), minute)`
  from `raw_ghl.conversations` (the highest-cadence NRT source).
  (source: "Dashboards" overview, Metabase Learn notebook, source 04cf5679 —
  make cards interactive; use text cards for context)

## When to deviate

**Cache the tile (non-zero `cache_ttl`) when:**
- The underlying SQL does heavy aggregation over a large window
  (> 10M rows scanned) AND the answer genuinely changes on daily cadence.
- A third-party tile (e.g., a public-share link to a non-authed audience)
  needs to absorb traffic without re-querying BQ each render.
- The underlying mart is built on a `table` materialization refreshed
  only in the nightly workflow (not in the 2-min builder).

**Accept slower-than-1-min cadence when:**
- The source vendor doesn't support sub-5-min polling (e.g., Fivetran
  free/standard tier).
- The API rate limit doesn't allow 1-min cadence without bursting 429s.
- The tile's SLA is inherently coarser than minute-level (monthly
  reporting, daily ops reviews).

Decisions to deviate are recorded in the relevant track's WORKLOG entry
with a one-line rationale. Override in calling code: `upsert_card(..., cache_ttl=3600)`.

## Upstream chain

- Track W (2026-04-22): GHL extractor → Cloud Run Jobs (1-min hot / 15-min cold)
- Track X (2026-04-22): Calendly extractor → Cloud Run Jobs (1-min)
- Track Y (2026-04-22): dbt incremental + 2-min Cloud Run builder + freshness-threshold retune
- Track Z (2026-04-22): THIS rule + Metabase defaults + freshness tile

## Related rules

- `.claude/rules/metabase.md` — full Metabase authoring conventions (5 rules + Lessons Learned)
- `.claude/rules/mart-naming.md` — dashboard-facing naming conventions
- `.claude/rules/ingest.md` — NRT ingestion patterns (Cloud Run Jobs + Scheduler)
- `.claude/rules/dbt-marts-rollups.md` — incremental strategy details

## Lessons learned

- **Dashboard auto-refresh is URL-fragment only on OSS.** `refresh_period`
  does not exist as a REST API field on Metabase OSS v0.60.1 (or any OSS
  version confirmed 2026-04-22 corpus query). Do not try to PUT this key —
  it will silently be ignored or error. Use `#refresh=60` on the share URL.
  (source: Metabase Craft + Metabase Learn notebooks, corpus query 2026-04-22)

- **`cache_ttl=0` IS respected on OSS v0.60.1.** Track D (2026-04-22)
  empirically confirmed that per-dashboard `cache_ttl` DOES persist on OSS
  v0.60.1, contradicting the Metabase Learn corpus note that called it
  "Pro/Enterprise only." Use `cache_ttl=0` explicitly rather than `null` —
  `0` is self-documenting ("live") whereas `null` is ambiguous ("server default").

(Populate further as live-by-default issues surface — cache poisoning, stale
 refreshes, Cloud Run cold starts, etc.)
