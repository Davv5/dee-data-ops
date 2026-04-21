# Looker Studio dashboards — D-DEE v1

This directory holds the **dashboard-as-code compromise** for the Looker
Studio layer. The GUI render lives in Google's cloud; the things that
define what the render should be — the SQL, the tile specs, the theme —
live here, version-controlled, reusable for client #2.

Per the DataOps methodology (corpus-confirmed):

> *"Looker Studio wins because it is free, zero hosting, [and uses]
> Google-native auth that the client likely already has. Rather than
> managing per-user OAuth, the methodology recommends using a shared
> service account for the BigQuery connection."*

> *"Embed the dashboard in a Notion page or just share the link?
> Default to bare link; add embed only if requested."*

## What lives here

| File | Purpose |
|---|---|
| `page-1-speed-to-lead.md` | Click-spec for Page 1: mechanical tile-by-tile build instructions. Follow top-to-bottom, no design decisions needed. |
| `page-2-funnel-attribution.md` | (pending) Page 2 spec — `lead_journey` grain |
| `page-3-revenue.md` | (pending) Page 3 spec — `revenue_detail` grain |
| `theme.md` | Shared brand + style settings applied to every page |

## What lives in `dbt/models/marts/rollups/`

Pre-aggregated mart views sized for Looker Studio's 3,000-row cap and
optimized for cache behavior. Each tile in the click-specs points at
exactly one rollup — no Looker blends, no calculated fields, no
business logic in the GUI.

Current rollups:

- `rollups/speed_to_lead/stl_headline_7d` — 1-row aggregate, headline scorecards
- `rollups/speed_to_lead/stl_headline_trend_daily` — 30-day daily trend for sparklines
- `rollups/speed_to_lead/stl_daily_volume_by_source` — stacked-area data
- `rollups/speed_to_lead/stl_sdr_leaderboard_30d` — primary results table
- `rollups/speed_to_lead/stl_attribution_quality_30d` — DQ mix
- `rollups/speed_to_lead/stl_lead_detail_recent` — drill-down table

## Reuse pattern for client #2

1. Fork the repo
2. Swap `dee-data-ops-prod` → `<new-client>-prod` in profiles
3. Run the rollups — same SQL, new data
4. In Looker Studio, **File → Make a copy** of the D-DEE report
5. Swap each data source connection to the new client's rollup tables
6. Re-theme (if brand palette differs) via `theme.md`

This is the "build once, clone forever" pattern Looker Studio supports
via its Reports API. ~15 min per client after the initial build.

## Never do

- **Calculated fields in Looker.** Push all business logic to the rollup SQL.
- **Blended data sources.** Pre-join in dbt; Looker just renders.
- **Direct-to-wide-mart connections.** Always connect to a rollup, never to `sales_activity_detail` / `lead_journey` / `revenue_detail` directly — they're too wide and defeat the cache.
- **Per-user OAuth for the BQ connection.** Use "owner's credentials" mode so viewers inherit David's access.
