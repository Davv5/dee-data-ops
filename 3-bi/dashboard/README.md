# D-DEE Dashboard

Click-around BI dashboard product for D-DEE.

Architecture: **Cabinet shell, Kim simplicity**.

- Cabinet shell: Next.js, Tailwind, shadcn-compatible component layout, sidebar navigation, app feel.
- Kim simplicity: dashboard definitions + named query contracts instead of bespoke page logic.
- Data boundary: server-side named queries only. No browser-side BigQuery credentials and no raw SQL from the client.

## Current Scope

This scaffold now includes the first live read-only Speed-to-Lead slice:

- app shell
- sidebar
- D-DEE config seam
- Speed-to-Lead dashboard definition
- named-query contracts
- server-side BigQuery reads against the current project
- live KPI, trend, rep breakdown, and freshness state
- `/api/health`
- `/api/speed-to-lead`

Auth and deployment are intentionally deferred to later PRs.

## Development

```bash
npm install
npm run dev
```

The dashboard uses Google Application Default Credentials or the deployed
runtime service account. Keep BigQuery credentials server-side only.

Health route:

```bash
curl http://localhost:3000/api/health
curl http://localhost:3000/api/speed-to-lead
```

## Data Contracts

Speed-to-Lead v1 consumes existing bq-ingest report tables:

- `Marts.mrt_speed_to_lead_overall`
- `Marts.mrt_speed_to_lead_daily`
- `Marts.fct_speed_to_lead`
- `Marts.rpt_speed_to_lead_week`
- `Marts.rpt_rep_scorecard_week`

Read `../../docs/discovery/current-data-layer-truth-map.md` before changing dashboard data bindings.
