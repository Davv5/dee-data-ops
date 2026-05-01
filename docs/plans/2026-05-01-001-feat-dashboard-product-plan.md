---
title: Build a click-around BI dashboard product (Cabinet shell, Kim simplicity)
type: feat
status: active
date: 2026-05-01
related:
  - docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md (PARKED 2026-04-26; superseded by this plan)
  - docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md (PARKED 2026-04-26; superseded by this plan)
  - docs/plans/2026-04-30-mart-collapse-fct-sks-plan.md (active; feeds later dashboard chapters)
supersedes:
  - "BI direction = dabi" framing in `~/.claude/projects/-Users-david-Documents-data-ops/memory/project_bi_direction_dabi.md`
---

# Build a click-around BI dashboard product

## Thesis

David is building a **client-facing dashboard product**, not another BI experiment. D-DEE is the first tenant. The first shipped chapter is the retired Metabase v1.6 Speed-to-Lead Page 1, rebuilt as a polished authenticated web app.

The right architecture is:

> **Cabinet shell, Kim simplicity.**

Use Cabinet (`/Users/david/.cabinet/app/v0.3.4/`) for the product substrate: Next.js, Tailwind, shadcn/ui, sidebar app shell, login shape, API route conventions, and a polished interactive feel.

Use Joshua Kim's dashboard idea as an authoring discipline: dashboards should be source-controlled, query-backed, easy for agents to edit, and free of BI-vendor runtime drag.

Do **not** build a full generative-BI platform in v1. Ship the smallest durable product slice:

- `3-bi/dashboard/` as a Cabinet-style Next.js app
- Hardcoded D-DEE config
- Server-side named queries over BigQuery `Marts.*`
- Reusable KPI/chart/table cards
- Speed-to-Lead Page 1
- Freshness indicator
- Basic magic-link auth before client-facing production use

The app should feel like a real product. The implementation should stay boring.

## Why This Pivot

Kim's original direction is powerful because a dashboard can be "just HTML plus SQL." That makes iteration fast and agent-friendly.

But D-DEE does not need a pile of generated HTML files. They need a trusted product surface: sidebar navigation, authentication, clear metrics, freshness, and something David can sell again to the next coaching or agency client.

Cabinet supplies the missing product shell. Kim supplies the simplicity constraint. The dashboard product should borrow both and fully become neither.

## Requirements

- **R1.** Single-tenant web-hosted dashboard app for D-DEE.
- **R2.** Cabinet-style sidebar with at least two v1 sections: Speed-to-Lead and Rep Breakdown.
- **R3.** KPI cards, charts, and tables render from BigQuery `Marts.*`.
- **R4.** BigQuery credentials stay server-side.
- **R5.** Browser never submits raw SQL; it calls named queries only.
- **R6.** Speed-to-Lead headline metric matches the retired Metabase v1.6 baseline for equivalent windows.
- **R7.** Freshness is visible on every dashboard page.
- **R8.** Auth gates production/client access.
- **R9.** D-DEE-specific values live in one config seam for a later client #2 refactor.

## Scope

### Build in v1

- Next.js app in `3-bi/dashboard/`
- D-DEE tenant config
- Sidebar/app shell
- Named-query API route
- KPI, chart, table, and freshness components
- Speed-to-Lead Page 1
- Basic magic-link auth
- Cloud Run or existing GCE/Caddy deploy path, chosen at implementation time

### Do Not Build in v1

- Chat UI
- Generative-BI runtime
- Browser-side BigQuery
- Arbitrary SQL endpoint
- Full multi-tenant system
- Electron app
- Dashboard-builder UI
- Separate query proxy unless Next.js API routes prove insufficient
- Refresh daemon unless simple cache/freshness handling fails
- New dbt marts for the first release

## Carry Forward vs. Drop

| Source | Keep | Drop |
|---|---|---|
| Cabinet | Next.js scaffold, app shell, sidebar feel, shadcn/ui, login shape, API conventions | Electron, terminal, Tiptap/editor, agents, file tree, search, mission-control concepts |
| Kim/dabi | Source-controlled dashboards, named queries, agent-editable definitions, no BI runtime | Standalone HTML as product, browser OAuth, generative-BI/chat framing, separate platform build |
| Metabase v1.6 | Card inventory, metric parity target, freshness lessons | Metabase runtime, REST authoring flow, public URL pattern as product surface |

## Key Decisions

1. **Cabinet shell, not Cabinet fork.** Copy patterns, not the whole app.
2. **Kim simplicity, not dabi platform.** A tile should be added by editing a dashboard definition and a named query, not by hand-building a bespoke page.
3. **Next.js API routes first.** `/api/query/[name]` is enough for v1. A standalone query proxy is deferred.
4. **Named queries only.** Query names map to static SQL and validated params.
5. **Dashboard definitions are source of truth.** `src/lib/dashboards/speed-to-lead.ts` should describe tiles, query names, fields, formats, and chart/table bindings.
6. **Freshness first, daemon later.** Show `mart_refreshed_at`; add a daemon only if cache invalidation becomes a real problem.
7. **Hardcoded D-DEE tenant.** Client #2 triggers multi-tenant routing.
8. **Metric parity, not pixel parity.** Metabase is the numerical/reference surface, not a visual clone target.

## Output Shape

```text
3-bi/dashboard/
├── README.md
├── package.json
├── next.config.ts
├── tsconfig.json
├── tailwind.config.ts
├── postcss.config.mjs
├── eslint.config.mjs
├── components.json
├── .env.example
├── public/
├── src/
│   ├── app/
│   │   ├── api/
│   │   │   ├── health/route.ts
│   │   │   ├── freshness/route.ts
│   │   │   ├── query/[name]/route.ts
│   │   │   └── auth/{magic-link,verify}/route.ts
│   │   ├── login/page.tsx
│   │   ├── speed-to-lead/page.tsx
│   │   ├── layout.tsx
│   │   └── page.tsx
│   ├── components/
│   │   ├── dashboard/{KpiCard,ChartCard,TableCard,FreshnessBadge,DashboardRenderer}.tsx
│   │   ├── layout/{AppShell,Sidebar}.tsx
│   │   └── ui/
│   ├── lib/
│   │   ├── auth/{magic-link,session}.ts
│   │   ├── bigquery/{client,named-queries,cache}.ts
│   │   ├── config/dee.ts
│   │   ├── dashboards/speed-to-lead.ts
│   │   ├── sections.ts
│   │   └── utils.ts
│   ├── middleware.ts
│   └── types/
└── tests/
    ├── unit/
    └── integration/
```

## Dashboard Definition Pattern

The dashboard should be mostly declarative:

```ts
export const speedToLeadDashboard = {
  slug: "speed-to-lead",
  title: "Speed-to-Lead",
  sections: [
    {
      title: "Overview",
      tiles: [
        {
          type: "kpi",
          title: "Within 5 minutes",
          query: "speed_to_lead_overall",
          field: "pct_within_5m",
          format: "percent",
        },
        {
          type: "line",
          title: "Daily trend",
          query: "speed_to_lead_daily",
          x: "report_date",
          y: "pct_within_5m",
        },
      ],
    },
  ],
} as const;
```

This is where the Kim lesson lives. A future agent should be able to add a tile by adding:

- one named query
- one tile definition
- optionally one test

If every tile becomes custom JSX, the plan has drifted.

## Data Boundary

```text
Browser
  -> Next.js middleware checks session cookie
  -> /api/query/[name] validates query name and params
  -> named query registry resolves static SQL
  -> BigQuery client runs server-side query
  -> JSON rows return to cards
```

Rules:

- No client-side BigQuery credentials.
- No raw SQL from the browser.
- Unknown query names return 400.
- Invalid params return 400.
- BigQuery errors are logged server-side and shown as card-level failures.
- `mart_refreshed_at` is returned with query responses when available.

## Implementation Units

- [ ] **U1. Lean Cabinet scaffold**

Create `3-bi/dashboard/` with a working Next.js app, Cabinet-inspired config, shadcn primitives, app shell, sidebar placeholder, and `/api/health`.

Use Cabinet configs as references. Prune dependencies aggressively. Do not copy Electron, terminal, editor, agent, file tree, or search code.

Verification:
- `npm install`
- `npm run dev`
- `/api/health` returns `{"ok":true}`
- `npm run build` succeeds

- [ ] **U2. D-DEE config and dashboard definition**

Create:

- `src/lib/config/dee.ts`
- `src/lib/sections.ts`
- `src/lib/dashboards/speed-to-lead.ts`

Read `docs/_archive/client_v1_scope_speed_to_lead.md` and map the retired Metabase Page 1 cards into the first dashboard definition. Keep uncertain cards as explicit TODOs rather than inventing metrics.

Verification:
- TypeScript validates required config fields.
- Every tile references a known query name placeholder.

- [ ] **U3. Named-query BigQuery layer**

Create:

- `src/lib/bigquery/client.ts`
- `src/lib/bigquery/named-queries.ts`
- `src/lib/bigquery/cache.ts`
- `src/app/api/query/[name]/route.ts`
- `src/app/api/freshness/route.ts`

Initial query candidates:

- `speed_to_lead_overall`
- `speed_to_lead_daily`
- `speed_to_lead_by_rep`
- `speed_to_lead_by_source`
- `rep_scorecard_week`
- `freshness`

Use ADC locally and service identity/WIF in production. Start with simple TTL cache plus freshness metadata.

Verification:
- Known query returns rows.
- Unknown query returns 400.
- Invalid params return 400.
- Response includes `rows`, `mart_refreshed_at`, and `cache_hit`.

- [ ] **U4. Dashboard rendering primitives**

Create reusable cards and renderer:

- `KpiCard`
- `ChartCard`
- `TableCard`
- `FreshnessBadge`
- `DashboardRenderer`

Cards handle loading, empty, and error states. `DashboardRenderer` consumes the dashboard definition.

Verification:
- One KPI, one chart, and one table render from mock or live data.
- Empty data renders cleanly.
- One failed query does not break the whole page.

- [ ] **U5. Speed-to-Lead Page 1**

Create `src/app/speed-to-lead/page.tsx` and complete the query/definition bindings.

Use:

- `docs/_archive/client_v1_scope_speed_to_lead.md` for card inventory
- `CLAUDE.local.md` for locked metric definition
- current `Marts.*` tables for actual data

Do not chase pixel parity with Metabase. Chase numerical and semantic parity.

Verification:
- All in-scope cards load.
- Headline `pct_within_5m` matches the retired Metabase baseline for the same window.
- Freshness is visible.
- Desktop and mobile layouts are usable.

- [ ] **U6. Basic magic-link auth**

Create:

- `src/app/login/page.tsx`
- `src/app/api/auth/magic-link/route.ts`
- `src/app/api/auth/verify/route.ts`
- `src/lib/auth/magic-link.ts`
- `src/lib/auth/session.ts`
- `src/middleware.ts`

Allowed users live in D-DEE config for v1. Local/dev mode may log magic links instead of sending email. Production provider is chosen in this unit: Resend, SendGrid, Postmark, or SMTP.

Verification:
- Allowed user can request link and access dashboard.
- Unknown user gets generic response but no access.
- Expired or malformed token fails cleanly.
- Unauthenticated dashboard request redirects to `/login`.

- [ ] **U7. Deploy single app**

Create deploy artifacts after the app is useful:

- `Dockerfile`
- `.dockerignore`
- `deploy/README.md`
- `.github/workflows/dashboard-deploy.yml` if CI deploy is in scope

Prefer Cloud Run unless the existing GCE/Caddy route is materially faster. Use server-side identity for BigQuery.

Verification:
- Public `/api/health` returns 200.
- Login works.
- David can view Speed-to-Lead.
- Queries run with server-side credentials.

- [ ] **U8. Pivot-discipline cleanup**

Close the strategic debt:

- Mark the two parked Kim/dabi plans as superseded by this plan.
- Update `CLAUDE.local.md` to name the dashboard product as the current BI direction.
- Update or replace `project_bi_direction_dabi.md`.
- Update `.claude/state/project-state.md`.
- Grep `.claude/rules/*.md` for active stale `dabi` references.

Verification:
- Future agents no longer see dabi as the current direction.
- Historical docs still preserve the story.

## Suggested PR Slices

**PR-1: Decision + Scaffold**

- U1
- U2 skeleton
- U8 cleanup

Exit: app boots locally and future sessions know the direction.

**PR-2: Data Boundary + Renderer**

- U3
- U4

Exit: dashboard definitions render through generic cards.

**PR-3: Speed-to-Lead v1**

- U5

Exit: first useful dashboard chapter works locally and passes metric parity.

**PR-4: Auth + Deploy**

- U6
- U7

Exit: authenticated public URL is ready for preview.

## Risks

| Risk | Mitigation |
|---|---|
| The plan expands back into a platform | Defer daemon, proxy, multi-tenant, dashboard builder |
| Cabinet copy brings too much weight | Copy configs and patterns only |
| Definitions become over-abstracted | Abstract only repeated card/rendering structure |
| Metabase parity is murky | Gate headline metric and card meaning, not exact pixels |
| Magic-link setup delays preview | Local/dev logs links; production email waits until deploy slice |
| Client #2 arrives early | D-DEE config is isolated for bounded refactor |

## Open Questions

- Which exact Metabase v1.6 cards still belong in Speed-to-Lead Page 1?
- Should Rep Breakdown be its own route or a section on the Speed-to-Lead page?
- Which email provider should magic-link use?
- Is Cloud Run definitely the first deploy target, or is the old GCE/Caddy path faster?
- Should longer SQL live in `.sql` files instead of `named-queries.ts`? Default: start inline, split when queries get long.

## Done Definition

The plan is shipped when:

- `3-bi/dashboard/` is a working app.
- David can authenticate and view Speed-to-Lead at a public URL.
- No browser-side BigQuery credentials exist.
- The headline Speed-to-Lead metric matches the locked definition.
- Freshness is visible.
- Future agents know the current BI direction is the click-around dashboard product, not dabi.

## Sources

- Cabinet reference: `/Users/david/.cabinet/app/v0.3.4/`
- Original v1 scope: `docs/_archive/client_v1_scope_speed_to_lead.md`
- Locked metric: `CLAUDE.local.md`
- Parked Kim/dabi plans:
  - `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md`
  - `docs/plans/2026-04-24-002-feat-generative-bi-platform-plan.md`
- Pivot discipline: `.claude/rules/pivot-discipline.md`
- Mart naming: `.claude/rules/mart-naming.md`
- Freshness conventions: `.claude/rules/live-by-default.md`
- Operator loop: `docs/runbooks/operator-fast-loop.md`
