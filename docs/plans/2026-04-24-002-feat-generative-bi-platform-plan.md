---
title: Build dabi — generative BI platform inspired by Joshua Kim
type: feat
status: parked
parked_on: 2026-04-26
reactivation_gate: |
  Reactivate ONLY when ALL of:
  1. Strategic Reset Sprint exits (`docs/plans/2026-05-xx-gold-layer-rebuild.md` published).
  2. Gold-layer roadmap names a dashboard or surface that requires the new BI direction.
  3. Plan A (`2026-04-24-001-feat-pivot-to-generative-bi-plan.md`) is reactivated — Plan B is the platform; Plan A consumes its outputs.
  Until all three hold, do not propose dabi platform work. The BI direction itself (dabi per Kim recipe) is reaffirmed (2026-04-26); only the build is gated.
date: 2026-04-24
origin: conversation 2026-04-24 (this session); [Joshua Kim, "Building Dashboards Without BI SaaS" — Medium, April 2026](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb)
related: docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md (Plan A — D-DEE pivot consumes Plan B's outputs); docs/plans/2026-04-24-strategic-reset.md (active Discovery Sprint — gates Plan B build units); docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md (Port plan, paused at U3-complete)
---

# Build dabi — a generative BI platform

## Fresh session startup

If you're opening this plan in a new Claude session:

1. Read `.claude/state/project-state.md` — confirm sprint status and whether the dabi pivot decision (Plan A U1) has landed.
2. Read `docs/plans/2026-04-24-strategic-reset.md` — sprint gates Plan B's build units.
3. Read `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md` (Plan A) — the engagement-level pivot Plan B feeds. Plan A's U2 is the first dabi-served dashboard (Speed-to-Lead) and depends on Plan B Phase 1 exit.
4. Read this plan in full — Phase 1 (U1-U5) ships "first light" (sample dashboard renders end-to-end, unauthed); Phase 2 (U6-U8) productionizes (auth + runtime + CI). U1 is sprint-compatible (docs-only); U2-U8 are build PRs and wait for sprint exit.

---

## Overview

**dabi** is a generative-BI platform inspired by Joshua Kim's recipe ("[Building Dashboards Without BI SaaS](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb)") and adapted for non-technical viewers (sales teams, not engineers). The pattern:

- A Claude Code Skill (`.claude/skills/dabi-author/`) takes a natural-language request ("add a tile that splits Speed-to-Lead by source, last 14 days") and emits a standalone HTML dashboard file plus a declared SQL query.
- The HTML calls a tiny Cloud Run query proxy that holds the BigQuery service-account, runs the named query, and returns JSON. Charts render via a vendored JS library (`dabi-core.js`).
- Magic-link auth gates dashboard access. The agency owns the user roster (no Google accounts in the GCP project for D-DEE staff).
- Hosted on the existing GCE VM behind the existing Caddy + ACME cert (same `*.nip.io` URL the public Metabase dashboard uses today).

This plan lives at `3-bi/dabi/` mirroring the existing `3-bi/metabase/` shape. It is the *buildable foundation* that Plan A (`docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md`) consumes to ship Speed-to-Lead off Metabase.

**Sprint reality.** The Strategic Reset Sprint is active and bans warehouse / mart / dashboard PRs until the Gold-layer roadmap exists. Plan B U1 (conventions doc + `.claude/rules/dabi.md`) is sprint-compatible (docs-only) and lands now. U2-U8 are build PRs and wait for sprint exit.

---

## Problem Frame

### Why a new BI surface at all

The Metabase v1.6 + planned Evidence.dev path was overruled in the 2026-04-24 conversation for three reasons (see Plan A Problem Frame for the full version): slow iteration on dashboard changes, no per-viewer slice, and the public-or-Metabase-account dichotomy. Joshua Kim's pattern solves all three by making Claude Code the dashboard author, plain-text HTML+SQL the source format, and the BI vendor's runtime entirely optional.

### Why a build, not a fork

Kim's published codebase is a personal project — `file://` hosting, browser-OAuth via `gcloud`, single viewer. The shape is right; the operational assumptions don't match a delivery context. Specifically:

- **Viewers are not engineers.** D-DEE SDRs / closers / managers don't have `gcloud`, don't have Google accounts in `project-41542e21-...`, and won't clone a git repo to view a dashboard. They want a URL, magic-link login, and tiles that update.
- **Public URLs need real auth, not "trust the URL."** Today's Metabase public URL leaks all team metrics to anyone with the link. A magic-link-auth front door is the minimum bar.
- **The proxy is a security + cost gate.** Browser-side BQ (Kim's pattern) means the browser runs arbitrary SQL; in our context that would be SQL-injectable to a hostile viewer and uncapped on cost. A proxy with declared queries is the right shape.

### Why this is a separate plan from Plan A

Plan A is engagement-shaped (D-DEE pivot, Metabase decommission, parity gate). Plan B is platform-shaped (the actual code: lib, proxy, Skill, auth, runtime). Bundling them would mix engagement decisions with platform engineering and make both harder to reason about. Plan B's outputs are reusable across other PS engagements; Plan A is D-DEE-specific.

---

## Requirements Trace

- R1. A Claude Code Skill at `.claude/skills/dabi-author/` accepts a natural-language description ("show Speed-to-Lead headline + breakdown by SDR for the last 14 days") and writes:
  - `3-bi/dabi/dashboards/<name>/<file>.html` (the dashboard page)
  - One or more entries in `3-bi/dabi/dashboards/<name>/queries.sql` (or per-tile `.sql` files; structure decided in U1)
  - One or more entries in `3-bi/dabi/proxy/queries/<name>.sql` (the declared-query allowlist on the proxy side)
  - Updates `3-bi/dabi/dashboards/_registry.js` with the new dashboard.
- R2. The Cloud Run query proxy accepts `POST /api/query/<query_name>` with a JSON body of params, validates against a per-query schema, runs the named SQL against BigQuery using a project-scoped service-account, returns JSON. The proxy NEVER accepts raw SQL strings from the browser.
- R3. The shared `dabi-core.js` library renders charts (line, bar, scorecard, table, pivot) from JSON returned by the proxy. Loading + error states; no React; no build step.
- R4. Magic-link auth: a user requests a link with their email, receives a signed token via SMTP, exchanges it for a session cookie. Session-bearing requests are routed by Caddy (or the proxy) to the dashboard; non-session requests get the login page. Roster lives in `3-bi/dabi/authoring/users.yaml`.
- R5. The platform deploys onto the existing GCE VM via the existing Caddy. New routes added: `/dashboards/*` (static), `/api/query/*` (proxy), `/auth/*` (magic-link service). Existing Metabase routes preserved through Plan A cutover.
- R6. Terraform manages the new GCP resources (Cloud Run service for proxy, Secret Manager entries for magic-link signing key + SMTP creds, IAM for the proxy SA). GH Actions deploys on merge to main.
- R7. The first sample dashboard (NOT Speed-to-Lead — picked in U5) renders end-to-end in Phase 1 with the loop: Skill → HTML → proxy → BigQuery → JSON → chart. Proves the platform.
- R8. `.claude/rules/dabi.md` governs all `3-bi/dabi/**` work; a future Claude session opening any dabi file gets the conventions in context. Cross-references: Plan A (engagement consumer), Plan B (this plan, the build), Kim (the source pattern).

---

## Scope Boundaries

- This plan does **not** ship Speed-to-Lead. Speed-to-Lead lands in Plan A U2 as the first real dashboard *consuming* Plan B's outputs. Plan B U5 ships a *different* sample dashboard to prove the loop.
- This plan does **not** decommission Metabase. Metabase decommission is Plan A U4.
- This plan does **not** rewrite any dbt model or mart. dabi reads `marts.*` exactly as Metabase does today.
- This plan does **not** define the next 5 dashboards beyond the sample. The Gold-layer roadmap (sprint output) drives that list; each future dashboard rides Plan B's platform via its own Plan-A-style integration.
- This plan does **not** introduce a new vendor. No Cloudflare, no Vercel, no Supabase, no Auth0. Everything runs on the existing GCE VM + GCP project.

### Deferred to Follow-Up Work

- **Pure-Kim local mode** for David's own internal exploration. Degraded variant of the same architecture: skip the proxy, use `gcloud auth print-access-token` from the browser, run from `file://`. Lives as a documented `dev/` mode in `3-bi/dabi/runtime/`. Picked up only if David wants the Kim-as-published workflow for his own non-shared dashboards.
- **Per-SDR filter injection.** The proxy injects a `WHERE assignee = :user_id` clause when the session belongs to a non-admin user. Lives in U6 follow-up; current scope ships shared views only.
- **Multi-tenant dabi.** Same platform serving multiple PS engagements. Folder layout already supports it (`3-bi/dabi/dashboards/<engagement>/<name>/`); auth roster currently single-tenant. Picked up when a second engagement adopts dabi.
- **Dashboard authoring UI for non-engineers.** A web form that calls the same Skill backend so David's clients can request tiles directly. Currently David is the only Skill operator.
- **Custom dbt materializations** following Kim's [BigQuery TVF article](https://joshua-data.medium.com/bigquery-tvf-custom-materialization-in-dbt-en-56b62e02ec7e) — added to NotebookLM corpus this session; codified into a rule only when a real need surfaces.

---

## Context & Research

### Relevant Code and Patterns

- `3-bi/metabase/runtime/Caddyfile` — the existing routing config. Plan B U7 extends it with `/dashboards/*`, `/api/query/*`, `/auth/*` blocks.
- `3-bi/metabase/runtime/docker-compose.yml` — the GCE VM stack. Plan B U7 adds containers for: dabi static (sidecar nginx or python `http.server`), magic-link service. Cloud Run runs the proxy out-of-VM.
- `3-bi/metabase/runtime/startup-script.sh` — the bootstrap that pulls compose + Caddy from the ops bucket and starts containers. Plan B U7 extends it for dabi assets.
- `3-bi/metabase/terraform/*.tf` — existing GCP infrastructure. Plan B U8 adds Cloud Run service + IAM + Secret Manager entries; reuses the existing static IP, ops bucket, BQ data-reader SA.
- `3-bi/metabase/authoring/dashboards/speed_to_lead.py` — the Python authoring script. Spec source for the queries Plan A U2 transcribes into dabi.
- `1-raw-landing/deploy/calendly-extractor/` + `ghl-extractor/` — Cloud Run job deploy patterns. Plan B U8's proxy deploy follows the same shape (Dockerfile + GH Actions deploy).
- `.claude/skills/ask-corpus/SKILL.md` — Skill format reference. Plan B U4's `dabi-author` Skill follows the same shape.
- `.claude/rules/metabase.md` — rule format reference. Plan B U1's `dabi.md` follows the same shape.

### Institutional Learnings

- `feedback_ship_over_ceremony.md` — direct execution preferred. Plan B units run in main session; no plan-architect → executor pipeline.
- `feedback_preserve_working_infra.md` — keeps the dabi build on the existing VM + Caddy + IP rather than spinning up new infra.
- `project_gcp_consolidation_decision.md` — confirms `project-41542e21-470f-4589-96d` as the GCP home; Plan B's terraform targets this project.
- The Strategic Reset Sprint plan defines the docs-only ban; Plan B U1 lands during the sprint, U2-U8 wait for exit.

### External References

- [Joshua Kim, "Building Dashboards Without BI SaaS"](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb) — the source pattern. Plan B's KTDs explicitly enumerate where it deviates.
- [Joshua Kim, "The Order in which I Model Data"](https://joshua-data.medium.com/my-analytics-engineering-process-en-435445038897) — modeling-process companion; integrated into dbt rules separately this session, NOT into Plan B (modeling vs. BI).
- [Observable Plot](https://observablehq.com/plot/) — the candidate JS chart library for `dabi-core.js`. Decision in U2.
- [Caddy v2 reverse_proxy + handle_path docs](https://caddyserver.com/docs/caddyfile/directives/handle) — the routing primitives U7 uses to add dabi routes alongside the Metabase block.

---

## Key Technical Decisions

1. **Adapted-Kim, not pure Kim.** Browser-OAuth via `gcloud` → Cloud Run query proxy with a service-account. `file://` hosting → static hosting on the existing GCE VM behind Caddy. *Rationale:* sales-team viewers don't have `gcloud` or Google accounts in the GCP project; URLs must be browser-bookmarkable; proxy is the security + cost gate.

2. **Declared queries, not arbitrary SQL.** The proxy accepts `{query_name, params}`, looks up the SQL in `3-bi/dabi/proxy/queries/<name>.sql`, validates params against a per-query schema (`<name>.schema.json`), and runs only that exact statement. The browser never sends raw SQL. *Rationale:* kills SQL-injection surface; caps BQ cost per query (per-query rate limit + max-rows enforced server-side); enables central observability of who ran which query when.

3. **Reuse existing GCE VM + Caddy + static IP + ACME cert.** No Cloudflare, no Vercel, no new domain. *Rationale:* domain stability through Plan A cutover; zero new vendor onboarding; terraform mostly already exists; the cert + Caddy state survives the swap.

4. **Magic-link auth on day one.** dabi ships authed at first cutover. No public-then-add-auth sequence. *Rationale:* avoids a second cutover; matches the deferred-work line in CLAUDE.local.md ("Retirement of the public dashboard URL once SDR/Manager accounts come online"); the agency owns the user list.

5. **`3-bi/dabi/` mirrors `3-bi/metabase/` folder shape.** Same three top-level directories: `authoring/` (Skill output land here, plus `users.yaml`), `runtime/` (Caddy block + sidecar containers if any), `terraform/` (GCP resources). Plus a fourth: `proxy/` (Cloud Run source + queries allowlist). *Rationale:* pattern reuse; David is the operator and already navigates the Metabase shape.

6. **Skill output is constrained to four file types.** The `dabi-author` Skill writes:
   - `dashboards/<name>/<file>.html` (the page)
   - `dashboards/<name>/queries.sql` (or per-tile files; structure in U1)
   - `proxy/queries/<query_name>.sql` (the declared-query allowlist entry)
   - `proxy/queries/<query_name>.schema.json` (params validation)
   - One entry in `dashboards/_registry.js`
   *Rationale:* predictable surface area; enforces declared-query discipline (no Skill-emitted page can hit a query that isn't on the proxy allowlist); easier review (every Skill PR has the same 5-file shape).

7. **Charts via Observable Plot.** Vendored as `dabi-core/vendor/plot.js`; no npm; no build step. `dabi-core.js` exposes a thin façade so future swaps to a different library are local. *Rationale:* minimal JS; SVG out-of-the-box (sharp on retina, accessible, copy-pastable); designed for declarative tile-shaped charts; tiny footprint vs. Recharts/Chart.js+React/d3-from-scratch.

8. **Static dashboards served from a sidecar nginx on the GCE VM.** Same Caddy reverse-proxies `/dashboards/*` to the nginx container. *Rationale:* simplest delivery; no GCS+CDN stack needed for D-DEE volumes; reuses existing VM + cert; static files deploy via the same ops-bucket pattern Caddy already uses.

---

## Open Questions

### Resolved During Planning

- *Browser OAuth vs Cloud Run proxy?* Proxy. Per KTD 1, Kim's browser-OAuth assumes the viewer is the engineer.
- *Cloudflare/Vercel vs existing GCE VM?* GCE VM. Per KTD 3, domain stability through cutover and zero new vendor cost more than the marginal CDN benefit at D-DEE volumes.
- *Magic-link from day one vs public-then-add-auth?* Day one. Per KTD 4, second cutover is worse than slightly more upfront work.
- *Same folder shape as Metabase or new?* Same. Per KTD 5, pattern reuse beats novelty.
- *Which chart library?* Observable Plot. Per KTD 7. Deferrable to U2 if a deal-breaker surfaces.
- *Does Plan B unblock during the Strategic Reset Sprint?* U1 only (docs). U2-U8 wait for sprint exit per the same logic Plan A uses.

### Deferred to Implementation

- *SMTP provider for magic-link delivery.* Default candidates: SendGrid free tier, Mailgun free tier, Cloud-hosted SMTP. Decided in U6.
- *Magic-link token TTL.* Default 30 minutes for the link itself, 7 days for the session cookie. Decided in U6 with David.
- *Proxy auth model.* Two options: (a) Caddy enforces session cookie before forwarding to proxy, or (b) Proxy validates session cookie itself. Default (a) — Caddy is the auth boundary, proxy assumes "if I see this request, the session was already validated." Decided in U7.
- *Per-tile SQL files vs single `queries.sql` per dashboard.* Default per-tile (`<dashboard>/queries/<tile>.sql`) for grep-ability. Decided in U1 with the convention doc.
- *Whether `dabi-core.js` is one file or modular.* Default single-file with named exports for simplicity; if it grows past ~500 lines, split. Decided in U2.
- *How U7 handles the `/legacy-metabase` path during Plan A's 30-day rollback window.* Default: Caddy `handle_path /legacy-metabase/*` → metabase container, identical to today's `/` behavior with a path strip. Decided in U7.

---

## Output Structure

```
3-bi/dabi/
├── README.md                          # NEW — U1
├── authoring/
│   ├── README.md                      # NEW — U1
│   └── users.yaml                     # NEW — U6 (initial roster)
├── dashboards/                        # NEW — U4 / U5 produce content here
│   ├── _registry.js                   # NEW — U2 (lib bootstraps from this)
│   ├── _shared/
│   │   └── theme.css                  # NEW — U2
│   └── <sample>/                      # NEW — U5 (first sample dashboard)
│       ├── index.html
│       └── queries/
│           └── <tile>.sql
├── proxy/                             # NEW — U3 (Cloud Run source)
│   ├── Dockerfile
│   ├── main.py                        # FastAPI / Flask app
│   ├── requirements.txt
│   ├── README.md
│   └── queries/                       # the declared-query allowlist
│       └── <query_name>.sql
│       └── <query_name>.schema.json
├── auth/                              # NEW — U6 (magic-link service)
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── README.md
├── lib/                               # NEW — U2 (dabi-core.js)
│   ├── dabi-core.js
│   ├── dabi-core.test.html
│   └── vendor/
│       └── plot.js                    # vendored Observable Plot
├── runtime/                           # NEW — U7
│   ├── Caddyfile.dabi-blocks          # to be appended into the existing Caddyfile
│   ├── docker-compose.dabi.yml        # nginx static + auth service
│   └── nginx.conf                     # sidecar nginx config
└── terraform/                         # NEW — U8
    ├── cloud_run_proxy.tf
    ├── secrets.tf
    ├── iam.tf
    └── README.md

.claude/
├── rules/
│   └── dabi.md                        # NEW — U1
└── skills/
    └── dabi-author/
        ├── SKILL.md                   # NEW — U4
        └── references/
            └── dashboard-template.html # NEW — U4 (HTML scaffold)
            └── query-template.sql      # NEW — U4

.github/workflows/
└── dabi-deploy.yml                    # NEW — U8
```

The structure is greenfield except for the additions to `.claude/rules/`, `.claude/skills/`, `.github/workflows/`, and the future appends to `3-bi/metabase/runtime/Caddyfile` (handled in U7 as a Caddyfile reorg, not a separate file).

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```
                    ┌───────────────────────────────────┐
                    │  GCE VM (existing)                │
                    │  static IP + Caddy + ACME cert    │
                    │                                   │
SDR/Closer/Manager  │   Caddy routes:                   │
   │                │     /                 → nginx     │
   │  HTTPS         │     /dashboards/*     → nginx     │
   ▼                │     /api/query/*      → Cloud Run │
*.nip.io ───────────┼──►  /auth/*           → auth svc  │
                    │     /legacy-metabase  → metabase  │ (Plan A 30-day rollback)
                    │                                   │
                    │   Sidecar containers:             │
                    │     nginx          (static HTML)  │
                    │     metabase       (during A2/A3) │
                    │     auth-service   (magic-link)   │
                    │     cloud-sql-proxy (auth DB)     │
                    └───────────┬───────────────────────┘
                                │
                  ┌─────────────┴──────────────┐
                  │  Cloud SQL Postgres        │
                  │  - magic_link_tokens       │
                  │  - sessions                │
                  │  - users (mirror of yaml)  │
                  └────────────────────────────┘

                                ▲
                                │ session cookie set on /auth/exchange
                                │
              ┌─────────────────┴───────────────────┐
              │  HTML dashboard in browser          │
              │  ┌──────────────────────────────┐   │
              │  │ <script src="/lib/dabi-core" │   │
              │  │ dabi.tile('lead-vol-30d', { │   │
              │  │   chart: 'bar', x: 'src', y:│   │
              │  │   'count' });                │   │
              │  └────────────┬─────────────────┘   │
              │               │                     │
              │  POST /api/query/lead-vol-30d       │
              │       {days: 30}                    │
              └───────────────┬─────────────────────┘
                              │
                              ▼
                   ┌──────────────────────────┐
                   │  Cloud Run proxy         │
                   │  ┌────────────────────┐  │
                   │  │ resolve query_name │  │
                   │  │ → proxy/queries/…  │  │
                   │  │ validate params    │  │
                   │  │ enforce rate limit │  │
                   │  │ run on BQ          │  │
                   │  │ return JSON        │  │
                   │  └────────────────────┘  │
                   └──────────┬───────────────┘
                              │
                              ▼
                   ┌──────────────────────────┐
                   │  BigQuery                │
                   │  project-41542e21-…      │
                   │  marts.* (read by SA)    │
                   └──────────────────────────┘

  Authoring loop (offline, Claude Code):

   David: "add a Speed-to-Lead split by source for last 14 days"
       │
       ▼
   .claude/skills/dabi-author/SKILL.md  ──►  emits 5 files:
       │                                          - dashboards/<name>/<file>.html (or appends a tile)
       │                                          - proxy/queries/<query_name>.sql
       │                                          - proxy/queries/<query_name>.schema.json
       │                                          - dashboards/_registry.js (entry)
       │                                          - dashboards/<name>/queries/<tile>.sql (mirror, for grep)
       ▼
   git commit + push  ──►  GH Actions: deploy proxy + sync nginx static
                                          to ops bucket → VM picks up on next reload
```

The authoring loop produces git-tracked artifacts. The runtime loop is what viewers interact with. The proxy is the only component with BQ creds; the browser never holds them.

---

## Implementation Units

- [ ] U1. **Conventions doc + `.claude/rules/dabi.md` (sprint-compatible, docs-only)**

**Goal:** Document the dabi architecture, the folder shape, the Skill output contract, the security posture, and the per-tile SQL convention. Without this, U2-U8 spawn architectural drift.

**Requirements:** R8

**Dependencies:** None. Lands during the Strategic Reset Sprint (docs-only).

**Files:**
- Create: `3-bi/dabi/README.md` (top-level — what is dabi, when to use it, when not to)
- Create: `.claude/rules/dabi.md` (paths: `["3-bi/dabi/**", ".claude/skills/dabi-author/**"]`) — the conventions agents auto-load
- Create: `3-bi/dabi/authoring/README.md` (Skill operator's guide; user-roster format)
- Create: `3-bi/dabi/proxy/README.md` (proxy contract; declared-query format; schema.json shape)
- Create: `3-bi/dabi/lib/README.md` (chart API; tile() function signature; theme conventions)

**Approach:**
- Document the 8 KTDs (above) verbatim in `3-bi/dabi/README.md` so the rationale travels with the system.
- The `.claude/rules/dabi.md` rule should be tight — focus on the Skill output contract (the 5-file shape), the declared-query rule (no raw SQL), the file-path conventions, and the no-build-step constraint (Observable Plot vendored, no npm). Cross-link to the 4 README files for detail.
- The per-tile SQL convention: each tile gets one SQL file at `dashboards/<name>/queries/<tile>.sql` AND that file is mirrored to `proxy/queries/<query_name>.sql`. Grep-ability matters; one source of truth could come back to bite, so explicit mirror with a CI check that they match (proposed in U8).

**Patterns to follow:**
- `.claude/rules/metabase.md` for the rule shape
- `3-bi/metabase/README.md` for the top-level README shape
- `.claude/skills/ask-corpus/SKILL.md` for skill format (this informs U4 prep)

**Test scenarios:**
- *Sprint check:* `git diff` after the unit lands touches only `3-bi/dabi/**` (new files) and `.claude/rules/dabi.md`. No `2-dbt/`, no `1-raw-landing/`, no other rule modifications.
- *Conventions check:* a fresh Claude session that opens `3-bi/dabi/README.md` and reads `.claude/rules/dabi.md` can answer "where does a Skill-emitted query file land?" without ambiguity.
- *Observable contract:* the rule explicitly lists the 5-file Skill output shape; U4 implementation must produce all 5.

**Verification:**
- 5 files created.
- Rule loads on `3-bi/dabi/**` per its frontmatter.
- A WORKLOG entry references the conventions landing.

---

- [ ] U2. **`dabi-core` JS library — chart components, query call, registry loader**

**Goal:** The shared client library every dashboard imports. Provides: a `dabi.tile()` API for declaring charts, a `dabi.query()` function that POSTs to the proxy, a registry loader that reads `_registry.js`, loading + error states, and a theme.

**Requirements:** R3

**Dependencies:** U1 (conventions doc), Strategic Reset Sprint exit.

**Files:**
- Create: `3-bi/dabi/lib/dabi-core.js` (~400 lines target)
- Create: `3-bi/dabi/lib/vendor/plot.js` (vendored Observable Plot, ~600KB UMD)
- Create: `3-bi/dabi/lib/dabi-core.test.html` (manual test harness — open in browser, exercises every chart type against a mock proxy)
- Create: `3-bi/dabi/dashboards/_registry.js` (initially empty array; U5 adds the first entry)
- Create: `3-bi/dabi/dashboards/_shared/theme.css` (typography, color tokens, tile spacing)

**Approach:**
- API surface (intentionally tiny):
  ```js
  dabi.tile(elementId, {
    query: 'lead-vol-30d',         // proxy query_name
    params: { days: 30 },           // POSTed as JSON body
    chart: 'bar',                   // bar | line | scorecard | table | pivot
    x: 'source', y: 'count',        // dimension/measure mappings
    title: 'Lead volume by source, last 30 days'
  });
  ```
- `dabi.query(name, params)` returns a Promise<{ rows, schema }>. Used internally by `tile()`; exposed for advanced cases.
- Loading state: render a placeholder skeleton for each tile while the query is in flight. Error state: render the error message inline (with the query_name so debug is fast).
- Vendor Observable Plot, do not load it from CDN (deterministic, offline-buildable, no third-party drift).
- Theme tokens in `_shared/theme.css` — sales-team-friendly defaults (dark text on light bg, larger numbers, big-headline scorecard tiles).

**Execution note:** Test-first via the manual `dabi-core.test.html` harness. Mock the proxy with a static JSON file in `lib/vendor/test-fixtures/`; verify every chart type renders before any real proxy work.

**Patterns to follow:**
- Single-file module pattern (no build step; modern browsers parse ES2020+)
- Observable Plot's existing API (don't reinvent chart wrappers)

**Test scenarios:**
- *Happy path:* `dabi-core.test.html` renders all 5 chart types from mock fixtures.
- *Edge case:* an empty result set renders the empty-state placeholder, not a JS error.
- *Edge case:* a malformed proxy response (missing `rows`) renders the error tile, not a stack trace.
- *Edge case:* a tile referencing a query_name not in `_registry.js` renders an error tile naming the missing query.
- *Performance:* a dashboard with 10 tiles loads with < 1 second of perceived delay on the test harness (mock proxy returning instantly).
- *Accessibility:* every chart type renders with `<title>` element so screen readers describe it.

**Verification:**
- All chart types exercised in the test harness.
- File sizes documented in `lib/README.md` (gzipped delivery cost).

---

- [ ] U3. **Cloud Run query proxy — declared-queries allowlist, params validation, BQ access**

**Goal:** A small Python service (FastAPI) that holds the BQ service-account, exposes `POST /api/query/<query_name>`, validates params, runs the named SQL on BQ, returns JSON. Deployed to Cloud Run in `project-41542e21-...`.

**Requirements:** R2

**Dependencies:** U1 (proxy contract documented), Strategic Reset Sprint exit.

**Files:**
- Create: `3-bi/dabi/proxy/main.py` (FastAPI app; ~300 lines)
- Create: `3-bi/dabi/proxy/Dockerfile`
- Create: `3-bi/dabi/proxy/requirements.txt` (fastapi, uvicorn, google-cloud-bigquery, jsonschema, python-jose for token verify in U6)
- Create: `3-bi/dabi/proxy/README.md`
- Create: `3-bi/dabi/proxy/queries/.gitkeep` (empty allowlist; U5/Plan A populates)
- Create: `3-bi/dabi/proxy/test_main.py` (pytest, exercises every endpoint with a mock BQ client)

**Approach:**
- **Declared-query lookup.** On `POST /api/query/<query_name>`:
  1. Resolve `proxy/queries/<query_name>.sql` (404 if missing).
  2. Resolve `proxy/queries/<query_name>.schema.json` (treat as `{}` if missing — no params accepted).
  3. Validate request body against the schema (jsonschema).
  4. Render the SQL with params (use BQ's parameterized queries, NOT string interpolation).
  5. Run on BQ with a 30-second job timeout, max 100k rows, max 10MB result.
  6. Return `{ rows: [...], schema: [{ name, type }, ...], stats: { ms, bytes } }`.
- **Per-query rate limit.** In-memory token bucket per (query_name, session_id) — 10 requests / minute default, configurable per query in `<query_name>.config.yaml`.
- **Auth.** In Phase 1 (U3 standalone), accept all requests. In Phase 2 (U6 + U7), require a valid session cookie (validated by Caddy upstream OR re-validated here — decided in U7).
- **Service account.** Cloud Run service runs as `dabi-proxy@project-41542e21-...`, granted `roles/bigquery.dataViewer` on the `marts.*` schema only (not on `staging.*` or `warehouse.*` — query allowlist is the gate, but defense-in-depth).
- **Structured logs.** JSON to stdout (Cloud Run picks up). Every query run logs query_name, params hash (NOT raw params — PII), session_id (when auth lands), duration_ms, bytes_billed.

**Execution note:** Test-first. `test_main.py` covers the declared-query lookup, params validation failure, params validation pass, BQ result shaping, rate-limit enforcement, and the auth-required path (mocked).

**Patterns to follow:**
- `1-raw-landing/ghl/` for the Cloud Run + Dockerfile shape
- Existing GH Actions deploy workflow patterns

**Test scenarios:**
- *Happy path:* `POST /api/query/sample-query {param: 'foo'}` returns `{rows: [...], schema: [...]}`.
- *Error path — unknown query_name:* 404 with `{error: "query 'unknown' not in allowlist"}`.
- *Error path — params validation fail:* 400 with the jsonschema error message.
- *Error path — BQ permission denied:* 502 with structured error (the proxy SA is not granted the underlying table; surface clearly so debugging is one log-line).
- *Error path — BQ timeout:* 504 with `{error: "query exceeded 30s timeout"}`.
- *Rate-limit exceeded:* 429 after 10 hits in 60s for the same (query, session).
- *Integration:* end-to-end against a real BQ table (the U5 sample query) — proxy returns expected rows.
- *Security:* a request with a SQL-injection attempt in a string param doesn't escape; query runs as parameterized SQL.

**Verification:**
- Local: proxy runs, `test_main.py` green, manual curl roundtrip works.
- Deployed: Cloud Run service responds; auth-less Phase 1 endpoint reachable from the GCE VM (via VPC connector OR public-with-IP-allowlist — decided in U8).

---

- [ ] U4. **`dabi-author` Claude Code Skill — generates HTML + queries + registry**

**Goal:** A Claude Code Skill at `.claude/skills/dabi-author/` that takes a natural-language description and emits the 5-file output shape from KTD 6.

**Requirements:** R1

**Dependencies:** U1 (conventions), U2 (lib API stable), U3 (proxy contract stable). Strategic Reset Sprint exit.

**Files:**
- Create: `.claude/skills/dabi-author/SKILL.md` (the skill definition + instructions)
- Create: `.claude/skills/dabi-author/references/dashboard-template.html` (HTML scaffold)
- Create: `.claude/skills/dabi-author/references/query-template.sql` (SQL scaffold with placeholders)
- Create: `.claude/skills/dabi-author/references/schema-template.json` (params schema scaffold)
- Create: `.claude/skills/dabi-author/references/conventions.md` (mirrors `.claude/rules/dabi.md`'s Skill-output contract for in-skill self-reference)

**Approach:**
- Skill input: natural-language task ("add a tile to Speed-to-Lead that splits by source for the last 14 days") OR a structured spec ("dashboard: speed_to_lead, tile: split-by-source, query: marts.speed_to_lead_detail GROUP BY source LIMIT 14d, chart: bar").
- Skill workflow:
  1. Parse the request — identify dashboard name (new or existing), tile name(s), query intent, chart type.
  2. If the dashboard exists, read `dashboards/<name>/index.html` and inject the tile div + script call. If new, create from `dashboard-template.html`.
  3. Generate the SQL from `query-template.sql` against `marts.*` (the Skill must read the relevant `_<dir>__docs.md` from `2-dbt/models/marts/` to know the schema).
  4. Generate the schema.json from the params extracted from the SQL.
  5. Write files to both `dashboards/<name>/queries/<tile>.sql` AND `proxy/queries/<query_name>.sql` (mirror per U1 convention).
  6. Update `_registry.js` with the new dashboard / tile.
  7. Output a one-line PR description summarizing what was added.
- The Skill MUST cite the dbt mart-naming rule when picking dashboard names (business-friendly, not `fct_*`). The Skill MUST NOT invent SQL against `staging.*` or `warehouse.*` directly — only `marts.*`.
- The Skill should also support edit operations ("change the chart on the X tile from bar to line") via the same authoring loop.

**Execution note:** This is the interaction surface David will touch most often. Optimize for one-turn-to-complete: the Skill should rarely need to ask follow-up questions if the request is well-formed. When the request is ambiguous (e.g., "show conversions"), the Skill asks ONE clarifying question (using AskUserQuestion when in Claude Code).

**Patterns to follow:**
- `.claude/skills/ask-corpus/SKILL.md` for skill format
- `.claude/skills/skill-creator/SKILL.md` for skill-authoring conventions

**Test scenarios:**
- *Happy path — new dashboard:* "Build a dashboard that shows lead volume by source for the last 30 days." Output: 5 files, registry entry, dashboard renders against the proxy.
- *Happy path — add tile to existing:* "Add a tile to lead-volume that splits by week instead of day." Output: 1 modified HTML, 1 new SQL, 1 new schema, 1 registry update.
- *Happy path — edit existing tile:* "Change the chart on lead-volume by-source from bar to scorecard." Output: 1 modified HTML, no new SQL.
- *Error path — request mentions a `staging.*` table:* Skill refuses, explains marts-only policy.
- *Error path — request asks for a query against a non-existent column:* Skill reads the `_marts__docs.md`, fails the request with a list of available columns.
- *Edge case — ambiguous chart type ("show me X"):* Skill asks one clarifying question (bar/line/scorecard?).
- *Edge case — query_name collision:* Skill detects an existing query with the same name and either aliases (`<name>-v2`) or asks David to confirm overwrite.

**Verification:**
- Skill produces the 5-file output shape.
- A new dashboard authored entirely via the Skill renders end-to-end against the local U3 proxy.
- The Skill's SKILL.md frontmatter `description` triggers correctly when David says "add a tile" / "build a dashboard" in Claude Code.

---

- [ ] U5. **First sample dashboard — proves the Skill → lib → proxy → BQ loop**

**Goal:** Use the `dabi-author` Skill to author one real dashboard against the consolidated GCP project. NOT Speed-to-Lead (that's Plan A). Pick a low-stakes, easy-to-verify dashboard that exercises every chart type.

**Requirements:** R7

**Dependencies:** U2, U3, U4 all green. Strategic Reset Sprint exit.

**Candidate dashboards:**
- "Lead volume by source, last 30 days" (bar + scorecard) — exercises GHL contacts data; easy to verify against the existing `dim_contacts`.
- "GHL conversation volume + freshness, last 7 days" (line + scorecard) — exercises the `raw_ghl.ghl__conversations_raw` data; doubles as a freshness monitor for David's daily ops.
- **Recommendation:** Start with "lead volume by source." Lowest blast radius if anything is wrong; multi-tile (proves layout); doesn't depend on the Speed-to-Lead chain.

**Files:**
- Create: `3-bi/dabi/dashboards/lead-volume/index.html`
- Create: `3-bi/dabi/dashboards/lead-volume/queries/by-source-30d.sql`
- Create: `3-bi/dabi/dashboards/lead-volume/queries/total-30d.sql`
- Create: `3-bi/dabi/proxy/queries/lead-volume-by-source-30d.sql`
- Create: `3-bi/dabi/proxy/queries/lead-volume-by-source-30d.schema.json`
- Create: `3-bi/dabi/proxy/queries/lead-volume-total-30d.sql`
- Create: `3-bi/dabi/proxy/queries/lead-volume-total-30d.schema.json`
- Modify: `3-bi/dabi/dashboards/_registry.js` (add lead-volume entry)
- Test: `2-dbt/tests/lead_volume_dashboard_query_parity.sql` (the dabi query results match a hand-written `bq query` baseline)

**Approach:**
- Use the Skill (don't hand-write the HTML). This is the loop validation.
- Author against `marts.*` if a `lead_volume` mart exists; if not, author against `warehouse.dim_contacts` joined to `dim_calendar_dates` and surface a follow-up to add a `lead_volume_detail` mart.
- Verify visually: open `https://<vm-url>/dashboards/lead-volume` (Phase 2 routing) OR `file:///.../3-bi/dabi/dashboards/lead-volume/index.html` (Phase 1 local) and confirm both tiles render with reasonable numbers.
- Run the parity test: `bq query` the same SQL by hand, compare row count and aggregate to the dabi-served result.

**Test scenarios:**
- *Happy path:* dashboard renders end-to-end with both tiles populated.
- *Parity:* dabi-served `total-30d` matches the hand-run `bq query` (± 0).
- *Skill verification:* the dashboard was authored by the Skill in one or two turns (not hand-edited).

**Verification:**
- Dashboard renders.
- Parity green.
- The `dabi-author` Skill has a real-world reference to validate its template choices.

---

- [ ] U6. **Magic-link auth — email + signed token + cookie session**

**Goal:** Stand up authentication so dabi can ship to D-DEE viewers without putting dashboards behind a public URL.

**Requirements:** R4

**Dependencies:** U5 (Phase 1 first-light proven). Strategic Reset Sprint exit.

**Files:**
- Create: `3-bi/dabi/auth/main.py` (FastAPI; routes: `POST /auth/request-link`, `GET /auth/exchange?token=…`, `POST /auth/logout`)
- Create: `3-bi/dabi/auth/Dockerfile`
- Create: `3-bi/dabi/auth/requirements.txt` (fastapi, uvicorn, python-jose, sqlalchemy, psycopg2-binary, sendgrid OR aiosmtplib)
- Create: `3-bi/dabi/auth/README.md`
- Create: `3-bi/dabi/auth/test_main.py` (pytest)
- Create: `3-bi/dabi/auth/migrations/001_initial.sql` (Postgres schema: users, magic_link_tokens, sessions)
- Create: `3-bi/dabi/authoring/users.yaml` (initial roster — David + agency PoCs; SDR/AE/Manager rows added via Plan A U3 pre-cutover)
- Create: `3-bi/dabi/authoring/sync_users.py` (one-shot script: read `users.yaml`, upsert into Postgres `users` table)
- Create: `docs/runbooks/dabi-magic-link-rotation.md` (signing-key rotation procedure)

**Approach:**
- **Token format.** Signed JWT (HS256 or RS256 — decided based on whether the proxy in U3 also validates). Payload: `{ sub: email, exp: 30min, jti: uuid }`. Stored in Postgres `magic_link_tokens` for single-use enforcement (delete on redemption).
- **Email delivery.** SendGrid free tier (100 emails/day — well within roster size) OR Mailgun. Decision in implementation.
- **Cookie.** `Set-Cookie: dabi_session=<jwt>; HttpOnly; Secure; SameSite=Lax; Max-Age=604800` (7-day session). Validated by Caddy in U7.
- **Postgres backing store.** Reuse the existing Cloud SQL Postgres instance from `3-bi/metabase/terraform/` — separate database `dabi_auth`. Saves a new Cloud SQL bill.
- **Roster sync.** `users.yaml` is the source of truth (git-tracked, reviewable). `sync_users.py` runs in CI on merge to main, upserts into Postgres. Removing a user from yaml + re-running drops their access.
- **Login UX.** Plain login page at `/auth/login` (HTML, no framework). Success: redirect to `/dashboards/<default>`. Magic-link email body is plain text + a single button.

**Execution note:** Test-first on the auth flows. Cover happy path + every failure mode (expired token, used-once token re-redeemed, unknown email, rate limit on link requests).

**Test scenarios:**
- *Happy path:* email request → email received → click link → session cookie set → dashboard accessible.
- *Single-use:* a redeemed token cannot be used again (returns 410 Gone).
- *Expiry:* a token older than 30 minutes returns 410.
- *Roster gate:* a non-roster email requesting a link gets a 200 (don't leak roster membership) but no email is actually sent.
- *Logout:* clears the session cookie and invalidates the underlying Postgres session row.
- *Rate limit:* 5 link requests per email per hour; over → 429.
- *Roster sync:* removing an email from `users.yaml` + running `sync_users.py` revokes their existing sessions on next request.

**Verification:**
- Auth service runs locally, `test_main.py` green.
- A real email round-trip works (David + one agency PoC tested before Plan A U3).
- Roster sync from yaml works.

---

- [ ] U7. **Runtime — extend Caddy with dabi routes; sidecar nginx for static; auth wired in**

**Goal:** Get all the dabi routes live on the existing GCE VM. Caddy routes traffic; nginx serves static; auth service handles `/auth/*`; Caddy validates session cookie before forwarding to proxy or static dashboards.

**Requirements:** R5

**Dependencies:** U2, U3, U6 deployed. Strategic Reset Sprint exit. Plan A U2 parity green (Phase 2 boundary).

**Files:**
- Modify: `3-bi/metabase/runtime/Caddyfile` (add dabi route blocks; move metabase block to `/legacy-metabase` per Plan A U3)
- Create: `3-bi/dabi/runtime/Caddyfile.dabi-blocks` (the new blocks, kept as a separate file for diff readability; appended into the active Caddyfile via the build step in U8)
- Modify: `3-bi/metabase/runtime/docker-compose.yml` (add `dabi-static` (nginx) and `dabi-auth` services; pull dashboard static from the ops bucket on startup)
- Create: `3-bi/dabi/runtime/nginx.conf` (sidecar nginx config — serves `/var/www/dabi/dashboards/*`)
- Create: `3-bi/dabi/runtime/docker-compose.dabi.yml` (the new service blocks; merged with metabase compose at deploy time)
- Modify: `3-bi/metabase/runtime/startup-script.sh` (pull dashboard static from ops bucket; pull dabi compose blocks; merge with metabase compose)
- Create: `docs/runbooks/dabi-runtime-cutover.md` (the deploy sequence; the rollback)

**Approach:**
- **Caddy session validation.** Use Caddy's `forward_auth` directive (or a small inline `@authed` matcher) that calls `auth-service:8000/auth/validate` with the session cookie before forwarding. 200 → forward, 401 → redirect to `/auth/login`.
- **Routing table** (final shape):
  - `/` → redirect to `/dashboards/index` (or whatever the default is)
  - `/dashboards/*` → forward_auth → nginx static
  - `/api/query/*` → forward_auth → Cloud Run proxy
  - `/auth/*` → auth-service (no forward_auth — this IS the auth)
  - `/lib/*` → nginx static (the dabi-core.js + vendor; cached aggressively)
  - `/legacy-metabase/*` → metabase container (per Plan A U3, 30-day rollback path)
- **Static deploy.** GH Actions builds `3-bi/dabi/dashboards/` + `3-bi/dabi/lib/` into a tarball, uploads to ops bucket, VM pulls on startup OR on a webhook (decided in U8).

**Execution note:** Test-first via a staging VM if cost allows; otherwise dry-run the Caddyfile with `caddy validate --config <file>` before deploying. Production deploy is off-hours per Plan A U3 patterns.

**Test scenarios:**
- *Happy path:* a viewer with a session cookie hits `/dashboards/lead-volume` and the page renders.
- *Auth gate:* a viewer without a session cookie hits `/dashboards/anything` and gets redirected to `/auth/login`.
- *API gate:* an unauthenticated `POST /api/query/foo` returns 401 (not 502).
- *Legacy path:* `/legacy-metabase` still serves the Metabase v1.6 dashboard (Plan A rollback alive).
- *Caddy reload:* a Caddyfile change reloads without dropping the existing connections.
- *Static cache:* `/lib/dabi-core.js` returns with a long Cache-Control header (immutable + filename-hashed in U8).

**Verification:**
- All five route classes work as expected from a fresh browser session.
- Plan A U3 rollback path verified end-to-end (revert Caddy commit + reset VM → Metabase serves at `/`).

---

- [ ] U8. **Terraform + CI — Cloud Run service, Secret Manager, GH Actions deploy**

**Goal:** Make Plan B's GCP infra reproducible. CI deploys the proxy + auth service on merge to main; Secret Manager holds magic-link signing key + SMTP creds; IAM grants the proxy SA narrow BQ access.

**Requirements:** R6

**Dependencies:** U3, U6, U7 all working locally / manually deployed. Strategic Reset Sprint exit.

**Files:**
- Create: `3-bi/dabi/terraform/cloud_run_proxy.tf` (Cloud Run service for the proxy; us-central1; concurrency, memory, timeout configured)
- Create: `3-bi/dabi/terraform/cloud_run_auth.tf` (Cloud Run service for the auth-service — OR keep it sidecar on the VM if simpler; decision in U8)
- Create: `3-bi/dabi/terraform/secrets.tf` (Secret Manager: `dabi-magic-link-signing-key`, `dabi-smtp-api-key`, `dabi-postgres-password`)
- Create: `3-bi/dabi/terraform/iam.tf` (proxy SA `dabi-proxy@…` with `roles/bigquery.dataViewer` on `marts` only; auth SA with Cloud SQL access)
- Create: `3-bi/dabi/terraform/README.md` (apply order; rollback)
- Create: `.github/workflows/dabi-deploy.yml` (on push to main: build proxy image → push to Artifact Registry → `gcloud run deploy`; sync dashboard static + lib to ops bucket)
- Create: `.github/workflows/dabi-ci.yml` (on PR: lint + pytest the proxy and auth service; smoke-test the Skill output shape; verify proxy/queries/* SQL is parseable)

**Approach:**
- **Reuse where possible.** Static IP, GCE VM, Caddy ACME state, ops bucket are all in `3-bi/metabase/terraform/`. Plan B U8 references those outputs rather than recreating.
- **Cloud Run config.** Min instances: 0 (scale to zero between requests); max instances: 5 (soft cap, raise if SDR usage climbs); memory: 512Mi (FastAPI + BQ client = ~200Mi headroom); timeout: 60s.
- **Cold start.** Acceptable for SDR usage patterns (dashboard opens are sparse vs. always-on monitoring). Document the latency expectation in the runbook.
- **CI deploy gates.** PR CI must pass before merge. Merge → deploy. Staging environment skipped for now (single-tenant; rollback is `gcloud run revisions list` + traffic split).
- **Secrets rotation.** Magic-link signing key rotation: bump the key, restart auth service, all existing sessions invalidated (acceptable trade-off; viewers re-login). Documented in U6 runbook.

**Test scenarios:**
- *CI green:* PR with a Skill-emitted dashboard passes the smoke-test workflow.
- *Deploy:* merge to main triggers proxy redeploy on Cloud Run; new revision serves traffic within 2 minutes.
- *Static sync:* dashboard HTML files in the merge are reflected at `/dashboards/*` within 2 minutes.
- *Rollback:* a bad proxy revision can be rolled back via `gcloud run services update-traffic --to-revisions=<previous>` in <1 minute.
- *Secrets:* the proxy reads its BQ credentials from the runtime SA (not from Secret Manager); the auth service reads SMTP + Postgres creds from Secret Manager at startup.

**Verification:**
- Proxy + auth service deployable via `terraform apply` from an empty state in a sandbox project (proves the IaC).
- Two consecutive PR merges deploy cleanly.
- Rollback drill executed and timed.

---

## System-Wide Impact

- **Interaction graph:** dabi reuses the GCE VM, Caddy, static IP, ACME cert, and ops bucket from `3-bi/metabase/terraform/`. Adds: Cloud Run service for proxy (and possibly auth), Postgres database `dabi_auth` on the existing Cloud SQL instance, sidecar nginx + auth containers on the VM, new Caddy routes, new Secret Manager entries.
- **Error propagation:** Proxy 5xx → tile renders error state inline (per U2). Auth service down → all dashboards hard-fail at the Caddy `forward_auth` step (acceptable; auth being down means no one logs in; no half-broken state). Cloud Run cold start adds ~2s latency to first query post-idle (documented).
- **State lifecycle risks:** Magic-link signing key rotation invalidates all sessions (intended, infrequent). Postgres database for `dabi_auth` is separate from `metabase` database on the same instance — Plan A U4 destroy-of-Metabase-Postgres must explicitly preserve `dabi_auth` (called out in Plan A U4 explicitly).
- **API surface parity:** The dabi `tile()` API and the proxy `/api/query/<name>` shape are the new contracts. Both are versioned in `3-bi/dabi/lib/dabi-core.js` (`dabi.VERSION`) so future changes are observable.
- **Integration coverage:** Plan A U2 parity gate is the single cross-layer test that validates dabi can reproduce a real Metabase dashboard. Unit tests on lib + proxy alone do not prove this.
- **Unchanged invariants:** dbt models, the marts layer, the GCE VM static IP, the ACME cert, the existing Caddy host, the BQ data-reader SA used by Metabase (different SA used by proxy).

---

## Risks & Dependencies

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Cloud Run cold start under SDR usage patterns is unacceptable (>3s perceived) | Medium | Low UX | Min-instance=1 if needed (cost: ~$5/mo); measured during U5 sample dashboard usage. |
| Magic-link emails land in spam for SDRs at common providers (Gmail/Outlook) | Medium | UX disruption | SendGrid + DKIM/SPF set up at U6; pre-cutover dry-run with David + one SDR to verify deliverability; fall-back: agency-side manual link distribution if needed. |
| Skill emits unsafe HTML (XSS in a tile title or query name) | Low | Security | Skill template uses HTML escaping; pytest in `dabi-ci.yml` smoke-tests Skill output for `<script>` injection. |
| Declared-query SQL drifts from `dashboards/<name>/queries/<tile>.sql` mirror | Medium | Correctness — dashboard shows wrong data | CI workflow asserts the two files are byte-identical. |
| Observable Plot has a rendering quirk that doesn't match a Metabase v1.6 chart shape (Plan A U2) | Medium | Plan A parity gap | Catch in U5 with the sample dashboard; if Plot can't render a needed chart shape, swap libraries (Vega-Lite is the next candidate); Plan A U2 documents the gap if minor. |
| Postgres `dabi_auth` database accidentally destroyed when Plan A U4 destroys Metabase Postgres | Low (mitigated) | Auth lost | Plan A U4 explicitly preserves the `dabi_auth` database; Cloud SQL automated backups retained for 90 days; auth state can be rebuilt from `users.yaml` (sessions invalidated, viewers re-login). |
| Cloud Run proxy SA gets broader BQ access than `marts.*` (operator error) | Low | Cost / privacy | IAM in `iam.tf` is explicit and reviewed; no `roles/bigquery.user` at project level. |
| Magic-link signing key leaked from Secret Manager | Low | Auth bypass | Standard GCP IAM on Secret Manager; rotation runbook documented; key rotation invalidates all existing sessions (intended on suspected compromise). |
| `dabi-author` Skill produces inconsistent output across Claude versions / sessions | Medium | Drift | Skill includes self-test references; CI smoke-tests every Skill-emitted PR for the 5-file shape. |
| Sprint exit slips, delaying U2-U8 | Medium | Schedule slip | U1 still lands. If David carves out a docs-only Plan B sub-track during sprint slip, U2 (lib) and U4 (Skill) can also be designed-on-paper as docs without writing code. |

---

## Phased Delivery

### Phase 1 — First light (sprint-compatible U1 now; U2-U5 after sprint exit)
- U1 — conventions + `.claude/rules/dabi.md` (sprint-compatible)
- U2 — `dabi-core` lib (post-sprint)
- U3 — Cloud Run query proxy (post-sprint, deployed)
- U4 — `dabi-author` Skill (post-sprint)
- U5 — first sample dashboard (post-sprint)

**Exit criteria:**
1. The sample dashboard renders end-to-end via the Skill → lib → proxy → BQ loop.
2. Dabi-served numbers match a hand-run `bq query` (parity test).
3. The Skill produces all 5 file types per emit.
4. CI smoke-tests pass.

**Unblocks:** Plan A U2 (Speed-to-Lead authored on dabi).

### Phase 2 — Production (auth + runtime + CI; after Phase 1)
- U6 — magic-link auth
- U7 — Caddy + sidecar runtime; auth wired in
- U8 — terraform + CI deploy

**Exit criteria:**
1. A roster user can request a link, click it, and access a dashboard.
2. Caddy enforces auth on `/dashboards/*` and `/api/query/*`.
3. CI deploys proxy + auth + static on merge.
4. Rollback drill executed.

**Unblocks:** Plan A U3 (cutover — repoint public URL from Metabase to dabi).

### Phase 3 — Hand-off enablers (during/after Plan A U3)
- Roster pre-load (David + SDRs/AEs/Managers; populated from `2-dbt/seeds/ghl_sdr_roster.csv` + agency contacts)
- Magic-link rotation runbook tested
- Monitoring set up (Cloud Run logs → Cloud Logging dashboard; magic-link request rate alarm)
- WORKLOG entry summarizing Plan B as shipped; `.claude/state/project-state.md` updated

**Exit:** Plan B is operational; future Claude sessions can author dashboards via the Skill without re-deriving the architecture.

---

## Alternative Approaches Considered

- **Pure Kim — file:// + browser-OAuth + no proxy.** Rejected for D-DEE because viewers are not engineers (KTD 1). May ship later as the deferred local-mode for David's own internal tools.
- **Pre-baked HTML — CI runs queries, embeds results in HTML at build time.** Rejected for Speed-to-Lead because real-time-ish data is the point; SDRs check stats during/between calls. Kept in mind as a fallback if Cloud Run cold-start proves unworkable.
- **Cloudflare Pages / Vercel + serverless edge proxy.** Rejected because the public URL hostname must stay stable through Plan A cutover (KTD 3); switching CDN at the same time as switching BI surface compounds risk.
- **Browser-side BQ with Google OAuth.** Rejected because viewer Google accounts in the GCP project is the exact friction this whole pivot avoids (KTD 1).
- **Adopt Evidence.dev as the runtime, use Claude Code as the editor.** Rejected because Evidence's value-add (component library, theming, Markdown DSL) is duplicative of what dabi gets from Observable Plot + plain HTML, AND because dropping the framework removes a dependency-update + framework-bug surface entirely.
- **Re-skin Metabase via its REST API.** Rejected — preserves the slow-iteration problem (still REST API + sync per change) and doesn't solve the per-viewer slice or the public-URL leak.

---

## Documentation Plan

- `3-bi/dabi/README.md` — top-level architecture + when to use dabi vs metabase (during the cutover window) — written in U1.
- `.claude/rules/dabi.md` — the conventions auto-loaded on `3-bi/dabi/**` work — written in U1.
- 4× sub-READMEs (`authoring/`, `proxy/`, `lib/`, `terraform/`) — written alongside their unit.
- `docs/runbooks/dabi-magic-link-rotation.md` — auth signing-key rotation — written in U6.
- `docs/runbooks/dabi-runtime-cutover.md` — runtime deploy + rollback — written in U7.
- `docs/runbooks/dabi-author-skill-usage.md` — David's day-to-day Skill workflow — written in U4 (or as a fast-follow once Skill stabilizes after a few weeks of real use).
- `WORKLOG.md` — entry per phase boundary.
- `.claude/state/project-state.md` — updated at end of Phase 1 (first light) and end of Phase 2 (production).

---

## Operational / Rollout Notes

- **Cost.** Cloud Run cost: ~$0 at idle (scale-to-zero); ~$5/mo if min-instance=1 is needed. Postgres `dabi_auth` reuses existing Cloud SQL instance: ~$0 marginal. SendGrid free tier: 100 emails/day, well within roster bounds. Net new cost: $0–$5/mo.
- **Monitoring.** Cloud Run logs → Cloud Logging; alarm on 5xx rate or magic-link request anomaly. Magic-link delivery monitored via SendGrid dashboard.
- **Backup.** `users.yaml` is git-tracked; Postgres `dabi_auth` rides the existing Cloud SQL automated backups. Dashboard static is git-tracked.
- **Disaster recovery.** Worst case (full GCP project loss): re-run `terraform apply` against `project-41542e21-...` from the Plan B `terraform/` dir; re-deploy proxy + auth from CI; re-sync dashboards from git; users re-login via `users.yaml` sync.

---

## Future Considerations

These are deferred, but mentioned so the architecture choice doesn't accidentally block them:

- **Pure-Kim local mode.** A `dev/` script that lets David serve `3-bi/dabi/dashboards/<name>/` from `file://` and use `gcloud auth print-access-token` for browser-side BQ — the published Kim recipe, for David's own non-shared dashboards. The same dashboard files work in both modes; `dabi-core.js` detects which auth path is available. Picked up only if David wants it.
- **Per-SDR filter injection.** Proxy injects `WHERE assignee = :user_id` for non-admin sessions. Requires the dashboard's queries to be written with that filter slot; the Skill should have a "filter-aware" template variant.
- **Multi-tenant dabi.** Folder layout already supports `dashboards/<engagement>/<name>/`. Auth roster is currently single-tenant; per-engagement auth would mean partitioning `users.yaml` and the Postgres roster.
- **Dashboard authoring UI.** A web form that calls the same Skill backend via Claude Code's headless API. Lets David's clients request tiles directly without going through David. Real product; lots of scope.
- **SCIM / SSO.** If a client's IT department demands SSO, the magic-link layer is replaceable with a Google-Workspace-OAuth or Microsoft-AAD-OIDC layer. Same downstream contract (`Set-Cookie: dabi_session`); only the `auth/` service changes.

---

## Sources & References

- **Origin:** conversation 2026-04-24 (this session); [Joshua Kim, "Building Dashboards Without BI SaaS" — Medium, April 2026](https://joshua-data.medium.com/generative-bi-en-3669ffd08ddb)
- **Approved meta-plan:** `/Users/david/.claude/plans/giggly-conjuring-tarjan.md`
- **Plan A (engagement consumer):** `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md`
- **Strategic Reset Sprint (active):** `docs/plans/2026-04-24-strategic-reset.md`
- **Port plan (paused):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Existing BI pattern reference:** `3-bi/metabase/` (folder shape mirror)
- **Skill format reference:** `.claude/skills/ask-corpus/SKILL.md`
- **Rule format reference:** `.claude/rules/metabase.md`
- **Chart library reference:** [Observable Plot](https://observablehq.com/plot/)
