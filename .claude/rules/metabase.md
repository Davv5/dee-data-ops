---
paths: ["3-bi/metabase/**"]
---

# Metabase conventions

Load when working on anything under `3-bi/metabase/**`. Metabase is D-DEE's legacy dashboard layer — self-hosted OSS on GCP, currently reading `dee-data-ops-prod.marts.*` until the BI/runtime cutover moves it to the consolidated project.

Two corpus sources ground these conventions — cite them when extending:
- *Metabase official docs* (https://www.metabase.com/docs/latest/)
- *Metabase REST API reference* (served at `{MB_URL}/api/docs` on every instance)

## The five rules

### 1. Dashboards are code

Every dashboard, question, and collection on the **production** Metabase instance must be the output of a Python script committed under `3-bi/metabase/authoring/`. Nothing in production is GUI-authored.

- Authoring scripts call the Metabase REST API (`POST /api/card`, `POST /api/dashboard`, `POST /api/dashboard/:id/cards`, …).
- Every script is idempotent — running it twice produces the same Metabase state. Use `entity_id` on all upserts.
- A new dashboard is a new script; a change to an existing dashboard is a PR editing the script.
- The GUI on prod is **view-only in practice** — any change a user makes there will be overwritten on the next authoring-script run.
- A *development* Metabase instance (local Docker or a separate VM) is allowed to be GUI-mutable — that's where you prototype before committing the capture to a script.

*Why not just use Metabase's Pro serialization feature:* Pro is $575/mo. OSS + REST-API authoring gives equivalent reproducibility for $0, with full DataOps discipline.

### 2. App-DB is backed up, not version-controlled

Dashboards-as-code means the Postgres app-DB state is **derivable** from the authoring scripts. But users, permissions, activity history, and any accidentally-GUI-authored state still live there. Therefore:

- `pg_dump` runs nightly from a GCE cron job, writes to `gs://dee-data-ops-prod-metabase-backups/app-db/YYYY-MM-DD.sql.gz`.
- Retention: 30 days. Lifecycle rule on the bucket moves >30-day backups to cold storage or deletes.
- Recovery drill is documented in `3-bi/metabase/RECOVERY.md` — run it once every quarter.
- Do NOT commit `pg_dump` output to git. The backup bucket is the source of truth for app-DB state.

### 3. Every connection is code

BigQuery datasource config, Google OAuth config, email/Slack notification channels — all are declared in authoring scripts under `3-bi/metabase/authoring/infrastructure/`, not clicked in the GUI.

- Secrets (BQ SA JSON, OAuth client secret, SMTP password) live in **GCP Secret Manager** in `dee-data-ops-prod`. Authoring scripts resolve them at runtime via `google.cloud.secretmanager`.
- The VM's startup script pulls secrets into container env vars — the same pattern Track J established for the GHL extractor.
- Never hardcode a credential in an authoring script. Never commit a `.env.metabase` file.

### 4. dbt metadata flows into Metabase

`dbt-metabase` (https://github.com/gouline/dbt-metabase) is the canonical bridge. After every `dbt deploy` to prod, a follow-up step runs:

```bash
dbt-metabase models \
  --dbt-manifest-path 2-dbt/target/manifest.json \
  --metabase-url "$MB_URL" \
  --metabase-api-key "$MB_SESSION_TOKEN" \
  --metabase-database "dee-data-ops-prod"
```

This propagates column-level `description:`, relationship tests, and semantic types from dbt `.yml` into Metabase's schema browser. dbt stays authoritative; Metabase displays.

Never hand-edit a column description in Metabase. Edit it in the dbt `.yml` and let the next deploy sync.

### 5. The authoring script names itself from the dbt mart

One authoring script per mart. File name mirrors the mart:

```
3-bi/metabase/authoring/dashboards/
  sales_activity_detail.py   # reads marts.sales_activity_detail → Page 1
  lead_journey.py            # reads marts.lead_journey           → Page 2
  revenue_detail.py          # reads marts.revenue_detail         → Page 3
```

Each script owns:
- One collection in Metabase (e.g., `Speed-to-Lead`)
- The questions inside it (named after the underlying mart column it surfaces)
- The dashboard(s) those questions roll into

Cross-mart dashboards live in their own script under `3-bi/metabase/authoring/dashboards/executive_summary.py` etc.

## Directory layout

```
3-bi/metabase/
├── README.md                         # install + run guide (top-level)
├── RECOVERY.md                       # restore-from-backup drill
├── terraform/                        # GCP infra as code
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── README.md
├── runtime/                          # what lives on the VM
│   ├── docker-compose.yml            # metabase + caddy
│   ├── Caddyfile
│   └── startup-script.sh             # GCE instance startup: pull secrets, start compose
└── authoring/                        # REST-API dashboards as code
    ├── client.py                     # thin HTTP wrapper over /api
    ├── sync.py                       # upsert helpers (entity_id keyed)
    ├── infrastructure/
    │   ├── bigquery_connection.py
    │   └── users_and_groups.py
    └── dashboards/
        ├── speed_to_lead.py
        ├── lead_journey.py
        └── revenue_detail.py
```

## Environment separation

| Env | Metabase instance | BQ source | App-DB | How reached |
|---|---|---|---|---|
| dev | local Docker on David's laptop | `dee-data-ops` (dev) | ephemeral Postgres container | `http://localhost:3000` |
| prod | GCE VM | `dee-data-ops-prod` | Cloud SQL Postgres | `https://<IP>.nip.io` (or custom domain later) |

There is NO `ci` Metabase. CI runs the authoring scripts in dry-run mode (`--dry-run` flag) against a mock API, verifying JSON payloads are well-formed. Full integration testing happens on the dev instance.

## MCP integration

Metabase v60+ ships an official MCP server (see `/docs/latest/ai/mcp` on your instance). Once the prod instance is up:

1. Add the Metabase MCP server to Claude Code's MCP config (`~/.claude/mcp.json` or project-local) using the instance URL + a session token provisioned for MCP use.
2. Claude Code can then introspect tables/questions/dashboards, run SQL against the semantic layer, and (with caveat: MCP ↔ OSS feature parity varies) create objects conversationally.
3. MCP sessions are **dev-loop tools**, not substitutes for authoring scripts. Prototype in MCP, commit via authoring script.

## Template-ability for next client

What ports forward to a new PS engagement unchanged:
- Everything under `3-bi/metabase/terraform/` (parameterized by project ID + SA names)
- Everything under `3-bi/metabase/runtime/`
- `3-bi/metabase/authoring/client.py` + `sync.py`
- This rules file

What is client-specific (edit per engagement):
- `3-bi/metabase/authoring/dashboards/*.py` — mart-specific
- `3-bi/metabase/authoring/infrastructure/bigquery_connection.py` — project-specific
- The domain/hostname

Add a new-client adaptation to `NEW_CLIENT_METABASE_SOP.md` post-v1.

## Lessons learned

- Enable query-result caching at the server level (`MB_ENABLE_QUERY_CACHING=true`
  in docker-compose.yml) so per-dashboard overrides work. Per-dashboard
  `cache_ttl` is aligned with the upstream rollup refresh cadence:
  - **Live-by-default rollups** (2-min Cloud Run builder): `cache_ttl=0`
    (explicit bypass — every render queries fresh).
  - **Daily-cadence rollups** (nightly-only): `cache_ttl=21600` (6h).
  Use `0` rather than `null` for live dashboards — `0` is self-documenting
  ("live"), `null` is ambiguous ("server default"). Keep
  `details["include-user-id-and-hash"] = False` on the BQ connection so
  cache misses can still fall through to BigQuery's free 24-hour native
  result cache. See `.claude/rules/live-by-default.md` for the end-to-end chain.
  (source: *"Google BigQuery | Metabase Documentation"*, Metabase Craft
  notebook; *"Caching query results"*, Metabase Learn notebook, source
  d6a8e3ae.)
  **OSS v0.60.1 gotcha:** `enable-query-caching` is a **read-only
  setting** via the REST API — `PUT /api/setting/enable-query-caching`
  returns HTTP 500 "read-only setting." Enable via the
  `MB_ENABLE_QUERY_CACHING=true` environment variable on the server
  (docker-compose.yml), then restart Metabase. The per-dashboard
  `cache_ttl` can be set via the dashboard PUT endpoint even on OSS
  (Track D empirical 2026-04-22: cache_ttl=21600 persisted on v0.60.1,
  contradicting the Metabase Learn "Pro-only" note in source d6a8e3ae).
  **OSS v0.60.1 gotcha — dashboard auto-refresh:** There is no `refresh_period`
  or equivalent key on the `/api/dashboard` PUT endpoint. Auto-refresh is
  a frontend-only feature. Activate by appending `#refresh=60` to the
  dashboard's public share URL or iframe src. Embedded dashboards inherit
  this via the embedding URL. No server-side API call needed or available.
  (source: *"Dashboards"* overview, Metabase Learn notebook, source 04cf5679;
  Metabase Craft notebook corpus query 2026-04-22)

- Dashboard subscriptions (pulses) are admin-config, not dashboard-code.
  Per Metabase Learn's "Pushing data" guidance, match the subscription
  cadence to the decision-making cadence (weekly for SDR-management
  decisions). Recipient lists live in env (gitignored), never in the
  repo. SMTP must be configured first — check `email-configured?` in
  `GET /api/setting` before running the subscriptions script; the
  `/api/email` endpoint returns 404 on OSS v0.60.1 (not a valid route
  on this build). When SMTP is not configured, the
  `dashboard_subscriptions.py` script exits 1 with setup instructions.
  `schedule_hour` is interpreted in the instance's report-timezone —
  assert `report-timezone = America/New_York` before creating the pulse
  so Monday 06:00 fires at 06:00 ET, not UTC.
  (source: *"Pushing data"*, Metabase Learn notebook, source 46e8daaf;
  *"Dashboard subscriptions"*, Metabase Learn notebook, source 9fe1ca85.)
