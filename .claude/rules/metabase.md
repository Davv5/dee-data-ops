---
paths: ["ops/metabase/**"]
---

# Metabase conventions

Load when working on anything under `ops/metabase/**`. Metabase is D-DEE's dashboard layer — self-hosted OSS on GCP, reading `dee-data-ops-prod.marts.*`.

Two corpus sources ground these conventions — cite them when extending:
- *Metabase official docs* (https://www.metabase.com/docs/latest/)
- *Metabase REST API reference* (served at `{MB_URL}/api/docs` on every instance)

## The five rules

### 1. Dashboards are code

Every dashboard, question, and collection on the **production** Metabase instance must be the output of a Python script committed under `ops/metabase/authoring/`. Nothing in production is GUI-authored.

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
- Recovery drill is documented in `ops/metabase/RECOVERY.md` — run it once every quarter.
- Do NOT commit `pg_dump` output to git. The backup bucket is the source of truth for app-DB state.

### 3. Every connection is code

BigQuery datasource config, Google OAuth config, email/Slack notification channels — all are declared in authoring scripts under `ops/metabase/authoring/infrastructure/`, not clicked in the GUI.

- Secrets (BQ SA JSON, OAuth client secret, SMTP password) live in **GCP Secret Manager** in `dee-data-ops-prod`. Authoring scripts resolve them at runtime via `google.cloud.secretmanager`.
- The VM's startup script pulls secrets into container env vars — the same pattern Track J established for the GHL extractor.
- Never hardcode a credential in an authoring script. Never commit a `.env.metabase` file.

### 4. dbt metadata flows into Metabase

`dbt-metabase` (https://github.com/gouline/dbt-metabase) is the canonical bridge. After every `dbt deploy` to prod, a follow-up step runs:

```bash
dbt-metabase models \
  --dbt-manifest-path dbt/target/manifest.json \
  --metabase-url "$MB_URL" \
  --metabase-api-key "$MB_SESSION_TOKEN" \
  --metabase-database "dee-data-ops-prod"
```

This propagates column-level `description:`, relationship tests, and semantic types from dbt `.yml` into Metabase's schema browser. dbt stays authoritative; Metabase displays.

Never hand-edit a column description in Metabase. Edit it in the dbt `.yml` and let the next deploy sync.

### 5. The authoring script names itself from the dbt mart

One authoring script per mart. File name mirrors the mart:

```
ops/metabase/authoring/dashboards/
  sales_activity_detail.py   # reads marts.sales_activity_detail → Page 1
  lead_journey.py            # reads marts.lead_journey           → Page 2
  revenue_detail.py          # reads marts.revenue_detail         → Page 3
```

Each script owns:
- One collection in Metabase (e.g., `Speed-to-Lead`)
- The questions inside it (named after the underlying mart column it surfaces)
- The dashboard(s) those questions roll into

Cross-mart dashboards live in their own script under `ops/metabase/authoring/dashboards/executive_summary.py` etc.

## Directory layout

```
ops/metabase/
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
- Everything under `ops/metabase/terraform/` (parameterized by project ID + SA names)
- Everything under `ops/metabase/runtime/`
- `ops/metabase/authoring/client.py` + `sync.py`
- This rules file

What is client-specific (edit per engagement):
- `ops/metabase/authoring/dashboards/*.py` — mart-specific
- `ops/metabase/authoring/infrastructure/bigquery_connection.py` — project-specific
- The domain/hostname

Add a new-client adaptation to `NEW_CLIENT_METABASE_SOP.md` post-v1.

## Lessons learned

- Dashboard filters on native-SQL questions bind via template tags. Use
  Field Filters (type=dimension, target shape `["dimension", ["template-tag",
  "<tag>"]]`) for smart date/category widgets; omit the column name and
  `=` operator in the SQL (Metabase injects the subquery). Wrap the WHERE
  in `[[...]]` so the card renders standalone when unfiltered. Partial
  filter coverage — leaving pre-aggregated cards unbound — is acceptable
  when the rollup doesn't carry the filter's dimension
  (sources: "Field Filters" + "Adding filters and making interactive BI
  dashboards", Metabase Learn notebook).

- Enable query-result caching on any public-share dashboard reading
  BigQuery. Set a per-dashboard `cache_ttl` aligned with the upstream
  rollup refresh cadence (6h for dbt-prod-daily rollups). Keep
  `details["include-user-id-and-hash"] = False` on the BQ connection
  so cache misses can still fall through to BigQuery's free 24-hour
  native result cache.
  (source: *"Google BigQuery | Metabase Documentation"*, Metabase Craft
  notebook; *"Caching query results"*, Metabase Learn notebook, source
  d6a8e3ae.)
  **OSS v0.60.1 gotcha:** `enable-query-caching` is a **read-only
  setting** via the REST API — `PUT /api/setting/enable-query-caching`
  returns HTTP 500 "read-only setting." Enable via the
  `MB_ENABLE_QUERY_CACHING=true` environment variable on the server
  (docker-compose.yml), then restart Metabase. The per-dashboard
  `cache_ttl` can be set via the dashboard PUT endpoint even on OSS.

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

- **Any track introducing a new `stl_*` rollup (or any new dbt model
  referenced by a Metabase card) must run `dbt build --target prod
  --select <model>` BEFORE the authoring script pushes the dashboard
  change to prod Metabase.** The authoring script mutates the live
  prod dashboard; if the backing table doesn't exist in prod BQ yet,
  the new tile errors and the dashboard shows a top-level banner for
  end users. Track E (PR #50) hit this exact trap on 2026-04-22 when
  `stl_data_freshness.sql` was built in dev but not prod. Future
  plan-architect track files must include a "dbt prod build" pre-step
  for any new model, and pr-reviewer should request-changes if the
  track introduces a new rollup without that step.
  *Caveat:* local-shell `--target prod` is hook-blocked until the
  Phase-6 CI workflow (`dbt-deploy.yml` + `GCP_SA_KEY_PROD`) ships —
  the step is human-only for now, which is why it must be explicit
  in the track file.
