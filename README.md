# dee-data-ops

Data engineering monorepo for the D-DEE speed-to-lead program.
Top-level folders are ordered left-to-right along the medallion pipeline:

```
  Raw Landing  →  Staging  →  Warehouse  →  Marts  →  BI Tools
 (1-raw-landing)                  (2-dbt)                  (3-bi)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^
                             all inside 2-dbt/models/
```

## Where things live

| Folder           | Stage                      | What's inside |
|------------------|----------------------------|---------------|
| `1-raw-landing/` | Raw Landing                | Custom Python extractors (GHL, Calendly, Fanbasis) + their Cloud Run deploy configs under `deploy/`. |
| `2-dbt/`         | Staging + Warehouse + Marts | The dbt project. Models split into `staging/`, `warehouse/{dimensions,facts,bridges}/`, and `marts/`. Plus seeds, macros, tests, snapshots. |
| `3-bi/metabase/` | BI Tools                   | Self-hosted Metabase OSS — `authoring/` (dashboards as code), `runtime/` (Docker on GCE), `terraform/` (GCP infra). |
| `docs/`          | —                          | Plans, discovery artifacts, runbooks, conventions, proposals. `_archive/` holds retired docs. |
| `.claude/`       | —                          | Agent scaffolding: rules, skills, agents, commands, scripts, state, corpus config. |
| `.github/workflows/` | —                      | CI (PR build + docs + deploy), scheduled ingest, nightly refresh. |

## Current GCP Map

The project is mid-cutover to one GCP home. Treat
`project-41542e21-470f-4589-96d` as the active consolidated target for dbt,
CI, docs, and the Gold-layer rebuild. Legacy `dee-data-ops` and
`dee-data-ops-prod` references still exist where runtime infrastructure has not
yet been moved or where a document is preserving historical context.

| Area | Current target | Notes |
|---|---|---|
| dbt dev / ci / prod | `project-41542e21-470f-4589-96d` | See `2-dbt/profiles.yml` and `2-dbt/macros/generate_schema_name.sql`. |
| dbt PR CI | `project-41542e21-470f-4589-96d` | Per-PR datasets are named `ci_pr_<number>`. |
| Discovery Sprint / Gold roadmap | `project-41542e21-470f-4589-96d` | Source inventory snapshots were taken against the consolidated project. |
| Legacy ingest / BI runtime | `dee-data-ops`, `dee-data-ops-prod` | Transitional only. Do not assume these names are the final architecture. Check the relevant runbook before changing workflow values. |

## Entry points

- **Current state snapshot:** [`.claude/state/project-state.md`](.claude/state/project-state.md) — 40–60 line index of what's true right now
- **Session log:** [`WORKLOG.md`](WORKLOG.md) — append-only; newest entry at the top
- **Portable conventions:** [`CLAUDE.md`](CLAUDE.md) — the craft, same across clients
- **Engagement overlay:** `CLAUDE.local.md` — gitignored, this-engagement context
- **dbt style:** [`docs/conventions/dbt_style_guide.md`](docs/conventions/dbt_style_guide.md)
- **dbt docs (prod):** https://davv5.github.io/dee-data-ops/ — auto-published by [`.github/workflows/dbt-docs.yml`](.github/workflows/dbt-docs.yml) after every push to `main` that touches `2-dbt/**`.

## Getting started

See the **Initial Setup** section in [`CLAUDE.md`](CLAUDE.md#initial-setup).
