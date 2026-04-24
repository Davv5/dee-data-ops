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

## Entry points

- **Current state snapshot:** [`.claude/state/project-state.md`](.claude/state/project-state.md) — 40–60 line index of what's true right now
- **Session log:** [`WORKLOG.md`](WORKLOG.md) — append-only; newest entry at the top
- **Portable conventions:** [`CLAUDE.md`](CLAUDE.md) — the craft, same across clients
- **Engagement overlay:** `CLAUDE.local.md` — gitignored, this-engagement context
- **dbt style:** [`docs/conventions/dbt_style_guide.md`](docs/conventions/dbt_style_guide.md)
- **dbt docs (prod):** https://davv5.github.io/dee-data-ops/ — auto-published by [`.github/workflows/dbt-docs.yml`](.github/workflows/dbt-docs.yml) after every push to `main` that touches `2-dbt/**`.

## Getting started

See the **Initial Setup** section in [`CLAUDE.md`](CLAUDE.md#initial-setup).
