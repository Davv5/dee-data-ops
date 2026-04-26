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
| `3-bi/metabase/` | BI Tools (retired)         | Historical reference. Self-hosted Metabase OSS ran D-DEE through 2026-04; runtime is now torn down. Code preserved as a migration source for the new BI direction (dabi, see `docs/plans/2026-04-24-001-feat-pivot-to-generative-bi-plan.md`). |
| `docs/`          | —                          | Plans, discovery artifacts, runbooks, conventions, proposals. `_archive/` holds retired docs. |
| `.claude/`       | —                          | Agent scaffolding: rules, skills, agents, commands, scripts, state, corpus config. |
| `.github/workflows/` | —                      | CI (PR build + docs + deploy), scheduled ingest, nightly refresh. |

### Folder layout (expanded)

```
1-raw-landing/                            # Raw Landing (pipeline stage 1)
  <source>/                               # one dir per source: extract.py + Dockerfile + requirements.txt
  deploy/                                 # Cloud Run + terraform for each extractor
    <source>-extractor/
2-dbt/                                    # Staging + Warehouse + Marts (pipeline stage 2)
  models/
    staging/          # Views, 1:1 with source tables (stg_<source>__<table>.sql)
    warehouse/        # Kimball star schema (dim_ and fct_ tables)
      dimensions/
      facts/
    marts/            # Wide, denormalized — business-friendly names (see .claude/rules/mart-naming.md)
  macros/             # Includes generate_schema_name for env-based routing
  profiles.yml        # Dev/prod/ci targets, driven by env vars
3-bi/                                     # BI Tools (pipeline stage 3)
  metabase/
    authoring/        # dashboards-as-code (REST API scripts)
    runtime/          # Docker compose + Caddy + startup script
    terraform/        # GCP infra for Metabase

.github/workflows/                        # CI on PR, prod deploy on merge
docs/                                     # plans, discovery, runbooks, conventions, _archive
.claude/                                  # rules, skills, agents, commands, scripts, state
.env                                      # your credentials, gitignored
.env.example                              # template for .env
```

## Current GCP map

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
- **Always-on Claude operating instructions:** [`CLAUDE.md`](CLAUDE.md)
- **Engagement overlay (gitignored):** `CLAUDE.local.md` — locked metric, oracle numbers
- **dbt style:** [`docs/conventions/dbt_style_guide.md`](docs/conventions/dbt_style_guide.md)
- **dbt docs (prod):** https://davv5.github.io/dee-data-ops/ — auto-published by [`.github/workflows/dbt-docs.yml`](.github/workflows/dbt-docs.yml) after every push to `main` that touches `2-dbt/**`.

## Initial setup

### Prerequisites

- Python 3.11+
- Git
- A BigQuery account with access to `project-41542e21-470f-4589-96d` (or the appropriate GCP project for your engagement)
- Claude Code CLI (`npm install -g @anthropic-ai/claude-code`) or the desktop / IDE extension

### 1. Create the virtual environment and install dbt

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # or: pip install dbt-bigquery
```

### 2. Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your connection details. Set the schema to a personal dev schema (e.g., `dev_jdoe`) so your work is isolated.

### 3. Verify profiles.yml

`2-dbt/profiles.yml` reads from environment variables, so credentials stay in `.env` (which is gitignored). No manual edits required if you use the standard variable names.

### 4. Source environment and verify

```bash
source .venv/bin/activate
set -a && source .env && set +a   # loads .env vars into your shell
cd 2-dbt && dbt debug              # should show "All checks passed!"
```

### 5. Install dbt packages

```bash
dbt deps
```

## Running dbt

Always run from `2-dbt/` with the venv activated and `.env` sourced:

```bash
dbt build --target dev -s <selection>   # build + test
dbt test --select <selection>           # test only
dbt docs generate                       # generate documentation
```

`--target prod` is reserved for CI. A `.claude/settings.json` hook blocks local prod runs deterministically.

## Claude Code rules system

Path-scoped conventions live in `.claude/rules/*.md`. Each rule file has a `paths:` frontmatter that controls when Claude loads it — the rule activates automatically when Claude opens a file matching its path. Rules are committed to git so every team member (human or AI) follows the same conventions.

Existing rules:

- `staging.md` — staging-model conventions (1:1 with sources, materialized as views)
- `warehouse.md` — warehouse / dimension / fact conventions
- `mart-naming.md` — mart layer naming (business-friendly, no `fct_`/`dim_` prefix in marts)
- `ingest.md` — raw-landing extractor conventions
- `metabase.md` — Metabase ops + authoring rules
- `data-modeling-process.md` — Joshua Kim's "order in which I model data"
- `using-the-notebook.md` — when to invoke `ask-corpus`, cite-source norm, raw-MCP / Perplexity boundary
- `worklog.md` — when to update `WORKLOG.md` and `.claude/state/project-state.md`
- `live-by-default.md` — Phase A discovery vs Phase B build cadence

To add a rule, create a new `.md` file in `.claude/rules/` with `paths:` frontmatter:

```markdown
---
paths: ["2-dbt/models/staging/**"]
---

# Staging Conventions

- Staging models are 1:1 with source tables
- Named: stg_<source>__<table_name>.sql
- Always materialized as views
- Only staging models select from sources
```

Hooks live in `.claude/settings.json` and **enforce** guardrails deterministically (e.g., blocking `--target prod`). Use a hook when the rule is binary and the cost of getting it wrong is high; use a rule when it's a convention you want Claude to default to.

## Onboarding audio

A generated audio overview — **"dbt architecture with Claude Code"** — covers the 3-layer pipeline, 3-environment design, the three strategy killers, and how AI agents fit into the workflow. Play it from the Data Ops notebook's Studio panel:

- [Open the Data Ops notebook → Studio](https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a)

Download a local copy:

```bash
nlm download audio 7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a -o "onboarding - dbt architecture with Claude Code.m4a"
```
