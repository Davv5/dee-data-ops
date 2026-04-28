# dbt Project

This folder is the transformation layer for the D-DEE data-ops repo.

## Layout

```
2-dbt/
├── models/
│   ├── staging/      # 1:1 source cleanup views
│   ├── warehouse/    # dimensions, facts, bridges, volume monitors
│   └── marts/        # business-facing tables for BI
├── seeds/            # version-controlled reference data
├── snapshots/        # point-in-time history
├── tests/            # custom data tests and release gates
├── macros/           # schema routing and reusable dbt logic
├── dbt_project.yml
└── profiles.yml
```

## GCP Targeting

The active dbt target project is `project-41542e21-470f-4589-96d` for dev,
ci, and prod. `generate_schema_name.sql` keeps dev/ci builds consolidated into
one working dataset while prod uses the layer schemas from `dbt_project.yml`
(`staging`, `warehouse`, `marts`).

Legacy `dee-data-ops` and `dee-data-ops-prod` references may still appear in
ingest or BI runtime docs during the transition, but dbt should not be pointed
back at them unless a rollback plan explicitly says so.

## Running Locally

From the repo root:

```bash
source .venv/bin/activate
set -a && source .env && set +a
cd 2-dbt
dbt debug --target dev_local
dbt build --target dev_local -s <selection>
```

Two profile flavors cover the two local auth situations:

- **`dev_local` / `ci_local`** — use ADC (run `gcloud auth application-default
  login` once). Default in `.env` because no consolidated-project SA keyfile
  is provisioned yet. No keyfile required.
- **`dev` / `ci`** — use a service-account keyfile via `BQ_KEYFILE_PATH`.
  Switch `DBT_TARGET=dev` in `.env` once you drop in a key at
  `~/.config/gcloud/<consolidated-project>/dbt-dev.json`.

Keep `DBT_PROFILES_DIR` pointed at the absolute path to this folder, as shown in
`.env.example`.

## Local CI (`scripts/local-ci.sh`)

`2-dbt/scripts/local-ci.sh` mirrors the GH Actions `dbt-ci.yml` workflow on
your laptop. Use it as a pre-push smoke check, when GH Actions is degraded
and a PR needs to merge, or when you want to eyeball a parity-test result
before opening the PR.

```bash
bash 2-dbt/scripts/local-ci.sh <pr-number>                    # full build
bash 2-dbt/scripts/local-ci.sh <pr-number> --select <model>+  # subset
```

The script picks SA-keyfile vs ADC auth automatically, provisions the per-PR
dataset (`ci_pr_<num>`) idempotently, and runs `dbt build --target {ci|ci_local}`
against it. It is **not** a substitute for the GH Actions merge gate — public
CI history is part of the engagement deliverable.

## Conventions

- SQL and YAML style: `docs/conventions/dbt_style_guide.md`
- Current project state: `.claude/state/project-state.md`
- Source discovery and known gaps: `docs/discovery/`
- Layer rules: `.claude/rules/staging.md`, `.claude/rules/warehouse.md`,
  `.claude/rules/mart-naming.md`
