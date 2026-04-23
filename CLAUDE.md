# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## What This Is

A template for data engineering projects using **dbt**, structured for AI-assisted development with Claude Code. Conventions and guardrails are version-controlled so every team member (human or AI) follows the same rules.

## Current State

**This template is pre-scaffolding.** The sections below describe conventions, file layouts, and setup steps for the *intended* end-state. Most of the `dbt/` project, `scripts/`, and `.env*` files referenced throughout **do not exist on disk yet** — they get created when you run Initial Setup for a specific client. Treat the Project Structure section as a target, not a map of what's there right now.

Existing today:
- `CLAUDE.md` (this file), `dbt_style_guide.md`, `WORKLOG.md`, `client_v1_scope_speed_to_lead.md` at the repo root
- `reference/starter-guides/` — local copies of the MDS / dbt / GitHub / Snowflake / Modern Data starter guides (also indexed in the notebook corpus)
- `reference/github-templates/` — PR template, CI workflow, deploy workflow (reference copies, not yet wired into a live `.github/` directory)
- `.claude/rules/` with `using-the-notebook.md`, `mart-naming.md`, `worklog.md`
- `.claude/skills/` with `ask-corpus`, `skill-creator`
- `.claude/scripts/sync-rule-to-notebook.sh` — helper invoked by the PostToolUse hook to upsert rule files into the NotebookLM corpus
- `.claude/settings.json` — hooks (SessionStart worklog-tail, PostToolUse rule-sync) + NotebookLM MCP allowlist

Everything else in Project Structure below is planned.

## Corpus config

Per-project NotebookLM corpus declaration lives in **`.claude/corpus.yaml`** — notebook IDs are no longer hardcoded in the `ask-corpus` skill or in rule files. To swap or add a notebook, edit that file. The skill reads it at invocation time. See `.claude/rules/using-the-notebook.md` for routing modes (`methodology.data_ops`, `methodology.metabase`, `methodology` cross-query, `engagement`).

## Reference Corpus (NotebookLM)

This project is paired with a NotebookLM notebook containing ~50 expert sources on dbt, modeling, CI/CD, and the modern data stack.

- **Notebook:** [Data Ops on NotebookLM](https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a)
- **ID:** `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`
- **What's in it:** the Data By Design free course, ~35 Modern Data Community videos (star schema, SCDs, medallion, 3-environment design, CI/CD, deployment, dbt + Claude Code), plus the 10 MDS Components / dbt / GitHub / Snowflake / Modern Data Starter Guides, this `CLAUDE.md`, `dbt_style_guide.md`, and the GitHub workflow + PR templates.

### How to use it

When writing a rule, scaffolding a dbt model, or answering a "why/how" question about data architecture, **query the notebook first**. The repo-local skill at `.claude/skills/ask-corpus/SKILL.md` wraps `mcp__notebooklm-mcp__notebook_query` and returns source-linked answers. Cite the source title inline in the file you're writing so every convention is traceable back to an expert source.

For SQL style, naming conventions, CTE patterns, and model-level configuration specifically, consult **`dbt_style_guide.md`** at the repo root — that's the canonical in-repo style reference, and it's also indexed in the notebook.

The `notebook_query` call is free (no Pro-search quota) — use it liberally instead of inventing conventions.

### Onboarding audio

A generated audio overview — **"dbt architecture with Claude Code"** — covers the 3-layer pipeline, 3-environment design, the three strategy killers, and how AI agents fit into the workflow. Play it from the notebook's Studio panel:

- [Open the Data Ops notebook → Studio](https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a)

To download a local copy for offline listening:

```bash
nlm download audio 7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a -o "onboarding - dbt architecture with Claude Code.m4a"
```

## Initial Setup

### Prerequisites
- Python 3.11+
- Git
- A data warehouse account (Snowflake, BigQuery, Redshift, etc.)
- Claude Code CLI (`npm install -g @anthropic-ai/claude-code`) or the desktop/IDE extension

### 1. Create the virtual environment and install dbt
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install dbt-<your-adapter>   # e.g., dbt-snowflake, dbt-bigquery, dbt-redshift
pip freeze > requirements.txt
```

### 2. Configure credentials
Copy the example and fill in your warehouse details:
```bash
cp .env.example .env
```
Edit `.env` with your connection details. Set the schema to a personal dev schema (e.g., `dev_jdoe`) so your work is isolated.

### 3. Configure profiles.yml
Edit `dbt/profiles.yml` with your warehouse connection settings. The template uses environment variables so credentials stay in `.env` (which is gitignored).

### 4. Source environment and verify
```bash
source .venv/bin/activate
set -a && source .env && set +a   # loads .env vars into your shell
cd dbt && dbt debug                # should show "All checks passed!"
```

### 5. Install dbt packages
```bash
dbt deps
```

## Before Running Any dbt Command

Before executing any dbt command, you MUST verify that the environment is ready:
1. Check that `.venv/` exists. If not, create it: `python3 -m venv .venv`
2. Activate it: `source .venv/bin/activate`
3. Check that dbt is installed: `which dbt`. If not found, install it: `pip install dbt-snowflake` (or the appropriate adapter from `requirements.txt` if it exists)
4. Check that `.env` exists. If not, warn the user to create one from `.env.example`
5. Source the environment: `set -a && source .env && set +a`
6. Run commands from the `dbt/` directory

Never skip these checks. Never create dbt directories manually — always use `dbt init` or the dbt CLI.

## Branching

Never commit directly to main. Always branch:
```bash
git checkout main && git pull origin main
git checkout -b feature/<description>
```

## Solo-operator pre-authorization

David is the sole operator on this engagement. When David asks Claude (in any session) to merge a PR, push a branch, delete a remote branch, close an issue, or otherwise execute an action on the default branch or shared GitHub state, **do it directly without a per-action confirmation round-trip.** This applies to `gh pr merge`, `gh pr create`, `gh pr close`, `git push`, `git push --delete`, and equivalent operations that the Bash allowlist already covers.

This is pre-authorization in the spirit of the system prompt's "risky actions" guidance: David has weighed the blast radius for this single-operator engagement and accepts it. The `.claude/settings.json` allowlist encodes the Bash layer; this clause encodes the Claude decision layer. Both are needed — the allowlist without this clause still triggers semantic confirmation prompts on "merge to default branch" and similar.

Scope: this pre-authorization covers GitHub / git / Cloud Run / BigQuery operations against David's own projects (`dee-data-ops`, `dee-data-ops-prod`, `project-41542e21-470f-4589-96d`). It does **not** cover actions against third-party systems (Fivetran billing, Metabase public-dashboard URLs with outside viewers, client communication channels) — those still require explicit per-action sign-off.

When in doubt: if David asked for it, do it. If David didn't ask and you're considering it, still confirm.

## dbt Commands

Run from the `dbt/` directory with the virtual environment activated and `.env` sourced:
```bash
dbt build --target dev -s <selection> # build + test
dbt test --select <selection>         # test only
dbt docs generate                     # generate documentation
```

## Project Structure

Legend: **(exists)** = on disk today, **(planned)** = created during Initial Setup or first client build.

```
.claude/                                  (exists)
  rules/                                  (exists)
    using-the-notebook.md                 (exists)
    mart-naming.md                        (exists)
  skills/                                 (exists)
    ask-corpus/                           (exists)
    skill-creator/                        (exists)
  settings.json                           (planned — hooks, e.g., block --target prod)
dbt/                                      (planned)
  models/
    staging/          # Views, 1:1 with source tables (stg_<source>__<table>.sql)
    warehouse/        # Kimball star schema (dim_ and fct_ tables)
      dimensions/
      facts/
    marts/            # Wide, denormalized — business-friendly names, no fct_/dim_ prefixes (see .claude/rules/mart-naming.md)
  macros/             # Includes generate_custom_schema for env-based routing
  profiles.yml        # Dev/prod/ci targets, driven by env vars
  .github/workflows/  # CI on PR, prod deploy on merge
scripts/                                  (planned — project scripts as needed)
.env                                      (planned — your credentials, gitignored)
.env.example                              (planned — template for .env)
```

## Claude Code Rules

Rules are markdown files in `.claude/rules/` that Claude Code loads automatically based on which files you're working on. They're committed to git so every team member gets the same conventions.

### How they work

Each rule file has a `paths:` frontmatter that controls when it loads. When you (or Claude) open a file matching that path, the rule activates — no action needed.

### How to create one

Add a `.md` file to `.claude/rules/` with this format:

```markdown
---
paths: ["dbt/models/staging/**"]
---

# Staging Conventions

- Staging models are 1:1 with source tables
- Named: stg_<source>__<table_name>.sql
- Always materialized as views
- Only staging models select from sources

## Lessons Learned
- (Add entries here as issues are discovered)
```

### What to put in rules

Rules should capture conventions that aren't obvious from the code itself:
- **Naming patterns** — how to name models, columns, files
- **Style guide** — SQL formatting, CTE patterns, YAML conventions
- **Data security** — what Claude should and shouldn't do with data
- **Warehouse-specific** — environment setup, roles, permissions
- **Lessons learned** — failure modes, gotchas, things discovered over time

Start with what matters most to your team and add rules as you go. One file per topic, scoped to the relevant paths.

### Hooks

The `.claude/settings.json` file contains hooks that **enforce** guardrails deterministically. For example, a hook can block `--target prod` commands so production deploys only happen through CI/CD. Unlike rules (which are guidance), hooks run as shell commands and can block actions before they execute.

## Next Steps

1. Update `profiles.yml` and `.env` for your warehouse
2. Add your first source in `dbt/models/staging/<source_name>/`
3. Build staging models, then layer up into warehouse dimensions/facts, then marts
4. Create `.claude/rules/` files for your team's conventions as they emerge

If you have an existing dbt project, provide it to Claude and ask it to align with this structure.
