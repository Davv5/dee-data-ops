# CLAUDE.md

Always-on Claude operating instructions for the **D-DEE engagement** data ops repo (consolidating onto GCP project `project-41542e21-470f-4589-96d`).

For human onboarding (initial setup, project layout, how the rules system works), see [`README.md`](README.md). Path-scoped conventions live under [`.claude/rules/`](.claude/rules/) and load automatically when matching files are open.

## Where to look

- **Session-start state:** [`.claude/state/project-state.md`](.claude/state/project-state.md) — auto-injected at session start
- **Session history:** [`WORKLOG.md`](WORKLOG.md) — `grep -n` for past decisions
- **Engagement overlay (gitignored):** `CLAUDE.local.md` — locked metric, oracle numbers, client-specific context
- **dbt style:** [`docs/conventions/dbt_style_guide.md`](docs/conventions/dbt_style_guide.md)
- **Corpus queries:** [`ask-corpus` skill](.claude/skills/ask-corpus/SKILL.md) (v2 planner / fan-out / fuse / rerank engine; not a `notebook_query` wrapper). Trigger conditions, cite-source norm, and the boundary with raw MCP / Perplexity live in [`.claude/rules/using-the-notebook.md`](.claude/rules/using-the-notebook.md).

## Pivot discipline

When you save a memory file under `~/.claude/projects/-Users-david-Documents-data-ops/memory/` that captures a *strategic pivot* (BI direction change, source-of-truth change, scope cut, work-bucket pause/resume, supersession of a prior memory), the same session must walk the checklist in [`.claude/rules/pivot-discipline.md`](.claude/rules/pivot-discipline.md) and update or banner-archive the docs the pivot supersedes (`CLAUDE.local.md`, affected rules, plans, scope docs, project-state index). If the walk can't happen in-session, surface "pivot-debt" as an Open Thread in `project-state.md` per that rule's escape-valve clause. Memory writes about pivots without the walk create the compound-fog risk this rule was written to prevent (empirical anchor: 2026-04-28 reconciliation sweep that found 7 stale artifacts caused by un-walked pivots).

## dbt operating note

Run from `2-dbt/` with the venv activated and `.env` sourced. Default target is `dev`. **Never run `dbt build --target prod` locally** — production goes through CI on merge to `main`. A `.claude/settings.json` hook blocks `--target prod` deterministically.

## Branching

Never commit directly to `main`. Branch from `main`, push, open a PR, merge through `gh pr merge`. CI runs dbt + ingest + deploy workflows on every PR.

## Solo-operator pre-authorization

David is the sole operator on this engagement. When David asks Claude (in any session) to merge a PR, push a branch, delete a remote branch, close an issue, or otherwise execute an action on the default branch or shared GitHub state, **do it directly without a per-action confirmation round-trip.** This applies to `gh pr merge`, `gh pr create`, `gh pr close`, `git push`, `git push --delete`, and equivalent operations that the Bash allowlist already covers.

This is pre-authorization in the spirit of the system prompt's "risky actions" guidance: David has weighed the blast radius for this single-operator engagement and accepts it. The `.claude/settings.json` allowlist encodes the Bash layer; this clause encodes the Claude decision layer. Both are needed — the allowlist without this clause still triggers semantic confirmation prompts on "merge to default branch" and similar.

Scope: this pre-authorization covers GitHub / git / Cloud Run / BigQuery operations against David's own projects. The active/consolidated GCP project is `project-41542e21-470f-4589-96d` (post-U2 retarget 2026-04-23); `dee-data-ops` and `dee-data-ops-prod` remain in scope during the transition until they are decommissioned at U14. It does **not** cover actions against third-party systems (Fivetran billing, Metabase public-dashboard URLs with outside viewers, client communication channels) — those still require explicit per-action sign-off.

When in doubt: if David asked for it, do it. If David didn't ask and you're considering it, still confirm.
