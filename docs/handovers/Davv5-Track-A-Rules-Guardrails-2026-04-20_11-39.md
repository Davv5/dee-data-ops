# Session Handover — Track A: rules + AI-workflow guardrails

**Branch:** `Davv5/Track-A-Rules-Guardrails`
**Timestamp:** `2026-04-20_11-39` (local)
**Author:** Claude Code (Opus 4.7, 1M context) — autonomous Track A session
**PR:** pending `git push` + `gh pr create` (see "First task for next session")

---

## Session goal

Execute Track A of the parallel worktree plan: port + adapt four path-scoped `.claude/rules/` files, wire a `/handover` slash command + template, add a PreToolUse hook blocking local prod dbt runs, and drop in the three lint/formatter configs (sqlfluff, pre-commit, pyproject).

## Changed files

```
.claude/rules/staging.md                              — created — ported AS-IS from GTM lead warehouse; path ref fixed to dbt_style_guide.md
.claude/rules/warehouse.md                            — created — ported; dim_contact identity-spine adapted to v1 GHL-only anchor
.claude/rules/ingest.md                               — created — ported; Cloud Run Jobs → GitHub Actions; source list trimmed to D-DEE's 5
.claude/commands/handover.md                          — created — ported AS-IS from fanbasis-ingest
.claude/settings.json                                 — edited  — added PreToolUse Bash hook; preserved SessionStart + PostToolUse
docs/handovers/TEMPLATE.md                            — created — structure for session-continuity docs
docs/handovers/Davv5-Track-A-Rules-Guardrails-2026-04-20_11-39.md — created — this file
.sqlfluff                                             — created — ported AS-IS (BigQuery + jinja templater)
.pre-commit-config.yaml                               — created — ported AS-IS (5 local guardrails incl. forbid-dbt-target-prod)
pyproject.toml                                        — created — ported AS-IS; known-first-party = ["ingestion", "ingest"]
WORKLOG.md                                            — edited  — appended dated entry at top per worklog rule
```

## Commands run / run IDs

- `jq . .claude/settings.json > /dev/null && echo "settings.json valid"` → `settings.json valid`
- PreToolUse hook dry-test: verification bash command containing `dbt build --target prod` was **blocked** by the live hook with message `BLOCKED: --target prod from local shell. Use GitHub Actions.` — proof the hook is active.
- `tail -n 20 /tmp/dataops-sync-rule.log` confirmed three new source IDs in the Data Ops NotebookLM notebook:
  - `staging.md` → `4a2159d0-1127-4f60-bb64-186e99802a24`
  - `warehouse.md` → `edbb2b72-9790-48e0-bdea-03d2488c716e`
  - `ingest.md` → `6a9408a8-fc25-406c-b985-1c6888788021`

## Decisions made

- **Adapted `dim_contact` identity-spine in `warehouse.md` to GHL-only anchor** for v1. Removed Calendly/Stripe/Fanbasis/Fathom bridges per the Track A prompt and D-DEE's v1 scope. Left a forward-looking paragraph so the rule still governs when cross-source bridges land.
- **Adapted `ingest.md` from Cloud Run Jobs → GitHub Actions** (`workflow_dispatch` + `schedule:`). Kept the raw-dataset contract (`raw_<source>.<source>__<object>_raw` + `_ingested_at` + JSON payload) verbatim. Trimmed source list to D-DEE's 5 (GHL + Fanbasis Python/GH Actions; Typeform + Calendly + Stripe Fivetran).
- **Did NOT port `marts.md`** from the source repo — the existing `.claude/rules/mart-naming.md` is already correct and more complete.
- **Merged prod-block hook into existing `.claude/settings.json` hooks array via `Edit`**, not `Write` — preserves SessionStart worklog-tail and PostToolUse notebook-sync hooks verbatim, as required by the track contract.
- **Kept the `no-legacy-table-refs` pre-commit hook** even though it targets a GTM-lead-warehouse pre-v2 migration. Reason: task spec said "port AS-IS" for `.pre-commit-config.yaml`; the hook is a no-op in this greenfield project and harmless.

## Unresolved risks

- [ ] **PR not yet opened.** `git add` + `git commit` + `git push` + `gh pr create` still outstanding. No shared-file edits except `.claude/settings.json` (merged into existing hooks).
- [ ] **`pre-commit install` not run.** The hooks config is committed but not locally active on any clone until someone runs `pre-commit install`. Not a blocker for PR merge.
- [ ] **`.github/pull_request_template.md` does not exist** (Track J owns). PR will use a plain description rather than the template.
- [ ] **Branch name mismatch with plan.** Track spec specifies `feat/track-a-rules-guardrails`; the Orca worktree was pre-set to `Davv5/Track-A-Rules-Guardrails`. Using the pre-set branch; rename is optional and cosmetic.

## First task for next session

**Commit Track A's changes and open the PR against `main`** — `git add` the 10 created/edited paths, commit with a Co-Authored-By trailer, `git push -u origin Davv5/Track-A-Rules-Guardrails`, then `gh pr create --base main` with the Track A summary + test-plan checklist. Confirm the PostToolUse sync log still shows the three new rule files in the notebook after the push.

## Context links

- Track spec: `/Users/david/Documents/data ops/docs/worktree-prompts/track-a.md`
- Plan reference: `/Users/david/.claude/plans/this-is-a-sorted-rabbit.md` (referenced by track spec)
- Worklog entry: `WORKLOG.md` → top entry, dated 2026-04-20
- NotebookLM notebook: https://notebooklm.google.com/notebook/7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a (three new sources confirmed in sync log)
