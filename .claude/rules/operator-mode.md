---
paths: ["**/*"]
---

# Operator Mode

This is David's default operating agreement for this repo. Load it before
planning, coding, modeling, dashboarding, Notion work, or cleanup.

## North star

We use AI to help businesses make more money.

That means the work is judged by whether it helps the client see revenue leaks,
sales execution gaps, pipeline movement, follow-up quality, or the next action
that can improve the business.

## How we move

- One home, one next move. Do not create parallel planning surfaces unless the
  user explicitly asks for them.
- Speed matters, but only with receipts: ship small, verify against live data,
  push the branch, and leave the next agent with a clear truth trail.
- Prefer real operating surfaces over speculative architecture. A working
  dashboard, live BigQuery query, or deployed job beats another plan.
- Keep the tone human and direct. This is an agency with close collaborators,
  not a corporate PMO. Always vibes, always serving, still rigorous.
- Do not hide data gaps behind polished UI. Label truth plainly: unknown rep,
  needs mapping, stale mart, legacy project, source mismatch.
- Keep dashboards compact and useful on first view. Long scroll is a smell
  unless drill-down detail genuinely needs it.
- Make numbers answer "of what?", "who?", "worked by whom?", "what leaked?",
  and "what should we do next?"
- Avoid deletion as the first cleanup move. Label truth, archive stale paths,
  and only remove once ownership and replacement are clear.

## First-read order

At the start of a session, read these before making assumptions:

1. `CLAUDE.md`
2. `.claude/rules/operator-mode.md`
3. `.claude/state/project-state.md`
4. `AGENTS.md`
5. Any path-scoped rule under `.claude/rules/` for files being touched
6. Current `git status` and the active branch

If these disagree, trust the newest verified artifact: live code, live data,
recent commits, current PR, and then this operator mode. Update the stale
artifact instead of letting the contradiction persist.

## Data and dashboard defaults

- For marts and dbt work, follow the three-layer truth: staging cleans one source
  table at a time, warehouse models facts and dimensions, marts present business
  answers.
- For live dashboards, BigQuery-backed truth is the default. If the dashboard is
  using temporary report tables while the durable mart stabilizes, say that
  plainly in the data contract.
- Every dashboard should make the business question obvious without requiring
  the user to know the pipeline internals.
- Clickable/drill-down detail is good when it exposes source rows, identities,
  examples, cohorts, or the reason behind a metric.

## Current client operating focus

Speed-to-Lead proved the pace: source data to live operating dashboard quickly,
with attribution truth improved instead of hand-waved.

The next work should keep that pattern:

- pick the highest-revenue question,
- verify source truth,
- model only what is needed,
- ship the surface,
- then improve attribution and drill-down depth.
