# Session Handover — <one-line summary>

**Branch:** `<branch-name>`
**Timestamp:** `YYYY-MM-DD_HH-mm` (local)
**Author:** <human or agent name>
**PR:** <link, or "not yet opened">

---

## Session goal

One or two sentences: what this session set out to do.

## Changed files

Exact paths, grouped by area. One line per file.

```
path/to/file.sql          — created / edited / deleted — <3-6 word note>
.claude/rules/staging.md  — edited — tightened SCD lesson
```

## Commands run / run IDs

Concrete commands and their identifiers. Include:

- `dbt build` invocations and `run_results.json` run IDs where relevant
- GitHub Actions workflow run URLs (`gh run view <id> --web`)
- Any long-running jobs (backfills, Fivetran syncs) with their start/end timestamps

## Decisions made

One bullet per decision, with a one-line *why*.

- Decided X because Y
- Deferred Z to next session because it depends on <blocker>

## Unresolved risks

What is known to be uncertain, broken, or gated on external input.

- [ ] Risk 1 — owner / waiting-on
- [ ] Risk 2 — owner / waiting-on

## First task for next session

**One actionable item.** The first thing the next session (or next operator) should do on wake-up. Not a list — one item. If there are multiple competing next-steps, pick one and note the alternatives under "Unresolved risks."

Example: `Run dbt build --target ci on feat/foo and confirm the relationships test on fct_payments.contact_sk is green.`

## Context links

- Scope doc / design doc
- Related PRs / issues
- Worklog entry (if one was appended)
- NotebookLM corpus queries referenced
