# Track Backlog — ready-to-fire work for track-executor

Self-sufficient track files that the `track-executor` agent can pick up cold.
Fire by invoking the agent and passing the track file path.

**Orchestration pattern:**
```
main session
  ↓ (hands off a track path)
track-executor (Sonnet 4.6, in a worktree)
  ↓ (local commit — does NOT push)
pr-reviewer (Opus 4.7)
  ↓ (review + WORKLOG/handover gate + push + PR)
David reviews PR in Orca → merge
```

## The 6-step critical path (NOT a backlog item — this is what David is focused on)

| # | Step | Owner | Status |
|---|---|---|---|
| 1 | Metabase scaffold PR | David + Claude | ✅ PR #34 open |
| 2 | `terraform apply` | David | ⬅ next |
| 3 | Upload runtime assets + BQ key + reboot VM | David | blocked on #2 |
| 4 | Metabase setup wizard + API key + `bigquery_connection.py` | David | blocked on #3 |
| 5 | Fill in `speed_to_lead.py` cards → Page 1 live | Claude (main session) | blocked on #4 |
| 6 | `lead_journey.py` + `revenue_detail.py` → Pages 2 + 3 | Claude (main session) | blocked on #5 |

**Keep these on the main session — they're sequential, each step informs the next, and David wants tight feedback loops on the dashboard shape.**

## Backlog — delegable to agents

These can fire in parallel while the critical path advances. Ordered by recommended fire sequence, not dependency (most are independent of each other).

| Track | Scope | Status | Gate / blocker |
|---|---|---|---|
| [N — Evidence decommission](Davv5-Track-N-Evidence-Decommission-2026-04-21_10-04.md) | Delete `dashboards/evidence/` + evidence-preview workflow; repo cleanup | **Ready** | — |
| [O — Stale GH secrets cleanup](Davv5-Track-O-Stale-GH-Secrets-Cleanup-2026-04-21_10-04.md) | Delete `GHL_API_KEY` + `GHL_LOCATION_ID` GH secrets (superseded by Secret Manager) | **Ready** | — |
| [P — Slack webhook fix](Davv5-Track-P-Slack-Webhook-Fix-2026-04-21_10-04.md) | Fix Track K's Slack failure-alert in 3 workflows | **Ready** | Requires `SLACK_WEBHOOK_URL` secret to be set — if missing, executor will stop-and-ask |
| [Q — Release-gate severity flip](Davv5-Track-Q-Release-Gate-Severity-Flip-2026-04-21_10-04.md) | Flip 3 release-gate tests from `'warn'` back to `'error'` | **⚠ Gated** | All 3 marts must be within ±5% of oracle for 1 week first. Do not fire until data has caught up. |
| [R — dim_contacts enrichment](Davv5-Track-R-DimContacts-Enrichment-2026-04-21_10-04.md) | Populate first_touch_*, last_touch_*, lead_magnet_first_engaged, full_name on `dim_contacts` | **Ready with stop-points** | UTM field history and lead-magnet tag taxonomy may require David's input mid-execution |
| [S — WORKLOG → index refactor](Davv5-Track-S-Worklog-Index-Refactor-2026-04-21_10-04.md) | Replace tail-injection with curated project-state index at SessionStart | **Ready** | — |
| [T — Corpus config decouple](Davv5-Track-T-Corpus-Config-Decouple-2026-04-21_10-04.md) | Add `.claude/corpus.yaml`; `ask-corpus` reads from it (portability for future clients) | **Ready** | — |

## Recommended fire order

If you want to offload the full backlog in one sweep:

**Wave 1 — safe, independent, no user input needed** (fire in parallel, 3 worktrees)
- N (Evidence decommission)
- O (GH secrets cleanup)
- T (Corpus config decouple)

**Wave 2 — independent but may need 1 stop-and-ask**
- P (Slack fix) — verify webhook secret exists first
- S (WORKLOG refactor) — benefits from reviewing N's commit pattern first
- R (dim_contacts enrichment) — 2 mid-execution stop-points for domain questions

**Wave 3 — gated**
- Q (Severity flip) — fire only after data catches up

## Invoking the executor

From the main Claude Code session:

```
Use the track-executor agent, isolation: "worktree", with this prompt:
  "Execute docs/handovers/Davv5-Track-N-Evidence-Decommission-2026-04-21_10-04.md"
```

The executor opens the track, works through the tasks, commits locally, reports back with `Ready for review: yes`. Then hand that report to pr-reviewer.

## Invoking the reviewer

After executor reports back:

```
Use the pr-reviewer agent with this prompt:
  "Review branch <branch-name> (commit <hash>). Track file:
   docs/handovers/Davv5-Track-N-Evidence-Decommission-2026-04-21_10-04.md.
   Executor report: <paste executor's final report>"
```

Reviewer classifies Clean / Push-with-notes / Request-changes. On Clean or Push-with-notes, pushes + opens PR. On Request-changes, returns a fix list you hand back to the executor.

## Adding a new track to the backlog

1. Invoke `plan-architect` with the high-level goal ("clean up X," "add feature Y")
2. Architect writes a new file at `docs/handovers/Davv5-Track-<Letter>-<slug>-<timestamp>.md`
3. Add a row to this table

Letters N through T are used. Next available: U, V, W, …

## What this pattern does NOT handle

- **Merge cascade / rebase conflicts** when multiple parallel tracks land. David resolves in Orca (or a future `merge-conductor` agent — out of scope for now).
- **Infra operations** like `terraform apply`, `gcloud iam keys create`. Those stay human-operated by design — the executor agent's hard limits forbid touching prod.
- **Client-facing decisions** (metric definitions, scope, stakeholder tradeoffs). Architect flags these as stop-and-ask open questions; David resolves.
