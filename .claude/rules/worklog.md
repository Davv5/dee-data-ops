---
paths: ["**/*"]
---

# End-of-session routing — WORKLOG is the destination of last resort

This project has two state artifacts plus a routing discipline:

- **`.claude/state/project-state.md`** — the curated **snapshot index**. Auto-injected at session start via the `SessionStart` hook. Answers *"what is true right now?"* Target 40–60 lines. Constantly overwritten.
- **`WORKLOG.md`** — the append-only audit log, **but only for content that doesn't fit any other destination**. Most sessions don't need a new entry. Newest entry on top.
- **End-of-session routing** (this rule) — the discipline of asking *"where does this thing belong?"* before defaulting to WORKLOG.

Anthropic's official Claude Code guidance (verified 2026-04-26) does not recommend WORKLOG-style append-only session logs. They rely on GitHub (commits + PR descriptions) + auto-memory + path-scoped rules + (optionally) a session-start state index. This rule encodes that lean position for this project: WORKLOG fires only when nothing else captures the content.

## End-of-session routing table

For each thing this session produced, route it to its **real home**. Skip the WORKLOG entry if any of the rows below already captured it.

| Content type | Right destination | Captures it for future-Claude? |
|---|---|---|
| Code that shipped | PR description + commits + `git log` | ✓ Yes |
| New convention or pattern | `.claude/rules/*.md` (auto-syncs to Data Ops notebook via PostToolUse hook) | ✓ Yes |
| Cross-session learning Claude discovered | Auto-memory (`MEMORY.md` + per-fact files in `~/.claude/projects/.../memory/`) | ✓ Yes |
| Standalone decision worth preserving (architecture, scope cut, tool selection) | `docs/decisions/YYYY-MM-DD-slug.md` (ADR-style — see `docs/decisions/README.md`) | ✓ Yes |
| Research or exploration that didn't ship | `docs/discovery/<topic>.md` OR a GitHub issue | ✓ Yes |
| Engagement-specific scope / oracle / locked metric | `CLAUDE.local.md` (gitignored overlay) | ✓ Yes (for David's local context) |

## Project-state index regeneration is independent

`.claude/state/project-state.md` is regenerated on its own trigger, **not** as part of WORKLOG bookkeeping. The trigger is simple:

> Regenerate `.claude/state/project-state.md` if any merged PR or material state change is newer than the file's last edit.

You can — and usually should — regenerate the project-state index even when no WORKLOG entry is appended. They serve different purposes: WORKLOG narrates *what happened*; the index describes *what is true now*.

Required sections in `project-state.md`, in order:

1. **Where we are** — current phase, active branch, last PR merged, headline metric anchor.
2. **Last 3 decisions** — one line each. Link the destination that captured each (PR URL, ADR file, memory file).
3. **Open threads** — what's pending / blocked / waiting on the client.
4. **Where to look** — retrieval map: file paths and grep patterns the agent reaches for on demand.

Keep the index 40–60 lines (excluding the `_meta` section described below). Drop anything that stopped being true.

## When DOES WORKLOG fire?

Append a WORKLOG entry only if **all** of the following are true:

1. Something happened this session that's worth a future agent finding.
2. None of the destinations in the routing table captured it.
3. It doesn't fit a *new* destination type that you should be defining instead.

If WORKLOG fires often, that's a signal to add a new destination type to the routing table — not to loosen the trigger.

### Common cases that DO get WORKLOG entries

- Session was research-only and didn't produce a discovery doc, ADR, memory, rule, or PR — but reached a non-obvious finding worth preserving.
- A meaningful client conversation produced new facts or changed direction, and no PR / ADR / memory captures the change.
- Discovery of a blocker or open thread that doesn't yet have a home (and won't fit project-state's "Open threads").
- Multi-session arc where the *cumulative* narrative is the load-bearing artifact, not any individual PR.

### Common cases that DON'T get WORKLOG entries

- Session shipped a PR with a thorough description. The PR is the audit log.
- Session created/updated a `.claude/rules/*.md` file. The rule is the artifact.
- Session created/updated a memory. The memory is self-documenting.
- Session created an ADR under `docs/decisions/`. The ADR is the artifact.
- Routine dbt runs, typo fixes, in-session iteration that lands in a later "shipped" PR.

## When you skip WORKLOG, log the skip-reason in project-state

When you regenerate `.claude/state/project-state.md` without appending a new WORKLOG entry, include a one-line skip-reason at the bottom of the index under a `_meta` section:

```markdown
## _meta

- Last regen: 2026-04-26 evening
- WORKLOG: skipped — all session output captured in PRs #76, #77, #78 + memory swap (project_bi_direction_dabi.md)
```

Skip-reasons accumulate as evidence the routing is working. If you ever look back and wish a skipped session had logged something, that's the signal to **evolve a destination type**, not to loosen the WORKLOG trigger.

## WORKLOG entry format (when it does fire)

```markdown
## YYYY-MM-DD — <one-line summary>

**What happened**
- Tight bullets of what actually shipped or changed
- Link to files / commits / PRs — do not paste code or diffs

**Decisions** (only if not captured in an ADR, memory, or PR description)
- "Decided X because Y" — with the *why* on any non-obvious choice

**Open threads**
- Work that is known-pending but not yet done
- Blockers, questions waiting on the client, unknowns
```

- Newest entry at the top of the file, immediately under the `---` separator.
- Keep entries under ~30 lines; if bigger, the work probably deserves its own scope/design doc.

## Proactive update norm

At the end of a work session, walk the routing table once and ask: did any of the rows fire? If yes, the content is captured — skip WORKLOG and just regenerate project-state. If no row fired but something IS worth remembering, append a WORKLOG entry. Always regenerate project-state when state has changed.

Don't update silently during every tool call; wait until the session has a natural endpoint or the user signals wrap-up.

## What NOT to put anywhere in this project

- Code, diffs, long prose → live in the actual files or PR descriptions.
- User profile / preferences → memory system, not WORKLOG or project-state.
- Convention-level rules → `.claude/rules/*.md`, not WORKLOG or project-state.
- Secrets, credentials, PII → never, anywhere in the repo.

## Why this rule exists in this shape

The earlier version of this rule fired on "any session where material work happened — even if the above didn't trigger." That produced ceremony on PR-heavy sessions where everything was already in GitHub. Anthropic's own Claude Code guidance leans on git + auto-memory and doesn't recommend WORKLOG-style logs. This rule aligns with that stance while preserving WORKLOG as a real destination for the residual case (research, cross-session arcs, blockers without a home). The skip-reason metadata creates a self-correcting feedback loop: WORKLOG firing rate becomes evidence about whether the routing table is complete.
