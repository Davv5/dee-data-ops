---
paths: ["**/*"]
---

# Keeping the Worklog Current

This project has a rolling worklog at `WORKLOG.md` (repo root). The tail (last 200 lines) is injected into Claude Code's context at every session start via the `SessionStart` hook in `.claude/settings.json`. That's how future sessions understand "the present moment" without the user having to re-explain.

**If the worklog is stale, future-Claude's context is stale.** Keeping it current is part of the work, not overhead.

## When to append an entry

Append a new dated entry when any of the following happens:

- A deliverable ships (scope doc, rule file, model layer, dashboard, PR merged)
- A non-trivial decision is made (architecture choice, scope cut, tool selection, naming convention)
- A meaningful client conversation produces new facts or changed direction
- A blocker or open thread is discovered that will outlive the current session
- At the end of any session where material work happened — even if the above didn't trigger

**Do not append for:** small edits, typo fixes, iterative in-session tweaks that get captured in a later "shipped" entry, or routine dbt runs.

## Entry format

```markdown
## YYYY-MM-DD — <one-line summary>

**What happened**
- Tight bullets of what actually shipped or changed
- Link to files/commits/PRs — do not paste code or diffs

**Decisions**
- Decisions made, with a one-line *why* when non-obvious
- "Decided X because Y" — not just "decided X"

**Open threads**
- Work that is known-pending but not yet done
- Blockers, questions waiting on the client, unknowns
```

- Newest entry at the top of the file, immediately under the `---` separator
- One entry per session (not per task) — bundle related bullets
- Keep entries under ~30 lines; if bigger, the work probably deserves its own scope/design doc

## What NOT to put in the worklog

- Code, diffs, long prose → live in the files or PR descriptions
- User profile / preferences → memory system, not the worklog
- Convention-level rules → `.claude/rules/*.md`, not the worklog
- Secrets, credentials, PII → never, anywhere in the repo

If an entry is tempted to grow large, split the underlying artifact out as its own file and link to it from the worklog.

## Proactive update norm

At the end of a work session — especially one where files were created, scope shifted, or a decision was made — append an entry before finishing, without waiting to be asked. Do not append silently during every tool call; wait until the session has a natural endpoint or the user signals wrap-up.

When appending, read `WORKLOG.md` first (it may already have a today-dated entry to update rather than a new one to create).
