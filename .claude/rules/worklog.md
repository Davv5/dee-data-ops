---
paths: ["**/*"]
---

# Keeping the Worklog Current

This project has two state artifacts that serve different purposes:

- **`WORKLOG.md`** (repo root) — the append-only audit log. Chronological. Answers *"what just happened?"* Never pruned.
- **`.claude/state/project-state.md`** — the curated project-state **index**. Stateful snapshot. Answers *"what is true right now?"* Constantly overwritten. Target 40–60 lines.

At session start, the index (not the worklog tail) is injected into Claude Code's context via the `SessionStart` hook in `.claude/settings.json`, which runs `.claude/scripts/sessionstart-inject-state.sh`. Agents then `Read` the index to orient and `Grep WORKLOG.md` for history on demand.

Why the split: the index stays bounded as the project ages. Pattern mirrors the path-scoped rule loader in this repo — Claude Code loads a rule file "when a file matching that path is opened", not pre-loaded every turn (source: `CLAUDE.md` "Claude Code Rules" section, Data Ops notebook). Same retrieval-over-injection idea for project state: pre-load a lightweight map, retrieve the deep history only when a task needs it.

**If either artifact is stale, future-Claude's context is stale.** Keeping them current is part of the work, not overhead.

## When to append a WORKLOG entry

Append a new dated entry when any of the following happens:

- A deliverable ships (scope doc, rule file, model layer, dashboard, PR merged)
- A non-trivial decision is made (architecture choice, scope cut, tool selection, naming convention)
- A meaningful client conversation produces new facts or changed direction
- A blocker or open thread is discovered that will outlive the current session
- At the end of any session where material work happened — even if the above didn't trigger

**Do not append for:** small edits, typo fixes, iterative in-session tweaks that get captured in a later "shipped" entry, or routine dbt runs.

## WORKLOG entry format

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

## Project-state index format

`/Users/david/Documents/data ops/.claude/state/project-state.md` is the file. Required sections, in order:

1. **Where we are** — current phase, active branch, last PR merged, headline metric anchor
2. **Last 3 decisions** — one line each, with a `grep` hint pointing at the full WORKLOG entry
3. **Open threads** — what's pending / blocked / waiting on the client
4. **Where to look** — retrieval map: file paths and `grep` patterns the agent reaches for on demand

Keep the index 40–60 lines. Drop anything that stopped being true. Do not grow it into a second worklog.

## End-of-session regeneration

The index is regenerated **manually** at session end (v1 — EndSession hooks are not a confirmed Claude Code feature, so no automated fire-and-forget yet). Run:

```bash
# From the repo root. Reads the newest ~300 lines of WORKLOG.md and gives you
# raw material to distill into the index; overwrite `.claude/state/project-state.md`
# with a fresh ≤60-line snapshot following the format above.
tail -n 300 WORKLOG.md | pbcopy   # or: less, or pipe into your editor of choice
$EDITOR .claude/state/project-state.md
```

Rule of thumb: if the WORKLOG gained a new entry this session, the index almost certainly needs a refresh (decisions move, open threads close, phase advances). If the WORKLOG is unchanged and the index still reads true, skip it.

Smoke-test the hook after editing:

```bash
bash .claude/scripts/sessionstart-inject-state.sh | jq -r '.hookSpecificOutput.additionalContext' | head -n 20
```

## What NOT to put in either artifact

- Code, diffs, long prose → live in the files or PR descriptions
- User profile / preferences → memory system, not the worklog or index
- Convention-level rules → `.claude/rules/*.md`, not the worklog or index
- Secrets, credentials, PII → never, anywhere in the repo

If an entry is tempted to grow large, split the underlying artifact out as its own file and link to it from the worklog.

## Proactive update norm

At the end of a work session — especially one where files were created, scope shifted, or a decision was made — append a WORKLOG entry **and** refresh the project-state index before finishing, without waiting to be asked. Do not update silently during every tool call; wait until the session has a natural endpoint or the user signals wrap-up.

When appending, read `WORKLOG.md` first (it may already have a today-dated entry to update rather than a new one to create).
