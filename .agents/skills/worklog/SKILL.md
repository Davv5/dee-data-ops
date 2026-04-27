---
name: worklog
description: End-of-session bookkeeping — append a new dated WORKLOG.md entry AND regenerate .Codex/state/project-state.md based on recent git activity, then open a chore PR. Use when the user says "wrap up", "log the session", "update the worklog", "end of session"; when a session has produced merged PRs or material decisions that aren't yet in WORKLOG.md; or when .Codex/state/project-state.md looks stale relative to git history. Grounded in .Codex/rules/worklog.md.
---

# Worklog skill — end-of-session bookkeeping

Appends a dated WORKLOG.md entry + regenerates the project-state index + opens a tiny chore PR, in one pass. Solves the "I forgot to update the log" failure mode.

## When to invoke

- User says: "wrap up", "log the session", "update the worklog", "end of session".
- You notice the session has merged PRs or material decisions that aren't yet in WORKLOG.md.
- `.Codex/state/project-state.md` references a PR number or phase that's no longer current.

## When to skip

- Trivial in-session iteration with no commits landed and no decisions made.
- Unmerged PRs still in flight — better to wait until they merge so the entry names the landed state.

## Workflow

### Step 1 — figure out what's new since the last WORKLOG entry

```bash
cd "$(git rev-parse --show-toplevel)"
LAST_DATE=$(grep -m 1 '^## [0-9]' WORKLOG.md | sed 's/^## \([0-9-]*\).*/\1/')
echo "Last WORKLOG entry: $LAST_DATE"
echo "--- commits on main since then (excluding chore: worklog) ---"
git log --oneline --since="$LAST_DATE" --grep '^chore: worklog' --invert-grep main | head -40
echo "--- PRs merged since then ---"
gh pr list --state merged --search "merged:>=$LAST_DATE" --limit 15
echo "--- open PRs ---"
gh pr list --state open --limit 10
```

### Step 2 — draft the WORKLOG entry

Follow `.Codex/rules/worklog.md`'s format exactly:

```markdown
## YYYY-MM-DD — <one-line summary>

**What happened**
- Tight bullets of what shipped or changed.
- Link to files/commits/PRs — do not paste code or diffs.

**Decisions**
- "Decided X because Y" — with the *why* on any non-obvious choice.

**Open threads**
- Work that is known-pending but not yet done.
- Blockers, questions waiting on the client, unknowns.
```

Constraints:
- Keep the entry under ~30 lines total (per `.Codex/rules/worklog.md`). If bigger, the work deserves its own scope/design doc that this entry links to.
- Bullets tight — log, not narrative.
- No code, no diffs, no multi-paragraph prose.
- **Newest entry at the top of the file**, immediately under the `---` separator (use an `Edit` targeting the existing `---\n\n## <prev-date>` block).

### Step 3 — regenerate the project-state index

Overwrite `.Codex/state/project-state.md`. Target 40-60 lines. Required sections in order:

1. **Where we are** — current phase, active branch, last PR merged, headline metric anchor, public dashboard URL if applicable.
2. **Last 3 decisions** — one-liner each with a `grep` hint pointing at the full WORKLOG entry.
3. **Open threads** — what's pending / blocked / waiting on the client.
4. **Where to look** — retrieval map: file paths and grep patterns for on-demand lookup.

Drop anything that stopped being true. Do not grow it into a second worklog.

### Step 4 — commit + branch + PR

```bash
DATE=$(date +%Y-%m-%d)
git checkout -b "chore/worklog-$DATE" main
git add WORKLOG.md .Codex/state/project-state.md
git commit -m "chore: worklog + project-state for $DATE"
git push -u origin "chore/worklog-$DATE"
gh pr create --base main --head "chore/worklog-$DATE" \
  --title "chore: worklog + project-state for $DATE" \
  --body "Routine bookkeeping. Prepends the $DATE WORKLOG entry + regenerates \
.Codex/state/project-state.md to match current repo state. \
No functional/code changes."
```

### Step 5 — report

Output to the user:
- PR URL
- The one-line summary of the WORKLOG entry you wrote
- Anything you noticed that looks like an open thread they should act on

## Guardrails

- Do NOT commit to `main` directly — use `chore/worklog-<date>` branch per `AGENTS.md`'s "always branch" rule.
- Do NOT push or open a PR if the working tree has unrelated uncommitted changes — stash them first, do the worklog PR cleanly, then pop.
- If two WORKLOG entries would land on the same date (e.g., a second wrap within the same day), UPDATE the existing entry rather than creating a second.
- Never paste secrets, credentials, or PII into either artifact.

## Cost

Free. No LLM calls, no Perplexity quota. Pure filesystem + git + gh operations.
