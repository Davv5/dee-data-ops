---
name: worklog
description: End-of-session routing — walk the destinations table from `.claude/rules/worklog.md` to determine what's already captured and where unanchored content (if any) should land. Always regenerate `.claude/state/project-state.md` when state has changed; only append a `WORKLOG.md` entry when no other destination fits. Use when the user says "wrap up", "log the session", "end of session", or "update the worklog"; or when a session has produced merged PRs, decisions, or research findings that may need a destination.
---

# Worklog skill — end-of-session routing

This skill is the operational counterpart to `.claude/rules/worklog.md`. It walks the routing table at session-end, decides what (if anything) needs an entry in `WORKLOG.md`, regenerates `.claude/state/project-state.md` independently, and opens a single chore PR with whatever changed.

The headline behavior change from earlier versions: **WORKLOG is no longer the default destination.** Most sessions ship PRs / rules / memories / ADRs that already capture what happened — and on those sessions this skill regenerates project-state but skips the WORKLOG entry. WORKLOG fires only on the residual case (research, cross-session arcs, blockers without a home).

## When to invoke

- User says: "wrap up", "log the session", "update the worklog", "end of session".
- You notice the session has produced merged PRs, an ADR, a memory swap, or a meaningful state change that's not yet reflected in `.claude/state/project-state.md`.

## When to skip

- Trivial in-session iteration with no commits landed and no decisions made.
- Unmerged PRs still in flight — better to wait until they merge so project-state names the landed state.

## Workflow

### Step 0 — drift check (unmerged feature branches)

Before drafting anything, scan local branches for unmerged feature work that should be PR'd before "session done" makes sense. Carry findings into Step 5's report.

```bash
cd "$(git rev-parse --show-toplevel)"
git fetch origin main --quiet 2>/dev/null || true
echo "--- local branches ahead of origin/main, with PR state ---"
for branch in $(git for-each-ref --format='%(refname:short)' refs/heads/); do
  case "$branch" in main|master) continue ;; esac
  ahead=$(git rev-list --count "origin/main..$branch" 2>/dev/null || echo 0)
  [ "$ahead" -gt 0 ] || continue
  pr_json=$(gh pr list --head "$branch" --state all --json number,state,url --limit 1 2>/dev/null || echo '[]')
  pr_state=$(printf '%s' "$pr_json" | jq -r '.[0].state // "NONE"')
  pr_url=$(printf '%s' "$pr_json" | jq -r '.[0].url // ""')
  printf "  %-50s ahead=%-3s pr=%-7s %s\n" "$branch" "$ahead" "$pr_state" "$pr_url"
done
```

Treat these as drift findings:

- `pr=NONE` and ahead of main → unmerged work with no PR open. Surface as "REMAINING TO SHIP."
- `pr=OPEN` → PR exists, awaiting review/merge. Surface as "AWAITING MERGE."
- `pr=MERGED` and still ahead of main → branch wasn't deleted post-merge; safe but worth surfacing.

Do NOT auto-open feat PRs from this skill — the user opens those.

### Step 1 — gather what's new since the last project-state regen

```bash
cd "$(git rev-parse --show-toplevel)"
LAST_REGEN=$(grep -m 1 'Last regen:' .claude/state/project-state.md | sed 's/.*Last regen: //' | head -c 10)
echo "Last project-state regen: ${LAST_REGEN:-<not recorded>}"
echo "--- commits on main since then ---"
git log --oneline --since="${LAST_REGEN:-7 days ago}" --grep '^chore: worklog' --invert-grep main | head -40
echo "--- PRs merged since then ---"
gh pr list --state merged --search "merged:>=${LAST_REGEN:-2026-04-01}" --limit 15
echo "--- open PRs ---"
gh pr list --state open --limit 10
```

### Step 2 — walk the routing table

For each thing this session produced, route it. Mark each row with where it lives.

| Content | Destination | Where it landed (URL / path) |
|---|---|---|
| Code that shipped | PR description + commits + `git log` | PR #XX, PR #YY |
| New convention | `.claude/rules/*.md` | (path or N/A) |
| Cross-session learning | Auto-memory | (memory file or N/A) |
| Standalone decision | `docs/decisions/YYYY-MM-DD-slug.md` | (path or N/A) |
| Research / exploration | `docs/discovery/<topic>.md` or GitHub issue | (path or N/A) |
| Engagement-specific scope | `CLAUDE.local.md` | (gitignored, mention only) |

If every row is captured → **skip the WORKLOG entry.** Proceed to Step 3.
If something happened that none of the rows captured → consider whether it needs a *new* destination type (file an issue / add to the routing table). If it genuinely belongs in WORKLOG, draft an entry per the format in `.claude/rules/worklog.md`.

### Step 3 — regenerate `.claude/state/project-state.md`

Always do this when state has changed (merged PR, new memory, new ADR, new rule, scope shift). Independent of whether WORKLOG fires.

Required sections in order:

1. **Where we are** — current phase, active branch, last PR merged, headline metric anchor.
2. **Last 3 decisions** — one line each. Link the destination that captured each (PR URL, ADR file, memory file).
3. **Open threads** — what's pending / blocked / waiting on the client.
4. **Where to look** — retrieval map: file paths and grep patterns.

End the file with a `## _meta` section. If it doesn't exist yet, append it; otherwise update the existing block in place rather than creating a second one.

```markdown
## _meta

- Last regen: YYYY-MM-DD <morning|afternoon|evening>
- WORKLOG: <appended entry "<one-line summary>" | skipped — <one-line skip reason>>
```

The skip-reason is the self-correcting feedback loop. Examples:
- `WORKLOG: skipped — all session output captured in PRs #76, #77, #78 + memory swap`
- `WORKLOG: appended entry "research session — vendor API surface mapping (no PR)"`

Drop content from the index that stopped being true. Do not grow it past 60 lines (excluding `_meta`).

### Step 4 — commit + branch + PR

```bash
DATE=$(date +%Y-%m-%d)

# Branch from origin/main (not local main — local main may be checked out
# in a sibling worktree, which makes `git checkout -b ... main` fail).
git fetch origin main --quiet
git checkout -b "chore/state-$DATE" origin/main

# Always stage the project-state regen.
git add .claude/state/project-state.md

# Stage WORKLOG.md only if Step 2 actually modified it. The check is the
# real signal (file diff vs HEAD), not a manually-set sentinel variable.
if ! git diff --quiet -- WORKLOG.md; then
  git add WORKLOG.md
  WORKLOG_NOTE="Also appends a WORKLOG entry for content not captured by other destinations."
else
  WORKLOG_NOTE="No WORKLOG entry — all session output captured by other destinations (see project-state _meta skip-reason)."
fi

git commit -m "chore: project-state for $DATE"
git push -u origin "chore/state-$DATE"

# Compute the body in bash before passing to gh — avoids the
# escaped-dollar-in-double-quotes pitfall.
PR_BODY="Routine bookkeeping. Regenerates .claude/state/project-state.md to match current repo state. $WORKLOG_NOTE No functional/code changes."

gh pr create --base main --head "chore/state-$DATE" \
  --title "chore: project-state for $DATE" \
  --body "$PR_BODY"
```

Naming change from earlier versions: branches are `chore/state-<date>` (not `chore/worklog-<date>`) since project-state regen is the always-on action and WORKLOG is now optional.

### Step 5 — report

Output to the user:
- PR URL (the chore PR you just opened).
- Routing-table summary: which rows fired and where each thing landed.
- The one-line skip-reason or appended-entry summary written to project-state's `_meta`.
- **REMAINING TO SHIP** — branches from Step 0 with `pr=NONE` and ahead of main.
- **AWAITING MERGE** — branches with `pr=OPEN` from Step 0.
- Anything else that looks like an open thread the user should act on.

## Guardrails

- Do NOT commit to `main` directly — branch via `chore/state-<date>` per `CLAUDE.md`.
- Do NOT push or open a PR if the working tree has unrelated uncommitted changes — stash them first, do the bookkeeping PR cleanly, then pop.
- If two regens would land on the same date, UPDATE the existing `_meta` block (and the existing entry, if WORKLOG fired) rather than creating a second.
- Never paste secrets, credentials, or PII into either artifact.

## When in doubt about a content destination

Default-skip WORKLOG. The skill is biased toward "capture it in its real home and move on." If you're unsure whether a piece of content fits another destination, the safer move is to *create that destination* (an ADR, a discovery doc, a memory) rather than fall through to WORKLOG. Falling through is the last resort, not the safety net.

## Cost

Free. No LLM calls, no Perplexity quota. Pure filesystem + git + gh operations.
