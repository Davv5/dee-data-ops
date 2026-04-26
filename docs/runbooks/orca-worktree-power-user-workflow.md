# Orca Worktree Power User Workflow

Purpose: keep Orca's parallel-agent workflow fast without letting branches,
worktrees, and uncommitted files become invisible debt.

Orca is worktree-native: each task gets its own checkout and branch. That is the
right shape for parallel agent work, but it needs a retirement protocol.

## Mental model

There are four separate things:

| Thing | What it is | What moves it |
|---|---|---|
| `main` | local trunk branch | `git switch main && git pull --ff-only origin main` |
| `origin/main` | latest GitHub trunk snapshot | `git fetch origin` |
| task branch | one unit of agent/user work | commit/push/merge |
| worktree | a folder checked out at a branch | `git worktree add/remove` or Orca |

Deleting a PR branch on GitHub does not remove the local branch. Merging a PR
does not remove the Orca worktree. That cleanup is an explicit final step.

## Non-negotiables

- The control-room repo at `/Users/david/Documents/data ops` stays clean except
  for intentional triage work.
- One unit of work = one branch + one worktree + one PR.
- If a branch has no PR and no uncommitted work, it is a scratch branch and
  should be removed or deliberately promoted.
- Do not start new agent work from stale local `main`; fetch first and branch
  from `origin/main` or update local `main`.
- No orphaned worktrees older than 7 days unless the branch name or PR explains
  why it is parked.

## Branch naming

Use boring, sortable names:

```text
codex/YYYY-MM-DD-short-purpose
orca/YYYY-MM-DD-short-purpose
chore/YYYY-MM-DD-short-purpose
fix/YYYY-MM-DD-short-purpose
feat/YYYY-MM-DD-short-purpose
```

Examples:

```text
codex/2026-04-26-orca-worktree-hygiene
fix/2026-04-26-fanbasis-source-yaml
chore/2026-04-26-discovery-docs
```

Avoid multiple names for the same effort (`u4a`, `Phase-three`,
`feat/U4a-plumbing-parity`) unless they are genuinely different PRs.

## Daily start

Run this in the control-room repo:

```bash
cd "/Users/david/Documents/data ops"
.claude/scripts/orca-worktree-audit.sh --fetch
```

Then decide:

1. **Dirty current workspace?** Commit, split, or stash before switching.
2. **Old dirty worktree?** Open it and either finish, stash, or archive the diff.
3. **Clean merged worktree?** Remove the worktree, then delete the branch.
4. **Clean unmerged branch?** Check whether it has a PR. If no PR, either open
   one or mark it disposable.

## Starting work

Preferred flow:

```bash
cd "/Users/david/Documents/data ops"
git fetch --prune origin
git switch main
git pull --ff-only origin main
git switch -c codex/$(date +%Y-%m-%d)-short-purpose
```

If using Orca, create the task from the updated trunk and name the worktree after
the branch purpose, not after a conversation title.

## Ending work

Before leaving a task:

```bash
git status --short --branch
git diff --stat
git add <intentional-files>
git commit -m "<type>(<scope>): <what changed>"
git push -u origin <branch>
```

Open or update the PR. Once the PR merges:

```bash
cd "/Users/david/Documents/data ops"
git fetch --prune origin
git worktree remove "<worktree-path>"
git branch -d "<branch>"
```

If `git branch -d` refuses because it cannot prove the branch is merged, inspect
first:

```bash
git log --oneline origin/main.."<branch>"
```

Only use `git branch -D` after that command shows no unique work you still need.

## Weekly cleanup

Run:

```bash
cd "/Users/david/Documents/data ops"
.claude/scripts/orca-worktree-audit.sh --fetch
git branch --merged origin/main
git branch -vv
```

Cleanup order:

1. Remove clean worktrees whose branch is merged into `origin/main`.
2. Delete their local branches.
3. Review branches with upstream `gone`.
4. Prune leftover worktree metadata:

```bash
git worktree prune
```

## Current repo triage order

For the current April 2026 state, do this in order:

1. Protect the uncommitted work on `chore/triage-2026-04-23`.
2. Push its existing committed change if it is intentional.
3. Commit the discovery/rules/docs packet in logical slices.
4. Update local `main`.
5. Retire old Orca worktrees and stale `Davv5/...` branches using the audit
   script.
