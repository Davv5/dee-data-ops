# Operator Fast Loop

Purpose: make David's day-to-day workflow fast enough to trust. This is the
short operating loop that sits above the deeper Orca, corpus, dbt, and worklog
rules.

Use this when starting a session, choosing the next task, or turning a loose
idea into a clean PR.

## Mental model

The system has four jobs:

| Layer | Job | Primary files |
|---|---|---|
| Control room | Know what is true right now | `.claude/state/project-state.md`, `WORKLOG.md` |
| Knowledge base | Answer why/how before building | `.claude/corpus.yaml`, `.claude/rules/using-the-notebook.md` |
| Execution | Isolate work into one branch/worktree/PR | `docs/runbooks/orca-worktree-power-user-workflow.md` |
| Delivery | Verify, merge, and retire work | `.github/workflows/`, `docs/handovers/`, `WORKLOG.md` |

The fast loop is not a new process. It is the shortest path through the process
that already exists.

## Start of session

Run from the control-room repo:

```bash
cd "/Users/david/Documents/data ops"
.claude/scripts/orca-worktree-audit.sh
git status --short --branch
```

Then classify the session into exactly one mode:

| Mode | Use when | First move |
|---|---|---|
| Cleanup | old worktrees, stale branches, or untracked files hide the real state | inspect, protect, retire |
| Discovery | the active deliverable is a decision doc or source/business map | edit docs only |
| Build | the roadmap names the model, grain, PK, and blocker status | create one task branch/worktree |
| Review | a branch already has a local commit | compare to the track, test, push/PR |

Do not mix modes casually. If a discovery session exposes a build task, write it
down and keep the current session docs-only unless David explicitly changes the
mode.

## Current fast path

As of 2026-04-26, the project is still in the Strategic Reset / Discovery
Sprint. The fastest path is:

1. Protect the control-room branch state (`chore/triage-2026-04-23`) and the
   untracked `.agents/` + `AGENTS.md` files.
2. Finish `docs/discovery/gold-layer-roadmap.md`.
3. Use that roadmap to write `docs/plans/2026-05-xx-gold-layer-rebuild.md`.
4. Only then resume dbt/warehouse/mart build work.
5. Retire old Orca worktrees that are merged or intentionally abandoned.

During this sprint, no new dbt, warehouse, mart, extractor, or dashboard work
should start unless the deliverable is documentation.

## From idea to task

Before work begins, reduce the idea to this shape:

```text
Question:
Decision needed:
Source of truth:
Files touched:
Done when:
Branch name:
```

Examples:

```text
Question: What revenue mart should come first?
Decision needed: mart grain, PK, source dependencies, blocker status
Source of truth: docs/discovery/* + engagement corpus
Files touched: docs/discovery/gold-layer-roadmap.md
Done when: ranked mart list exists with grain/PK/blockers
Branch name: codex/2026-04-26-gold-roadmap
```

```text
Question: Can this old Orca worktree be removed?
Decision needed: merged? dirty? unique commits?
Source of truth: .claude/scripts/orca-worktree-audit.sh + git log
Files touched: none, unless preserving a diff
Done when: worktree removed or promoted to a real task
Branch name: none unless preserving work
```

If the idea cannot fit this shape, it is still a conversation, not a task.

## Knowledge routing

Use the notebooks before locking decisions:

| Question type | Scope |
|---|---|
| dbt/modeling/CI/CD/medallion | `methodology.data_ops` |
| Metabase ops/integration/licensing | `methodology.metabase` |
| Metabase dashboard authoring/SQL/chart choice | `methodology.metabase_learn` |
| D-DEE history/client decisions | `engagement` |
| uncertain craft question | `methodology` |

When a corpus answer changes a convention, model, workflow, or client-facing
doc, cite the source title inline in the file.

## Branch and worktree rules

- One task = one branch + one worktree + one PR.
- Branch from fresh trunk, not stale local `main`.
- Use boring names:

```text
codex/YYYY-MM-DD-short-purpose
orca/YYYY-MM-DD-short-purpose
fix/YYYY-MM-DD-short-purpose
chore/YYYY-MM-DD-short-purpose
```

- Keep the control-room repo clean except for intentional triage.
- If work is already dirty, protect or commit that work before switching tasks.

## Fast review gate

Before pushing or opening a PR, answer:

1. Did the work match the stated task?
2. Are unrelated files absent from the diff?
3. Did tests/checks run, or is the reason they did not run documented?
4. Did `WORKLOG.md` get an entry if a decision or deliverable changed?
5. Does `.claude/state/project-state.md` still tell the truth?

If any answer is no, fix that before PR.

## Stop rules

Stop and ask David before:

- touching third-party/client-facing systems outside the repo/GCP/GitHub scope;
- starting non-doc build work during the Discovery Sprint;
- deleting a dirty worktree or force-deleting a branch with unique commits;
- changing metric definitions, stakeholder priorities, or roadmap order without
  a cited source or explicit decision.

## End of session

Before leaving:

```bash
git status --short --branch
git diff --stat
```

Then:

1. Commit or clearly name any remaining uncommitted work.
2. Append `WORKLOG.md` if a deliverable, decision, or durable blocker changed.
3. Refresh `.claude/state/project-state.md` if the worklog changed or the
   current phase moved.
4. Remove clean merged worktrees, or write down why they are parked.

