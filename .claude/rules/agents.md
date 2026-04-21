---
paths: [".claude/agents/**", "docs/handovers/**"]
---

# Agent pipeline conventions

Load when working on anything under `.claude/agents/` or `docs/handovers/`.
Rules for how the three agents (`plan-architect`, `track-executor`,
`pr-reviewer`) get invoked and chained together.

The agent definitions themselves live in `.claude/agents/*.md`. The
backlog of track files the executor picks up from lives in
`docs/handovers/`. The BACKLOG index lives at `docs/handovers/BACKLOG.md`.

## The pipeline shape

```
main session
  ↓ passes a goal or track file
plan-architect (Opus)                  [produces track file, no code changes]
  ↓ passes track file path
track-executor (Sonnet, in a worktree) [produces code changes, local commit]
  ↓ reports back — main session gates
pr-reviewer (Opus)                     [pushes + opens PR]
  ↓
David reviews PR in Orca → merge
```

Each agent's output is committed at a different moment; each PR has a
single owner and a single purpose.

## Rule 1 — Worktrees are pre-created with readable names

When firing `track-executor` on an existing track, **do not** rely on the
Agent tool's `isolation: "worktree"` auto-generated hash path (you get
`.claude/worktrees/agent-a6d7ea60` — unreadable).

Instead, the main session (David, or Claude on David's behalf)
pre-creates the worktree with a readable name, then passes the path to
the executor:

```bash
git worktree add \
  .claude/worktrees/track-<Letter>-<short-slug> \
  -b Davv5/Track-<Letter>-<PascalCaseSlug> \
  main
```

Example:

```bash
git worktree add \
  .claude/worktrees/track-T-corpus-config \
  -b Davv5/Track-T-Corpus-Config-Decouple \
  main
```

Then invoke the executor pointing at that path (via `cwd` in the Agent
tool call, NOT `isolation: "worktree"`). The executor sees:

- Worktree at: `.claude/worktrees/track-T-corpus-config/`
- Branch: `Davv5/Track-T-Corpus-Config-Decouple`
- Both readable; both match what surfaces in the PR title.

**Why** — the opaque `agent-<hash>` path is fine for transient sub-agent
work but painful for track work that's supposed to have a reviewable
lifecycle. Branch names + worktree paths should match the track file's
letter (`Track-N`, `Track-T`, etc.) so a glance at `git worktree list`
tells you what's in flight.

**Cleanup** — after the PR merges, remove the worktree:

```bash
git worktree remove .claude/worktrees/track-T-corpus-config
```

If the executor crashed mid-run and the worktree is in a weird state,
force-remove with `git worktree remove --force`.

## Rule 2 — Main session gates between agents with a one-word prompt

After `track-executor` finishes and reports back, the main session does
NOT auto-chain to `pr-reviewer`. Instead, the main session summarizes
the executor's output and ends with a single-line gate prompt:

> **Reply `proceed` to fire pr-reviewer, `hold` to stop, or name a fix.**

David's responses drive the chain:

| Response | Action |
|---|---|
| `proceed` | Main session fires `pr-reviewer` with the executor's report |
| `hold` | Main session stops; work stays in the worktree for a later turn |
| `<any fix instruction>` | Main session treats the response as a change request and either (a) edits the worktree directly and re-reports to David, or (b) re-invokes the executor with the fix instruction |

**Why** — keeps the human-in-the-loop gate cheap and explicit. One-word
response = one-word cost. Prevents auto-chaining a bad executor run
straight into a PR that then needs close-and-redo.

The same gate applies between `plan-architect` and `track-executor` —
after architect writes a track, main session ends with:

> **Reply `execute` to fire track-executor on this plan, `revise` with changes, or `hold`.**

## Rule 3 — Plans and implementations are separate PRs

- `plan-architect` produces a track file. Committed in a planning PR (or
  as scaffolding in another PR).
- `track-executor` produces the actual code changes + filled-in track
  checkboxes + WORKLOG entry + handover doc. Committed on an
  implementation branch, local-commit only.
- `pr-reviewer` pushes + opens THE implementation PR. One PR per track.

The track file's checkboxes get filled in on the implementation branch,
not on the planning branch — because that's where the executor actually
runs and verifies each step.

**Why** — lets David review plans before firing execution, and review
implementations separately. If a plan is wrong, only the plan PR changes;
no code has to be un-merged.

## Rule 4 — pr-reviewer's three hygiene checks are non-negotiable

`pr-reviewer` runs three deterministic checks before any push:

1. **WORKLOG diff non-empty** — `git diff <base>..HEAD -- WORKLOG.md` must
   include a dated H2 with What/Decisions/Open-threads structure per
   `.claude/rules/worklog.md`.
2. **Handover doc exists** — a new `docs/handovers/Davv5-Track-*-*.md`
   in the branch's commit history.
3. **Convention grounding** — any new `.claude/rules/*.md`,
   `dbt/models/**`, or `.github/workflows/*.yml` must cite a corpus
   source inline, OR the reviewer invokes `ask-corpus` / notebook_query
   to confirm the pattern.

Any failure = hard `Request-changes`. Reviewer returns the fix list to
main session; executor gets re-fired (or David fixes manually).

## Rule 5 — Parallel fire is for scope-disjoint tracks only

Firing multiple executors in parallel (Wave 1 in BACKLOG.md) is safe
ONLY when the tracks touch disjoint file scopes. Check before
parallelizing:

- N touches `dashboards/evidence/` + one workflow file
- O touches no repo files (just `gh secret delete`)
- T touches `.claude/corpus.yaml` + `.claude/skills/ask-corpus/` +
  `.claude/rules/using-the-notebook.md` + `CLAUDE.md`

These three overlap in zero files → safe to parallelize.

Tracks that touch `WORKLOG.md`, `CLAUDE.md`, `.claude/settings.json`,
`dbt_project.yml`, or any shared rule file will collide on append —
run them sequentially.

## Fallback when custom agents aren't registered

If `subagent_type: "track-executor"` errors with "Agent type not found"
(custom agents in `.claude/agents/` haven't loaded in the current
Claude Code session), fallback pattern:

1. Use `subagent_type: "general-purpose"` instead
2. Embed the agent's full system prompt (from `.claude/agents/<name>.md`)
   into the `prompt:` parameter
3. Label it clearly: *"You are acting as the track-executor agent. The
   persona is embedded below because custom agents didn't register in
   this session..."*

This worked for the Track S dogfood run. Not recommended long-term;
restart Claude Code so `.claude/agents/*.md` registers properly before
the next wave.

## Lessons learned

*(Populate as the workflow matures.)*
