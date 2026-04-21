# Session Handover — Track S: WORKLOG → index refactor (session-start context)

**Branch:** `Davv5/Track-S-Worklog-Index-Refactor`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Replace the SessionStart hook's raw tail-of-WORKLOG injection with a curated **project-state index**, so context stays bounded regardless of project age. Pattern mirrors `MEMORY.md`: a small indexed file points at the relevant history; agents retrieve specific entries on demand via `Read` / `Grep`.

## Changed files (expected)

```
.claude/scripts/sessionstart-inject-state.sh   — created — replaces tail-injection logic
.claude/scripts/endsession-regenerate-state.sh — created — rewrites the index at end of session
.claude/settings.json                          — edited — swap hook commands
.claude/state/project-state.md                 — created — the index file (initial version)
.claude/rules/worklog.md                       — edited — document the new session-start mechanic
WORKLOG.md                                      — edited — dated entry
```

## Tasks

- [ ] Read `.claude/settings.json` to identify the current SessionStart hook shape (it currently tails WORKLOG.md)
- [ ] Read `.claude/rules/worklog.md` to understand what the hook is supposed to inject + the 200-line cap
- [ ] Invoke `ask-corpus` with the question: "What's the canonical pattern in Claude Code for persisting project state across sessions without bloating context?" — ground the refactor in corpus findings (likely maps to memory-system / retrieval-over-injection patterns)
- [ ] Design the index format (target: 40–60 lines max). Required sections:
      - Current phase / branch / last successful PR merged
      - Last 3 decisions (1 line each, with link to WORKLOG entry)
      - Open threads (bullet, with link)
      - Where-to-look pointers (grep patterns, file paths)
- [ ] Write `sessionstart-inject-state.sh` — cat the index file, nothing else
- [ ] Write `endsession-regenerate-state.sh` — reads the last ~500 lines of WORKLOG and invokes Claude (via the MCP bridge if available, or via a simple template script) to rewrite `.claude/state/project-state.md`. For v1, make this a manual command rather than an EndSession hook — too much complexity for a first pass. Document the command in `.claude/rules/worklog.md`.
- [ ] Populate `.claude/state/project-state.md` from the current WORKLOG by hand (one-time bootstrap)
- [ ] Update `.claude/settings.json` SessionStart hook command
- [ ] Update `.claude/rules/worklog.md` to document the new flow:
      - WORKLOG.md remains the append-only archive (unchanged convention)
      - `.claude/state/project-state.md` is the auto-injected context
      - Agents run `Read ~/.claude/state/project-state.md` to orient; `Grep WORKLOG.md` for historical detail
      - End-of-session regeneration command
- [ ] Start a fresh Claude Code session (`claude` in a new terminal) and verify the index gets injected instead of the worklog tail
- [ ] Append WORKLOG entry
- [ ] Run `/handover`
- [ ] Commit locally

## Decisions already made

- **WORKLOG.md is NOT pruned.** It stays append-only as the audit log. The refactor is about what gets auto-injected, not about losing history.
- **EndSession regeneration is manual for v1.** `.claude/state/project-state.md` gets rewritten by running a command (e.g., `/refresh-state` slash command or a simple bash script). An actual EndSession hook with LLM invocation is a v2 concern.
- **Index lives at `.claude/state/project-state.md`, not at root.** Keeps the root clean.

## Open questions

- Does Claude Code currently support an EndSession hook? If yes, v1 can use it; if no, fall back to manual regeneration. Check corpus or docs. If unclear, **pick sensible default** (manual) and note in commit.
- Should the index be grepped by agents or fully loaded? Load it — 60 lines is cheap. Don't over-engineer.

## Done when

- Opening a new Claude Code session shows the index content in the initial context, not the WORKLOG tail
- The index is ≤60 lines
- `.claude/rules/worklog.md` documents the new mechanic clearly enough that future agents don't revert to tail-injection
- Old tail-inject script still works (not deleted) so we can revert fast if the new flow has issues
- WORKLOG entry + handover doc + commit local

## Context links

- Current `.claude/settings.json` SessionStart hook (the thing being replaced)
- `.claude/rules/worklog.md` (current convention)
- `~/.claude/projects/<project>/memory/MEMORY.md` — the pattern this mirrors
- Plan file: `/Users/david/.claude/plans/this-is-a-sorted-rabbit.md` — research motivation
