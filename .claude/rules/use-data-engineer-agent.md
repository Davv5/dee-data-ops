---
paths: ["**/*"]
---

# Use the data-engineer agent for engagement lifecycle work

This project has the agent-kit's `data-engineer` subagent installed at `~/.claude/agents/data-engineer.md` (symlink to `/Users/david/Documents/agent-kit/agents/data-engineer/AGENT.md`). The agent owns the engagement lifecycle (preflight → discovery → 3-layer build → operate → handover) and delegates to specialist skills at named seams.

## Invoke `data-engineer` when the request matches one of:

- **Discovery / scope:** "produce the gold-layer roadmap", "run discovery for this engagement", "what should we build first", "build the source inventory / business-area map / coverage matrix"
- **Layer build:** "build the staging layer for X", "scaffold a fact table for Y", "collapse marts for Z", "add a new mart for the <business-question> dashboard"
- **Operate:** "review this dbt PR", "validate cutover parity for X", "check this DDL change for safety"
- **Handover:** "produce the engagement handover doc"
- **Engagement init:** "we just signed a new client" (Phase 0)

Invoke via `Agent(subagent_type="data-engineer", prompt=...)`. The agent will route to the appropriate kit skill: `engagement-init`, `staging-scaffold`, `warehouse-fct-scaffold`, `mart-collapse`, `source-inventory-build`, `business-area-template`, `coverage-matrix-build`, `mart-roadmap-rank`, or one of the `altimate-*` LAW-pattern skills (`sql-review`, `data-parity`, `schema-migration`, `dbt-unit-tests`).

## Why this rule exists

Discoverability. Without this rule, the orchestrator session would default to writing dbt models / SQL reviews ad-hoc instead of routing through the agent + its specialist skills. That's the named failure mode the LAW pattern was designed to prevent (see `~/.claude/skills/altimate-sql-review/SKILL.md`).

## Hooks (deterministic, not discoverability-dependent)

The `.claude/settings.json` `PreToolUse(Write|Edit)` hook fires `pre-sql-altimate-review.sh` on every `*.sql` file write — Altimate's specialist runs regardless of whether the orchestrator routed through the agent. The `PostToolUse(Write|Edit)` hook fires `post-sql-qa-baseline.sh` on `models/**/*.sql` writes to append baseline schema.yml tests. Both hooks are belt-and-suspenders to the agent's LAW 1.

## Anti-patterns (do NOT)

- Do NOT write SQL ad-hoc when a generator skill applies. Reach for `staging-scaffold` / `warehouse-fct-scaffold` / `mart-collapse` first.
- Do NOT write SQL review prose without invoking `altimate-sql-review`. The hook will fire automatically; the LAW says don't substitute host-written analysis.
- Do NOT skip the engagement-init step when bootstrapping a new client.

## See also

- `~/.claude/agents/data-engineer.md` — the agent's full system prompt + 3 LAWs
- `/Users/david/Documents/agent-kit/agents/data-engineer/lifecycle.md` — per-phase skill list and exit criteria
- `/Users/david/Documents/agent-kit/docs/handovers/conventions-from-U2.md` — kit-wide conventions (frontmatter shape, LAW pattern, badge format, color palette)
