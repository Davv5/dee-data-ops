# Phase A → Phase B transition: ship as v1

**Date:** 2026-04-27
**Status:** Accepted

## Context

Phase A (Steering / Discovery Sprint) ran 2026-04-24 → 2026-04-27. The sprint's exit gate per `docs/plans/2026-04-24-strategic-reset.md` § "Exit criteria" required:

1. `docs/discovery/source-inventory.md` — landed prior
2. `docs/discovery/business-area-map.md` — landed prior
3. `docs/discovery/coverage-matrix.md` — landed prior
4. `docs/discovery/gold-layer-roadmap.md` — **landed today (this PR)**

Plus David's sign-off on the roadmap content + Phase A → B transition logged.

In parallel, the agent-kit project (in `/Users/david/Documents/agent-kit`, GitHub `Davv5/agent-kit`) productized the data-engineer agent + skill catalog (U1-U9 across EU1-EU7 of two coupled plans). EU7b validation produced Probe A pass (catalog autonomously produced today's `gold-layer-roadmap.md` from the merged main) and Probe B pass (catalog autonomously surfaced in a non-D-DEE tmpdir without the discoverability rule cheating). Validation writeup at `/Users/david/Documents/agent-kit/docs/validation/2026-04-27-d-dee-validation.md`.

## Decision

**Phase A is closed. Phase B (Velocity / Layer Build) is authorized.** The agent-kit data-engineer + LAW-skill catalog ships as v1 — both probes passed, the catalog produced a concrete deliverable, and David's review of the roadmap content confirmed it was good.

## Consequences

**Unlocks:**
- Phase B principles reactivate: `feedback_multi_agent_orchestration.md` ("orchestration is leverage, not ceremony") applies; `live-by-default.md` no longer suppressed by Phase A.
- The data-engineer agent at `~/.claude/agents/data-engineer.md` (symlink → `/Users/david/Documents/agent-kit/agents/data-engineer/AGENT.md`) is the route for Phase B build work. Invoke via `Agent(subagent_type="data-engineer", ...)`.
- Discoverability rule at `.claude/rules/use-data-engineer-agent.md` auto-loads in every session and routes engagement-lifecycle prompts to the agent.
- Hooks fire deterministically: `PreToolUse(Write|Edit) → pre-sql-altimate-review.sh` runs Altimate review on every `*.sql` write; `PostToolUse(Write|Edit) → post-sql-qa-baseline.sh` appends baseline schema.yml tests on `models/**/*.sql` writes.
- First Phase B build invocation per the roadmap: `data-engineer` → `staging-scaffold` for `stg_fanbasis__transactions` (the matrix's biggest single blocker-flip — flips Q2 / Q8 / Q9 cells from blocked to working).

**Forecloses:**
- The Strategic Reset's "no new build" constraint is lifted. Future architectural pauses require an explicit declared reset, not a drift back into Phase A.
- Stripe-based revenue modeling is historical-only; the `revenue_detail` mart pivots to Fanbasis-primary (rank 3 on the roadmap).

**Reactivation gate (if circumstances change):**
A new declared Strategic Reset would re-enter Phase A. Triggers (inlined here since the prior `docs/methodology.md` was removed as a drift-magnet in PR #77): locked metric proves uncalibrated, oracle numbers diverge from roadmap-derived counts, or scope drift outpaces what the current roadmap supports. The reset must be explicit (declared in WORKLOG.md or a new ADR), not a quiet drift back into docs-only mode.

## Related

- Roadmap: `docs/discovery/gold-layer-roadmap.md`
- Validation: `/Users/david/Documents/agent-kit/docs/validation/2026-04-27-d-dee-validation.md`
- Strategic Reset plan (closed): `docs/plans/2026-04-24-strategic-reset.md`
- KISS-sweep that retired `docs/methodology.md`: PR #77
- Memory: `project_phase_a_active.md` (now stale — Phase A closed; orchestrator-side memory hygiene)
