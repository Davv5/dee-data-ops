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

## Reviews always pair: data-engineer (or main agent) → CE reviewer

Whenever the `data-engineer` agent produces a fix, model, or scaffold — or whenever the main agent ships a non-trivial diff in service code, dbt models, SQL, infra, rules, or docs — spawn a CE reviewer on the output before merge. The two agent classes have complementary failure modes:

- **`data-engineer` / main agent** is strong at *production*: routing to specialist skills, matching project conventions, bringing domain context. Tends to be complete on first pass when the prompt is well-scoped.
- **CE reviewers** (`ce-adversarial-reviewer`, `ce-project-standards-reviewer`, `ce-correctness-reviewer`, `ce-coherence-reviewer`, `ce-maintainability-reviewer`, `ce-testing-reviewer`, plus the conditional personas) are strong at *failure construction*: building scenarios that break the diff, surfacing what the producer missed, checking against the repo's own standards.

Pair them. Always.

**Empirical anchor (PR #107, 2026-04-28).** The fix-the-6-broken-default-modules problem was solved twice in parallel: once by the main agent on `Davv5/fix/bq-ingest-sql-path-fallback` (incremental, missed `data_quality.py` on first pass at commit `cae4d82` — `ce-adversarial-reviewer` caught it on round 2, fixed in `8fa4cf1`), and once by the `data-engineer` agent in an isolated worktree on `agent/sql-path-fix` at commit `1e0ee0c` (caught all 6 first pass; ran a path-resolution smoke test that `compileall` would have missed). Both producers needed CE adversarial review to catch what they missed; neither was good enough alone. Lesson: producer + reviewer is the unit, not either alone.

### When to pick which CE reviewer

- **High-stakes diff, large surface, or production-deploy-bound** → `ce-adversarial-reviewer` (constructs failure scenarios). Default for anything touching auth, payments, data mutations, external APIs, or 50+ changed lines.
- **Repo conventions / frontmatter / cross-platform / tooling drift** → `ce-project-standards-reviewer`. Default for new rule files, doc updates, and config changes.
- **Logic or state-management bugs** → `ce-correctness-reviewer`. Always-on review pass for any non-trivial code diff.
- **Planning documents (specs, plans, ADRs)** → invoke the `compound-engineering:ce-doc-review` *skill* (via `Skill` tool or `/ce-doc-review`), which orchestrates `ce-coherence-reviewer`, `ce-feasibility-reviewer`, `ce-product-lens-reviewer`, `ce-scope-guardian-reviewer`, `ce-design-lens-reviewer`, and `ce-security-lens-reviewer` in parallel. (Note: `ce-doc-review` is a skill, not an agent — `Agent(subagent_type="ce-doc-review", ...)` will error. For ad-hoc single-persona doc review, spawn `ce-adversarial-document-reviewer` directly.)
- **In doubt, run two in parallel.** Adversarial + standards is a cheap default for code; coherence + feasibility is a cheap default for plans.

### What this does NOT mean

- Don't spawn reviewers for trivial diffs (typo fixes, single-line config tweaks, project-state regens). Use judgment.
- Don't replace specialist skills with generic CE review. `altimate-sql-review` still runs on `*.sql` writes via the PreToolUse hook regardless of which agent produced the SQL — that's the LAW pattern, and it's belt-and-suspenders to this rule.
- Don't pair when the `data-engineer` agent's specialist skill *is* the review (e.g. `altimate-data-parity` for cutover parity, `altimate-schema-migration` for DDL safety). The specialist already is the rigorous pass.

## See also

- `~/.claude/agents/data-engineer.md` — the agent's full system prompt + 3 LAWs
- `/Users/david/Documents/agent-kit/agents/data-engineer/lifecycle.md` — per-phase skill list and exit criteria
- `/Users/david/Documents/agent-kit/docs/handovers/conventions-from-U2.md` — kit-wide conventions (frontmatter shape, LAW pattern, badge format, color palette)
