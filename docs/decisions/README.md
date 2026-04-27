# Decision records (ADRs)

This folder holds **Architecture / Architectural Decision Records** — one file per significant decision that's worth preserving but doesn't fit cleanly in a PR description, a `.claude/rules/*.md`, an auto-memory entry, or a plan document.

Decision records are a specific destination in the [end-of-session routing rule](../../.claude/rules/worklog.md). When a session produces a load-bearing decision that *isn't* captured by another artifact, it lands here.

## When to write an ADR vs use another destination

| If the decision is… | Right home |
|---|---|
| Captured by a PR description (and the PR is the natural home) | The PR. Don't duplicate. |
| A repeating convention or pattern that future code should follow | `.claude/rules/<topic>.md` |
| A learning Claude itself should carry across sessions | Auto-memory |
| A multi-step build with units / phases / acceptance criteria | `docs/plans/YYYY-MM-DD-NNN-<slug>.md` |
| Standalone, load-bearing, *and* doesn't fit any of the above | **An ADR here.** |

The ADR threshold: the decision will outlast its current PR, you'd want to find it in 6 months, and "what was the why?" is the load-bearing question. If the *why* is obvious from the code, you don't need an ADR.

## Naming

```
YYYY-MM-DD-short-kebab-slug.md
```

Examples:
- `2026-04-26-bi-direction-dabi.md`
- `2026-04-23-gcp-project-consolidation.md`
- `2026-05-08-gold-layer-mart-prioritization.md`

One decision per file. If two decisions are entangled, write two files and cross-link them under "Related."

## Format

```markdown
# <Decision title in plain English>

**Date:** YYYY-MM-DD
**Status:** Proposed | Accepted | Superseded | Deprecated
**Supersedes:** (optional — link to prior ADR this replaces)
**Superseded by:** (optional — link forward when this one gets replaced)
**Related:** (optional — links to plans, rules, memories, PRs that connect)

## Context

What's the situation that forced a decision? What constraints, tradeoffs, or facts on the ground shaped it? Keep this tight — the reader needs enough to evaluate the decision but doesn't need a project history.

## Decision

The decision itself, stated unambiguously. One paragraph. If it's a list, use a list — but keep it short.

## Consequences

What changes because of this decision — both intended and likely-but-unintended. What does it foreclose? What does it unlock? What's the reactivation gate if circumstances change?
```

Keep ADRs under 80 lines. If an ADR wants to be longer, it's probably a plan (under `docs/plans/`) or a discovery doc (under `docs/discovery/`).

## Lifecycle

- **Proposed** — the decision is drafted but not yet committed-to. Use sparingly; most ADRs are written *after* the decision is accepted.
- **Accepted** — the decision is in force. Default state for newly-written ADRs.
- **Superseded** — a later ADR replaced this one. Update the `Status:` field, add a `Superseded by:` link, but **don't delete** — the historical record stays.
- **Deprecated** — the decision is no longer in force, but no successor ADR exists yet. Less common than "Superseded."

When you supersede an ADR, the new one's `Supersedes:` field links back. The old one's `Superseded by:` field links forward. The chain stays navigable.

## Discoverability

ADRs are not auto-loaded into Claude's context. They're *retrieved* — when a session needs to know "why did we decide X?", a `grep` or `gh search` finds the ADR. Treat them like searchable institutional memory, not always-on instructions.

If an ADR is so load-bearing it should be in every session's context, it probably wants a `.claude/rules/*.md` rule entry pointing at it (or the rule itself encodes the decision).

## What NOT to put here

- In-progress thinking or brainstorming → goes in `docs/discovery/<topic>.md` until a decision is reached.
- Multi-step plans with units / phases → goes in `docs/plans/`.
- Conventions / patterns → goes in `.claude/rules/`.
- Routine PR-level decisions captured in PR descriptions → don't duplicate.
- Personal preferences / user profile → auto-memory.

## Seed entries to consider (not required)

If you want to backfill ADRs for past load-bearing decisions, candidates from this project's recent history:

- 2026-04-23 — GCP project consolidation onto `project-41542e21-470f-4589-96d`
- 2026-04-24 — Strategic Reset: pause new build, run Discovery Sprint
- 2026-04-26 — BI direction is dabi (Kim's generative-BI recipe), Metabase retired, Evidence.dev no longer the planned replacement

Backfill only if the decision is still load-bearing AND a future session would benefit from finding the ADR. Otherwise, the decision lives in its current home (memory, plan frontmatter, PR description) and that's fine.
