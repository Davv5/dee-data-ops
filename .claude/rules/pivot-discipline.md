---
paths: ["**/*"]
---

# Pivot discipline — retire superseded artifacts in the same session

Always-on rule (loaded via the pointer in `CLAUDE.md`, glob set to match the same `["**/*"]` pattern `worklog.md` and `using-the-notebook.md` use — these three rules govern session-level discipline rather than path-scoped conventions, so the load-trigger is the *moment of decision*, not the *file you're editing*). Fires when a session writes a memory file under `~/.claude/projects/-Users-david-Documents-data-ops/memory/` that captures a *strategic pivot*, OR when editing any plan / scope doc / archive entry / `CLAUDE.local.md` whose content depends on a pivot that may have already happened.

## Why this rule exists

This project carries six months of strategic pivots — Looker Studio → Metabase → Evidence.dev → dabi (BI direction); v1 plan → bq-ingest consolidation → Strategic Reset (build path); Grok-as-ranker → rubric-skill (decision process); Stripe-live → Stripe-banned-Fanbasis-live (revenue source). Each pivot got captured as a *new* memory or plan; few pivots removed the *old* artifacts. The compound effect is fog: plans, scope docs, rule v1-inventories, and `CLAUDE.local.md` describe a present that hasn't been current for weeks. Future-Claude reads them as authoritative, recreates retired skeletons, and re-walks confusion that should have been closed at pivot time.

This rule is the antibody. It says: when you save a memory about a pivot, in the same session, walk a fixed checklist and either update or banner-archive every doc the pivot supersedes.

**Empirical anchor (2026-04-28).** A reconciliation pass found seven stale artifacts (`1-raw-landing/fanbasis/` skeleton, `ingest.md` v1 inventory line, `CLAUDE.local.md` Evidence.dev language, `client_v1_scope_speed_to_lead.md` lacking a frozen banner, `2026-04-24-strategic-reset.md` Grok-in-loop steps, `2026-04-23-001-feat-gtm-source-port-plan.md` paused-but-not-marked, and `feedback_ship_over_ceremony.md` still cited despite being superseded). All seven were caused by pivots that got captured in memory but did not propagate to the docs the memory superseded. Cost: an entire session of cleanup. This rule prevents the next round.

## What counts as a strategic pivot

Not every memory write is a pivot. A pivot is a memory whose content changes one of:

- **BI / dashboard / surface direction** (Metabase → Evidence.dev → dabi)
- **Source-of-truth path** (Stripe → Fanbasis; Fivetran → Cloud Run; etc.)
- **Scope cut or expansion** (V1 → V2 → V1.6 retired; "Phase A closed, B active")
- **Tool / framework selection** (Looker → Metabase; Grok → rubric skill)
- **Work-bucket pause / resume** (U4a paused; vendor-API corpus parked)
- **Stakeholder / actor change** (Grok out of the loop; Stripe banned by D-DEE)

If the memory says "we used to X, now we Y" — it's a pivot, and this rule applies. If the memory just records a new fact ("the BQ project ID is …"), it's not a pivot.

## The walk — what to update or retire when a pivot lands

Walk this checklist **in the same session** as the memory write. For each row, either update the doc, banner-archive it, or note explicitly "doesn't reference this".

| Surface | What to check | Disposition options |
|---|---|---|
| `CLAUDE.local.md` | "Engagement at a glance", "Stack decisions", "Current status", and any table referencing the superseded thing | Update in-place. Don't banner; this file is the live overlay. |
| `.claude/rules/*.md` | Grep for the superseded term (old tool name, old source name, old work-bucket label) | Update. Rules are load-bearing for path-scoped sessions; stale rules teach future-Claude wrong. |
| `docs/plans/*.md` (active) | Steps that assumed the old direction, status banners, "supersedes" frontmatter | Add a stale-step flag to the affected step *and* update the plan-level status banner. Don't silently rewrite shipped decisions — preserve the historical text in italics with a "now stale" call-out. |
| `docs/plans/*.md` (shipped/historical) | Same as above, but lower priority — these are records of decisions, not active guidance | Banner-archive instead of in-place update. Add a "FROZEN" banner; let the original text remain so the audit trail survives. |
| Scope docs (`client_v1_*.md`, `*_build_plan.md`, etc.) | If shipped: banner-archive. If pre-ship: update. | Move to `docs/_archive/` if not already there; add the FROZEN banner. |
| `.claude/state/project-state.md` | Always regenerate at session end if state moved. The pivot is a state move. | Regenerate. |
| `MEMORY.md` index entries | If the new memory supersedes an older memory, mark it in the older entry's hook line | Update. The reader scans hooks first; an unmarked supersession misleads. |

**Always do:** grep the repo for the *superseded term* (e.g. `grep -ri "Evidence.dev" --include="*.md"`) to catch surfaces this checklist forgot. The checklist is a default; the grep is the safety net.

## When you can't do the walk in the same session

If the pivot is captured mid-debug or in a session that doesn't have time to chase down the affected docs, **don't ship the memory write without acknowledging the debt.** Surface "**pivot-debt**" as an Open Thread in `.claude/state/project-state.md` with a list of affected docs and a one-line rationale for deferring. Example:

```markdown
### Open threads

- **Pivot-debt (deferred 2026-04-28):** captured `project_bi_direction_dabi.md`
  superseding Evidence.dev. Have NOT yet walked: `CLAUDE.local.md` "Stack
  decisions", `docs/plans/2026-04-24-strategic-reset.md` step 1, the
  `evidence-dev` mention in `.claude/rules/operational-health.md`. Next pass
  needs to close these or this rule's compound-fog risk fires again.
```

This is escape-valve, not norm. If pivot-debt accumulates more than two unresolved entries, the rule has been violated systemically and we need a sweep session.

## What this rule is NOT

- **Not a rule about every memory write.** Routine memory writes (user role, session-start fact, lookup pointer) don't trigger this. Only *pivots*.
- **Not a rule against ever editing memories.** It's about propagation. The memory is the trigger; the walk is the obligation.
- **Not an alternative to `worklog.md`'s end-of-session routing.** That rule decides where session output lands; this rule decides what to retire when *future direction* changes. They compose.

## See also

- `.claude/rules/worklog.md` — end-of-session routing for session output (sister always-on rule)
- `.claude/rules/using-the-notebook.md` — corpus-citation norm (sister always-on rule). The "Empirical anchor" pattern this rule introduces is *distinct* from corpus citation: corpus citation references a notebook source; an empirical anchor references an in-repo incident that prompted a rule. Both styles can appear in the same rule file when both kinds of grounding apply.
- `.claude/rules/use-data-engineer-agent.md` — when retirement edits a rule file, pair the producer with a CE reviewer per that file's "Reviews always pair" clause
- Memory: `MEMORY.md` (index) + per-fact files in `~/.claude/projects/-Users-david-Documents-data-ops/memory/`

## Lessons learned

- **2026-04-28:** Rule created after a reconciliation sweep found seven stale artifacts caused by un-walked pivots. Empirical anchor lives in this rule's "Why" section.
- *(Populate as new pivot-debt incidents surface.)*
