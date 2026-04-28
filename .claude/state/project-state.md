# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-27 (Phase A closed: roadmap + agent-kit install merged via PR #80; this PR completes the input sharpening)_

## Where we are

- **Strategic Reset / Discovery Sprint Phase A: CLOSED.** Canonical Gold-layer roadmap landed via PR #80 (`docs/discovery/gold-layer-roadmap.md`). The agent-kit's `data-engineer` subagent + LAW skills are installed globally; discoverability rule + PreToolUse / PostToolUse hooks fire deterministically. PR #81 was closed in favor of PR #80 (kit's standard rank-table format won over PR #81's narrative).
- **Phase B (Layer Build) authorized.** Per `docs/decisions/2026-04-27-phase-a-to-b-transition.md`: the "no new build" constraint is lifted; future architectural pauses require an explicit declared reset.
- **First Phase B work order:** `data-engineer` → `staging-scaffold` for `stg_fanbasis__transactions`. Highest-leverage move on the matrix — flips Q2 / Q4 / Q5 / Q8 / Q9 / Q11 from blocked toward usable. Replaces the placeholder CTE in `2-dbt/models/warehouse/facts/fct_revenue.sql:40–66`.
- **Existing wide marts auto-widen on Fanbasis staging:** `lead_journey`, `revenue_detail`, `sales_activity_detail`, `speed_to_lead_detail` are shipped; refresh-only work after Fanbasis lands.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path; raw `notebook_query` calls are reserved for ad-hoc lookups.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 13 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when GHL trusted-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Sprint (closed):** `docs/plans/2026-04-24-strategic-reset.md` — Phase A complete; Phase B reactivates per the ADR.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-27** — Phase A → Phase B transition: ship the agent-kit `data-engineer` agent + LAW-skill catalog as v1; close the Strategic Reset; authorize Phase B build work. Destination: `docs/decisions/2026-04-27-phase-a-to-b-transition.md` (ADR) + PR #80.
- **2026-04-27** — Discovery Sprint inputs sharpened: "one wide mart per playbook chapter" architecture commitment + Owner-as-role column on `business-area-map.md`; mart-roadmap-rank scaffold-audit failure mode logged. Destination: this PR (`chore/discovery-input-sharpen-2026-04-27` → main) + WORKLOG entry for the cross-session learning.
- **2026-04-26** — ask-corpus v2 corpus research engine: two-phase host-LLM JSON handshake; quality-aware diversity guard; 3 LAWs at launch (mart-naming = LAW 3). Destination: PR #74 (merged to main as `9199b8b`) + WORKLOG entry.

## Open threads

- **Phase B kickoff** (per the canonical roadmap): (1) `stg_fanbasis__transactions` → `fct_revenue` placeholder fill (highest-leverage); (2) period-grain rollups on top of existing wide marts; (3) net-new facts (`fct_calls_held` from Fathom; `fct_opportunity_stage_transitions` from GHL; `fct_refunds`); (4) `dim_typeform_form`; (5) Tier C marts gated on vendor-support. Invoke through `data-engineer` agent so LAW pattern fires.
- **GHL trusted-copy decision** — single named blocker for several Tier B/refresh marts. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move.
- **Fanbasis bridge match-method** — read `bridge_identity_contact_payment` before starting Phase B.1 to confirm whether Fanbasis `payment_id` shape needs a Fanbasis-aware match-method addition.
- **Fathom → GHL contact join key** — attendee email reliability. Affects `fct_calls_held`.
- **GHL stage-transition event presence** — confirm raw landing carries stage-change events with timestamps before scaffolding `fct_opportunity_stage_transitions`.
- **Empirical tuning of corpus-engine constants** — `DIVERSITY_RELEVANCE_THRESHOLD=0.30` + `QUALITY_PARITY_FLOOR=0.6` ship provisional. Lock after first production queries.
- **`bq-ingest` service repair**, **Typeform `form_id` gap**, **GHL `messages` / `users` / `tasks` 0-row upstream**, **Fathom transcript landing**, **Stripe staleness** — all paused per Strategic Reset; revisit on cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).

## Where to look

- **Canonical roadmap (on main):** `docs/discovery/gold-layer-roadmap.md`
- **Phase A → B ADR:** `docs/decisions/2026-04-27-phase-a-to-b-transition.md`
- **Mart architecture commitment:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Existing wide marts (auto-widen on Fanbasis staging):** `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Phase B placeholder:** `2-dbt/models/warehouse/facts/fct_revenue.sql:40–66` (the `fanbasis_payments` CTE waiting for staging)
- **Data-engineer agent + LAW skills:** `~/.claude/agents/data-engineer.md` owns engagement lifecycle; specialist seams via `altimate-{sql-review,data-parity,schema-migration,dbt-unit-tests}` skills. Discoverability rule: `.claude/rules/use-data-engineer-agent.md`. Hooks: PreToolUse(Write|Edit) → `pre-sql-altimate-review.sh`; PostToolUse(Write|Edit) → `post-sql-qa-baseline.sh`.
- **Corpus engine v2:** `.claude/skills/ask-corpus/scripts/` (engine) + `.claude/skills/ask-corpus/SKILL.md` (voice contract) + `SKILL-v1.md` (backup)
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit` (installed globally via `~/.claude/agents/data-engineer.md` symlink + 16 `~/.claude/skills/<kit-name>` symlinks; project-imported via `import-agent-kit.sh --symlink`)
- **Codex parity:** `AGENTS.md` + `.agents/skills/{ask-corpus,skill-creator,worklog}/`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix,gold-layer-roadmap}.md`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-27
- WORKLOG: fired this session — captured the cross-session failure-mode learning (`mart-roadmap-rank` without scaffold audit) that doesn't fit any other destination, plus the operator-fast-loop runbook entry that was unstaged on the abandoned triage branch.
