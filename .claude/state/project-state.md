# Project state index

<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — present-moment snapshot

_Last regenerated: 2026-04-27 (Discovery Sprint inputs sharpened; PR #80 + PR #81 own the roadmap)_

## Where we are

- **Strategic Reset / Discovery Sprint Phase A: closing on PR #80 + PR #81.** Canonical Gold-layer roadmap is owned by those two open PRs (overlapping; one will need its roadmap file dropped before merge). Phase B (Layer Build) reactivates after the roadmap lands.
- **Today's input-sharpening work** (this PR's scope): coverage-matrix architecture commitment ("one wide mart per playbook chapter") + business-area-map Owner column (role-as-owner inferred from playbook chapter) + Codex parity (`AGENTS.md` + `.agents/skills/`) + per-machine gitignore additions. Both inputs feed the roadmap on PR #80/#81.
- **Phase B direction (per the canonical roadmap on PR #81):** highest-leverage move is `stg_fanbasis__transactions` → replace placeholder CTE in `fct_revenue.sql:40–66`. `lead_journey`, `revenue_detail`, `sales_activity_detail`, `speed_to_lead_detail` already exist on disk and auto-widen when Fanbasis staging lands.
- **`ask-corpus` v2 engine** lives on main (PR #74). Routing rule + voice contract are the canonical query path; raw `notebook_query` calls are reserved for ad-hoc lookups outside the rule.
- **Foundation intact (do not rebuild):** BigQuery + dbt + 13 staging models + `(id, _ingested_at, payload)` raw-landing discipline. U1 / U2 / U3 stay shipped.
- **GCP consolidation plan PAUSED at U3-complete.** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` U4a+ resumes when the trusted-GHL-copy decision lands.
- **Headline metric (locked 2026-04-19):** unchanged.

## Active plans

- **Sprint:** `docs/plans/2026-04-24-strategic-reset.md` — Phase A deliverables landing on PR #80 + PR #81. Phase B cadence reactivates when the roadmap merges.
- **Methodology (on main):** `docs/plans/2026-04-26-001-feat-corpus-research-engine-plan.md` — corpus engine v2; 11/13 active units shipped.
- **Paused (cutover):** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md` — U4a+ resumes when GHL trusted-copy decision lands.

## Last 3 decisions

- **2026-04-27** — Discovery Sprint inputs sharpened (architecture commitment + Owner-as-role column); mart-roadmap-rank scaffold-audit failure mode captured. Destination: this PR (`chore/discovery-input-sharpen-2026-04-27` → main) + WORKLOG entry for the cross-session learning.
- **2026-04-26** — ask-corpus v2 corpus research engine: two-phase host-LLM JSON handshake; quality-aware diversity guard; 3 LAWs at launch (mart-naming = LAW 3). Destination: PR #74 (merged to main as `9199b8b`) + `WORKLOG.md` entry.
- **2026-04-24** — Strategic Reset: pause new build, run Discovery Sprint, rebuild Gold against ranked roadmap. Foundation sound; problem was visibility + prioritization. Destination: `docs/plans/2026-04-24-strategic-reset.md` + `WORKLOG.md` entry.

## Open threads

- **Resolve duplicate roadmap on PR #80 vs PR #81** before either merges. Both add `docs/discovery/gold-layer-roadmap.md` (171 vs 173 lines). Recommend: drop the roadmap from PR #80 (keep the agent-kit install + decision doc + hooks); let PR #81 land the canonical roadmap.
- **PR #80 hook paths are absolute machine-local** (`/Users/david/Documents/agent-kit/hooks/...`). Confirm the scripts filter by `*.sql` extension internally before doing real work, or thread an env var, before merge.
- **GHL trusted-copy decision** — single named blocker for Tier B build work. Choose between legacy blob (1,314 conversation rows) and Phase-2 (101 rows). Resolves 92% undercount + four empty entities + dual-source ambiguity in one move.
- **Phase B kickoff candidates** (per canonical roadmap on PR #81): (1) `stg_fanbasis__transactions` → `fct_revenue` placeholder fill (highest-leverage); (2) period-grain rollups on top of existing wide marts; (3) net-new facts (`fct_calls_held` from Fathom; `fct_opportunity_stage_transitions` from GHL; `fct_refunds`); (4) `dim_typeform_form`; (5) Tier C marts gated on vendor-support.
- **`bq-ingest` service repair**, **Typeform `form_id` gap**, **GHL `messages` / `users` / `tasks` 0-row upstream**, **Fathom transcript landing**, **Stripe staleness**, **Fanbasis dbt wiring** — all paused per Strategic Reset; revisit during cutover resume.
- **Week-0 client asks** — unchanged (Fanbasis docs, SLA thresholds, access verification).
- **Agent kit publication** — decide later whether to push `/Users/david/Documents/agent-kit` to GitHub and import as canonical skills source.

## Where to look

- **Canonical roadmap (open PRs):** PR #80 (`Davv5/eu7-install-and-roadmap`) + PR #81 (`Davv5/Phase-B`)
- **Mart architecture rule:** `docs/discovery/coverage-matrix.md` "Mart architecture commitment" + `.claude/rules/mart-naming.md` Rule 2
- **Business-area Owner column:** `docs/discovery/business-area-map.md`
- **Existing wide marts (auto-widen on Fanbasis staging):** `2-dbt/models/marts/{lead_journey,revenue_detail,sales_activity_detail,speed_to_lead_detail}.sql`
- **Fast operating loop:** `docs/runbooks/operator-fast-loop.md`
- **Shared portable kit:** `/Users/david/Documents/agent-kit`
- **Active sprint plan:** `docs/plans/2026-04-24-strategic-reset.md`
- **Paused cutover plan:** `docs/plans/2026-04-23-001-feat-gtm-source-port-plan.md`
- **Sprint artifacts:** `docs/discovery/{source-inventory,source-shapes,staging-models,gap-analysis,insights-summary,business-area-map,coverage-matrix}.md`
- **Codex parity:** `AGENTS.md` + `.agents/skills/{ask-corpus,skill-creator,worklog}/`
- **Memories (auto-loaded):** see `MEMORY.md`
- **Engagement context / locked metric:** `CLAUDE.local.md` (gitignored overlay)
- **Portable conventions:** `CLAUDE.md` + `.claude/rules/*.md`
- **Routing rule for end-of-session:** `.claude/rules/worklog.md`
- **Full history:** `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-04-27
- WORKLOG: fired this session — captured the cross-session failure-mode learning (`mart-roadmap-rank` without scaffold audit) that doesn't fit any other destination, plus the operator-fast-loop runbook entry that was unstaged on the abandoned triage branch.
