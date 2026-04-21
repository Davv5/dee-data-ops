<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-21_

## Where we are

- **Phase:** Phase 4 (marts) shipping; Phase 5 (Looker Studio dashboard) started; Phase 6 (tests/docs/CI) partially delivered via Tracks G/J/K.
- **Active branch:** `phase-1-5/ghl-messages-extractor` (worktree cuts branch off of it for Track S). `main` has the merged CI/CD + marts + rollups work.
- **Last PR merged to `main`:** #33 `looker-studio: Page 1 click-spec + 6 Speed-to-Lead rollup views` (commit `b705961`).
- **Headline metric (locked 2026-04-19):** % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes. See `CLAUDE.local.md` "Locked metric" table — do not mutate without re-asking David.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-20** — Pivoted dashboard to **Looker Studio** over Evidence/Hashboard; 6 pre-aggregated rollups under `dbt/models/marts/rollups/speed_to_lead/` feed a click-spec'd report. (`grep -n "Pivot to Looker Studio" WORKLOG.md`)
- **2026-04-20** — Track E shipped the warehouse layer: 4 new dims + 3 facts + 1 bridge + 1 SCD2 snapshot; bridge match-rate 94%/3%/2% on 3,375 Stripe charges. (`grep -n "Track E: warehouse" WORKLOG.md`)
- **2026-04-20** — Track D reconciled Calendly-grain for the booking denominator; `dim_pipeline_stages.is_booked_stage` is a dim attribute, not a metric grain. (`grep -n "Track D:" WORKLOG.md`)

## Open threads (what's pending)

- Build Looker Studio rollups in prod (`dbt build --target prod --select path:models/marts/rollups/speed_to_lead`), then render Page 1 tiles per `docs/looker-studio/page-1-speed-to-lead.md`.
- Page 2 (`lead_journey`) + Page 3 (`revenue_detail`) click-specs owed after Page 1 proof-of-life.
- **Week-0 client asks still owed:** Fanbasis API docs/credentials, layered SLA thresholds, end-to-end access verification. See `CLAUDE.local.md` "Week-0 asks" section.
- **GHL PIT rotation** (transcript-exposed 2026-04-19): secret-manager migration landed via Track J; David still needs to regenerate the PIT and run the 7 manual steps in WORKLOG 2026-04-20 Track J entry.
- Roster gaps: Ayaan Menon, Jake Lynch role unresolved; Moayad + Halle flagged by oracle cross-reference. Do NOT edit `dbt/seeds/ghl_sdr_roster.csv` autonomously.
- `SLACK_WEBHOOK_URL` + `GCP_SA_KEY_PROD` repo secrets not yet set (CI/CD + observability blocked on these).
- Stripe Fivetran connector sync gap: 4,750 checkout sessions but zero rows in customer/charge/invoice/payment_intent.

## Where to look (retrieval map)

- **Engagement context / client facts / locked metric:** `CLAUDE.local.md` (gitignored overlay).
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md` (path-scoped, auto-load).
- **Scope / plan:** `client_v1_scope_speed_to_lead.md`, `v1_build_plan.md`.
- **Dashboard specs:** `docs/looker-studio/page-1-speed-to-lead.md`, `docs/looker-studio/theme.md`, `docs/looker-studio/README.md`.
- **Handover docs per track:** `docs/handovers/Davv5-Track-*.md` (one per executed track; read before touching the same area).
- **Full history:** `grep -n "^## " WORKLOG.md` lists every dated entry; `grep -n "Track X" WORKLOG.md` jumps to a specific track.
- **Corpus (free, no quota):** NotebookLM `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a` via `.claude/skills/ask-corpus/` before writing rules or scaffolding.
