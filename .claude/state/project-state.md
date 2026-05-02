# Project state index

<!--
Curated project-state index. Loaded at every Claude Code SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: short, current, and
useful. WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — Present-Moment Snapshot

_Last regenerated: 2026-05-02 after Speed-to-Lead dashboard operating view work and cross-agent operator-mode alignment._

## Where we are

- **Default operating mode:** read `.claude/rules/operator-mode.md` before planning. North star: "We use AI to help businesses make more money." Move fast, verify truth, keep one next action, avoid corporate/process bloat.
- **Active branch:** `codex/dashboard-live-speed-to-lead`, tracking `origin/codex/dashboard-live-speed-to-lead`.
- **Active PR:** #153 — `https://github.com/Davv5/dee-data-ops/pull/153`.
- **Latest pushed commits:** `3703725 Remove dashboard sidebar from operating view`; `d9baa41 Add Speed-to-Lead operating signals`; `dc45bfa Compact Speed-to-Lead operating view`.
- **Local dashboard URL:** `http://localhost:3001/speed-to-lead` when the Next dev server is running from `3-bi/dashboard`.
- **Current BI surface:** `3-bi/dashboard/` is the live working app. The legacy `3-bi/metabase/` tree is historical/reference unless explicitly revived.
- **Speed-to-Lead route:** `3-bi/dashboard/src/app/speed-to-lead/page.tsx`.
- **Operating view:** `3-bi/dashboard/src/components/dashboard/SpeedToLeadOperatingView.tsx`.
- **Shell:** `3-bi/dashboard/src/components/layout/AppShell.tsx` no longer renders the sidebar; the dashboard should use the full viewport.
- **Live API:** `3-bi/dashboard/src/app/api/speed-to-lead/route.ts` backed by `src/lib/bigquery/speed-to-lead-live.ts` and `src/lib/bigquery/named-queries.ts`.
- **Data contract:** Speed-to-Lead v1 uses live BigQuery report/fact tables while the durable dbt mart layer stabilizes. Say this plainly in UI/docs; do not pretend the temporary path is final.
- **Active GCP project:** always pass `--project` / `--project_id` for `project-41542e21-470f-4589-96d`. Local `gcloud` defaults can be misleading.
- **Latest smoke truth from live API:** 17,753 lead events; 1,350 reached by phone; 15,485 still not worked; 62 appointment-booking triggers reached within 45m out of 5,092.
- **Reached-by identity truth:** `0 Unknown` means no blank attribution labels. It does not mean every reached call is mapped to a named human. Current gap is 262 reached-call identities that still need verified mapping.
- **Aariz/Ayaan truth:** Aariz Menon (`leBv9MtltaKdfSijVEhb`, `aariz@precisionscaling.io`) and Ayaan Menon (`eWA0YcbNP3rklPwRFFwM`, `ayaan@precisionscaling.io`) are distinct GHL users. Do not collapse them because of shared last name or casing.
- **Dashboard design preference:** first screen should show what matters without wasteful scrolling. Use drill-down/clickable detail for sources, identities, examples, and explanations behind metrics.
- **Notion human home:** Data Ops Home — `https://app.notion.com/p/35405f2aff2981e29136c401ef0855eb`. Keep it light: North Star, Now, Truth Notes, Receipts, Parking Lot.
- **Dashboard product plan:** `docs/plans/2026-05-01-001-feat-dashboard-product-plan.md`.
- **Current data-layer truth map:** `docs/discovery/current-data-layer-truth-map.md` is the first read before marts/dashboard work.
- **Cloud project provenance:** `docs/discovery/cloud-project-provenance-map.md`, `docs/discovery/duplicate-data-audit-2026-05-01.md`, and `docs/discovery/source-id-comparison-audit-2026-05-01.md` are first reads before pausing/deleting/migrating legacy assets.
- **bq-ingest marts SQL:** `services/bq-ingest/sql/marts.sql`. If this changes, service deploy alone is not enough; `pipeline-marts-hourly` uses the separate Cloud Run Jobs runtime image and needs the runtime stack deploy path.

## Last 3 decisions

- **2026-05-02 — Cross-agent memory alignment.** Added `.claude/rules/operator-mode.md` and linked it from `CLAUDE.md` + `AGENTS.md` so Claude Code and Codex start from the same working agreement.
- **2026-05-02 — Dashboard should be an operating surface, not a corporate app shell.** Removed the sidebar from `AppShell`; Speed-to-Lead should use the viewport for the report.
- **2026-05-02 — Speed-to-Lead first screen is the proof pattern.** Keep compact KPI/action tiles, reached-by identity, attribution confidence, leak snapshot, aging, and audit tables. Next improvements should add source-level drill-down and remaining identity mapping.

## Open threads

- **Map the remaining 262 reached-call identities** without faking names. Goal is named-human attribution where source truth supports it; otherwise keep clear labels.
- **Add clickable drill-downs** so dashboard numbers can reveal source rows, cohorts, reps, identities, and examples behind the metric.
- **Merge/ship PR #153** after final visual smoke and user approval.
- **Choose the next revenue dashboard** after Speed-to-Lead is live. Prioritize questions that show money movement or leakage, not vanity reporting.
- **Legacy cleanup remains label-first, delete-later.** Do not pause/delete legacy projects, raw tables, jobs, or Metabase assets without checking the provenance/comparison docs and keeping rollback.
- **Stale state risk:** if a future agent starts from old `.Codex` references, correct them to `.claude` and update the stale file.

## Where to look

- Operating agreement: `.claude/rules/operator-mode.md`
- Claude Code entry point: `CLAUDE.md`
- Codex entry point: `AGENTS.md`
- Fast loop runbook: `docs/runbooks/operator-fast-loop.md`
- Speed-to-Lead dashboard code: `3-bi/dashboard/src/components/dashboard/SpeedToLeadOperatingView.tsx`
- Dashboard shell: `3-bi/dashboard/src/components/layout/AppShell.tsx`
- Speed-to-Lead live data code: `3-bi/dashboard/src/lib/bigquery/speed-to-lead-live.ts`
- Speed-to-Lead BigQuery query definitions: `3-bi/dashboard/src/lib/bigquery/named-queries.ts`
- bq-ingest marts writer: `services/bq-ingest/sql/marts.sql`
- Data-layer truth: `docs/discovery/current-data-layer-truth-map.md`
- Cloud project truth: `docs/discovery/cloud-project-provenance-map.md`
- Full history: `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-05-02, cross-agent operator-mode + Speed-to-Lead dashboard state.
- WORKLOG: skipped — this state change is captured by the new rule, the entry-point docs, and PR #153 commits.
