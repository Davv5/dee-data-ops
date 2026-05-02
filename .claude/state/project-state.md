# Project state index

<!--
Curated project-state index. Loaded at every Claude Code SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: short, current, and
useful. WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Data Ops — Present-Moment Snapshot

_Last regenerated: 2026-05-02 after Speed-to-Lead dashboard operating view work, cross-agent operator-mode alignment, and lead-magnet mart validation._

## Where we are

- **Default operating mode:** read `.claude/rules/operator-mode.md` before planning. North star: "We use AI to help businesses make more money." Move fast, verify truth, keep one next action, avoid corporate/process bloat.
- **Active branch:** `codex/lead-magnet-mart`.
- **Dashboard PR:** #153 — `https://github.com/Davv5/dee-data-ops/pull/153` remains the Speed-to-Lead dashboard PR. The current lead-magnet branch does not have a PR yet.
- **Latest pushed dashboard commits:** `d493f14 Align agent operating memory`; `3ad4daf Clarify reached-by identity casing`; `61a6b5f Add Speed-to-Lead time filters`.
- **Local dashboard URL:** `http://localhost:3001/speed-to-lead` when the Next dev server is running from `3-bi/dashboard`.
- **Current BI surface:** `3-bi/dashboard/` is the live working app. The legacy `3-bi/metabase/` tree is historical/reference unless explicitly revived.
- **Speed-to-Lead route:** `3-bi/dashboard/src/app/speed-to-lead/page.tsx`.
- **Operating view:** `3-bi/dashboard/src/components/dashboard/SpeedToLeadOperatingView.tsx`.
- **Shell:** `3-bi/dashboard/src/components/layout/AppShell.tsx` no longer renders the sidebar; the dashboard should use the full viewport.
- **Live API:** `3-bi/dashboard/src/app/api/speed-to-lead/route.ts` backed by `src/lib/bigquery/speed-to-lead-live.ts` and `src/lib/bigquery/named-queries.ts`.
- **Time filter:** Speed-to-Lead supports `?range=today|7d|30d|90d|all`; default is `30d`, filtered by lead trigger time in Eastern time.
- **Newest dbt mart:** `2-dbt/models/marts/lead_magnet_detail.sql` on branch `codex/lead-magnet-mart`. Grain is one GHL opportunity. It treats GHL pipelines as lead-magnet/funnel lanes and uses contact opportunity windows to attribute outreach, bookings, and revenue without double-counting multi-magnet contacts.
- **Lead magnet taxonomy:** `2-dbt/seeds/lead_magnet_pipeline_taxonomy.csv` classifies 36 current GHL pipelines as true lead magnet, launch/event, waitlist, or sales/operating pipeline. All rows are initially `taxonomy_confidence = inferred_from_name`; review with David before calling the taxonomy final.
- **Lead magnet truth:** 26,229 opportunities, 15,600 contacts, 4,671 attributed bookings, $275,228.16 dev net revenue after refunds. 44.6% of contacts appear in more than one pipeline; compare all-window, first-opportunity, latest-opportunity, and category views separately.
- **Lead magnet first readout:** `docs/discovery/lead-magnet-mart-readout-2026-05-02.md`.
- **Data contract:** Speed-to-Lead v1 uses live BigQuery report/fact tables while the durable dbt mart layer stabilizes. Say this plainly in UI/docs; do not pretend the temporary path is final.
- **Active GCP project:** always pass `--project` / `--project_id` for `project-41542e21-470f-4589-96d`. Local `gcloud` defaults can be misleading.
- **Latest all-time smoke truth from live API:** 17,753 lead events; 1,350 reached by phone; 15,485 still not worked; 62 appointment-booking triggers reached within 45m out of 5,092.
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
- **2026-05-02 — Lead magnets need opportunity-window attribution.** Added `lead_magnet_detail` as the durable next mart candidate. Do not join all contact revenue to every pipeline; multi-magnet contacts are too common.
- **2026-05-02 — Dashboard should be an operating surface, not a corporate app shell.** Removed the sidebar from `AppShell`; Speed-to-Lead should use the viewport for the report.

## Open threads

- **Map the remaining 262 reached-call identities** without faking names. Goal is named-human attribution where source truth supports it; otherwise keep clear labels.
- **Add clickable drill-downs** so dashboard numbers can reveal source rows, cohorts, reps, identities, and examples behind the metric.
- **Review lead-magnet taxonomy** before dashboard comparisons: true magnet, launch/event, waitlist, sales/operating pipeline, internal/test/retired.
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
- Lead-magnet mart: `2-dbt/models/marts/lead_magnet_detail.sql`
- Lead-magnet readout: `docs/discovery/lead-magnet-mart-readout-2026-05-02.md`
- bq-ingest marts writer: `services/bq-ingest/sql/marts.sql`
- Data-layer truth: `docs/discovery/current-data-layer-truth-map.md`
- Cloud project truth: `docs/discovery/cloud-project-provenance-map.md`
- Full history: `grep -n "^## " WORKLOG.md`

## _meta

- Last regen: 2026-05-02, cross-agent operator-mode + Speed-to-Lead dashboard state + lead-magnet mart readout.
- WORKLOG: skipped — this state change is captured by the lead-magnet mart docs and should be logged when the branch is wrapped.
