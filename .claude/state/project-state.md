<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-22_

## Where we are

- **Phase:** Phase 5 (dashboard) shipped through v1.3 on self-hosted Metabase OSS. Phase 6 (tests/docs/CI) partially delivered; `dbt-deploy.yml` exists but blocked on `GCP_SA_KEY_PROD` secret.
- **Active branch:** `main` — four PRs just merged this session. Local checkout may be on `metabase/cos-startup-curl-hardening` (PR #44 open for follow-up review).
- **Last PRs merged to `main`:** #40 (COS hotfix), #41 (Secret Manager + BQ connection + dbt-metabase script), #42 (data-layer v1.3), #43 (dashboard v1.1–v1.3). #44 open for review.
- **Public dashboard URL:** `https://34-66-7-243.nip.io/public/dashboard/163abd8d-b16a-4f88-95b9-881a506aa461` — 14 dashcards at v1.3 state.
- **Headline metric (locked 2026-04-19):** % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes, SDR-attributed denominator. See `CLAUDE.local.md` "Locked metric" table — do not mutate without re-asking David.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-22** — Dashboard v1.3 ships 4 new rollups (outcome-by-bucket, cumulative distribution, source × outcome, day × hour coverage), P90 + volume smart-scalars, click-through drill to Lead Detail, `era_flag` dim. (`grep -n "2026-04-22" WORKLOG.md`)
- **2026-04-22** — `stl_headline_trend_weekly` median/P90/5-min-rate scoped to SDR-attributed bookings. Pre-fix showed 38k-min medians on ramping weeks because no-touch NULLs polluted the quantile pool. (`grep -n "SDR-scoped" WORKLOG.md`)
- **2026-04-22** — Data Ops + Metabase Craft corpus audits drove vocabulary refresh: snake_case column aliases → Title Case, jargon (`SLA`/`DQ`) out of tile titles, business-phrased donut categories. (`grep -n "v1.1" WORKLOG.md`)

## Open threads (what's pending)

- **PR #44** (`curl -fsS` hardening, deferred from PR #40 review) open.
- **`GCP_SA_KEY_PROD` repo secret still unset** → CI/CD `dbt-deploy.yml` blocked. Prod dbt runs go through local oauth profile today (`/tmp/dbt-oauth/profiles.yml`, David's personal ADC).
- **`dbt_metadata_sync.py` never run** — Metabase column hovers still empty. Requires `pip install dbt-metabase==1.6.0` + one manual run after `dbt parse`.
- **Mart lacks real `show_outcome` column** — three v1.3 outcome rollups fall back to `close_outcome IS NOT NULL` (over-counts `'pending'` as showed). Revisit as a future Track.
- **Roster gaps unresolved:** Ayaan Menon, Jake Lynch need role decisions; Moayad + Halle leaderboard-evidenced but not in seed. `docs/proposals/roster_update_from_oracle.md` staged, awaiting David's manual commit (no-autonomous-seed rule).
- **Metabase cruft on prod:** 3 stale cards (old names) + a renamed duplicate collection 5. Not in any active dashboard; cleanup deferred.
- **GHL PIT rotation** still owed (transcript-exposed 2026-04-19).
- **Stripe Fivetran sync gap**: 4,750 checkout sessions but zero rows in customer/charge/invoice/payment_intent.
- **Week-0 client asks** still owed (Fanbasis API docs/credentials, layered SLA thresholds, end-to-end access verification).

## Where to look (retrieval map)

- **Engagement context / client facts / locked metric:** `CLAUDE.local.md` (gitignored overlay).
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md` (path-scoped, auto-load).
- **Metabase dashboard authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py` (all tiles + layout); `ops/metabase/authoring/sync.py` (idempotent upsert helpers); `ops/metabase/authoring/infrastructure/{bigquery_connection,dbt_metadata_sync}.py`.
- **Speed-to-Lead rollups:** `dbt/models/marts/rollups/speed_to_lead/stl_*.sql` (11 rollups including headline_7d/trend_daily/trend_weekly, daily_volume_by_source, sdr_leaderboard_30d, attribution_quality_30d, lead_detail_recent, outcome_by_touch_bucket_30d, response_time_distribution_30d, source_outcome_30d, coverage_heatmap_30d).
- **Handover docs per track:** `docs/handovers/Davv5-Track-*.md` (read before touching the same area).
- **Full history:** `grep -n "^## " WORKLOG.md` lists every dated entry; `grep -n "v1\.[123]" WORKLOG.md` jumps to the dashboard evolution.
- **Corpus (free, no quota):** ask-corpus skill → `.claude/corpus.yaml` → 3 notebooks (Data Ops, Metabase Craft, D-DEE Engagement).
