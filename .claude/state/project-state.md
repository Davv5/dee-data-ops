<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-22 (pm)_

## Where we are

- **Phase:** Phase 5 (dashboard) — v1.6 shipped on Metabase OSS via `main`; v1.3.1 polish (Track D caching + Track E authoring polish) in flight. Phase 6 (tests/docs/CI) partial; `dbt-deploy.yml` still blocked on `GCP_SA_KEY_PROD`.
- **`main` tip:** `13ad334` (Track C merge). Seven PRs landed this session: #40 COS hotfix, #41 auth infra, #42 data-layer v1.3, #43 dashboard v1.1–v1.3, #46 Track A storytelling v1.4, #47 Track B hero v1.5, #48 Track C vocabulary/heatmap/orphan v1.6.
- **Active branch:** `chore/worklog-2026-04-22` (this session's worklog PR #45).
- **Open PRs:** #49 Track D caching + subscription scaffold; #45 this chore worklog; #44 curl hardening (deferred from #40).
- **Public dashboard URL:** `https://34-66-7-243.nip.io/public/dashboard/163abd8d-b16a-4f88-95b9-881a506aa461` — v1.6 state. Public link must retire when auth is introduced (permissions research 2026-04-22 pm).
- **Headline metric (locked 2026-04-19):** % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes, SDR-attributed denominator. See `CLAUDE.local.md` "Locked metric" table — do not mutate without re-asking David.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-22 pm** — OSS data-permissions pattern = native SQL leaderboard + 3 groups / 3 collections + `Blocked` DB perms; sandboxes are Pro-only. (`grep -n "OSS permissions" WORKLOG.md`)
- **2026-04-22 pm** — Metabase Learn split into its own notebook (149 sources) vs Metabase Craft (14 ops sources). New scope `methodology.metabase_learn`. (`grep -n "Metabase Learn" WORKLOG.md`)
- **2026-04-22 pm** — Ship Track D as partial: per-dashboard `cache_ttl` persists on OSS (corpus gap logged); server-wide caching needs env var + restart; SMTP-guarded subscription scaffold committed. (`grep -n "Track D" WORKLOG.md`)

## Open threads (what's pending)

- **PR #49 (Track D)** awaiting merge.
- **Track E** executor committed on `Davv5/Track-E-STL-v131-Authoring-Polish` (`f56415d`) — filters, dividers, footer, freshness tile, `stl_data_freshness` rollup. pr-reviewer not yet fired.
- **Metabase Docs notebook planned** — next session: rename "Metabase Craft" → "Metabase Docs" + ingest full `metabase.com/docs` crawl. David supplies CSV.
- **`MB_ENABLE_QUERY_CACHING=true`** env var owed on prod VM (docker-compose edit + restart), then rerun `caching_config.py` to flip server-wide caching.
- **SMTP bootstrap** owed (SendGrid free tier recommended) before `dashboard_subscriptions.py` can create the Monday 06:00 ET digest.
- **Public URL retires** when SDR/Manager accounts come online — public links bypass user-based perms.
- **PR #44** (`curl -fsS` hardening) open.
- **`GCP_SA_KEY_PROD` repo secret still unset** → CI/CD `dbt-deploy.yml` blocked.
- **`dbt_metadata_sync.py` never run** — Metabase column hovers still empty.
- **Mart lacks real `show_outcome` column** — three v1.3 outcome rollups fall back to `close_outcome IS NOT NULL`.
- **Roster gaps unresolved:** Ayaan Menon, Jake Lynch need role decisions; Moayad + Halle leaderboard-evidenced but not in seed. `docs/proposals/roster_update_from_oracle.md` staged awaiting David's manual commit.
- **Metabase cruft on prod:** 3 stale cards + a renamed duplicate collection 5 (not on any active dashboard).
- **GHL PIT rotation** still owed (transcript-exposed 2026-04-19).
- **Stripe Fivetran sync gap**: 4,750 checkout sessions, zero rows in customer/charge/invoice/payment_intent.
- **Week-0 client asks** still owed (Fanbasis API docs/credentials, layered SLA thresholds, end-to-end access verification).

## Where to look (retrieval map)

- **Engagement context / client facts / locked metric:** `CLAUDE.local.md` (gitignored overlay).
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md` (path-scoped, auto-load).
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks now (Data Ops, Metabase Craft, **Metabase Learn** (new), D-DEE Engagement). Scope routing in `.claude/rules/using-the-notebook.md`.
- **Metabase dashboard authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py` (all tiles + layout); `ops/metabase/authoring/sync.py` (idempotent upsert); `ops/metabase/authoring/infrastructure/{bigquery_connection,dbt_metadata_sync,caching_config,dashboard_subscriptions}.py`.
- **Speed-to-Lead rollups:** `dbt/models/marts/rollups/speed_to_lead/stl_*.sql` (headline_7d, trend_daily, trend_weekly, daily_volume_by_source, sdr_leaderboard_30d, attribution_quality_30d, lead_detail_recent, outcome_by_touch_bucket_30d, response_time_distribution_30d, source_outcome_30d, coverage_heatmap_30d; + `stl_data_freshness` inbound via Track E).
- **Handover docs per track:** `docs/handovers/Davv5-Track-*.md` (read before touching the same area).
- **Full history:** `grep -n "^## " WORKLOG.md` lists every dated entry; `grep -n "Track [A-E]" WORKLOG.md` jumps to track-specific entries; `grep -n "v1\.[1-6]" WORKLOG.md` walks the dashboard evolution.
- **Corpus (free, no quota):** ask-corpus skill → `.claude/corpus.yaml` → 4 notebooks.
