<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-23 (close-out)_

## Where we are

- **Metabase Learn Implementation: CLOSED.** Speed-to-Lead dashboard refactor landed end-to-end — fct + conformed dims + wide mart `speed_to_lead_detail` in prod; 15 Metabase cards aggregating the wide mart directly; 11 legacy `stl_*` rollups dropped from prod BQ. Public dashboard live on star schema.
- **`main` tip:** `cc9b1ad` (F3 merge). Deploy on `cc9b1ad` GREEN. 7 PRs merged this session: #55 F3, #56 `is_first_touch` fix, #57 source_freshness narrow, #59 Track X staging, #60 `--dry-run`, #62 `cache_ttl=0`. Plus #51 (F1) and #52 (F2) earlier.
- **Open PRs:** this chore worklog #61; #50 (Track E — author resolves speed_to_lead.py conflict); #44 (curl hardening, stale).
- **Public dashboard URL:** `https://34-66-7-243.nip.io/public/dashboard/163abd8d-b16a-4f88-95b9-881a506aa461`. Live on `speed_to_lead_detail` (15/15 cards, verified via Metabase REST API).
- **Headline metric (locked 2026-04-19):** % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes, SDR-attributed denominator. Last prod parity check before F3 retired the test: GREEN on `2be3675`.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-23 close-out** — Speed-to-Lead star-schema refactor declared CLOSED after live dashboard verification + 11-table drop. (`grep -n "CLOSED" WORKLOG.md`)
- **2026-04-23** — `cache_ttl=0` serialized as payload omission; Metabase OSS v0.60.1 rejects explicit 0. Track Z assumption corrected. (`grep -n "cache_ttl" WORKLOG.md`)
- **2026-04-23** — `is_first_touch` = overall earliest touch, any role; paired with `is_sdr_touch` reproduces legacy `first_toucher_role = 'SDR'` filter. Root-caused the 2.6pp prod parity divergence. (`grep -n "is_first_touch" WORKLOG.md`)

## Open threads (outside Speed-to-Lead refactor)

- **Track X operational bringup** — Cloud Run Calendly poller needs secret creation + Docker smoke + `terraform apply` per `docs/runbooks/calendly-cloud-run-extractor.md`. Staging fix (#59) is forward-compatible; nothing flows through the poller until bringup.
- **PR #50 (Track E)** has merge conflicts on `ops/metabase/authoring/dashboards/speed_to_lead.py` from F2's rewrite. Track E author resolves; freshness tile already live via Track Z (PR #54).
- **PR #44** (curl hardening) still open from earlier session.
- **`dbt_metadata_sync.py` first-run** deferred — populates Metabase column tooltips from dbt docs; not blocking dashboard function.
- **SMTP bootstrap** owed before `dashboard_subscriptions.py` can create the Monday 06:00 ET digest.
- **`MB_ENABLE_QUERY_CACHING=true`** env var owed on prod VM for server-wide caching.
- **Public URL retires** when SDR/Manager Metabase accounts come online.
- **Roster gaps unresolved:** Ayaan Menon, Jake Lynch need role decisions; Moayad + Halle leaderboard-evidenced but not in seed. `dim_source.is_paid` NULL for 98/111 campaign labels.
- **Stripe Fivetran sync gap:** 4,750 checkout_sessions, zero rows in customer/charge/invoice/payment_intent; `source_freshness` narrowed to exclude.
- **GHL PIT rotation** still owed (transcript-exposed 2026-04-19).
- **Week-0 client asks** still owed (Fanbasis API docs/credentials, layered SLA thresholds, end-to-end access verification).

## Where to look (retrieval map)

- **Engagement context / client facts / locked metric:** `CLAUDE.local.md` (gitignored overlay).
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md` (path-scoped, auto-load).
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks (Data Ops, Metabase Craft, Metabase Learn, D-DEE Engagement). Scope routing in `.claude/rules/using-the-notebook.md`.
- **Star-schema marts:** `dbt/models/warehouse/facts/fct_speed_to_lead_touch.sql` (lowest grain), `dbt/models/warehouse/dimensions/{dim_sdr,dim_source}.sql`, `dbt/models/marts/speed_to_lead_detail.sql`. `stl_data_freshness` kept at `dbt/models/marts/rollups/speed_to_lead/`.
- **Metabase authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py` (tiles + layout); `ops/metabase/authoring/{sync,client}.py` — `--dry-run` flag supported. `ops/metabase/authoring/infrastructure/{bigquery_connection,dbt_metadata_sync,caching_config,dashboard_subscriptions}.py`.
- **Handover docs per track:** `docs/handovers/Davv5-Track-*.md`.
- **Full history:** `grep -n "^## " WORKLOG.md`; `grep -n "CLOSED" WORKLOG.md` surfaces the close-out entry.
- **Corpus (free, no quota):** ask-corpus skill → `.claude/corpus.yaml` → 4 notebooks.
