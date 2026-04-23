<!--
Curated project-state index. Loaded at every SessionStart via
`.claude/scripts/sessionstart-inject-state.sh`. Target: 40-60 lines.
Regenerate at session end per `.claude/rules/worklog.md`. Newest decisions at top.
WORKLOG.md is the append-only audit log; grep it for history.
-->

# D-DEE Speed-to-Lead — present-moment snapshot

_Last regenerated: 2026-04-22 (evening — F3 executor)_

## Where we are

- **Phase:** Phase 5 (dashboard) — Speed-to-Lead star-schema refactor Tracks F1/F2/F3 in flight. F1 (PR #51) merged. F2 (PR #52) awaiting merge to main. F3 code-ready on branch `Davv5/Track-F3-STL-Rollup-Deprecation` (draft PR pending after F2 merge + prod gate).
- **`main` tip:** `a1fd57c` (F1 merge). Open PRs: F2 (#52), Track E (#50), Track D (#49).
- **Active branch (F3 worktree):** `Davv5/Track-F3-STL-Rollup-Deprecation`.
- **Public dashboard URL:** `https://34-66-7-243.nip.io/public/dashboard/163abd8d-b16a-4f88-95b9-881a506aa461` — v1.6 state.
- **Headline metric (locked 2026-04-19):** % of Calendly-booked calls with a human SDR CALL/SMS touch within 5 minutes, SDR-attributed denominator.

## Last 3 decisions (full entries in WORKLOG.md)

- **2026-04-22 F3** — Delete 11 stl_* rollup SQLs + retire stl_headline_parity; keep stl_data_freshness. (`grep -n "Track F3" WORKLOG.md`)
- **2026-04-22 F2** — wide mart speed_to_lead_detail ships; Metabase cards rewired from rollups to direct SQL on the mart. (`grep -n "Track F2" WORKLOG.md`)
- **2026-04-22 F1** — fct_speed_to_lead_touch + dim_sdr + dim_source additive-only (PR #51 merged). (`grep -n "Track F1" WORKLOG.md`)

## Open threads (what's pending)

- **F2 (PR #52) awaiting merge** — must merge before F3 PR can un-draft.
- **F3 prod gate** — F3 PR is DRAFT until: F2 deployed to prod + `stl_headline_parity` green in prod for one refresh cycle. David un-drafts manually.
- **`dbt_metadata_sync.py` not yet run against prod** — human step post F3 merge (documented in F3 PR body). Column hovers in Metabase still empty.
- **`show_outcome` fallback: defer to Track G or fix now?** — STOP-AND-ASK open question per F3 track file. `speed_to_lead_detail` uses real `show_outcome = 'showed'` (F2 shipped this); the rollup YAML comments mentioning the old fallback are now retired. Confirm with David whether any Track G work is needed.
- **PR #50 (Track E)** awaiting merge — dashboard filters + freshness tile + stl_data_freshness rollup.
- **PR #49 (Track D)** awaiting merge.
- **`MB_ENABLE_QUERY_CACHING=true`** env var owed on prod VM.
- **SMTP bootstrap** owed before `dashboard_subscriptions.py` can create digest.
- **Public URL retires** when SDR/Manager accounts come online.
- **`GCP_SA_KEY_PROD` repo secret still unset** → CI/CD `dbt-deploy.yml` blocked.
- **Roster gaps:** Ayaan Menon, Jake Lynch role decisions; Moayad + Halle not in seed.
- **GHL PIT rotation** still owed (transcript-exposed 2026-04-19).
- **Stripe Fivetran sync gap**: 4,750 checkout sessions, zero downstream rows.

## Where to look (retrieval map)

- **Engagement context / client facts / locked metric:** `CLAUDE.local.md` (gitignored).
- **Portable conventions:** `CLAUDE.md` + `dbt_style_guide.md` + `.claude/rules/*.md`.
- **Corpus declaration:** `.claude/corpus.yaml` — 4 notebooks (Data Ops, Metabase Craft, Metabase Learn, D-DEE Engagement).
- **Speed-to-Lead mart:** `dbt/models/marts/speed_to_lead_detail.sql` + `_marts__models.yml`. Rollup layer now contains only `stl_data_freshness` (freshness tile). Old 11 rollups deleted in F3.
- **Speed-to-Lead warehouse layer:** `dbt/models/warehouse/facts/fct_speed_to_lead_touch.sql` + `dim_sdr.sql` + `dim_source.sql`.
- **Singular tests (live):** `dbt/tests/stl_grain_integrity.sql` (speed_to_lead_detail grain contract). `stl_headline_parity.sql` RETIRED in F3.
- **Metabase dashboard authoring:** `ops/metabase/authoring/dashboards/speed_to_lead.py` (all tiles — rewired to speed_to_lead_detail in F2).
- **Handover docs:** `docs/handovers/Davv5-Track-F[123]-*.md`.
- **Full history:** `grep -n "^## " WORKLOG.md`.
