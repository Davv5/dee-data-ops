# Session Handover — Track F3 Execution: stl_* rollup deprecation

**Branch:** `Davv5/Track-F3-STL-Rollup-Deprecation`
**Worktree:** `.claude/worktrees/agent-a3190dab/` (renamed from `worktree-agent-a3190dab`)
**Timestamp:** `2026-04-22_23-58`
**Executor:** Claude Sonnet 4.6
**Status:** LOCALLY COMMITTED — ready for DRAFT PR (do NOT un-draft until F2 prod gate met)

---

## What was done

### Files DELETED (12 total)

11 rollup SQL files:
- `dbt/models/marts/rollups/speed_to_lead/stl_headline_7d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_daily.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_weekly.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_daily_volume_by_source.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_sdr_leaderboard_30d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_attribution_quality_30d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_lead_detail_recent.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_outcome_by_touch_bucket_30d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_response_time_distribution_30d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_source_outcome_30d.sql`
- `dbt/models/marts/rollups/speed_to_lead/stl_coverage_heatmap_30d.sql`

1 parity test:
- `dbt/tests/stl_headline_parity.sql`

### Files CREATED (2)

- `dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql` — sourced from Track E branch (commit f56415d). Feeds the "Data as of" freshness tile. NOT a rollup; kept intentionally.
- `docs/handovers/Davv5-Track-F3-Execution-2026-04-22_23-58.md` — this file.

### Files MODIFIED (4)

- `dbt/models/marts/rollups/speed_to_lead/_stl__models.yml` — stripped from 11 model blocks to 1 (`stl_data_freshness` only). `grep -c "^  - name:"` returns `1`.
- `.claude/rules/mart-naming.md` — appended "Fact-first-then-wide-mart refactors" Lessons Learned bullet.
- `.claude/state/project-state.md` — regenerated to reflect F3 state; removed stale "dbt_metadata_sync.py never run" + "Mart lacks real show_outcome" as primary threads; replaced with current-state notes.
- `WORKLOG.md` — prepended F3 entry.

### Files KEPT (explicit)

- `dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql` — NOT deleted.
- `dbt/tests/stl_grain_integrity.sql` — NOT deleted (live contract on `speed_to_lead_detail`).

---

## dbt build --target dev results

Post-deletion build:
- **PASS=222 WARN=2 ERROR=1 SKIP=11**
- ERROR: `source_freshness` — pre-existing data-staleness check; confirmed failing before F3 changes.
- WARN: `release_gate_revenue_detail` + `release_gate_sales_activity_detail` — pre-existing, unrelated to F3.
- `speed_to_lead_detail` — PASS (15.3k rows)
- `stl_data_freshness` — PASS (1 row)
- `stl_grain_integrity` — PASS
- Zero compile errors from deleted rollup models

---

## Tasks NOT completed (human-only or blocked)

### dbt_metadata_sync.py (human step post-merge)

Per David's explicit authorization: the track file's metadata-sync step is HUMAN-ONLY. First-ever sync against prod Metabase is a Big Event (all column descriptions on all tables will populate simultaneously). David must run:

```bash
source .venv/bin/activate
set -a && source ops/metabase/.env.metabase && set +a   # prod Metabase URL + token
cd dbt && dbt docs generate --target prod
python -m ops.metabase.authoring.infrastructure.dbt_metadata_sync
```

### bq rm prod table drops (human step post-merge)

dbt deletion of a model does NOT drop the BQ table — it stops rebuilding it. David must manually drop the 11 stale tables after F3 merges and `dbt build --target prod` has run:

```bash
for tbl in stl_headline_7d stl_headline_trend_daily stl_headline_trend_weekly \
           stl_daily_volume_by_source stl_sdr_leaderboard_30d stl_attribution_quality_30d \
           stl_lead_detail_recent stl_outcome_by_touch_bucket_30d stl_response_time_distribution_30d \
           stl_source_outcome_30d stl_coverage_heatmap_30d; do
  bq rm -f -t "dee-data-ops-prod:marts.$tbl"
done
```

Rollback: BQ time-travel (7 days default) via `FOR SYSTEM_TIME AS OF` if needed.

### Metabase orphan cleanup (skip — F2 not yet merged)

The track file's Task 5 (verify orphan cleanup in dev Metabase) was skipped because Track E's `speed_to_lead.py` (which contains the orphan-cleanup block updates) is not yet in this branch. The F2 `speed_to_lead.py` version (in this branch) predates Track E's freshness tile + filter additions. No new orphans are expected from F3 (F3 deletes SQL files, not Metabase cards — F2 already rewired the cards away from the rollups).

---

## STOP-AND-ASK: show_outcome fallback question

The track file flags this as a "must stop and ask David" item:

> Should `fct_speed_to_lead_touch`'s `show_outcome` fallback (if F1 shipped the fallback variant) be fixed in F3, or deferred to Track G?

**Context:** F2 already shipped `speed_to_lead_detail` with real `show_outcome = 'showed'` (not the `close_outcome IS NOT NULL` fallback). The old rollup YAML had comments about the fallback — those are now retired with the rollups. The question is: is there any remaining Track G work for `show_outcome`, or is F2's behavior sufficient?

**Executor action:** NOT resolved. Reported here for David's decision.

---

## Merge gate (DRAFT PR)

F3 PR must stay DRAFT until ALL of:
1. F2 (PR #52) merged to main
2. `dbt build --target prod` run on F2 (human step)
3. `stl_headline_parity` green in prod for at least one `dbt-prod-daily` refresh cycle
4. David explicitly un-drafts

---

## Baseline state note

This worktree was created via `isolation: "worktree"` (Agent tool fallback), not the pre-created readable-path pattern in `.claude/rules/agents.md` Rule 1. The branch was renamed from `worktree-agent-a3190dab` to `Davv5/Track-F3-STL-Rollup-Deprecation` during this session. The F2 branch (`Davv5/Track-F2-STL-Wide-Mart-Card-Rewire`) was merged into this branch via fast-forward before F3 changes were made — so this branch contains all F1 + F2 + F3 work.
