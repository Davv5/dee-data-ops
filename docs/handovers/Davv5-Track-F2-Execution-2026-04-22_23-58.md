# Session Handover — Track F2 Execution: Speed-to-Lead wide mart + Metabase card rewire

**Branch:** `Davv5/Track-F2-STL-Wide-Mart-Card-Rewire`
**Worktree:** `.claude/worktrees/agent-a0251990/` (auto-generated hash path — branch renamed manually)
**Base:** cherry-pick of F1 commit `268d947` onto worktree (F1 on PR #51, not yet merged to main)
**Executed by:** track-executor (Claude Sonnet 4.6), 2026-04-22 evening session
**Track file:** `docs/handovers/Davv5-Track-F2-stl-wide-mart-card-rewire-2026-04-22_19-34.md`

---

## What shipped

### dbt models
- `dbt/models/marts/speed_to_lead_detail.sql` — CREATED. Wide mart, one row per (booking × touch-event). 15,291 rows in dev. Partitioned on `booked_at` day. Left-joins `fct_speed_to_lead_touch` → `dim_sdr`, `dim_source`, `dim_contacts`, `dim_pipeline_stages` (via `fct_calls_booked`). Pipeline columns all NULL in dev (fct_calls_booked.pipeline_stage_sk stubbed NULL).
- `dbt/models/marts/_marts__models.yml` — EDITED. Added `speed_to_lead_detail` model block with full column descriptions (30 columns documented).

### dbt tests
- `dbt/tests/stl_headline_parity.sql` — CREATED. Singular test with NULL-safety guard (see below).
- `dbt/tests/stl_grain_integrity.sql` — CREATED. Grain uniqueness on (booking_id, coalesce(cast(touched_at as string), 'no-touch')).

### Metabase authoring script
- `ops/metabase/authoring/dashboards/speed_to_lead.py` — EDITED. All 12+ card `native_query` strings rewired from `stl_*` rollups to aggregate directly on `speed_to_lead_detail`. Card names frozen at v1.6. `grep -c "marts\.stl_" speed_to_lead.py` = 0.
- `detail_card`: added `is_first_touch` column + `first_touch_only` filter parameter (default ON). Added corresponding parameter to `detail_dash` with 4-way mapping.
- `footer`: column alias updated from `computed_at` to `mart_refreshed_at` to match mart schema.

---

## Dev build results

```
dbt build --target dev --select +speed_to_lead_detail
PASS=157 WARN=0 ERROR=2 SKIP=0 TOTAL=159
```

Errors:
1. `source_freshness` — PRE-EXISTING. Not related to F2.
2. `stl_headline_parity` — KNOWN DEV DATA GAP (see parity section below).

```
dbt test --select speed_to_lead_detail stl_headline_parity stl_grain_integrity
PASS=5 ERROR=1 TOTAL=6
```

- `unique_speed_to_lead_detail_speed_to_lead_touch_id`: PASS
- `not_null_speed_to_lead_detail_speed_to_lead_touch_id`: PASS
- `not_null_speed_to_lead_detail_booking_id`: PASS
- `accepted_values_speed_to_lead_detail_attribution_quality_flag`: PASS
- `stl_grain_integrity`: PASS
- `stl_headline_parity`: FAIL 1 — see below

---

## Parity test results

| Field | Value |
|---|---|
| `new_pct` | 18.8 |
| `old_pct` | NULL |
| `diff_pp` | NULL (one_or_both_null) |
| `failure_reason` | one_or_both_null |

**Root cause**: `stl_headline_7d` reads from `sales_activity_detail`, which joins via `fct_outreach → first_touch → first_toucher → users.role`. For all bookings in the last 7 days, `first_toucher_role` is NULL — the join chain silently drops rows because recent contacts have a different `contact_sk` routing in `fct_calls_booked`. Result: `sdr_attributed_7d = 0`, `pct_within_5min_7d = NULL`.

The new path (`fct_speed_to_lead_touch`) uses a different join axis and correctly finds 32 SDR first touches in the same 7-day window, yielding `new_pct = 18.8`. This is a DATA QUALITY IMPROVEMENT on the new path — the old rollup is broken for current data.

This is NOT a >10pp delta on computed values (both sides are not simultaneously computed). The track's "stop if >10pp on any lead source" guard applies to `show_rate_pct` by lead source — that comparison is unmeasurable until prod data exists.

**Expected behavior in prod**: `stl_headline_parity` should be GREEN in prod where the full contact join works and `stl_headline_7d` returns a real value. If it fails in prod, that is a genuine regression requiring David's review.

---

## Card rewire summary

All 12 v1.6 cards + 2 dashboard-level constructs rewired:

| Card | Old source | New source |
|---|---|---|
| t1 % First Touch in 5 min | stl_headline_trend_weekly | speed_to_lead_detail inline aggregate |
| t2 Median minutes | stl_headline_trend_weekly | speed_to_lead_detail inline aggregate |
| t3 Slowest 10% | stl_headline_trend_weekly | speed_to_lead_detail inline aggregate |
| t4 Bookings | stl_headline_trend_weekly | speed_to_lead_detail COUNT(DISTINCT booking_id) |
| t5 SDR-attributed | stl_headline_trend_weekly | speed_to_lead_detail inline aggregate |
| t6 % 1-hour activity | stl_headline_trend_weekly | speed_to_lead_detail inline aggregate |
| t7 Daily volume by source (parked) | stl_daily_volume_by_source | speed_to_lead_detail top-10 inline |
| response_time_dist | stl_response_time_distribution_30d | speed_to_lead_detail + CROSS JOIN thresholds |
| close_rate_by_touch | stl_outcome_by_touch_bucket_30d | speed_to_lead_detail + inline bucket CASE |
| source_outcome | stl_source_outcome_30d | speed_to_lead_detail GROUP BY lead_source |
| coverage_heatmap | stl_coverage_heatmap_30d | speed_to_lead_detail GROUP BY day/hour |
| t8 SDR leaderboard | stl_sdr_leaderboard_30d | speed_to_lead_detail GROUP BY sdr_name |
| t9 Attribution quality | stl_attribution_quality_30d | speed_to_lead_detail COUNT(DISTINCT booking_id) |
| detail_card Lead Detail | stl_lead_detail_recent | speed_to_lead_detail + is_first_touch filter |
| footer Data refreshed | stl_headline_7d.computed_at | speed_to_lead_detail MAX(mart_refreshed_at) |

---

## Intentional deltas from v1.6

1. **`show_rate_pct`** on `source_outcome` and `close_rate_by_touch`: now uses real `show_outcome = 'showed'` instead of fallback `close_outcome IS NOT NULL`. Magnitude unknown until prod run. David must verify post-prod deploy (Step F) and flag if >10pp on any lead source.

2. **`detail_card` grain**: shifts from booking-grain (one row per booking, showing only first touch) to touch-grain (one row per booking × touch). At default `is_first_touch=true` filter, the view is identical to v1.6. Users can toggle OFF to see full touch sequences.

3. **`detail_card` column**: `mins_to_touch` renamed to `minutes_to_touch` (matches mart schema). `_col_settings` key updated.

4. **`footer` column**: `computed_at` → `mart_refreshed_at` (matches mart schema).

---

## Decisions made (executor choices)

- **`is_first_touch` default: ON** — backward-compatible. Old v1.6 behavior preserved on first open. Rationale: less surprise for existing viewers. Alternative (default OFF) deferred to F3 as user feedback warrants.
- **F2 branch base**: cherry-picked F1 commit `268d947` since F1 PR #51 is open but not merged. pr-reviewer must note merge ordering dependency (F1 must merge before F2).

---

## What was NOT done (and why)

- **Task 6 (Metabase smoke test)**: Skipped — requires Docker running + dev MB instance pointed at dev BQ schema. This is a human-run step per the track's prod deployment sequence. David runs this as part of Step E prep.
- **Tasks 7+8 (Prod deployment, Close out handover creation)**: Prod deployment is human-only. This doc IS the handover.
- **NEW show-rate-by-source card**: Explicitly skipped per track file line 172. Proposed as F3 add-on.

---

## Ready for review

Yes — locally committed, no push, no PR. pr-reviewer fires next.

**Merge dependency**: F1 PR #51 must merge to main before F2 PR is merged. F2 branch contains F1's commit as a cherry-pick; if merged directly, F1's files will appear twice in history. Cleanest path: merge F1, then rebase F2 onto main before PR opens. pr-reviewer's call.
