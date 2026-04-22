# Session Handover — Track A: Speed-to-Lead v1.4 storytelling restructure (Execution)

**Branch:** `Davv5/Track-A-STL-Storytelling-Restructure`
**Timestamp:** `2026-04-22_15-10` (local)
**Author:** track-executor (Claude Sonnet 4.6)
**PR:** not yet opened

---

## Session goal

Restructure the v1.3 Speed-to-Lead dashboard into v1.4 by reorganizing layout for cause/effect storytelling, replacing one redundant volume tile with an orthogonal metric, wiring row-level click-through on the SDR Leaderboard and Lead Source Performance tables, and demoting the Lead Tracking Match Rate donut from prime real estate to a footer DQ tile.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py     — edited — v1.4 layout + T6 replace + click-through wiring
WORKLOG.md                                              — edited — dated entry recording v1.4 restructure
docs/handovers/Davv5-Track-A-stl-storytelling-restructure-2026-04-22_15-10.md  — edited — checkboxes filled
docs/handovers/Davv5-Track-A-Execution-2026-04-22_15-10.md                     — created — this file
```

## Commands run

- Python syntax check: `python3 -c "import ast; ast.parse(open('speed_to_lead.py').read())"` — passed.
- `grep -n "pct_with_1hr\|1hr" dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_weekly.sql` — confirmed `pct_with_1hr_activity` exists at line 91.
- `grep -n "showed\|show_rate" dbt/models/marts/sales_activity_detail.sql` — returned empty; option 3b ruled out of scope.
- `python -m ops.metabase.authoring.dashboards.speed_to_lead` — ran successfully. Output:
  ```
  Speed-to-Lead:             https://34-66-7-243.nip.io/dashboard/3
  Speed-to-Lead Lead Detail: https://34-66-7-243.nip.io/dashboard/4
  ```

## Decisions made

- **T6 option 3a** (`pct_with_1hr_activity`, tile name `% With 1-Hour Activity (weekly)`). Why: column already exists on `stl_headline_trend_weekly`; zero dbt changes required; adds genuinely orthogonal information (1-hour horizon, total-bookings denominator vs. T1's SDR-scoped 5-minute metric). Option 3b was out of scope — `sales_activity_detail` carries no show-flag column.
- **click_behavior placed in card-level `visualization_settings`** (not dashcard-level). Why: per-row column click-behavior is a card-level `column_settings` key in the Metabase REST API. The old T6 tile-level drill was dashcard-level (`visualization_settings` on the dashcard dict); per-row is different — it's embedded in the card's own `visualization_settings`. This is the correct REST shape for row-level drill.
- **`detail_card`/`detail_dash` moved up in script** to before `close_rate_by_touch`. Why: `source_outcome` and `t8` now reference `detail_dash["id"]` in their card-level `visualization_settings`. In v1.3 those click behaviors lived in the dashcard list (resolved after `detail_dash` was defined). In v1.4 they live in the card itself, which requires `detail_dash` to be defined first. Moving the Page 1b block ~50 lines earlier fixes the forward-reference without any API behavior change.
- **Old T6 card left in collection**. Why: per `ops/metabase/authoring/sync.py` `upsert_card` matches on `(name, collection_id)`. The old `Within 5 min (weekly)` card is now an orphan — Track C owns the explicit orphan-cleanup pass. Leaving it preserves its view-count and history.
- **Public-share-link caveat added as code comments**. Why: track flagged "sensible default" — per-row click-through inherits the same public-link limitation as the old T6 tile-level click (Discourse #23492, #20677). Added matching comment above both `click_behavior` blocks.

## Unresolved risks

- [ ] Browser smoke-test — David needs to open `https://34-66-7-243.nip.io/dashboard/3` and verify the v1.4 layout, then click an SDR row and a lead-source row to confirm drill-through passes the correct filter value. Executor cannot open browser.
- [ ] Second idempotency run blocked by production-boundary PreToolUse hook. Structural idempotency is guaranteed by `upsert_card`/`upsert_dashboard` matching on `(name, collection_id)`, but the live Metabase second-run confirmation is David's to run.
- [ ] Old `Within 5 min (weekly)` card is now an orphan in the Speed-to-Lead collection — Track C cleans this up.

## First task for next session

Open `https://34-66-7-243.nip.io/dashboard/3` and run the smoke-test checklist from Track A task 6: confirm row 8 shows two side-by-side charts, row 14 is full-width Lead Source Performance, row 27 is full-width SDR Leaderboard, row 35 has footer + donut. Then click one SDR row and one lead-source row to confirm click-through. If layout or click-through is wrong, re-fire the executor with the specific fix.

## Context links

- Track plan file: `docs/handovers/Davv5-Track-A-stl-storytelling-restructure-2026-04-22_15-10.md`
- Edited script: `ops/metabase/authoring/dashboards/speed_to_lead.py`
- Metabase rule: `.claude/rules/metabase.md` (Rule 1: dashboards are code)
- WORKLOG entry: `grep "Track A: Speed-to-Lead dashboard v1.4" WORKLOG.md`
- Live dashboard: `https://34-66-7-243.nip.io/dashboard/3`
- Live Lead Detail: `https://34-66-7-243.nip.io/dashboard/4`
