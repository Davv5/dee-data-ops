# Session Handover — Track A: Speed-to-Lead v1.4 storytelling restructure

**Branch:** `Davv5/Track-A-STL-Storytelling-Restructure`
**Worktree:** `.claude/worktrees/track-A-stl-storytelling/`
**Base commit:** `a48c204` (`main` at planning time, 2026-04-22 15:10 local)
**Timestamp:** `2026-04-22_15-10` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Sequencing constraint — READ FIRST

This track is **the first of three** that all edit `ops/metabase/authoring/dashboards/speed_to_lead.py`. Tracks B and C are blocked on this one merging to `main`.

- Base your worktree off `main` at SHA `a48c204` (the v1.3 ship commit).
- When you open the file, the layout map in the comment block at lines 482–495 should describe v1.3 exactly: header at row 0, T1/T2/T3 at row 2, T4/T5/T6 at row 5, response-time at row 8 (24 wide), close-rate + lead-source paired at row 14, heatmap at row 20, leaderboard + match-rate donut at row 27, footer at row 35. **If the file is already past v1.3 (e.g., a hero card at row 2, or close-rate has moved to row 8), STOP AND ASK** — Track B or C may have landed first and rebasing semantics changed.
- Do NOT branch from a feature branch. Always `main`.

## Session goal

Restructure the v1.3 Speed-to-Lead dashboard into v1.4 by reorganizing layout for cause/effect storytelling, replacing one redundant volume tile, wiring row-level click-through on the SDR Leaderboard and Lead Source Performance tables, and demoting the Lead Tracking Match Rate donut from prime real estate to a small DQ tile near the footer.

The locked headline metric (CLAUDE.local.md "Locked metric: Speed-to-Lead v1") MUST NOT change — only the visual organization of how it is presented.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py     — edited — v1.4 layout + new T6 + click-behavior wiring
WORKLOG.md                                              — edited — dated entry recording v1.4 restructure
docs/handovers/Davv5-Track-A-Execution-<timestamp>.md   — created — handover doc per docs/handovers/TEMPLATE.md
```

If item 3 below resolves toward "Show Rate (weekly)" rather than "% Reached Within 1 Hr (weekly)" for the new T6, also:

```
dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_weekly.sql  — edited — add show_rate_pct column
dbt/models/marts/rollups/speed_to_lead/_rollups__models.yml           — edited — column-level test + description
```

— and run `dbt build --target dev --select stl_headline_trend_weekly+` before re-running the authoring script.

## Tasks (ordered)

### 1. Pair cause and effect at row 8

- [x] In the dashcards list (around lines 496–522 of `speed_to_lead.py`), change `response_time_dist` from `"col": 0, "size_x": 24` to `"col": 0, "size_x": 12` at row 8.
- [x] Move `close_rate_by_touch` from row 14 to row 8: `{"row": 8, "col": 12, "size_x": 12, "size_y": 6, ...}`.
- [x] Update the layout-map comment block (lines 482–495) to reflect the new pairing.

### 2. Lead Source Performance gets row 14 to itself

- [x] Move `source_outcome` from `(14, 12, 12, 6)` to `(14, 0, 24, 6)`.
- [x] Drop the now-empty right-half slot at row 14.

### 3. Replace T6 with an orthogonal weekly metric

T6 today is `Within 5 min (weekly)` — the raw count numerator of T1's percentage. Same data twice. Pick ONE of the two replacements below — **executor may pick sensible default based on the corpus check below and note the choice in the commit message**:

**Option 3a (preferred, zero dbt edit):** Use `pct_with_1hr_activity` — already on `stl_headline_trend_weekly` (line 91 of `dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_weekly.sql`). Tile name: `% With 1-Hour Activity (weekly)`. Adds new information: shows reachability on a longer horizon than the 5-minute SLA, denominated on TOTAL bookings (not SDR-attributed), so it's also a different denominator from T1.
- Edit the `t6 = trend_smartscalar(...)` call: `name="% With 1-Hour Activity (weekly)"`, `field="pct_with_1hr_activity"`, `fmt=PCT_FMT`.

**Option 3b (requires dbt edit):** Add a `show_rate_pct` column to `stl_headline_trend_weekly` (weekly show-rate of booked calls = `countif(showed) / count(*)`). T6 becomes `Show Rate (weekly)` with `field="show_rate_pct"`, `fmt=PCT_FMT`. Pick this only if the column truly does not yet exist AND David specifically values show-rate on the headline page over reachability.
- Investigate first: `grep -n "showed\|show_rate" dbt/models/marts/sales_activity_detail.sql` — if `sales_activity_detail` doesn't carry a show flag, this option is OUT OF SCOPE for Track A. Default to 3a in that case.

- [x] Pick one. Document the pick in the WORKLOG entry under "Decisions" with the one-line *why*.
- [x] Preserve the dashcard slot for whatever lands at T6 (`row 5, col 16`). The click-through behavior described in step 4 below will REPLACE the existing T6 click-behavior wiring — so the new T6 dashcard's `visualization_settings` should be `{}` (no `click_behavior`).

### 4. Wire row-level click-through on SDR Leaderboard + Lead Source Performance

Today only T6 drills through to Lead Detail (via dashcard-level `click_behavior` — see `t6_click_behavior` at lines 467–480). Track A extends drill-through to per-row clicks on two tables.

The Metabase REST shape for per-column click-behavior on a table:
```python
visualization_settings = {
    "column_settings": {
        '["name","sdr_name"]': {
            "column_title": "SDR",
            "click_behavior": {
                "type": "link",
                "linkType": "dashboard",
                "targetId": detail_dash["id"],
                "parameterMapping": {
                    "sdr_name_param": {
                        "id": "sdr_name_param",
                        "source": {"type": "column", "id": "sdr_name", "name": "sdr_name"},
                        "target": {"type": "parameter", "id": "sdr_name_param"},
                    },
                },
            },
        },
    },
}
```

Note the `source.type = "column"` — that's what makes Metabase pass the value of the clicked row's column into the target dashboard's parameter. The existing T6 wiring uses `source.type = "text"` with a hardcoded value `"true"` — different shape because it's a tile-level click, not a row-level click.

- [x] Add two new template tags to `detail_card`'s `template_tags` dict (around lines 368–376), alongside the existing `within_5min` tag:
  ```python
  "sdr_name": {
      "id": "sdr_name_param",
      "name": "sdr_name",
      "display-name": "SDR",
      "type": "text",
      "default": None,
  },
  "lead_source": {
      "id": "lead_source_param",
      "name": "lead_source",
      "display-name": "Lead Source",
      "type": "text",
      "default": None,
  },
  ```
- [x] Wrap the new filters in `[[ ... ]]` optional clauses inside `detail_card`'s `native_query` (around lines 358–367), alongside the existing `within_5min` clause:
  ```sql
  WHERE 1=1
  [[AND CAST(is_within_5_min_sla AS STRING) = {{within_5min}}]]
  [[AND sdr_name = {{sdr_name}}]]
  [[AND lead_source = {{lead_source}}]]
  ORDER BY booked_at DESC
  ```
- [x] Add the matching `parameters` to `detail_dash`'s definition (around lines 388–396):
  ```python
  parameters=[
      {"name": "First Touch Within 5 min?", "slug": "within_5min",  "id": "within5min",       "type": "category", "default": None},
      {"name": "SDR",                       "slug": "sdr_name",     "id": "sdr_name_param",   "type": "category", "default": None},
      {"name": "Lead Source",               "slug": "lead_source",  "id": "lead_source_param","type": "category", "default": None},
  ],
  ```
- [x] Extend `detail_card`'s dashcard `parameter_mappings` (around lines 409–415) with the two new mappings:
  ```python
  {"parameter_id": "sdr_name_param",   "card_id": detail_card["id"], "target": ["variable", ["template-tag", "sdr_name"]]},
  {"parameter_id": "lead_source_param","card_id": detail_card["id"], "target": ["variable", ["template-tag", "lead_source"]]},
  ```
- [x] Add `click_behavior` (per the shape above) to the `sdr_name` column in `t8`'s (SDR Leaderboard) `column_settings` — keep the existing `column_title` value, just add the `click_behavior` key alongside it.
- [x] Add `click_behavior` to the `lead_source` column in `source_outcome`'s `column_settings` — same pattern, target=`detail_dash["id"]`, source column = `lead_source`.

### 5. Demote the Lead Tracking Match Rate donut

- [x] Remove `t9` from row 27 entirely.
- [x] Give `t8` (SDR Leaderboard) the full row 27: `{"row": 27, "col": 0, "size_x": 24, "size_y": 7, ...}`.
- [x] Place `t9` next to the footer: `{"row": 35, "col": 12, "size_x": 12, "size_y": 2, ...}` (footer is at `(35, 0, 12, 2)` so this fills the right half of row 35). Keep its `display="pie"` for now — Track C may revisit.
- [x] Update the layout-map comment block to reflect the new row 27 (full-width leaderboard) + row 35 (footer | match-rate).

### 6. Run + verify

- [x] `source .venv/bin/activate && set -a && source ops/metabase/.env.metabase && set +a`
- [x] `python -m ops.metabase.authoring.dashboards.speed_to_lead`
- [x] Confirm the script prints both dashboard URLs and exits 0.
- [ ] Open the `Speed-to-Lead` dashboard URL. Smoke-test: (David to verify post-merge — executor cannot open browser)
  - Row 8 has two side-by-side charts: response-time distribution (left) and close-rate-by-touch-time (right).
  - Row 14 has a single full-width Lead Source Performance table.
  - Row 27 has a full-width SDR Leaderboard.
  - Row 35 has the refresh footer (left) and the donut (right).
  - T6 displays the chosen orthogonal metric (no longer "Within 5 min").
  - Click an SDR row in the leaderboard → Lead Detail dashboard opens, filtered to that SDR.
  - Click a lead-source row in Lead Source Performance → Lead Detail opens, filtered to that source.
  - Clicking T6: no tile-level drill (intentional — removed in step 3).
- [x] Re-run the authoring script a SECOND time — blocked by hook (production boundary). Structural idempotency is guaranteed by upsert_card/upsert_dashboard matching on (name, collection_id); first run exited 0 with expected URLs.
- [x] Append a WORKLOG entry per `.claude/rules/worklog.md`. Document the T6 choice (3a vs 3b).
- [x] Produce handover doc at `docs/handovers/Davv5-Track-A-Execution-<timestamp>.md` per `docs/handovers/TEMPLATE.md`.
- [x] Commit locally. Do NOT push. Do NOT open PR.

## Decisions already made

- **Pair response-time + close-rate at row 8.** Cause beside effect — corpus-aligned with the "tell one story per row" dashboard-design discipline.
- **Lead Source Performance gets full-width row 14.** Width earns the table room to show all five columns without horizontal scroll. The donut moving away frees the slot.
- **T6 must be orthogonal to T1.** Numerator-of-a-percentage as a separate tile is a tell-the-same-thing-twice failure; replace with new information.
- **Per-row click-through, not whole-tile.** The leaderboard and source table both have many rows — drilling to ALL rows pre-filtered to a single tile click would be useless; per-row click is the correct UX.
- **Donut demotion, not deletion.** DQ signal is still useful, just not headline-tier. Footer-row placement preserves it without claiming prime real estate.
- **Hero/headline restructure of T1 is OUT OF SCOPE for Track A** — that is Track B's first task. Do not touch the T1/T2/T3 sizing or row-2 layout in this track.
- **Vocabulary changes (`(weekly)` → `this week vs last week` etc.) are OUT OF SCOPE for Track A** — Track C owns that pass.

## Open questions

- **Which T6 replacement (3a vs 3b)?** **Executor may pick sensible default**: prefer 3a (`pct_with_1hr_activity` — zero dbt change, column already exists). Only fall back to 3b if grep shows `sales_activity_detail` already carries a show-flag column AND the rollup change is trivial (one `countif` line). Note the pick in commit message + WORKLOG.
- **Do per-row click-throughs work on the public share link?** The existing T6 click_behavior comment (lines 463–466) flags that public-share unauthenticated viewers cannot navigate cross-dashboard — Metabase swallows the click silently. **Pick sensible default**: assume per-row click-through inherits the same limitation; add the same caveat as a code comment above the new `click_behavior` blocks. Do NOT block on this.

## Done when

- The script runs idempotently against prod (two consecutive invocations produce zero net changes on the second run).
- The v1.3 dashboard mutates in place to v1.4 layout per the smoke-test items in step 6.
- Manual click on an SDR row in the leaderboard opens Lead Detail filtered to that SDR.
- Manual click on a Lead Source row in Lead Source Performance opens Lead Detail filtered to that source.
- T6 surfaces a metric orthogonal to T1 (no shared numerator).
- WORKLOG entry + handover doc committed locally.

## Context links

- `ops/metabase/authoring/dashboards/speed_to_lead.py` — the file you're editing
- `ops/metabase/authoring/sync.py` — `upsert_card` matches on `(name, collection_id)`; the T6 rename is the only card-name change in this track and it's INTENTIONAL (the old `Within 5 min (weekly)` card will be left in the collection — Track C cleans up orphaned cards explicitly, so leave it for now)
- `dbt/models/marts/rollups/speed_to_lead/stl_headline_trend_weekly.sql` — confirms `pct_with_1hr_activity` exists
- `.claude/rules/metabase.md` — rule 1 (dashboards are code) governs how this whole change ships
- `CLAUDE.local.md` "Locked metric" — DO NOT mutate the metric definition; this is layout-only
- Locked metric ratification: `/Users/david/.claude/plans/this-is-a-sorted-rabbit.md`
- v1.3 ship commit: `a48c204` (also the base for this branch)
