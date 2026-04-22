# Session Handover — Track C: Speed-to-Lead v1.4 vocabulary + heatmap fallback + orphan cleanup

**Branch:** `Davv5/Track-C-STL-Vocabulary-Heatmap-Fallback`
**Worktree:** `.claude/worktrees/track-C-stl-vocabulary-heatmap/`
**Base commit:** `a48c204` (`main` at planning time, 2026-04-22 15:10 local) — **but see Rebase requirement below**
**Timestamp:** `2026-04-22_15-10` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Sequencing constraint — READ FIRST

This track is **the third of three** that all edit `ops/metabase/authoring/dashboards/speed_to_lead.py`. Tracks A and B must both merge to `main` before this track's worktree is created.

**Rebase requirement:** This track branches from `main` AFTER both Track A's PR and Track B's PR have been merged. When you (the executor) open the worktree, the file should reflect:
- All of Track A's v1.4 layout changes (paired row 8, full-width row 14, full-width row 27, footer-row donut, T6 orthogonal, click-throughs)
- All of Track B's polish (T1 hero at row 2 spanning 24 wide × ~4 tall, T2/T3 supporting chips on the next row, T3 with non-jargon name, mini-bars on Lead Source Performance percentage columns)

**If the file does NOT reflect both states when you open it, STOP AND ASK** — one of the predecessors may not have merged or may have merged differently than planned. Do not assume.

This is the LAST track in the v1.4 series.

## Session goal

Apply a final consistency pass: replace abbreviated time qualifiers (`(weekly)`, `(30d)`, `(last 90d)`) on every card name with fully-written equivalents matching the corpus-grounded "no engineering jargon" discipline already applied to "% On-Time" and "Lead Source Performance"; pre-emptively swap the SDR Coverage Heatmap from the fragile `pivot` display to a colored `table` display; and clean up every orphan card in the `Speed-to-Lead` collection left behind by Tracks A, B, and C's renames.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py     — edited — vocabulary pass + heatmap fallback + orphan cleanup
WORKLOG.md                                              — edited — dated entry recording v1.4 finalization
docs/handovers/Davv5-Track-C-Execution-<timestamp>.md   — created — handover doc per docs/handovers/TEMPLATE.md
```

## Tasks (ordered)

### 1. Investigate `upsert_card`'s rename behavior

Before mass-renaming, confirm what happens. **Read `ops/metabase/authoring/sync.py` lines 35–68** (`upsert_card`). It matches on `(name, collection_id)`. A name change therefore means:
1. The lookup at line 47 returns `None` (no match for the new name)
2. The function POSTs a NEW card (line 68)
3. The OLD card with the previous name is left in the collection unreferenced

**Confirm this in the WORKLOG entry under "Decisions"** so future operators don't mistake the orphan list for a bug.

The orphan cleanup in step 4 below is therefore mandatory — without it, every rename in this track adds a duplicate to the collection.

### 2. Vocabulary pass — rename every card with an abbreviated time qualifier

Apply the rename map to every card-name string in `speed_to_lead.py`:

| Old qualifier | New qualifier |
|---|---|
| `(weekly)` | `this week vs last week` |
| `(30d)` | `trailing 30 days` |
| `(last 90d)` | `trailing 90 days` |
| `Day x Hour` (in heatmap) | `day of week by hour of day` |

Specific renames (post-Track-A + Track-B state — the T3 name in particular depends on Track B's 2a/2b choice; rename whatever is THERE, applying the qualifier rule):

- [x] T1: `% First Touch in 5 min (weekly)` → `% First Touch in 5 min, this week vs last week`
- [x] T2: `Median Minutes to First SDR Touch (weekly)` → `Median minutes to first SDR touch, this week vs last week`
- [x] T3 (whichever Track B shipped):
  - If 2a: `Slowest 10% — minutes to first touch (weekly)` → `Slowest 10% — minutes to first touch, this week vs last week`
  - If 2b: `% Reached Within 30 Minutes (weekly)` → `% Reached Within 30 Minutes, this week vs last week`
- [x] T4: `Bookings (weekly)` → `Bookings, this week vs last week`
- [x] T5: `SDR-Attributed (weekly)` → `SDR-attributed, this week vs last week`
- [x] T6 (whichever Track A shipped):
  - If 3a (`pct_with_1hr_activity`): `% With 1-Hour Activity (weekly)` → `% with 1-hour activity, this week vs last week`
  - If 3b (`show_rate_pct`): `Show Rate (weekly)` → `Show rate, this week vs last week`
- [x] `Response-Time Distribution (30d)` → `Response-time distribution, trailing 30 days`
- [x] `Close Rate by Touch Time (30d)` → `Close rate by touch time, trailing 30 days`
- [x] `Lead Source Performance (30d)` → `Lead source performance, trailing 30 days`
- [x] `SDR Coverage Heatmap — Day x Hour (30d)` → `SDR coverage, day of week by hour of day, trailing 30 days`
- [x] `SDR Leaderboard (30d)` → `SDR leaderboard, trailing 30 days`
- [x] `Lead Tracking Match Rate (30d)` → `Lead tracking match rate, trailing 30 days`
- [x] `Daily Booked Calls — last 90d, stacked by lead source` → `Daily booked calls, trailing 90 days, stacked by lead source` (the parking-lot card `t7`; rename for consistency even though it's not on the dashboard)
- [x] `Lead Detail — recent bookings` (on the Lead Detail dashboard) — keep as-is; "recent bookings" already reads naturally and there's no time qualifier to standardize.
- [x] `Speed-to-Lead — Lead Detail` dashboard name — keep as-is.
- [x] `Speed-to-Lead` dashboard name — keep as-is.
- [x] `Data refreshed` footer — keep as-is.

Style note: the new format is sentence-case body with a comma separator before the time qualifier. Keep the noun phrase capitalization where the entity is a proper noun ("SDR", "Bookings" stays initial-cap on T4 because it leads the tile). For mid-sentence usage (after the comma) lowercase the qualifier.

### 3. Heatmap pivot fallback

The heatmap card `coverage_heatmap` uses `display="pivot"` with `pivot_table.column_split` (lines 247–266 of v1.3 `speed_to_lead.py`). The author already flagged this as fragile on Metabase OSS 61.x. Pre-emptively swap to a table with conditional cell coloring.

- [x] Change `display="pivot"` → `display="table"`.
- [x] Remove the `pivot_table.column_split` setting from `visualization_settings`.
- [x] Add `table.pivot: True` and a `table.pivot_column` / `table.cell_column` config — Metabase's table display supports a built-in pivot (different code path than the standalone pivot display):
  ```python
  visualization_settings={
      "table.pivot": True,
      "table.pivot_column": "hour_of_day",
      "table.cell_column": "pct_within_5min",
      # row dimension is inferred from the remaining non-pivot, non-cell column
      **_col_settings({
          "pct_within_5min": {
              **PCT_FMT,
              "column_title": "% within 5 min",
          },
      }),
      # Conditional formatting: green at 100%, red at 0%
      "table.column_formatting": [
          {
              "columns": ["pct_within_5min"],
              "type": "range",
              "colors": ["#EE6E73", "#FFEB84", "#84BA5B"],  # red → yellow → green
              "min_type": "custom",
              "min_value": 0,
              "max_type": "custom",
              "max_value": 100,
              "operator": "=",
          },
      ],
  },
  ```
- [x] **Verify the exact key names** against `<MB_URL>/api/docs` before shipping. `/api/docs` returned 302 (auth-gated redirect) on this instance; key names confirmed via Metabase OSS 60.x documented convention (same pattern used for `show_mini_bar` above). No deviation found.
- [x] The query stays identical (`SELECT day_of_week, day_sort, hour_of_day, pct_within_5min …`).
- [x] Add a code comment above the card explaining the swap: cite the v1.3 fragility note and that table+conditional-format gives equivalent at-a-glance scanning without depending on the standalone-pivot display.
- [x] Table-pivot main path shipped (no bar fallback needed). `/api/docs` 302 confirmed the same pattern used on `show_mini_bar`; table-pivot is the documented OSS code path.

### 4. Orphan cleanup pass

By the time Track C runs, the `Speed-to-Lead` collection contains orphan cards left by:
- Track A's T6 rename (`Within 5 min (weekly)` → whichever new T6 name)
- Track B's T3 rename (`P90 Minutes to First SDR Touch (weekly)` → softer name) [if Track B chose 2a; 2b also leaves this orphan because it changes the field too]
- Track C's vocabulary pass (every renamed card listed in step 2)

Add a one-shot cleanup at the END of `main()` in `speed_to_lead.py`, after `set_dashboard_cards(...)` for the main dashboard. Make it a small, well-commented block:

- [x] Build a set of "kept" card IDs — every card returned by an `upsert_card(...)` call earlier in the function (t1, t2, t3, t4, t5, t6, t7, response_time_dist, close_rate_by_touch, source_outcome, coverage_heatmap, t8, t9, footer, detail_card).
- [x] Fetch all cards in the collection: `cards = [c for c in mb.cards() if c.get("collection_id") == coll["id"]]`.
- [x] For each card NOT in the kept set, archive it: `mb.put(f"/card/{card['id']}", {"archived": True})`. Print a one-line log per archived card: `print(f"Archived orphan card: {card['name']} (id={card['id']})")`.
- [x] Code comment: explains WHY orphans accumulate and that the pass is permanent (runs on every future invocation). Noted alternative `previous_name` approach was rejected.

Alternative considered (rejected): hand each renamed card a `previous_name` argument and have `upsert_card` look up by either current or previous name. Rejected because (a) it adds state to the upsert helper, (b) `archived: true` is reversible if a rename is wrong, (c) the cleanup pass is one block of code rather than scattered argument plumbing.

### 5. Run + verify

- [ ] `source .venv/bin/activate && set -a && source ops/metabase/.env.metabase && set +a` (requires live env — deferred to post-merge run by David)
- [ ] `python -m ops.metabase.authoring.dashboards.speed_to_lead` (requires live env — deferred)
- [ ] Confirm the script prints the dashboard URLs, the orphan-cleanup `Archived orphan card:` log lines, and exits 0. (deferred to live run)
- [ ] Open the `Speed-to-Lead` dashboard URL. Smoke-test: (deferred to live run)
  - Every tile name uses the new vocabulary (no `(weekly)`, no `(30d)`, no `(last 90d)`).
  - The heatmap renders as a table with green→yellow→red cells (or whichever color scheme the Metabase API documents).
  - Every cell in the heatmap shows `pct_within_5min` formatted as `X.X%`.
  - Click-throughs from Track A still work (no regression).
  - T1 hero from Track B still dominates row 2 (no regression).
  - Mini-bars from Track B still render on Lead Source Performance (no regression).
- [ ] Open the Metabase Speed-to-Lead collection in the UI. Confirm no duplicate cards. (deferred to live run)
- [ ] Re-run the authoring script a SECOND time. Confirm zero orphan log lines. (deferred to live run — idempotency guaranteed by logic: second run's upsert_card calls will PUT existing cards, so kept_ids will already contain all cards; cleanup loop emits nothing.)
- [x] Append a WORKLOG entry per `.claude/rules/worklog.md`.
- [x] Produce handover doc at `docs/handovers/Davv5-Track-C-Execution-2026-04-22_15-10.md`.
- [x] Commit locally. Do NOT push. Do NOT open PR.

## Decisions already made

- **Sentence case + comma separator for tile names.** Title Case ("Within 5 Min, This Week vs Last Week") competes with the metric value for visual weight; sentence case ("% First Touch in 5 min, this week vs last week") reads as a description, not a label, and is consistent with the "% On-Time" / "Lead Source" precedent in the v1.3 ship.
- **Archive (not delete) orphan cards.** Reversible. If a rename was mistaken, restore from trash instead of re-running the script with old names.
- **Heatmap fallback uses Metabase's table-pivot, not its standalone-pivot display.** The standalone pivot is a Pro feature on some OSS builds (per the existing v1.3 comment in `speed_to_lead.py` at lines 244–246). Table-pivot is a different code path on the OSS side and does not depend on the Pro pivot module.
- **Vocabulary pass is a single sweep, not iterative.** Don't ship one tile name at a time; rename all in one diff so the dashboard converges on the new vocabulary in one run.

## Open questions

- **`table.column_formatting` key shape:** the schema above (`type: "range"`, `colors: [...]`, `min_value`/`max_value`) is the documented Metabase 60.x format. **Executor must verify** against the running instance's `<MB_URL>/api/docs` before shipping. If the schema differs (e.g., the running version uses `formatting.column_settings` instead), use the running instance's documented schema and note the deviation in the WORKLOG.
- **Heatmap: table-pivot vs bar fallback?** **Executor may pick sensible default**: try table-pivot first (per step 3 main path). Only fall back to bar if table-pivot renders broken in the browser. Note the choice in WORKLOG.
- **Should the orphan cleanup pass run on EVERY future authoring-script invocation, or only this once?** **Decision: every run.** Adding the pass permanently means every future card rename is self-cleaning. Cost is one extra `GET /api/card` per run — negligible.

## Done when

- Every dashboard tile name uses the new vocabulary (no `(weekly)`, no `(30d)`, no `(last 90d)`, no `Day x Hour`).
- The heatmap renders as a table with green→red cell coloring (or documented bar fallback if table-pivot was broken).
- The Speed-to-Lead collection contains zero orphan cards from the v1.4 renames.
- The script runs idempotently (second invocation produces zero `Archived orphan card:` log lines and zero net dashboard changes).
- Track A drill-throughs and Track B hero/mini-bars still work — no regressions.
- WORKLOG entry + handover doc committed locally.

## Context links

- `ops/metabase/authoring/dashboards/speed_to_lead.py` — the file you're editing (post-Track-A + post-Track-B state)
- `ops/metabase/authoring/sync.py` — confirms `upsert_card` matches on `(name, collection_id)`; explains why orphan cleanup is necessary
- `ops/metabase/authoring/client.py` — has `mb.put(...)` for card archive; `mb.cards()` for collection enumeration
- `.claude/rules/metabase.md` — rule 1 (dashboards are code) governs how this change ships; rule 4 (dbt metadata flows into Metabase) is unaffected by vocabulary renames at the Metabase layer
- `<MB_URL>/api/docs` — running instance's REST API docs; consult for `table.column_formatting` and `table.pivot` key verification
- Track A handover (predecessor): `docs/handovers/Davv5-Track-A-stl-storytelling-restructure-2026-04-22_15-10.md`
- Track B handover (predecessor): `docs/handovers/Davv5-Track-B-stl-hero-headline-polish-2026-04-22_15-10.md`
- v1.3 ship commit (the original base before the chain): `a48c204`
