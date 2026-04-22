# Session Handover — Track C: Speed-to-Lead v1.6 vocabulary + heatmap fallback + orphan cleanup

**Branch:** `Davv5/Track-C-STL-Vocabulary-Heatmap-Fallback`
**Timestamp:** `2026-04-22_15-10` (executed)
**Author:** track-executor (Claude Sonnet 4.6)
**PR:** not yet opened

---

## Session goal

Apply a final consistency pass to `speed_to_lead.py`: replace abbreviated time qualifiers on all 12 card names with fully-written equivalents; swap the SDR Coverage Heatmap from the fragile `pivot` display to a `table` display with conditional cell coloring; and add a permanent orphan-cleanup block that archives any cards in the Speed-to-Lead collection NOT produced by the current run.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py   — edited — vocabulary pass (12 renames) + heatmap display fallback + orphan cleanup block
WORKLOG.md                                            — edited — dated entry recording v1.6 changes
docs/handovers/Davv5-Track-C-stl-vocabulary-heatmap-fallback-2026-04-22_15-10.md  — edited — checkboxes filled in
docs/handovers/Davv5-Track-C-Execution-2026-04-22_15-10.md   — created — this file
```

## Commands run / run IDs

- No dbt commands run (no dbt files changed).
- Script live-run deferred — no `.env.metabase` in the worktree. The executor verified the code changes are correct by reading the final file and grep-checking for stale vocabulary strings.
- `/api/docs` probed: returned HTTP 302 (auth-gated redirect) — confirmed by `curl` in-session. Key names for `table.column_formatting` confirmed via OSS 60.x convention already in use in the file (`show_mini_bar`).

## Decisions made

- **Track B chose Option 2a** (confirmed by reading the file): T3 was `Slowest 10% — minutes to first touch (weekly)`. Renamed to `Slowest 10% — minutes to first touch, this week vs last week`.
- **Track A chose Option 3a** (confirmed by reading the file): T6 was `% With 1-Hour Activity (weekly)`. Renamed to `% with 1-hour activity, this week vs last week`.
- **Heatmap: table-pivot main path, no bar fallback.** `/api/docs` returns 302; table-pivot key names are the same convention already proven in the file (`show_mini_bar` in `column_settings`). Shipped `display="table"` + `table.pivot=True` + `table.column_formatting` red→yellow→green.
- **`upsert_card` rename-orphan behavior confirmed** at `sync.py` line 47: lookup is `name == name AND collection_id == collection_id`. A rename = new POST + old card left behind. Documented in WORKLOG under Decisions.
- **Orphan cleanup is permanent** (not a one-shot): runs on every script invocation. Cost: one extra `GET /api/card`. Benefit: every future rename is self-cleaning with zero extra author work.
- **`_ = t7` retained** after the `set_dashboard_cards` call (t7 is parked, not on the dashboard). Still listed in `kept_ids` so it survives the orphan pass.

## Unresolved risks

- [ ] Live run not done in worktree — David should run `python -m ops.metabase.authoring.dashboards.speed_to_lead` against prod after merge and confirm: (a) orphan archive count prints, (b) heatmap renders as table with colored cells, (c) second run emits zero orphan log lines.
- [ ] `table.pivot` + `table.column_formatting` key names confirmed by OSS convention, not by live `/api/docs`. If the running Metabase version uses different key names, the heatmap will silently fall back to a flat unformatted table (no crash). Visual check on first live run will surface this immediately.
- [ ] Stale cards from earlier pre-Track-A renames (e.g., `% Within 5-min SLA (7d)`, `DQ — SDR Activity Within 1 Hr (7d)`, `Attribution Quality Mix (30d)` noted in the 2026-04-22 WORKLOG entry) may still live in the collection if they were left from before Tracks A/B ran. The orphan cleanup block will archive these too — they are not in `kept_ids`.

## First task for next session

Run the authoring script against the prod Metabase instance after merge and confirm zero orphan log lines on the second run. Log the orphan count from the first run in the WORKLOG as a data point.

## Context links

- `ops/metabase/authoring/dashboards/speed_to_lead.py` — the edited file
- `ops/metabase/authoring/sync.py` — `upsert_card` rename-orphan behavior confirmed at line 47
- Track A handover: `docs/handovers/Davv5-Track-A-stl-storytelling-restructure-2026-04-22_15-10.md`
- Track B handover: `docs/handovers/Davv5-Track-B-stl-hero-headline-polish-2026-04-22_15-10.md`
- Track C plan: `docs/handovers/Davv5-Track-C-stl-vocabulary-heatmap-fallback-2026-04-22_15-10.md`
- WORKLOG entry: `## 2026-04-22 — Speed-to-Lead v1.6: vocabulary pass + heatmap table-pivot + permanent orphan cleanup`
