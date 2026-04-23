# Session Handover — Track E: Speed-to-Lead v1.3.1 authoring polish

**Branch:** `Davv5/Track-E-STL-v131-Authoring-Polish`
**Worktree:** `.claude/worktrees/track-E-stl-v131-authoring-polish/`
**Timestamp:** `2026-04-22_21-37` (executed)
**Author:** track-executor (Claude Sonnet 4.6)
**Base commit:** `13ad334` (Track C merge on main — Tracks A/B/C all present)
**PR:** not yet opened

---

## Session goal

Ship the four v1.3.1 authoring polishes: (1) dashboard-level Date + SDR filters; (2) four markdown section dividers; (3) footer maintainer-contact text card; (4) "Data as of" freshness number card backed by new `stl_data_freshness` rollup.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py              — edited — filters, dividers, footer, freshness tile, layout shift
dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql   — created — 1-row freshness rollup
dbt/models/marts/rollups/speed_to_lead/_stl__models.yml         — edited — stl_data_freshness model doc + column descriptions
.claude/rules/metabase.md                                        — edited — Field Filter lessons-learned bullet
WORKLOG.md                                                       — edited — dated entry for Track E
docs/handovers/Davv5-Track-E-Execution-2026-04-22_21-37.md      — created — this file
```

## Commands run / run IDs

- `dbt build --target dev --select stl_data_freshness` — 1 success, 0 failures. BQ job: `dee-data-ops:bqjob_..._15f01b93-1293-4574-ab55-717734c83660`. 1 row: `last_booking_at=2026-04-20 13:40:46`, `run_ts=2026-04-22 21:38:33`.
- (Prerequisite) `dbt build --target dev --select sales_activity_detail` — 1 table, 4 tests pass, 1 WARN (pre-existing release_gate). Upstream was not materialised in dev_david; built first to unblock `stl_data_freshness`.
- `python -m ops.metabase.authoring.dashboards.speed_to_lead` — run 1: exit 0, no orphan cards. Run 2 (idempotency): exit 0, no orphan cards.
- `curl -sSI https://34-66-7-243.nip.io/public/dashboard/3` — HTTP/2 200.
- `mb.get('/dashboard/3')` post-run: 2 parameters set correctly, 20 dashcards, `cache_ttl=21600` intact.

## Open questions resolved

**Q1 — Metabase relative-date default string format:** `"past7days~"` accepted by Metabase without rewriting. GET on the dashboard after run confirms `default="past7days~"`. This is the canonical form for "Last 7 days including current period" on this OSS 60.x instance.

**Q2 — Freshness tile layout:** Option C selected (row 45 col 12, adjacent-right of "Data refreshed" scalar). T1 hero is full-width 24×4, so Option A (col 16 on T1 row) was not viable. Option C pairs both footer scalars as a "currency vs recency" pair.

**Q3 — Public share interactive filters:** Public URL returns HTTP 200. Interactive filter behaviour for anonymous viewers not verified in-session — this is a known Metabase OSS limitation (filters may render read-only for anonymous public share viewers). Authenticated access works. Not a blocker; documented in WORKLOG Open Threads.

**Q4 — Keep existing "Data refreshed" scalar:** YES — kept both. "Data refreshed" = dbt run time (computed_at from stl_headline_7d); "Data as of" = latest event in mart (max(booked_at) from stl_data_freshness). Different signals, both retained.

## Decisions made

- T1 is full-width 24×4 (confirmed from reading the file before Track E edits). Option C freshness tile layout follows automatically.
- `"past7days~"` confirmed empirically as the canonical Metabase OSS relative-date string for "Last 7 days including current period".
- `cache_ttl=21600` survived `upsert_dashboard` re-PUT: `sync.py`'s PUT payload doesn't include `cache_ttl`, so Metabase preserves the existing value. No fix to `sync.py` required (Task 7 answer: Track D's setting intact).
- Lead Detail child dashboard (`Speed-to-Lead — Lead Detail`) updated with matching `date-range-dash` + `sdr-filter-dash` parameters and `parameter_mappings` on the detail card. Cross-dashboard filter context preserved on drill-through.
- `detail_card` had existing `sdr_name` (text type) template tag for the detail dashboard's own category filter. Added separate `sdr_filter` (dimension/Field Filter type) for the parent dashboard SDR filter binding. Two different tags, two different purposes — no collision.

## Post-merge verification checklist (for David / pr-reviewer)

- [ ] Open `https://34-66-7-243.nip.io/dashboard/3` (authenticated). Verify Date filter pill + SDR filter pill visible in top bar.
- [ ] Set Date filter to "Last 7 days" — T1-T6 hero tiles narrow; Lead Detail drill-through narrows. `_30d` cards unchanged (expected).
- [ ] Set SDR filter to a specific rep name — leaderboard narrows to that row; Lead Detail narrows. `_30d` cards unchanged (expected).
- [ ] Four section dividers render as bold H2 headings: Speed Metrics / Distribution & Outcome / Coverage & Rep Performance / Data Quality Notes.
- [ ] Footer markdown card at row 47 shows maintainer email as clickable `mailto:` link.
- [ ] "Data as of" tile (row 45 col 12) shows a datetime in `MMM D, YYYY HH:MM` format.
- [ ] Public share URL interactive-filter limitation: document if filters are read-only for anonymous viewers.

## Context links

- `ops/metabase/authoring/dashboards/speed_to_lead.py` — edited file
- `dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql` — new rollup
- `ops/metabase/authoring/sync.py` — `upsert_dashboard` PUT payload shape (explains cache_ttl preservation)
- Track E plan: `docs/handovers/Davv5-Track-E-stl-v131-authoring-polish-2026-04-22_17-25.md`
- Track C handover (predecessor): `docs/handovers/Davv5-Track-C-Execution-2026-04-22_15-10.md`
- Track D handover (parallel sibling): `docs/handovers/Davv5-Track-D-stl-metabase-admin-config-2026-04-22_17-12.md`
- WORKLOG entry: `## 2026-04-22 — Track E: Speed-to-Lead v1.3.1 authoring polish`
