# Session Handover — Track E: Speed-to-Lead v1.3.1 authoring polish (dashboard filters + section dividers + footer + freshness card)

**Branch:** `Davv5/Track-E-STL-v131-Authoring-Polish`
**Worktree:** `.claude/worktrees/track-E-stl-v131-authoring-polish/`
**Base commit:** **`main` at the tip post-Tracks-A/B/C merge** — see Sequencing constraint below. The track brief references `13ad334` as the post-C tip; at plan-authoring time (2026-04-22 17:25 local) the `main` HEAD is still `a48c204` (v1.3 ship) + `44ba878` (worklog chore). Tracks A/B/C are planned but not yet merged. **The executor MUST verify A/B/C are merged before starting (Task 0 below).**
**Timestamp:** `2026-04-22_17-25` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Sequencing constraint — READ FIRST

**This track is the fourth in the v1.3-polish series on `ops/metabase/authoring/dashboards/speed_to_lead.py`.**

- Tracks A, B, C all edit `speed_to_lead.py` (storytelling restructure + hero polish + vocabulary/heatmap). They must all be merged to `main` before Track E opens its worktree.
- The track brief states "Current `main` tip is `13ad334` (Track C v1.6 vocabulary pass + heatmap table-pivot + orphan cleanup)." Plan-authoring confirms `main` at planning time is still `a48c204` + `44ba878` (A/B/C unmerged). **If when you open the worktree `git log -1 main` does not show the Track-C merge commit, STOP AND ASK David.** One of the predecessors may have merged differently, or the executor may be on the wrong branch.

**Parallel-safety with Track D:**

- Track D edits `ops/metabase/authoring/infrastructure/caching_config.py` and `...dashboard_subscriptions.py` — new files, no collision with `dashboards/speed_to_lead.py`. Tracks D and E can run in parallel.
- **Soft couplings** (shared files, trivial append-conflicts if two tracks land in the same merge wave — per `.claude/rules/agents.md` Rule 5):
  - `WORKLOG.md` — both tracks append a dated entry
  - `.claude/rules/metabase.md` — both tracks may add a "Lessons learned" bullet
  - Merge order if D and E PR in the same wave: whichever lands second resolves the append conflict trivially (keep both entries; they are chronological).
- **Interaction-level caveat:** if Track D has already set `cache_ttl = 21600s` on the Speed-to-Lead dashboard, Track E's dashboard re-author is *safe*: Metabase's `cache_ttl` is a dashboard attribute independent of the dashcard list, and `upsert_dashboard` in `sync.py` preserves attributes it doesn't explicitly override. Still, **Task 8 verification** re-reads `cache_ttl` after running the authoring script to confirm Track D's setting survived.

## Session goal

Ship the four v1.3.1 authoring polishes flagged by the round-2 Metabase-corpus gap analysis:

1. **Dashboard-level filters** — a Date filter (default: Last 7 days including current period) and an SDR filter (default: all SDRs), linked to every card whose underlying rollup carries the corresponding dimension.
2. **Markdown section dividers** — four section-header text cards bracketing the narrative arc (`Speed Metrics`, `Distribution & Outcome`, `Coverage & Rep Performance`, `Data Quality Notes`).
3. **Footer text card** — full-width markdown card with last-refresh note + maintainer contact email, below the existing footer donut.
4. **"Data as of HH:MM" number card** — a freshness indicator backed by a new `stl_data_freshness` rollup reading `MAX(booked_at)` off `sales_activity_detail`.

All four are corpus-grounded (Metabase Learn notebook, round-2 gap analysis). No business-logic decisions pending — every choice below is locked.

## Changed files

```
ops/metabase/authoring/dashboards/speed_to_lead.py          — edited — add parameters, section text cards, footer text card, freshness card, re-lay-out
dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql — created — 1-row rollup exposing max(booked_at) for the freshness tile
dbt/models/marts/rollups/speed_to_lead/_stl__models.yml     — edited — add stl_data_freshness model doc + column descriptions
WORKLOG.md                                                   — edited — dated entry recording v1.3.1 ship
docs/handovers/Davv5-Track-E-Execution-<timestamp>.md        — created — executor's handover doc per docs/handovers/TEMPLATE.md
```

**NOT touched in this track:**
- `ops/metabase/authoring/infrastructure/**` — Track D's scope.
- Any other `dbt/models/**` file beyond `stl_data_freshness.sql` + its YAML doc. *Specifically: the existing `stl_*_30d.sql` rollups are NOT edited to add `{{date_range}}` template tags.* See **Decisions Already Made §2** and **Open Questions §Q1** for the full rationale — TL;DR: native-SQL template tags in BigQuery are executor-authored at the Metabase card-SQL layer (lives in `speed_to_lead.py`), not at the dbt rollup layer. The dbt rollups stay hardcoded to their current windows (30d / weekly / 90d).

## Tasks (ordered)

### 0. Pre-flight — verify predecessors merged + open the worktree

- [x] `git fetch origin`
- [x] `git log --oneline origin/main | head -10` — expect to see the Track-A, Track-B, Track-C merge commits above `a48c204` (v1.3 ship). If any of the three is missing, STOP AND ASK David.
- [x] Read `ops/metabase/authoring/dashboards/speed_to_lead.py` on `origin/main`. Confirm:
  - [x] A hero smart-scalar at row 2 spans the full headline section (Track B). CONFIRMED: T1 at (2, 0, 24, 4).
  - [x] The heatmap uses `display="table"` with `table.pivot=True`, NOT `display="pivot"` (Track C's fallback). CONFIRMED.
  - [x] Card names use the "trailing 30 days" / "this week vs last week" vocabulary (Track C's rename). CONFIRMED.
  - [x] Row 14 is paired (close-rate + lead-source-performance) and row 20 is the coverage heatmap full-width (Track A's layout). NOTE: pre-E the paired row was row 12 (not 14); row 14 is post-E. Track A's layout uses rows 12/18/24/31/39 — confirmed by reading the file.
  - [x] If any of the above is NOT the case, the predecessors didn't merge as planned. STOP AND ASK. — all confirmed, proceeded.
- [x] Pre-create the worktree per `.claude/rules/agents.md` Rule 1:
  ```bash
  git worktree add \
    .claude/worktrees/track-E-stl-v131-authoring-polish \
    -b Davv5/Track-E-STL-v131-Authoring-Polish \
    origin/main
  ```
- [x] `cd .claude/worktrees/track-E-stl-v131-authoring-polish` and verify the pre-flight checks above hold inside the worktree too.

### 1. Item 4 — new rollup `stl_data_freshness.sql` + YAML doc

Smallest-blast-radius task first: ship the dbt rollup for the freshness card before the dashboard code that consumes it.

#### 1a. Create `dbt/models/marts/rollups/speed_to_lead/stl_data_freshness.sql`

- [x] Create the file with exactly this content:

  ```sql
  -- One-row rollup exposing the latest booking timestamp on sales_activity_detail
  -- plus the run timestamp of the dbt build. Feeds the "Data as of HH:MM"
  -- freshness number card on the Speed-to-Lead dashboard.
  --
  -- Grain: exactly 1 row. Materialized as a table so Metabase's result cache +
  -- BigQuery's native result cache both get a stable identity to key off.
  --
  -- Why max(booked_at), not current_timestamp(): per Metabase Learn
  -- "BI Dashboard Visualization Best Practices" (corpus: Metabase Learn
  -- notebook), a freshness card should show "the latest value of a timestamp
  -- column that represents the data's natural reporting cadence" — NOT the
  -- query execution time. current_timestamp() would be a tautology (it is
  -- always "now"). max(booked_at) tells the viewer the latest event the
  -- mart has seen, which is what "data is current as of X" really means.
  {{ config(materialized='table') }}

  select
      max(booked_at)      as last_booking_at,
      current_timestamp() as run_ts
  from {{ ref('sales_activity_detail') }}
  ```

- [x] Build it: `cd .. && source .venv/bin/activate && set -a && source .env && set +a && cd dbt && dbt build --target dev --select stl_data_freshness`. PASS: 1 success, 0 failures. BQ job: `15f01b93-1293-4574-ab55-717734c83660`. Note: built `sales_activity_detail` upstream first (not materialised in dev_david).
- [x] Spot-check the row: `bq query ... select * from dev_david.stl_data_freshness`. PASS: 1 row, last_booking_at=2026-04-20 13:40:46, run_ts=2026-04-22 21:38:33.

#### 1b. Add the model doc to `_stl__models.yml`

- [x] Append a new model block to `dbt/models/marts/rollups/speed_to_lead/_stl__models.yml`:

  ```yaml
  - name: stl_data_freshness
    description: >
      One-row rollup feeding the "Data as of" freshness number card on the
      Speed-to-Lead dashboard. Grain: exactly one row. Exposes the latest
      booking timestamp on sales_activity_detail and the dbt run timestamp
      so viewers can tell at a glance how current the dashboard is.
    columns:
      - name: last_booking_at
        description: >
          Max(booked_at) on sales_activity_detail. Represents the latest event
          the Speed-to-Lead data layer has seen. This is what the "Data as of"
          freshness tile on the dashboard displays, formatted as a datetime.
      - name: run_ts
        description: >
          current_timestamp() evaluated at dbt build time. Useful for
          distinguishing "the mart is stale because no new bookings" vs
          "the mart is stale because dbt hasn't run recently."
  ```

- [-] Run `dbt docs generate` locally to sanity-check the YAML parses — skipped per plan guidance ("skip if docs-gen is slow; the YAML is copy-from-the-plan so syntax-safe").

### 2. Item 1, part A — Add dashboard parameters (Date + SDR)

The only native-SQL-question shape compatible with dashboard filters in Metabase is a **template-tag-backed query**. Per the Metabase Learn corpus (source: *"SQL variables and field filters"*), each card that participates in a filter needs its own template tag; the dashboard's `parameters` array + each dashcard's `parameter_mappings` array do the binding.

#### 2a. Decide the filter bind surface

Not every card can be filtered. The rollups that carry each dimension:

| Rollup | has a date column the filter can narrow? | has an SDR column the filter can narrow? |
|---|---|---|
| `stl_headline_trend_weekly` | `week_start` (12 rows, one per week) | no (not SDR-grained) |
| `stl_response_time_distribution_30d` | no date column (pre-aggregated to 6 buckets) | no |
| `stl_outcome_by_touch_bucket_30d` | no date column | no |
| `stl_source_outcome_30d` | no date column | no |
| `stl_sdr_leaderboard_30d` | no date column (aggregated) | `sdr_name` ✓ |
| `stl_coverage_heatmap_30d` | no date column | no |
| `stl_attribution_quality_30d` | no date column | no |
| `stl_data_freshness` (new) | `last_booking_at` (1 row) | no |
| `stl_lead_detail_recent` | `booked_at` (lead grain) ✓ | `sdr_name` ✓ |
| `stl_headline_7d` | `computed_at` (1 row) | no |

**Therefore the Track E filter surface is:**

- **Date filter** wires to: `stl_headline_trend_weekly` (T1/T2/T3/T4/T5/T6 hero tiles) + `stl_lead_detail_recent` (Lead Detail dashboard). Every `_30d` card is **pre-aggregated at the rollup layer** and cannot be meaningfully filtered by dashboard date — its window is fixed by the rollup's `WHERE` clause. This is acceptable partial coverage per Metabase Learn (source: *"Adding filters and making interactive BI dashboards"*, Metabase Learn notebook — "it is perfectly acceptable to leave some cards unfiltered").
- **SDR filter** wires to: `stl_sdr_leaderboard_30d` (SDR Leaderboard card) + `stl_lead_detail_recent`. Every other card is pre-aggregated without an SDR dimension.

**This is a known v1.3.1 constraint.** Document it in the dashboard's top-of-file docstring (a one-liner) + in the WORKLOG Decisions. v1.4 could optionally add SDR-grained variants of the `_30d` rollups and re-wire; out of scope here.

- [x] **Verify the SDR column name is `sdr_name`** by re-reading `dbt/models/marts/rollups/speed_to_lead/stl_sdr_leaderboard_30d.sql` line 6 — CONFIRMED: `coalesce(first_toucher_name, '(unassigned)') as sdr_name`.

#### 2b. Add template tags to the affected Metabase cards

The dbt rollup SQL is **not** edited. The template tag goes in the `native_query` string Metabase runs — which is currently hardcoded in `speed_to_lead.py`'s `upsert_card` calls. That's where the tags go.

There is ONE subtle issue: **`trend_smartscalar` reads from `stl_headline_trend_weekly` which has a column `week_start`, not `booked_at`.** The dashboard date filter is most natural to express on `booked_at`-grained data. Here the query is already pre-aggregated to ISO-week granularity. The Metabase date filter semantics (*"Last 7 days including current period"*) will interact with `week_start` by clipping the 12-week series to the week(s) that fall in the 7-day window — which, for a 7-day filter, is at most one week-row.

**That's actually the right behavior.** The hero is "this week vs last week" — a 7-day filter means "just this week's row" and the smart-scalar shows the most recent value. A 30-day filter means ~4-5 week rows, and the smart-scalar comparator walks the series.

- [x] Edit `trend_smartscalar` (around line 56-80 of pre-Tracks-A/B/C `speed_to_lead.py`; may have moved after A/B/C) to add a `template_tags` kwarg and inject a field-filter `{{date_range}}` into the WHERE clause. Done: literal field-name form accepted by Metabase OSS 60.x. The rollup table `stl_headline_trend_weekly` exposes `week_start` (DATE). **Use a Field Filter (dimension target), not a basic input variable**, so the dashboard widget gets "Relative Date" options like "Last 7 days including current period":

  ```python
  def trend_smartscalar(*, name: str, field: str, fmt: dict) -> dict:
      return upsert_card(
          mb,
          name=name,
          collection_id=coll["id"],
          database_id=db_id,
          display="smartscalar",
          native_query=(
              f"SELECT week_start, {field} "
              "FROM `dee-data-ops-prod.marts.stl_headline_trend_weekly` "
              # Field-filter WHERE (no column + operator; Metabase injects the subquery).
              # [[...]] makes the whole clause optional so the card renders
              # standalone when no filter is bound.
              "[[WHERE {{date_range}}]] "
              "ORDER BY week_start"
          ),
          template_tags={
              "date_range": {
                  "id": "date-range-weekly",
                  "name": "date_range",
                  "display-name": "Date range",
                  "type": "dimension",
                  "dimension": ["field", "week_start", {"base-type": "type/Date"}],
                  "widget-type": "date/all-options",
                  "default": None,
              },
          },
          visualization_settings={
              "scalar.field": field,
              "scalar.comparisons": [{"id": "1", "type": "previousPeriod"}],
              **_col_settings({field: fmt}),
          },
      )
  ```

  Caveats to verify before shipping:
  - The `["field", "week_start", {"base-type": "type/Date"}]` dimension-reference shape is Metabase OSS 60.x's literal-field form (no `field_id`, matched by column name). If the instance doesn't accept literal-name field refs, fall back to the numeric `field_id` — discover via `mb.get(f"/card/{card_id}")` after a first upsert and read `dataset_query.native.template-tags.date_range.dimension`. **Executor may pick sensible default + note in commit message** if literal form fails; use the numeric form returned by the GUI-authored equivalent (author once in dev Metabase, copy the shape).
  - Metabase Learn corpus confirms field filters omit the `=` operator and the column name (source: *"Field Filters"*, Metabase Learn notebook). The `[[WHERE {{date_range}}]]` wrapper + optional-clause braces `[[ ]]` is the canonical pattern (source: same).

- [x] Edit `stl_lead_detail_recent` card in the same way (search for `name="Lead Detail — recent bookings"`). Target column: `booked_at` (TIMESTAMP). Done: `date_range` + `sdr_filter` Field Filters added alongside existing tags.

  ```python
  # Inside the detail_card upsert_card call:
  native_query=(
      "SELECT booked_at, full_name, email, sdr_name, mins_to_touch, "
      "is_within_5_min_sla, had_any_sdr_activity_within_1_hr, "
      "lead_source, first_touch_campaign, close_outcome, lost_reason, "
      "attribution_quality_flag "
      "FROM `dee-data-ops-prod.marts.stl_lead_detail_recent` "
      "WHERE 1=1 "
      "[[AND CAST(is_within_5_min_sla AS STRING) = {{within_5min}}]] "
      "[[AND {{date_range}}]] "
      "ORDER BY booked_at DESC"
  ),
  template_tags={
      "within_5min": { ... },  # keep existing
      "date_range": {
          "id": "date-range-detail",
          "name": "date_range",
          "display-name": "Date range",
          "type": "dimension",
          "dimension": ["field", "booked_at", {"base-type": "type/DateTime"}],
          "widget-type": "date/all-options",
          "default": None,
      },
  },
  ```

#### 2c. Add SDR filter template tag to the leaderboard + detail cards

The SDR dimension is `sdr_name` on `stl_sdr_leaderboard_30d` (aliased from `first_toucher_name`) and `stl_lead_detail_recent`.

- [x] On the leaderboard card (search for `name="SDR Leaderboard..."` — post-Track-C its name is `"SDR leaderboard, trailing 30 days"` or similar, whatever Track C shipped). Done: `[[WHERE {{sdr_filter}}]]` + dimension template tag added.

  ```python
  native_query=(
      "SELECT sdr_name, bookings, within_5min, pct_within_5min, "
      "median_mins, closed_won, pct_closed_won "
      "FROM `dee-data-ops-prod.marts.stl_sdr_leaderboard_30d` "
      "[[WHERE {{sdr_filter}}]] "
      "ORDER BY bookings DESC"
  ),
  template_tags={
      "sdr_filter": {
          "id": "sdr-filter-leaderboard",
          "name": "sdr_filter",
          "display-name": "SDR",
          "type": "dimension",
          "dimension": ["field", "sdr_name", {"base-type": "type/Text"}],
          "widget-type": "string/=",
          "default": None,
      },
  },
  ```

- [x] On the Lead Detail card, add `sdr_filter` template tag alongside the date + within_5min tags. Done: `[[AND {{sdr_filter}}]]` added to WHERE; `sdr_filter` dimension tag added.

#### 2d. Add `parameters` to the dashboard object; add `parameter_mappings` to each bound dashcard

- [x] On the `upsert_dashboard(... name="Speed-to-Lead" ...)` call (the parent dashboard, around line 421 pre-A/B/C), add a `parameters` kwarg. Done: both parameters added. Q1 resolved: `"past7days~"` is the canonical string (Metabase did not rewrite it).

  ```python
  dash = upsert_dashboard(
      mb,
      name="Speed-to-Lead",
      collection_id=coll["id"],
      description=( ... existing ... ),
      parameters=[
          {
              "id": "date-range-dash",
              "name": "Date range",
              "slug": "date_range",
              "type": "date/all-options",
              # Default: "Last 7 days including current period" per Metabase
              # Learn's internal relative-date string convention. Source:
              # "Filter with date filters" (Metabase Learn notebook) — the
              # "Include current period" toggle on "Last N period" filters
              # extends the range to include today's in-progress data.
              "default": "past7days~",
          },
          {
              "id": "sdr-filter-dash",
              "name": "SDR",
              "slug": "sdr",
              "type": "string/=",
              "default": None,  # all SDRs
          },
      ],
  )
  ```

  **Verification caveat:** Metabase OSS's internal relative-date-string format is not documented in the Metabase Learn notebook (the corpus answer flagged this as "outside knowledge, verify independently"). Two candidates: `"past7days~"` (tilde denotes include-current) or `"past7days-include-this"`. **Executor: dry-run the upsert, then `mb.get(f"/dashboard/{dash['id']}")` and inspect `parameters[0].default`.** If Metabase rewrote or rejected the string, try the alternate form. If both fail, GUI-author once in the dev Metabase instance (set filter to "Last 7 days" + toggle "Include current period" on), then `mb.get` the dev dashboard and copy the exact default string. **Executor may pick sensible default + note in commit message** after empirical verification; cite the decision in the WORKLOG.

- [x] On the main `set_dashboard_cards(...)` call for the `Speed-to-Lead` dashboard, add `parameter_mappings` to each affected dashcard. Done: T1-T6 wired to date_range, leaderboard wired to sdr_filter.

  ```python
  # T1 dashcard with date_range wired
  {
      "card_id": t1["id"], "row": 2, "col": 0, "size_x": 8, "size_y": 3,
      "visualization_settings": {},
      "parameter_mappings": [
          {
              "parameter_id": "date-range-dash",
              "card_id": t1["id"],
              "target": ["dimension", ["template-tag", "date_range"]],
          },
      ],
  },
  # ... same shape for T2, T3, T4, T5, T6 (all reading stl_headline_trend_weekly)

  # SDR leaderboard dashcard with BOTH date_range (no — leaderboard is 30d
  # rollup, no date column) and sdr_filter.
  # → Only wire sdr_filter on leaderboard, NOT date_range:
  {
      "card_id": t8["id"], "row": 27, "col": 0, "size_x": 16, "size_y": 7,
      "visualization_settings": {},
      "parameter_mappings": [
          {
              "parameter_id": "sdr-filter-dash",
              "card_id": t8["id"],
              "target": ["dimension", ["template-tag", "sdr_filter"]],
          },
      ],
  },
  ```

  Non-wired dashcards (every `_30d` card except leaderboard) keep their existing dashcard dict, with no `parameter_mappings` key. Per corpus, this leaves them unfiltered-by-dashboard — acceptable.

- [x] The **Lead Detail child dashboard** (`Speed-to-Lead — Lead Detail`) also needs a matching `parameters` entry for `date_range` + `sdr_filter` so clicking through from the parent preserves filter context. Done: both parameters + parameter_mappings added to detail_dash + detail_card dashcard.

#### 2e. Sanity-check the parameter defaults after first upsert

Idempotent re-runs of `speed_to_lead.py` should not churn parameter defaults. Metabase sometimes rewrites relative-date strings on `GET`. After first run:

- [x] `python -m ops.metabase.authoring.dashboards.speed_to_lead` (first run). Exit 0, no orphan cards.
- [x] Inspected `parameters[0].default` via `mb.get('/dashboard/3')`. Q1 resolved: `"past7days~"` is the canonical Metabase string. No update needed.
- [x] Re-run a second time. Exit 0, no orphan cards. Idempotent confirmed.

### 3. Item 2 — Markdown section dividers

Four text dashcards. Each is `card_id=None`, `virtual_card.display="text"`, `size_x=24`, `size_y=1`. They slot between existing rows, shifting subsequent content down by 1 row per inserted divider.

#### 3a. Plan the new row arithmetic

**Current row map (post-A/B/C — see pre-flight Task 0 for verification):**

```
Row  0 — header banner                             (0,  0, 24, 2)
Row  2 — T1 hero (full-width or 24-span smart-scalar per Track B's ship)
...    — T2/T3 supporting chips (Track B's ship)
Row  5 — T4 | T5 | T6 volume smart-scalars
Row  8 — Response-Time Distribution | Close Rate by Touch Time (paired, Track A ship)
Row 14 — Lead Source Performance (full-width, Track A ship)
Row 20 — SDR Coverage Heatmap (full-width)
Row 27 — SDR Leaderboard | Lead Tracking Match Rate
Row 35 — Data refreshed footer (may or may not exist as-is post-A)
```

**Post-Track-E row map (4 section dividers inserted + 1 freshness tile + 1 footer text card):**

```
Row  0 — header banner                              (0,  0, 24, 2)
Row  2 — "## Speed Metrics" section divider         (2,  0, 24, 1)
Row  3 — T1 hero  +  freshness tile top-right       (3,  0, ...)  ← see Item 4 layout below
Row  7 — T4 | T5 | T6 volume smart-scalars          (7,  0, 8, 3 each)
Row 10 — "## Distribution & Outcome" section divider (10, 0, 24, 1)
Row 11 — Response-Time Distribution | Close Rate     (11, 0, 12, 6) | (11, 12, 12, 6)
Row 17 — Lead Source Performance (full-width)       (17, 0, 24, 6)
Row 23 — "## Coverage & Rep Performance" divider    (23, 0, 24, 1)
Row 24 — SDR Coverage Heatmap (full-width)          (24, 0, 24, 6)
Row 30 — SDR Leaderboard (full-width — Match rate donut gets demoted below) (30, 0, 24, 7)
Row 37 — "## Data Quality Notes" section divider    (37, 0, 24, 1)
Row 38 — Lead Tracking Match Rate (demoted half-width) (38, 0, 12, 7) + optional spacer
Row 45 — Footer refresh scalar (existing "Data refreshed")      (45, 0, 12, 2)
Row 47 — Footer markdown text card (Item 3)         (47, 0, 24, 2)
```

**Important notes on the row math above:**
- The numbers are a *proposed* layout. Track A's actual ship may have chosen slightly different row positions (full-width vs half-width on certain rows) — the executor should walk the CURRENT dashcards list in `speed_to_lead.py`, compute the +1-per-divider offset cumulatively, and land the final numbers. **Do not blindly copy the table above.** Use it as a sanity check after re-computing.
- T1 hero's size (`size_y`) depends on what Track B shipped. If Track B made it 4-tall spanning 24-wide, the freshness tile has to find a different home (see Item 4 layout-alternative below).
- The demoted match-rate donut **MUST stay under the "Data Quality Notes" divider** per the round-2 research finding. If Track A's ship placed it elsewhere, move it here as part of Track E.

#### 3b. Add a helper for the text dashcard shape

- [x] At the top of `main()`, below `_col_settings`, add a small helper:

  ```python
  def _text_dashcard(*, row: int, text: str, size_y: int = 1) -> dict:
      """Full-width markdown text dashcard for section dividers + footer."""
      return {
          "card_id": None,
          "row": row, "col": 0, "size_x": 24, "size_y": size_y,
          "visualization_settings": {
              "text": text,
              "virtual_card": {
                  "name": None, "display": "text", "archived": False,
                  "dataset_query": {}, "visualization_settings": {},
              },
          },
      }
  ```

#### 3c. Insert the four dividers into the `set_dashboard_cards` call

- [x] Insert, in the dashcards list, the four section dividers at their computed rows. Final rows: 2 (Speed Metrics), 13 (Distribution & Outcome), 26 (Coverage & Rep Performance), 40 (Data Quality Notes).

  ```python
  _text_dashcard(row=2,  text="## Speed Metrics"),
  _text_dashcard(row=10, text="## Distribution & Outcome"),
  _text_dashcard(row=23, text="## Coverage & Rep Performance"),
  _text_dashcard(row=37, text="## Data Quality Notes"),
  ```

  (Exact row numbers depend on Task 3a's recomputation — use whichever final values the executor lands on.)

- [x] Update the **layout-map comment block** at the top of `set_dashboard_cards` to reflect the new v1.3.1 layout. Done.

### 4. Item 3 — Footer markdown text card

- [x] At the bottom of the dashcards list, add a footer markdown dashcard below the existing `Data refreshed` scalar footer. Done: at row 47, size_y=2.

  ```python
  _text_dashcard(
      row=47,  # or whatever row is the first free slot below the existing footer scalar
      size_y=2,
      text=(
          "Last refreshed at 6am PT daily from BigQuery. "
          "Contact [mannyshah4344@gmail.com](mailto:mannyshah4344@gmail.com) "
          "for questions."
      ),
  ),
  ```

  Note: the existing scalar footer card (reading `computed_at` from `stl_headline_7d`) is a SCALAR, not text. This new one is text and SITS BELOW it. They are two separate footer elements — one dynamic number, one static markdown. Do not merge them.

- [x] Corpus ground the rationale in a comment next to the text dashcard:

  ```python
  # Source: "BI Dashboard Visualization Best Practices" (Metabase Learn
  # notebook) — "it is a best practice to use a text box as a dashboard
  # footer to include the maintainer's contact info, context on the data,
  # and helpful links."
  ```

### 5. Item 4 — Freshness number card

#### 5a. Add the card factory call

- [x] Add a new `upsert_card` call in `speed_to_lead.py`, near the existing footer scalar card (`footer = upsert_card(...)` for "Data refreshed"). Done: `data_as_of` card created reading `stl_data_freshness.last_booking_at`.

  ```python
  data_as_of = upsert_card(
      mb,
      name="Data as of",
      collection_id=coll["id"],
      database_id=db_id,
      display="scalar",
      native_query=(
          "SELECT last_booking_at "
          "FROM `dee-data-ops-prod.marts.stl_data_freshness`"
      ),
      visualization_settings=_col_settings({
          "last_booking_at": {
              "date_style": "MMM D, YYYY",
              "time_enabled": "minutes",
              # Per Metabase Learn corpus ("BI Dashboard Visualization
              # Best Practices") — a freshness tile should present the
              # latest timestamp on the underlying data, NOT
              # current_timestamp(). stl_data_freshness.last_booking_at
              # = max(booked_at) on sales_activity_detail.
          },
      }),
  )
  ```

#### 5b. Place the freshness tile near the hero

Round-2 decision: top-right of the hero row. Track B's hero may span 24-wide; if so, placing the freshness tile *inside* the hero row is impossible. Two layout options; pick whichever fits what Track B shipped:

- **Option A (if T1 hero is ≤ 16 wide):** Place `data_as_of` at `(row=T1_row, col=16, size_x=8, size_y=3)` — adjacent-right of the hero.
- **Option B (if T1 hero spans full 24 wide):** Place `data_as_of` at its own mini-row just above the Speed Metrics divider — `(row=1, col=16, size_x=8, size_y=1)` — compact, top-right corner, above the header? Not ideal.
- **Option C (always safe):** Place it as a small tile in the **footer row** next to the existing "Data refreshed" scalar: `(row=45, col=12, size_x=6, size_y=2)`. Grounded in Metabase Learn corpus: "you can pair this Number card with a Markdown text card explaining the underlying data's natural reporting cadence" — Option C does exactly that, neighbored to the footer markdown.

- [x] **Executor picked Option C.** T1 hero is full-width 24×4, so Option A not viable. Option C: `(row=45, col=12, size_x=6, size_y=2)` adjacent-right of "Data refreshed" footer scalar.

- [x] After placing the dashcard, update the layout-map comment block. Done.

### 6. Run the authoring script + verify

- [x] Source the venv + Metabase env. Done.
- [x] Run `python -m ops.metabase.authoring.dashboards.speed_to_lead`. Exit 0, no orphan cards. Dashboard id=3 (Speed-to-Lead), dashboard id=4 (Lead Detail).
- [x] Re-run a second time. Exit 0, idempotent confirmed.
- [x] **Render verification:**
  - [x] `curl -sSI https://34-66-7-243.nip.io/public/dashboard/3` → HTTP/2 200.
  - [-] Manual browser spot-check — deferred to David post-merge per Track C precedent. Interactive filter behaviour for anonymous viewers on public share not tested in-session (Q3 limitation documented in WORKLOG Open Threads).
    - [-] Date filter pill, SDR filter pill, filter narrowing — deferred to manual check.
    - [-] Section dividers, footer markdown, freshness tile render — deferred to manual check.

### 7. Interaction with Track D's caching

Track D may have set `cache_ttl = 21600s` on the Speed-to-Lead dashboard before Track E runs. The authoring script re-PUTs the dashboard via `upsert_dashboard`, which may or may not clobber `cache_ttl` depending on `sync.py`'s PUT-payload shape.

- [x] After the first Track E run: `mb.get('/dashboard/3').get('cache_ttl')` → `21600`. Track D's TTL survived. `sync.py`'s `upsert_dashboard` PUT payload doesn't include `cache_ttl`, so Metabase preserves the existing value. No fix to `sync.py` needed.

### 8. Ground the shipped work in the rule

- [x] Edit `.claude/rules/metabase.md`'s `## Lessons learned` section, add exactly one bullet. Done.

  ```markdown
  - Dashboard filters on native-SQL questions bind via template tags. Use
    Field Filters (type=dimension, target shape `["dimension", ["template-tag",
    "<tag>"]]`) for smart date/category widgets; omit the column name and
    `=` operator in the SQL (Metabase injects the subquery). Wrap the WHERE
    in `[[...]]` so the card renders standalone when unfiltered. Partial
    filter coverage — leaving pre-aggregated cards unbound — is acceptable
    when the rollup doesn't carry the filter's dimension
    (sources: "Field Filters" + "Adding filters and making interactive BI
    dashboards", Metabase Learn notebook).
  ```

- [-] The `PostToolUse` hook auto-syncs the rule to the Data Ops notebook — hook is in the main Claude Code session, not in this worktree context. Sync may fire asynchronously.

### 9. WORKLOG + handover + commit

- [ ] Append a dated WORKLOG entry per `.claude/rules/worklog.md`. Shape:

  ```markdown
  ## 2026-04-22 — Track E: Speed-to-Lead v1.3.1 authoring polish

  **What happened**
  - Added stl_data_freshness rollup (max(booked_at) from sales_activity_detail).
  - Added dashboard-level Date filter (default: Last 7 days including current
    period) + SDR filter (default: all SDRs) to Speed-to-Lead dashboard.
  - Wired Date filter to T1-T6 hero tiles and Lead Detail card. Wired SDR
    filter to SDR Leaderboard + Lead Detail. Pre-aggregated _30d cards
    intentionally left unbound per Metabase Learn "partial coverage is
    acceptable" guidance.
  - Added 4 markdown section dividers (Speed Metrics / Distribution &
    Outcome / Coverage & Rep Performance / Data Quality Notes).
  - Added footer markdown text card with maintainer contact.
  - Added "Data as of" freshness number card reading stl_data_freshness.
  - Corpus grounding: Metabase Learn sources "Adding filters and making
    interactive BI dashboards", "Field Filters", "BI Dashboard Visualization
    Best Practices", "Filter with date filters".

  **Decisions**
  - Date filter default "Last 7 days including current period" because sales-
    floor decision cadence is weekly. Chose Field Filter (dimension target)
    over basic date variable so the UI offers relative-date options.
  - SDR filter defaults to unset — the leaderboard IS the point of the page;
    an all-SDRs default preserves the comparison-across-reps story.
  - Pre-aggregated _30d cards NOT re-wired to the dashboard date filter; a
    meaningful rebinding would require SDR/date-grained variants of the
    rollups (v1.4 scope). Partial coverage is corpus-blessed.
  - Freshness tile shows max(booked_at), not current_timestamp() — former
    is a meaningful currency indicator, latter is a tautology.
  - Era filter remains out of scope (v1.4 follow-up).

  **Open threads**
  - If Track D landed first, verify dashboard cache_ttl survived Track E's
    upsert_dashboard re-PUT (Task 7).
  - If public share disables interactive filters, document in future
    handover as known Metabase OSS 60.x limitation.
  - Metabase OSS relative-date default-string format empirically verified
    at run time — record the canonical form (`past7days~` vs
    `past7days-include-this`) in CLAUDE.local.md for future tracks.
  ```

- [x] Create handover doc at `docs/handovers/Davv5-Track-E-Execution-2026-04-22_21-37.md`. All four items documented: BQ job ID, Option C layout, `"past7days~"` canonical string, cache_ttl=21600 survived.

- [x] `git add` staged files per list.
- [x] Commit locally. Done. NOT pushed. NOT PR opened.

## Decisions already made

Every decision below is grounded in a round-2 corpus query; the citation goes inline in `speed_to_lead.py` or in the WORKLOG per `.claude/rules/agents.md` Rule 4.

### §1 — Date filter default: "Last 7 days including current period"

- Sales-floor decision cadence is weekly, not monthly. "Include current period" extends the window to include today's in-progress data — per Metabase Learn's *"Filter with date filters"* (corpus source `213272ec-7839-4e10-b762-bdad0a05408d`), this is a dedicated toggle on relative-date filters that "adds another full period that includes the current date."
- NOT 30d. SDRs and sales leadership review this page weekly, not monthly; a 30-day default would hide the this-week-vs-last-week story the hero tiles tell.
- Use a **Field Filter (dimension target)**, not a basic input variable — Field Filters unlock the "Relative Date" widget type (source: *"Field Filters"*, Metabase Learn notebook, source `9e261983-3d7a-480a-bc83-e7cccd280ba3`).

### §2 — Template tags go at the Metabase card-SQL layer, NOT in dbt rollup SQL

- The user's brief asked the executor to *"research which is cleaner"* between adding `{{date_range}}` in the dbt rollup vs. the Metabase card's native_query. Answer: **Metabase card-SQL layer only**, for three reasons:
  1. **`{{ }}` collision.** dbt uses `{{ ref() }}` / `{{ config() }}` as its own templating syntax. Adding Metabase template tags (`{{date_range}}`) to a `.sql` file in `dbt/models/**` is either impossible (dbt tries to resolve them at compile time) or requires Jinja-escape acrobatics. Native Metabase template tags live in the Metabase card definition, which is plain SQL — no dbt compile step in the middle.
  2. **Separation of concerns** (source: *"How to Create a Data Modeling Pipeline (3 Layer Approach)"*, Data Ops notebook). dbt owns the materialized mart; Metabase owns the BI presentation. Parameters are a BI-presentation concern.
  3. **Idempotency.** dbt rollups are append-only artifacts — they should not need to change every time a dashboard filter is added. Template tags belong in authoring-script-generated native SQL so the dbt layer stays stable.
- The only dbt change Track E makes is the *new* `stl_data_freshness` rollup. Every existing `stl_*_30d` rollup's SQL is unchanged.

### §3 — Partial filter coverage is intentional, not a bug

- `stl_response_time_distribution_30d`, `stl_outcome_by_touch_bucket_30d`, `stl_source_outcome_30d`, `stl_coverage_heatmap_30d`, `stl_attribution_quality_30d` are all pre-aggregated without SDR or per-day grain. The dashboard Date + SDR filters **will not bind to these cards.**
- Corpus-blessed: *"it is perfectly acceptable to leave some cards unfiltered"* (Metabase Learn source `8e991cc3-4340-412d-8a9b-44d3e77b7d78`, *"Adding filters and making interactive BI dashboards"*).
- Document in the dashboard's top-of-file docstring so future track authors don't mistake the partial coverage for a scripting gap.

### §4 — Section-divider markdown headers

- Exactly four, in order:
  - `## Speed Metrics` — above the hero row
  - `## Distribution & Outcome` — above the paired response-time + close-rate row
  - `## Coverage & Rep Performance` — above the heatmap row
  - `## Data Quality Notes` — above the demoted match-rate donut
- Round-2 decision: **stay as one long-scroll dashboard, not split to tabs.** Source: *"BI Dashboard Visualization Best Practices"* (Metabase Learn source `ffa397bb-e573-4465-8c62-67ff5ad45a23`) — "it is better to have a dashboard that requires some scrolling than to omit relevant critical signals" + *"a great story with just a handful of questions or even a single question"* (source `38ebfd5d-3863-4fae-be9a-28f41de148dc`). 14 tiles, one narrative (SDR speed) = single dashboard.

### §5 — Footer maintainer-contact markdown

- Exact body: `"Last refreshed at 6am PT daily from BigQuery. Contact [mannyshah4344@gmail.com](mailto:mannyshah4344@gmail.com) for questions."` — David's email per his pre-launch staging preference (engagement overlay, `CLAUDE.local.md`).
- Full-width (`size_x=24`), 2 rows tall (`size_y=2`), position at the very bottom below the existing `Data refreshed` scalar.
- Source: *"BI Dashboard Visualization Best Practices"* (Metabase Learn source `ffa397bb-e573-4465-8c62-67ff5ad45a23`) — "it is a best practice to use a text box as a dashboard footer to include the maintainer's contact info."

### §6 — Freshness card content is `max(booked_at)`, not `current_timestamp()`

- `max(booked_at)` answers "what's the latest event this mart has seen?" — a real freshness signal. `current_timestamp()` would be tautologically "now." Source: corpus round-2 answer, *"BI Dashboard Visualization Best Practices"*, Metabase Learn notebook.
- Materialized as a `table` (not a view) so Metabase's result cache + BigQuery's native result cache both get a stable row to key off.

### §7 — SDR column name confirmed as `sdr_name`

- `stl_sdr_leaderboard_30d` line 6 exposes `coalesce(first_toucher_name, '(unassigned)') as sdr_name`. `stl_lead_detail_recent` also uses `sdr_name`. The mart-level column is `first_toucher_name`; the rollup layer aliases it. Use `sdr_name` in Metabase template tags.

### §8 — No edits to Track D's files

- `ops/metabase/authoring/infrastructure/**` is explicitly out of scope per the track brief. Cache TTL preserved or re-asserted in Task 7.

## Open questions

- **Q1 — Metabase OSS default relative-date string format (`past7days~` vs `past7days-include-this` vs other).** **Executor may pick sensible default + note in commit message.** The Metabase Learn corpus did not resolve this — the answer flagged it as "outside knowledge, verify independently." Task 2e has the empirical procedure: first-run the script with `"past7days~"`, inspect the stored default via `mb.get(...)`, and if Metabase rewrote it, update the literal to match. If both candidate strings fail, GUI-author the filter once in dev Metabase, copy the exact string, commit.

- **Q2 — Freshness tile layout (Option A vs Option C).** **Executor may pick sensible default + note in commit message.** Depends on what Track B shipped for the T1 hero's width. If T1 is ≤16 wide → Option A (top-right of hero row). If T1 is full 24 wide → Option C (next to the footer "Data refreshed" scalar). Avoid Option B.

- **Q3 — Does the public share link support interactive dashboard filters on Metabase OSS 60.x?** **Executor must note result in WORKLOG Open Threads.** Some builds render filters read-only for anonymous viewers. Not a blocker for this track (the filters still work for authenticated viewers and for Track D's weekly email subscription), but a known-limitation to document.

- **Q4 — Is the existing `Data refreshed` footer scalar (reading `computed_at` from `stl_headline_7d`) still the right card to keep alongside the new `Data as of` tile?** **Executor must pick sensible default + note in WORKLOG.** Two possibilities:
  - (a) Keep both: `Data refreshed` (from `stl_headline_7d.computed_at`) = "when did dbt run" + new `Data as of` (from `stl_data_freshness.last_booking_at`) = "latest event the mart has seen." They communicate different things; keeping both is corpus-compatible.
  - (b) Retire the existing scalar and replace with `Data as of`. Cleaner but loses the dbt-run-time signal.
  - Recommended: (a). Documented as such in Task 5 + Task 6.

## Done when

- `dbt build --select stl_data_freshness` passes in dev; the rollup has 1 row with a real `last_booking_at` timestamp.
- `_stl__models.yml` carries a description block for `stl_data_freshness` with both column-level descriptions.
- `python -m ops.metabase.authoring.dashboards.speed_to_lead` exits 0; second run is idempotent (zero PUTs on unchanged cards).
- The public Speed-to-Lead dashboard URL returns HTTP 200 and renders:
  - [ ] A Date filter pill in the top bar, defaulted to "Last 7 Days" (with include-current-period semantics — verify by reading the filter's `default` via `mb.get`).
  - [ ] An SDR filter pill in the top bar, defaulted to empty (all SDRs).
  - [ ] Changing the Date filter to a narrower window narrows the weekly hero tiles (T1-T6) and the Lead Detail drill-through.
  - [ ] Changing the SDR filter narrows the SDR Leaderboard and the Lead Detail drill-through; leaves the `_30d` cards unchanged (expected per §3).
  - [ ] Four section dividers rendered as bold `##` headings (Speed Metrics / Distribution & Outcome / Coverage & Rep Performance / Data Quality Notes).
  - [ ] Footer markdown card with maintainer email as clickable `mailto:` link.
  - [ ] `Data as of` tile rendering a datetime in `MMM D, YYYY HH:MM` format.
- `.claude/rules/metabase.md` has the new Lessons-learned bullet with its inline corpus citation.
- WORKLOG entry + handover doc committed locally (not pushed).
- If Track D ran first: dashboard `cache_ttl` is still `21600` after Track E's authoring-script run (Task 7).

## Context links

- **Round-1 and round-2 gap-analysis conclusions** — the two corpus-grounded audit rounds that surfaced the four items in this track. Not in-repo artifacts; they are the reasoning embedded in this session's prompt + the round-2 corpus queries saved in the Metabase Learn notebook history. The round-2 decisions are captured in full under "Decisions already made" above, so a cold executor has the outputs without needing the inputs.
- `ops/metabase/authoring/dashboards/speed_to_lead.py` — the single Python file Track E edits. Read end-to-end before starting; in particular the layout-map comment block inside `set_dashboard_cards` dictates the row arithmetic.
- `ops/metabase/authoring/sync.py` — `upsert_card` / `upsert_dashboard` / `set_dashboard_cards` semantics; read `upsert_dashboard` lines handling `parameters` + `cache_ttl` before running (confirm it preserves `cache_ttl` on re-PUT, else see Task 7).
- `ops/metabase/authoring/client.py` — `MetabaseClient.get/put/post`; no new verbs needed.
- `dbt/models/marts/rollups/speed_to_lead/_stl__models.yml` — existing model documentation; Task 1b appends a new `stl_data_freshness` model block at the end.
- `dbt/models/marts/rollups/speed_to_lead/stl_sdr_leaderboard_30d.sql` — canonical source for the `sdr_name` alias; re-verify before writing the SDR template tag.
- `.claude/rules/metabase.md` — Rule 1 ("Dashboards are code") is why all this goes in `speed_to_lead.py`, not the GUI.
- `.claude/rules/agents.md` — Rule 1 (worktree pre-create), Rule 4 (corpus citation mandatory), Rule 5 (parallel-safe).
- `CLAUDE.local.md` "Client-specific facts" — `MB_URL = https://34-66-7-243.nip.io`; David's email for the footer.
- Track A handover (predecessor in the series): `docs/handovers/Davv5-Track-A-stl-storytelling-restructure-2026-04-22_15-10.md`
- Track B handover (predecessor): `docs/handovers/Davv5-Track-B-stl-hero-headline-polish-2026-04-22_15-10.md`
- Track C handover (predecessor): `docs/handovers/Davv5-Track-C-stl-vocabulary-heatmap-fallback-2026-04-22_15-10.md`
- Track D handover (parallel sibling): `docs/handovers/Davv5-Track-D-stl-metabase-admin-config-2026-04-22_17-12.md`
- Metabase Learn corpus (notebook id `417bc4d3-59b4-4732-b8cb-d537dacf8477`) — the four sources cited in this plan, each queryable via the `ask-corpus` skill:
  - *"Adding filters and making interactive BI dashboards"* (source id `ffa397bb-e573-4465-8c62-67ff5ad45a23`) — filter-wiring + partial-coverage acceptability
  - *"Field Filters"* (source id `9e261983-3d7a-480a-bc83-e7cccd280ba3`) — Field Filter syntax, dimension targets, omit `=`
  - *"Filter with date filters"* (source id `213272ec-7839-4e10-b762-bdad0a05408d`) — "Include current period" toggle semantics
  - *"BI Dashboard Visualization Best Practices"* (source id `ffa397bb-e573-4465-8c62-67ff5ad45a23` — same file as #1) — long-scroll best practice, footer maintainer-contact, freshness-card pattern
  - *"Markdown in dashboards / Dashboards: organizing with text boxes"* (source id `f168a4d0-09b6-4768-b372-92d2e586234c`) — text cards as section dividers
- v1.3 ship commit (the reference point the rest of the v1.3-series tracks branch from): `a48c204`
