# Page 1 — Speed-to-Lead (click-spec)

Follow top-to-bottom. No design decisions — every field, filter, color
is prescribed. Target total build time: **2–3 hours** for the first pass,
~30 min of styling polish after.

---

## Prerequisites (one-time, ~10 min)

### 1. Build the rollup tables in prod

The page reads from 6 pre-aggregated views under
`dbt/models/marts/rollups/speed_to_lead/`. Build them before touching
Looker:

```bash
cd "/Users/david/Documents/data ops"
source .venv/bin/activate
set -a && source .env && set +a
cd dbt && dbt deps && dbt build --target prod --select models/marts/rollups/speed_to_lead
```

Verify in BQ:

```bash
bq ls --project_id=dee-data-ops-prod dee-data-ops-prod:marts | grep stl_
```

Expected: 6 tables — `stl_headline_7d`, `stl_headline_trend_daily`,
`stl_daily_volume_by_source`, `stl_sdr_leaderboard_30d`,
`stl_attribution_quality_30d`, `stl_lead_detail_recent`.

### 2. Set up the Looker Studio → BigQuery connection

1. Sign into Looker Studio at [lookerstudio.google.com](https://lookerstudio.google.com) with the Google account that has BigQuery access on `dee-data-ops-prod` (i.e., **dforero122@gmail.com**)
2. Click **Create → Data source → BigQuery**
3. Authorize BigQuery when prompted (this uses your Google OAuth — already granted since you're an Owner)
4. Repeat **Create → Data source → BigQuery** for each of the 6 rollup tables above. For each:
   - **MY PROJECTS** → `dee-data-ops-prod` → dataset `marts` → table `stl_<name>`
   - Name the data source exactly: `d_dee_stl_<name>` (e.g., `d_dee_stl_headline_7d`)
   - Leave all field types on auto-detected
   - **Data freshness**: set to **1 hour** (top-right of the connector config screen)
5. After all 6 are created, they appear in **Resource → Manage added data sources**

### 3. Create the empty report

1. Click **Create → Report**
2. When prompted for a data source, pick `d_dee_stl_headline_7d` (the first one; we'll add the rest below)
3. Name the report: **"D-DEE Speed-to-Lead v1 — Page 1"**
4. In the blank report, **Resource → Manage added data sources → Add a data source** → add the remaining 5 rollups so all 6 are available to tiles

### 4. Apply the theme

Open `theme.md` in this repo and configure **Theme and layout → Customize → Custom** with the typography, color palette, and chart defaults specified there. Do this BEFORE adding tiles so each tile inherits the right defaults.

### 5. Set page size

**Page → Current page settings → Canvas size → Custom** → 1440 × 1024 px.

---

## Layout grid

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "D-DEE Speed-to-Lead — v1"       [Date range ctrl] │  row 0 (y=0..80)
├─────────────────────────────────────────────────────────────┤
│ T1: % within 5  │ T2: Median min   │ T3: % 1-hr activity   │  row 1 (y=96..280)
│   scorecard     │   scorecard      │   scorecard           │
├─────────────────────────────────────────────────────────────┤
│ T4: Bookings 7d │ T5: SDR-attrib  │ T6: Within-5-min 7d    │  row 2 (y=296..440)
│   scorecard     │   scorecard     │   scorecard            │
├─────────────────────────────────────────────────────────────┤
│ T7: Daily booked calls, stacked by lead source              │  row 3 (y=456..760)
│   stacked area chart                                         │
├─────────────────────────────────────────────────────────────┤
│ T8: SDR leaderboard (table)    │  T9: Attribution quality  │  row 4 (y=776..960)
│                                │      donut                │
├─────────────────────────────────────────────────────────────┤
│ FOOTER: "Refreshed: <mart_refreshed_at>"                    │  row 5 (y=976..1024)
└─────────────────────────────────────────────────────────────┘

Secondary page (Page 1b) — lead-level drill-down table (full width)
```

---

## Tile-by-tile spec

### Header row

**Report title (text box)**
- Tool: **Insert → Text**
- Position: x=24, y=24, w=900, h=32
- Content: `D-DEE Speed-to-Lead — v1`
- Style: Inter 18pt Semibold, color `#111827`

**Date range control**
- Tool: **Insert → Date range control**
- Position: x=1144, y=24, w=272, h=32
- Default range: **Last 30 days**
- This control filters T7 (the main area chart) + T8 (leaderboard) automatically; T1–T6 have their own fixed 7-day window via the rollup view, so they're date-range-immune

---

### Row 1 — Headline scorecards (3 tiles)

#### T1 — % within 5-min SLA

- Tool: **Insert → Scorecard (with sparkline)**
- Position: x=24, y=96, w=448, h=184
- Data source: `d_dee_stl_headline_7d`
- **Metric**: `pct_within_5min_7d` (aggregation: Average — it's already aggregated)
- Format: Percent, 1 decimal, append `%`
- **Compare to**: Previous period (auto-calculated — leave off if rollup doesn't include prior period; use the trend sparkline below for visual comparison instead)
- **Sparkline**:
   - Blend in `d_dee_stl_headline_trend_daily`
   - Dimension: `booking_date`
   - Metric: `pct_within_5min`
   - Sparkline color: `#1D4ED8` (accent-primary)
- Tile title: `% Within 5-min SLA (7d)`
- Tile label: `SDR-attributed bookings touched in under 5 minutes`

#### T2 — Median min to first SDR touch

- Tool: **Scorecard (with sparkline)**
- Position: x=488, y=96, w=448, h=184
- Data source: `d_dee_stl_headline_7d`
- Metric: `median_mins_7d`
- Format: Number, 1 decimal, suffix ` min`
- Sparkline data: `d_dee_stl_headline_trend_daily` — dimension `booking_date`, metric `pct_within_5min` (trend showing the rate, not median — median can swing wildly day-to-day on low volume; rate is the smoother trend to show here)
  - **Alternative if you prefer the literal median sparkline**: build one more rollup `stl_median_trend_daily` and point here. v1 can use pct trend as a proxy.
- Tile title: `Median Min to First SDR Touch (7d)`
- Tile label: `Minutes elapsed between booking and first SDR outbound`

#### T3 — % with 1-hr activity (DQ diagnostic)

- Tool: **Scorecard**
- Position: x=952, y=96, w=464, h=184
- Data source: `d_dee_stl_headline_7d`
- Metric: `pct_with_1hr_activity_7d`
- Format: Percent, 1 decimal, append `%`
- Sparkline: skip (DQ diagnostic, not a trend-worthy metric)
- Tile title: `DQ — SDR activity within 1 hr (7d)`
- Tile label: `% of SDR-attributed bookings with ANY touch within 1 hour. Distinguishes "missed SLA" from "nobody tried"`
- Color:
  - If ≥ 90%: background `#D1FAE5`, text `#065F46`
  - If 70%–90%: background `#FEF3C7`, text `#92400E`
  - If < 70%: background `#FEE2E2`, text `#991B1B`

---

### Row 2 — Volume scorecards (3 tiles)

#### T4 — Bookings (7d)

- Tool: **Scorecard (with sparkline)**
- Position: x=24, y=296, w=448, h=144
- Data source: `d_dee_stl_headline_7d`
- Metric: `bookings_7d`
- Format: Number, thousands separator, 0 decimals
- Sparkline: `d_dee_stl_headline_trend_daily` — dim `booking_date`, metric `bookings`, color `#64748B`
- Tile title: `Bookings (7d)`
- Tile label: `Calendly events, all sources, last 7 days`

#### T5 — SDR-attributed (7d)

- Tool: **Scorecard (with sparkline)**
- Position: x=488, y=296, w=448, h=144
- Data source: `d_dee_stl_headline_7d`
- Metric: `sdr_attributed_7d`
- Format: Number, 0 decimals
- Sparkline: `d_dee_stl_headline_trend_daily` — `booking_date` × `sdr_attributed`
- Tile title: `SDR-Attributed (7d)`

#### T6 — Within 5 min (7d count)

- Tool: **Scorecard (with sparkline)**
- Position: x=952, y=296, w=464, h=144
- Data source: `d_dee_stl_headline_7d`
- Metric: `within_5min_7d`
- Format: Number, 0 decimals
- Sparkline: `d_dee_stl_headline_trend_daily` — `booking_date` × `within_5min`
- Tile title: `Within 5 Min (7d)`

---

### Row 3 — Main area chart

#### T7 — Daily booked calls, stacked by lead source

- Tool: **Insert → Area chart → Stacked**
- Position: x=24, y=456, w=1392, h=304
- Data source: `d_dee_stl_daily_volume_by_source`
- **Dimension (time)**: `booking_date`
- **Breakdown dimension**: `lead_source`
- **Metric**: `bookings` (SUM)
- **Stacking**: Stacked (not 100% stacked)
- Sort: by metric descending
- Legend: Top position
- Colors: apply the **stacked-series palette** from `theme.md` (10 ordered colors + grey for `other`)
- Axis:
  - X axis: date format `MMM D`, tick every 7 days
  - Y axis: number, thousands separator, title hidden
  - Gridlines: horizontal only, color `#F3F4F6`
- Tile title: `Daily Booked Calls — last 90 days, stacked by lead source`
- Tile label: none (chart speaks for itself)

---

### Row 4 — Leaderboard + attribution

#### T8 — SDR leaderboard

- Tool: **Insert → Table**
- Position: x=24, y=776, w=912, h=184
- Data source: `d_dee_stl_sdr_leaderboard_30d`
- Columns (in order):
  1. `sdr_name` — header "SDR", left-align, width 160
  2. `bookings` — header "Bookings", right-align, width 100, format `#,##0`
  3. `within_5min` — header "Within 5 min", right-align, width 110, format `#,##0`
  4. `pct_within_5min` — header "SLA %", right-align, width 110, format `0.0%` (divide-by-100 if needed), **conditional format** per `theme.md` traffic-light rules
  5. `median_mins` — header "Median Min", right-align, width 110, format `0.0`
  6. `closed_won` — header "Closed Won", right-align, width 100, format `#,##0`
  7. `pct_closed_won` — header "Close %", right-align, width 100, format `0.0%`
- Sort: `bookings` descending
- Show row numbers: OFF
- Pagination: OFF (only ~7 SDRs, fit on screen)
- Totals row: ON (sum for bookings / within_5min / closed_won; weighted avg for %)

#### T9 — Attribution quality donut

- Tool: **Insert → Pie chart** → Donut style
- Position: x=952, y=776, w=464, h=184
- Data source: `d_dee_stl_attribution_quality_30d`
- Dimension: `flag`
- Metric: `bookings`
- Donut hole: 60%
- Sort: by metric descending
- Colors (per flag — fixed, reuse across pages):
  - `clean`: `#059669` (accent-success)
  - `no_sdr_touch`: `#DC2626` (accent-danger)
  - `role_unknown`: `#D97706` (accent-warn)
  - `pre_utm_era`: `#94A3B8` (accent-neutral-2)
  - `ambiguous_contact_match`: `#64748B` (accent-neutral-1)
- Labels: show percentage inside slices, ≥ 5% only
- Legend: right side, vertical
- Tile title: `Attribution Quality Mix (30d)`
- Tile label: `clean = trustworthy rows; others are DQ-flagged`

---

### Row 5 — Footer

**Refresh timestamp**
- Tool: **Insert → Scorecard** (headless style)
- Position: x=24, y=976, w=600, h=32
- Data source: `d_dee_stl_headline_7d`
- Metric: `computed_at`
- Format: Date, `MMM D, YYYY h:mm A`
- Tile title: hidden
- Label: `Data refreshed:` (as part of tile display)
- Text color: `#6B7280` (text-secondary)

---

## Page 1b — Lead-level drill-down

Add a second page to the report via **Page → New Page** and name it `Page 1b — Lead Detail`.

Single full-width table:

- Tool: **Insert → Table**
- Position: x=24, y=96, w=1392, h=864
- Data source: `d_dee_stl_lead_detail_recent`
- Columns (all shown):
  1. `booked_at` — "Booked", format `MMM D h:mm A`, width 140
  2. `full_name` — "Lead", width 160
  3. `email` — width 200
  4. `sdr_name` — "SDR", width 140
  5. `mins_to_touch` — "Mins", right-align, format `#,##0`, width 80
  6. `is_within_5_min_sla` — "SLA Hit", boolean, width 80
  7. `had_any_sdr_activity_within_1_hr` — "1h Activity", boolean, width 100
  8. `lead_source` — "Source", width 140
  9. `first_touch_campaign` — "Campaign", width 180
  10. `close_outcome` — "Outcome", width 100
  11. `lost_reason` — "Lost reason", width 160
  12. `attribution_quality_flag` — "DQ", width 120
- Sort: `booked_at` descending
- Pagination: ON, 25 rows per page
- **Search bar**: ON (top of table)
- **Filter controls** (add above the table as separate filter components):
  - Dropdown: `sdr_name`
  - Dropdown: `is_within_5_min_sla`
  - Dropdown: `attribution_quality_flag`
  - Dropdown: `lead_source`
- Row row highlighting:
  - `is_within_5_min_sla = TRUE` → row background `#D1FAE5`
  - `is_within_5_min_sla = FALSE AND had_any_sdr_activity_within_1_hr = FALSE` → row background `#FEE2E2`

---

## Sharing settings

1. Click **Share** (top-right)
2. **Data credentials** → **Owner's credentials** (so viewers inherit your BQ access — no per-viewer OAuth)
3. **Link sharing** → **Anyone with the link can view**
   - For D-DEE internal only: use **Specific people** → add D-DEE stakeholder emails (Viewer role)
4. **Embed report**: leave off (default — bare link, per corpus)
5. Copy the share link → paste into `CLAUDE.local.md` under "Looker Studio report URL"

---

## Definition of done

- [ ] 6 rollup tables built green in `dee-data-ops-prod.marts.*`
- [ ] 6 data sources created in Looker Studio, freshness = 1hr
- [ ] Theme applied (typography, colors, chart defaults, page size)
- [ ] Page 1: header + 9 tiles + footer all placed per grid
- [ ] Page 1b: lead detail table with 4 filter controls + search
- [ ] SLA % conditional formatting works on the leaderboard
- [ ] Attribution donut colors match `theme.md`
- [ ] Report shared with owner's credentials, bare link mode
- [ ] URL written into `CLAUDE.local.md`
- [ ] Screenshot of Page 1 added to `dashboards/README.md`

---

## If a tile breaks

- **Scorecard shows blank** → rollup returned 0 rows for the 7-day window (no recent bookings). Check `stl_headline_7d` has a row.
- **Sparkline shows flat line** → blend didn't resolve. Verify both data sources (`stl_headline_7d` + `stl_headline_trend_daily`) are added to the report.
- **Area chart has 40+ series in legend** → `stl_daily_volume_by_source` isn't top-10-bucketing. Re-run the rollup.
- **Donut has wrong colors** → manual color override per series, per T9 spec.
- **Cannot find field in data source** → hit the refresh icon on the data source card in Resource Manager; Looker caches the schema.

## If this is going to take you longer than 3 hours

Stop and flag it. Something structural is off — either the rollups need
a different shape, or the spec missed a decision, or a Looker Studio
feature isn't behaving as assumed. Fix the spec, don't fix the report.
