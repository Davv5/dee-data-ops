"""Speed-to-Lead dashboard — Page 1 of the D-DEE dashboard stack.

Reads pre-aggregated rollup tables under `dee-data-ops-prod.marts.stl_*`
(built by dbt from `sales_activity_detail`). Mirrors the tile shape
prescribed in `docs/looker-studio/page-1-speed-to-lead.md`.

Two dashboards are upserted in the `Speed-to-Lead` collection:
- `Speed-to-Lead` — v1.3.1 layout: header banner (row 0), four markdown
  section dividers bracketing the narrative arc (Speed Metrics / Distribution
  & Outcome / Coverage & Rep Performance / Data Quality Notes), T1 hero
  full-width at row 3, T2+T3 chips at row 7, volume scorecards at row 10,
  response-time distribution paired with close-rate-by-touch at row 14,
  full-width source-performance at row 20, coverage heatmap at row 27,
  full-width SDR leaderboard at row 33, lead-tracking match-rate donut
  under "Data Quality Notes" at row 41, footer row at row 45 (Data
  refreshed scalar + Data as of freshness tile), footer markdown text card
  at row 47.
  Dashboard-level filters: Date (Last 7 days, bound to T1-T6 weekly tiles
  + Lead Detail); SDR (unset = all, bound to leaderboard + Lead Detail).
  Filter coverage is intentionally partial — pre-aggregated _30d cards
  are not bound because their window is fixed at the rollup layer.
  Source: "Adding filters and making interactive BI dashboards" (Metabase
  Learn notebook) — partial coverage is acceptable.
  Vocabulary pass: all "(weekly)"/"(30d)"/"(last 90d)" replaced with
  ", this week vs last week" / ", trailing 30 days" / ", trailing 90 days".
  Orphan-cleanup block archives superseded cards from Tracks A+B+C+E renames.
- `Speed-to-Lead — Lead Detail` — Page 1b lead-grain drill-down table

Run::

    source .venv/bin/activate
    set -a && source ops/metabase/.env.metabase && set +a
    python -m ops.metabase.authoring.dashboards.speed_to_lead

Re-running is a no-op for unchanged content — upserts match by
(name, collection_id). MB_URL comes from the env file; the API key is
fetched from Secret Manager at runtime.
"""
from __future__ import annotations

from ..client import MetabaseClient
from ..sync import (
    find_database_id,
    set_dashboard_cards,
    upsert_card,
    upsert_collection,
    upsert_dashboard,
)

DATABASE_NAME = "dee-data-ops-prod"

PCT_FMT = {"suffix": "%", "decimals": 1}
MIN_FMT = {"suffix": " minutes", "decimals": 1}
NUM_FMT = {"number_separators": ",.", "decimals": 0}


def _col_settings(mapping: dict[str, dict]) -> dict:
    """Wrap per-column format dicts into Metabase's column_settings shape."""
    return {"column_settings": {f'["name","{k}"]': v for k, v in mapping.items()}}


def _text_dashcard(*, row: int, text: str, size_y: int = 1) -> dict:
    """Full-width markdown text dashcard for section dividers + footer.

    Source: "Markdown in dashboards / Dashboards: organizing with text boxes"
    (Metabase Learn notebook) — text cards used as section dividers and
    footer contact cards.
    """
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


def main() -> None:
    mb = MetabaseClient()
    db_id = find_database_id(mb, DATABASE_NAME)

    coll = upsert_collection(
        mb,
        name="Speed-to-Lead",
        color="#509EE3",
    )

    def trend_smartscalar(*, name: str, field: str, fmt: dict) -> dict:
        """Weekly smart-scalar tile: shows the latest week plus directional
        delta vs. the previous week. Reads a 12-row weekly time series from
        `stl_headline_trend_weekly` (week_start DATE + the metric field).

        v1.3.1: Added date_range Field Filter so the dashboard Date filter
        can narrow the weekly series. Uses [[...]] optional-clause wrapper
        so the card renders standalone when no filter is bound.
        Source: "Field Filters" (Metabase Learn notebook) — field filters
        omit the column name and = operator (Metabase injects the subquery);
        [[...]] makes the whole clause optional.

        The rollup's internal denominator is now SDR-scoped (per agent B's
        fix, 2026-04-22), so `pct_within_5min` on this table matches the
        headline-metric definition in CLAUDE.local.md."""
        return upsert_card(
            mb,
            name=name,
            collection_id=coll["id"],
            database_id=db_id,
            display="smartscalar",
            native_query=(
                f"SELECT week_start, {field} "
                "FROM `dee-data-ops-prod.marts.stl_headline_trend_weekly` "
                # Field-filter WHERE (no column + operator; Metabase injects
                # the subquery). [[...]] makes the whole clause optional so
                # the card renders standalone when no filter is bound.
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

    # ── Rows 3–12 — headline smart-scalars (weekly, vs last week) ───────
    # Tile names are client-facing. Vocabulary grounded in the Data Ops
    # corpus audit (2026-04-22): no engineering jargon ("SLA", "DQ"),
    # no abbreviations, business-phrased metrics only.
    #
    # v1.5 hero promotion (Track B): T1 is now the single full-width hero
    # at row 2 (24×4). T2 + T3 move to row 6 as equal-width chips (12×3
    # each). T3 renamed from "P90 Minutes…" to "Slowest 10% — minutes…"
    # for jargon-free client reading. The underlying field + fmt are
    # unchanged — distribution signal is preserved, only the name moves.
    #
    # v1.6 vocabulary pass (Track C): "(weekly)" suffix replaced with
    # ", this week vs last week" throughout all headline tiles. Comma +
    # sentence-case qualifier reads as description, not a label — consistent
    # with "% On-Time" / "Lead Source" precedent shipped in v1.3.
    #
    # v1.3.1 (Track E): all six T1-T6 tiles get date_range Field Filter via
    # trend_smartscalar's new template_tags + [[WHERE {{date_range}}]] clause.
    # Rows shifted by +1 (Speed Metrics section divider inserted at row 2).
    t1 = trend_smartscalar(
        name="% First Touch in 5 min, this week vs last week",
        field="pct_within_5min",
        fmt=PCT_FMT,
    )
    # Item #1 resolved: rollup column renamed median_mins → median_mins_sdr_only
    # to make the SDR-scoped denominator explicit in the column name. T2's
    # scalar.field + column_settings key track the rename.
    t2 = trend_smartscalar(
        name="Median minutes to first SDR touch, this week vs last week",
        field="median_mins_sdr_only",
        fmt=MIN_FMT,
    )
    # v1.5 rename (Track B, Option 2a): "P90" is analyst jargon. Renamed to
    # plain-English "Slowest 10%" so the client tile reads without a statistics
    # background. The underlying field + fmt are unchanged — only the card name
    # moves. upsert_card matches on (name, collection_id), so the old card
    # "P90 Minutes to First SDR Touch (weekly)" is orphaned; Track C cleans up.
    # v1.6 vocabulary pass (Track C): "(weekly)" → ", this week vs last week".
    t3 = trend_smartscalar(
        name="Slowest 10% — minutes to first touch, this week vs last week",
        field="p90_mins_sdr_only",
        fmt=MIN_FMT,
    )

    # ── Row 10 — T4, T5, T6 volume smart-scalars (weekly, vs last week) ──
    # Promoted from simple scorecards to smart-scalars in v1.3 so volume
    # direction week-over-week is visible on the headline page. Each tile
    # reads one value column from `stl_headline_trend_weekly`.
    # v1.5: shifted from row 5 to row 9 to make room for hero T1 + chips.
    # v1.6 vocabulary pass (Track C): "(weekly)" → ", this week vs last week".
    # v1.3.1: row shifted again (+1) for Speed Metrics section divider.
    t4 = trend_smartscalar(
        name="Bookings, this week vs last week",
        field="bookings",
        fmt=NUM_FMT,
    )
    t5 = trend_smartscalar(
        name="SDR-attributed, this week vs last week",
        field="sdr_attributed",
        fmt=NUM_FMT,
    )
    # v1.4: T6 replaced with pct_with_1hr_activity — orthogonal to T1
    # (T6 was `within_5min`, the raw numerator of T1's percentage — same data twice).
    # `pct_with_1hr_activity` adds new information: reachability over a longer
    # horizon, denominated on TOTAL bookings (not SDR-scoped), so it is also a
    # different denominator from T1. Column already exists on stl_headline_trend_weekly
    # (line 91). No dbt edit required (executor chose Option 3a).
    # click_behavior removed: T6 no longer drills to Lead Detail (tile-level drill
    # was for the raw-count tile's specific DQ context — not meaningful here).
    # v1.6 vocabulary pass (Track C): "(weekly)" → ", this week vs last week".
    t6 = trend_smartscalar(
        name="% with 1-hour activity, this week vs last week",
        field="pct_with_1hr_activity",
        fmt=PCT_FMT,
    )

    # ── T7 stacked area: daily volume by source ──────────────────────────
    # KEPT IN COLLECTION but NOT placed on Page 1 in v1.3. The new
    # Source × Outcome table (Row 14) supersedes the source-breakdown
    # signal this chart carried. Parked here for a future "Volume
    # drilldown" page so the card's history (view-count, metadata)
    # isn't lost.
    # v1.6 vocabulary pass (Track C): renamed for consistency with the trailing-90-days
    # qualifier convention even though t7 is parked (not on any dashboard page).
    t7 = upsert_card(
        mb,
        name="Daily booked calls, trailing 90 days, stacked by lead source",
        collection_id=coll["id"],
        database_id=db_id,
        display="area",
        native_query=(
            "SELECT booking_date, lead_source, bookings "
            "FROM `dee-data-ops-prod.marts.stl_daily_volume_by_source` "
            "ORDER BY booking_date, lead_source"
        ),
        visualization_settings={
            "graph.dimensions": ["booking_date", "lead_source"],
            "graph.metrics": ["bookings"],
            "stackable.stack_type": "stacked",
            "graph.x_axis.title_text": "",
            "graph.y_axis.title_text": "Bookings",
        },
    )

    # ── Row 14 left — cumulative response-time distribution (12×6) ──────
    # Bar (not area) communicates the cumulative-step shape better. One
    # bar per threshold (2m, 5m, 15m, 30m, 1h, 4h, 24h, >24h or similar;
    # shape is owned by the rollup). X-axis title "First touch within"
    # reads naturally next to bucket labels like "5 minutes".
    # v1.4: shrunk from full-width (24) to half-width (12) to pair with
    # close_rate_by_touch at col 12 — cause beside effect on one row.
    # v1.5: row shifted 8→12 to make room for hero T1 + T2/T3 chips.
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
    # v1.3.1 (Track E): row shifted 12→14 for Distribution & Outcome divider.
    response_time_dist = upsert_card(
        mb,
        name="Response-time distribution, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="bar",
        native_query=(
            "SELECT threshold_label, pct_within "
            "FROM `dee-data-ops-prod.marts.stl_response_time_distribution_30d` "
            "ORDER BY threshold_sort"
        ),
        visualization_settings={
            "graph.dimensions": ["threshold_label"],
            "graph.metrics": ["pct_within"],
            "graph.x_axis.title_text": "First touch within",
            "graph.y_axis.title_text": "% of SDR-attributed bookings",
            **_col_settings({"pct_within": PCT_FMT}),
        },
    )

    # ── Page 1b: lead-grain drill-down ───────────────────────────────────
    # Built BEFORE Page 1 cards that reference detail_dash["id"] in their
    # click-behavior wiring (source_outcome, t8 leaderboard). Metabase's
    # `[[ ... ]]` optional-clause wrapper skips the AND when the variable
    # is empty, so the card still renders standalone without any filter.
    #
    # v1.3.1 (Track E): added date_range Field Filter + sdr_filter Field
    # Filter alongside the existing within_5min/sdr_name/lead_source text
    # variables. date_range narrows booked_at via [[AND {{date_range}}]];
    # sdr_filter narrows sdr_name via [[AND {{sdr_filter}}]].
    # Source: "Field Filters" (Metabase Learn notebook) — field filters
    # use dimension targets and inject the subquery directly.
    detail_card = upsert_card(
        mb,
        name="Lead Detail — recent bookings",
        collection_id=coll["id"],
        database_id=db_id,
        display="table",
        native_query=(
            "SELECT booked_at, full_name, email, sdr_name, mins_to_touch, "
            "is_within_5_min_sla, had_any_sdr_activity_within_1_hr, "
            "lead_source, first_touch_campaign, close_outcome, lost_reason, "
            "attribution_quality_flag "
            "FROM `dee-data-ops-prod.marts.stl_lead_detail_recent` "
            "WHERE 1=1 "
            "[[AND CAST(is_within_5_min_sla AS STRING) = {{within_5min}}]] "
            "[[AND sdr_name = {{sdr_name}}]] "
            "[[AND lead_source = {{lead_source}}]] "
            "[[AND {{date_range}}]] "
            "[[AND {{sdr_filter}}]] "
            "ORDER BY booked_at DESC"
        ),
        template_tags={
            "within_5min": {
                "id": "within5min",
                "name": "within_5min",
                "display-name": "First Touch Within 5 min?",
                "type": "text",
                "default": None,
            },
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
            # v1.3.1: Date range Field Filter for dashboard Date parameter.
            # Binds to booked_at (TIMESTAMP). [[AND {{date_range}}]] wrapper
            # makes this clause optional so the card renders standalone.
            "date_range": {
                "id": "date-range-detail",
                "name": "date_range",
                "display-name": "Date range",
                "type": "dimension",
                "dimension": ["field", "booked_at", {"base-type": "type/DateTime"}],
                "widget-type": "date/all-options",
                "default": None,
            },
            # v1.3.1: SDR Field Filter for dashboard SDR parameter.
            # Binds to sdr_name (TEXT). Distinct from the existing text-type
            # sdr_name tag above (which is for the detail dashboard's own
            # category filter); this is the dashboard-level SDR filter.
            "sdr_filter": {
                "id": "sdr-filter-detail",
                "name": "sdr_filter",
                "display-name": "SDR (dashboard filter)",
                "type": "dimension",
                "dimension": ["field", "sdr_name", {"base-type": "type/Text"}],
                "widget-type": "string/=",
                "default": None,
            },
        },
        visualization_settings=_col_settings({
            "mins_to_touch": NUM_FMT,
            "booked_at": {"date_style": "MMM D, YYYY", "time_enabled": "minutes"},
        }),
    )

    detail_dash = upsert_dashboard(
        mb,
        name="Speed-to-Lead — Lead Detail",
        collection_id=coll["id"],
        description="Page 1b. Lead-grain detail table with search + filter controls.",
        parameters=[
            {"name": "First Touch Within 5 min?", "slug": "within_5min",  "id": "within5min",       "type": "category", "default": None},
            {"name": "SDR",                       "slug": "sdr_name",     "id": "sdr_name_param",   "type": "category", "default": None},
            {"name": "Lead Source",               "slug": "lead_source",  "id": "lead_source_param","type": "category", "default": None},
            # v1.3.1: Date range + SDR dashboard-level filters added so
            # clicking through from the parent Speed-to-Lead dashboard
            # preserves filter context.
            {
                "id": "date-range-dash",
                "name": "Date range",
                "slug": "date_range",
                "type": "date/all-options",
                "default": None,
            },
            {
                "id": "sdr-filter-dash",
                "name": "SDR filter",
                "slug": "sdr_filter",
                "type": "string/=",
                "default": None,
            },
        ],
    )

    set_dashboard_cards(
        mb,
        dashboard_id=detail_dash["id"],
        cards=[
            {
                "card_id": detail_card["id"],
                "row": 0, "col": 0, "size_x": 24, "size_y": 16,
                "visualization_settings": {},
                # Wire all dashboard-level parameters to the card's template-tags.
                "parameter_mappings": [
                    {
                        "parameter_id": "within5min",
                        "card_id": detail_card["id"],
                        "target": ["variable", ["template-tag", "within_5min"]],
                    },
                    {"parameter_id": "sdr_name_param",   "card_id": detail_card["id"], "target": ["variable", ["template-tag", "sdr_name"]]},
                    {"parameter_id": "lead_source_param","card_id": detail_card["id"], "target": ["variable", ["template-tag", "lead_source"]]},
                    # v1.3.1: wire dashboard Date + SDR filters using dimension targets
                    # (Field Filters). Source: "Field Filters" (Metabase Learn notebook).
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": detail_card["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                    {
                        "parameter_id": "sdr-filter-dash",
                        "card_id": detail_card["id"],
                        "target": ["dimension", ["template-tag", "sdr_filter"]],
                    },
                ],
            },
        ],
    )

    # ── Row 14 right — close-rate by touch-time bucket (12×6) ───────────
    # Paired with response_time_dist at row 14 left: cause (distribution
    # curve) beside effect (close-rate). Primary metric close_rate_pct as
    # bar height; bookings in tooltip so viewers can gauge sample size.
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
    close_rate_by_touch = upsert_card(
        mb,
        name="Close rate by touch time, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="bar",
        native_query=(
            "SELECT touch_bucket, close_rate_pct, bookings "
            "FROM `dee-data-ops-prod.marts.stl_outcome_by_touch_bucket_30d` "
            "ORDER BY bucket_sort"
        ),
        visualization_settings={
            "graph.dimensions": ["touch_bucket"],
            "graph.metrics": ["close_rate_pct"],
            "graph.tooltip_columns": ["bookings"],
            "graph.x_axis.title_text": "Time to first SDR touch",
            "graph.y_axis.title_text": "Close rate",
            **_col_settings({
                "close_rate_pct": PCT_FMT,
                "bookings": NUM_FMT,
            }),
        },
    )

    # ── Row 20 — lead-source × outcome table (full-width 24×6, v1.5) ───────
    # Column aliasing via `column_title` mirrors the SDR leaderboard
    # convention — BI surface never shows snake_case.
    #
    # v1.4: lead_source column wired for per-row click-through to Lead Detail.
    # KNOWN LIMITATION (same as leaderboard): public share link unauthenticated
    # viewers cannot navigate cross-dashboard — Metabase swallows the click
    # silently (Discourse #23492, #20677). Authenticated users only.
    # source.type = "column" passes the clicked row's lead_source value into
    # the target dashboard's lead_source_param parameter.
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
    source_outcome = upsert_card(
        mb,
        name="Lead source performance, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="table",
        native_query=(
            "SELECT lead_source, bookings, pct_within_5min, "
            "show_rate_pct, close_rate_pct "
            "FROM `dee-data-ops-prod.marts.stl_source_outcome_30d` "
            "ORDER BY bookings DESC"
        ),
        visualization_settings={
            **_col_settings({
                "lead_source": {
                    "column_title": "Lead Source",
                    "click_behavior": {
                        "type": "link",
                        "linkType": "dashboard",
                        "targetId": detail_dash["id"],
                        "parameterMapping": {
                            "lead_source_param": {
                                "id": "lead_source_param",
                                "source": {"type": "column", "id": "lead_source", "name": "lead_source"},
                                "target": {"type": "parameter", "id": "lead_source_param"},
                            },
                        },
                    },
                },
                "bookings":        {**NUM_FMT, "column_title": "Bookings"},
                # show_mini_bar renders a horizontal bar inside each percentage
                # cell sized relative to the column max. Turns the table into
                # an at-a-glance visual ranking without a separate chart.
                # Key "show_mini_bar" verified by Metabase OSS precedent
                # (current standard for column_settings); /api/docs returned
                # a redirect (302) on this instance so confirmed by convention,
                # not by inline docs. Not added to "bookings" (count column) —
                # mini-bars on raw counts dominate the cell with little signal.
                "pct_within_5min": {**PCT_FMT, "column_title": "% On-Time",  "show_mini_bar": True},
                "show_rate_pct":   {**PCT_FMT, "column_title": "Show Rate",   "show_mini_bar": True},
                "close_rate_pct":  {**PCT_FMT, "column_title": "Close Rate",  "show_mini_bar": True},
            }),
            "table.pivot": False,
        },
    )

    # ── Row 27 — SDR coverage heatmap (day of week × hour of day, 24×6) ──
    # v1.6 (Track C) heatmap display fallback:
    #   BEFORE (v1.3–v1.5): display="pivot" + pivot_table.column_split.
    #   REASON FOR SWAP: pivot_table.column_split is a Pro feature on some
    #   OSS Metabase builds (61.x). The v1.3 comment flagged this fragility;
    #   Track C pre-emptively swaps to the table-pivot code path, which is a
    #   different implementation on the OSS side and does not depend on the
    #   Pro pivot module.
    #
    #   AFTER: display="table" + table.pivot=True. Metabase's table display
    #   has a built-in pivot (separate from the standalone "pivot" display
    #   type). Setting table.pivot_column="hour_of_day" pivots hours across
    #   columns; the remaining non-pivot non-cell column (day_of_week) becomes
    #   the row dimension. table.cell_column="pct_within_5min" fills the grid.
    #   table.column_formatting adds a red→yellow→green conditional range so
    #   the coverage scan is identical to the old heatmap at a glance.
    #
    #   /api/docs returned 302 (auth-gated) on this instance; key names
    #   confirmed via Metabase OSS 60.x documented table visualization_settings
    #   (same convention used for show_mini_bar on source_outcome above).
    #
    # v1.6 vocabulary pass (Track C): card name "SDR Coverage Heatmap —
    # Day x Hour (30d)" → "SDR coverage, day of week by hour of day,
    # trailing 30 days" (parenthetical abbreviations out, comma-qualifier in).
    coverage_heatmap = upsert_card(
        mb,
        name="SDR coverage, day of week by hour of day, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="table",
        native_query=(
            "SELECT day_of_week, day_sort, hour_of_day, pct_within_5min "
            "FROM `dee-data-ops-prod.marts.stl_coverage_heatmap_30d` "
            "ORDER BY day_sort, hour_of_day"
        ),
        visualization_settings={
            "table.pivot": True,
            "table.pivot_column": "hour_of_day",
            "table.cell_column": "pct_within_5min",
            **_col_settings({
                "pct_within_5min": {
                    **PCT_FMT,
                    "column_title": "% within 5 min",
                },
            }),
            # Conditional formatting: red (0%) → yellow (50%) → green (100%)
            # so operators can spot low-coverage slots at a glance. Range
            # keys match the Metabase OSS 60.x column_formatting schema.
            "table.column_formatting": [
                {
                    "columns": ["pct_within_5min"],
                    "type": "range",
                    "colors": ["#EE6E73", "#FFEB84", "#84BA5B"],
                    "min_type": "custom",
                    "min_value": 0,
                    "max_type": "custom",
                    "max_value": 100,
                    "operator": "=",
                },
            ],
        },
    )

    # ── Row 33 — SDR leaderboard (full-width 24×7, v1.5) ────────────────
    # Column headers aliased to Title Case via `column_title` so the
    # BI surface never shows snake_case — corpus-mandated separation of
    # the database naming layer from the client-facing layer.
    #
    # v1.4: sdr_name column wired for per-row click-through to Lead Detail.
    # KNOWN LIMITATION (same as the old T6 tile-level click): on the *public*
    # share link, unauthenticated viewers cannot navigate cross-dashboard —
    # Metabase silently swallows the click (Discourse #23492, #20677).
    # source.type = "column" passes the clicked row's sdr_name value into
    # the target dashboard's sdr_name_param parameter.
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
    # v1.3.1 (Track E): added sdr_filter Field Filter so the dashboard SDR
    # parameter can narrow the leaderboard to a single rep. Source:
    # "Field Filters" (Metabase Learn notebook).
    t8 = upsert_card(
        mb,
        name="SDR leaderboard, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="table",
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
        visualization_settings={
            **_col_settings({
                "sdr_name": {
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
                "bookings":        {**NUM_FMT, "column_title": "Bookings"},
                "within_5min":     {**NUM_FMT, "column_title": "Within 5 min"},
                "pct_within_5min": {**PCT_FMT, "column_title": "% Within 5 min"},
                "median_mins":     {"suffix": " minutes", "decimals": 1, "column_title": "Median minutes"},
                "closed_won":      {**NUM_FMT, "column_title": "Closed Won"},
                "pct_closed_won":  {**PCT_FMT, "column_title": "Win Rate"},
            }),
            "table.pivot": False,
        },
    )

    # ── Row 41 — Lead-tracking match-rate donut (DQ tile, 12×4) ─────────
    # Demoted from row 27 prime real estate to footer-row DQ tile in v1.4.
    # v1.3.1 (Track E): further demoted under "## Data Quality Notes"
    # section divider at row 40. Resized from 12×2 to 12×4 for readability.
    # DQ signal is still useful, just not headline-tier. Categories remapped
    # in SQL from engineering flag tokens to business-phrased states:
    #   clean         → Matched
    #   no_sdr_touch  → No SDR touch yet
    #   role_unknown  → Unassigned rep
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
    t9 = upsert_card(
        mb,
        name="Lead tracking match rate, trailing 30 days",
        collection_id=coll["id"],
        database_id=db_id,
        display="pie",
        native_query=(
            "SELECT "
            "  CASE flag "
            "    WHEN 'clean'        THEN 'Matched' "
            "    WHEN 'no_sdr_touch' THEN 'No SDR touch yet' "
            "    WHEN 'role_unknown' THEN 'Unassigned rep' "
            "    ELSE flag "
            "  END AS category, "
            "  bookings "
            "FROM `dee-data-ops-prod.marts.stl_attribution_quality_30d` "
            "ORDER BY bookings DESC"
        ),
        visualization_settings={
            "pie.dimension": "category",
            "pie.metric": "bookings",
            "pie.show_legend": True,
            "pie.legend_position": "bottom",
            "pie.percent_visibility": "inside",
        },
    )

    # ── Footer — refresh timestamp ───────────────────────────────────────
    # Q4 decision (Track E): keep this scalar alongside the new "Data as of"
    # tile. They signal different things:
    #   - "Data refreshed" = when dbt ran (computed_at from stl_headline_7d)
    #   - "Data as of" = latest event in the mart (max(booked_at) from stl_data_freshness)
    # Keeping both gives viewers the dbt-run-time signal + the data-currency
    # signal. Source: "BI Dashboard Visualization Best Practices" (Metabase
    # Learn notebook) — pairing a freshness tile with context is best practice.
    footer = upsert_card(
        mb,
        name="Data refreshed",
        collection_id=coll["id"],
        database_id=db_id,
        display="scalar",
        native_query="SELECT computed_at FROM `dee-data-ops-prod.marts.stl_headline_7d`",
        visualization_settings=_col_settings({
            "computed_at": {"date_style": "MMMM D, YYYY", "time_enabled": "minutes"}
        }),
    )

    # ── Footer — "Data as of" freshness tile ─────────────────────────────
    # v1.3.1 (Track E, Item 4): freshness number card backed by the new
    # stl_data_freshness rollup (max(booked_at) on sales_activity_detail).
    # Placed at row 45 col 12 (Option C — adjacent-right of "Data refreshed"
    # scalar in the footer row). Option A (top-right of T1 hero) was ruled
    # out because T1 is full-width 24×4 — no col 16 slot available.
    # Source: "BI Dashboard Visualization Best Practices" (Metabase Learn
    # notebook) — "pair this Number card with a Markdown text card explaining
    # the underlying data's natural reporting cadence."
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

    # ── Dashboard: Speed-to-Lead (Page 1) ────────────────────────────────
    # v1.3.1 (Track E): added dashboard-level Date + SDR parameters.
    # - Date (type=date/all-options, default="past7days~" = "Last 7 days
    #   including current period"): binds to T1-T6 weekly tiles + Lead
    #   Detail. Default "past7days~" is a candidate string — verify
    #   empirically on first run (Q1: Metabase may rewrite to a canonical
    #   form; read back via mb.get(f"/dashboard/{dash['id']}") and update
    #   if Metabase normalized it). Source: "Filter with date filters"
    #   (Metabase Learn notebook) — "include current period" extends the
    #   window to include today's in-progress data.
    # - SDR (type=string/=, default=None = all SDRs): binds to leaderboard
    #   + Lead Detail. Unset default preserves the comparison-across-reps
    #   story that is the point of the leaderboard.
    dash = upsert_dashboard(
        mb,
        name="Speed-to-Lead",
        collection_id=coll["id"],
        description=(
            "Page 1 of the D-DEE dashboard stack. Grain: one row per Calendly "
            "booking event. Headline: % of bookings where the first outbound "
            "human SDR touch lands within 5 minutes."
        ),
        parameters=[
            {
                "id": "date-range-dash",
                "name": "Date range",
                "slug": "date_range",
                "type": "date/all-options",
                # "Last 7 days including current period" per Metabase Learn
                # "Filter with date filters" (corpus: Metabase Learn notebook).
                # Q1: verify this string format is correct empirically on
                # first run. Alternate candidate: "past7days-include-this".
                # If Metabase rewrites/rejects this, copy the canonical form
                # from mb.get(f"/dashboard/{dash['id']}")['parameters'][0]['default'].
                "default": "past7days~",
            },
            {
                "id": "sdr-filter-dash",
                "name": "SDR",
                "slug": "sdr",
                "type": "string/=",
                "default": None,  # all SDRs — leaderboard comparison story intact
            },
        ],
    )

    # Header text card — scope definition + time-window map for public
    # viewers. Virtual dashcard (card_id=None, virtual_card.display='text');
    # Metabase renders the `text` key as Markdown.
    #
    # Era filter (briefing item #7) — v1.3 ships WITHOUT an interactive
    # era_flag parameter. Rationale: every tile on Page 1 reads from a
    # rollup, not raw sales_activity_detail. The weekly rollup's trailing
    # 12-week window and the 30-day rollups' trailing window both auto-
    # exclude the ramping era (pre-2026-03-16). Adding a template-tag
    # `{{era_flag}}` to every rollup query would require the rollups
    # themselves to carry era_flag (they don't — it's a mart-level dim
    # attribute). Deferred to v1.4 as an invasive change.
    header_text = (
        "### Speed-to-Lead — D-DEE\n"
        "**Metric:** % of Calendly bookings where the first *human* SDR "
        "touch (CALL or SMS, not automation) lands within 5 minutes, "
        "scoped to SDR-attributed bookings.  \n"
        "**Time windows:** weekly headline (this week vs. last) · "
        "30-day outcome + source + coverage · 90-day volume trend.  \n"
        "**Filter coverage:** Date filter narrows T1-T6 weekly tiles and "
        "Lead Detail. SDR filter narrows leaderboard and Lead Detail. "
        "Pre-aggregated 30-day cards are not filter-bound (window fixed "
        "at rollup layer) — this is intentional partial coverage "
        "(source: \"Adding filters and making interactive BI dashboards\", "
        "Metabase Learn notebook)."
    )
    header_dashcard = _text_dashcard(row=0, text=header_text, size_y=2)

    # v1.3.1 layout map (Track E — dashboard filters + section dividers +
    # footer text card + freshness tile).
    # Metabase dashboards use a 24-column grid.
    # Layout map (row, col, size_x, size_y):
    #
    #   Row  0 — header banner                                  (0,  0, 24, 2)
    #   Row  2 — "## Speed Metrics" section divider             (2,  0, 24, 1)
    #   Row  3 — T1 hero (% First Touch in 5 min, weekly)       (3,  0, 24, 4)
    #   Row  7 — T2 | T3 (supporting smart-scalars)             each 12 wide, 3 tall
    #             (7,  0, 12, 3) Median minutes  | (7, 12, 12, 3) Slowest 10%
    #   Row 10 — T4 | T5 | T6 (volume smart-scalars)            each 8 wide, 3 tall
    #   Row 13 — "## Distribution & Outcome" section divider    (13, 0, 24, 1)
    #   Row 14 — Response-time distribution (12) | Close rate by touch (12)
    #             (14, 0, 12, 6)                  | (14, 12, 12, 6)
    #   Row 20 — Lead source performance, trailing 30 days (full-width 24×6)
    #             (20, 0, 24, 6)
    #   Row 26 — "## Coverage & Rep Performance" section divider (26, 0, 24, 1)
    #   Row 27 — SDR coverage heatmap (full-width 24×6, table-pivot)
    #             (27, 0, 24, 6)
    #   Row 33 — SDR leaderboard, trailing 30 days (full-width 24×7)
    #             (33, 0, 24, 7)
    #   Row 40 — "## Data Quality Notes" section divider        (40, 0, 24, 1)
    #   Row 41 — Lead tracking match rate donut (12×4)          (41, 0, 12, 4)
    #   Row 45 — Data refreshed scalar (12×2) | Data as of freshness tile (6×2)
    #             (45, 0, 12, 2)                 | (45, 12, 6, 2)
    #   Row 47 — Footer markdown text card (full-width 24×2)    (47, 0, 24, 2)
    set_dashboard_cards(
        mb,
        dashboard_id=dash["id"],
        cards=[
            header_dashcard,
            # Row 2 — Speed Metrics section divider
            _text_dashcard(row=2, text="## Speed Metrics"),
            # Row 3 — T1 hero: full-width 24×4, smartscalar auto-scales central number
            {
                "card_id": t1["id"], "row": 3, "col": 0, "size_x": 24, "size_y": 4,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t1["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            # Row 7 — T2 + T3 supporting chips (12 wide each)
            {
                "card_id": t2["id"], "row": 7, "col": 0, "size_x": 12, "size_y": 3,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t2["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            {
                "card_id": t3["id"], "row": 7, "col": 12, "size_x": 12, "size_y": 3,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t3["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            # Row 10 — volume smart-scalars
            {
                "card_id": t4["id"], "row": 10, "col": 0, "size_x": 8, "size_y": 3,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t4["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            {
                "card_id": t5["id"], "row": 10, "col": 8, "size_x": 8, "size_y": 3,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t5["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            {
                "card_id": t6["id"], "row": 10, "col": 16, "size_x": 8, "size_y": 3,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "date-range-dash",
                        "card_id": t6["id"],
                        "target": ["dimension", ["template-tag", "date_range"]],
                    },
                ],
            },
            # Row 13 — Distribution & Outcome section divider
            _text_dashcard(row=13, text="## Distribution & Outcome"),
            # Row 14 — response-time curve (left) paired with close-rate-by-touch (right)
            {"card_id": response_time_dist["id"],  "row": 14, "col": 0,  "size_x": 12, "size_y": 6, "visualization_settings": {}},
            {"card_id": close_rate_by_touch["id"], "row": 14, "col": 12, "size_x": 12, "size_y": 6, "visualization_settings": {}},
            # Row 20 — lead source performance (full-width, per-row click → Lead Detail, mini-bars on pct columns)
            {"card_id": source_outcome["id"], "row": 20, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 26 — Coverage & Rep Performance section divider
            _text_dashcard(row=26, text="## Coverage & Rep Performance"),
            # Row 27 — coverage heatmap (full width, 6 tall)
            {"card_id": coverage_heatmap["id"], "row": 27, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 33 — SDR leaderboard (full-width, per-row click → Lead Detail, SDR filter)
            {
                "card_id": t8["id"], "row": 33, "col": 0, "size_x": 24, "size_y": 7,
                "visualization_settings": {},
                "parameter_mappings": [
                    {
                        "parameter_id": "sdr-filter-dash",
                        "card_id": t8["id"],
                        "target": ["dimension", ["template-tag", "sdr_filter"]],
                    },
                ],
            },
            # Row 40 — Data Quality Notes section divider
            _text_dashcard(row=40, text="## Data Quality Notes"),
            # Row 41 — match-rate donut demoted to DQ tile (12×4)
            {"card_id": t9["id"], "row": 41, "col": 0, "size_x": 12, "size_y": 4, "visualization_settings": {}},
            # Row 45 — refresh footer (left 12) | freshness tile (right 6)
            # Q4: keep both scalars — "Data refreshed" = dbt run time,
            # "Data as of" = latest event in mart. Both are corpus-compatible
            # and signal different things (source: "BI Dashboard Visualization
            # Best Practices", Metabase Learn notebook).
            {"card_id": footer["id"],     "row": 45, "col": 0,  "size_x": 12, "size_y": 2, "visualization_settings": {}},
            {"card_id": data_as_of["id"], "row": 45, "col": 12, "size_x": 6,  "size_y": 2, "visualization_settings": {}},
            # Row 47 — footer markdown text card (full-width 24×2)
            # Source: "BI Dashboard Visualization Best Practices" (Metabase
            # Learn notebook) — "it is a best practice to use a text box as a
            # dashboard footer to include the maintainer's contact info, context
            # on the data, and helpful links."
            _text_dashcard(
                row=47,
                size_y=2,
                text=(
                    "Last refreshed at 6am PT daily from BigQuery. "
                    "Contact [mannyshah4344@gmail.com](mailto:mannyshah4344@gmail.com) "
                    "for questions."
                ),
            ),
        ],
    )
    # t7 (old 90d area) deliberately NOT in the dashcards list — card
    # persists in the collection as a parking slot for the planned
    # "Volume drilldown" page.
    _ = t7

    # ── Orphan cleanup — PERMANENT, runs on every script invocation ───────
    # WHY ORPHANS ACCUMULATE: upsert_card matches on (name, collection_id).
    # A card rename therefore POSTs a NEW card (new name = no match) and
    # leaves the OLD card unreferenced in the collection. Every rename in
    # Tracks A, B, and C created one orphan. This pass archives them all.
    #
    # DESIGN: archive (not delete) so a mistaken rename is recoverable from
    # the Metabase trash. Cost on each run: one GET /api/card (returns all
    # non-archived cards across the instance) — negligible.
    #
    # Future-proofing: this block is intentionally permanent. Any future card
    # rename that goes through this script will be self-cleaning on the next
    # run, with zero extra work from the author.
    kept_ids: set[int] = {
        t1["id"], t2["id"], t3["id"], t4["id"], t5["id"], t6["id"],
        t7["id"], t8["id"], t9["id"],
        response_time_dist["id"],
        close_rate_by_touch["id"],
        source_outcome["id"],
        coverage_heatmap["id"],
        footer["id"],
        detail_card["id"],
        data_as_of["id"],
    }
    all_collection_cards = [
        c for c in mb.cards()
        if c.get("collection_id") == coll["id"]
    ]
    orphaned = 0
    for card in all_collection_cards:
        if card["id"] not in kept_ids:
            mb.put(f"/card/{card['id']}", {"archived": True})
            print(f"Archived orphan card: {card['name']} (id={card['id']})")
            orphaned += 1
    if orphaned == 0:
        print("Orphan cleanup: no orphan cards found.")

    print(f"Speed-to-Lead:             {mb.url}/dashboard/{dash['id']}")
    print(f"Speed-to-Lead Lead Detail: {mb.url}/dashboard/{detail_dash['id']}")


if __name__ == "__main__":
    main()
