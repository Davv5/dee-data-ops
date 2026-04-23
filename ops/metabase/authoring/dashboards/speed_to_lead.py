"""Speed-to-Lead dashboard — Page 1 of the D-DEE dashboard stack.

Reads pre-aggregated rollup tables under `dee-data-ops-prod.marts.stl_*`
(built by dbt from `sales_activity_detail`). Mirrors the tile shape
prescribed in `docs/looker-studio/page-1-speed-to-lead.md`.

Two dashboards are upserted in the `Speed-to-Lead` collection:
- `Speed-to-Lead` — v1.6 layout: T1 hero (full-width 24×4), T2+T3
  supporting chips at row 6, volume scorecards at row 9 (T6 = % with
  1-hour activity), response-time distribution paired with close-rate-by-touch
  at row 12, full-width source-performance at row 18 with mini-bar formatting
  on percentage columns, coverage heatmap (table-pivot + conditional coloring)
  at row 24, full-width SDR leaderboard with per-row drill-through at row 31,
  lead-tracking match-rate donut demoted to footer-row DQ tile at row 39.
  Vocabulary pass: all "(weekly)"/"(30d)"/"(last 90d)" abbreviations replaced
  with ", this week vs last week" / ", trailing 30 days" / ", trailing 90 days".
  Orphan-cleanup block archives superseded cards from Tracks A+B+C renames.
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
                "ORDER BY week_start"
            ),
            visualization_settings={
                "scalar.field": field,
                "scalar.comparisons": [{"id": "1", "type": "previousPeriod"}],
                **_col_settings({field: fmt}),
            },
        )

    # ── Rows 2–11 — headline smart-scalars (weekly, vs last week) ───────
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
    # with "% On-Time" / "Lead Source Performance" precedent shipped in v1.3.
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

    # ── Row 9 — T4, T5, T6 volume smart-scalars (weekly, vs last week) ──
    # Promoted from simple scorecards to smart-scalars in v1.3 so volume
    # direction week-over-week is visible on the headline page. Each tile
    # reads one value column from `stl_headline_trend_weekly`.
    # v1.5: shifted from row 5 to row 9 to make room for hero T1 + chips.
    # v1.6 vocabulary pass (Track C): "(weekly)" → ", this week vs last week".
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

    # ── Row 12 left — cumulative response-time distribution (12×6) ──────
    # Bar (not area) communicates the cumulative-step shape better. One
    # bar per threshold (2m, 5m, 15m, 30m, 1h, 4h, 24h, >24h or similar;
    # shape is owned by the rollup). X-axis title "First touch within"
    # reads naturally next to bucket labels like "5 minutes".
    # v1.4: shrunk from full-width (24) to half-width (12) to pair with
    # close_rate_by_touch at col 12 — cause beside effect on one row.
    # v1.5: row shifted 8→12 to make room for hero T1 + T2/T3 chips.
    # v1.6 vocabulary pass (Track C): "(30d)" → ", trailing 30 days".
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
                # Wire all three dashboard-level parameters to the card's template-tags.
                "parameter_mappings": [
                    {
                        "parameter_id": "within5min",
                        "card_id": detail_card["id"],
                        "target": ["variable", ["template-tag", "within_5min"]],
                    },
                    {"parameter_id": "sdr_name_param",   "card_id": detail_card["id"], "target": ["variable", ["template-tag", "sdr_name"]]},
                    {"parameter_id": "lead_source_param","card_id": detail_card["id"], "target": ["variable", ["template-tag", "lead_source"]]},
                ],
            },
        ],
    )

    # ── Row 12 right — close-rate by touch-time bucket (12×6) ───────────
    # Paired with response_time_dist at row 12 left: cause (distribution
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

    # ── Row 18 — lead-source × outcome table (full-width 24×6, v1.5) ───────
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

    # ── Row 24 — SDR coverage heatmap (day of week × hour of day, 24×6) ──
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

    # ── Row 31 — SDR leaderboard (full-width 24×7, v1.5) ────────────────
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
            "ORDER BY bookings DESC"
        ),
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

    # ── Row 39 right — Lead-tracking match-rate donut (DQ tile, 12×2) ───
    # Demoted from row 27 prime real estate to footer-row DQ tile in v1.4.
    # DQ signal is still useful, just not headline-tier. Track C may
    # revisit the display type. Categories remapped in SQL from engineering
    # flag tokens to business-phrased states (Data Ops corpus audit 2026-04-22):
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

    # ── Row 0 — Data freshness tile (end-to-end raw ingest lag) ─────────
    # Track Z (2026-04-22): live-by-default ops indicator. Shows how many
    # minutes since the last raw GHL event landed in `raw_ghl.conversations`.
    # Answers: "How stale is the data we're rolling up?" If this reads > 5 min,
    # the ingest pipeline has paused — visible immediately.
    #
    # DISTINCT from Track E's "Data refreshed" footer tile (row 41 below):
    #   - Track Z tile (this one): "How stale is the raw data?" — sourced from
    #     raw_ghl.conversations._ingested_at. Live ops indicator. TOP of dashboard.
    #   - Track E tile (footer): "When did the rollup last recompute?" — sourced
    #     from stl_headline_7d.computed_at. BOTTOM of dashboard.
    # Both are needed; they measure different things in the freshness chain.
    #
    # Corpus: "Caching query results" (Metabase Learn, source d6a8e3ae) —
    # cache_ttl=0 ensures every render queries fresh (live-by-default).
    freshness_tile = upsert_card(
        mb,
        name="Data freshness (end-to-end lag)",
        collection_id=coll["id"],
        database_id=db_id,
        display="scalar",
        cache_ttl=0,
        native_query=(
            "SELECT timestamp_diff(current_timestamp(), max(_ingested_at), minute) "
            "AS minutes_since_raw_ingest "
            "FROM `dee-data-ops-prod.raw_ghl.conversations`"
        ),
        visualization_settings={
            "scalar.field": "minutes_since_raw_ingest",
            **_col_settings({
                "minutes_since_raw_ingest": {"suffix": " min", "decimals": 0},
            }),
        },
    )

    # ── Footer — refresh timestamp ───────────────────────────────────────
    # Track E (PR #50): "When did the rollup last recompute?" — a different
    # freshness question from the Track Z tile above. Stays at the footer.
    # Track E owns this tile; do not modify from this script (Track Z).
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

    # ── Dashboard: Speed-to-Lead (Page 1) ────────────────────────────────
    dash = upsert_dashboard(
        mb,
        name="Speed-to-Lead",
        collection_id=coll["id"],
        description=(
            "Page 1 of the D-DEE dashboard stack. Grain: one row per Calendly "
            "booking event. Headline: % of bookings where the first outbound "
            "human SDR touch lands within 5 minutes."
        ),
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
        "30-day outcome + source + coverage · 90-day volume trend."
    )
    header_dashcard = {
        "card_id": None,
        "row": 0, "col": 0, "size_x": 24, "size_y": 2,
        "visualization_settings": {
            "text": header_text,
            "virtual_card": {"name": None, "display": "text", "archived": False, "dataset_query": {}, "visualization_settings": {}},
        },
    }

    # v1.7 layout map (Track Z — live-by-default freshness tile at row 0).
    # All rows shifted down by 2 to make room for the new freshness tile.
    # Freshness tile: Track Z's end-to-end ingest lag indicator (top-left, 6×2).
    # Metabase dashboards use a 24-column grid.
    # Layout map (row, col, size_x, size_y):
    #
    #   Row  0 — freshness tile "Data freshness (end-to-end lag)"
    #             (0,  0, 6, 2) — top-left scalar; live-by-default ops indicator.
    #             Track Z (2026-04-22). Reads raw_ghl.conversations._ingested_at.
    #             Distinct from Track E's "Data refreshed" footer (row 41).
    #   Row  2 — header banner                                  (2,  0, 24, 2)
    #   Row  4 — T1 hero (% First Touch in 5 min, this week vs last week)
    #             (4,  0, 24, 4)  ← full-width hero; smartscalar auto-scales.
    #   Row  8 — T2 | T3 (supporting smart-scalars)             each 12 wide, 3 tall
    #             (8,  0, 12, 3) Median minutes … | (8, 12, 12, 3) Slowest 10% …
    #   Row 11 — T4 | T5 | T6 (volume smart-scalars)            each 8 wide, 3 tall
    #             T6 = % with 1-hour activity, this week vs last week
    #   Row 14 — Response-time distribution (12) | Close rate by touch time (12)
    #             (14, 0, 12, 6)                  | (14, 12, 12, 6)
    #             Cause (curve) beside effect (close-rate) — one story per row
    #   Row 20 — Lead source performance, trailing 30 days (full-width 24×6)
    #             (20, 0, 24, 6) — percentage columns have show_mini_bar:True
    #   Row 26 — SDR coverage, day of week by hour of day (full-width 24×6)
    #             (26, 0, 24, 6) — table-pivot + conditional cell coloring (v1.6)
    #   Row 33 — SDR leaderboard, trailing 30 days (full-width 24×7)
    #             (33, 0, 24, 7) — per-row click → Lead Detail
    #   Row 41 — Data refreshed footer | Lead tracking match rate (DQ tile)
    #             (41, 0, 12, 2)       | (41, 12, 12, 2)
    #             Track E owns "Data refreshed" footer; Track Z freshness tile is at row 0.
    header_dashcard["row"] = 2  # shifted down 2 from v1.6's row 0
    set_dashboard_cards(
        mb,
        dashboard_id=dash["id"],
        cards=[
            # Row 0 — freshness tile: 6×2 top-left, live-by-default ops indicator
            {"card_id": freshness_tile["id"], "row": 0, "col": 0, "size_x": 6, "size_y": 2, "visualization_settings": {}},
            header_dashcard,
            # Row 4 — T1 hero: full-width 24×4, smartscalar auto-scales central number
            {"card_id": t1["id"], "row": 4,  "col": 0,  "size_x": 24, "size_y": 4, "visualization_settings": {}},
            # Row 8 — T2 + T3 supporting chips (12 wide each) — median + slowest-10% read
            {"card_id": t2["id"], "row": 8,  "col": 0,  "size_x": 12, "size_y": 3, "visualization_settings": {}},
            {"card_id": t3["id"], "row": 8,  "col": 12, "size_x": 12, "size_y": 3, "visualization_settings": {}},
            # Row 11 — volume smart-scalars (T6 = % With 1-Hour Activity, no tile-level drill)
            {"card_id": t4["id"], "row": 11,  "col": 0,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t5["id"], "row": 11,  "col": 8,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t6["id"], "row": 11,  "col": 16, "size_x": 8, "size_y": 3, "visualization_settings": {}},
            # Row 14 — response-time curve (left) paired with close-rate-by-touch (right)
            {"card_id": response_time_dist["id"],  "row": 14, "col": 0,  "size_x": 12, "size_y": 6, "visualization_settings": {}},
            {"card_id": close_rate_by_touch["id"], "row": 14, "col": 12, "size_x": 12, "size_y": 6, "visualization_settings": {}},
            # Row 20 — lead source performance (full-width, per-row click → Lead Detail, mini-bars on pct columns)
            {"card_id": source_outcome["id"], "row": 20, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 26 — coverage heatmap (full width, 6 tall)
            {"card_id": coverage_heatmap["id"], "row": 26, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 33 — SDR leaderboard (full-width, per-row click → Lead Detail)
            {"card_id": t8["id"], "row": 33, "col": 0, "size_x": 24, "size_y": 7, "visualization_settings": {}},
            # Row 41 — refresh footer (left) | match-rate donut demoted to DQ tile (right)
            # Track E owns "Data refreshed" footer tile; Track Z freshness tile is at row 0.
            {"card_id": footer["id"], "row": 41, "col": 0,  "size_x": 12, "size_y": 2, "visualization_settings": {}},
            {"card_id": t9["id"],     "row": 41, "col": 12, "size_x": 12, "size_y": 2, "visualization_settings": {}},
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
        freshness_tile["id"],  # Track Z: top-of-dashboard raw ingest lag tile
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
