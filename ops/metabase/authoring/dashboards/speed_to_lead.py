"""Speed-to-Lead dashboard — Page 1 of the D-DEE dashboard stack.

Reads pre-aggregated rollup tables under `dee-data-ops-prod.marts.stl_*`
(built by dbt from `sales_activity_detail`). Mirrors the tile shape
prescribed in `docs/looker-studio/page-1-speed-to-lead.md`.

Two dashboards are upserted in the `Speed-to-Lead` collection:
- `Speed-to-Lead` — v1.3 layout: headline scorecards, volume scorecards,
  response-time distribution, close-rate-by-touch-bucket, source-performance,
  coverage heatmap, SDR leaderboard, lead-tracking match-rate donut.
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

    # ── Row 2 — T1, T2, T3 headline smart-scalars (weekly, vs last week) ─
    # Tile names are client-facing. Vocabulary grounded in the Data Ops
    # corpus audit (2026-04-22): no engineering jargon ("SLA", "DQ"),
    # no abbreviations, business-phrased metrics only.
    #
    # T3 changed in v1.3: `% Reached Within 1 Hr` retired. The 1-hour
    # threshold is now one step on the cumulative response-time curve
    # (Row 8). T3 here surfaces a tail-sensitivity metric (P90 minutes)
    # that complements the median — together they give a two-point read
    # on the response-time distribution.
    t1 = trend_smartscalar(
        name="% First Touch in 5 min (weekly)",
        field="pct_within_5min",
        fmt=PCT_FMT,
    )
    # Item #1 resolved: rollup column renamed median_mins → median_mins_sdr_only
    # to make the SDR-scoped denominator explicit in the column name. T2's
    # scalar.field + column_settings key track the rename.
    t2 = trend_smartscalar(
        name="Median Minutes to First SDR Touch (weekly)",
        field="median_mins_sdr_only",
        fmt=MIN_FMT,
    )
    t3 = trend_smartscalar(
        name="P90 Minutes to First SDR Touch (weekly)",
        field="p90_mins_sdr_only",
        fmt=MIN_FMT,
    )

    # ── Row 5 — T4, T5, T6 volume smart-scalars (weekly, vs last week) ──
    # Promoted from simple scorecards to smart-scalars in v1.3 so volume
    # direction week-over-week is visible on the headline page. Each tile
    # reads one value column from `stl_headline_trend_weekly`.
    t4 = trend_smartscalar(
        name="Bookings (weekly)",
        field="bookings",
        fmt=NUM_FMT,
    )
    t5 = trend_smartscalar(
        name="SDR-Attributed (weekly)",
        field="sdr_attributed",
        fmt=NUM_FMT,
    )
    t6 = trend_smartscalar(
        name="Within 5 min (weekly)",
        field="within_5min",
        fmt=NUM_FMT,
    )

    # ── T7 stacked area: daily volume by source ──────────────────────────
    # KEPT IN COLLECTION but NOT placed on Page 1 in v1.3. The new
    # Source × Outcome table (Row 14) supersedes the source-breakdown
    # signal this chart carried. Parked here for a future "Volume
    # drilldown" page so the card's history (view-count, metadata)
    # isn't lost.
    t7 = upsert_card(
        mb,
        name="Daily Booked Calls — last 90d, stacked by lead source",
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

    # ── Row 8 — NEW cumulative response-time distribution (24×6) ─────────
    # Bar (not area) communicates the cumulative-step shape better. One
    # bar per threshold (2m, 5m, 15m, 30m, 1h, 4h, 24h, >24h or similar;
    # shape is owned by the rollup). X-axis title "First touch within"
    # reads naturally next to bucket labels like "5 minutes".
    response_time_dist = upsert_card(
        mb,
        name="Response-Time Distribution (30d)",
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

    # ── Row 14 left — NEW close-rate by touch-time bucket (12×6) ─────────
    # Primary metric close_rate_pct as bar height; bookings carried in
    # the tooltip so viewers can gauge sample size per bucket.
    close_rate_by_touch = upsert_card(
        mb,
        name="Close Rate by Touch Time (30d)",
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

    # ── Row 14 right — NEW lead-source × outcome table (12×6) ────────────
    # Column aliasing via `column_title` mirrors the SDR leaderboard
    # convention — BI surface never shows snake_case.
    source_outcome = upsert_card(
        mb,
        name="Lead Source Performance (30d)",
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
                "lead_source":     {"column_title": "Lead Source"},
                "bookings":        {**NUM_FMT, "column_title": "Bookings"},
                "pct_within_5min": {**PCT_FMT, "column_title": "% On-Time"},
                "show_rate_pct":   {**PCT_FMT, "column_title": "Show Rate"},
                "close_rate_pct":  {**PCT_FMT, "column_title": "Close Rate"},
            }),
            "table.pivot": False,
        },
    )

    # ── Row 20 — NEW SDR coverage heatmap (day × hour, 24×6) ─────────────
    # Uses Metabase's `pivot` display with a column_split: rows=day_of_week,
    # columns=hour_of_day, values=pct_within_5min. day_sort is selected so
    # the pivot respects Monday→Sunday ordering (Metabase sorts rows by the
    # first-selected column when no explicit ordering is provided; including
    # day_sort keeps day_of_week labels but orders by the numeric sort key).
    #
    # Fallback: if the `pivot` display shape doesn't render correctly on
    # Metabase 61.x (pivot_table.column_split is a Pro feature on some
    # older OSS builds), swap display→"table" and drop the pivot_table
    # visualization_setting. Query remains identical.
    coverage_heatmap = upsert_card(
        mb,
        name="SDR Coverage Heatmap — Day x Hour (30d)",
        collection_id=coll["id"],
        database_id=db_id,
        display="pivot",
        native_query=(
            "SELECT day_of_week, day_sort, hour_of_day, pct_within_5min "
            "FROM `dee-data-ops-prod.marts.stl_coverage_heatmap_30d` "
            "ORDER BY day_sort, hour_of_day"
        ),
        visualization_settings={
            "pivot_table.column_split": {
                "rows": ["day_of_week"],
                "columns": ["hour_of_day"],
                "values": ["pct_within_5min"],
            },
            **_col_settings({"pct_within_5min": PCT_FMT}),
        },
    )

    # ── Row 27 left — SDR leaderboard (16×7) ─────────────────────────────
    # Column headers aliased to Title Case via `column_title` so the
    # BI surface never shows snake_case — corpus-mandated separation of
    # the database naming layer from the client-facing layer.
    t8 = upsert_card(
        mb,
        name="SDR Leaderboard (30d)",
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
                "sdr_name":        {"column_title": "SDR"},
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

    # ── Row 27 right — Lead-tracking match-rate donut (8×7) ──────────────
    # Categories remapped in SQL from engineering flag tokens to
    # business-phrased states (Data Ops corpus audit 2026-04-22):
    #   clean         → Matched
    #   no_sdr_touch  → No SDR touch yet
    #   role_unknown  → Unassigned rep
    t9 = upsert_card(
        mb,
        name="Lead Tracking Match Rate (30d)",
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

    # ── Page 1b: lead-grain drill-down ───────────────────────────────────
    # Built BEFORE Page 1 because the T6 dashcard's click-behavior needs
    # `detail_dash["id"]` to wire the cross-dashboard drill-through.
    #
    # The detail card accepts an optional `within_5min` template-tag so the
    # parent dashboard can pre-filter via click-behavior. Metabase's
    # `[[ ... ]]` optional-clause wrapper skips the AND when the variable
    # is empty, so the card still renders standalone.
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
            {
                "name": "First Touch Within 5 min?",
                "slug": "within_5min",
                "id": "within5min",
                "type": "category",
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
                # Wire the dashboard-level `within5min` parameter to the
                # card's `within_5min` template-tag.
                "parameter_mappings": [
                    {
                        "parameter_id": "within5min",
                        "card_id": detail_card["id"],
                        "target": ["variable", ["template-tag", "within_5min"]],
                    },
                ],
            },
        ],
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

    # T6 click-behavior: drill through to Lead Detail pre-filtered to
    # within-5-min rows. KNOWN LIMITATION: on the *public* share link,
    # unauthenticated viewers cannot navigate to another internal dashboard —
    # Metabase silently swallows the click (Discourse #23492, #20677). This
    # feature only activates for authenticated users; the user has
    # acknowledged the tradeoff.
    t6_click_behavior = {
        "click_behavior": {
            "type": "link",
            "linkType": "dashboard",
            "targetId": detail_dash["id"],
            "parameterMapping": {
                "within5min": {
                    "id": "within5min",
                    "source": {"type": "text", "id": "true", "name": "true"},
                    "target": {"type": "parameter", "id": "within5min"},
                },
            },
        },
    }

    # Metabase dashboards use a 24-column grid. Rows below the header card
    # shift down by 2 to make room for the banner. Layout map (row, col,
    # size_x, size_y):
    #
    #   Row  0 — header banner                        (0,  0, 24, 2)
    #   Row  2 — T1 | T2 | T3 (headline smart-scalars) each 8 wide, 3 tall
    #   Row  5 — T4 | T5 | T6 (volume smart-scalars)  each 8 wide, 3 tall
    #   Row  8 — Response-Time Distribution           (8,  0, 24, 6)
    #   Row 14 — Close Rate by Touch Time | Lead Source Performance
    #             (14, 0, 12, 6)          | (14, 12, 12, 6)
    #   Row 20 — SDR Coverage Heatmap                 (20, 0, 24, 6)
    #   Row 27 — SDR Leaderboard | Lead Tracking Match Rate
    #             (27, 0, 16, 7)| (27, 16, 8, 7)
    #   Row 35 — Data refreshed footer                (35, 0, 12, 2)
    set_dashboard_cards(
        mb,
        dashboard_id=dash["id"],
        cards=[
            header_dashcard,
            # Row 2 — headline smart-scalars
            {"card_id": t1["id"], "row": 2,  "col": 0,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t2["id"], "row": 2,  "col": 8,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t3["id"], "row": 2,  "col": 16, "size_x": 8, "size_y": 3, "visualization_settings": {}},
            # Row 5 — volume smart-scalars (T6 drills to Lead Detail)
            {"card_id": t4["id"], "row": 5,  "col": 0,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t5["id"], "row": 5,  "col": 8,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t6["id"], "row": 5,  "col": 16, "size_x": 8, "size_y": 3, "visualization_settings": t6_click_behavior},
            # Row 8 — response-time distribution (full width, 6 tall)
            {"card_id": response_time_dist["id"], "row": 8, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 14 — close rate by touch time | lead source performance
            {"card_id": close_rate_by_touch["id"], "row": 14, "col": 0,  "size_x": 12, "size_y": 6, "visualization_settings": {}},
            {"card_id": source_outcome["id"],      "row": 14, "col": 12, "size_x": 12, "size_y": 6, "visualization_settings": {}},
            # Row 20 — coverage heatmap (full width, 6 tall)
            {"card_id": coverage_heatmap["id"], "row": 20, "col": 0, "size_x": 24, "size_y": 6, "visualization_settings": {}},
            # Row 27 — SDR leaderboard | Match-rate donut
            {"card_id": t8["id"], "row": 27, "col": 0,  "size_x": 16, "size_y": 7, "visualization_settings": {}},
            {"card_id": t9["id"], "row": 27, "col": 16, "size_x": 8,  "size_y": 7, "visualization_settings": {}},
            # Row 35 — refresh footer
            {"card_id": footer["id"], "row": 35, "col": 0, "size_x": 12, "size_y": 2, "visualization_settings": {}},
        ],
    )
    # t7 (old 90d area) deliberately NOT in the dashcards list — card
    # persists in the collection as a parking slot for the planned
    # "Volume drilldown" page.
    _ = t7

    print(f"Speed-to-Lead:             {mb.url}/dashboard/{dash['id']}")
    print(f"Speed-to-Lead Lead Detail: {mb.url}/dashboard/{detail_dash['id']}")


if __name__ == "__main__":
    main()
