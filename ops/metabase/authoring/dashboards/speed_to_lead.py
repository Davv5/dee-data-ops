"""Speed-to-Lead dashboard — Page 1 of the D-DEE dashboard stack.

Reads six pre-aggregated rollup tables under
`dee-data-ops-prod.marts.stl_*` (built by dbt from `sales_activity_detail`).
Mirrors the tile shape prescribed in `docs/looker-studio/page-1-speed-to-lead.md`.

Two dashboards are upserted in the `Speed-to-Lead` collection:
- `Speed-to-Lead` — T1–T9 + refresh footer (the headline page)
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

    def scorecard(*, name: str, field: str, fmt: dict) -> dict:
        return upsert_card(
            mb,
            name=name,
            collection_id=coll["id"],
            database_id=db_id,
            display="scalar",
            native_query=f"SELECT {field} FROM `dee-data-ops-prod.marts.stl_headline_7d`",
            visualization_settings=_col_settings({field: fmt}),
        )

    def trend_smartscalar(*, name: str, field: str, fmt: dict) -> dict:
        """Weekly smart-scalar tile: shows the latest week plus directional
        delta vs. the previous week. Reads a 12-row weekly time series from
        `stl_headline_trend_weekly` (week_start DATE + the metric field)."""
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

    # ── Row 1 — T1, T2, T3 headline smart-scalars (weekly, vs last week) ─
    # Tile names are client-facing. Vocabulary grounded in the Data Ops
    # corpus audit (2026-04-22): no engineering jargon ("SLA", "DQ"),
    # no abbreviations, business-phrased metrics only.
    #
    # T1 design tradeoff: we considered a gauge viz with 30/60/90% threshold
    # coloring, but Metabase's `table.column_formatting` only wires into
    # display="table" — scalars/gauges ignore it (corpus/research audit
    # 2026-04-22). Smart-scalar's directional arrow + week-over-week delta
    # is more informationally dense than a static threshold, so we ship that
    # alone for v1.2. A dedicated gauge alongside the trend would require a
    # second tile — skipped for v1.2.
    t1 = trend_smartscalar(
        name="% First Touch in 5 min (weekly)",
        field="pct_within_5min",
        fmt=PCT_FMT,
    )
    t2 = trend_smartscalar(
        name="Median Minutes to First SDR Touch (weekly)",
        field="median_mins",
        fmt=MIN_FMT,
    )
    t3 = trend_smartscalar(
        name="% Reached Within 1 Hr (weekly)",
        field="pct_with_1hr_activity",
        fmt=PCT_FMT,
    )

    # ── Row 2 — T4, T5, T6 volume scorecards ─────────────────────────────
    t4 = scorecard(
        name="Bookings (7d)",
        field="bookings_7d",
        fmt=NUM_FMT,
    )
    t5 = scorecard(
        name="SDR-Attributed (7d)",
        field="sdr_attributed_7d",
        fmt=NUM_FMT,
    )
    t6 = scorecard(
        name="Within 5 min (7d)",
        field="within_5min_7d",
        fmt=NUM_FMT,
    )

    # ── Row 3 — T7 stacked area: daily volume by source ──────────────────
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

    # ── Row 4 — T8 SDR leaderboard ───────────────────────────────────────
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

    # ── Row 4 — T9 lead-tracking match-rate donut ────────────────────────
    # Categories remapped in SQL from engineering flag tokens to
    # business-phrased states (Data Ops corpus audit 2026-04-22):
    #   clean         → Matched
    #   no_sdr_touch  → No SDR touch yet
    #   role_unknown  → Unassigned rep
    # Card title renamed from "Attribution Quality Mix" to match.
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
    header_text = (
        "### Speed-to-Lead — D-DEE\n"
        "**Metric:** % of Calendly-booked calls where the first *human* SDR "
        "touch (CALL or SMS — not automation) lands within 5 minutes.  \n"
        "**Time windows:** **weekly headline metrics** (this week vs. last) · 30-day people & attribution mix · 90-day volume trend."
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
    # shift down by 2 to make room for the banner.
    set_dashboard_cards(
        mb,
        dashboard_id=dash["id"],
        cards=[
            header_dashcard,
            # Row 2: T1 | T2 | T3
            {"card_id": t1["id"], "row": 2,  "col": 0,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t2["id"], "row": 2,  "col": 8,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t3["id"], "row": 2,  "col": 16, "size_x": 8, "size_y": 3, "visualization_settings": {}},
            # Row 5: T4 | T5 | T6
            {"card_id": t4["id"], "row": 5,  "col": 0,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t5["id"], "row": 5,  "col": 8,  "size_x": 8, "size_y": 3, "visualization_settings": {}},
            {"card_id": t6["id"], "row": 5,  "col": 16, "size_x": 8, "size_y": 3, "visualization_settings": t6_click_behavior},
            # Row 8: T7 stacked area (full width)
            {"card_id": t7["id"], "row": 8,  "col": 0,  "size_x": 24, "size_y": 7, "visualization_settings": {}},
            # Row 15: T8 leaderboard | T9 donut
            {"card_id": t8["id"], "row": 15, "col": 0,  "size_x": 16, "size_y": 7, "visualization_settings": {}},
            {"card_id": t9["id"], "row": 15, "col": 16, "size_x": 8,  "size_y": 7, "visualization_settings": {}},
            # Row 22: refresh footer
            {"card_id": footer["id"], "row": 22, "col": 0, "size_x": 12, "size_y": 2, "visualization_settings": {}},
        ],
    )

    print(f"Speed-to-Lead:             {mb.url}/dashboard/{dash['id']}")
    print(f"Speed-to-Lead Lead Detail: {mb.url}/dashboard/{detail_dash['id']}")


if __name__ == "__main__":
    main()
