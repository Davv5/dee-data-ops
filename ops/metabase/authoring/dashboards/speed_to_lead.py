"""Speed-to-Lead dashboard — Page 1 of the D-DEE dashboard stack.

Mart: dee-data-ops-prod.marts.sales_activity_detail
Grain: one row per Calendly booking event (~3,141 rows).

Run::

    source .venv/bin/activate
    set -a && source .env.metabase && set +a
    python -m ops.metabase.authoring.dashboards.speed_to_lead

Re-running is a no-op except for queries/names that actually changed — every
upsert is keyed on a stable entity_id.

STATUS: scaffold only. Phase 3 of the metabase self-host plan fills in the
actual cards (headline SLA scorecard, median time by SDR, daily volume,
SDR leaderboard, DQ diagnostic, attribution quality, lead drill-down).
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

MART = "sales_activity_detail"
DATABASE_NAME = "dee-data-ops-prod"


def main() -> None:
    mb = MetabaseClient()
    db_id = find_database_id(mb, DATABASE_NAME)

    coll = upsert_collection(
        mb,
        name="Speed-to-Lead",
        key="speed_to_lead",
        color="#509EE3",
    )

    # ── Card 1: Headline SLA % (last 7d) ─────────────────────────────────
    headline = upsert_card(
        mb,
        name="Speed-to-Lead SLA — last 7 days",
        key="speed_to_lead__headline_7d",
        collection_id=coll["id"],
        database_id=db_id,
        display="scalar",
        native_query="""
            select
              safe_divide(
                countif(is_within_5_min_sla),
                count(*)
              ) as sla_pct
            from `{{db}}.marts.sales_activity_detail`
            where date_trunc(booked_at, week) = date_trunc(current_date(), week) - 7
        """.strip(),
        visualization_settings={"scalar.field": "sla_pct"},
    )

    # Phase 3 adds: median-by-SDR, daily volume, DQ diagnostic, SDR
    # leaderboard, attribution-quality donut, lead-level drill-down.

    # ── Dashboard wire-up ────────────────────────────────────────────────
    dash = upsert_dashboard(
        mb,
        name="Speed-to-Lead",
        key="dashboard__speed_to_lead",
        collection_id=coll["id"],
        description=(
            "Page 1 of the D-DEE dashboard stack. Grain: one row per Calendly "
            "booking event. Headline metric: % of bookings where the first "
            "outbound human SDR touch lands within 5 minutes."
        ),
    )

    set_dashboard_cards(
        mb,
        dashboard_id=dash["id"],
        cards=[
            {
                "card_id": headline["id"],
                "row": 0, "col": 0,
                "size_x": 12, "size_y": 4,
                "visualization_settings": {},
            },
        ],
    )

    print(f"Speed-to-Lead dashboard upserted: {mb.url}/dashboard/{dash['id']}")


if __name__ == "__main__":
    main()
