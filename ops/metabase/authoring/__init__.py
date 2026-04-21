"""Metabase dashboards-as-code.

Usage from an authoring script::

    from ops.metabase.authoring.client import MetabaseClient
    from ops.metabase.authoring.sync import (
        upsert_collection, upsert_card, upsert_dashboard,
        set_dashboard_cards, find_database_id,
    )

    mb = MetabaseClient()  # reads MB_URL + MB_SESSION from env
    db_id = find_database_id(mb, "dee-data-ops-prod")
    coll = upsert_collection(mb, name="Speed-to-Lead", key="speed_to_lead")
    ...
"""
