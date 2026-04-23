"""Idempotent upsert helpers for Metabase authoring scripts.

Each helper matches existing entities by `(name, collection_id)` — Metabase
overrides any client-supplied `entity_id` on create, so name-within-scope is
the stable identity. Collection names are unique within a parent, card/
dashboard names are unique within a collection.

Running an authoring script against an instance that already has its
entities produces PUTs instead of POSTs: same side-effect, no duplicates.
"""
from __future__ import annotations

from typing import Any

from .client import MetabaseClient


def upsert_collection(
    mb: MetabaseClient,
    *,
    name: str,
    parent_id: int | None = None,
    color: str = "#509EE3",
) -> dict:
    existing = next(
        (c for c in mb.collections() if c.get("name") == name and c.get("parent_id") == parent_id),
        None,
    )
    payload = {"name": name, "color": color, "parent_id": parent_id}
    if existing:
        return mb.put(f"/collection/{existing['id']}", payload)
    return mb.post("/collection", payload)


def upsert_card(
    mb: MetabaseClient,
    *,
    name: str,
    collection_id: int,
    database_id: int,
    native_query: str,
    display: str = "table",
    visualization_settings: dict | None = None,
    template_tags: dict | None = None,
    cache_ttl: int | None = 0,
) -> dict:
    """Upsert a native-query card.

    ``cache_ttl`` defaults to ``0`` (live-by-default: no per-question cache).
    Set it to a positive integer (seconds) to cache heavily-aggregated tiles
    that legitimately refresh on daily cadence. Set to ``None`` to inherit the
    server-wide default.

    See ``.claude/rules/live-by-default.md`` for the full live-by-default
    policy and the "When to deviate" section for valid override cases.

    Corpus: *"Caching query results"* (Metabase Learn notebook, source
    d6a8e3ae) — ``cache_ttl=0`` = explicit bypass; ``cache_ttl=null`` =
    inherit server default.
    """
    existing = next(
        (c for c in mb.cards() if c.get("name") == name and c.get("collection_id") == collection_id),
        None,
    )
    native: dict[str, Any] = {"query": native_query}
    if template_tags:
        # Metabase's native API expects hyphenated `template-tags` (not `template_tags`).
        native["template-tags"] = template_tags
    dataset_query = {
        "type": "native",
        "database": database_id,
        "native": native,
    }
    payload: dict[str, Any] = {
        "name": name,
        "display": display,
        "dataset_query": dataset_query,
        "visualization_settings": visualization_settings or {},
        "collection_id": collection_id,
    }
    # Metabase OSS v0.60.1 rejects cache_ttl=0 with HTTP 400
    # ("value must be an integer greater than zero"). Server-side contract
    # is null-or-positive-int, so serialize 0 as omission (= null = server
    # default, which == live when MB_ENABLE_QUERY_CACHING is false).
    if cache_ttl is not None and cache_ttl > 0:
        payload["cache_ttl"] = cache_ttl
    if existing:
        return mb.put(f"/card/{existing['id']}", payload)
    return mb.post("/card", payload)


def upsert_dashboard(
    mb: MetabaseClient,
    *,
    name: str,
    collection_id: int,
    description: str = "",
    parameters: list[dict] | None = None,
    cache_ttl: int | None = 0,
) -> dict:
    """Upsert a dashboard.

    ``cache_ttl`` defaults to ``0`` (live-by-default: no dashboard-level cache).
    Set to a positive integer (seconds) for daily-cadence dashboards that
    legitimately benefit from caching. Set to ``None`` to inherit the
    server-wide default.

    Note: dashboard-level auto-refresh (60s tick) is NOT configurable via the
    REST API on Metabase OSS v0.60.1. Auto-refresh is a frontend-only feature
    activated by the URL fragment ``#refresh=60``. Add this fragment to the
    public-share link or iframe src when setting up the dashboard.
    See ``docs/runbooks/metabase-live-dashboard-setup.md`` step 4.

    Corpus: *"Caching query results"* (Metabase Learn notebook, source
    d6a8e3ae) — cache_ttl=0 = explicit bypass; cache_ttl=null = server default.
    Corpus: *"Dashboards"* overview (Metabase Learn notebook, source
    04cf5679) — auto-refresh is a frontend/URL-fragment feature, not an
    API-stored property.
    """
    existing = next(
        (d for d in mb.dashboards() if d.get("name") == name and d.get("collection_id") == collection_id),
        None,
    )
    payload: dict[str, Any] = {
        "name": name,
        "description": description,
        "collection_id": collection_id,
    }
    # See upsert_card for rationale — cache_ttl=0 rejected by Metabase OSS
    # v0.60.1; serialize as omission (server default = live when caching off).
    if cache_ttl is not None and cache_ttl > 0:
        payload["cache_ttl"] = cache_ttl
    if parameters is not None:
        # Dashboard-level parameters (Metabase filter widgets). These are
        # wired to card template-tags via each dashcard's `parameter_mappings`.
        payload["parameters"] = parameters
    if existing:
        return mb.put(f"/dashboard/{existing['id']}", payload)
    return mb.post("/dashboard", payload)


def set_dashboard_cards(
    mb: MetabaseClient,
    *,
    dashboard_id: int,
    cards: list[dict],
) -> None:
    """Replace the dashboard's card layout.

    Each entry in `cards`: `{card_id, size_x, size_y, row, col, visualization_settings}`.
    Grid is 24 columns. Reuses existing dashcard ids (matched by card_id) so
    re-runs update positions in-place instead of accumulating duplicates.
    """
    current = mb.get(f"/dashboard/{dashboard_id}")
    existing_by_card = {
        dc["card_id"]: dc["id"]
        for dc in current.get("dashcards", [])
        if dc.get("card_id") is not None
    }
    dashcards = [
        {
            "id": existing_by_card.get(c["card_id"], -(i + 1)),
            "card_id": c["card_id"],
            "row": c["row"],
            "col": c["col"],
            "size_x": c["size_x"],
            "size_y": c["size_y"],
            "visualization_settings": c.get("visualization_settings", {}),
            "parameter_mappings": c.get("parameter_mappings", []),
        }
        for i, c in enumerate(cards)
    ]
    mb.put(f"/dashboard/{dashboard_id}", {"dashcards": dashcards})


def find_database_id(mb: MetabaseClient, name: str) -> int:
    if getattr(mb, "dry_run", False):
        # In dry-run there's no real Metabase to query. Return a stub id so the
        # downstream card queries can reference it in their dataset_query; the
        # actual integer doesn't matter because every POST/PUT is intercepted.
        return -9999
    for db in mb.databases():
        if db["name"] == name:
            return db["id"]
    raise LookupError(f"No Metabase database named {name!r} — connect it first")
