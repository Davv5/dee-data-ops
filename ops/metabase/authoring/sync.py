"""Idempotent upsert helpers keyed on `entity_id`.

Every authoring script goes through these functions instead of raw POSTs so
that re-running a script against an existing Metabase instance is a no-op.

Metabase's `entity_id` is a stable 21-char string assigned to every
collection / card / dashboard when it's created. We assign our own
deterministic entity_ids (short hashes of a stable logical name) so the
script can look up + update on subsequent runs.
"""
from __future__ import annotations

import hashlib
from typing import Any

from .client import MetabaseClient


def _eid(key: str) -> str:
    """Deterministic 21-char entity_id from a logical key."""
    return hashlib.sha256(key.encode()).hexdigest()[:21]


def upsert_collection(
    mb: MetabaseClient,
    *,
    name: str,
    key: str,
    parent_id: int | None = None,
    color: str = "#509EE3",
) -> dict:
    """Create or update a collection identified by `key`.

    Returns the collection dict with `id` populated.
    """
    eid = _eid(f"collection::{key}")
    existing = next(
        (c for c in mb.collections() if c.get("entity_id") == eid),
        None,
    )
    payload = {
        "name": name,
        "color": color,
        "parent_id": parent_id,
    }
    if existing:
        return mb.put(f"/collection/{existing['id']}", payload)
    payload["entity_id"] = eid
    return mb.post("/collection", payload)


def upsert_card(
    mb: MetabaseClient,
    *,
    name: str,
    key: str,
    collection_id: int,
    database_id: int,
    native_query: str,
    display: str = "table",
    visualization_settings: dict | None = None,
) -> dict:
    """Create or update a native-SQL question.

    `native_query` is the SQL string (BigQuery dialect for this project).
    Display options: table, bar, line, scalar, pie, row, funnel, …
    """
    eid = _eid(f"card::{key}")
    existing = next(
        (c for c in mb.cards() if c.get("entity_id") == eid),
        None,
    )
    dataset_query = {
        "type": "native",
        "database": database_id,
        "native": {"query": native_query},
    }
    payload: dict[str, Any] = {
        "name": name,
        "display": display,
        "dataset_query": dataset_query,
        "visualization_settings": visualization_settings or {},
        "collection_id": collection_id,
    }
    if existing:
        return mb.put(f"/card/{existing['id']}", payload)
    payload["entity_id"] = eid
    return mb.post("/card", payload)


def upsert_dashboard(
    mb: MetabaseClient,
    *,
    name: str,
    key: str,
    collection_id: int,
    description: str = "",
) -> dict:
    eid = _eid(f"dashboard::{key}")
    existing = next(
        (d for d in mb.dashboards() if d.get("entity_id") == eid),
        None,
    )
    payload = {
        "name": name,
        "description": description,
        "collection_id": collection_id,
    }
    if existing:
        return mb.put(f"/dashboard/{existing['id']}", payload)
    payload["entity_id"] = eid
    return mb.post("/dashboard", payload)


def set_dashboard_cards(
    mb: MetabaseClient,
    *,
    dashboard_id: int,
    cards: list[dict],
) -> None:
    """Replace the dashboard's card layout with the provided list.

    Each entry in `cards` should be:
        {"card_id": int, "size_x": int, "size_y": int, "row": int, "col": int,
         "visualization_settings": {...}}

    Grid is 24 columns wide; `size_x` is in grid columns.
    """
    payload = {"cards": cards}
    mb.put(f"/dashboard/{dashboard_id}/cards", payload)


def find_database_id(mb: MetabaseClient, name: str) -> int:
    for db in mb.databases():
        if db["name"] == name:
            return db["id"]
    raise LookupError(f"No Metabase database named {name!r} — connect it first")
