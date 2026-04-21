"""Declare the BigQuery data source as code.

First authoring script to run after the Metabase instance is up. Once this
creates the connection, every dashboard script can reference it via
`find_database_id(mb, "dee-data-ops-prod")`.

Reads the BQ SA key from Secret Manager at run time — the key is never on
disk, never committed.

Run::

    source .venv/bin/activate
    set -a && source .env.metabase && set +a
    python -m ops.metabase.authoring.infrastructure.bigquery_connection
"""
from __future__ import annotations

import json
import os

from google.cloud import secretmanager

from ..client import MetabaseClient

PROJECT_ID = "dee-data-ops-prod"
DATASET = "marts"
SECRET_NAME = "metabase-bq-reader-key"
DB_NAME_IN_METABASE = "dee-data-ops-prod"


def _fetch_bq_sa_key() -> dict:
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(
        name=f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    )
    return json.loads(resp.payload.data.decode())


def main() -> None:
    mb = MetabaseClient()
    key = _fetch_bq_sa_key()

    existing = next(
        (d for d in mb.databases() if d["name"] == DB_NAME_IN_METABASE),
        None,
    )
    payload = {
        "engine": "bigquery-cloud-sdk",
        "name": DB_NAME_IN_METABASE,
        "details": {
            "project-id": PROJECT_ID,
            "service-account-json": json.dumps(key),
            "dataset-filters-type": "inclusion",
            "dataset-filters-patterns": DATASET,
            "include-user-id-and-hash": False,  # preserves BQ result cache
        },
        "is_on_demand": False,
        "is_full_sync": True,
    }
    if existing:
        mb.put(f"/database/{existing['id']}", payload)
        print(f"Updated BQ connection (id={existing['id']})")
    else:
        created = mb.post("/database", payload)
        print(f"Created BQ connection (id={created['id']})")


if __name__ == "__main__":
    main()
