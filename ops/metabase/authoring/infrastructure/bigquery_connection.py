"""Assert desired state on the BigQuery data source connection.

The BQ connection (`dee-data-ops-prod`, engine `bigquery-cloud-sdk`) is
bootstrapped *once* through the Metabase GUI — the service-account JSON is
injected by hand from Secret Manager that one time. We don't re-create it
from code because Metabase redacts `details["service-account-json"]` to the
sentinel string `**MetabasePass**` on every GET, so round-tripping the
details blob would clobber the real credential with the sentinel.

This script idempotently asserts the two cost-sensitive flags the GUI
bootstrap doesn't cover:

- ``auto_run_queries = False`` (top-level): when ``True``, Metabase re-hits
  BigQuery on every filter-picker click during interactive exploration —
  runaway BQ bytes-scanned bill.
- ``details["include-user-id-and-hash"] = False``: when ``True``, Metabase
  appends a per-user comment to every BQ query, which defeats BigQuery's
  24-hour result cache (every user sees a cache miss).

Both recommendations are cited in the Metabase Craft NotebookLM corpus,
source: *"Google BigQuery | Metabase Documentation"*.

Run::

    source .venv/bin/activate
    set -a && source ops/metabase/.env.metabase && set +a
    python -m ops.metabase.authoring.infrastructure.bigquery_connection

Re-running is a safe no-op when the state already matches. The script
*refuses* to PUT a changed ``details`` blob — if ``include-user-id-and-hash``
drifts, it prints a remediation note and exits non-zero rather than silently
overwriting the redacted service-account JSON.
"""
from __future__ import annotations

import sys

from ..client import MetabaseClient

DB_NAME_IN_METABASE = "dee-data-ops-prod"

# Desired state — both flags must be False to preserve BQ result caching
# and avoid re-hitting BQ on every dashboard interaction.
DESIRED_AUTO_RUN_QUERIES = False
DESIRED_INCLUDE_USER_ID_AND_HASH = False


def _find_db(mb: MetabaseClient, name: str) -> dict:
    """Look up the BQ database by name. Fail loudly if missing.

    Unlike dashboards, we do NOT create the DB connection from code — the
    service-account JSON has to be injected once via the GUI (or a bespoke
    Secret-Manager-aware bootstrap) and Metabase redacts it on subsequent
    reads. If the connection is gone, a human needs to re-bootstrap it.
    """
    for db in mb.databases():
        if db["name"] == name:
            # /database list endpoint returns a thin row. Fetch the full
            # record so `details` is populated.
            return mb.get(f"/database/{db['id']}")
    raise LookupError(
        f"No Metabase database named {name!r}. The BQ connection must be "
        "bootstrapped once via the Metabase GUI (Admin → Databases) with the "
        "service-account JSON pulled from Secret Manager "
        "(secret: metabase-bq-reader-key). This script only asserts flags; "
        "it does not create the connection."
    )


def main() -> None:
    mb = MetabaseClient()
    db = _find_db(mb, DB_NAME_IN_METABASE)
    db_id = db["id"]

    before_auto_run = db.get("auto_run_queries")
    before_include_hash = (db.get("details") or {}).get("include-user-id-and-hash")

    print(f"BigQuery connection: {DB_NAME_IN_METABASE} (id={db_id})")
    print("Before:")
    print(f"  auto_run_queries             = {before_auto_run!r}")
    print(f"  details.include-user-id-and-hash = {before_include_hash!r}")

    # ── Guard: details-level drift ──────────────────────────────────────
    # Metabase redacts `details["service-account-json"]` to the sentinel
    # string "**MetabasePass**" on GET. If we PUT the details blob back as-is,
    # the real SA credential is overwritten with the sentinel and the
    # connection breaks on the next query.
    #
    # So for the details-nested `include-user-id-and-hash` flag we refuse to
    # PUT unless the user does a safe update path that re-injects the real
    # SA JSON from Secret Manager. In the common case (already False), this
    # branch is a no-op.
    if before_include_hash != DESIRED_INCLUDE_USER_ID_AND_HASH:
        print(
            f"\nERROR: details.include-user-id-and-hash is {before_include_hash!r}, "
            f"want {DESIRED_INCLUDE_USER_ID_AND_HASH!r}.\n"
            "Not PUTting — Metabase redacts details['service-account-json'] on GET, "
            "so round-tripping the details blob would clobber the real SA credential.\n"
            "Remediation: fetch the SA JSON from Secret Manager "
            "(projects/dee-data-ops-prod/secrets/metabase-bq-reader-key) and PUT a "
            "details blob that includes both the real service-account-json AND the "
            "corrected include-user-id-and-hash value. Or toggle the flag once via "
            "the Metabase GUI (Admin → Databases → dee-data-ops-prod → Show advanced "
            "options → 'Include User ID and query hash in queries' = off)."
        )
        sys.exit(1)

    # ── Top-level flag: safe to PUT the minimal diff ────────────────────
    # Only the top-level `auto_run_queries` needs changing here. We PUT a
    # minimal payload of {engine, name, auto_run_queries} — critically, we
    # never include `details`, so the redacted sentinel never reaches the
    # server and the SA credential is untouched.
    put_fired = False
    if before_auto_run != DESIRED_AUTO_RUN_QUERIES:
        mb.put(
            f"/database/{db_id}",
            {
                "engine": db["engine"],
                "name": db["name"],
                "auto_run_queries": DESIRED_AUTO_RUN_QUERIES,
            },
        )
        put_fired = True

    # ── Verify final state ──────────────────────────────────────────────
    after = mb.get(f"/database/{db_id}")
    after_auto_run = after.get("auto_run_queries")
    after_include_hash = (after.get("details") or {}).get("include-user-id-and-hash")

    print("\nAfter:")
    print(f"  auto_run_queries             = {after_auto_run!r}")
    print(f"  details.include-user-id-and-hash = {after_include_hash!r}")
    print(f"\nPUT fired: {put_fired}")
    print(
        "State matches desired"
        if (
            after_auto_run == DESIRED_AUTO_RUN_QUERIES
            and after_include_hash == DESIRED_INCLUDE_USER_ID_AND_HASH
        )
        else "WARNING: final state does not match desired"
    )


if __name__ == "__main__":
    main()
