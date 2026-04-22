"""Assert desired state on Metabase query-result caching.

Two layers of control:

1. Server-wide feature toggle — ``enable-query-caching``.

   IMPORTANT (v0.60.1): This setting is READ-ONLY via the REST API. The
   Metabase server returns HTTP 500 with
   ``"You cannot set enable-query-caching; it is a read-only setting."``
   when PUT /api/setting/enable-query-caching is called.

   To enable query caching on OSS v0.60.1, set the environment variable
   ``MB_ENABLE_QUERY_CACHING=true`` on the Metabase server **before
   starting the container**. On this deployment, that means:

     a. SSH into the GCE VM running Metabase.
     b. Edit ``ops/metabase/runtime/docker-compose.yml`` to add::

            environment:
              MB_ENABLE_QUERY_CACHING: "true"

     c. Restart the container: ``docker compose down && docker compose up -d``
     d. Verify: GET /api/setting → ``enable-query-caching.value`` becomes
        ``true``.
     e. Re-run this script: it will skip the PUT and confirm the toggle is ON.

   This script reads the current value and WARNS (does not error) if it is
   OFF, because the fix requires a VM-level change outside script scope.

   Corpus: *"Working with the Metabase API"* (Metabase Learn notebook,
   source 4e547d7e) — /api/setting for reading global settings.
   API behaviour confirmed live: v0.60.1 returns 500 on PUT with message
   "read-only setting" — MB_ENABLE_QUERY_CACHING env var is the correct
   lever.

2. Per-dashboard TTL override on ``Speed-to-Lead``.
   - cache_ttl: 21600 seconds (6 hours).
   - Rationale: the 11 stl_* rollups all refresh on dbt prod daily; within
     a 6-hour window the public-share dashboard should serve from cache,
     not re-scan BigQuery on every anonymous page load.

   OSS LIMITATION: Per *"Caching query results"* (Metabase Learn notebook,
   source d6a8e3ae), per-dashboard cache_ttl overrides are a Pro/Enterprise
   feature. On OSS v0.60.1 the PUT may succeed but the value may not
   persist. The script warns rather than hard-fails — server-wide caching
   (once the env var is set) is the primary deliverable.

   The Metabase cache sits in front of BigQuery's own 24-hour result
   cache. If Metabase misses (TTL expired), BQ may still serve free IF
   ``include-user-id-and-hash`` is OFF — which bigquery_connection.py
   enforces. Run that script first to verify.

Corpus citations:
- *"Caching query results"* (Metabase Learn notebook, source d6a8e3ae) —
  caching configured via Admin Panel → Performance tab; per-dashboard TTL
  is Pro/Enterprise on newer builds.
- *"Working with the Metabase API"* (Metabase Learn notebook, source
  4e547d7e) — /api/setting endpoint for reading global settings.
- *"Google BigQuery | Metabase Documentation"* (Metabase Craft notebook)
  — "Include User ID and query hash in queries … prevents BigQuery from
  caching results and may increase your costs."

Implementation notes (live API behaviour observed 2026-04-22, v0.60.1):
- ``GET /api/setting/<key>`` returns empty body (not JSON) for many
  settings on this instance. Read from ``GET /api/setting`` (list).
- ``enable-query-caching`` is a read-only setting on v0.60.1; PUT returns
  HTTP 500 with "You cannot set enable-query-caching; it is a read-only
  setting." Enable via MB_ENABLE_QUERY_CACHING=true env var on the server.
- ``query-caching-min-ttl`` and ``query-caching-ttl-ratio`` are absent
  from the settings list entirely — code-level defaults (60 s, 10×).
- The primary Speed-to-Lead dashboard (collection 6, public_uuid set) is
  id=3. A second copy (collection 5, id=2) has no public UUID.

Run::

    source .venv/bin/activate
    set -a && source ops/metabase/.env.metabase && set +a
    python -m ops.metabase.authoring.infrastructure.caching_config

Re-running is a no-op when state already matches. If enable-query-caching
is still OFF, the script warns but does not exit non-zero — fixing that
requires the MB_ENABLE_QUERY_CACHING env var change on the server.
"""
from __future__ import annotations

import sys

from ..client import MetabaseClient

DASHBOARD_NAME = "Speed-to-Lead"
DASHBOARD_CACHE_TTL_SEC = 6 * 60 * 60  # 6 hours = 21600 seconds

# The read-only server setting we CHECK (cannot SET via API on v0.60.1).
CACHING_TOGGLE_KEY = "enable-query-caching"
CACHING_TOGGLE_DESIRED = True


def _get_setting_value(all_settings: list[dict], key: str):
    """Read a setting's value from the full /api/setting list.

    Necessary because GET /api/setting/<key> returns empty body (not JSON)
    on this Metabase OSS instance — raising JSONDecodeError if parsed.
    """
    entry = next((s for s in all_settings if s.get("key") == key), None)
    if entry is None:
        return None
    return entry.get("value")


def _find_dashboard(mb: MetabaseClient, name: str) -> dict:
    """Return the dashboard dict for the given name.

    If multiple dashboards share the same name, prefer the one with a
    public_uuid set (i.e. the live public-share target).
    """
    matches = [d for d in mb.dashboards() if d.get("name") == name]
    if not matches:
        raise LookupError(
            f"No Metabase dashboard named {name!r}. Ship the authoring "
            "script for it first (ops/metabase/authoring/dashboards/speed_to_lead.py)."
        )
    # Prefer the publicly-shared copy if there are duplicates.
    public = [d for d in matches if d.get("public_uuid")]
    best = public[0] if public else matches[0]
    return mb.get(f"/dashboard/{best['id']}")


def main() -> None:
    mb = MetabaseClient()

    # ── 1. Server-wide caching toggle (READ ONLY on v0.60.1) ────────────
    #
    # Read from the full settings list (GET /setting) because individual-key
    # GET returns empty body on this build.
    all_settings = mb.get("/setting")
    caching_on = _get_setting_value(all_settings, CACHING_TOGGLE_KEY)
    print("Server-wide caching settings:")
    print(f"  {CACHING_TOGGLE_KEY}: have={caching_on!r} want={CACHING_TOGGLE_DESIRED!r}", end=" ")

    if caching_on == CACHING_TOGGLE_DESIRED:
        print("-- ok")
    else:
        print("-- READ-ONLY (cannot PUT via API on v0.60.1)")
        print(
            "\n  ACTION REQUIRED: Set MB_ENABLE_QUERY_CACHING=true in "
            "ops/metabase/runtime/docker-compose.yml, restart the container,\n"
            "  then re-run this script. See this file's docstring for exact steps."
        )
        # Continue to set the per-dashboard TTL even if the toggle is off,
        # so that state is ready when the env var is applied.

    # ── 2. Per-dashboard cache_ttl override ────────────────────────────
    #
    # NOTE: Per *"Caching query results"* (Metabase Learn, source d6a8e3ae),
    # per-dashboard cache_ttl overrides are a Pro/Enterprise feature. On
    # OSS v0.60.1 the PUT may succeed but the value may not persist.
    dash = _find_dashboard(mb, DASHBOARD_NAME)
    before_ttl = dash.get("cache_ttl")
    print(f"\nDashboard {DASHBOARD_NAME!r} (id={dash['id']}):")
    print(f"  cache_ttl before: {before_ttl!r}  want: {DASHBOARD_CACHE_TTL_SEC!r}")
    if before_ttl != DASHBOARD_CACHE_TTL_SEC:
        # Minimal-diff PUT — only fields we're changing plus identity.
        # Mirrors bigquery_connection.py's discipline: don't round-trip
        # fields whose read-form differs from write-form.
        mb.put(
            f"/dashboard/{dash['id']}",
            {
                "name": dash["name"],
                "collection_id": dash.get("collection_id"),
                "cache_ttl": DASHBOARD_CACHE_TTL_SEC,
            },
        )
        print("  PUT fired.")
    else:
        print("  ok (no PUT).")

    # ── 3. Verify ──────────────────────────────────────────────────────
    after = mb.get(f"/dashboard/{dash['id']}")
    after_ttl = after.get("cache_ttl")
    print(f"  cache_ttl after:  {after_ttl!r}")
    if after_ttl != DASHBOARD_CACHE_TTL_SEC:
        print(
            "  NOTE: cache_ttl did not persist — likely an OSS limitation "
            "(per-dashboard TTL is Pro/Enterprise per Metabase Learn).\n"
            "  Server-wide caching (once MB_ENABLE_QUERY_CACHING env var is set) "
            "will still cache using the TTL-ratio × query duration formula."
        )

    # ── 4. Summary ─────────────────────────────────────────────────────
    print()
    if caching_on == CACHING_TOGGLE_DESIRED:
        if after_ttl == DASHBOARD_CACHE_TTL_SEC:
            print("State matches desired.")
        else:
            print(
                "Partial: enable-query-caching=ON but per-dashboard TTL not persisted "
                "(OSS limitation). Cache active with server-wide TTL-ratio formula."
            )
    else:
        print(
            "ACTION NEEDED: enable-query-caching is still OFF.\n"
            "Set MB_ENABLE_QUERY_CACHING=true on the server and restart Metabase."
        )


if __name__ == "__main__":
    main()
