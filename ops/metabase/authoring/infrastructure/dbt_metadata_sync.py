"""Propagate dbt column-level metadata into Metabase's schema browser.

Reads `dbt/target/manifest.json` and pushes `description:`, `meta:` (semantic
types), and relationship tests from the dbt project into Metabase so that a
stakeholder hovering a column in the Metabase UI sees the same documentation
that lives in the dbt `.yml`. dbt stays authoritative; Metabase displays.

Rule 4 of `.claude/rules/metabase.md` — dbt metadata flows into Metabase via
dbt-metabase (https://github.com/gouline/dbt-metabase). This script is the
canonical post-`dbt deploy` follow-up step; without it, the dbt `.yml`
descriptions are effectively disconnected from the BI surface.

Auth reuses the same pattern as `bigquery_connection.py` — `MetabaseClient`
resolves the API key from GCP Secret Manager at runtime, and that key is
passed through to `dbtmetabase.DbtMetabase` as `metabase_api_key=`.

Default sync scope is every dbt model whose schema starts with `marts` (the
client-facing layer). Pass ``--select`` to narrow, e.g.::

    python -m ops.metabase.authoring.infrastructure.dbt_metadata_sync \\
        --select 'stl_*'

Run::

    source .venv/bin/activate
    set -a && source ops/metabase/.env.metabase && set +a
    cd dbt && dbt parse && cd ..   # if manifest.json is stale
    python -m ops.metabase.authoring.infrastructure.dbt_metadata_sync

Re-running is a no-op for unchanged descriptions — `dbt-metabase` compares
each field's current Metabase state to the dbt-derived desired state and
only PATCHes when they diverge.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ..client import MetabaseClient

# `dee-data-ops-prod.marts.*` is the client-facing layer per
# `.claude/rules/mart-naming.md`. Matches schema names beginning with "marts"
# (e.g. `marts`, `marts_speed_to_lead` if we ever split by audience).
DB_NAME_IN_METABASE = "dee-data-ops-prod"
DEFAULT_SCHEMA_INCLUDE = ["marts*"]

# Repo root = parents[4] from this file:
#   .../ops/metabase/authoring/infrastructure/dbt_metadata_sync.py
#    0                                         1
#    parents[0] = infrastructure/
#    parents[1] = authoring/
#    parents[2] = metabase/
#    parents[3] = ops/
#    parents[4] = <repo root>
REPO_ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = REPO_ROOT / "dbt" / "target" / "manifest.json"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync dbt column descriptions / semantic types into Metabase.",
    )
    p.add_argument(
        "--select",
        action="append",
        default=None,
        help=(
            "Optional model-name glob(s) to narrow the sync (fnmatch, "
            "case-insensitive). E.g. --select 'stl_*'. May be repeated. "
            "If omitted, all models in `marts*` schemas are synced."
        ),
    )
    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG logging from dbt-metabase (per-field change log).",
    )
    return p.parse_args()


def _require_dbt_metabase():
    """Import dbt-metabase or print install hint and exit.

    We don't auto-install — that would mutate the venv from a code path that
    looks read-only to the caller. Requiring an explicit `pip install` keeps
    the dependency surface visible in `requirements.txt`.
    """
    try:
        from dbtmetabase import DbtMetabase, Filter  # noqa: WPS433
    except ImportError:
        print(
            "ERROR: dbt-metabase is not installed in this environment.\n"
            "Install with:\n"
            "    pip install dbt-metabase\n"
            "Then pin the version in requirements.txt to match.",
            file=sys.stderr,
        )
        sys.exit(1)
    return DbtMetabase, Filter


def _require_manifest() -> Path:
    """Ensure the dbt manifest exists on disk.

    `manifest.json` is produced by either `dbt parse` (fast, no warehouse
    hit) or `dbt compile` / `dbt build` (produces it as a side-effect). If
    it's missing the user probably hasn't run dbt in this worktree yet.
    """
    if not MANIFEST_PATH.exists():
        print(
            f"ERROR: dbt manifest not found at {MANIFEST_PATH}.\n"
            "Run one of the following first:\n"
            "    cd dbt && dbt parse\n"
            "    cd dbt && dbt compile\n"
            "Either command produces target/manifest.json.",
            file=sys.stderr,
        )
        sys.exit(1)
    return MANIFEST_PATH


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("dbt_metadata_sync")

    DbtMetabase, Filter = _require_dbt_metabase()
    manifest_path = _require_manifest()

    # Reuse the existing auth wrapper: resolves MB_URL from env and the API
    # key from GCP Secret Manager (or MB_SESSION env in CI). We only need
    # the resolved URL + key to hand off to dbt-metabase's own HTTP client.
    mb = MetabaseClient()
    log.info("Metabase URL: %s", mb.url)
    log.info("Manifest:     %s", manifest_path)

    dbt_mb = DbtMetabase(
        manifest_path=str(manifest_path),
        metabase_url=mb.url,
        metabase_api_key=mb.session,
    )

    # Guard: fail loudly if the target database isn't connected yet. dbt-
    # metabase would raise MetabaseStateError deeper in export_models, but
    # catching here gives a clearer remediation message aligned with the
    # sibling bigquery_connection.py bootstrap note.
    if not dbt_mb.metabase.find_database(name=DB_NAME_IN_METABASE):
        print(
            f"ERROR: no Metabase database named {DB_NAME_IN_METABASE!r}. "
            "The BQ connection must be bootstrapped once via the Metabase "
            "GUI (Admin -> Databases) before metadata can be synced. See "
            "ops/metabase/authoring/infrastructure/bigquery_connection.py "
            "for the flag-level follow-up.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Narrow to the marts layer by default. `--select` tightens further.
    schema_filter = Filter(include=list(DEFAULT_SCHEMA_INCLUDE))
    model_filter = Filter(include=list(args.select)) if args.select else None

    selected_models = [
        m
        for m in dbt_mb.manifest.read_models()
        if schema_filter.match(m.schema)
        and (model_filter is None or model_filter.match(m.name))
    ]
    log.info(
        "Selected %d dbt model(s) for sync (schema=%s, select=%s)",
        len(selected_models),
        DEFAULT_SCHEMA_INCLUDE,
        args.select or "<all>",
    )
    if not selected_models:
        print(
            "WARNING: no dbt models matched the filter. Nothing to sync.",
            file=sys.stderr,
        )
        return

    # Let dbt-metabase drive the diff + PATCH cycle. It is idempotent: fields
    # already matching desired state are skipped. Updates are logged at INFO
    # (table-level summary) and DEBUG (per-field payload).
    try:
        dbt_mb.export_models(
            metabase_database=DB_NAME_IN_METABASE,
            schema_filter=schema_filter,
            model_filter=model_filter,
            # Don't touch dbt sources — they're the raw landing zone, not
            # something stakeholders browse in Metabase.
            skip_sources=True,
        )
    except Exception as exc:  # noqa: BLE001 — re-raise after logging context
        log.error(
            "dbt-metabase export_models failed (database=%s): %s",
            DB_NAME_IN_METABASE,
            exc,
        )
        raise

    # Summary. dbt-metabase doesn't return a structured result from
    # export_models (it just raises on failure), so we report counts from
    # the pre-filter selection and point at the INFO log for per-model
    # detail. Models the tool couldn't map surface as WARNING log lines
    # ("Table 'SCHEMA.MODEL' not in schema ...") emitted during sync.
    print(
        f"\nSync complete against {DB_NAME_IN_METABASE}.\n"
        f"  models considered: {len(selected_models)}\n"
        f"  schema filter:     include={DEFAULT_SCHEMA_INCLUDE}\n"
        f"  model filter:      {args.select or '<all>'}\n"
        "Per-field change detail in the INFO log above. Any "
        "'Table ... not in schema' or 'Field ... does not exist' warnings "
        "indicate dbt models that haven't materialized in Metabase's copy "
        "of the schema yet (run Metabase's Sync Schema or wait for the "
        "next scheduled sync)."
    )


if __name__ == "__main__":
    main()
