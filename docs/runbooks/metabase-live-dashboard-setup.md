# Runbook: Building a new live-by-default Metabase dashboard

How to ship a new dashboard that inherits the live-by-default policy:
~60s end-to-end freshness from raw ingest to dashboard render.

See `.claude/rules/live-by-default.md` for the full policy and when to deviate.

---

## Pre-requisites

- The underlying mart is built and deployed to `dee-data-ops-prod.marts.*`.
- `ops/metabase/.env.metabase` is configured with `MB_URL` and API key.
- The virtual environment is active: `source .venv/bin/activate`.

---

## Checklist

### 1. Confirm the mart is under `marts/rollups/**` (or equivalent live path)

Live-by-default policy requires the mart to be rebuilt by the 2-minute
Cloud Run Job, not only by the nightly GHA workflow. Check `dbt_project.yml`
for `materialized: incremental` under the model's path.

If the mart is nightly-only, set `cache_ttl=21600` instead of `0` in the
authoring script (see step 3) — it does not qualify for the live default.

### 2. Create the authoring script

Add a new file:

```
ops/metabase/authoring/dashboards/<mart_name>.py
```

Copy the structure of `speed_to_lead.py`. Key conventions:
- Import `upsert_card`, `upsert_dashboard`, `set_dashboard_cards` from `..sync`.
- Use `MetabaseClient()` (resolves `MB_URL` + API key from env).
- Match one collection per mart, named after the dashboard's business topic.

### 3. `upsert_card` inherits `cache_ttl=0` automatically

The `upsert_card` helper defaults `cache_ttl=0` — live-by-default, no
per-question cache. Do NOT override unless the "When to deviate" section of
`.claude/rules/live-by-default.md` applies.

For heavy-aggregation tiles that legitimately refresh daily:

```python
upsert_card(..., cache_ttl=3600)  # explicit override — document why in comments
```

### 4. Dashboard auto-refresh is URL-fragment only on OSS

`upsert_dashboard` does NOT accept a `refresh_period` parameter — this key
does not exist on the Metabase OSS REST API. Auto-refresh is a frontend
feature activated by the URL fragment.

After deploying the dashboard, append `#refresh=60` to the public share
URL (or iframe src if embedding):

```
https://<metabase-host>/public/dashboard/<public_uuid>#refresh=60
```

For the Metabase GUI: open the dashboard → click the refresh icon
in the top-right → select "1 minute". This is client-side state, not
server-persisted, so document the URL fragment in the client handoff doc.

(source: "Dashboards" overview, Metabase Learn notebook, source 04cf5679;
Metabase Craft corpus query 2026-04-22 — no REST API key for auto-refresh
exists on OSS)

### 5. Add a top-of-page freshness tile

Every live-by-default dashboard requires a "Data freshness" scalar tile
at the top reading from the raw source's `_ingested_at` column. This makes
ingest-pipeline regressions visible in the dashboard itself.

Copy this block from `speed_to_lead.py` and adjust the source table:

```python
freshness_tile = upsert_card(
    mb,
    name="Data freshness (end-to-end lag)",
    collection_id=coll["id"],
    database_id=db_id,
    display="scalar",
    cache_ttl=0,
    native_query=(
        "SELECT timestamp_diff(current_timestamp(), max(_ingested_at), minute) "
        "AS minutes_since_raw_ingest "
        "FROM `dee-data-ops-prod.raw_<source>.<table>`"
    ),
    visualization_settings={
        "scalar.field": "minutes_since_raw_ingest",
        **_col_settings({
            "minutes_since_raw_ingest": {"suffix": " min", "decimals": 0},
        }),
    },
)
```

Place it at `row=0, col=0, size_x=6, size_y=2` in `set_dashboard_cards`.
Push all other rows down by 2.

Use the highest-cadence raw source for the dashboard's primary data. For
Speed-to-Lead that is `raw_ghl.conversations`. For a Calendly-only
dashboard, use `raw_calendly.<table>`.

### 6. Add the dashboard to `caching_config.py`

Add an entry to `DASHBOARD_CACHE_TTL_SEC` in
`ops/metabase/authoring/infrastructure/caching_config.py`:

```python
DASHBOARD_CACHE_TTL_SEC: dict[str, int | None] = {
    "Speed-to-Lead": 0,
    "Speed-to-Lead — Lead Detail": 0,
    "<Your New Dashboard Name>": 0,  # live-by-default
}
```

Or rely on `DEFAULT_CACHE_TTL_SEC = 0` — any dashboard not in the dict
automatically gets `0`. Explicitly naming it makes intent legible.

### 7. Deploy: run the authoring script against prod Metabase

```bash
source .venv/bin/activate
set -a && source ops/metabase/.env.metabase && set +a
python -m ops.metabase.authoring.dashboards.<mart_name>
```

Confirm the output shows:
- Card upserts (PUT or POST for new cards)
- Dashboard upsert (PUT or POST)
- `Orphan cleanup: no orphan cards found.` (or a list of archived orphans)
- The dashboard URL printed at the end

### 8. Run `caching_config.py` to set cache_ttl=0 on all STL cards

```bash
python -m ops.metabase.authoring.infrastructure.caching_config
```

Confirm output shows `cache_ttl after: 0` for your dashboard. If it shows
`cache_ttl after: None` or another value, it did not persist — see the OSS
limitation note in the script's docstring. Server-wide caching
(`MB_ENABLE_QUERY_CACHING=true`) still operates via TTL-ratio formula.

### 9. Manual verification in the browser

1. Load the dashboard in a browser tab.
2. Add `#refresh=60` to the URL and reload — confirm the 60-second refresh
   ticker appears in the top-right corner.
3. Confirm the freshness tile shows the expected lag:
   - Pre-Tracks-W+Y: whatever the current GHA cadence produces (5–15 min).
   - Post-Tracks-W+Y: should show `< 2 min` at steady state.
4. Confirm no tile shows "Question failed" or spinner-of-death.
5. Confirm the orphan cleanup ran (no stale cards in the collection).

---

## One-off reset: flip existing dashboard to live-by-default

If an existing dashboard was built with `cache_ttl=21600` (old daily-cadence
default) and you're switching it to live:

1. Update the dashboard's `DASHBOARD_CACHE_TTL_SEC` entry to `0`.
2. Re-run `caching_config.py` — it's idempotent and will PUT the new value.
3. Re-run the authoring script — `upsert_card` now sends `cache_ttl=0` on
   all tiles.
4. Verify in browser.

No dbt changes needed — this is Metabase-layer only.

---

## Reference

- `.claude/rules/live-by-default.md` — full policy + when to deviate
- `.claude/rules/metabase.md` — all five Metabase authoring conventions
- `ops/metabase/authoring/dashboards/speed_to_lead.py` — canonical reference script
- `ops/metabase/authoring/infrastructure/caching_config.py` — per-dashboard TTL dict
