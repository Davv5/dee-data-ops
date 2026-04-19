# GHL (GoHighLevel) Extractor

Custom Python extractor for GHL → BigQuery `raw_ghl.*`. Runs on a GitHub Actions cron
(see `.github/workflows/ingest.yml`). Fivetran does not support GHL on the free tier,
so this lives in-repo.

## What it pulls

Four v1 endpoints (scope §4 / v1 plan Phase 1):

- `contacts` — lead roster, tags, attribution fields
- `conversations` — calls + SMS (the "SDR touched the lead" signal)
- `opportunities` — pipeline stages
- `users` — SDR / AE roster

Everything lands untransformed in `raw_ghl.<endpoint>` (WRITE_APPEND) with a
`_ingested_at` UTC timestamp column. Deduping and type-casting happen in staging
(Phase 2), not here. Source for the raw-landing discipline: *"Data Ingestion / Raw
Landing Zone"*, Data Ops notebook.

## Schema organization — one dataset per source

`raw_ghl` is its own dataset, alongside `raw_calendly` / `raw_typeform` / `raw_stripe` /
`raw_fanbasis`. This is the corpus-recommended "one schema per source" pattern (source:
*"Data Ingestion / Raw Landing Zone"*, Data Ops notebook) — easier to lock down per-source
permissions, easier to read at a glance.

## Cursor state

`raw_ghl._sync_state` holds a watermark per endpoint:

| column | type | note |
|---|---|---|
| `endpoint` | STRING | one of `contacts` / `conversations` / `opportunities` / `users` |
| `last_synced_at` | TIMESTAMP | UTC; the time *this run started*, written after a successful load |
| `updated_at` | TIMESTAMP | row-write audit |

On each run, `read_cursor(endpoint)` returns the most recent `last_synced_at`; the
extractor passes that as `since` to `fetch_endpoint`. First run sees `None` and pulls
everything.

> **Not corpus-prescribed.** The corpus is silent on cursor storage, upsert vs append,
> and metadata columns. The watermark-in-BQ + append-only pattern here is David's
> reasoned default: keeps state next to the data it describes, avoids a second
> store, and is easy to inspect with `SELECT * FROM raw_ghl._sync_state`.

Once GHL API behavior is confirmed in Week 0:

- If the API returns a reliable `updated_at` on records, use *that* for `last_synced_at`
  (per-record high-water mark, cleaner than wall-clock).
- If it doesn't, keep wall-clock and accept some row overlap — staging dedupes on
  `id` + latest `_ingested_at` anyway (Phase 2).

## Running it

### Local

```bash
cd "/Users/david/Documents/data ops"
set -a && source .env && set +a
source .venv/bin/activate
pip install -r ingestion/ghl/requirements.txt

# Smoke test — exercises BQ client + state table, doesn't hit GHL:
python ingestion/ghl/extract.py --dry-run

# Real run (no-op until fetch_endpoint is wired to the API):
python ingestion/ghl/extract.py
```

### CI

Invoked by `.github/workflows/ingest.yml` daily at 06:00 UTC (+ `workflow_dispatch`
for on-demand reruns). Auth is via `google-github-actions/auth@v2` + repo secret
`GCP_SA_KEY` (base64-encoded JSON of the deploy service account).

Required GitHub Actions secrets:

- `GCP_SA_KEY` — dbt-prod (or a dedicated ingest SA) JSON keyfile, base64
- `GHL_API_KEY` — GHL private integration token (Week-0 client ask)

## Status

**Stub.** `fetch_endpoint` returns `[]`; everything else is live. To finish:

1. Week-0: get GHL API credentials from client; confirm tenant + endpoint base URL.
2. Implement `fetch_endpoint`:
   - `GET https://services.leadconnectorhq.com/{endpoint}` (or the equivalent — confirm)
   - Paginate; filter by `updatedSince` if the endpoint supports it, else full pull
     and let staging dedupe.
3. Trigger `workflow_dispatch` manually; verify rows land in `raw_ghl.<endpoint>`.
4. Wait one cron cycle; confirm second run pulls deltas only (check
   `raw_ghl._sync_state`).

## Troubleshooting

- **`google.api_core.exceptions.Forbidden: 403 Access Denied`** — the active SA doesn't
  have `BigQuery Data Editor` + `Job User` on `dee-data-ops`. Fixed in Phase 0 for the
  dev SA; CI uses a separate key.
- **`KeyError: 'GCP_PROJECT_ID_DEV'`** — `.env` not sourced. `set -a && source .env && set +a`.
- **`_sync_state` has no row for an endpoint after a successful run** — means `load_rows`
  got 0 rows back from `fetch_endpoint`; cursor is only written when `loaded > 0` so
  the next run re-pulls the same window.
