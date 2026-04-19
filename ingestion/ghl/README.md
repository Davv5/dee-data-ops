# GHL (GoHighLevel) Extractor

Custom Python extractor for GHL → BigQuery `raw_ghl.*`. Runs on a GitHub Actions cron
(see `.github/workflows/ingest.yml`). Fivetran does not support GHL on the free tier,
so this lives in-repo.

## What it pulls

Four v2 / LeadConnector endpoints (scope §4 / v1 plan Phase 1):

- `contacts` — lead roster, tags, attribution fields
- `conversations` — calls + SMS (the "SDR touched the lead" signal)
- `opportunities` — pipeline stages
- `users` — SDR / AE roster

Everything lands untransformed in `raw_ghl.<endpoint>` (WRITE_APPEND) with a
`_ingested_at` UTC timestamp column. Deduping and type-casting happen in staging
(Phase 2), not here. Source for the raw-landing discipline: *"Data Ingestion / Raw
Landing Zone"*, Data Ops notebook.

## API shape (v2 / LeadConnector)

- **Base URL:** `https://services.leadconnectorhq.com`
- **Auth:** `Authorization: Bearer <GHL_API_KEY>` — a Private Integration Token
  minted from Settings → Private Integrations in the GHL sub-account.
- **Version header:** every request needs a `Version: YYYY-MM-DD` header. Most
  endpoints sit on `2021-07-28`; **`conversations` is pinned to `2021-04-15`**
  — confirmed against the published OpenAPI spec. Per-endpoint mapping lives
  in the `VERSIONS` dict at the top of `extract.py`.
- **Location-scoped:** v2 tokens are issued per location (sub-account), so every
  request passes `locationId=<GHL_LOCATION_ID>` (snake_case `location_id` for the
  opportunities endpoint — GHL's own inconsistency, not ours).
- **Pagination:** cursor-based via `startAfter` + `startAfterId` on contacts,
  `startAfterDate` on conversations, and `meta.startAfter` / `meta.startAfterId`
  on opportunities. `users` is small enough to fetch in one shot.

## Incremental vs full pull

| endpoint | mode | cursor field |
|---|---|---|
| `contacts` | full pull, page through | `dateAdded` / `id` (pagination only, not since-filter) |
| `conversations` | **incremental** | `startAfterDate` seeded from `_sync_state.last_synced_at` |
| `opportunities` | full pull, page through | `meta.startAfter` / `meta.startAfterId` (pagination only) |
| `users` | full pull, one page | — |

Only `conversations` honors a since-filter cleanly. For the others, the GET
endpoints don't expose a reliable `updated_since` parameter, so we accept a full
pull and let staging dedupe on `id` + latest `_ingested_at` (Phase 2). Revisit if
daily volume becomes a cost or latency concern.

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
everything. The cursor is only written when rows loaded > 0, so an empty pull leaves
the window open for the next run to retry.

> **Not corpus-prescribed.** The corpus is silent on cursor storage, upsert vs append,
> and metadata columns. The watermark-in-BQ + append-only pattern here is a reasoned
> default: keeps state next to the data it describes, avoids a second store, and is
> easy to inspect with `SELECT * FROM raw_ghl._sync_state`.

## Required env vars

| var | where | purpose |
|---|---|---|
| `GCP_PROJECT_ID_DEV` | `.env` + CI workflow env | target BQ project (`dee-data-ops`) |
| `GHL_API_KEY` | `.env` (local) / GH Actions secret | Private Integration Token |
| `GHL_LOCATION_ID` | `.env` (local) / GH Actions secret | sub-account ID the token is scoped to |
| `BQ_KEYFILE_PATH` | `.env` (local only) | path to dev SA JSON keyfile |

CI auth is handled by `google-github-actions/auth@v2` using the `GCP_SA_KEY` secret
— no keyfile path needed there.

## Running it

### Local

```bash
cd "/Users/david/Documents/data ops"
set -a && source .env && set +a
source .venv/bin/activate
pip install -r ingestion/ghl/requirements.txt

# Smoke test — exercises BQ client + state table, hits GHL read-only:
python ingestion/ghl/extract.py --dry-run

# Real run:
python ingestion/ghl/extract.py
```

### CI

Invoked by `.github/workflows/ingest.yml` daily at 06:00 UTC (+ `workflow_dispatch`
for on-demand reruns). Auth is via `google-github-actions/auth@v2` + repo secret
`GCP_SA_KEY` (raw JSON of the ingest service account).

Required GitHub Actions secrets: `GCP_SA_KEY`, `GHL_API_KEY`, `GHL_LOCATION_ID`.

## Troubleshooting

- **`401 Unauthorized`** — token expired or wrong location. Re-mint the Private
  Integration Token and confirm `GHL_LOCATION_ID` matches the location it was
  created under.
- **`400 Bad Request` on `/conversations/search`** — usually a Version-header
  mismatch. Conversations uses `2021-04-15`, not `2021-07-28`.
- **`google.api_core.exceptions.Forbidden: 403 Access Denied`** — the active SA
  doesn't have `BigQuery Data Editor` + `Job User` on `dee-data-ops`.
- **`KeyError: 'GCP_PROJECT_ID_DEV'`** — `.env` not sourced. `set -a && source .env && set +a`.
- **`_sync_state` has no row for an endpoint after a successful run** — means
  `load_rows` got 0 rows back from `fetch_endpoint`; cursor is only written when
  `loaded > 0` so the next run re-pulls the same window.
