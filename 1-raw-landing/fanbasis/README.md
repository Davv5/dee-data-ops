# Fanbasis Extractor

Custom Python extractor for Fanbasis → BigQuery `raw_fanbasis.*`. Same shape as
`1-raw-landing/ghl/` — see that README for the design-choice rationale, corpus citations,
and cursor-state pattern.

## Current status

This extractor is still a repo-local skeleton: BigQuery loading/state helpers
exist, but `fetch_endpoint()` returns no rows until the Fanbasis API contract is
implemented. Discovery Sprint findings show Fanbasis transaction rows landing in
`project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`; that table
appears to come from an upstream/GTM-side path, not this script.

Before extending this extractor, confirm whether the GTM-side writer is the
authoritative path or whether this repo should own Fanbasis ingestion going
forward.

## What it pulls

Three v1 endpoints (scope §4):

- `customers`
- `subscriptions`
- `payments`

Intended repo-local landing target: `raw_fanbasis.<endpoint>` (WRITE_APPEND)
with a `_ingested_at` UTC column. This does not match the currently observed
GTM-side table shape under `Raw.fanbasis_transactions_txn_raw`; reconcile that
before building production dbt models.

## CSV-export fallback (scope Risk #5)

The Fanbasis API is the lowest-confidence source in v1 — reliability and endpoint
coverage are unknown until Week 0. If the API proves unworkable:

1. Fanbasis admin console exports CSV per entity.
2. Land CSVs to `gs://dee-data-ops-raw/fanbasis/<endpoint>/YYYY-MM-DD.csv` (manual
   drop is fine for v1 cadence — payments don't need same-day freshness).
3. Replace `fetch_endpoint` with a `load_csv_from_gcs(endpoint, date)` helper;
   everything downstream (cursor, staging) stays the same.
4. Escalate to weekly cadence if CSV is manual-only; the headline metric doesn't
   depend on Fanbasis (GHL + Calendly do).

## Running it

Same as `1-raw-landing/ghl/`:

```bash
python 1-raw-landing/fanbasis/extract.py --dry-run
python 1-raw-landing/fanbasis/extract.py
```

Required GitHub Actions secrets:

- `GCP_SA_KEY` — shared with ghl
- `FANBASIS_API_KEY` — Week-0 client ask
