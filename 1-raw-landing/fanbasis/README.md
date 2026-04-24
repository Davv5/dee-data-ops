# Fanbasis Extractor

Custom Python extractor for Fanbasis → BigQuery `raw_fanbasis.*`. Same shape as
`1-raw-landing/ghl/` — see that README for the design-choice rationale, corpus citations,
and cursor-state pattern.

## What it pulls

Three v1 endpoints (scope §4):

- `customers`
- `subscriptions`
- `payments`

Lands in `raw_fanbasis.<endpoint>` (WRITE_APPEND) with a `_ingested_at` UTC column.

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
