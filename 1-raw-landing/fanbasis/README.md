# Fanbasis ingestion — moved

**This directory is retired.** It used to hold a Week-0-blocked GH Actions
extractor skeleton (`extract.py` returning no rows until D-DEE delivered Fanbasis
API credentials). Those credentials never landed via the v1 path; meanwhile the
authoritative Fanbasis pipeline grew up inside the `gtm-lead-warehouse`
`bq-ingest` service and was consolidated into this repo on 2026-04-28 (PR #102,
Step 2 of the bq-ingest consolidation).

## Where Fanbasis ingestion lives now

`services/bq-ingest/sources/fanbasis/`

- **Pipeline:** `fanbasis_pipeline.py` — paginated `/checkout-sessions/transactions`
  poller, MERGE-by-`transaction_id`, separate `fanbasis_backfill_state` table
- **Backfill:** `fanbasis_backfill.py`
- **Raw landing target:** `project-41542e21-470f-4589-96d.Raw.fanbasis_transactions_txn_raw`
- **Trigger:** Cloud Run Flask service (`bq-ingest`), invoked by the bq-ingest
  scheduler — not GitHub Actions
- **Service rule:** `.claude/rules/bq-ingest.md`
- **Ingestion contract:** `.claude/rules/ingest.md` (v1 source inventory updated
  in the same PR that retired this skeleton)

## Downstream wiring

- **Staging:** `2-dbt/models/staging/fanbasis/stg_fanbasis__transactions.sql` +
  `stg_fanbasis__refunds.sql`
- **Warehouse:** `fct_payments`, `fct_refunds`, `bridge_identity_contact_payment`
- **Mart:** `revenue_detail`

## Why the directory itself isn't deleted

Kept as a tombstone so future-Claude searching `1-raw-landing/fanbasis/` lands
here instead of recreating the skeleton. The directory has no extractor, no
requirements.txt, no executable code — only this README. Safe to delete in a
later cleanup pass once enough sessions have referenced the new location that
nobody looks here first.
