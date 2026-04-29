---
paths: ["services/bq-ingest/**"]
---

# bq-ingest service rules

Load when working on any file under `services/bq-ingest/` (the Cloud Run Flask + Cloud Run Jobs ingestion path consolidated from `gtm-lead-warehouse` per `docs/plans/2026-04-28-bq-ingest-consolidation-plan.md`). Sister rule: `.claude/rules/ingest.md` — covers the broader ingestion contract and the dual-path coexistence with `1-raw-landing/`.

## Refresh order: Raw → Core → Marts

Manual refreshes inside this service must respect upstream order. Never skip Core when chasing a stale Mart number; never touch a Mart without first refreshing the upstream Core. Marts always run last.

This is enforced automatically by `dbt build` in the `2-dbt/` path (DAG ordering) and by `services/bq-ingest/sources/marts/mart_models.py` in the bq-ingest path (it runs upstream models first, then `services/bq-ingest/sql/marts.sql` and `services/bq-ingest/sql/dims/*.sql`). The SQL files live under the package-root `sql/` tree, not under `sources/marts/sql/` — historically the default `sql_file_path` resolved to the latter (non-existent) path, but PR #107 added a `parents[2]` fallback to all six previously-broken modules (calendly/fathom/fanbasis/marts/stripe `run_models()` plus `shared/data_quality.py:_resolve_dq_sql_file()`), matching the working pattern from `ghl_pipeline.py:1041-1045`. The rule still matters when (a) refreshing one source's Core via `python -m ops.runner.cli run pipeline.<source>` outside the marts wrapper, (b) hitting a single `/refresh-<source>-models` route on `app.py`, or (c) reasoning about which layer to investigate when a number looks wrong.

(Source: ported from `gtm-lead-warehouse/RUNBOOK.md` core rule #1 and the dropped `sources/marts/CLAUDE.md` "safe change order" line — both retired with PR #102 cleanup.)

## Fathom: core SQL only — no LLM enrichment lane in this tree

`services/bq-ingest/` contains the **core** Fathom ingestion path (transcript fetch + meeting models in `sql/fathom_models.sql`). It does NOT contain an LLM enrichment lane. The original `gtm-lead-warehouse/enrichment/fathom/` tree (Gemini transcript analysis, separate Dockerfile, high-memory runtime) was deliberately **dropped** during consolidation per the audit at `docs/discovery/bq-ingest-dependency-audit.md` because nothing in bq-ingest imported from it.

If a future task needs LLM transcript enrichment:

- Do NOT recreate it inside `services/bq-ingest/enrichment/` — the runtime profile (Gemini SDK, GPU/high-memory, separate auth scope) is materially different from the rest of bq-ingest, and merging them is the same anti-pattern the audit cleared out.
- Stand up a sibling service at `services/<name>/` with its own Dockerfile, deploy lifecycle, and rule file. The `services/` directory pattern accommodates additional sibling services.
- The dropped tree's source code, if needed for reference, lives in the archived `heidyforero1/gtm-lead-warehouse@515c89a` snapshot.

(Source: ported from the dropped `sources/fathom/CLAUDE.md` split-runtime note — retired with PR #102 cleanup.)

## Hourly HTTP path skips heavy model refresh; Cloud Run Job context owns it

Per-source `*_RUN_MODELS_AFTER_INCREMENTAL` env vars (CALENDLY/FATHOM/GHL/etc.) gate whether the hourly HTTP `/ingest-<source>` route calls `run_models()` inline after fetching new data. **These should be `false` on the bq-ingest Cloud Run service** (the in-code default). Reasons:

- The bq-ingest service has a 300s Cloud Run request timeout (default), and the gunicorn worker timeout is shorter still. Heavy model refreshes (>180s) hang the worker and produce 5xx for the hourly scheduler.
- The `pipeline-marts-hourly` Cloud Run Job already runs `model.<source>` via `run_marts_with_dependencies` (see `ops/runner/tasks.py:106-122`) before refreshing marts. Cloud Run **Jobs** have multi-hour timeouts (`timeout_seconds: 10800`) — that's where slow refreshes belong.
- The hourly HTTP endpoint's job is to land new Raw data (cheap); model refresh is downstream and runs on the marts cadence.

**Empirical anchor (2026-04-29).** `FATHOM_RUN_MODELS_AFTER_INCREMENTAL=true` was set on the live bq-ingest service for some prior reason. Hourly `/ingest-fathom` calls hit `run_models()` which exceeded ~180s (sum of `bridge_fathom_contact_match_candidates` 80-130s + BQML model retrain 70-100s + ML.PREDICT + enriched/diagnostics). The fathom hourly scheduler had been failing with 500s for 5+ days. Fix: flip the env-var to `false` (or remove the override; in-code default already `false`). Same architectural rule applies to `CALENDLY_RUN_MODELS_AFTER_INCREMENTAL` etc. — verify each is `false` on the service or that the corresponding source's `run_models()` is fast enough to fit the worker budget. A prototype SQL split into hourly + daily files was attempted and reverted because `marts.sql` reads tables from across the full file — splitting broke the marts hourly refresh contract.

## GHL `/contacts/search` uses `searchAfter` scroll, NOT page-mode

The contacts-incremental path in `sources/ghl/ghl_pipeline.py` deliberately overrides `pagination_mode` from `"page"` (the entry in `ENTITY_DEFAULT_PAGINATION`) to `"scroll"` for `POST /contacts/search`. The vendor contract is:

- **Request cursor:** body field `searchAfter` (a JSON array, not a string).
- **Response cursor:** each contact carries its own `searchAfter` array (no `meta` block).
- **Filter (date fields):** `{field: "dateUpdated", operator: "range", value: {gt: <epoch_ms_int>, lt: <epoch_ms_int>}}` — *not* bare `gt` / `gte` (those exist in the global operator enum but are rejected for date fields). The inner-value object accepts `{gt, lt}` or `{gte, lte}`. Value must be epoch milliseconds (int), not ISO-8601 string.

**Empirical anchor (2026-04-29, two iterations).** PRs #118/#119 first switched the filter from `gte`+ISO to `gt`+epoch_ms based on `accounting-qs/compete-iq`'s production code. Deployed as `bq-ingest-00093-xiv`. Live probe revealed `gt` was *also* rejected — the LeadConnector error message itself surfaced the global operator enum (`eq, not_eq, contains, not_contains, wildcard, not_wildcard, match, not_match, exists, not_exists, range, not_range, contains_set, contains_not_set, gt, gte, lt, lte, nested, nested_not, has_child, has_parent`) but the per-field validation rejected bare `gt`/`gte` on `date_updated` with `"Invalid Operator (gt) passed for field date_updated"`. PR #127 corrected with `operator: "range"` and inner-value `{gt: epoch_ms, lt: epoch_ms}` — verified against the live API with a far-future date range that returned 0 rows. Deployed as `bq-ingest-00083-5kk` (current live revision; `00093-xiv` retained at 0% as rollback). Compete-iq's pattern works for their tenant but not D-DEE's, suggesting either tenant divergence or vendor API change post-compete-iq's last successful call. **Lesson:** when probing an unknown vendor contract, the 422 error body often exposes the operator enum — use it as ground truth instead of compete-iq or community docs. The `ENTITY_DEFAULT_PAGINATION['contacts'] = 'page'` entry is *intentionally* still set for the GET `/contacts/` paths; do NOT "clean up" the contacts-incremental scroll override or the `_derive_pagination` scroll-branch's `searchAfter`-then-`sort` probe-order — both are vendor-contract carve-outs, not inconsistencies.

## Lessons learned

- **2026-04-29 (PRs #118/#119/#127, current live: `bq-ingest-00083-5kk`).** The 422 had been silently failing every hourly `/ingest-ghl` call for an unknown duration — `parse_object_types()` defaults to a list with `contacts` first, so contacts failed first every hour. The route returned HTTP 200 because per-entity failures were absorbed inside the loop (`ghl_pipeline.py:1211-1224`). Cloud Scheduler showed green; no monitoring caught it. Surfaced only when a separate audit (`*_RUN_MODELS_AFTER_INCREMENTAL` cleanup) made per-entity statuses visible in the JSON response. **Defense:** the operational-health rule's true-signal table is right — `MAX(ingested_at) FROM Raw.ghl_objects_raw WHERE entity_type='contacts'` is the only reliable freshness signal (the per-entity legacy `raw_ghl.ghl__contacts_raw` table stopped writing 2026-04-19; query the union table directly). The `/ingest-ghl` HTTP 200 is a false signal because per-entity failures don't propagate.
